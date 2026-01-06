import asyncio
import logging
import os
import re
import tarfile
import tempfile
import zipfile

from pathlib import Path
from typing import get_args, Literal

from base.api.documents import FragmentUri
from base.strings.data import (
    MIME_TYPE_ARXIV_SRC,
    MIME_TYPE_ARXIV_TEX,
    DataUri,
    MimeType,
)
from base.strings.file import FileName, FilePath

from documents.extractors.image import (
    load_image_as_data_uri_sync,
    load_pdf_as_data_uri_sync,
)
from documents.extractors.pandoc import extract_pandoc_sync
from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted, DownloadedFile
from documents.models.processing import ExtractOptions, Extractor

logger = logging.getLogger(__name__)

UNIX_SYMLINK_MASK = 0o120000

ARCHIVE_MIME_TYPES: tuple[MimeType, ...] = (MIME_TYPE_ARXIV_SRC,)

ArchiveExt = Literal[".tar", ".tar.gz", ".tgz", ".zip"]


class ArchiveExtractor(Extractor):
    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(
            (downloaded.mime_type in ARCHIVE_MIME_TYPES)
            or (
                downloaded.filename
                and downloaded.filename.ext() in get_args(ArchiveExt)
            )
        )

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        if not isinstance(downloaded, DownloadedFile):
            raise ExtractError.fail("archive", "requires DownloadedFile")

        return await asyncio.to_thread(_archive_extract_sync, downloaded)


def _archive_extract_sync(downloaded: DownloadedFile) -> Extracted:
    if not downloaded.filename:
        raise ExtractError.fail("archive", "requires filename")

    archive_exts: tuple[ArchiveExt, ...] = get_args(ArchiveExt)
    filename_ext: ArchiveExt
    if (filename_ext_str := downloaded.filename.ext()) in archive_exts:
        filename_ext = filename_ext_str
    else:
        raise ExtractError.fail(
            "archive",
            f"requires extension of {', '.join(archive_exts)}, got {filename_ext_str}",
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        if filename_ext == ".zip":
            basename = downloaded.filename.removesuffix(".zip")
            _safe_extract_zip_from_path(
                str(downloaded.tempfile_path),
                temp_dir,
            )

        elif filename_ext in (".tar.gz", ".tgz", ".tar"):
            if filename_ext == ".tar.gz":
                basename = downloaded.filename.removesuffix(".tar.gz")
                mode = "r:gz"
            elif filename_ext == ".tgz":
                basename = downloaded.filename.removesuffix(".tgz")
                mode = "r:gz"
            else:  # .tar
                basename = downloaded.filename.removesuffix(".tar")
                mode = "r"

            _safe_extract_tar_from_path(
                str(downloaded.tempfile_path),
                temp_dir,
                mode,
            )

        if extracted := _extract_archive_latex_sync(temp_dir, basename):
            return extracted
        else:
            raise ExtractError.fail("archive", "unknown archive format: supports LaTeX")


##
## Unzip
##


def _safe_extract_zip(zip_ref: zipfile.ZipFile, extract_path: str) -> None:
    """Safely extract a zip file with comprehensive path validation."""
    extract_path_resolved = Path(extract_path).resolve()

    # Two-pass approach: First validate ALL files, then extract if ALL are safe
    files_to_extract = []

    for member in zip_ref.infolist():
        # Skip directories (these are safe).
        if member.filename.endswith("/") or member.is_dir():
            continue

        # Check for symlinks.
        if member.external_attr & UNIX_SYMLINK_MASK == UNIX_SYMLINK_MASK:
            raise ExtractError.security_violation("ZIP", "unsafe content detected")

        # Validate path safety.
        normalized_filename = os.path.normpath(member.filename)
        if normalized_filename.startswith(("..", "/")) or (
            len(normalized_filename) > 1 and normalized_filename[1] == ":"
        ):
            raise ExtractError.security_violation("ZIP", "unsafe content detected")

        # Ensure extraction stays within target directory.
        try:
            member_path_resolved = (extract_path_resolved / member.filename).resolve()
            if not str(member_path_resolved).startswith(
                str(extract_path_resolved) + os.sep
            ):
                raise ExtractError.security_violation("ZIP", "unsafe content detected")
        except (ValueError, OSError):
            raise ExtractError.security_violation(  # noqa: B904
                "ZIP", "unsafe content detected"
            )

        # File is safe, add to extraction list.
        files_to_extract.append(member)

    # All files validated successfully - now extract them.
    for member in files_to_extract:
        zip_ref.extract(member, extract_path)


def _safe_extract_tar(tar_ref: tarfile.TarFile, extract_path: str) -> None:
    """Safely extract a tar file using filter='data' with consistent error handling."""
    try:
        tar_ref.extractall(extract_path, filter="data")
    except (
        tarfile.OutsideDestinationError,
        tarfile.AbsolutePathError,
        tarfile.LinkOutsideDestinationError,
    ):
        raise ExtractError.security_violation(  # noqa: B904
            "TAR", "unsafe content detected"
        )


def _safe_extract_zip_from_path(file_path: str, temp_dir: str) -> None:
    """Safely extract ZIP archive from file path with security validation"""
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        _safe_extract_zip(zip_ref, temp_dir)


def _safe_extract_tar_from_path(
    file_path: str,
    temp_dir: str,
    mode: Literal["r", "r:gz"],
) -> None:
    """Safely extract TAR archive from file path with security validation"""
    with tarfile.open(file_path, mode=mode) as tar_ref:
        _safe_extract_tar(tar_ref, temp_dir)


##
## Parsing
##


def _extract_archive_latex_sync(  # noqa: C901, PLR0912
    temp_dir: str,
    basename: str,
) -> Extracted | None:
    # Find the main file of the LaTeX document.
    root_latex_files = [
        file.name
        for file in Path(temp_dir).iterdir()
        if file.is_file() and file.suffix == ".tex"
    ]
    source_filename: FileName | None
    if len(root_latex_files) == 1:
        source_filename = FileName.normalize(root_latex_files[0])
    else:
        main_filename = next(filter(lambda f: "main" in f, root_latex_files), None)
        source_filename = FileName.normalize(main_filename) if main_filename else None

    if not source_filename:
        return None

    source_path = Path(temp_dir) / source_filename
    content = source_path.read_text()

    # Extract figures as PNGs.
    replacements: dict[str, FragmentUri] = {}
    all_images: dict[FragmentUri, DataUri] = {}

    for root, _, files in os.walk(temp_dir):
        for file in files:
            original_media_type = MimeType.guess_or_plain(file)
            file_path = Path(root) / file
            relative_path = file_path.relative_to(temp_dir)

            image_path = FilePath.normalize(str(relative_path))
            if not image_path:
                continue

            image_uri = FragmentUri.new(image_path)
            image_data: DataUri | None = None
            if original_media_type == "application/pdf":
                image_data = load_pdf_as_data_uri_sync(file_path)
            elif original_media_type.startswith("image/"):
                image_data = load_image_as_data_uri_sync(file_path)

            if image_data:
                replacements[str(relative_path)] = image_uri
                all_images[image_uri] = image_data

    # Insert "\input" directives before passing to Pandoc.
    # We handle 3 levels of nested \input directives.
    for _ in range(3):
        if "\\input{" not in content:
            break

        input_files = {}
        for match in re.finditer(r"\\input\{([^}]+)\}", content):
            directive = match.group(0)
            input_key = match.group(1)
            if not input_key.endswith(".tex"):
                input_key = f"{input_key}.tex"

            input_path = Path(temp_dir) / input_key
            if input_path.is_file():
                input_content = input_path.read_text()
                if "/" in input_key:
                    input_content = input_content.replace(
                        "\\input{",
                        f"\\input{{{Path(input_key).parent}/}}",
                    )
                input_files[directive] = input_content
            else:
                logger.warning("LaTeX input not found: %s", directive)

        for directive, input_content in input_files.items():
            content = content.replace(directive, input_content)

    # Update figures to reference PNGs.
    # Only include images that appear in the document (\input included).
    selected_images: dict[FragmentUri, DataUri] = {}
    for before, after in replacements.items():
        if before in content:
            content = content.replace(before, str(after))
            selected_images[after] = all_images[after]

    # Extract the content as Markdown.
    # NOTE: We do not include the bibliography to save on tokens, but it is
    # trivial to add if the need ever arises.
    source_path.write_text(content)
    latex_file = DownloadedFile(
        url=None,
        response_headers={},
        name=None,
        mime_type=MIME_TYPE_ARXIV_TEX,
        filename=source_filename,
        charset=None,
        tempfile_path=source_path,
    )
    extracted = extract_pandoc_sync(latex_file)

    return Extracted(
        mode="markdown",
        name=extracted.name,
        path=FileName.decode(f"{basename}.tex").filepath(),
        mime_type=MIME_TYPE_ARXIV_TEX,
        blobs={**extracted.blobs, **all_images},
        text=extracted.text,
    )
