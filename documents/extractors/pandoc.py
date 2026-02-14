import asyncio
import contextlib
import logging
import os
import shutil
import subprocess
import tempfile
import zipfile

from pathlib import Path

from base.api.documents import FragmentUri
from base.strings.data import DataUri, MimeType
from base.strings.file import FilePath

from documents.extractors.image import image_bytes_as_data_uri_sync
from documents.models.exceptions import DocumentsError, ExtractError
from documents.models.pending import Downloaded, Extracted, DownloadedFile
from documents.models.processing import ExtractOptions, Extractor

logger = logging.getLogger(__name__)

# fmt: off
PANDOC_EXTS = [
    ".bib", ".dbk", ".docx", ".epub", ".fb2", ".ipynb", ".muse", ".odt",
    ".opml", ".org", ".ris", ".rst", ".rtf", ".t2t", ".tex", ".textile",
    ".tsv",
]
# fmt: on


class PandocExtractor(Extractor):
    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(downloaded.filename and downloaded.filename.ext() in PANDOC_EXTS)

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        if not isinstance(downloaded, DownloadedFile):
            raise ExtractError.fail("pandoc", "requires DownloadedFile")

        return await extract_pandoc(downloaded)


async def extract_pandoc(downloaded: DownloadedFile) -> Extracted:
    return await asyncio.to_thread(extract_pandoc_sync, downloaded)


def extract_pandoc_sync(downloaded: DownloadedFile) -> Extracted:
    if not downloaded.mime_type:
        raise ExtractError.fail("pandoc", "requires mime_type")
    if not downloaded.filename:
        raise ExtractError.fail("pandoc", "requires filename")
    if not (file_ext := downloaded.filename.ext()):
        raise ExtractError.fail("pandoc", "requires filename extension")

    try:
        options = [
            "markdown",
            "-header_attributes",
            "-link_attributes",
            "-native_divs",
            "-native_spans",
            "-raw_html",
            "+hard_line_breaks",
            "+latex_macros",
            "+pipe_tables",
            "-fenced_divs",
            "-bracketed_spans",
        ]

        # SECURITY: Use sandboxed pandoc to prevent LaTeX injection
        content = run_pandoc_sandboxed(
            source_file=str(downloaded.tempfile_path),
            output_format="".join(options),
        )

        # Extract images embedded in Word document.
        blobs: dict[FragmentUri, DataUri] = {}
        if file_ext == ".docx":
            for old_path, image in _extract_docx_images(
                downloaded.tempfile_path
            ).items():
                image_self_url, image_data_uri = image
                blobs[image_self_url] = image_data_uri
                content = content.replace(old_path, str(image_self_url))

        return Extracted(
            mode="markdown",
            name=None,
            path=None,
            mime_type=downloaded.mime_type,
            blobs=blobs,
            text=content,
        )
    except Exception as exc:
        raise ExtractError.fail("pandoc", str(exc)) from exc


def _extract_docx_images(docx_path: Path) -> dict[str, tuple[FragmentUri, DataUri]]:
    """
    Extract images from a .docx file and convert them to base64 strings.
    Returns a dictionary mapping original filenames to base64 strings.
    """
    images: dict[str, tuple[FragmentUri, DataUri]] = {}
    with zipfile.ZipFile(docx_path) as docx_zip:
        for file in docx_zip.namelist():
            try:
                if file.startswith("word/media/"):
                    media_type = MimeType.guess_or_plain(file)
                    if media_type == "image/emf":
                        continue  # Not supported by Pillow.
                    if media_type.startswith("image/"):
                        image_path = file.removeprefix("word/")  # Match Pandoc MD.
                        image_bytes = docx_zip.read(file)
                        if (parsed_path := FilePath.try_normalize(image_path)) and (
                            image_data_uri := image_bytes_as_data_uri_sync(
                                image_bytes, media_type
                            )
                        ):
                            images[image_path] = (
                                FragmentUri.new(parsed_path),
                                image_data_uri,
                            )
            except Exception:
                # Ignore images that cannot be read.
                logger.exception("Failed to load image: %s", file)

    return images


##
## Utils
##


class PandocError(DocumentsError):
    """Custom exception for pandoc-related errors"""


# File extension to pandoc input format mapping
# For security: we avoid formats that can execute code or access filesystem
PANDOC_FORMAT_MAP = {
    ".tex": "latex",  # LaTeX files
    ".md": "markdown",  # Markdown files
    ".rst": "rst",  # reStructuredText
    ".org": "org",  # Org-mode
    ".textile": "textile",  # Textile
    ".docx": "docx",  # Word documents
    ".odt": "odt",  # OpenDocument
    ".epub": "epub",  # EPUB
    ".fb2": "fb2",  # FictionBook2
    ".ipynb": "ipynb",  # Jupyter notebooks
    ".rtf": "rtf",  # RTF
    ".t2t": "t2t",  # txt2tags
    ".bib": "bibtex",  # BibTeX
    ".ris": "ris",  # RIS bibliography
    ".dbk": "docbook",  # DocBook
    ".opml": "opml",  # OPML
    ".muse": "muse",  # Muse
    ".tsv": "tsv",  # TSV tables
}


def get_safe_environment() -> dict[str, str]:
    """
    Get a restricted environment for running pandoc securely.

    Returns:
        Dictionary of safe environment variables
    """
    env: dict[str, str] = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",  # Minimal PATH incl. common pandoc locations
        "HOME": "/tmp",  # Fake HOME directory  # noqa: S108
        "USER": "nobody",  # Non-privileged user name
        "LANG": "C.UTF-8",  # Basic locale
    }

    # Preserve PANDOC_DATA_DIR if it exists to ensure access to built-in templates/resources
    existing_data_dir = os.environ.get("PANDOC_DATA_DIR")
    if existing_data_dir:
        env["PANDOC_DATA_DIR"] = existing_data_dir

    return env


def _get_pandoc_path() -> str:
    """
    Resolve the absolute path to the pandoc executable.

    Raises:
        PandocError: If pandoc cannot be found in PATH
    """
    pandoc_path = shutil.which("pandoc")
    if not pandoc_path:
        raise PandocError("Pandoc executable not found in PATH")
    return pandoc_path


def detect_input_format(file_path: str) -> str:
    """
    Detect pandoc input format based on file extension.

    Args:
        file_path: Path to the input file

    Returns:
        Pandoc input format string
    """
    file_ext = Path(file_path).suffix.lower()
    return PANDOC_FORMAT_MAP.get(file_ext, "markdown")


def run_pandoc_sandboxed(  # noqa: C901, PLR0912, PLR0915
    source_file: str | None = None,
    source_content: str | None = None,
    output_format: str = "markdown",
    output_file: str | None = None,
    input_format: str | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 60,
    use_sandbox: bool = True,
) -> str:
    """
    Run pandoc in a secure environment to prevent security vulnerabilities
    like LaTeX injection attacks that could read environment variables.

    Args:
        source_file: Path to input file (either this or source_content required)
        source_content: Input content as string (either this or source_file required)
        output_format: Pandoc output format (default: "markdown")
        output_file: Optional output file path (if not provided, returns stdout)
        input_format: Optional input format (auto-detected if not provided)
        extra_args: Additional pandoc arguments
        timeout: Command timeout in seconds
        use_sandbox: Whether to use --sandbox mode (default: True). Set to False
                    for formats like docx that need access to data files.

    Returns:
        Pandoc output as string (if output_file not provided)

    Raises:
        PandocError: If pandoc execution fails
    """
    if not source_file and not source_content:
        raise PandocError("Either source_file or source_content must be provided")

    # Create a dedicated sandbox working directory
    sandbox_dir = Path(tempfile.mkdtemp(prefix="pandoc_sandbox_"))
    staged_input_path: Path
    staged_output_path: Path | None = None
    original_output_target: Path | None = Path(output_file) if output_file else None

    # Stage input inside sandbox
    if source_content is not None:
        staged_input_path = sandbox_dir / "input.md"
        staged_input_path.write_text(source_content)
    else:
        assert source_file is not None
        input_ext = Path(source_file).suffix or ".md"
        staged_input_path = sandbox_dir / f"input{input_ext}"
        shutil.copy2(source_file, staged_input_path)

    try:
        # Auto-detect input format if not provided
        if not input_format:
            if source_file:
                input_format = detect_input_format(source_file)
            else:
                input_format = "markdown"  # Default to markdown for content input

        # Apply security restrictions to input format
        # Disable raw HTML and TeX to prevent XSS and injection attacks
        secure_input_format = input_format
        if input_format in ["markdown", "commonmark", "gfm"]:
            secure_input_format = f"{input_format}-raw_html-raw_tex"

        # Rewrite extra args to ensure reference doc (if any) is accessible inside sandbox
        rewritten_extra_args: list[str] = []
        if extra_args:
            i = 0
            while i < len(extra_args):
                arg = extra_args[i]
                if arg.startswith("--reference-doc="):
                    ref_path = arg.split("=", 1)[1]
                    staged_ref = sandbox_dir / "reference.docx"
                    try:
                        shutil.copy2(ref_path, staged_ref)
                        rewritten_extra_args.append(f"--reference-doc={staged_ref}")
                    except Exception as ref_exc:
                        raise PandocError(
                            f"Failed to stage reference doc: {ref_exc!s}"
                        ) from ref_exc
                    i += 1
                    continue
                if arg == "--reference-doc" and i + 1 < len(extra_args):
                    ref_path = extra_args[i + 1]
                    staged_ref = sandbox_dir / "reference.docx"
                    try:
                        shutil.copy2(ref_path, staged_ref)
                        rewritten_extra_args.extend(
                            ["--reference-doc", str(staged_ref)]
                        )
                    except Exception as ref_exc:
                        raise PandocError(
                            f"Failed to stage reference doc: {ref_exc!s}"
                        ) from ref_exc
                    i += 2
                    continue
                rewritten_extra_args.append(arg)
                i += 1

        # Determine staged output path
        if original_output_target is not None:
            staged_output_path = sandbox_dir / original_output_target.name

        # Build pandoc command
        pandoc_path = _get_pandoc_path()
        cmd = [
            pandoc_path,
            str(staged_input_path),
            "--to",
            output_format,
            "--from",
            secure_input_format,
            "--no-highlight",
            "--standalone",  # Ensure proper document structure
        ]

        if use_sandbox:
            cmd.append("--sandbox")

        if staged_output_path is not None:
            cmd.extend(["--output", str(staged_output_path)])

        if rewritten_extra_args:
            cmd.extend(rewritten_extra_args)

        # Run with restricted environment
        result = subprocess.run(  # noqa: S603
            cmd,
            env=get_safe_environment(),
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(sandbox_dir),
            check=False,
        )

        if result.returncode != 0:
            raise PandocError(f"Pandoc failed: {result.stderr}")

        # If output written to file, move it back to target path
        if staged_output_path is not None and original_output_target is not None:
            try:
                shutil.copy2(staged_output_path, original_output_target)
            except Exception as copy_out_exc:
                raise PandocError(
                    f"Failed to move pandoc output: {copy_out_exc!s}"
                ) from copy_out_exc
            return ""

        return result.stdout

    except subprocess.TimeoutExpired as exc:
        raise PandocError("Pandoc conversion timed out") from exc
    except Exception as exc:
        raise PandocError(f"Pandoc execution failed: {exc!s}") from exc
    finally:
        # Cleanup sandbox directory
        with contextlib.suppress(Exception):
            shutil.rmtree(sandbox_dir, ignore_errors=True)
