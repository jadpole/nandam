import asyncio
import shutil
import tempfile

from dataclasses import replace
from pathlib import Path
from typing import Literal

from base.strings.data import MimeType
from base.strings.file import FileName

from documents.extractors.pandoc import extract_pandoc
from documents.extractors.pdf import PdfExtractor
from documents.extractors.transcript import _run_shell
from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted, DownloadedFile
from documents.models.processing import ExtractOptions, Extractor

ConvertedMimeType = Literal[
    "application/msword",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
]
CONVERTED_MIME_TYPES: tuple[ConvertedMimeType, ...] = (
    "application/msword",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
)


class ConversionExtractor(Extractor):
    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(downloaded.mime_type in CONVERTED_MIME_TYPES)

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        if not isinstance(downloaded, DownloadedFile):
            raise ExtractError.fail("conversion", "requires DownloadedFile")
        if not downloaded.mime_type:
            raise ExtractError.fail("conversion", "requires mime_type")

        mime_type: ConvertedMimeType
        if (mime_type_str := str(downloaded.mime_type)) in CONVERTED_MIME_TYPES:
            mime_type = mime_type_str
        else:
            raise ExtractError.fail(
                "conversion",
                f"requires mime_type of {' | '.join(CONVERTED_MIME_TYPES)}, got {downloaded.mime_type}",
            )

        try:
            # Translate the document into docx using LibreOffice.
            # The directory (and temporary docx within) are automatically deleted.
            # We do not mutate `downloaded` so the output uses the content_type
            # and filename of the original downloaded file.
            with tempfile.TemporaryDirectory() as temp_dir:
                match mime_type:
                    case "application/msword":
                        target_ext = ".docx"
                        target_format = "docx:MS Word 2007 XML"
                        target_mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                    case "application/vnd.openxmlformats-officedocument.presentationml.presentation":
                        target_ext = ".pdf"
                        target_format = "pdf:impress_pdf_Export"
                        target_mime_type = "application/pdf"

                # Tempfile alone produces "source_file is not a valid path", but if
                # we copy into the temporary directory, it works.
                input_path = Path(temp_dir) / Path(downloaded.tempfile_path).name
                output_path = input_path.with_suffix(target_ext)
                await asyncio.to_thread(
                    shutil.copyfile, downloaded.tempfile_path, input_path
                )

                out = await _run_shell(
                    command=[
                        "soffice",
                        "--headless",
                        "--convert-to",
                        f'"{target_format}"',
                        "--outdir",
                        str(temp_dir),
                        str(input_path),
                    ],
                )
                if out and "Error:" in out:
                    err = out.replace("\n", " ")
                    raise RuntimeError(f"libreoffice conversion failed: {err}")

                artificial_download = DownloadedFile(
                    url=downloaded.url,
                    tempfile_path=output_path,
                    response_headers={},
                    name=output_path.name,
                    filename=FileName.normalize(output_path.name),
                    mime_type=MimeType(target_mime_type),
                    charset=downloaded.charset,
                )
                match mime_type:
                    case "application/msword":
                        extracted = await extract_pandoc(artificial_download)
                    case "application/vnd.openxmlformats-officedocument.presentationml.presentation":
                        # For PPTX, enable pagination to separate slides
                        pptx_options = replace(
                            options,
                            doc=options.doc.model_copy(update={"paginate": True}),
                        )
                        extracted = await PdfExtractor().extract(
                            artificial_download, pptx_options, user_id
                        )

                return Extracted(
                    mode=extracted.mode,
                    name=extracted.name,
                    path=None,
                    mime_type=downloaded.mime_type,
                    blobs=extracted.blobs,
                    text=extracted.text,
                )
        except Exception as exc:
            raise ExtractError.fail("conversion", str(exc))  # noqa: B904
