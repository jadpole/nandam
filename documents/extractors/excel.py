import asyncio
from pathlib import Path
import pandas as pd

from io import StringIO
from typing import cast

from base.api.documents import FragmentMode

from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted, DownloadedFile
from documents.models.processing import ExtractOptions, Extractor


class ExcelExtractor(Extractor):
    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(
            downloaded.mime_type and downloaded.mime_type.mode() == "spreadsheet"
        )

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        try:
            if not isinstance(downloaded, DownloadedFile):
                raise ExtractError.fail("excel", "requires DownloadedFile")
            if not downloaded.mime_type:
                raise ExtractError.fail("excel", "cannot infer MIME type")

            mode: FragmentMode = "data" if options.original else "markdown"
            if downloaded.mime_type == "text/csv":
                mode, text = await asyncio.to_thread(
                    _extract_csv_sync,
                    downloaded.tempfile_path,
                    downloaded.charset,
                    mode,
                )
            else:
                mode, text = await asyncio.to_thread(
                    _extract_excel_sync,
                    downloaded.tempfile_path,
                    mode,
                )

            return Extracted(
                mode=mode,
                name=None,
                path=None,
                mime_type=downloaded.mime_type,
                blobs={},
                text=text,
            )
        except ExtractError:
            raise
        except Exception as exc:
            raise ExtractError.fail("excel", str(exc)) from exc


def _extract_csv_sync(
    file_path: Path,
    charset: str | None,
    mode: FragmentMode,
) -> tuple[FragmentMode, str]:
    """Extract CSV from file path without loading entire file into memory"""
    try:
        data_xls = pd.read_csv(file_path)
        output = StringIO()
        if mode == "markdown":
            data_xls.to_markdown(output, index=False, tablefmt="github")
        else:
            data_xls.to_csv(output, index=False)

        text = output.getvalue()
        return mode, text
    except Exception:
        # Fall back to reading raw content.
        with open(file_path, "rb") as f:
            text = f.read().decode(charset or "utf-8", errors="ignore")
            return "data", text


def _extract_excel_sync(
    file_path: Path,
    mode: FragmentMode,
) -> tuple[FragmentMode, str]:
    """Extract Excel from file path without loading entire file into memory"""
    extracted: dict[str, str] = {}
    data_xls = pd.ExcelFile(file_path)
    for sheet_name in data_xls.sheet_names:
        sheet = cast(
            "pd.DataFrame",
            data_xls.parse(
                sheet_name,
                dtype="object",
                keep_default_na=False,
            ),
        )

        output = StringIO()
        if mode == "markdown":
            sheet.to_markdown(output, index=False, tablefmt="github")
        else:
            sheet.to_csv(output, index=False)

        extracted[str(sheet_name)] = output.getvalue()

    if len(extracted) == 1:
        _, text = extracted.popitem()
    else:
        text = "\n\n".join(
            f"## {sheet_name}\n\n{content}" for sheet_name, content in extracted.items()
        )

    return mode, text
