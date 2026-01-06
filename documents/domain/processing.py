from base.api.documents import DocumentsReadResponse
from base.strings.data import MimeType
from base.strings.resource import WebUrl

from documents.config import DocumentsConfig
from documents.downloaders.confluence import ConfluenceDownloader
from documents.downloaders.tableau import TableauDownloader
from documents.downloaders.web import WebDownloader
from documents.extractors.archive import ArchiveExtractor
from documents.extractors.conversion import ConversionExtractor
from documents.extractors.excel import ExcelExtractor
from documents.extractors.html_page import HtmlPageExtractor
from documents.extractors.image import ImageExtractor
from documents.extractors.pandoc import PandocExtractor
from documents.extractors.pdf import PdfExtractor
from documents.extractors.plain_text import PlainTextExtractor
from documents.extractors.transcript import TranscriptExtractor
from documents.extractors.unstructured import UnstructuredExtractor
from documents.models.exceptions import DocumentsError
from documents.models.pending import Downloaded, Extracted
from documents.models.processing import Downloader, ExtractOptions, Extractor


async def run_download_and_extract(
    url: WebUrl,
    options: ExtractOptions,
    headers: dict[str, str],
    authorization: str | None,
    user_id: str | None,
) -> DocumentsReadResponse:
    downloaded = await run_download(url, options, headers, authorization)
    extracted = await run_extract(downloaded, options, user_id)
    return convert_document_response(downloaded, extracted)


async def run_download(
    url: WebUrl,
    options: ExtractOptions,
    headers: dict[str, str],
    authorization: str | None,
) -> Downloaded:
    downloaders: list[Downloader] = []
    downloaders.extend(
        ConfluenceDownloader(domain=domain)
        for domain in DocumentsConfig.domains.confluence
    )
    downloaders.extend(
        TableauDownloader(domain=domain) for domain in DocumentsConfig.domains.tableau
    )
    downloaders.append(WebDownloader())

    for downloader in downloaders:
        if downloader.match(url):
            return await downloader.download_url(url, options, headers, authorization)

    raise DocumentsError("Internal Server Error: no downloader found for URL")


async def run_extract(
    downloaded: Downloaded,
    options: ExtractOptions,
    user_id: str | None,
) -> Extracted:
    extractors: list[Extractor] = [
        ArchiveExtractor(),
        ConversionExtractor(),
        ExcelExtractor(),
        HtmlPageExtractor(),
        ImageExtractor(),
        PandocExtractor(),
        PdfExtractor(),
        PlainTextExtractor(),
        TranscriptExtractor(),
        UnstructuredExtractor(),
    ]

    for extractor in extractors:
        if extractor.match(downloaded, options):
            return await extractor.extract(downloaded, options, user_id)

    raise DocumentsError("Internal Server Error: no extractor found for file")


def convert_document_response(
    downloaded: Downloaded,
    extracted: Extracted,
) -> DocumentsReadResponse:
    # fmt: off
    return DocumentsReadResponse(
        name=(
            extracted.name
            or downloaded.name
            or extracted.path
            or downloaded.filename
            or "unknown"
        ),
        mime_type=(
            extracted.mime_type
            or downloaded.mime_type
            or MimeType.decode("text/plain")
        ),
        headers=downloaded.response_headers,
        mode=extracted.mode,
        text=extracted.text.replace("\r\n", "\n"),
        blobs=extracted.blobs,
    )
