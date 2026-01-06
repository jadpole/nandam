from dataclasses import dataclass

from base.api.documents import HtmlOptions, TranscriptOptions, DocOptions
from base.strings.data import MimeType
from base.strings.resource import WebUrl

from documents.models.pending import Downloaded, Extracted


@dataclass(kw_only=True)
class ExtractOptions:
    original: bool
    mime_type: MimeType | None
    doc: DocOptions
    html: HtmlOptions
    transcript: TranscriptOptions


@dataclass(kw_only=True)
class Downloader:
    """
    The Downloader bypasses the normal file process and, instead, immediately
    returns an `ExtractedFile`.  Typically used when accessing internal links
    with a specific format or when we fake the URL by invoking an API.
    """

    def match(self, url: WebUrl) -> bool:
        """
        Check whether the URL should be intercepted handled by this downloader,
        instead of going through the normal flow.
        """
        raise NotImplementedError("Subclasses must implement Downloader.match")

    async def download_url(
        self,
        url: WebUrl,
        options: ExtractOptions,
        headers: dict[str, str],
        authorization: str | None,
    ) -> Downloaded:
        raise NotImplementedError("Subclasses must implement Downloader.read")


class Extractor:
    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        """Check whether the file format should be handled by this parser."""
        raise NotImplementedError("Subclasses must implement Parser.match")

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        """Parse the downloaded file into a standard extract format."""
        raise NotImplementedError("Subclasses must implement Parser.read")
