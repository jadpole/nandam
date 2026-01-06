from dataclasses import dataclass

from base.strings.resource import WebUrl

from documents.downloaders.web import download_web_url
from documents.models.exceptions import DownloadError
from documents.models.pending import Downloaded
from documents.models.processing import Downloader, ExtractOptions


@dataclass(kw_only=True)
class ConfluenceDownloader(Downloader):
    domain: str

    def match(self, url: WebUrl) -> bool:
        return url.domain == self.domain and not url.path.startswith("rest/")

    async def download_url(
        self,
        url: WebUrl,
        options: ExtractOptions,
        headers: dict[str, str],
        authorization: str | None,
    ) -> Downloaded:
        if options.original:
            raise DownloadError(
                "Bad Request: cannot read Confluence in original format",
                code=400,
            )

        if not authorization or not authorization.startswith("Bearer "):
            raise DownloadError.unauthorized("Bearer")

        downloaded = await download_web_url(
            url, headers, authorization, options.mime_type
        )

        if not downloaded.mime_type:
            raise DownloadError.bad_response("confluence", 404, "no content-type")

        if (
            downloaded.mime_type == "text/html"
            and "<title>Log In -" in downloaded.read_text()
        ):
            raise DownloadError.unauthorized("confluence")

        return downloaded
