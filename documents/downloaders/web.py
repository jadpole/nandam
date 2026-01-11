import aiohttp
import logging
import requests
import ssl
import tempfile

from dataclasses import dataclass
from pathlib import Path
from scrapfly import ScrapeConfig, ScrapflyClient
from ssl import SSLContext

from base.strings.data import MIME_TYPES_USELESS, MimeType
from base.strings.file import FileName
from base.strings.resource import WebUrl

from documents.config import DocumentsConfig
from documents.models.exceptions import DocumentsError, DownloadError
from documents.models.pending import Downloaded, DownloadedFile
from documents.models.processing import Downloader, ExtractOptions

logger = logging.getLogger(__name__)

MIB_AS_BYTES = 1_048_576
MAX_FILE_SIZE = 100 * MIB_AS_BYTES
"""
We download files of 100 MiB maximum and raise an error (without downloading) if
the Content-Length header is any larger.  In TEST, the server requests 512 MiB
of memory, so downloading multiple large files in parallel might crash it.
"""


@dataclass(kw_only=True)
class WebDownloader(Downloader):
    def match(self, url: WebUrl) -> bool:
        return True

    async def download_url(
        self,
        url: WebUrl,
        options: ExtractOptions,
        headers: dict[str, str],
        authorization: str | None,
    ) -> Downloaded:
        downloaded = await download_web_url(
            url=url,
            headers=headers,
            authorization=authorization,
            force_mime_type=options.mime_type,
        )
        info_log = (
            f"Downloaded '{downloaded.filename and downloaded.filename.ext()}' file: "
            f"charset={downloaded.charset}, "
            f"content_type={downloaded.mime_type}",
        )
        logger.info(info_log)
        return downloaded


async def download_web_url(
    url: WebUrl,
    headers: dict[str, str],
    authorization: str | None,
    force_mime_type: MimeType | None = None,
) -> Downloaded:
    downloaded_aiohttp: Downloaded | None = None
    download_scrapfly: Downloaded | None = None
    error: DownloadError | None = None

    try:
        downloaded_aiohttp = await _download_aiohttp(
            url, headers, authorization, force_mime_type
        )
    except DownloadError as exc:
        error = exc

    if (
        (not downloaded_aiohttp or downloaded_aiohttp.mime_type == "text/html")
        and not authorization
        and not headers
        and _should_use_scrapfly(url)
    ):
        try:
            download_scrapfly = await _download_scrapfly(url, force_mime_type)
        except DownloadError as exc:
            error = error or exc

    if download_scrapfly is not None:
        if downloaded_aiohttp is not None:
            downloaded_aiohttp.delete_tempfile()
        return download_scrapfly
    elif downloaded_aiohttp is not None:
        return downloaded_aiohttp
    elif error:
        raise error
    else:
        raise DocumentsError("unreachable: neither download succeeded")


def _should_use_scrapfly(url: WebUrl) -> bool:
    return (
        bool(DocumentsConfig.scrapfly.api_key)
        and url.domain not in DocumentsConfig.scrapfly.disabled_domains
        and not url.domain.endswith(tuple(DocumentsConfig.scrapfly.disabled_suffixes))
    )


def _parse_response(
    url: WebUrl,
    headers: dict[str, str],
    content_type: str,
    content_length: int | None,
    force_mime_type: MimeType | None,
) -> tuple[MimeType | None, FileName | None, str | None]:
    mime_type: MimeType | None = None
    if force_mime_type:
        mime_type = force_mime_type
    elif content_type not in MIME_TYPES_USELESS:
        mime_type = MimeType.decode(content_type)

    filename = FileName.from_http_headers(headers) or url.guess_filename(mime_type)
    if not mime_type and filename:
        mime_type = MimeType.guess(str(filename), True)

    if (
        content_length
        and (not mime_type or not mime_type.startswith(("audio/", "video/")))
        and content_length > MAX_FILE_SIZE
    ):
        raise DownloadError.bad_response("web", 413)

    # Store the response as a temporary file.
    # fmt: off
    tempfile_ext: str | None = (
        (mime_type and mime_type.guess_extension())
        or (filename and filename.ext())
    )
    return mime_type, filename, tempfile_ext


##
## Async IO Http
##


async def _download_aiohttp(  # noqa: C901
    url: WebUrl,
    headers: dict[str, str],
    authorization: str | None,
    force_mime_type: MimeType | None,
) -> Downloaded:
    # Include the authorization header.
    # NOTE: GitLab uses "Private-Token" as its header.
    headers = headers.copy()
    if authorization:
        if authorization.startswith("Private-Token "):
            headers["Private-Token"] = authorization.removeprefix("Private-Token ")
        else:
            headers["Authorization"] = authorization

    try:
        ssl_verify: bool | SSLContext
        if url.domain in DocumentsConfig.ssl.legacy:
            ssl_verify = SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_verify.load_default_certs()
            ssl_verify.options |= ssl.OP_LEGACY_SERVER_CONNECT
        else:
            ssl_verify = (
                DocumentsConfig.is_kubernetes()
                and url.domain not in DocumentsConfig.ssl.disabled
            )

        async with (
            aiohttp.ClientSession() as session,
            session.get(str(url), headers=headers, ssl=ssl_verify) as response,
        ):
            response.raise_for_status()
            return await _handle_download_aiohttp(url, response, force_mime_type)
    except DownloadError:
        raise
    except aiohttp.ClientResponseError as exc:
        if DocumentsConfig.verbose:
            logger.exception("Response error during Documents download")
        raise DownloadError.bad_response("web", exc.status)  # noqa: B904
    except aiohttp.ClientError as exc:
        if DocumentsConfig.verbose:
            logger.exception("Network error during Documents download")
        raise DownloadError.network(str(exc))  # noqa: B904
    except Exception as exc:
        if DocumentsConfig.verbose:
            logger.exception("Unexpected error during Documents download")
        raise DownloadError.unexpected(str(exc))  # noqa: B904


async def _handle_download_aiohttp(
    url: WebUrl,
    response: aiohttp.ClientResponse,
    force_mime_type: MimeType | None,
) -> Downloaded:
    headers = {k.lower(): v for k, v in response.headers.items()}

    mime_type, filename, tempfile_ext = _parse_response(
        url=url,
        headers=headers,
        content_type=response.content_type,
        content_length=response.content_length,
        force_mime_type=force_mime_type,
    )

    # Store the response as a temporary file.
    first_chunk: bytes | None = None
    with tempfile.NamedTemporaryFile(delete=False, suffix=tempfile_ext) as temp_file:
        tempfile_path = Path(temp_file.name)
        async for chunk in response.content.iter_chunked(1024):
            if not first_chunk:
                first_chunk = chunk
            temp_file.write(chunk)

    if not mime_type and first_chunk:
        mime_type = MimeType.guess_from_bytes(first_chunk)

    return DownloadedFile(
        url=url,
        response_headers=headers,
        name=None,
        mime_type=mime_type,
        filename=filename,
        charset=response.charset,
        tempfile_path=tempfile_path,
    )


##
## ScrapFly
##


async def _download_scrapfly(
    url: WebUrl,
    force_mime_type: MimeType | None,
) -> Downloaded:
    try:
        assert DocumentsConfig.scrapfly.api_key
        scrapfly = ScrapflyClient(key=DocumentsConfig.scrapfly.api_key)
        result = await scrapfly.async_scrape(
            ScrapeConfig(
                tags=set(),  # {"player_project_default"},  # Adjusted to match the regex pattern
                asp=True,
                render_js=True,
                url=str(url),
                country="us",
            ),
        )
        if result.status_code != 200:  # noqa: PLR2004
            raise DownloadError.bad_response("scrapfly", result.status_code)
        elif response := result.upstream_result_into_response():
            return await _handle_download_scrapfly(url, response, force_mime_type)
        else:
            raise DownloadError.unexpected("ScrapFly request failed")
    except DownloadError:
        raise
    except Exception as exc:
        if DocumentsConfig.verbose:
            logger.exception("Unexpected error during ScrapFly download")
        raise DownloadError.unexpected(str(exc))  # noqa: B904


async def _handle_download_scrapfly(
    url: WebUrl,
    response: requests.Response,
    force_mime_type: MimeType | None,
) -> Downloaded:
    # Content-Disposition header is often "mastodon.php", so it's useless.
    headers = {k.lower(): v for k, v in response.headers.items()}
    headers.pop("content-disposition", None)

    mime_type, filename, tempfile_ext = _parse_response(
        url=url,
        headers=headers,
        content_type=response.headers.get("content-type", "").split(";")[0],
        content_length=None,
        force_mime_type=force_mime_type,
    )

    with tempfile.NamedTemporaryFile(delete=False, suffix=tempfile_ext) as temp_file:
        tempfile_path = Path(temp_file.name)
        tempfile_bytes = response.content
        temp_file.write(tempfile_bytes)

    if not mime_type:
        mime_type = MimeType.guess_from_bytes(tempfile_bytes)

    return DownloadedFile(
        url=url,
        response_headers=headers,
        name=None,
        mime_type=mime_type,
        filename=filename,
        charset=response.encoding,
        tempfile_path=tempfile_path,
    )
