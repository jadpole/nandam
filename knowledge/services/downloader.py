import aiohttp
import logging

from dataclasses import dataclass
from typing import Any

from base.api.documents import (
    DocOptions,
    DocumentsBlobRequest,
    HtmlOptions,
    documents_blob,
    documents_download,
    DocumentsDownloadRequest,
    DocumentsReadResponse,
    TranscriptOptions,
)
from base.core.exceptions import ApiError, UnavailableError
from base.models.context import NdService
from base.strings.auth import RequestId, ServiceId, UserId
from base.strings.data import MimeType
from base.strings.resource import WebUrl

from knowledge.config import KnowledgeConfig
from knowledge.models.context import KnowledgeContext
from knowledge.models.exceptions import DownloadError

logger = logging.getLogger(__name__)

SVC_DOWNLOADER = ServiceId.decode("svc-downloader")


@dataclass(kw_only=True)
class SvcDownloader(NdService):
    service_id: ServiceId = SVC_DOWNLOADER

    @staticmethod
    def initialize(context: KnowledgeContext) -> "SvcDownloader":
        return SvcDownloaderApi.initialize(context)

    async def fetch_bytes(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> tuple[bytes, MimeType, dict[str, str]]:
        raise NotImplementedError("Subclasses must implement Downloader.fetch_bytes")

    async def fetch_head(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> dict[str, str]:
        raise NotImplementedError("Subclasses must implement Downloader.fetch_head")

    async def fetch_json(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> tuple[Any, dict[str, str]]:
        raise NotImplementedError("Subclasses must implement Downloader.fetch_json")

    async def documents_read_blob(
        self,
        name: str,
        mime_type: MimeType,
        blob: str,
        original: bool = False,
        doc: DocOptions | None = None,
        html: HtmlOptions | None = None,
        transcript: TranscriptOptions | None = None,
    ) -> DocumentsReadResponse:
        raise NotImplementedError("Subclasses must implement Downloader.upload_blob")

    async def documents_read_download(
        self,
        url: WebUrl,
        authorization: str | None,
        headers: dict[str, str] | None = None,
        put_url: str | None = None,
        original: bool = False,
        mime_type: MimeType | None = None,
        doc: DocOptions | None = None,
        html: HtmlOptions | None = None,
        transcript: TranscriptOptions | None = None,
    ) -> DocumentsReadResponse:
        raise NotImplementedError(
            "Subclasses must implement Downloader.download_web_url"
        )


##
## Stub
##


@dataclass(kw_only=True)
class SvcDownloaderStub(SvcDownloader):
    stub_responses_head: dict[WebUrl, dict[str, str] | DownloadError]
    stub_responses_json: dict[WebUrl, Any | DownloadError]
    stub_responses_blob: dict[str, DocumentsReadResponse | DownloadError]
    stub_responses_download: dict[WebUrl, DocumentsReadResponse | DownloadError]

    @staticmethod
    def initialize(  # pyright: ignore[reportIncompatibleMethodOverride]
        stub_download: dict[str, DocumentsReadResponse | DownloadError] | None = None,
    ) -> "SvcDownloaderStub":
        return SvcDownloaderStub(
            stub_responses_head={},
            stub_responses_json={},
            stub_responses_blob={},
            stub_responses_download=(
                {WebUrl.decode(url): resp for url, resp in stub_download.items()}
                if stub_download
                else {}
            ),
        )

    async def fetch_bytes(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> tuple[bytes, MimeType, dict[str, str]]:
        raise UnavailableError.new()

    async def fetch_head(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> dict[str, str]:
        if response := self.stub_responses_head.get(url):
            if isinstance(response, Exception):
                raise response
            else:
                return response
        else:
            raise UnavailableError.new()

    async def fetch_json(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> tuple[Any, dict[str, str]]:
        if response := self.stub_responses_json.get(url):
            if isinstance(response, Exception):
                raise response
            else:
                return response, {}
        else:
            raise UnavailableError.new()

    async def documents_read_blob(
        self,
        name: str,
        mime_type: MimeType,
        blob: str,
        original: bool = False,
        doc: DocOptions | None = None,
        html: HtmlOptions | None = None,
        transcript: TranscriptOptions | None = None,
    ) -> DocumentsReadResponse:
        if response := self.stub_responses_blob.get(name):
            if isinstance(response, Exception):
                raise response
            else:
                return response
        else:
            raise UnavailableError.new()

    async def documents_read_download(
        self,
        url: WebUrl,
        authorization: str | None,
        headers: dict[str, str] | None = None,
        put_url: str | None = None,
        original: bool = False,
        mime_type: MimeType | None = None,
        doc: DocOptions | None = None,
        html: HtmlOptions | None = None,
        transcript: TranscriptOptions | None = None,
    ) -> DocumentsReadResponse:
        if response := self.stub_responses_download.get(url):
            if isinstance(response, Exception):
                raise response
            else:
                return response
        else:
            raise UnavailableError.new()


##
## API
##


@dataclass(kw_only=True)
class SvcDownloaderApi(SvcDownloader):
    request_id: RequestId | None
    user_id: UserId | None

    @staticmethod
    def initialize(context: KnowledgeContext) -> "SvcDownloaderApi":
        return SvcDownloaderApi(
            request_id=context.request_id,
            user_id=context.user_id(),
        )

    async def fetch_bytes(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> tuple[bytes, MimeType, dict[str, str]]:
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(str(url), headers=headers or {}) as response,
            ):
                response.raise_for_status()
                resp_bytes = await response.read()
                resp_mime_type = MimeType.decode(response.content_type)
                resp_headers = {k.lower(): v for k, v in response.headers.items()}
                return resp_bytes, resp_mime_type, resp_headers
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("GET BYTES %s failed", str(url))
            raise UnavailableError.new()  # noqa: B904

    async def fetch_head(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> dict[str, str]:
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.head(str(url), headers=headers or {}) as response,
            ):
                response.raise_for_status()
                return {k.lower(): v for k, v in response.headers.items()}
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("HEAD %s failed", str(url))
            return {}

    async def fetch_json(
        self,
        url: WebUrl,
        headers: dict[str, str] | None = None,
    ) -> tuple[Any, dict[str, str]]:
        try:
            async with (
                aiohttp.ClientSession() as session,
                session.get(str(url), headers=headers or {}) as response,
            ):
                response.raise_for_status()
                resp_data = await response.json()
                resp_headers = {k.lower(): v for k, v in response.headers.items()}
                return resp_data, resp_headers
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("GET JSON %s failed", str(url))
            raise UnavailableError.new()  # noqa: B904

    async def documents_read_blob(
        self,
        name: str,
        mime_type: MimeType,
        blob: str,
        original: bool = False,
        doc: DocOptions | None = None,
        html: HtmlOptions | None = None,
        transcript: TranscriptOptions | None = None,
    ) -> DocumentsReadResponse:
        if KnowledgeConfig.verbose:
            logger.info("documents_read_blob: %s", name)

        # Download and parse the file from the web.
        request = DocumentsBlobRequest(
            name=name,
            mime_type=mime_type,
            blob=blob,
            original=original,
            doc=doc or DocOptions(),
            html=html or HtmlOptions(),
            transcript=transcript or TranscriptOptions(),
        )
        try:
            return await documents_blob(
                req=request,
                request_id=self.request_id,
                user_id=self.user_id,
            )
        except ApiError as exc:
            # NOTE: When the source returns a "Not Found" or access error, strip the
            # details from the error so the client cannot use Knowledge to gather a
            # list of "valid but inaccessible" URLs.
            if exc.code in [401, 403, 404]:
                if KnowledgeConfig.verbose:
                    raise UnavailableError.new() from exc
                else:
                    raise UnavailableError.new()  # noqa: B904
            else:
                # However, do bubble up other errors for debugging.
                raise DownloadError(str(exc), code=exc.code) from exc

    async def documents_read_download(
        self,
        url: WebUrl,
        authorization: str | None,
        headers: dict[str, str] | None = None,
        put_url: str | None = None,
        original: bool = False,
        mime_type: MimeType | None = None,
        doc: DocOptions | None = None,
        html: HtmlOptions | None = None,
        transcript: TranscriptOptions | None = None,
    ) -> DocumentsReadResponse:
        if KnowledgeConfig.verbose:
            logger.info("documents_read_download: %s", url)

        # Download and parse the file from the web.
        request = DocumentsDownloadRequest(
            url=url,
            headers=headers,
            put_url=put_url,
            original=original,
            mime_type=mime_type,
            doc=doc or DocOptions(),
            html=html or HtmlOptions(),
            transcript=transcript or TranscriptOptions(),
        )
        try:
            return await documents_download(
                req=request,
                authorization=authorization,
                request_id=self.request_id,
                user_id=self.user_id,
            )
        except ApiError as exc:
            # NOTE: When the source returns a "Not Found" or access error, strip the
            # details from the error so the client cannot use Knowledge to gather a
            # list of "valid but inaccessible" URLs.
            if exc.code in [401, 403, 404]:
                if KnowledgeConfig.verbose:
                    raise UnavailableError.new() from exc
                else:
                    raise UnavailableError.new()  # noqa: B904
            else:
                # However, do bubble up other errors for debugging.
                raise DownloadError(str(exc), code=exc.code) from exc
