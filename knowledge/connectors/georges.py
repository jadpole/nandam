import logging
import re

from dataclasses import dataclass
from datetime import datetime, UTC
from pydantic import BaseModel
from typing import Literal

from base.core.exceptions import BadRequestError, UnavailableError
from base.resources.aff_body import AffBody
from base.resources.aff_file import AffFile
from base.resources.metadata import AffordanceInfo
from base.strings.data import DataUri, MimeType
from base.strings.file import FileName, FilePath, REGEX_FILENAME, REGEX_FILEPATH
from base.strings.resource import Observable, Realm, ResourceUri, RootReference, WebUrl

from knowledge.config import KnowledgeConfig
from knowledge.models.storage_metadata import Locator, MetadataDelta, ResourceView
from knowledge.models.storage_observed import BundleFile
from knowledge.server.context import (
    Connector,
    KnowledgeContext,
    ObserveResult,
    ResolveResult,
)
from knowledge.services.downloader import SvcDownloader

logger = logging.getLogger(__name__)


##
## Config
##


class GeorgesConnectorConfig(BaseModel):
    kind: Literal["georges"] = "georges"
    realm: Realm
    domain: str

    def instantiate(self, context: "KnowledgeContext") -> "GeorgesConnector":
        return GeorgesConnector(
            context=context,
            realm=self.realm,
            domain=self.domain,
        )


##
## Locators
##


class DalleImageLocator(Locator, frozen=True):
    """
    Example: "https://oaidalleapiprodscus.blob.core.windows.net/private/org-ZqPqDHsb3141ej8sLSPCq2TG/user-fjRbyaktsSj41p1go1p30I7n/img-n8AFv3uCOXSqxudTpoybufGO.png"
    Becomes: "ndk://georges/dalle/img-n8AFv3uCOXSqxudTpoybufGO.png"
    """

    kind: Literal["dalle_image"] = "dalle_image"
    file_path: FilePath

    @staticmethod
    def from_web(realm: Realm, reference: WebUrl) -> "DalleImageLocator | None":
        if (
            reference.domain == "oaidalleapiprodscus.blob.core.windows.net"
            and re.fullmatch(rf"private/{REGEX_FILEPATH}", reference.path)
            and (file_path := FilePath.try_decode(reference.path))
        ):
            return DalleImageLocator(realm=realm, file_path=file_path)
        return None

    def resource_uri(self) -> ResourceUri:
        file_name = self.file_path.filename()
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("dalle"),
            path=[file_name],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://oaidalleapiprodscus.blob.core.windows.net/{self.file_path}"
        )

    def citation_url(self) -> WebUrl | None:
        return self.content_url()


class FalImageLocator(Locator, frozen=True):
    kind: Literal["fal_image"] = "fal_image"
    category: FileName
    filename: FileName

    @staticmethod
    def from_web(realm: Realm, reference: WebUrl) -> "FalImageLocator | None":
        """
        Example: "https://fal.media/files/elephant/OIUhnU24095TiMysqXcOd.png"
        Becomes: "ndk://georges/fal/elephant/OIUhnU24095TiMysqXcOd.png"
        """
        if (
            reference.domain == "fal.media"
            and (
                match := re.fullmatch(
                    rf"files/({REGEX_FILENAME})/({REGEX_FILENAME})", reference.path
                )
            )
            and (category := FileName.try_decode(match.group(1)))
            and (file_name := FileName.try_decode(match.group(2)))
        ):
            return FalImageLocator(
                realm=realm,
                category=category,
                filename=file_name,
            )
        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("fal"),
            path=[self.category, self.filename],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(f"https://fal.media/files/{self.category}/{self.filename}")

    def citation_url(self) -> WebUrl:
        return self.content_url()


class OpenAIFileLocator(Locator, frozen=True):
    kind: Literal["openai_file"] = "openai_file"
    domain: str
    file_id: FileName

    @staticmethod
    def from_web(realm: Realm, domain: str, url: WebUrl) -> "OpenAIFileLocator | None":
        """
        Example: https://example-gateway.com/v1/files/file-abc123/content
        Becomes: "ndk://georges/files/file-abc123"
        """
        if (
            url.domain == domain
            and (
                match := re.fullmatch(
                    rf"v1/files/({REGEX_FILENAME})(?:/content)?", url.path
                )
            )
            and (file_id := FileName.try_decode(match.group(1)))
        ):
            return OpenAIFileLocator(realm=realm, domain=url.domain, file_id=file_id)
        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("files"),
            path=[self.file_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(f"https://{self.domain}/v1/files/{self.file_id}/content")

    def citation_url(self) -> WebUrl | None:
        return None  # URL is protected.


AnyGeorgesLocator = DalleImageLocator | FalImageLocator | OpenAIFileLocator


##
## Connector
##


@dataclass(kw_only=True)
class GeorgesConnector(Connector):
    _handle: "GeorgesHandle | None" = None
    domain: str

    async def _acquire_handle(self) -> "GeorgesHandle":
        if self._handle is None:
            self._handle = GeorgesHandle(
                context=self.context,
                realm=self.realm,
                domain_api=self.domain,
                authorization=self._get_authorization(),
            )
        return self._handle

    async def locator(self, reference: RootReference) -> Locator | None:  # noqa: PLR0911
        if isinstance(reference, WebUrl):
            return (
                DalleImageLocator.from_web(self.realm, reference)
                or FalImageLocator.from_web(self.realm, reference)
                or OpenAIFileLocator.from_web(self.realm, self.domain, reference)
            )

        elif isinstance(reference, ResourceUri):
            if reference.realm != self.realm:
                return None

            if reference.subrealm == "dalle":
                return None  # Cannot infer from the resource URI.

            if reference.subrealm == "fal" and len(reference.path) == 2:  # noqa: PLR2004
                return FalImageLocator(
                    realm=self.realm,
                    category=reference.path[0],
                    filename=reference.path[1],
                )

            if reference.subrealm == "files" and len(reference.path) == 1:
                return OpenAIFileLocator(
                    realm=self.realm,
                    domain=self.domain,
                    file_id=reference.path[0],
                )

            return None
        else:
            return None

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        assert isinstance(locator, AnyGeorgesLocator)
        handle = await self._acquire_handle()

        # Fetch metadata for OpenAI files
        metadata: MetadataDelta
        match locator:
            case OpenAIFileLocator():
                metadata, _ = await handle.fetch_openai_file_metadata(locator.file_id)
            case DalleImageLocator():
                metadata = MetadataDelta(
                    name=str(locator.file_path.filename()),
                    mime_type=MimeType.guess_or_default(locator.file_path, "image/png"),
                )
            case FalImageLocator():
                metadata = MetadataDelta(
                    name=str(locator.filename),
                    mime_type=MimeType.guess_or_default(locator.filename, "image/png"),
                )
            case _:
                raise UnavailableError.new()

        if not cached:
            metadata = metadata.with_update(
                MetadataDelta(
                    affordances=[
                        AffordanceInfo(suffix=AffBody.new()),
                        AffordanceInfo(
                            suffix=AffFile.new(), mime_type=metadata.mime_type
                        ),
                    ]
                )
            )

        return ResolveResult(
            metadata=metadata,
            expired=[],
            should_cache=True,
        )

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyGeorgesLocator)
        handle = await self._acquire_handle()

        match (locator, observable):
            case (
                (DalleImageLocator() | FalImageLocator() | OpenAIFileLocator()),
                AffBody(),
            ):
                return await _read_georges_document(handle, locator)
            case (
                (DalleImageLocator() | FalImageLocator() | OpenAIFileLocator()),
                AffFile(),
            ):
                return await _read_georges_rawfile(handle, locator)
            case _:
                raise BadRequestError.observable(observable.as_suffix())

    def _get_authorization(self) -> str:
        if private_api_key := self.context.creds.get(str(self.realm)):
            return f"Bearer {private_api_key}"
        elif default_api_key := KnowledgeConfig.llm.litellm_api_key:
            return f"Bearer {default_api_key}"
        else:
            raise UnavailableError.new()


##
## Read - Document
##


async def _read_georges_document(
    handle: "GeorgesHandle",
    locator: AnyGeorgesLocator,
) -> ObserveResult:
    downloader = handle.context.service(SvcDownloader)

    response = await downloader.documents_read_download(
        url=locator.content_url(),
        authorization=(
            handle.authorization if isinstance(locator, OpenAIFileLocator) else None
        ),
    )

    return ObserveResult(
        bundle=response.as_fragment(),
        should_cache=True,
        option_fields=True,
        option_relations_link=False,
    )


async def _read_georges_rawfile(
    handle: "GeorgesHandle",
    locator: AnyGeorgesLocator,
) -> ObserveResult:
    downloader = handle.context.service(SvcDownloader)

    # NOTE: We do not set `expiry` in `BundleFile`, since we're returning the
    # file contents as base64, rather than a signed URL, and we may wish to read
    # these bytes after the file expired in OpenAI.
    # expires_at: datetime | None = None
    # if isinstance(locator, OpenAIFileLocator):
    #     _, expires_at = await handle.fetch_openai_file_metadata(locator.file_id)

    try:
        headers: dict[str, str] = {}
        if isinstance(locator, OpenAIFileLocator):
            headers["Authorization"] = handle.authorization

        data_bytes, data_mime_type, _ = await downloader.fetch_bytes(
            url=locator.content_url(),
            headers=headers,
        )
        return ObserveResult(
            bundle=BundleFile(
                uri=locator.resource_uri().child_affordance(AffFile.new()),
                description=None,
                mime_type=data_mime_type,
                download_url=DataUri.new(data_mime_type, data_bytes),
                expiry=None,
            ),
            should_cache=True,
        )
    except Exception:
        if KnowledgeConfig.verbose:
            logger.exception(
                "Failed to download Georges rawfile: %s", locator.content_url()
            )
        raise UnavailableError.new()  # noqa: B904


##
## Handle
##


@dataclass(kw_only=True)
class GeorgesHandle:
    context: KnowledgeContext
    realm: Realm
    domain_api: str
    authorization: str

    async def fetch_openai_file_metadata(
        self,
        file_id: FileName,
    ) -> tuple[MetadataDelta, datetime | None]:
        """Fetch metadata for an OpenAI file."""
        downloader = self.context.service(SvcDownloader)

        try:
            data, _ = await downloader.fetch_json(
                url=WebUrl.decode(f"https://{self.domain_api}/v1/files/{file_id}"),
                headers={
                    "Authorization": self.authorization,
                    "Accept": "application/json",
                },
            )
            metadata = MetadataDelta(
                name=data.get("filename") or str(file_id),
                mime_type=(
                    MimeType.guess(filename)
                    if (filename := data.get("filename"))
                    else None
                ),
                created_at=(
                    datetime.fromtimestamp(created_at, UTC)
                    if (created_at := data.get("created_at"))
                    else None
                ),
            )
            expires_at = (
                datetime.fromtimestamp(expires_at, UTC)
                if (expires_at := data.get("expires_at"))
                else None
            )
            return metadata, expires_at
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("Failed to get OpenAI file metadata: %s", file_id)
            raise UnavailableError.new()  # noqa: B904
