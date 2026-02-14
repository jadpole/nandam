import asyncio
import contextlib
import dateutil.parser
import dateutil.relativedelta
import logging
import re
import weakref

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pydantic import BaseModel
from typing import Any, Literal
from urllib.parse import quote, unquote, unquote_plus, urlencode

from base.api.documents import Fragment
from base.core.exceptions import UnavailableError
from base.resources.aff_body import AffBody
from base.resources.aff_collection import AffCollection
from base.resources.aff_file import AffFile
from base.resources.metadata import AffordanceInfo
from base.resources.relation import Relation, RelationParent
from base.strings.auth import UserId
from base.strings.data import Base64Safe, DataUri, MimeType
from base.strings.file import FileName
from base.strings.microsoft import MsDriveItemId
from base.strings.resource import (
    ExternalUri,
    Observable,
    Realm,
    ResourceUri,
    RootReference,
    WebUrl,
)
from base.utils.markdown import markdown_from_msteams

from knowledge.config import KnowledgeConfig
from knowledge.domain.resolve import try_resolve_locator
from knowledge.models.storage_metadata import Locator, MetadataDelta, ResourceView
from knowledge.models.storage_observed import BundleCollection, BundleFile
from knowledge.models.utils import shorten_description
from knowledge.server.context import (
    Connector,
    KnowledgeContext,
    ObserveResult,
    ResolveResult,
)
from knowledge.services.downloader import SvcDownloader

logger = logging.getLogger(__name__)

EXPIRY_DOWNLOAD_URL = timedelta(minutes=5)
EMAIL_PREVIEW_MAX_LENGTH = 500

ItemKind = Literal["file", "folder"]


##
## Config
##


class MicrosoftMyConnectorConfig(BaseModel, frozen=True):
    kind: Literal["microsoft-my"] = "microsoft-my"
    realm: Realm
    domain: str
    tenant_id: str

    def instantiate(self, context: KnowledgeContext) -> MicrosoftMyConnector:
        return MicrosoftMyConnector(
            context=weakref.proxy(context),
            realm=self.realm,
            domain=self.domain,
            tenant_id=self.tenant_id,
        )


##
## Locator - OneDrive
##


class MsOneDriveFileLocator(Locator, frozen=True):
    kind: Literal["ms_onedrive_file"] = "ms_onedrive_file"
    domain: str
    """
    Expected: "company-my.sharepoint.com"
    """
    user_email: str
    """
    Expected: "user.name@company.com"
    """
    user_id: UserId
    item_id: MsDriveItemId
    item_kind: ItemKind
    item_path: str

    @staticmethod
    async def from_web(
        handle: MsHandle,
        url: WebUrl,
    ) -> MsOneDriveFileLocator | None:
        if (
            url.domain == handle.domain
            and (
                match := re.fullmatch(r"personal/([a-z0-9_]+)/Documents/(.+)", url.path)
            )
            and (path_email := match.group(1))
            and (item_path := unquote_plus(match.group(2)))
            and (user_info := await handle.fetch_user_info(path_email))
            and (user_id := UserId.try_decode(f"user-{user_info.get('id')}"))
            and (
                item_info := await handle.fetch_onedrive_info_by_path(
                    user_id, item_path
                )
            )
            and (item_id := MsDriveItemId.try_decode(item_info.get("id")))
            and (item_kind := _infer_item_kind(item_info))
        ):
            return MsOneDriveFileLocator(
                realm=handle.realm,
                domain=handle.domain,
                user_email=user_info["userPrincipalName"],
                user_id=UserId.decode(f"user-{user_info['id']}"),
                item_id=item_id,
                item_kind=item_kind,
                item_path=item_path,
            )

        return None

    @staticmethod
    async def from_uri(
        handle: MsHandle,
        uri: ResourceUri,
    ) -> MsOneDriveFileLocator | None:
        if (
            uri.realm == handle.realm
            and uri.subrealm.startswith("onedrive-")
            and (user_id := uri.subrealm.removeprefix("onedrive-"))
            and (user_info := await handle.fetch_user_info(user_id))
            and (user_id := UserId.try_decode(f"user-{user_info.get('id')}"))
            and len(uri.path) == 1
            and (item_id := MsDriveItemId.from_filename(uri.path[0]))
            and (item_info := await handle.fetch_onedrive_info_by_id(user_id, item_id))
            and (item_id := MsDriveItemId.try_decode(item_info.get("id")))
            and (item_kind := _infer_item_kind(item_info))
        ):
            return MsOneDriveFileLocator(
                realm=handle.realm,
                domain=handle.domain,
                user_email=user_info["userPrincipalName"],
                user_id=user_id,
                item_id=item_id,
                item_kind=item_kind,
                item_path=_infer_item_path(item_info),
            )

        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode(f"onedrive-{self.user_id.uuid()}"),
            path=[self.item_id.as_filename()],
        )

    def content_url(self) -> WebUrl:
        path_email = self.user_email.replace(".", "_").replace("@", "_")
        return WebUrl.decode(
            f"https://{self.domain}/personal/{path_email}/Documents/{self.item_path}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


##
## Locators - Outlook
##


class MsOutlookAttachmentLocator(Locator, frozen=True):
    kind: Literal["ms_outlook_attachment"] = "ms_outlook_attachment"
    message_id: Base64Safe
    attachment_id: Base64Safe

    @staticmethod
    async def from_web(
        handle: MsHandle,  # noqa: ARG004
        url: WebUrl,  # noqa: ARG004
    ) -> MsOutlookAttachmentLocator | None:
        return None

    @staticmethod
    async def from_uri(
        handle: MsHandle,
        uri: ResourceUri,
    ) -> MsOutlookAttachmentLocator | None:
        if (
            uri.realm == handle.realm
            and uri.subrealm == "outlook-email"
            and len(uri.path) == 3  # noqa: PLR2004
            and uri.path[1] == "attachments"
        ):
            return MsOutlookAttachmentLocator(
                realm=handle.realm,
                message_id=Base64Safe.from_filename(uri.path[0]),
                attachment_id=Base64Safe.from_filename(uri.path[2]),
            )
        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("outlook-email"),
            path=[
                self.message_id.as_filename(),
                FileName.decode("attachments"),
                self.attachment_id.as_filename(),
            ],
        )

    def content_url(self) -> WebUrl:
        # TODO: Find the corresponding deeplink.
        quoted_id = quote(self.message_id, safe="")
        quoted_attachment_id = quote(self.attachment_id, safe="")
        return WebUrl.decode(
            f"https://outlook.office365.com/owa/?ItemID={quoted_id}&AttachmentID={quoted_attachment_id}"
        )

    def citation_url(self) -> WebUrl | None:
        return None  # TODO: Find the corresponding deeplink.


class MsOutlookEmailLocator(Locator, frozen=True):
    kind: Literal["ms_outlook_email"] = "ms_outlook_email"
    message_id: Base64Safe

    @staticmethod
    async def from_web(
        handle: MsHandle,
        url: WebUrl,
    ) -> MsOutlookEmailLocator | None:
        if (
            url.domain
            in (
                "outlook.office365.com",
                "outlook.office.com",
                "outlook.live.com",
            )
            and (item_str := url.get_query("ItemID") or url.get_query("itemid"))
            and (item_id := Base64Safe.try_decode(item_str))
            and url.get_query("path") != "/calendar/item"
        ):
            return MsOutlookEmailLocator(realm=handle.realm, message_id=item_id)

        if (
            url.domain == "outlook.office.com"
            and (match := re.fullmatch(r"mail/0/inbox/id/([^/]+)", url.path))
            and (item_id := Base64Safe.try_decode(unquote(match.group(1))))
        ):
            return MsOutlookEmailLocator(realm=handle.realm, message_id=item_id)

        return None

    @staticmethod
    async def from_uri(
        handle: MsHandle,
        uri: ResourceUri,
    ) -> MsOutlookEmailLocator | None:
        if (
            uri.realm == handle.realm
            and uri.subrealm == "outlook-email"
            and len(uri.path) == 1
        ):
            return MsOutlookEmailLocator(
                realm=handle.realm,
                message_id=Base64Safe.from_filename(uri.path[0]),
            )
        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("outlook-email"),
            path=[self.message_id.as_filename()],
        )

    def content_url(self) -> WebUrl:
        quoted_id = quote(self.message_id, safe="")
        return WebUrl.decode(
            f"https://outlook.office365.com/owa/?ItemID={quoted_id}&viewmodel=ReadMessageItem"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class MsOutlookEventLocator(Locator, frozen=True):
    kind: Literal["ms_outlook_event"] = "ms_outlook_event"
    event_id: Base64Safe

    @staticmethod
    async def from_web(
        handle: MsHandle,
        url: WebUrl,
    ) -> MsOutlookEventLocator | None:
        if (
            url.domain
            in (
                "outlook.office365.com",
                "outlook.office.com",
                "outlook.live.com",
            )
            and (event_str := url.get_query("ItemID") or url.get_query("itemid"))
            and (event_id := Base64Safe.try_decode(event_str))
            and url.get_query("path") == "/calendar/item"
        ):
            return MsOutlookEventLocator(realm=handle.realm, event_id=event_id)
        return None

    @staticmethod
    async def from_uri(
        handle: MsHandle,
        uri: ResourceUri,
    ) -> MsOutlookEventLocator | None:
        if (
            uri.realm == handle.realm
            and uri.subrealm == "outlook-event"
            and len(uri.path) == 1
        ):
            return MsOutlookEventLocator(
                realm=handle.realm,
                event_id=Base64Safe.from_filename(uri.path[0]),
            )
        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("outlook-event"),
            path=[self.event_id.as_filename()],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://outlook.office365.com/owa/?itemid={quote(self.event_id)}&exvsurl=1&path=/calendar/item"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


AnyMsLocator = (
    MsOneDriveFileLocator
    | MsOutlookAttachmentLocator
    | MsOutlookEmailLocator
    | MsOutlookEventLocator
)


##
## Connector
##


@dataclass(kw_only=True)
class MicrosoftMyConnector(Connector):
    domain: str
    tenant_id: str
    _handle: MsHandle | None = None

    async def _acquire_handle(self) -> MsHandle:
        if self._handle is None:
            self._handle = MsHandle(
                context=self.context,
                realm=self.realm,
                domain=self.domain,
                authorization=await self._get_authorization(),
                _cache_user_info={},
                _cache_onedrive_children_by_id={},
                _cache_onedrive_info_by_id={},
                _cache_onedrive_info_key_by_path={},
                _cache_outlook_attachment_by_id={},
            )
        return self._handle

    async def locator(self, reference: RootReference) -> Locator | None:
        if isinstance(reference, WebUrl):
            if reference.domain not in (
                "graph.microsoft.com",
                "outlook.live.com",
                "outlook.office.com",
                "outlook.office365.com",
                "teams.microsoft.com",
                self.domain,
            ):
                return None

            handle = await self._acquire_handle()
            locator = (
                await MsOneDriveFileLocator.from_web(handle, reference)
                or await MsOutlookAttachmentLocator.from_web(handle, reference)
                or await MsOutlookEmailLocator.from_web(handle, reference)
                or await MsOutlookEventLocator.from_web(handle, reference)
            )
            if not locator:
                raise UnavailableError.new()

            return locator
        elif isinstance(reference, ExternalUri):
            return None
        else:
            if reference.realm != self.realm:
                return None

            handle = await self._acquire_handle()
            return (
                await MsOneDriveFileLocator.from_uri(handle, reference)
                or await MsOutlookAttachmentLocator.from_uri(handle, reference)
                or await MsOutlookEmailLocator.from_uri(handle, reference)
                or await MsOutlookEventLocator.from_uri(handle, reference)
            )

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        """
        TODO: Teams Group ID <-> SharePoint Site ID mapping
        TODO: Resolve TeamsMessageLocator as internal sites.
        """
        assert isinstance(locator, AnyMsLocator)
        handle = await self._acquire_handle()

        match locator:
            case MsOneDriveFileLocator():
                return await _resolve_onedrive_file(handle, locator, cached)
            case MsOutlookAttachmentLocator():
                return await _resolve_outlook_attachment(handle, locator, cached)
            case MsOutlookEmailLocator():
                return await _resolve_outlook_email(handle, locator, cached)
            case MsOutlookEventLocator():
                return await _resolve_outlook_event(handle, locator, cached)
            case _:
                raise UnavailableError.new()

    async def observe(  # noqa: PLR0911
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyMsLocator)
        handle = await self._acquire_handle()

        match (locator, observable):
            case (MsOneDriveFileLocator(), AffBody()):
                return await _read_onedrive_file_body(handle, locator)
            case (MsOneDriveFileLocator(), AffCollection()):
                return await _read_onedrive_file_collection(handle, locator)
            case (MsOneDriveFileLocator(), AffFile(path=[])):
                return await _read_onedrive_file_rawfile(handle, locator)
            case (MsOutlookAttachmentLocator(), AffBody()):
                return await _read_outlook_attachment_body(handle, locator)
            case (MsOutlookAttachmentLocator(), AffFile(path=[])):
                return await _read_outlook_attachment_file(handle, locator)
            case (MsOutlookEmailLocator(), AffBody()):
                return await _read_outlook_email_body(handle, locator)
            case (MsOutlookEventLocator(), AffBody()):
                return await _read_outlook_event_body(handle, locator)
            case _:
                raise UnavailableError.new()

    async def _get_authorization(self) -> str:
        """
        Get Microsoft authorization header.
        Works for any Microsoft product supported by Graph API.
        """
        if private_token := self.context.creds.get(str(self.realm)):
            return private_token
        if debug_token := KnowledgeConfig.get("DEBUG_MICROSOFT_ACCESS_TOKEN"):
            return (
                debug_token
                if debug_token.startswith("Bearer ")
                else f"Bearer {debug_token}"
            )
        raise UnavailableError.new()


##
## OneDrive - File
##


async def _resolve_onedrive_file(
    handle: MsHandle,
    locator: MsOneDriveFileLocator,
    cached: ResourceView | None,
) -> ResolveResult:
    """
    NOTE: If the request access token is unable to read the file, then this will
    raise an `UnavailableError`, checking that the user has access to the cached
    metadata and contents.
    """
    file_info = await handle.fetch_onedrive_info_by_id(locator.user_id, locator.item_id)
    if not file_info:
        raise UnavailableError.new()

    mime_type: MimeType | None = None
    if locator.item_kind == "file":
        mime_type = MimeType.guess_from_info(
            filename=file_info.get("name"),
            content_type=file_info.get("file", {}).get("mimeType"),
        )

    metadata = MetadataDelta(
        name=file_info.get("name", "Untitled"),
        mime_type=mime_type,
        created_at=(
            dateutil.parser.parse(created_dt)
            if (created_dt := file_info.get("createdDateTime"))
            else None
        ),
        updated_at=(
            dateutil.parser.parse(updated_dt)
            if (updated_dt := file_info.get("lastModifiedDateTime"))
            else None
        ),
        revision_data=file_info.get("cTag"),
        revision_meta=file_info.get("eTag"),
    )
    expired: list[Observable] = []

    if not cached:
        if locator.item_kind == "file":
            metadata = metadata.with_update(
                MetadataDelta(
                    affordances=[
                        AffordanceInfo(suffix=AffBody.new()),
                        AffordanceInfo(suffix=AffFile.new(), mime_type=mime_type),
                    ]
                )
            )
        else:
            assert locator.item_kind == "folder"
            metadata = metadata.with_update(
                MetadataDelta(
                    affordances=[AffordanceInfo(suffix=AffCollection.new())],
                )
            )
    elif (
        cached.metadata.revision_data
        and metadata.revision_data
        and cached.metadata.revision_data != metadata.revision_data
        and locator.item_kind == "file"
    ):
        expired.append(AffBody.new())

    return ResolveResult(
        metadata=metadata,
        expired=expired,
        should_cache=locator.item_kind == "file",
    )


async def _read_onedrive_file_body(
    handle: MsHandle,
    locator: MsOneDriveFileLocator,
) -> ObserveResult:
    downloader = handle.context.service(SvcDownloader)

    file_info = await handle.fetch_onedrive_info_by_id(locator.user_id, locator.item_id)
    if not file_info:
        raise UnavailableError.new()

    response = await downloader.documents_read_download(
        url=WebUrl.decode(file_info["@microsoft.graph.downloadUrl"]),
        authorization=handle.authorization,
        headers={},
        original=False,
    )

    return ObserveResult(
        bundle=response.as_fragment(),
        should_cache=True,
        option_labels=True,
        option_relations_link=True,
    )


async def _read_onedrive_file_collection(
    handle: MsHandle,
    locator: MsOneDriveFileLocator,
) -> ObserveResult:
    if locator.item_kind != "folder":
        raise UnavailableError.new()

    children = await handle.fetch_onedrive_children_by_id(
        locator.user_id, locator.item_id
    )
    if not children:
        raise UnavailableError.new()

    results: list[ResourceUri] = []
    for child in children:
        item_kind = _infer_item_kind(child)
        if not item_kind:
            continue

        child_locator = MsOneDriveFileLocator(
            realm=locator.realm,
            domain=locator.domain,
            user_email=locator.user_email,
            user_id=locator.user_id,
            item_id=MsDriveItemId.decode(child["id"]),
            item_kind=item_kind,
            item_path=_infer_item_path(child),
        )
        if await try_resolve_locator(handle.context, child_locator):
            results.append(child_locator.resource_uri())

    if not results:
        raise UnavailableError.new()

    return ObserveResult(
        bundle=BundleCollection(
            uri=locator.resource_uri().child_affordance(AffCollection.new()),
            results=results,
        ),
        should_cache=False,
        option_labels=False,
        option_relations_parent=True,
    )


async def _read_onedrive_file_rawfile(
    handle: MsHandle,
    locator: MsOneDriveFileLocator,
) -> ObserveResult:
    if locator.item_kind != "file":
        raise UnavailableError.new()

    file_info = await handle.fetch_onedrive_info_by_id(locator.user_id, locator.item_id)
    if not file_info:
        raise UnavailableError.new()

    mime_type = MimeType.guess_from_info(
        filename=file_info.get("name"),
        content_type=file_info.get("file", {}).get("mimeType"),
    )

    return ObserveResult(
        bundle=BundleFile(
            uri=locator.resource_uri().child_affordance(AffFile.new()),
            description=None,
            mime_type=mime_type,
            download_url=WebUrl.decode(file_info["@microsoft.graph.downloadUrl"]),
            expiry=datetime.now(UTC) + EXPIRY_DOWNLOAD_URL,
        ),
        should_cache=False,
    )


##
## Read - Outlook - Attachment
##


async def _resolve_outlook_attachment(
    handle: MsHandle,
    locator: MsOutlookAttachmentLocator,
    cached: ResourceView | None,
) -> ResolveResult:
    """
    NOTE: If the request access token is unable to read the file, then this will
    raise an `UnavailableError`, checking that the user has access to the cached
    metadata and contents.
    """
    attachment_data = await handle.fetch_outlook_attachment(
        locator.message_id, locator.attachment_id
    )
    if (
        not attachment_data
        or attachment_data.get("@odata.type") != "#microsoft.graph.fileAttachment"
        or not (mime_type := MimeType.try_decode(attachment_data.get("contentType")))
        or not attachment_data.get("contentBytes")
    ):
        raise UnavailableError.new()

    metadata = MetadataDelta(
        name=attachment_data.get("name", "Untitled Attachment"),
        mime_type=mime_type,
        updated_at=(
            dateutil.parser.parse(updated_at)
            if (updated_at := attachment_data.get("lastModifiedDateTime"))
            else None
        ),
    )

    if not cached:
        metadata = metadata.with_update(
            MetadataDelta(
                affordances=[
                    AffordanceInfo(suffix=AffBody.new()),
                    AffordanceInfo(suffix=AffFile.new(), mime_type=mime_type),
                ],
            )
        )

    return ResolveResult(metadata=metadata, expired=[], should_cache=False)


async def _read_outlook_attachment_body(
    handle: MsHandle,
    locator: MsOutlookAttachmentLocator,
) -> ObserveResult:
    downloader = handle.context.service(SvcDownloader)

    attachment_data = await handle.fetch_outlook_attachment(
        locator.message_id, locator.attachment_id
    )
    if (
        not attachment_data
        or attachment_data.get("@odata.type") != "#microsoft.graph.fileAttachment"
        or not (mime_type := MimeType.try_decode(attachment_data.get("contentType")))
        or not (blob_data := attachment_data.get("contentBytes"))
    ):
        raise UnavailableError.new()

    response = await downloader.documents_read_blob(
        name=attachment_data.get("name", "Untitled Attachment"),
        mime_type=mime_type,
        blob=blob_data,
        original=False,
    )

    return ObserveResult(
        bundle=response.as_fragment(),
        should_cache=True,
        option_labels=True,
        option_relations_link=True,
    )


async def _read_outlook_attachment_file(
    handle: MsHandle,
    locator: MsOutlookAttachmentLocator,
) -> ObserveResult:
    attachment_data = await handle.fetch_outlook_attachment(
        locator.message_id, locator.attachment_id
    )
    if (
        not attachment_data
        or attachment_data.get("@odata.type") != "#microsoft.graph.fileAttachment"
        or not (mime_type := MimeType.try_decode(attachment_data.get("contentType")))
        or not (blob_data := attachment_data.get("contentBytes"))
    ):
        raise UnavailableError.new()

    return ObserveResult(
        bundle=BundleFile(
            uri=locator.resource_uri().child_affordance(AffFile.new()),
            description=None,
            mime_type=mime_type,
            download_url=DataUri.new(mime_type, blob_data),
            expiry=None,
        ),
        should_cache=False,
    )


##
## Read - Outlook - Email
##


class OutlookAttachmentInfo(BaseModel):
    name: str
    item_id: Base64Safe
    attachment_id: Base64Safe
    mime_type: MimeType | None
    size: int


async def _resolve_outlook_email(
    handle: MsHandle,
    locator: MsOutlookEmailLocator,
    cached: ResourceView | None,
) -> ResolveResult:
    """
    NOTE: If the request access token is unable to read the file, then this will
    raise an `UnavailableError`, checking that the user has access to the cached
    metadata and contents.
    """
    email_data = await handle.fetch_graph_api(
        f"v1.0/me/messages/{locator.message_id}",
        headers={"Accept": "application/json"},
    )

    metadata = MetadataDelta(
        name=email_data.get("subject", "Untitled Email"),
        mime_type=MimeType.decode(
            "text/html"
            if email_data.get("body", {}).get("contentType") == "html"
            else "text/plain"
        ),
        created_at=(
            dateutil.parser.parse(sent_date)
            if (sent_date := email_data.get("sentDateTime"))
            else None
        ),
        updated_at=(
            dateutil.parser.parse(received_date)
            if (received_date := email_data.get("receivedDateTime"))
            else None
        ),
        description=shorten_description(_extract_email_preview(email_data)),
    )

    if not cached:
        metadata = metadata.with_update(
            MetadataDelta(
                affordances=[AffordanceInfo(suffix=AffBody.new())],
            ),
        )

    return ResolveResult(metadata=metadata, expired=[], should_cache=False)


async def _read_outlook_email_body(
    handle: MsHandle,
    locator: MsOutlookEmailLocator,
) -> ObserveResult:
    """
    TODO: In attachment, if "isInline", then include as a blob in the `Fragment`.
    """
    query_params = urlencode(
        {
            "$select": "subject,bodyPreview,body,from,toRecipients,ccRecipients,sentDateTime,receivedDateTime,hasAttachments,importance,isRead,conversationId",
            "$expand": "attachments($select=name,contentType,size)",
        }
    )
    email_data = await handle.fetch_graph_api(
        f"v1.0/me/messages/{locator.message_id}?{query_params}",
        headers={
            "Accept": "application/json",
            "Prefer": 'outlook.body-content-type="html"',
        },
    )
    text, attachments = _email_to_markdown(handle.realm, email_data)

    relations: list[Relation] = []
    for attachment in attachments:
        attachment_locator = MsOutlookAttachmentLocator(
            realm=handle.realm,
            message_id=attachment.item_id,
            attachment_id=attachment.attachment_id,
        )
        relations.append(
            RelationParent(
                parent=locator.resource_uri(),
                child=attachment_locator.resource_uri(),
            )
        )

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=text, blobs={}),
        relations=relations,
        should_cache=True,  # Emails are immutable
        option_labels=False,
        option_relations_link=True,
    )


def _extract_email_preview(email_data: dict[str, Any]) -> str:
    """Extract a preview/description from email data."""
    preview = email_data.get("bodyPreview", "")
    if len(preview) > EMAIL_PREVIEW_MAX_LENGTH:
        preview = preview[: EMAIL_PREVIEW_MAX_LENGTH - 3] + "..."
    return preview or "No preview available"


def _email_to_markdown(  # noqa: C901, PLR0912
    realm: Realm,
    email_data: dict[str, Any],
) -> tuple[str, list[OutlookAttachmentInfo]]:
    """Convert email data to markdown format."""
    parts = []

    # Header.
    if sender := email_data.get("from", {}):
        parts.append(f"From: {_format_recipients([sender])}")
    if to_recipients := email_data.get("toRecipients", []):
        parts.append(f"To: {_format_recipients(to_recipients)}")
    if cc_recipients := email_data.get("ccRecipients", []):
        parts.append(f"Cc: {_format_recipients(cc_recipients)}")
    if bcc_recipients := email_data.get("bccRecipients", []):
        parts.append(f"Bcc: {_format_recipients(bcc_recipients)}")
    if sent_date := email_data.get("sentDateTime"):
        try:
            dt = dateutil.parser.parse(sent_date)
            formatted_date = dt.strftime("%B %d, %Y at %I:%M %p")
            parts.append(f"Sent: {formatted_date}")
        except Exception:
            parts.append(f"Sent: {sent_date}")
    if subject := email_data.get("subject"):
        parts.append(f"Subject: {subject}")
    if (importance := email_data.get("importance")) and importance != "normal":
        parts.append(f"Importance: {importance.capitalize()}")

    # Convert HTML to markdown and return plain text as-is.
    # When the body cannot be read, fallback to the body preview.
    body_content: str
    email_body = email_data.get("body", {})
    if email_body.get("contentType") == "html" and email_body.get("content"):
        body_content, _ = markdown_from_msteams(email_body["content"])
    elif email_body.get("contentType") == "text" and email_body.get("content"):
        body_content = email_body["content"]
    else:
        body_content = email_data.get("bodyPreview") or ""
    parts.append(f"\n{body_content.rstrip()}")

    # Attachments
    attachments: list[OutlookAttachmentInfo] = []
    for attachment in email_data.get("attachments", []):
        if attachment.get("@odata.type") != "#microsoft.graph.fileAttachment":
            continue

        attachments.append(
            OutlookAttachmentInfo(
                name=attachment.get("name", "Unnamed Attachment"),
                item_id=Base64Safe.decode(email_data.get("id")),
                attachment_id=Base64Safe.decode(attachment.get("id")),
                mime_type=MimeType.try_decode(attachment.get("contentType", "unknown")),
                size=attachment.get("size", 0),
            )
        )

    if attachments:
        parts.append("\n## Attachments\n")
        parts.extend(
            f"- [{attachment.name}]"
            f"(ndk://{realm}/outlook-email/{attachment.item_id.as_filename()}"
            f"/attachments/{attachment.attachment_id.as_filename()})"
            for attachment in attachments
        )

    return "\n".join(parts), attachments


def _format_recipients(recipients: list[dict[str, Any]]) -> str:
    return ", ".join(
        [
            f"{email.get('name', '')} <{email.get('address', '')}>"
            for r in recipients
            if (email := r.get("emailAddress", {}))
        ]
    )


##
## Read - Outlook - Event
##


async def _resolve_outlook_event(
    handle: MsHandle,
    locator: MsOutlookEventLocator,
    cached: ResourceView | None,
) -> ResolveResult:
    """
    NOTE: If the request access token is unable to read the file, then this will
    raise an `UnavailableError`, checking that the user has access to the cached
    metadata and contents.
    """
    event_data = await handle.fetch_graph_api(
        f"v1.0/me/events/{locator.event_id}",
        headers={"Accept": "application/json"},
    )

    metadata = MetadataDelta(
        name=event_data.get("subject", "Untitled Event"),
        created_at=(
            dateutil.parser.parse(created_date)
            if (created_date := event_data.get("createdDateTime"))
            else None
        ),
        updated_at=(
            dateutil.parser.parse(modified_date)
            if (modified_date := event_data.get("lastModifiedDateTime"))
            else None
        ),
        description=shorten_description(_extract_event_description(event_data)),
        # TODO:
        # revision_data=event_data.get("cTag"),
        # revision_meta=event_data.get("eTag"),
    )

    if not cached:
        metadata = metadata.with_update(
            MetadataDelta(
                affordances=[AffordanceInfo(suffix=AffBody.new())],
            ),
        )

    return ResolveResult(metadata=metadata, expired=[], should_cache=False)


async def _read_outlook_event_body(
    handle: MsHandle,
    locator: MsOutlookEventLocator,
) -> ObserveResult:
    query_params = urlencode(
        {
            "$select": "subject,bodyPreview,body,organizer,attendees,start,end,location,importance,isAllDay,webLink,categories,showAs,sensitivity,responseStatus,createdDateTime,lastModifiedDateTime",
        }
    )
    event_data = await handle.fetch_graph_api(
        f"v1.0/me/events/{locator.event_id}?{query_params}",
        headers={
            "Accept": "application/json",
            "Prefer": 'outlook.body-content-type="html"',
        },
    )
    text = _event_to_markdown(event_data)

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=text, blobs={}),
        should_cache=False,  # Events can change
        option_labels=False,
        option_relations_link=True,
    )


def _extract_event_description(event_data: dict[str, Any]) -> str:
    """Extract a description from calendar event data."""
    # Extract start time
    start_time = None
    if start_datetime := event_data.get("start", {}).get("dateTime"):
        with contextlib.suppress(Exception):
            start_time = dateutil.parser.parse(start_datetime)

    # Extract end time
    end_time = None
    if end_datetime := event_data.get("end", {}).get("dateTime"):
        with contextlib.suppress(Exception):
            end_time = dateutil.parser.parse(end_datetime)

    # Generate description
    description_parts = []
    if event_data.get("isAllDay"):
        description_parts.append("All-day event")

    if start_time and end_time:
        if event_data.get("isAllDay"):
            description_parts.append(f"on {start_time.strftime('%B %d, %Y')}")
        else:
            description_parts.append(
                f"from {start_time.strftime('%B %d, %Y at %I:%M %p')} to {end_time.strftime('%I:%M %p')}"
            )

    if location := event_data.get("location", {}).get("displayName"):
        description_parts.append(f"at {location}")

    if body_preview := event_data.get("bodyPreview", "").strip():
        preview = body_preview[:EMAIL_PREVIEW_MAX_LENGTH]
        if len(body_preview) > EMAIL_PREVIEW_MAX_LENGTH:
            preview += "..."
        if preview:
            description_parts.append(preview)

    return (
        ". ".join(description_parts)
        if description_parts
        else "No description available"
    )


def _event_to_markdown(  # noqa: C901, PLR0912, PLR0915
    event_data: dict[str, Any],
) -> str:
    """Convert calendar event data to markdown format."""
    parts = []

    # Title
    subject = event_data.get("subject", "Untitled Event")
    parts.append(f"# {subject}")

    # Event Details section
    parts.append("\n## Event Details\n")

    # Date and Time
    if start_info := event_data.get("start"):
        start_datetime = start_info.get("dateTime", "")
        start_tz = start_info.get("timeZone", "")

        if end_info := event_data.get("end"):
            end_datetime = end_info.get("dateTime", "")
            end_tz = end_info.get("timeZone", "")

            if event_data.get("isAllDay"):
                # Format all-day events
                try:
                    start_dt = dateutil.parser.parse(start_datetime)
                    end_dt = dateutil.parser.parse(end_datetime)
                    # Subtract one day from end date for all-day events (Graph API convention)
                    end_dt = end_dt - dateutil.relativedelta.relativedelta(days=1)

                    if start_dt.date() == end_dt.date():
                        parts.append(
                            f"**Date:** {start_dt.strftime('%B %d, %Y')} (All day)"
                        )
                    else:
                        parts.append(
                            f"**Date:** {start_dt.strftime('%B %d, %Y')} - {end_dt.strftime('%B %d, %Y')} (All day)"
                        )
                except Exception:
                    parts.append(
                        f"**Date:** {start_datetime} - {end_datetime} (All day)"
                    )
            else:
                # Regular events with specific times
                try:
                    start_dt = dateutil.parser.parse(start_datetime)
                    end_dt = dateutil.parser.parse(end_datetime)

                    if start_dt.date() == end_dt.date():
                        parts.append(f"**Date:** {start_dt.strftime('%B %d, %Y')}")
                        parts.append(
                            f"**Time:** {start_dt.strftime('%I:%M %p')} - {end_dt.strftime('%I:%M %p')} {start_tz}"
                        )
                    else:
                        parts.append(
                            f"**Start:** {start_dt.strftime('%B %d, %Y at %I:%M %p')} {start_tz}"
                        )
                        parts.append(
                            f"**End:** {end_dt.strftime('%B %d, %Y at %I:%M %p')} {end_tz}"
                        )
                except Exception:
                    parts.append(f"**Start:** {start_datetime} {start_tz}")
                    parts.append(f"**End:** {end_datetime} {end_tz}")

    # Location
    if location := event_data.get("location", {}).get("displayName"):
        parts.append(f"**Location:** {location}")

    # Organizer
    if email_address := event_data.get("organizer", {}).get("emailAddress"):
        organizer_name = email_address.get(
            "name", email_address.get("address", "Unknown")
        )
        parts.append(f"**Organizer:** {organizer_name}")

    # Show As
    if show_as := event_data.get("showAs"):
        show_as_display = show_as.replace("oof", "Out of Office").title()
        parts.append(f"**Show as:** {show_as_display}")

    # Importance
    if (importance := event_data.get("importance")) and importance != "normal":
        parts.append(f"**Importance:** {importance.capitalize()}")

    # Categories
    if categories := event_data.get("categories"):
        parts.append(f"**Categories:** {', '.join(categories)}")

    # Attendees
    if attendees := event_data.get("attendees", []):
        parts.append("\n## Attendees\n")

        # Group attendees by response status
        accepted = []
        tentative = []
        declined = []
        no_response = []

        for attendee in attendees:
            if email_info := attendee.get("emailAddress"):
                name = email_info.get("name", email_info.get("address", "Unknown"))
                status = attendee.get("status", {}).get("response", "none")

                if status == "accepted":
                    accepted.append(name)
                elif status == "tentative":
                    tentative.append(name)
                elif status == "declined":
                    declined.append(name)
                else:
                    no_response.append(name)

        if accepted:
            parts.append(f"**Accepted ({len(accepted)}):** {', '.join(accepted)}")
        if tentative:
            parts.append(f"**Tentative ({len(tentative)}):** {', '.join(tentative)}")
        if declined:
            parts.append(f"**Declined ({len(declined)}):** {', '.join(declined)}")
        if no_response:
            parts.append(
                f"**No Response ({len(no_response)}):** {', '.join(no_response)}"
            )

    # Body/Description
    if body := event_data.get("body", {}):
        parts.append("\n## Description\n")

        if body.get("contentType") == "html" and body.get("content"):
            # Convert HTML to markdown
            markdown_content, _ = markdown_from_msteams(body["content"])
            parts.append(markdown_content)
        elif body.get("contentType") == "text" and body.get("content"):
            # Plain text - just add it
            parts.append(body["content"])
        else:
            # Fallback to body preview
            preview = event_data.get("bodyPreview", "")
            if preview:
                parts.append(preview)
            else:
                parts.append("*No description provided*")

    return "\n".join(parts)


##
## Handle
##


GRAPH_API_LOCK = asyncio.Lock()
"""
There should only be one concurrent request to Graph API in parallel, to avoid
responses with "429 Too Many Requests".
"""


@dataclass(kw_only=True)
class MsHandle:
    context: KnowledgeContext
    realm: Realm
    domain: str
    authorization: str
    _cache_user_info: dict[str, dict[str, Any] | None]
    _cache_onedrive_children_by_id: dict[str, list[dict[str, Any]] | None]
    _cache_onedrive_info_by_id: dict[str, dict[str, Any] | None]
    _cache_onedrive_info_key_by_path: dict[str, str | None]
    _cache_outlook_attachment_by_id: dict[str, dict[str, Any] | None]

    async def fetch_graph_api(
        self,
        endpoint: str,
        headers: dict[str, str] | None = None,
    ) -> Any:
        downloader = self.context.service(SvcDownloader)

        headers = {**headers} if headers else {}
        headers["Authorization"] = self.authorization

        try:
            async with GRAPH_API_LOCK:
                await asyncio.sleep(0.1)
                url = WebUrl.decode(f"https://graph.microsoft.com/{endpoint}")
                data, _ = await downloader.fetch_json(url=url, headers=headers)
                return data
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("Failed to fetch from Graph API")
            raise UnavailableError.new()  # noqa: B904

    ##
    ## Helpers - OneDrive
    ##

    async def fetch_user_info(self, email_or_id: str) -> dict[str, Any] | None:
        """
        NOTE: Translates pseudo-email from human URLs into a corporate email.
        For example,

        - "userhandle_mycompany_com"   ->  "userhandle@mycompany.com"
        - "user_handle_mycompany_com"  ->  "user.handle@mycompany.com"
        """
        email_or_id = email_or_id.lower()
        if "_" in email_or_id:
            email_or_id = ".".join(email_or_id.rsplit("_", 1))  # Last underscore -> "."
            email_or_id = "@".join(email_or_id.rsplit("_", 1))  # Second-to-last -> "@"
            email_or_id = email_or_id.replace("_", ".")  # Remaining underscores -> "."

        if email_or_id not in self._cache_user_info:
            try:
                data = await self.fetch_graph_api(f"v1.0/users/{email_or_id}")
                self._cache_user_info[data.get("id")] = data
                self._cache_user_info[data.get("userPrincipalName")] = data
            except Exception:
                self._cache_user_info[email_or_id] = None

        return self._cache_user_info[email_or_id]

    async def fetch_onedrive_info_by_id(
        self,
        user_id: UserId,
        item_id: MsDriveItemId,
    ) -> dict[str, Any] | None:
        key = f"{user_id.uuid()}/{item_id}"
        if key not in self._cache_onedrive_info_by_id:
            try:
                data = await self.fetch_graph_api(
                    f"v1.0/users/{user_id.uuid()}/drive/items/{item_id}"
                )
                item_key = f"{user_id.uuid()}/{data['id']}"
                alias_key_path = f"{user_id.uuid()}/{_infer_item_path(data)}"
                self._cache_onedrive_info_by_id[item_key] = data
                self._cache_onedrive_info_key_by_path[alias_key_path] = item_key
            except Exception:
                self._cache_onedrive_info_by_id[key] = None

        return self._cache_onedrive_info_by_id[key]

    async def fetch_onedrive_info_by_path(
        self,
        user_id: UserId,
        item_path: str,
    ) -> dict[str, Any] | None:
        key = f"{user_id.uuid()}/{item_path}"
        if key not in self._cache_onedrive_info_key_by_path:
            data = None
            try:
                encoded_path = quote(str(item_path).replace("'", "''"))
                data = await self.fetch_graph_api(
                    f"v1.0/users/{user_id.uuid()}/drive/items/root:/{encoded_path}"
                )
                item_key = f"{user_id.uuid()}/{data['id']}"
                alias_key_path = f"{user_id.uuid()}/{_infer_item_path(data)}"
                self._cache_onedrive_info_by_id[item_key] = data
                self._cache_onedrive_info_key_by_path[alias_key_path] = item_key
            except Exception:
                self._cache_onedrive_info_key_by_path[key] = None

        return self._cache_onedrive_info_by_id.get(
            self._cache_onedrive_info_key_by_path[key]  # type: ignore
        )

    async def fetch_onedrive_children_by_id(
        self,
        user_id: UserId,
        item_path: str,
    ) -> list[dict[str, Any]] | None:
        key = f"{user_id.uuid()}/{item_path}"
        if key not in self._cache_onedrive_children_by_id:
            try:
                data = await self.fetch_graph_api(
                    f"v1.0/users/{user_id.uuid()}/drive/items/{item_path}/children"
                )
                children = data.get("value", [])
                self._cache_onedrive_children_by_id[key] = children

                # Cache the children's info as well, for faster `resolve`.
                for child in children:
                    child_key = f"{user_id.uuid()}/{child['id']}"
                    child_alias_key = f"{user_id.uuid()}/{_infer_item_path(child)}"
                    self._cache_onedrive_info_by_id[child_key] = child
                    self._cache_onedrive_info_key_by_path[child_alias_key] = child_key
            except Exception:
                self._cache_onedrive_children_by_id[key] = None

        return self._cache_onedrive_children_by_id.get(key)

    ##
    ## Helpers - Outlook
    ##

    async def fetch_outlook_attachment(
        self,
        message_id: Base64Safe,
        attachment_id: Base64Safe,
    ) -> dict[str, Any] | None:
        key = f"{message_id}/{attachment_id}"
        if key not in self._cache_outlook_attachment_by_id:
            try:
                data = await self.fetch_graph_api(
                    f"v1.0/me/messages/{message_id}/attachments/{attachment_id}",
                    headers={"Accept": "application/json"},
                )
                self._cache_outlook_attachment_by_id[key] = data
            except Exception:
                self._cache_outlook_attachment_by_id[key] = None

        return self._cache_outlook_attachment_by_id[key]


def _infer_item_kind(item_info: dict[str, Any]) -> ItemKind | None:
    if item_info.get("file"):
        return "file"
    elif item_info.get("folder"):
        return "folder"
    else:
        return None


def _infer_item_path(item_info: dict[str, Any]) -> str:
    parent_path: str
    _, parent_path = item_info["parentReference"]["path"].split(":", 1)
    parent_path = parent_path.removeprefix("/")
    return f"{parent_path}/{item_info['name']}" if parent_path else item_info["name"]
