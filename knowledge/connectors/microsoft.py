import asyncio
import base64
import contextlib
import dateutil.parser
import dateutil.relativedelta
import html
import json
import logging
import msal
import re
import time

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pydantic import BaseModel
from tabulate import tabulate
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
from base.strings.file import FileName, FilePath
from base.strings.microsoft import (
    MsChannelId,
    MsDriveItemId,
    MsGroupId,
    MsSiteId,
    MsSiteName,
)
from base.strings.resource import (
    ExternalUri,
    Observable,
    Realm,
    ResourceUri,
    RootReference,
    WebUrl,
)
from base.utils.markdown import markdown_from_msteams, strip_keep_indent

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

EMAIL_PREVIEW_MAX_LENGTH = 500
TOKEN_EXPIRY_BUFFER: int = 600

EXPIRY_DOWNLOAD_URL = timedelta(minutes=5)

_CACHED_MICROSOFT_TOKEN: str | None = None
_CACHED_MICROSOFT_TOKEN_EXPIRY: int = 0

ItemKind = Literal["file", "folder"]


##
## Config
##


class MicrosoftConnectorConfig(BaseModel):
    realm: Realm
    domain_sharepoint: str
    domain_onedrive: str
    tenant_id: str
    public_client_id: str | None = None
    public_client_secret: str | None = None

    def instantiate(self, context: KnowledgeContext) -> "MicrosoftConnector":
        return MicrosoftConnector(
            context=context,
            realm=self.realm,
            domain_sharepoint=self.domain_sharepoint,
            domain_onedrive=self.domain_onedrive,
            tenant_id=self.tenant_id,
            public_client_id=self.public_client_id,
            public_client_secret=self.public_client_secret,
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
        handle: "MsHandle",
        url: WebUrl,
    ) -> "MsOneDriveFileLocator | None":
        if (
            url.domain == handle.domain_onedrive
            and (
                match := re.fullmatch(r"personal/([a-z0-9_]+)/Documents/(.+)", url.path)
            )
            and (path_email := match.group(1))
            and (item_path := unquote_plus(match.group(2)))
            and (user_info := await handle.fetch_user_info(path_email))
            and (user_id := UserId.try_decode(user_info.get("id")))
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
                domain=handle.domain_onedrive,
                user_email=user_info["userPrincipalName"],
                user_id=UserId.decode(user_info["id"]),
                item_id=item_id,
                item_kind=item_kind,
                item_path=item_path,
            )

        return None

    @staticmethod
    async def from_uri(
        handle: "MsHandle",
        uri: ResourceUri,
    ) -> "MsOneDriveFileLocator | None":
        if (
            uri.realm == handle.realm
            and uri.subrealm.startswith("onedrive-")
            and (user_id := uri.subrealm.removeprefix("onedrive-"))
            and (user_info := await handle.fetch_user_info(user_id))
            and (user_id := UserId.try_decode(user_info["id"]))
            and len(uri.path) == 2  # noqa: PLR2004
            and uri.path[0] == "Documents"
            and (item_id := MsDriveItemId.from_filename(uri.path[1]))
            and (item_info := await handle.fetch_onedrive_info_by_id(user_id, item_id))
            and (item_id := MsDriveItemId.try_decode(item_info.get("id")))
            and (item_kind := _infer_item_kind(item_info))
        ):
            return MsOneDriveFileLocator(
                realm=handle.realm,
                domain=handle.domain_onedrive,
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
            subrealm=FileName.decode(f"onedrive-{self.user_id}"),
            path=[FileName.decode("Documents"), self.item_id.as_filename()],
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
        handle: "MsHandle",  # noqa: ARG004
        url: WebUrl,  # noqa: ARG004
    ) -> "MsOutlookAttachmentLocator | None":
        return None

    @staticmethod
    async def from_uri(
        handle: "MsHandle",
        uri: ResourceUri,
    ) -> "MsOutlookAttachmentLocator | None":
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
        handle: "MsHandle",
        url: WebUrl,
    ) -> "MsOutlookEmailLocator | None":
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
        handle: "MsHandle",
        uri: ResourceUri,
    ) -> "MsOutlookEmailLocator | None":
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
        handle: "MsHandle",
        url: WebUrl,
    ) -> "MsOutlookEventLocator | None":
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
        handle: "MsHandle",
        uri: ResourceUri,
    ) -> "MsOutlookEventLocator | None":
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


##
## Locator - SharePoint
##


class MsSharePointFileLocator(Locator, frozen=True):
    kind: Literal["ms_file"] = "ms_file"
    domain: str
    site_id: MsSiteId
    site_name: MsSiteName
    item_id: MsDriveItemId
    item_kind: ItemKind
    item_path: str

    @staticmethod
    async def from_web(
        handle: "MsHandle",
        url: WebUrl,
    ) -> "MsSharePointFileLocator | None":
        if (
            url.domain == handle.domain_sharepoint
            and (
                match := re.fullmatch(
                    r"sites/([A-Za-z0-9_\-]+)/Shared%20Documents/(.+)", url.path
                )
            )
            and (site_name := MsSiteName.try_decode(match.group(1)))
            and (file_path := unquote_plus(match.group(2)))
            and (site_info := await handle.fetch_sharepoint_site_by_name(site_name))
            and (
                item_info := await handle.fetch_sharepoint_info_by_path(
                    site_info.site_id, file_path
                )
            )
            and (item_id := MsDriveItemId.try_decode(item_info.get("id", "")))
            and (item_kind := _infer_item_kind(item_info))
        ):
            return MsSharePointFileLocator(
                realm=handle.realm,
                domain=handle.domain_sharepoint,
                site_id=site_info.site_id,
                site_name=site_info.site_name,
                item_id=item_id,
                item_kind=item_kind,
                item_path=file_path,
            )

        return None

    @staticmethod
    async def from_uri(
        handle: "MsHandle",
        uri: ResourceUri,
    ) -> "MsSharePointFileLocator | None":
        if (
            uri.realm == handle.realm
            and uri.subrealm.startswith("sharepoint-")
            and len(uri.path) == 2  # noqa: PLR2004
            and uri.path[0] == "Shared_Documents"
            and (
                site_id := MsSiteId.try_decode(uri.subrealm.removeprefix("sharepoint-"))
            )
            and (site_info := await handle.fetch_sharepoint_site_by_id(site_id))
            and (item_id := MsDriveItemId.from_filename(uri.path[1]))
            and (
                item_info := await handle.fetch_sharepoint_info_by_id(site_id, item_id)
            )
            and (item_kind := _infer_item_kind(item_info))
        ):
            return MsSharePointFileLocator(
                realm=handle.realm,
                domain=handle.domain_sharepoint,
                site_id=site_info.site_id,
                site_name=site_info.site_name,
                item_id=item_id,
                item_kind=item_kind,
                item_path=_infer_item_path(item_info),
            )

        return None

    def resource_uri(self) -> ResourceUri:
        # Always use site_id and item_id for resource URI
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode(f"sharepoint-{self.site_id}"),
            path=[FileName.decode("Shared_Documents"), FileName.decode(self.item_id)],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/sites/{self.site_name}/Shared%20Documents/{quote(self.item_path)}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class MsSharePointListLocator(Locator, frozen=True):
    kind: Literal["ms_list"] = "ms_list"
    domain: str
    site_id: MsSiteId
    site_name: MsSiteName
    list_id: MsDriveItemId
    list_name: str

    @staticmethod
    async def from_web(
        handle: "MsHandle",
        url: WebUrl,
    ) -> "MsSharePointListLocator | None":
        if (
            url.domain == handle.domain_sharepoint
            and (
                match := re.fullmatch(
                    r"sites/([A-Za-z0-9_\-]+)/Lists/([^/]+)(?:/.+)?",
                    url.path,
                )
            )
            and (site_name := MsSiteName.try_decode(match.group(1)))
            and (list_name := unquote_plus(match.group(2)))
            and (site_info := await handle.fetch_sharepoint_site_by_name(site_name))
        ):
            response = await handle.fetch_graph_api(
                f"v1.0/sites/{site_info.site_id}/lists?$filter=name eq '{list_name}'"
            )
            if not response.get("value"):
                return None

            return MsSharePointListLocator(
                realm=handle.realm,
                domain=handle.domain_sharepoint,
                site_id=site_info.site_id,
                site_name=site_info.site_name,
                list_id=MsDriveItemId.decode(response["value"][0]["id"]),
                list_name=response["value"][0]["name"],
            )

        return None

    @staticmethod
    async def from_uri(
        handle: "MsHandle",
        uri: ResourceUri,
    ) -> "MsSharePointListLocator | None":
        if (
            uri.realm == handle.realm
            and uri.subrealm.startswith("sharepoint-")
            and (
                site_id := MsSiteId.try_decode(uri.subrealm.removeprefix("sharepoint-"))
            )
            and len(uri.path) == 2  # noqa: PLR2004
            and uri.path[0] == "Lists"
            and (item_id := MsDriveItemId.from_filename(uri.path[1]))
            and (site_info := await handle.fetch_sharepoint_site_by_id(site_id))
        ):
            response = await handle.fetch_graph_api(
                f"v1.0/sites/{site_info.site_id}/lists/{item_id}"
            )
            if not response.get("value"):
                return None

            return MsSharePointListLocator(
                realm=handle.realm,
                domain=handle.domain_sharepoint,
                site_id=site_info.site_id,
                site_name=site_info.site_name,
                list_id=MsDriveItemId.decode(response["value"][0]["id"]),
                list_name=response["value"][0]["name"],
            )

        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode(f"sharepoint-{self.site_id}"),
            path=[FileName.decode("Lists"), self.list_id.as_filename()],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/sites/{self.site_name}/Lists/{self.list_name}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class MsSharePointPageLocator(Locator, frozen=True):
    kind: Literal["ms_page"] = "ms_page"
    domain: str
    site_id: MsSiteId
    site_name: MsSiteName
    page_id: FileName
    page_path: str

    @staticmethod
    async def from_web(
        handle: "MsHandle",
        url: WebUrl,
    ) -> "MsSharePointPageLocator | None":
        if (
            url.domain == handle.domain_sharepoint
            and (
                match := re.fullmatch(
                    r"sites/([A-Za-z0-9_\-]+)/SitePages/(.+)", url.path
                )
            )
            and (site_name := MsSiteName.try_decode(match.group(1)))
            and (page_path := FilePath.try_decode(unquote_plus(match.group(2))))
            and (site_info := await handle.fetch_sharepoint_site_by_name(site_name))
        ):
            page_data = await handle.fetch_graph_api(
                f"v1.0/sites/{site_info.site_id}/pages?$filter=name eq '{page_path.filename()}'"
            )
            if not page_data.get("value"):
                raise UnavailableError.new()

            return MsSharePointPageLocator(
                realm=handle.realm,
                domain=handle.domain_sharepoint,
                site_id=site_info.site_id,
                site_name=site_info.site_name,
                page_id=FileName.decode(page_data["value"][0]["id"]),
                page_path=page_data["value"][0]["webUrl"].removeprefix(
                    f"https://{site_info.domain}/sites/{site_info.site_name}/SitePages/"
                ),
            )

        return None

    @staticmethod
    async def from_uri(
        handle: "MsHandle",
        uri: ResourceUri,
    ) -> "MsSharePointPageLocator | None":
        if (
            uri.realm == handle.realm
            and uri.subrealm.startswith("sharepoint-")
            and (
                site_id := MsSiteId.try_decode(uri.subrealm.removeprefix("sharepoint-"))
            )
            and len(uri.path) == 2  # noqa: PLR2004
            and uri.path[0] == "SitePages"
            and (site_info := await handle.fetch_sharepoint_site_by_id(site_id))
        ):
            page_data = await handle.fetch_graph_api(
                f"v1.0/sites/{site_info.site_id}/pages/{uri.path[1]}"
            )
            if not page_data.get("value"):
                return None

            return MsSharePointPageLocator(
                realm=handle.realm,
                domain=handle.domain_sharepoint,
                site_id=site_info.site_id,
                site_name=site_info.site_name,
                page_id=FileName.decode(page_data["value"][0]["id"]),
                page_path=page_data["value"][0]["webUrl"].removeprefix(
                    f"https://{site_info.domain}/sites/{site_info.site_name}/SitePages/"
                ),
            )

        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode(f"sharepoint-{self.site_id}"),
            path=[FileName.decode("SitePages"), self.page_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/sites/{self.site_name}/SitePages/{self.page_path}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


##
## Microsoft - Teams
##


class MsTeamsMessageLocator(Locator, frozen=True):
    kind: Literal["ms_teams_message"] = "ms_teams_message"
    group_id: MsGroupId
    channel_id: MsChannelId
    message_id: FileName
    parent_message_id: FileName | None

    @staticmethod
    async def from_web(
        handle: "MsHandle",
        url: WebUrl,
    ) -> "MsTeamsMessageLocator | None":
        """
        Example: "https://teams.microsoft.com/l/message/{channel_id}/{message_id}?groupId={team_id}"
        Becomes: "ndk://microsoft/teams-{group_id}/{channel_id}/{message_id}"
        """
        if (
            url.domain == "teams.microsoft.com"
            and (match := re.fullmatch(r"l/message/([^/]+)/(\d+)", url.path))
            and (channel_id := MsChannelId.try_decode(unquote(match.group(1))))
            and (message_id := FileName.try_decode(match.group(2)))
            and (group_id := MsGroupId.try_decode(url.get_query("groupId")))
        ):
            return MsTeamsMessageLocator(
                realm=handle.realm,
                group_id=group_id,
                channel_id=channel_id,
                message_id=message_id,
                parent_message_id=FileName.try_decode(url.get_query("parentMessageId")),
            )

        return None

    @staticmethod
    async def from_uri(
        handle: "MsHandle",
        uri: ResourceUri,
    ) -> "MsTeamsMessageLocator | None":
        if (
            uri.realm == handle.realm
            and uri.subrealm.startswith("teams-")
            and (group_id := MsGroupId.try_decode(uri.subrealm.removeprefix("teams-")))
            and len(uri.path) == 2  # noqa: PLR2004
            and (channel_id := MsChannelId.from_filename(uri.path[0]))
            and (message_id := FileName.try_decode(uri.path[1]))
        ):
            return MsTeamsMessageLocator(
                realm=handle.realm,
                group_id=group_id,
                channel_id=channel_id,
                message_id=message_id,
                parent_message_id=None,
            )

        return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode(f"teams-{self.group_id}"),
            path=[self.channel_id.as_filename(), self.message_id],
        )

    def content_url(self) -> WebUrl:
        url = f"https://teams.microsoft.com/l/message/{self.channel_id}/{self.message_id}?groupId={self.group_id}"
        if self.parent_message_id:
            url += f"&parentMessageId={self.parent_message_id}"
        return WebUrl.decode(url)

    def citation_url(self) -> WebUrl:
        return self.content_url()


AnyMsLocator = (
    MsOneDriveFileLocator
    | MsOutlookAttachmentLocator
    | MsOutlookEmailLocator
    | MsOutlookEventLocator
    | MsSharePointFileLocator
    | MsSharePointListLocator
    | MsSharePointPageLocator
    | MsTeamsMessageLocator
)


##
## Connector
##


@dataclass(kw_only=True)
class MicrosoftConnector(Connector):
    domain_sharepoint: str
    domain_onedrive: str
    tenant_id: str
    public_client_id: str | None
    public_client_secret: str | None
    _handle: "MsHandle | None" = None

    async def _acquire_handle(self) -> "MsHandle":
        if self._handle is None:
            self._handle = MsHandle(
                context=self.context,
                realm=self.realm,
                domain_sharepoint=self.domain_sharepoint,
                domain_onedrive=self.domain_onedrive,
                authorization=await self._get_authorization(),
                _cache_user_info={},
                _cache_onedrive_children_by_id={},
                _cache_onedrive_info_by_id={},
                _cache_onedrive_info_key_by_path={},
                _cache_outlook_attachment_by_id={},
                _cache_sharepoint_children_by_id={},
                _cache_sharepoint_info_by_id={},
                _cache_sharepoint_info_key_by_path={},
                _cache_sharepoint_site_by_id={},
                _cache_sharepoint_site_key_by_group={},
                _cache_sharepoint_site_key_by_name={},
                _cache_teams_conversation={},
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
                self.domain_onedrive,
                self.domain_sharepoint,
            ):
                return None

            handle = await self._acquire_handle()
            locator = (
                await MsOneDriveFileLocator.from_web(handle, reference)
                or await MsOutlookAttachmentLocator.from_web(handle, reference)
                or await MsOutlookEmailLocator.from_web(handle, reference)
                or await MsOutlookEventLocator.from_web(handle, reference)
                or await MsSharePointFileLocator.from_web(handle, reference)
                or await MsSharePointListLocator.from_web(handle, reference)
                or await MsSharePointPageLocator.from_web(handle, reference)
                or await MsTeamsMessageLocator.from_web(handle, reference)
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
                or await MsSharePointFileLocator.from_uri(handle, reference)
                or await MsSharePointListLocator.from_uri(handle, reference)
                or await MsSharePointPageLocator.from_uri(handle, reference)
                or await MsTeamsMessageLocator.from_uri(handle, reference)
            )

    async def resolve(  # noqa: PLR0911
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
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
            case MsSharePointFileLocator():
                return await _resolve_sharepoint_file(handle, locator, cached)
            case MsSharePointListLocator():
                return await _resolve_sharepoint_list(handle, locator, cached)
            case MsSharePointPageLocator():
                return await _resolve_sharepoint_page(handle, locator, cached)
            case MsTeamsMessageLocator():
                return await _resolve_teams_message(handle, locator, cached)
            case _:
                raise UnavailableError.new()

    async def observe(  # noqa: C901, PLR0911, PLR0912
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
            case (MsSharePointFileLocator(), AffBody()):
                return await _read_sharepoint_file_body(handle, locator)
            case (MsSharePointFileLocator(), AffCollection()):
                return await _read_sharepoint_file_collection(handle, locator)
            case (MsSharePointFileLocator(), AffFile(path=[])):
                return await _read_sharepoint_file_file(handle, locator)
            case (MsSharePointPageLocator(), AffBody()):
                return await _read_sharepoint_page_body(handle, locator)
            case (MsSharePointListLocator(), AffBody()):
                return await _read_sharepoint_list_body(handle, locator)
            case (MsTeamsMessageLocator(), AffBody()):
                return await _read_teams_message_body(handle, locator)
            case _:
                raise UnavailableError.new()

    async def _get_authorization(self) -> str:
        """
        Get Microsoft authorization header.
        Works for any Microsoft product supported by Graph API.
        """
        if private_token := self.context.creds.get(str(self.realm)):
            return private_token
        if (
            (client_id := KnowledgeConfig.get(self.public_client_id))
            and (client_secret := KnowledgeConfig.get(self.public_client_secret))
            and (
                public_token := asyncio.to_thread(
                    _acquire_microsoft_access_token,
                    self.tenant_id,
                    client_id,
                    client_secret,
                )
            )
        ):
            return f"Bearer {public_token}"
        raise UnavailableError.new()


def _acquire_microsoft_access_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
) -> str | None:
    """
    NOTE: The access token expires after 60 minutes, and a Knowledge request may
    last up to 10 minutes before timeout.  Thus, refresh 50 minutes after the
    token was acquired.

    TODO: Scope the tokens using the `realm`, in case many Microsoft connectors
    were instantiated in the same request.
    """
    global _CACHED_MICROSOFT_TOKEN, _CACHED_MICROSOFT_TOKEN_EXPIRY  # noqa: PLW0603

    if (
        _CACHED_MICROSOFT_TOKEN
        and time.time() + TOKEN_EXPIRY_BUFFER < _CACHED_MICROSOFT_TOKEN_EXPIRY
    ):
        return _CACHED_MICROSOFT_TOKEN

    try:
        app = msal.ConfidentialClientApplication(
            client_id,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
            client_credential=client_secret,
        )

        result = app.acquire_token_silent(
            ["https://graph.microsoft.com/.default"], account=None
        )
        if not result:
            result = app.acquire_token_for_client(
                scopes=["https://graph.microsoft.com/.default"]
            )

        if isinstance(result, dict) and (access_token := result.get("access_token")):
            payload_base64 = access_token.split(".")[1] + "=="
            payload_dict = json.loads(base64.b64decode(payload_base64))
            _CACHED_MICROSOFT_TOKEN = access_token
            _CACHED_MICROSOFT_TOKEN_EXPIRY = payload_dict.get("exp")
            return access_token
        elif isinstance(result, dict):
            logger.error(
                "Failed to acquire Graph API access token: %s - %s",
                result.get("error"),
                result.get("error_description"),
            )
            return None
        else:
            logger.error("Failed to acquire Graph API access token: %s", result)
            return None
    except Exception:
        logger.exception("Failed to acquire Graph API access token.")
        return None


##
## OneDrive - File
##


async def _resolve_onedrive_file(
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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
    handle: "MsHandle",
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


def _format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    size = float(size_bytes)  # Convert to float for division
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024.0:  # noqa: PLR2004
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


##
## Read - SharePoint - File
##


async def _resolve_sharepoint_file(
    handle: "MsHandle",
    locator: MsSharePointFileLocator,
    cached: ResourceView | None,
) -> ResolveResult:
    """
    NOTE: If the request access token is unable to read the file, then this will
    raise an `UnavailableError`, checking that the user has access to the cached
    metadata and contents.
    """
    encoded_path = quote(str(locator.item_path).replace("'", "''"))
    file_info: dict[str, Any] = await handle.fetch_graph_api(
        f"v1.0/sites/{locator.site_id}/drive/root:/{encoded_path}"
    )

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
                ),
            )
        else:
            assert locator.item_kind == "folder"
            metadata = metadata.with_update(
                MetadataDelta(
                    affordances=[AffordanceInfo(suffix=AffCollection.new())],
                ),
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


async def _read_sharepoint_file_body(
    handle: "MsHandle",
    locator: MsSharePointFileLocator,
) -> ObserveResult:
    downloader = handle.context.service(SvcDownloader)

    if locator.item_kind != "file":
        raise UnavailableError.new()

    encoded_path = quote(str(locator.item_path).replace("'", "''"))
    file_info = await handle.fetch_graph_api(
        f"v1.0/sites/{locator.site_id}/drive/root:/{encoded_path}"
    )

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


async def _read_sharepoint_file_collection(
    handle: "MsHandle",
    locator: MsSharePointFileLocator,
) -> ObserveResult:
    if locator.item_kind != "folder":
        raise UnavailableError.new()

    children = await handle.fetch_sharepoint_children_by_id(
        locator.site_id, locator.item_id
    )
    if not children:
        raise UnavailableError.new()

    results: list[ResourceUri] = []
    for child in children:
        item_kind = _infer_item_kind(child)
        if not item_kind:
            continue

        child_locator = MsSharePointFileLocator(
            realm=locator.realm,
            domain=locator.domain,
            site_id=locator.site_id,
            site_name=locator.site_name,
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


async def _read_sharepoint_file_file(
    handle: "MsHandle",
    locator: MsSharePointFileLocator,
) -> ObserveResult:
    if locator.item_kind != "file":
        raise UnavailableError.new()

    encoded_path = quote(str(locator.item_path).replace("'", "''"))
    file_info = await handle.fetch_graph_api(
        f"v1.0/sites/{locator.site_id}/drive/root:/{encoded_path}"
    )

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
## Read - SharePoint - List
##


async def _resolve_sharepoint_list(
    handle: "MsHandle",
    locator: MsSharePointListLocator,
    cached: ResourceView | None,
) -> ResolveResult:
    """
    NOTE: If the request access token is unable to read the file, then this will
    raise an `UnavailableError`, checking that the user has access to the cached
    metadata and contents.
    """
    list_metadata: dict[str, Any] = await handle.fetch_graph_api(
        f"v1.0/sites/{locator.site_id}/lists/{locator.list_name}"
    )

    metadata = MetadataDelta(
        name=list_metadata.get("name", "Untitled"),
        created_at=(
            dateutil.parser.parse(created_dt)
            if (created_dt := list_metadata.get("createdDateTime"))
            else None
        ),
        updated_at=(
            dateutil.parser.parse(updated_dt)
            if (updated_dt := list_metadata.get("lastModifiedDateTime"))
            else None
        ),
        # TODO:
        # revision_data=list_metadata.get("cTag"),
        # revision_meta=list_metadata.get("eTag"),
    )

    if not cached:
        metadata = metadata.with_update(
            MetadataDelta(
                affordances=[AffordanceInfo(suffix=AffBody.new())],
            ),
        )

    return ResolveResult(metadata=metadata, expired=[], should_cache=True)


async def _read_sharepoint_list_body(
    handle: "MsHandle",
    locator: MsSharePointListLocator,
) -> ObserveResult:
    response = await handle.fetch_graph_api(
        f"v1.0/sites/{locator.site_id}/lists/{locator.list_id}/items"
        f"?expand=fields&$orderby=lastModifiedDateTime desc&$top=100",
        headers={"Accept": "application/json;odata.metadata=minimal"},
    )
    text = _parse_list_content(response)

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=text, blobs={}),
        should_cache=False,
        option_labels=False,
        option_relations_link=True,
    )


def _parse_list_content(response: dict[str, Any]) -> str:
    if not response.get("value"):
        return "This list is empty."

    content = []
    for list_item in response["value"][:100]:
        if parsed_item := _parse_list_item(list_item):
            content.extend(parsed_item)

    if not content:
        return "This list is empty."

    return markdown_from_msteams("\n\n".join(content))[0]


def _parse_list_item(list_item: dict[str, Any]) -> list[str]:
    """Parse a SharePoint list item."""
    content = []
    table_headers = ["Field", "Value"]
    table_data = []

    if title := list_item.get("fields", {}).get("Title"):
        content.append(f"## {title}")

    # Common metadata fields
    if value := list_item.get("fields", {}).get("EventDate"):
        table_data.append(("Event Date", value))
    if value := list_item.get("fields", {}).get("EndDate"):
        table_data.append(("End Date", value))
    if value := list_item.get("fields", {}).get("Location"):
        table_data.append(("Location", value))

    if description := list_item.get("fields", {}).get("Description"):
        content.append(description)

    if table_data:
        content.append(tabulate(table_data, table_headers, tablefmt="github"))

    if web_url := list_item.get("webUrl"):
        content.append(f"[Read more]({web_url})")

    return content


##
## Read - SharePoint - Page
##


async def _resolve_sharepoint_page(
    handle: "MsHandle",
    locator: MsSharePointPageLocator,
    cached: ResourceView | None,
) -> ResolveResult:
    """
    NOTE: If the request access token is unable to read the file, then this will
    raise an `UnavailableError`, checking that the user has access to the cached
    metadata and contents.
    """
    response: dict[str, Any] = await handle.fetch_graph_api(
        f"v1.0/sites/{locator.site_id}/pages/{locator.page_id}"
    )
    if not response.get("value"):
        raise UnavailableError.new()

    page_metadata = response["value"][0]

    metadata = MetadataDelta(
        name=page_metadata.get("title") or page_metadata.get("name", "Untitled"),
        created_at=(
            dateutil.parser.parse(created_dt)
            if (created_dt := page_metadata.get("createdDateTime"))
            else None
        ),
        updated_at=(
            dateutil.parser.parse(updated_dt)
            if (updated_dt := page_metadata.get("lastModifiedDateTime"))
            else None
        ),
        revision_data=page_metadata.get("cTag"),
        revision_meta=page_metadata.get("eTag"),
    )

    expired: list[Observable] = []
    if not cached:
        metadata = metadata.with_update(
            MetadataDelta(
                affordances=[AffordanceInfo(suffix=AffBody.new())],
            ),
        )
    elif (
        cached.metadata.revision_data
        and metadata.revision_data
        and cached.metadata.revision_data != metadata.revision_data
    ):
        expired.append(AffBody.new())

    return ResolveResult(metadata=metadata, expired=expired, should_cache=True)


async def _read_sharepoint_page_body(
    handle: "MsHandle",
    locator: MsSharePointPageLocator,
) -> ObserveResult:
    response = await handle.fetch_graph_api(
        f"v1.0/sites/{locator.site_id}/pages/{locator.page_id}/microsoft.graph.sitePage/webParts",
        headers={"Accept": "application/json;odata.metadata=minimal"},
    )
    if not response.get("value"):
        raise UnavailableError.new()

    # Process web parts to extract content
    content = []
    for webpart in response["value"]:
        if parsed_part := _parse_sitepage_part(webpart):
            content.extend(parsed_part)

    if not content:
        text = "This page is empty."
    else:
        # Convert to Markdown
        text, _ = markdown_from_msteams("\n\n".join(content))

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=text, blobs={}),
        should_cache=True,
        option_labels=False,
        option_relations_link=True,
    )


def _parse_sitepage_part(webpart: dict[str, Any]) -> list[str]:
    """Parse a SharePoint page web part."""
    if webpart["@odata.type"] == "#microsoft.graph.textWebPart" and (
        inner_html := webpart.get("innerHtml")
    ):
        return [inner_html]

    elif webpart["@odata.type"] == "#microsoft.graph.standardWebPart":
        data = webpart.get("data", {})
        server_content = data.get("serverProcessedContent", {})

        if data.get("isDecorative"):
            return []

        content: list[str] = []
        if html_strings := server_content.get("htmlStrings"):
            content.extend(
                html_strings.values()
                if isinstance(html_strings, dict)
                else html_strings
            )

        return content

    return []


##
## Read - Teams - Message
##


async def _resolve_teams_message(
    handle: "MsHandle",
    locator: MsTeamsMessageLocator,
    cached: ResourceView | None,
) -> ResolveResult:
    """
    NOTE: If the request access token is unable to read the file, then this will
    raise an `UnavailableError`, checking that the user has access to the cached
    metadata and contents.
    """
    conversation_data = await handle.fetch_teams_conversation(
        locator.group_id,
        locator.channel_id,
        locator.message_id,
    )
    if not conversation_data:
        raise UnavailableError.new()

    message_data = conversation_data[0]

    # Construct a title that is as informative as possible from the metadata.
    # These heuristics are especially relevant when the subject is missing.
    name = "Teams message"
    if message_from := message_data.get("from"):
        if message_sender := message_from.get("user"):  # noqa: SIM114
            name += f" from {message_sender.get('displayName')}"
        elif message_sender := message_from.get("application"):
            name += f" from {message_sender.get('displayName')}"
    if message_date := (message_data.get("createdDateTime") or "")[:10]:
        name += f" on {message_date}"
    if subject := message_data.get("subject"):
        name += f": {subject}"

    metadata = MetadataDelta(
        name=name,
        mime_type=MimeType.decode("text/html"),
        citation_url=WebUrl.try_decode(message_data.get("webUrl")),
        created_at=(
            dateutil.parser.parse(created_date)
            if (created_date := message_data.get("createdDateTime"))
            else None
        ),
        updated_at=(
            dateutil.parser.parse(modified_date)
            if (modified_date := message_data.get("lastModifiedDateTime"))
            else None
        ),
    )

    # Use the start of the message contents as the description.
    if body_content := message_data.get("body", {}).get("content"):
        text = html.unescape(body_content)
        text = re.sub(r"<[^>]+>", "", text).strip()
        metadata = metadata.with_update(
            MetadataDelta(description=shorten_description(text[:500]))
        )

    if not cached:
        metadata = metadata.with_update(
            MetadataDelta(
                affordances=[AffordanceInfo(suffix=AffBody.new())],
            )
        )

    return ResolveResult(metadata=metadata, expired=[], should_cache=False)


async def _read_teams_message_body(
    handle: "MsHandle",
    locator: MsTeamsMessageLocator,
) -> ObserveResult:
    conversation_data = await handle.fetch_teams_conversation(
        locator.group_id,
        locator.channel_id,
        locator.message_id,
    )
    if not conversation_data:
        raise UnavailableError.new()

    text = _teams_message_to_markdown(conversation_data)

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=text, blobs={}),
        should_cache=False,
        option_labels=False,
        option_relations_link=True,
    )


def _teams_message_to_markdown(  # noqa: C901, PLR0912
    conversation_data: list[dict[str, Any]],
) -> str:
    """Convert Teams message data to markdown format."""
    parts: list[str] = []

    if subject := conversation_data[0].get("subject"):
        parts.append(f"# {subject}\n")

    for index, message_data in enumerate(conversation_data):
        # Use the same convention for message separators as Datalab for pages.
        separator = (
            "{" + str(index) + "}------------------------------------------------\n"
        )
        parts.append(separator if index == 0 else f"\n{separator}")

        # Start the message with an email-style metadata header.
        if message_from := message_data.get("from"):
            if message_sender := message_from.get("user"):  # noqa: SIM114
                parts.append(f"From: {message_sender.get('displayName')}")
            elif message_sender := message_from.get("application"):
                parts.append(f"From: {message_sender.get('displayName')}")
        if created_date := message_data.get("createdDateTime"):
            parts.append(f"Sent at: {created_date}")
        if edited_date := message_data.get("lastEditedDateTime"):
            parts.append(f"Last edited: {edited_date}")
        if (importance := message_data.get("importance")) and importance != "normal":
            parts.append(f"Importance: {importance.capitalize()}")

        body_text: str = "*No content.*"
        if body_content := message_data.get("body", {}).get("content"):
            if message_data.get("body", {}).get("contentType") == "html":
                body_text, _ = markdown_from_msteams(body_content)
            else:
                body_text = strip_keep_indent(body_content)
        parts.append(f"\n{body_text}")

        # Attachments
        if attachments := message_data.get("attachments", []):
            parts.append("\n## Attachments\n")
            for attachment in attachments:
                name = attachment.get("name", "Unnamed")
                content_type = attachment.get("contentType", "unknown")
                parts.append(f"- **{name}** ({content_type})")

        # Reactions
        if reactions := message_data.get("reactions", []):
            parts.append("\n## Reactions\n")
            reaction_summary: dict[str, int] = {}
            for reaction in reactions:
                reaction_type = reaction.get("displayName", "unknown")
                reaction_summary.setdefault(reaction_type, 0)
                reaction_summary[reaction_type] += 1

            reaction_strs = [
                f"{rtype}: {count}" for rtype, count in reaction_summary.items()
            ]
            parts.append(", ".join(reaction_strs))

    return "\n".join(parts)


##
## Handle
##


GRAPH_API_LOCK = asyncio.Lock()
"""
There should only be one concurrent request to Graph API in parallel, to avoid
responses with "429 Too Many Requests".
"""


class MsSharePointSiteInfo(BaseModel):
    domain: str
    group_id: MsGroupId
    site_id: MsSiteId
    site_name: MsSiteName

    @staticmethod
    def from_data(data: dict[str, Any]) -> "MsSharePointSiteInfo | None":
        domain, site_str, group_str = data["id"].split(",")
        if (
            not (group_id := MsGroupId.try_decode(group_str))
            or not (site_id := MsSiteId.try_decode(site_str))
            or not (site_name := MsSiteName.try_decode(data["name"]))
        ):
            return None

        return MsSharePointSiteInfo(
            domain=domain,
            group_id=group_id,
            site_id=site_id,
            site_name=site_name,
        )

    def site_full_id(self) -> str:
        return f"{self.domain},{self.group_id},{self.site_id}"


@dataclass(kw_only=True)
class MsHandle:
    context: KnowledgeContext
    realm: Realm
    domain_sharepoint: str
    domain_onedrive: str
    authorization: str
    _cache_user_info: dict[str, dict[str, Any] | None]
    _cache_onedrive_children_by_id: dict[str, list[dict[str, Any]] | None]
    _cache_onedrive_info_by_id: dict[str, dict[str, Any] | None]
    _cache_onedrive_info_key_by_path: dict[str, str | None]
    _cache_outlook_attachment_by_id: dict[str, dict[str, Any] | None]
    _cache_sharepoint_children_by_id: dict[str, list[dict[str, Any]] | None]
    _cache_sharepoint_info_by_id: dict[str, dict[str, Any] | None]
    _cache_sharepoint_info_key_by_path: dict[str, str | None]
    _cache_sharepoint_site_by_id: dict[MsSiteId, MsSharePointSiteInfo | None]
    _cache_sharepoint_site_key_by_group: dict[MsGroupId, MsSiteId | None]
    _cache_sharepoint_site_key_by_name: dict[MsSiteName, MsSiteId | None]
    _cache_teams_conversation: dict[str, list[dict[str, Any]] | None]

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
        key = f"{user_id}/{item_id}"
        if key not in self._cache_onedrive_info_by_id:
            try:
                data = await self.fetch_graph_api(
                    f"v1.0/users/{user_id}/drive/items/{item_id}"
                )
                item_key = f"{user_id}/{data['id']}"
                alias_key_path = f"{user_id}/{_infer_item_path(data)}"
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
        key = f"{user_id}/{item_path}"
        if key not in self._cache_onedrive_info_key_by_path:
            data = None
            try:
                encoded_path = quote(str(item_path).replace("'", "''"))
                data = await self.fetch_graph_api(
                    f"v1.0/users/{user_id}/drive/items/root:/{encoded_path}"
                )
                item_key = f"{user_id}/{data['id']}"
                alias_key_path = f"{user_id}/{_infer_item_path(data)}"
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
        key = f"{user_id}/{item_path}"
        if key not in self._cache_onedrive_children_by_id:
            try:
                data = await self.fetch_graph_api(
                    f"v1.0/users/{user_id}/drive/items/{item_path}/children"
                )
                children = data.get("value", [])
                self._cache_onedrive_children_by_id[key] = children

                # Cache the children's info as well, for faster `resolve`.
                for child in children:
                    child_key = f"{user_id}/{child['id']}"
                    child_alias_key = f"{user_id}/{_infer_item_path(child)}"
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

    ##
    ## Helpers - SharePoint
    ##

    async def fetch_sharepoint_site_by_id(
        self,
        site_id: MsSiteId,
    ) -> MsSharePointSiteInfo | None:
        if site_id not in self._cache_sharepoint_site_by_id:
            site_info: MsSharePointSiteInfo | None = None
            with contextlib.suppress(Exception):
                data = await self.fetch_graph_api(f"v1.0/sites/{site_id}")
                site_info = MsSharePointSiteInfo.from_data(data)

            if site_info:
                site_key = site_info.site_id
                self._cache_sharepoint_site_by_id[site_info.site_id] = site_info
                self._cache_sharepoint_site_key_by_group[site_info.group_id] = site_key
                self._cache_sharepoint_site_key_by_name[site_info.site_name] = site_key
            else:
                self._cache_sharepoint_site_by_id[site_id] = None

        return self._cache_sharepoint_site_by_id[site_id]

    async def fetch_sharepoint_site_by_name(
        self,
        site_name: MsSiteName,
    ) -> MsSharePointSiteInfo | None:
        if site_name not in self._cache_sharepoint_site_key_by_name:
            site_info: MsSharePointSiteInfo | None = None
            with contextlib.suppress(Exception):
                data = await self.fetch_graph_api(
                    f"v1.0/sites/{self.domain_sharepoint}:/sites/{site_name}"
                )
                site_info = MsSharePointSiteInfo.from_data(data)

            if site_info:
                site_id = site_info.site_id
                self._cache_sharepoint_site_by_id[site_info.site_id] = site_info
                self._cache_sharepoint_site_key_by_group[site_info.group_id] = site_id
                self._cache_sharepoint_site_key_by_name[site_info.site_name] = site_id
            else:
                self._cache_sharepoint_site_key_by_name[site_name] = None

        return self._cache_sharepoint_site_by_id.get(
            self._cache_sharepoint_site_key_by_name[site_name]  # type: ignore
        )

    async def fetch_sharepoint_info_by_id(
        self,
        site_id: MsSiteId,
        item_id: MsDriveItemId,
    ) -> dict[str, Any] | None:
        key = f"{site_id}/{item_id}"
        if key not in self._cache_sharepoint_info_by_id:
            try:
                data = await self.fetch_graph_api(
                    f"v1.0/sites/{site_id}/drive/items/{item_id}"
                )
                item_key = f"{site_id}/{data['id']}"
                alias_key_path = f"{site_id}/{_infer_item_path(data)}"
                self._cache_sharepoint_info_by_id[item_key] = data
                self._cache_sharepoint_info_key_by_path[alias_key_path] = item_key
            except Exception:
                self._cache_sharepoint_info_by_id[key] = None

        return self._cache_sharepoint_info_by_id[key]

    async def fetch_sharepoint_info_by_path(
        self,
        site_id: MsSiteId,
        item_path: str,
    ) -> dict[str, Any]:
        key = f"{site_id}/{item_path}"
        if key not in self._cache_sharepoint_info_key_by_path:
            data = None
            try:
                encoded_path = quote(item_path.replace("'", "''"))
                data = await self.fetch_graph_api(
                    f"v1.0/sites/{site_id}/drive/root:/{encoded_path}"
                )
                item_key = f"{site_id}/{data['id']}"
                alias_key_path = f"{site_id}/{_infer_item_path(data)}"
                self._cache_sharepoint_info_by_id[item_key] = data
                self._cache_sharepoint_info_key_by_path[alias_key_path] = item_key
            except Exception:
                self._cache_sharepoint_info_key_by_path[key] = None

        return self._cache_sharepoint_info_by_id.get(
            self._cache_sharepoint_info_key_by_path[key]  # type: ignore
        )

    async def fetch_sharepoint_children_by_id(
        self,
        site_id: MsSiteId,
        item_id: MsDriveItemId,
    ) -> list[dict[str, Any]] | None:
        key = f"{site_id}/{item_id}"
        if key not in self._cache_sharepoint_children_by_id:
            try:
                data = await self.fetch_graph_api(
                    f"v1.0/sites/{site_id}/drive/items/{item_id}/children"
                )
                children = data.get("value", [])
                self._cache_sharepoint_children_by_id[key] = children

                # Cache the children's info as well, for faster `resolve`.
                for child in children:
                    child_key = f"{site_id}/{child['id']}"
                    child_alias_key = f"{site_id}/{_infer_item_path(child)}"
                    self._cache_sharepoint_info_by_id[child_key] = child
                    self._cache_sharepoint_info_key_by_path[child_alias_key] = child_key
            except Exception:
                self._cache_sharepoint_children_by_id[key] = None

        return self._cache_sharepoint_children_by_id.get(key)

    ##
    ## Helpers - SharePoint
    ##

    async def fetch_teams_conversation(
        self,
        group_id: MsGroupId,
        channel_id: MsChannelId,
        message_id: FileName,
    ) -> list[dict[str, Any]] | None:
        key = f"{group_id}/"
        if key not in self._cache_teams_conversation:
            try:
                message_data: dict[str, Any] = await self.fetch_graph_api(
                    f"v1.0/teams/{group_id}/channels/{channel_id}/messages/{message_id}",
                    headers={"Accept": "application/json"},
                )

                replies: list[dict[str, Any]] = []
                with contextlib.suppress(Exception):
                    replies_data = await self.fetch_graph_api(
                        f"v1.0/teams/{group_id}/channels/{channel_id}/messages/{message_id}/replies",
                        headers={"Accept": "application/json"},
                    )
                    replies = replies_data.get("value", [])

                self._cache_teams_conversation[key] = [
                    message
                    for message in [message_data, *replies]
                    if message.get("messageType") == "message"
                ]
            except Exception:
                self._cache_teams_conversation[key] = None
        return self._cache_teams_conversation[key]


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
