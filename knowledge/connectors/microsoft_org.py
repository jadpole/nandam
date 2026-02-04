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
from pydantic import BaseModel, Field
from tabulate import tabulate
from typing import Any, Literal
from urllib.parse import quote, unquote, unquote_plus

from base.api.documents import Fragment
from base.core.exceptions import UnavailableError
from base.resources.aff_body import AffBody
from base.resources.aff_collection import AffCollection
from base.resources.aff_file import AffFile
from base.resources.metadata import AffordanceInfo
from base.strings.data import MimeType
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
from base.utils.sorted_list import bisect_extend

from knowledge.config import KnowledgeConfig
from knowledge.domain.resolve import try_resolve_locator
from knowledge.domain.storage import read_connector_data, save_connector_data
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

ItemKind = Literal["file", "folder"]


##
## Config
##


class MicrosoftOrgConnectorConfig(BaseModel, frozen=True):
    kind: Literal["microsoft-org"] = "microsoft-org"
    realm: Realm
    domain: str
    tenant_id: str
    public_client_id: str | None = None
    public_client_secret: str | None = None
    internal_site_ids: dict[MsSiteId, list[str]] = Field(default_factory=dict)
    refresh_site_ids: list[MsSiteId] = Field(default_factory=list)

    def instantiate(self, context: KnowledgeContext) -> "MicrosoftOrgConnector":
        return MicrosoftOrgConnector(
            context=context,
            realm=self.realm,
            domain=self.domain,
            tenant_id=self.tenant_id,
            public_client_id=self.public_client_id,
            public_client_secret=self.public_client_secret,
            internal_site_ids=self.internal_site_ids,
            refresh_site_ids=self.refresh_site_ids,
        )


class MicrosoftOrgState(BaseModel):
    delta_urls: dict[MsSiteId, WebUrl]
    """
    Mapping SharePoint site ID -> Graph API delta token.
    """


##
## Locator - SharePoint
##


class MsSharePointFileLocator(Locator, frozen=True):
    kind: Literal["ms_sharepoint_file"] = "ms_sharepoint_file"
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
            url.domain == handle.domain
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
                domain=handle.domain,
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
                domain=handle.domain,
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
    kind: Literal["ms_sharepoint_list"] = "ms_sharepoint_list"
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
            url.domain == handle.domain
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
                domain=handle.domain,
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
                domain=handle.domain,
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
    kind: Literal["ms_sharepoint_page"] = "ms_sharepoint_page"
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
            url.domain == handle.domain
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
                domain=handle.domain,
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
                domain=handle.domain,
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


AnyMsOrgLocator = (
    MsSharePointFileLocator
    | MsSharePointListLocator
    | MsSharePointPageLocator
    | MsTeamsMessageLocator
)


##
## Connector
##


@dataclass(kw_only=True)
class MicrosoftOrgConnector(Connector):
    domain: str
    tenant_id: str
    public_client_id: str | None
    public_client_secret: str | None
    internal_site_ids: dict[MsSiteId, list[str]]
    refresh_site_ids: list[MsSiteId]
    _handle: "MsHandle | None" = None

    async def _acquire_handle(self) -> "MsHandle":
        if self._handle is None:
            self._handle = MsHandle(
                context=self.context,
                realm=self.realm,
                domain=self.domain,
                authorization=await self._get_authorization(),
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
                self.domain,
            ):
                return None

            handle = await self._acquire_handle()
            locator = (
                await MsSharePointFileLocator.from_web(handle, reference)
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
                await MsSharePointFileLocator.from_uri(handle, reference)
                or await MsSharePointListLocator.from_uri(handle, reference)
                or await MsSharePointPageLocator.from_uri(handle, reference)
                or await MsTeamsMessageLocator.from_uri(handle, reference)
            )

    async def refresh(self) -> list[Locator]:
        """
        NOTE: Refreshes only shared resources: SharePoint sites, Teams groups.
        TODO: Refresh sharepoint pages.
        TODO: Refresh Teams messages.
        """
        handle = await self._acquire_handle()

        cache = await read_connector_data(
            self.context,
            self.realm,
            "state",
            MicrosoftOrgState,
        )
        if not cache:
            cache = MicrosoftOrgState(delta_urls={})

        changed_locators: list[Locator] = []

        for site_id in self.refresh_site_ids:
            old_delta_url = cache.delta_urls.get(site_id)
            new_delta_url, site_locators = await self._refresh_sharepoint_files(
                handle, old_delta_url, site_id
            )
            bisect_extend(
                changed_locators,
                site_locators,
                key=lambda loc: loc.resource_uri(),
            )
            if new_delta_url:
                cache.delta_urls[site_id] = new_delta_url

        if changed_locators:
            await save_connector_data(self.context, self.realm, "delta", cache)

        return changed_locators

    async def _refresh_sharepoint_files(
        self,
        handle: "MsHandle",
        delta_url: WebUrl | None,
        site_id: MsSiteId,
    ) -> tuple[WebUrl | None, list[MsSharePointFileLocator]]:
        new_delta_url = None
        changed_locators: list[MsSharePointFileLocator] = []

        next_link: str | None = (
            str(delta_url).removeprefix("https://graph.microsoft.com/")
            if delta_url
            else f"v1.0/sites/{site_id}/drive/root/delta"
        )
        while next_link:
            try:
                response = await handle.fetch_graph_api(next_link)
            except Exception:
                break  # If an error occurs, retry next time.

            for value in response["value"]:
                if loc := await self._parse_sharepoint_delta(handle, site_id, value):
                    changed_locators.append(loc)  # noqa: PERF401

            next_link = response.get("@odata.nextLink", None)
            if next_link:
                next_link = next_link.removeprefix("https://graph.microsoft.com/")
            if new_delta := response.get("@odata.deltaLink", None):
                new_delta_url = WebUrl.try_decode(new_delta)

        # NOTE: Either there were no changes or the access token is not allowed
        # to view them.  Simply retry using the previous delta URL next time.
        if not changed_locators:
            return None, []

        return new_delta_url, changed_locators

    async def _refresh_sharepoint_pages(
        self,
        handle: "MsHandle",
        delta_url: WebUrl | None,
        site_id: MsSiteId,
    ) -> list[MsSharePointPageLocator]:
        return []  # TODO

    async def _parse_sharepoint_delta(
        self,
        handle: "MsHandle",
        site_id: MsSiteId,
        value: dict[str, Any],
    ) -> MsSharePointFileLocator | None:
        if (
            value.get("file")
            and (item_id := MsDriveItemId.try_decode(value.get("id")))
            and (site_info := await handle.fetch_sharepoint_site_by_id(site_id))
        ):
            return MsSharePointFileLocator(
                realm=self.realm,
                domain=self.domain,
                site_id=site_id,
                site_name=site_info.site_name,
                item_id=item_id,
                item_kind="file",
                item_path=_infer_item_path(value),
            )
        else:
            return None

    async def resolve(  # noqa: C901
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        """
        TODO: Teams Group ID <-> SharePoint Site ID mapping
        TODO: Resolve TeamsMessageLocator as internal sites.
        """
        assert isinstance(locator, AnyMsOrgLocator)
        try:
            handle = await self._acquire_handle()
        except UnavailableError:
            # When no access token is available, but the resource is cached...
            if isinstance(locator, MsTeamsMessageLocator):
                raise  # TODO

            # SharePoint lists and pages are accessible when they belong to an
            # internal site.
            elif isinstance(locator, MsSharePointListLocator | MsSharePointPageLocator):
                if locator.site_id not in self.internal_site_ids:
                    raise
                return ResolveResult()

            # SharePoint files are accessible when they belong to an internal site
            # and the path matches an internal folder.
            else:
                if locator.site_id not in self.internal_site_ids:
                    raise
                internal_folders = self.internal_site_ids.get(locator.site_id, [])
                if internal_folders is not None and not any(
                    locator.item_path == f or locator.item_path.startswith(f"{f}/")
                    for f in internal_folders
                ):
                    raise
                return ResolveResult()

        match locator:
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

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyMsOrgLocator)
        handle = await self._acquire_handle()

        match (locator, observable):
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
        if debug_token := KnowledgeConfig.get("DEBUG_MICROSOFT_ACCESS_TOKEN"):
            return (
                debug_token
                if debug_token.startswith("Bearer ")
                else f"Bearer {debug_token}"
            )
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


TOKEN_EXPIRY_BUFFER_SECS: int = 600
_CACHED_MICROSOFT_TOKEN: str | None = None
_CACHED_MICROSOFT_TOKEN_EXPIRY: int = 0


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
        and time.time() + TOKEN_EXPIRY_BUFFER_SECS < _CACHED_MICROSOFT_TOKEN_EXPIRY
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
    domain: str
    authorization: str
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
                    f"v1.0/sites/{self.domain}:/sites/{site_name}"
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
