import asyncio
import dateutil.parser
import json
import logging
import re
import weakref

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any, Literal
from urllib.parse import quote_plus, unquote_plus

from base.api.documents import Fragment
from base.resources.aff_body import AffBody
from base.core.exceptions import BadRequestError, UnavailableError
from base.resources.metadata import AffordanceInfo
from base.resources.relation import Relation, RelationParent
from base.strings.data import MimeType
from base.strings.file import FileName
from base.strings.resource import (
    ExternalUri,
    Observable,
    Realm,
    ResourceUri,
    RootReference,
    WebUrl,
)

from base.utils.sorted_list import bisect_insert
from knowledge.config import KnowledgeConfig
from knowledge.domain.storage import read_connector_data, save_connector_data
from knowledge.models.storage_metadata import Locator, MetadataDelta, ResourceView
from knowledge.server.context import (
    Connector,
    KnowledgeContext,
    ObserveResult,
    ResolveResult,
)
from knowledge.services.downloader import SvcDownloader

logger = logging.getLogger(__name__)

REGEX_URL_PAGE_PRETTY = r"display/([A-Z]+)/([A-Za-z0-9_\-+]+)"
REGEX_URL_BLOG = r"display/([A-Z]+)/([0-9]{4}/[0-9]{2}/[0-9]{2}/(?:[A-Za-z0-9_\-+]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)"

PAGINATION_LIMIT = 100
REFRESH_PARALLEL_SPACES = 10


##
## Config
##


SpaceKey = FileName
PageId = FileName
BlogDate = FileName


class ConfluenceConnectorConfig(BaseModel, frozen=True):
    kind: Literal["confluence"] = "confluence"
    realm: Realm
    domain: str
    public_token: str | None

    def instantiate(self, context: KnowledgeContext) -> ConfluenceConnector:
        return ConfluenceConnector(
            context=weakref.proxy(context),
            realm=self.realm,
            domain=self.domain,
            public_token=self.public_token,
        )


class ConfluenceRefreshCache(BaseModel):
    spaces: dict[SpaceKey, dict[PageId, int]]
    """
    Mapping space key -> page ID -> version number.
    """


##
## Locators
##


class ConfluencePageLocator(Locator, frozen=True):
    kind: Literal["confluence_page"] = "confluence_page"
    realm: Realm
    domain: str
    space_key: SpaceKey
    page_id: PageId

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("page"),
            path=[self.space_key, self.page_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/pages/viewpage.action?pageId={self.page_id}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class ConfluenceBlogLocator(Locator, frozen=True):
    kind: Literal["confluence_blog"] = "confluence_blog"
    realm: Realm
    domain: str
    space_key: SpaceKey
    posting_day: BlogDate
    """
    Expected: "YYYY-MM-DD"
    """
    page_id: PageId

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("blog"),
            path=[self.space_key, self.posting_day, self.page_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/pages/viewpage.action?pageId={self.page_id}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


AnyConfluenceLocator = ConfluencePageLocator | ConfluenceBlogLocator


##
## Connector
##


@dataclass(kw_only=True)
class ConfluenceConnector(Connector):
    """
    API documentation for Confluence 7.19.4:
    https://docs.atlassian.com/ConfluenceServer/rest/7.19.4/
    """

    domain: str
    public_token: str | None
    _handle: ConfluenceHandle | None = None

    async def _acquire_handle(self) -> ConfluenceHandle:
        if self._handle is None:
            authorization, public_token = self._get_authorization()
            self._handle = ConfluenceHandle(
                context=self.context,
                realm=self.realm,
                domain=self.domain,
                mode="internal" if public_token else "private",
                _authorization=authorization,
                _cache_content={},
                _cache_spaces=None,
            )
        return self._handle

    async def locator(  # noqa: C901, PLR0911
        self,
        reference: RootReference,
    ) -> Locator | None:
        handle = await self._acquire_handle()

        if isinstance(reference, WebUrl):
            if reference.domain != self.domain:
                return None

            # 1) Page by ID: "/pages/viewpage.action?pageId=NNN"
            if (
                reference.path == "pages/viewpage.action"
                and (page_id := PageId.try_decode(reference.get_query("pageId")))
                and (loc := await handle.find_page_locator_by_id(page_id))
            ):
                return loc

            # 2) Pretty page URL: "/display/{SPACE}/{Title}"
            if (
                (match := re.fullmatch(REGEX_URL_PAGE_PRETTY, reference.path))
                and (space_key := SpaceKey.try_decode(match.group(1)))
                and (title := unquote_plus(match.group(2).replace("+", " ")))
                and (loc := await handle.find_page_locator_by_title(space_key, title))
            ):
                return loc

            # 3) Pretty blog URL: "/display/{SPACE}/{YYYY}/{MM}/{DD}/{Title}"
            if (match := re.fullmatch(REGEX_URL_BLOG, reference.path)) and (
                space_key := SpaceKey.try_decode(match.group(1))
            ):
                path = match.group(2)
                posting_day = path.rsplit("/", maxsplit=1)[0].replace("/", "-")
                title = unquote_plus(path.split("/", maxsplit=4)[-1].replace("+", " "))
                if loc := await handle.find_blog_locator_by_title(
                    space_key, posting_day, title
                ):
                    return loc

            raise UnavailableError.new()

        elif isinstance(reference, ExternalUri):
            return None

        else:  # ResourceUri
            if reference.realm != self.realm:
                return None

            if (
                reference.subrealm == "page" and len(reference.path) == 2  # noqa: PLR2004
            ):
                return ConfluencePageLocator(
                    realm=self.realm,
                    domain=self.domain,
                    space_key=reference.path[0],
                    page_id=reference.path[1],
                )

            if (
                reference.subrealm == "blog" and len(reference.path) == 3  # noqa: PLR2004
            ):
                return ConfluenceBlogLocator(
                    realm=self.realm,
                    domain=self.domain,
                    space_key=reference.path[0],
                    posting_day=reference.path[1],
                    page_id=reference.path[2],
                )

            return None

    async def refresh(self) -> list[Locator]:
        handle = await self._acquire_handle()

        cache = await read_connector_data(
            self.context,
            self.realm,
            "versions",
            ConfluenceRefreshCache,
        )
        if not cache:
            cache = ConfluenceRefreshCache(spaces={})

        changed_locators: list[Locator] = []
        space_keys = await handle.list_spaces()
        for start_index in range(0, len(space_keys), REFRESH_PARALLEL_SPACES):
            batch = space_keys[start_index : start_index + REFRESH_PARALLEL_SPACES]
            tasks = [
                self._refresh_space(handle, cache, space_key) for space_key in batch
            ]
            batch_results = await asyncio.gather(*tasks)
            for batch_result in batch_results:
                changed_locators.extend(batch_result)

        if changed_locators:
            await save_connector_data(self.context, self.realm, "versions", cache)

        return changed_locators

    async def _refresh_space(
        self,
        handle: ConfluenceHandle,
        cache: ConfluenceRefreshCache,
        space_key: SpaceKey,
    ) -> list[Locator]:
        cache_space = cache.spaces.setdefault(space_key, {})
        changed_locators: list[Locator] = []
        async for locator, version in handle.list_changes(space_key):
            if version > cache_space.get(locator.page_id, 0):
                changed_locators.append(locator)
                cache_space[locator.page_id] = version
        return changed_locators

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        """
        TODO: Use gatekeeper to check access when using the public token.
        """
        assert isinstance(locator, AnyConfluenceLocator)
        handle = await self._acquire_handle()

        # Fetch content via REST API to validate access and infer metadata.
        metadata = await handle.fetch_page_metadata(locator)

        if not cached:
            metadata = metadata.with_update(
                MetadataDelta(affordances=[AffordanceInfo(suffix=AffBody.new())])
            )

        expired = (
            cached
            and metadata.revision_data
            and cached.metadata.revision_data != metadata.revision_data
        )

        return ResolveResult(
            metadata=metadata,
            expired=[AffBody.new()] if expired else [],
            should_cache=True,
        )

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyConfluenceLocator)

        if observable != AffBody.new():
            raise BadRequestError.observable(observable.as_suffix())

        handle = await self._acquire_handle()
        fragment, _ = await handle.download_page(locator.page_id)

        return ObserveResult(
            bundle=fragment,
            should_cache=True,
            option_labels=True,
            option_relations_link=True,
        )

    def _get_authorization(self) -> tuple[str, bool]:
        return self.context.bearer_authorization(self.realm, self.public_token)


##
## Handle
##


@dataclass(kw_only=True)
class ConfluenceHandle:
    context: KnowledgeContext
    realm: Realm
    domain: str
    mode: Literal["internal", "private"]
    _authorization: str
    _cache_content: dict[PageId, dict[str, Any]]
    _cache_spaces: list[SpaceKey] | None

    def _endpoint(self, path: str, query: str | None = None) -> WebUrl:
        query_suffix = f"?{query}" if query else ""
        return WebUrl.decode(f"https://{self.domain}/{path}{query_suffix}")

    async def _fetch_endpoint_json(self, path: str, query: str | None = None) -> Any:
        downloader = self.context.service(SvcDownloader)
        url = self._endpoint(path=path, query=query)
        try:
            response = await downloader.documents_read_download(
                url=url,
                authorization=self._authorization,
                headers={"accept": "application/json"},
                original=True,
            )
            return json.loads(response.text)
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("Confluence.fetch_endpoint_json failed: %s", str(url))
            raise UnavailableError.new()  # noqa: B904

    async def _fetch_page(self, page_id: PageId) -> dict[str, Any]:
        if page_id not in self._cache_content:
            self._cache_content[page_id] = await self._fetch_endpoint_json(
                path=f"rest/api/content/{page_id}",
                query="expand=ancestors,history,space,version",
            )
        return self._cache_content[page_id]

    async def find_page_locator_by_id(
        self,
        page_id: PageId,
    ) -> ConfluencePageLocator | ConfluenceBlogLocator | None:
        try:
            data = await self._fetch_page(page_id)
        except UnavailableError:
            return None

        if data.get("type") == "blogpost":
            # Try to infer the posting day from the path; fallback to the created date.
            posting_day: str
            if re.fullmatch(REGEX_URL_BLOG, data.get("_links", {}).get("webui", "")):
                year, month, day = data["_links"]["webui"].split("/")[3:-1]
                posting_day = f"{year}-{month}-{day}"
            else:
                year, month, day = (
                    data["history"]["createdDate"].split("T")[0].split("-")
                )
                posting_day = f"{year}-{month}-{day}"

            return ConfluenceBlogLocator(
                realm=self.realm,
                domain=self.domain,
                space_key=SpaceKey.decode(data["space"]["key"]),
                posting_day=BlogDate.decode(posting_day),
                page_id=PageId.decode(data["id"]),
            )
        else:
            return ConfluencePageLocator(
                realm=self.realm,
                domain=self.domain,
                space_key=SpaceKey.decode(data["space"]["key"]),
                page_id=PageId.decode(data["id"]),
            )

    async def find_page_locator_by_title(
        self,
        space_key: SpaceKey,
        title: str,
    ) -> ConfluencePageLocator | None:
        data = await self._fetch_endpoint_json(
            path="rest/api/content",
            query=f"spaceKey={space_key}&title={quote_plus(title)}",
        )
        results: list[dict[str, Any]] = data.get("results", [])
        if not results:
            return None
        return ConfluencePageLocator(
            realm=self.realm,
            domain=self.domain,
            space_key=space_key,
            page_id=PageId.decode(results[0]["id"]),
        )

    async def find_blog_locator_by_title(
        self,
        space_key: SpaceKey,
        posting_day: str,
        title: str,
    ) -> ConfluenceBlogLocator | None:
        data = await self._fetch_endpoint_json(
            path="rest/api/content",
            query=(
                f"spaceKey={space_key}&title={quote_plus(title)}&postingDay={posting_day}&type=blogpost"
            ),
        )
        results: list[dict[str, Any]] = data.get("results", [])
        if not results:
            return None
        return ConfluenceBlogLocator(
            realm=self.realm,
            domain=self.domain,
            space_key=space_key,
            posting_day=BlogDate.decode(posting_day),
            page_id=PageId.decode(results[0]["id"]),
        )

    async def fetch_page_metadata(
        self,
        locator: AnyConfluenceLocator,
    ) -> MetadataDelta:
        data: dict[str, Any] = await self._fetch_page(locator.page_id)

        relations: list[Relation] = []
        if (
            (ancestors := data.get("ancestors", []))
            and (ancestor_str := ancestors[-1].get("id"))
            and (ancestor_id := PageId.decode(ancestor_str))
            and ancestor_id != locator.page_id
            and (ancestor_locator := await self.find_page_locator_by_id(ancestor_id))
        ):
            relations.append(
                RelationParent(
                    parent=ancestor_locator.resource_uri(),
                    child=locator.resource_uri(),
                )
            )

        return MetadataDelta(
            name=data.get("title") or f"Confluence page {locator.page_id}",
            citation_url=(
                WebUrl.decode(f"https://{self.domain}{clean_path}")
                if (clean_path := data.get("_links", {}).get("webui"))
                and clean_path.startswith("/display/")
                else None
            ),
            created_at=(
                dateutil.parser.parse(created_str)
                if (created_str := data.get("history", {}).get("createdDate"))
                else None
            ),
            updated_at=(
                dateutil.parser.parse(updated_str)
                if (updated_str := data.get("version", {}).get("when"))
                else None
            ),
            revision_data=(
                str(version_num)
                if (version_num := data.get("version", {}).get("number")) is not None
                else None
            ),
            relations=relations,
        )

    async def download_page(self, page_id: PageId) -> tuple[Fragment, MimeType]:
        downloader = self.context.service(SvcDownloader)
        url = self._endpoint("pages/viewpage.action", query=f"pageId={page_id}")
        response = await downloader.documents_read_download(
            url=url,
            authorization=self._authorization,
            original=False,
        )
        return response.as_fragment(), response.mime_type

    async def list_spaces(self) -> list[SpaceKey]:
        # TODO: Pagination
        if self._cache_spaces is None:
            self._cache_spaces = []
            async for space_key in self._list_spaces():
                bisect_insert(self._cache_spaces, space_key, key=id)

        return self._cache_spaces.copy()

    async def _list_spaces(self) -> AsyncGenerator[SpaceKey]:
        start: int = 0
        while True:
            response = await self._fetch_endpoint_json(
                path="rest/api/space",
                query=f"limit={PAGINATION_LIMIT}&start={start}",
            )
            start += PAGINATION_LIMIT
            if not response.get("results"):
                break

            for space in response["results"]:
                if space["type"] == "global" and (
                    space_key := SpaceKey.try_decode(space.get("key"))
                ):
                    yield space_key

    async def list_changes(
        self,
        space_key: SpaceKey,
    ) -> AsyncGenerator[tuple[AnyConfluenceLocator, int]]:
        regex_blog_link = r"/display/[A-Z]+/([0-9]{4}/[0-9]{2}/[0-9]{2})/.+"
        start: int = 0
        while True:
            response = await self._fetch_endpoint_json(
                path=f"rest/api/space/{space_key}/content",
                query=f"expand=version&limit={PAGINATION_LIMIT}&start={start}",
            )
            start += PAGINATION_LIMIT
            page_results = response.get("page", {}).get("results") or []
            blog_results = response.get("blogpost", {}).get("results") or []
            if not page_results and not blog_results:
                break

            for result in page_results:
                locator = ConfluencePageLocator(
                    realm=self.realm,
                    domain=self.domain,
                    space_key=space_key,
                    page_id=PageId.decode(result["id"]),
                )
                yield locator, result["version"]["number"]

            for result in blog_results:
                if match := re.fullmatch(regex_blog_link, result["_links"]["webui"]):
                    posting_day = match.group(1).replace("/", "-")
                    locator = ConfluenceBlogLocator(
                        realm=self.realm,
                        domain=self.domain,
                        space_key=space_key,
                        posting_day=BlogDate.decode(posting_day),
                        page_id=PageId.decode(result["id"]),
                    )
                    yield locator, result["version"]["number"]
