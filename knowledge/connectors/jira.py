import contextlib
import dateutil.parser
import json
import logging
import re

from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from pydantic import BaseModel
from tabulate import tabulate
from typing import Any, Literal, Never
from urllib.parse import quote

from base.api.documents import Fragment
from base.core.exceptions import UnavailableError
from base.core.unique_id import unique_id_from_str
from base.resources.aff_body import AffBody
from base.resources.aff_collection import AffCollection
from base.resources.metadata import AffordanceInfo
from base.resources.relation import Relation, RelationMisc, RelationParent
from base.strings.data import MimeType
from base.strings.file import FileName, FilePath
from base.strings.resource import (
    ExternalUri,
    Observable,
    Realm,
    ResourceUri,
    RootReference,
    WebUrl,
)

from knowledge.config import KnowledgeConfig
from knowledge.domain.resolve import try_infer_locator
from knowledge.models.utils import shorten_description
from knowledge.models.storage_metadata import Locator, MetadataDelta, ResourceView
from knowledge.models.storage_observed import BundleCollection
from knowledge.server.context import (
    Connector,
    KnowledgeContext,
    ObserveResult,
    ResolveResult,
)
from knowledge.services.downloader import SvcDownloader

logger = logging.getLogger(__name__)

REGEX_ISSUE_KEY = r"[A-Za-z]+-\d+"
REGEX_URL_ISSUE = r"browse/([A-Za-z]+-\d+)"
REGEX_URL_BOARD = r"(?:jira/software/.*/)?projects/([A-Za-z]+)/boards/(\d+)"

IssueKey = FileName
"""
Format: "AAA-1234".
"""


##
## Config
##


class JiraConnectorConfig(BaseModel):
    kind: Literal["jira"] = "jira"
    realm: Realm
    domain: str
    public_username: str | None = None
    public_token: str | None = None

    def instantiate(self, context: KnowledgeContext) -> "JiraConnector":
        return JiraConnector(
            context=context,
            realm=self.realm,
            domain=self.domain,
            public_username=self.public_username,
            public_token=self.public_token,
        )


##
## Locators
##


class JiraIssueLocator(Locator, frozen=True):
    kind: Literal["jira_issue"] = "jira_issue"
    realm: Realm
    domain: str
    project_key: FileName
    issue_id: FileName

    @staticmethod
    def from_issue_key(
        realm: Realm,
        domain: str,
        issue_key: str,
    ) -> "JiraIssueLocator | None":
        project_str, issue_str = issue_key.rsplit("-", maxsplit=1)
        if (
            not (project_key := FileName.try_decode(project_str.upper()))
            or not issue_str.isdigit()
            or not (issue_id := FileName.try_decode(issue_str))
        ):
            return None
        return JiraIssueLocator(
            realm=realm,
            domain=domain,
            project_key=project_key,
            issue_id=issue_id,
        )

    def issue_key(self) -> IssueKey:
        return IssueKey.decode(f"{self.project_key}-{self.issue_id}")

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("issue"),
            path=[self.issue_key()],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/browse/{self.project_key}-{self.issue_id}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class JiraBoardLocator(Locator, frozen=True):
    kind: Literal["jira_board"] = "jira_board"
    realm: Realm
    domain: str
    project_key: FileName
    board_id: FileName

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("board"),
            path=[self.project_key, self.board_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/jira/software/c/projects/{self.project_key}/boards/{self.board_id}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class JiraFilterLocator(Locator, frozen=True):
    kind: Literal["jira_filter"] = "jira_filter"
    realm: Realm
    domain: str
    filter_id: FileName

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("filter"),
            path=[self.filter_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(f"https://{self.domain}/issues/?filter={self.filter_id}")

    def citation_url(self) -> WebUrl:
        return self.content_url()


class JiraSearchLocator(Locator, frozen=True):
    kind: Literal["jira_search"] = "jira_search"
    realm: Realm
    domain: str
    jql: str

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("search"),
            path=[FileName.decode(_hash_jql_for_uri(self.jql, self.realm))],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(f"https://{self.domain}/issues/?jql={quote(self.jql)}")

    def citation_url(self) -> WebUrl:
        return self.content_url()


AnyJiraLocator = (
    JiraIssueLocator | JiraBoardLocator | JiraFilterLocator | JiraSearchLocator
)


def _hash_jql_for_uri(jql: str, realm: Realm) -> FilePath:
    hashed = unique_id_from_str(jql, num_chars=40, salt=str(realm))
    return FilePath.decode(hashed)


##
## Connector
##


@dataclass(kw_only=True)
class JiraConnector(Connector):
    domain: str
    public_username: str | None = None
    public_token: str | None = None
    _handle: "JiraHandle | None" = None

    async def _acquire_handle(self) -> "JiraHandle":
        if self._handle is None:
            self._handle = JiraHandle(
                context=self.context,
                realm=self.realm,
                domain=self.domain,
                _authorization=self._get_authorization(),
                _cache_issues={},
            )
        return self._handle

    async def locator(  # noqa: C901, PLR0911, PLR0912
        self,
        reference: RootReference,
    ) -> Locator | None:
        if isinstance(reference, WebUrl):
            if reference.domain != self.domain:
                return None

            # 1) Issue: "/browse/PROJ-123"
            if (match := re.fullmatch(REGEX_URL_ISSUE, reference.path)) and (
                issue_key := IssueKey.decode(match.group(1).upper())
            ):
                project_key, issue_id = issue_key.split("-", maxsplit=1)
                return JiraIssueLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_key=FileName.decode(project_key),
                    issue_id=FileName.decode(issue_id),
                )

            # 2) Board: ".../projects/PROJ/boards/ID"
            if (
                (match := re.fullmatch(REGEX_URL_BOARD, reference.path))
                and (project_key := FileName.try_decode(match.group(1).upper()))
                and (board_id := FileName.try_decode(match.group(2)))
            ):
                return JiraBoardLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_key=project_key,
                    board_id=board_id,
                )

            # 3) Filter by ID: /issues?filter=12345
            if re.fullmatch(r"(?:^|/)issues/?", reference.path):
                if (filter_id := reference.get_query("filter")) and filter_id.isdigit():
                    return JiraFilterLocator(
                        realm=self.realm,
                        domain=self.domain,
                        filter_id=FileName.decode(filter_id),
                    )

                # 4) Search by JQL: /issues?jql=...
                if jql := reference.get_query("jql"):
                    return JiraSearchLocator(
                        realm=self.realm,
                        domain=self.domain,
                        jql=jql,
                    )

            raise UnavailableError.new()
        elif isinstance(reference, ExternalUri):
            return None
        else:
            if reference.realm != self.realm:
                return None

            if (
                reference.subrealm == "issue"
                and len(reference.path) == 1
                and re.fullmatch(REGEX_ISSUE_KEY, reference.path[0])
                and (issue_parts := reference.path[0].split("-", maxsplit=1))
            ):
                return JiraIssueLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_key=FileName.decode(issue_parts[0]),
                    issue_id=FileName.decode(issue_parts[1]),
                )

            if (
                reference.subrealm == "board" and len(reference.path) == 2  # noqa: PLR2004
            ):
                return JiraBoardLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_key=reference.path[0],
                    board_id=reference.path[1],
                )

            if reference.subrealm == "filter" and len(reference.path) == 1:
                return JiraFilterLocator(
                    realm=self.realm,
                    domain=self.domain,
                    filter_id=reference.path[0],
                )

            return None

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        assert isinstance(locator, AnyJiraLocator)
        handle = await self._acquire_handle()

        name: str | None = None
        created_at: datetime | None = None
        updated_at: datetime | None = None
        description: str | None = None
        revision: str | None = None
        affordances: list[AffordanceInfo] = []
        should_cache = False

        if isinstance(locator, JiraIssueLocator):
            should_cache = True
            issue = await handle.fetch_issue(locator.issue_key())
            name = f"{issue.key}: {issue.summary}"
            created_at = (
                dateutil.parser.parse(created_str)
                if (created_str := issue.standard_fields.created)
                else None
            )
            updated_at = (
                dateutil.parser.parse(updated_str)
                if (updated_str := issue.standard_fields.updated)
                else None
            )
            description_prefix = (
                f"Issue of type {issue.standard_fields.issue_type}"
                f", status {issue.standard_fields.status}"
                f", priority {issue.standard_fields.priority}"
            )
            description = shorten_description(
                f"{description_prefix}: {issue.description}"
                if issue.description
                else description
            )
            if not cached:
                affordances.append(AffordanceInfo(suffix=AffBody.new()))

        elif isinstance(locator, JiraBoardLocator):
            if not cached:
                name = f"Jira Board - {locator.project_key} - {locator.board_id}"
                affordances.append(AffordanceInfo(suffix=AffBody.new()))

        elif isinstance(locator, JiraFilterLocator):
            should_cache = True
            name, jql = await handle.fetch_filter(locator.filter_id)
            description = shorten_description(f"JQL: {jql}")
            if not cached:
                affordances.append(AffordanceInfo(suffix=AffCollection.new()))

        elif isinstance(locator, JiraSearchLocator):
            revision = None
            name = "JQL Search Results"
            description = shorten_description("JQL: " + locator.jql.replace('"', "'"))
            if not cached:
                affordances.append(AffordanceInfo(suffix=AffCollection.new()))

        else:
            _: Never = locator
            raise UnavailableError.new()

        return ResolveResult(
            metadata=MetadataDelta(
                name=name,
                created_at=created_at,
                updated_at=updated_at,
                description=description,
                revision_data=revision,
                affordances=affordances,
            ),
            expired=[],
            should_cache=should_cache,
        )

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyJiraLocator)

        handle = await self._acquire_handle()

        match locator, observable:
            case JiraIssueLocator(), AffBody():
                return await _read_issue_body(handle, locator)
            case JiraBoardLocator(), AffBody():
                return await _read_board_body(handle, locator)
            case JiraFilterLocator(), AffCollection():
                return await _read_filter_collection(handle, locator, observable)
            case JiraSearchLocator(), AffCollection():
                return await _read_search_collection(handle, locator, observable)
            case _:
                raise UnavailableError.new()

    def _get_authorization(self) -> str:
        authorization, _ = self.context.basic_authorization(
            self.realm, self.public_username, self.public_token
        )
        return authorization


##
## Read - Board
##


async def _read_board_body(
    handle: "JiraHandle",
    locator: JiraBoardLocator,
) -> ObserveResult:
    fragment = await handle.fetch_board(locator.project_key, locator.board_id)
    return ObserveResult(
        bundle=fragment,
        should_cache=False,
        option_labels=False,
        option_relations_link=True,
    )


##
## Read - Issue
##


async def _read_issue_body(
    handle: "JiraHandle",
    locator: JiraIssueLocator,
) -> ObserveResult:
    # Build intermediate model then render markdown like the legacy connector
    issue = await handle.fetch_issue(locator.issue_key())
    text = _jira_issue_to_markdown(handle.domain, issue)

    resource_uri = locator.resource_uri()
    relations: list[Relation] = []
    if issue.parent:
        project_key, issue_id = issue.parent.key.split("-", maxsplit=1)
        parent_locator = JiraIssueLocator(
            realm=handle.realm,
            domain=handle.domain,
            project_key=FileName.decode(project_key),
            issue_id=FileName.decode(issue_id),
        )
        relations.append(
            RelationParent(
                parent=parent_locator.resource_uri(),
                child=resource_uri,
            )
        )
    for related in issue.related_issues:
        related_uri = related.locator(handle.realm).resource_uri()
        if related.link_type in ("is child task of",):
            relations.append(RelationParent(parent=related_uri, child=resource_uri))
        elif related.link_type in (
            "child issues",  # Epic
            "is parent task of",  # Story
        ):
            relations.append(RelationParent(parent=resource_uri, child=related_uri))
        else:
            relations.append(
                RelationMisc.new(related.link_type, resource_uri, related_uri)
            )
    if issue.plugin_teams_url and (
        teams_locator := await try_infer_locator(handle.context, issue.plugin_teams_url)
    ):
        relations.append(
            RelationParent(parent=resource_uri, child=teams_locator.resource_uri())
        )

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=text, blobs={}),
        relations=relations,
        should_cache=False,
        option_labels=False,
        option_relations_link=True,
    )


def _jira_issue_to_markdown(domain: str, issue: "JiraIssue") -> str:  # noqa: C901
    # Metadata table.
    metadata_headers = ["Field", "Value"]
    metadata_rows = [
        ("Type", issue.standard_fields.issue_type),
        ("Status", issue.standard_fields.status),
        ("Priority", issue.standard_fields.priority),
        ("Assignee", issue.standard_fields.assignee),
        ("Reporter", issue.standard_fields.reporter),
        ("Created", issue.standard_fields.created),
        ("Updated", issue.standard_fields.updated),
    ]
    if issue.parent:
        metadata_rows.append(
            (
                "Parent",
                f"[{issue.parent.key}: {issue.parent.summary}](https://{domain}/browse/{issue.parent.key})",
            ),
        )
    metadata_rows.extend(issue.custom_fields)

    # Related issues tables.
    related_headers = [
        "Issue",
        "Issue Type",
        "Assignee",
        "Priority",
        "Status",
    ]
    related_sorted = sorted(
        issue.related_issues,
        key=lambda rel: (rel.link_type, rel.key),
    )
    related_groups: dict[str, list[list[str]]] = {}
    for related_issue in related_sorted:
        related_groups.setdefault(related_issue.link_type, []).append(
            [
                f"[{related_issue.key}: {related_issue.summary}](https://{domain}/browse/{related_issue.key})",
                related_issue.issue_type,
                related_issue.assignee,
                related_issue.priority,
                related_issue.status,
            ],
        )

    # Attachments.
    attachment_images = [
        a for a in issue.attachments if a.media_type.startswith("image/")
    ]
    attachment_files = [
        a for a in issue.attachments if not a.media_type.startswith("image/")
    ]

    # Generate Markdown
    markdown = f"# {issue.key}: {issue.summary}"

    markdown += "\n\n## Metadata\n\n"
    markdown += tabulate(metadata_rows, headers=metadata_headers, tablefmt="github")

    markdown += "\n\n## Description\n\n"
    markdown += _jira_markup_to_markdown(issue.description)

    if issue.plugin_teams_url:
        markdown += (
            "\n\n## Microsoft Teams Conversation\n\n"
            f"![Microsoft Teams conversation]({issue.plugin_teams_url})"
        )

    if issue.related_issues:
        markdown += "\n\n## Related"
        for link_type, related_rows in related_groups.items():
            markdown += f"\n\n### {link_type.capitalize()}\n\n"
            markdown += tabulate(
                related_rows,
                headers=related_headers,
                tablefmt="github",
            )

    if issue.attachments:
        markdown += "\n\n## Attachments"
    if attachment_images:
        markdown += "\n\n" + "\n\n".join(
            f"![{attachment.label}]({attachment.url})"
            for attachment in attachment_images
        )
    if attachment_files:
        markdown += "\n\n" + "\n".join(
            f"- [{attachment.label}]({attachment.url})"
            for attachment in attachment_files
        )

    markdown += "\n\n## Comments"
    if issue.comments:
        for comment in issue.comments:
            markdown += f"\n\n### {comment.author} @ {comment.created}\n\n{_jira_markup_to_markdown(comment.body)}"
    else:
        markdown += "\n\nNo comments."

    return markdown


def _jira_markup_to_markdown(description: str | None) -> str:
    if not description:
        return "None"

    # Replacing {noformat} blocks with Markdown code blocks.
    description = re.sub(
        r"\{noformat\}(.*?)\{noformat\}",
        r"```\n\1\n```",
        description,
        flags=re.DOTALL,
    )

    return re.sub("\\n{3,}", "\n\n", description).strip()


##
## Read - JQL
##


async def _read_filter_collection(
    handle: "JiraHandle",
    locator: JiraFilterLocator,
    observable: AffCollection,
) -> ObserveResult:
    _, jql = await handle.fetch_filter(locator.filter_id)
    if not jql:
        raise UnavailableError.new()

    results_uris = [
        result.resource_uri() for result in await handle.fetch_search_results(jql)
    ]
    return ObserveResult(
        bundle=BundleCollection(
            uri=locator.resource_uri().child_affordance(observable),
            results=[r.resource_uri() for r in results_uris],
        ),
        should_cache=False,
        option_relations_parent=False,
    )


async def _read_search_collection(
    handle: "JiraHandle",
    locator: JiraSearchLocator,
    observable: AffCollection,
) -> ObserveResult:
    # Prefer the JQL from the locator; fall back to cached revision if locator carries only the hash.
    results_uris = [
        result.resource_uri()
        for result in await handle.fetch_search_results(locator.jql)
    ]
    return ObserveResult(
        bundle=BundleCollection(
            uri=locator.resource_uri().child_affordance(observable),
            results=[r.resource_uri() for r in results_uris],
        ),
        should_cache=False,
        option_relations_parent=False,
    )


##
## Handle
##


class JiraEpic(BaseModel):
    key: str
    summary: str


class RelatedIssue(BaseModel):
    key: str
    summary: str
    link_type: str
    issue_type: str
    assignee: str
    priority: str
    status: str

    def locator(self, realm: Realm) -> ResourceUri:
        project_key, issue_id = self.key.split("-", maxsplit=1)
        return ResourceUri(
            realm=realm,
            subrealm=FileName.decode("issue"),
            path=[FileName.decode(project_key), FileName.decode(issue_id)],
        )


class StandardFields(BaseModel):
    issue_type: str
    status: str
    priority: str
    assignee: str
    reporter: str
    created: str
    updated: str


class Attachment(BaseModel):
    label: str
    media_type: MimeType
    url: str


class Comment(BaseModel):
    author: str
    created: str
    body: str


class JiraIssue(BaseModel):
    key: str
    summary: str
    description: str | None = None
    parent: JiraEpic | None = None
    standard_fields: StandardFields
    custom_fields: list[tuple[str, str]]
    related_issues: list[RelatedIssue]
    attachments: list[Attachment]
    comments: list[Comment]
    plugin_teams_url: WebUrl | None = None


@dataclass(kw_only=True)
class JiraHandle:
    context: KnowledgeContext
    realm: Realm
    domain: str
    _authorization: str
    _cache_issues: dict[IssueKey, "JiraIssue"]

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
                logger.exception("Jira.fetch_endpoint_json failed: %s", str(url))
            raise UnavailableError.new()  # noqa: B904

    async def fetch_board(
        self,
        project_key: FileName,
        board_id: FileName,
    ) -> Fragment:
        """
        Render a lightweight board overview via the Agile REST API.
        """
        # NOTE: Only a subset of fields is required for a useful overview.
        # Using Agile API for stable pagination and grouping.
        data = await self._fetch_endpoint_json(
            path=f"rest/agile/1.0/board/{board_id}/issue",
            query="maxResults=1000&fields=key,summary,issuetype,status,assignee,priority,updated",
        )

        # Group by status for readability
        issues = data.get("issues", [])
        groups: dict[str, list[dict[str, Any]]] = {}
        updated_threshold = datetime.now(UTC) - timedelta(days=14)
        for issue in issues:
            fields = issue.get("fields", {})
            group_status = _issue_get(fields, ["status", "name"], "")
            if group_status == "Backlog":
                continue
            if (
                group_status in ("Cancelled", "Completed", "Done", "Resolved")
                and (issue_updated_str := _issue_get(fields, ["updated"], ""))
                and (issue_updated := dateutil.parser.parse(issue_updated_str))
                and issue_updated < updated_threshold
            ):
                continue

            groups.setdefault(group_status or "Unknown", []).append(issue)

        lines: list[str] = []
        lines.append(f"# Jira Board - {project_key} - {board_id}")

        group_headers = [
            "Issue",
            "Issue Type",
            "Assignee",
            "Priority",
        ]
        for status_name, group_items in groups.items():
            lines.append("")
            lines.append(f"## {status_name}")
            lines.append("")

            group_rows: list[tuple[str, ...]] = []
            for group_item in group_items:
                issue_locator = JiraIssueLocator.from_issue_key(
                    realm=self.realm,
                    domain=self.domain,
                    issue_key=group_item.get("key", ""),
                )
                if not issue_locator:
                    continue

                fields: dict[str, Any] = group_item.get("fields", {})
                summary = _issue_get(fields, ["summary"], "").strip()
                group_rows.append(
                    (
                        f"[{issue_locator.issue_key()}: {summary}]({issue_locator.resource_uri()})",
                        _issue_get(fields, ["issuetype", "name"], ""),
                        _issue_get(fields, ["assignee", "displayName"], "Unassigned"),
                        _issue_get(fields, ["priority", "name"], "Undefined"),
                    )
                )

            lines.append(tabulate(group_rows, headers=group_headers, tablefmt="github"))

        return Fragment(mode="markdown", text="\n".join(lines), blobs={})

    async def fetch_filter(
        self,
        filter_id: FileName,
    ) -> tuple[str, str | None]:
        data = await self._fetch_endpoint_json(path=f"rest/api/2/filter/{filter_id}")
        return data.get("name") or f"Filter {filter_id}", data.get("jql")

    async def fetch_issue(self, issue_key: IssueKey) -> "JiraIssue":
        """
        Return an intermediate JiraIssue representation parsed from REST data.
        Rendering to Markdown is delegated to _render_jira_issue.
        """
        if issue_key not in self._cache_issues:
            data = await self._fetch_endpoint_json(path=f"rest/api/2/issue/{issue_key}")
            issue = _parse_jira_issue(issue_key, data)
            if issue.standard_fields.issue_type == "Epic":
                epic_children = await self._fetch_jira_epic_children(issue_key)
                issue.related_issues.extend(epic_children)

            self._cache_issues[issue_key] = issue

        return self._cache_issues[issue_key]

    async def _fetch_jira_epic_children(
        self,
        issue_key: IssueKey,
    ) -> list[RelatedIssue]:
        max_results: int = 50
        results: list[RelatedIssue] = []

        # NOTE: Stop processing on network error and return partial results.
        with contextlib.suppress(Exception):
            start_at: int = 0
            total_results: int = 1  # NOTE: Overridden by response.
            while start_at < total_results:
                request_query = (
                    f"maxResults={max_results}&startAt={start_at}"
                    if start_at
                    else f"maxResults={max_results}"
                )
                epic_data: dict[str, Any] = await self._fetch_endpoint_json(
                    path=f"rest/agile/1.0/epic/{issue_key}/issue",
                    query=request_query,
                )

                total_results = epic_data.get("total", 0)
                max_results = epic_data.get("maxResults", 0) or max_results
                start_at += max_results

                results.extend(
                    RelatedIssue(
                        key=_issue_get(child, ["key"], ""),
                        summary=_issue_get(child, ["fields", "summary"], ""),
                        link_type="child issues",
                        issue_type=_issue_get(
                            child, ["fields", "issuetype", "name"], ""
                        ),
                        assignee=_issue_get(
                            child, ["fields", "assignee", "displayName"], "Unassigned"
                        ),
                        priority=_issue_get(
                            child, ["fields", "priority", "name"], "Undefined"
                        ),
                        status=_issue_get(child, ["fields", "status", "name"], ""),
                    )
                    for child in epic_data.get("issues") or []
                )

        return results

    async def fetch_search_results(self, jql: str) -> list[JiraIssueLocator]:
        data = await self._fetch_endpoint_json(
            path="rest/api/3/search/jql",
            query=f"jql={quote(jql)}&maxResults=1000&fields=key",
        )

        results: list[JiraIssueLocator] = []
        for item in data.get("issues", []):
            key: str | None = item.get("key")
            if not key or "-" not in key:
                continue

            project_key, issue_num = key.split("-", maxsplit=1)
            results.append(
                JiraIssueLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_key=FileName.decode(project_key.upper()),
                    issue_id=FileName.decode(issue_num),
                )
            )

        return results


def _parse_jira_issue(key: str, data: dict[str, Any]) -> JiraIssue:
    fields: dict[str, Any] = data.get("fields", {})

    parent: JiraEpic | None = None
    if (p := fields.get("epic") or fields.get("parent")) is not None:
        parent = JiraEpic(
            key=_issue_get(p, ["key"], ""),
            summary=_issue_get(p, ["fields", "summary"], ""),
        )

    related_issues: list[RelatedIssue] = []
    for link in fields.get("issuelinks") or []:
        if "outwardIssue" in link:
            related = link["outwardIssue"]
            link_type = link.get("type", {}).get("outward", "related to")
        elif "inwardIssue" in link:
            related = link["inwardIssue"]
            link_type = link.get("type", {}).get("inward", "related to")
        else:
            continue

        rel_fs = related.get("fields", {})
        related_issues.append(
            RelatedIssue(
                key=related.get("key", ""),
                summary=rel_fs.get("summary", ""),
                link_type=link_type,
                issue_type=rel_fs.get("issuetype", {}).get("name", ""),
                assignee=_issue_get(rel_fs, ["assignee", "displayName"], "Unassigned"),
                priority=_issue_get(rel_fs, ["priority", "name"], "Undefined"),
                status=_issue_get(rel_fs, ["status", "name"], ""),
            )
        )

    attachments: list[Attachment] = [
        Attachment(
            label=a.get("filename", ""),
            media_type=(
                MimeType.try_decode(a.get("mimeType", ""))
                or MimeType.decode("application/octet-stream")
            ),
            url=a.get("content", ""),
        )
        for a in _issue_get(fields, ["attachment"], [])
    ]

    plugin_teams_url: WebUrl | None = None
    comments: list[Comment] = []
    for c in _issue_get(fields, ["comment", "comments"], []):
        comment_author = _issue_get(c, ["author", "displayName"], "Unknown")
        comment_created = _issue_get(c, ["created"], "")
        comment_body = _issue_get(c, ["body"], "")
        if (
            comment_author == "Microsoft Teams for Jira & JSM (Helpdesk, Jira boards)"
            and "[Microsoft Teams conversation|" in comment_body
        ):
            # fmt: off
            teams_url = WebUrl.try_decode(
                comment_body.split("[Microsoft Teams conversation|", maxsplit=1)[1]
                .split("]")[0]
            )
            if teams_url:
                plugin_teams_url = teams_url
            continue

        comments.append(
            Comment(author=comment_author, created=comment_created, body=comment_body)
        )

    custom_fields: list[tuple[str, str]] = []

    return JiraIssue(
        key=key,
        summary=_issue_get(fields, ["summary"], ""),
        description=_issue_get(fields, ["description"], ""),
        parent=parent,
        standard_fields=StandardFields(
            issue_type=_issue_get(fields, ["issuetype", "name"], "Unknown"),
            status=_issue_get(fields, ["status", "name"], "Unknown"),
            priority=_issue_get(fields, ["priority", "name"], "Undefined"),
            assignee=_issue_get(fields, ["assignee", "displayName"], "Unassigned"),
            reporter=_issue_get(fields, ["reporter", "displayName"], "None"),
            created=_issue_get(fields, ["created"], ""),
            updated=_issue_get(fields, ["updated"], ""),
        ),
        custom_fields=custom_fields,
        related_issues=related_issues,
        attachments=attachments,
        comments=comments,
        plugin_teams_url=plugin_teams_url,
    )


def _issue_get[T](fields: dict[str, Any], path: list[str], default: T) -> T:
    cursor: Any = fields
    for p in path:
        if not isinstance(cursor, dict) or p not in cursor:
            return default
        cursor = cursor[p]
    return cursor or default
