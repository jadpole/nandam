import json
import logging
import re

from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from pydantic import BaseModel
from typing import Any, Literal

from base.api.documents import Fragment
from base.resources.aff_body import AffBody
from base.core.exceptions import BadRequestError, UnavailableError
from base.resources.metadata import AffordanceInfo
from base.resources.relation import RelationParent
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

from knowledge.config import KnowledgeConfig
from knowledge.models.storage_metadata import Locator, MetadataDelta, ResourceView
from knowledge.server.context import (
    Connector,
    KnowledgeContext,
    ObserveResult,
    ResolveResult,
)
from knowledge.services.downloader import SvcDownloader

logger = logging.getLogger(__name__)


REGEX_URL_ATTACHMENT = r"/attachments/get/(\d+)"
REGEX_URL_CASE = r"/cases/view/(\d+)"
REGEX_URL_PROJECT = r"/(?:projects|todos|milestones|reports)/overview/(\d+)"
REGEX_URL_SUITE = r"/suites/view/(\d+)"


##
## Config
##


class QATestRailConnectorConfig(BaseModel, frozen=True):
    kind: Literal["testrail"] = "testrail"
    realm: Realm
    domain: str
    public_username: str | None = None
    public_password: str | None = None

    def instantiate(self, context: KnowledgeContext) -> "QATestRailConnector":
        return QATestRailConnector(
            context=context,
            realm=self.realm,
            domain=self.domain,
            public_username=self.public_username,
            public_password=self.public_password,
        )


##
## Handle
##


@dataclass(kw_only=True)
class QATestRailHandle:
    context: KnowledgeContext
    realm: Realm
    domain: str
    _authorization: str

    def _api(self, path: str) -> WebUrl:
        return WebUrl.decode(f"https://{self.domain}/index.php?/api/v2/{path}")

    async def fetch_endpoint_json(self, path: str) -> Any:
        downloader = self.context.service(SvcDownloader)
        url = self._api(path)
        try:
            response = await downloader.documents_read_download(
                url=url,
                authorization=self._authorization,
                headers={
                    "accept": "application/json",
                    "content-type": "application/json",
                },
                original=True,
            )
            return json.loads(response.text)
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("TestRail.fetch_endpoint_json failed: %s", str(url))
            raise UnavailableError.new()  # noqa: B904

    async def download_attachment(
        self,
        attachment_id: FileName,
    ) -> tuple[Fragment, MimeType]:
        downloader = self.context.service(SvcDownloader)
        url = self._api(f"get_attachment/{attachment_id}")
        try:
            response = await downloader.documents_read_download(
                url=url,
                authorization=self._authorization,
                headers={"content-type": "application/json"},
                original=False,
            )
            return response.as_fragment(), response.mime_type
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("TestRail.download_attachment failed: %s", str(url))
            raise UnavailableError.new()  # noqa: B904

    async def get_project_name(self, project_id: FileName) -> str | None:
        try:
            data = await self.fetch_endpoint_json(f"get_project/{project_id}")
            return data.get("name")
        except Exception:
            return None

    async def get_suite_id_by_case(self, case_id: FileName) -> FileName | None:
        try:
            data = await self.fetch_endpoint_json(f"get_case/{case_id}")
            if (suite_id := data.get("suite_id")) and str(suite_id).isdigit():
                return FileName.decode(str(suite_id))
            else:
                return None
        except Exception:
            return None

    async def get_project_id_by_suite(self, suite_id: FileName) -> FileName | None:
        try:
            data = await self.fetch_endpoint_json(f"get_suite/{suite_id}")
            if (project_id := data.get("project_id")) and str(project_id).isdigit():
                return FileName.decode(str(project_id))
            else:
                return None
        except Exception:
            return None

    async def get_suite_id_by_project(self, project_id: FileName) -> FileName | None:
        try:
            data = await self.fetch_endpoint_json(f"get_suites/{project_id}")
            if (
                isinstance(data, list)
                and len(data) == 1
                and (suite_id := data[0].get("id"))
            ):
                return FileName.decode(str(suite_id))
            else:
                return None
        except Exception:
            return None

    async def fetch_case_metadata(
        self,
        case_id: FileName,
    ) -> tuple[str, datetime | None, datetime | None, str | None]:
        data = await self.fetch_endpoint_json(f"get_case/{case_id}")
        title = data.get("title") or f"Test Case {case_id}"
        created_at = (
            datetime.fromtimestamp(created_on, UTC)
            if (created_on := data.get("created_on"))
            else None
        )
        updated_at = (
            datetime.fromtimestamp(updated_on, UTC)
            if (updated_on := data.get("updated_on"))
            else None
        )
        revision = str(data.get("updated_on")) if data.get("updated_on") else None
        return title, created_at, updated_at, revision

    async def download_case(self, case_id: FileName) -> tuple[Fragment, MimeType]:
        data = await self.fetch_endpoint_json(f"get_case/{case_id}")
        markdown = _testrail_case_to_markdown(data)
        markdown = _replace_attachments_by_urls(markdown, self.domain)
        fragment = Fragment(mode="markdown", text=markdown, blobs={})
        return fragment, MimeType.decode("text/markdown")

    async def fetch_project_metadata(
        self,
        project_id: FileName,
        suite_id: FileName,
    ) -> tuple[str, str | None]:
        project_data = await self.fetch_endpoint_json(f"get_project/{project_id}")
        suite_data = await self.fetch_endpoint_json(f"get_suite/{suite_id}")
        description = suite_data.get("description") or None
        return project_data.get("name") or f"Project {project_id}", description

    async def download_project(
        self,
        project_id: FileName,
    ) -> tuple[Fragment, MimeType, list[ResourceUri]]:
        sections_data = await self.fetch_endpoint_json(f"get_sections/{project_id}")
        cases_data = await self.fetch_endpoint_json(f"get_cases/{project_id}")
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        runs_threshold = int((today - timedelta(days=28)).timestamp())
        runs_data = await self.fetch_endpoint_json(
            f"get_runs/{project_id}&created_after={runs_threshold}"
        )
        markdown = _testrail_project_to_markdown(
            self.domain, sections_data, cases_data, runs_data
        )
        markdown = _replace_attachments_by_urls(markdown, self.domain)
        fragment = Fragment(mode="markdown", text=markdown, blobs={})
        child_uris = [
            locator.resource_uri()
            for c in cases_data
            if (case_id := FileName.try_decode(c.get("id")))
            and (
                locator := QATestRailCaseLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_id=project_id,
                    case_id=case_id,
                )
            )
        ]
        return fragment, MimeType.decode("text/markdown"), child_uris


##
## Locators
##


class QATestRailAttachmentLocator(Locator, frozen=True):
    kind: Literal["testrail_attachment"] = "testrail_attachment"
    realm: Realm
    domain: str
    attachment_id: FileName

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("attachment"),
            path=[self.attachment_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/index.php?/attachments/get/{self.attachment_id}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class QATestRailCaseLocator(Locator, frozen=True):
    kind: Literal["testrail_case"] = "testrail_case"
    realm: Realm
    domain: str
    project_id: FileName
    case_id: FileName

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("case"),
            path=[self.project_id, self.case_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/index.php?/cases/view/{self.case_id}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class QATestRailProjectLocator(Locator, frozen=True):
    kind: Literal["testrail_project"] = "testrail_project"
    realm: Realm
    domain: str
    project_id: FileName
    suite_id: FileName

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("project"),
            path=[self.project_id],
        )

    def content_url(self) -> WebUrl:
        """
        Prefer content URLs with the project ID, to match the resource URI.
        """
        return WebUrl.decode(
            f"https://{self.domain}/index.php?/projects/overview/{self.project_id}"
        )

    def citation_url(self) -> WebUrl:
        """
        Prefer citation URLs with the suite ID, to open the list of test cases.
        """
        return WebUrl.decode(
            f"https://{self.domain}/index.php?/suites/view/{self.suite_id}"
        )


AnyTestRailLocator = (
    QATestRailAttachmentLocator | QATestRailCaseLocator | QATestRailProjectLocator
)


@dataclass(kw_only=True)
class QATestRailConnector(Connector):
    domain: str
    public_username: str | None
    public_password: str | None

    async def locator(  # noqa: C901, PLR0911, PLR0912
        self,
        reference: RootReference,
    ) -> Locator | None:
        handle = await self._acquire_handle()

        if isinstance(reference, WebUrl):
            if reference.domain != self.domain:
                return None
            if reference.path != "index.php" or not reference.query_path:
                raise UnavailableError.new()

            # 1) Attachment by ID: /attachments/get/ID
            if (match := re.fullmatch(REGEX_URL_ATTACHMENT, reference.query_path)) and (
                attachment_id := FileName.try_decode(match.group(1))
            ):
                return QATestRailAttachmentLocator(
                    realm=self.realm,
                    domain=self.domain,
                    attachment_id=attachment_id,
                )

            # 2) Case by ID: /cases/view/ID -> fetch project via suite
            if (
                (match := re.fullmatch(REGEX_URL_CASE, reference.query_path))
                and (case_id := FileName.try_decode(match.group(1)))
                and (suite_id := await handle.get_suite_id_by_case(case_id))
                and (project_id := await handle.get_project_id_by_suite(suite_id))
            ):
                return QATestRailCaseLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_id=project_id,
                    case_id=case_id,
                )

            # 3) Project overview-like URLs: /projects|todos|milestones|reports/overview/ID
            if (
                (match := re.fullmatch(REGEX_URL_PROJECT, reference.query_path))
                and (project_id := FileName.try_decode(match.group(1)))
                and (suite_id := await handle.get_suite_id_by_project(project_id))
            ):
                return QATestRailProjectLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_id=project_id,
                    suite_id=suite_id,
                )

            # 4) Suite URL: /suites/view/ID -> project
            if (
                (match := re.fullmatch(REGEX_URL_SUITE, reference.query_path))
                and (suite_id := FileName.try_decode(match.group(1)))
                and (project_id := await handle.get_project_id_by_suite(suite_id))
            ):
                return QATestRailProjectLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_id=project_id,
                    suite_id=suite_id,
                )

            raise UnavailableError.new()

        elif isinstance(reference, ExternalUri):
            return None

        else:
            if reference.realm != self.realm:
                return None

            if reference.subrealm == "attachment" and len(reference.path) == 1:
                return QATestRailAttachmentLocator(
                    realm=self.realm,
                    domain=self.domain,
                    attachment_id=reference.path[0],
                )

            if (
                reference.subrealm == "case" and len(reference.path) == 2  # noqa: PLR2004
            ):
                return QATestRailCaseLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_id=reference.path[0],
                    case_id=reference.path[1],
                )

            if (
                reference.subrealm == "project"
                and len(reference.path) == 1
                and (project_id := reference.path[0])
                and (suite_id := await handle.get_suite_id_by_project(project_id))
            ):
                return QATestRailProjectLocator(
                    realm=self.realm,
                    domain=self.domain,
                    project_id=project_id,
                    suite_id=suite_id,
                )

            raise UnavailableError.new()

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        assert isinstance(locator, AnyTestRailLocator)
        handle = await self._acquire_handle()

        # Since all TestRail cases are public and "virtual", i.e., they will be
        # automatically refreshed when their contents are read, exit quickly.
        if cached:
            return ResolveResult()

        name: str | None = None
        created_at: datetime | None = None
        updated_at: datetime | None = None
        revision: str | None = None

        match locator:
            case QATestRailAttachmentLocator():
                pass  # Lightweight: just advertise affordances.
            case QATestRailCaseLocator():
                (
                    name,
                    created_at,
                    updated_at,
                    revision,
                ) = await handle.fetch_case_metadata(locator.case_id)
            case QATestRailProjectLocator():
                name, _description = await handle.fetch_project_metadata(
                    locator.project_id, locator.suite_id
                )

        expired = bool(
            cached
            and revision is not None
            and cached.metadata.revision_data != revision
        )

        return ResolveResult(
            metadata=MetadataDelta(
                name=name,
                created_at=created_at,
                updated_at=updated_at,
                revision_data=revision,
                affordances=[AffordanceInfo(suffix=AffBody.new())],
            ),
            expired=[AffBody.new()] if expired else [],
            should_cache=True,
        )

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyTestRailLocator)

        handle = await self._acquire_handle()

        match locator, observable:
            case (QATestRailAttachmentLocator(), AffBody()):
                fragment, mime_type = await handle.download_attachment(
                    locator.attachment_id
                )
                return ObserveResult(
                    bundle=fragment,
                    metadata=MetadataDelta(mime_type=mime_type),
                    should_cache=mime_type.mode() in ("document", "media"),
                    option_labels=True,
                    option_relations_link=True,
                )

            case (QATestRailCaseLocator(), AffBody()):
                fragment, mime_type = await handle.download_case(locator.case_id)
                return ObserveResult(
                    bundle=fragment,
                    metadata=MetadataDelta(mime_type=mime_type),
                    should_cache=mime_type.mode() in ("document", "media"),
                    option_labels=True,
                    option_relations_link=False,
                )

            case (QATestRailProjectLocator(), AffBody()):
                fragment, mime_type, child_uris = await handle.download_project(
                    locator.project_id
                )
                return ObserveResult(
                    bundle=fragment,
                    metadata=MetadataDelta(mime_type=mime_type),
                    relations=[
                        RelationParent(parent=locator.resource_uri(), child=child_uri)
                        for child_uri in child_uris
                    ],
                    should_cache=mime_type.mode() in ("document", "media"),
                    option_labels=True,
                    option_relations_link=True,
                )

            case _:
                raise BadRequestError.observable(observable.as_suffix())

    async def _acquire_handle(self) -> QATestRailHandle:
        return QATestRailHandle(
            context=self.context,
            realm=self.realm,
            domain=self.domain,
            _authorization=self.get_authorization(),
        )

    def get_authorization(self) -> str:
        authorization, _ = self.context.basic_authorization(
            self.realm, self.public_username, self.public_password
        )
        return authorization


##
## Read
##


def _testrail_case_to_markdown(case_data: dict[str, Any]) -> str:
    content = f"# {case_data.get('title', '').strip()}"

    if preconds := (case_data.get("custom_preconds") or "").strip():
        content += f"\n\n## Preconditions\n\n{preconds}"

    if steps_sep := case_data.get("custom_steps_separated"):
        content += "\n\n## Steps"
        for index, step in enumerate(steps_sep):
            content += f"\n\n### Step {index + 1}"
            if step_content := (step.get("content") or "").strip():
                content += f"\n\n{step_content}"
            if expected := (step.get("expected") or "").strip():
                content += f"\n\n**Expected Result**: {expected}"
    else:
        if custom_steps := (case_data.get("custom_steps") or "").strip():
            content += f"\n\n## Steps\n\n{custom_steps}"
        if custom_expected := (case_data.get("custom_expected") or "").strip():
            content += f"\n\n## Expected Result\n\n{custom_expected}"

    return content.strip()


def _testrail_project_to_markdown(
    domain: str,
    sections_data: list[dict[str, Any]],
    cases_data: list[dict[str, Any]],
    runs_data: list[dict[str, Any]],
) -> str:
    return "\n\n".join(
        [
            "# Test Suite",
            _testrail_project_cases_to_markdown(domain, sections_data, cases_data),
            "# Test Runs (Last 28 Days)",
            _testrail_project_runs_to_markdown(runs_data),
        ]
    )


def _testrail_project_cases_to_markdown(
    domain: str,
    sections_data: list[dict[str, Any]],
    cases_data: list[dict[str, Any]],
) -> str:
    if not cases_data:
        return "No test cases."

    content = ""

    for section in sections_data:
        depth = int(section.get("depth") or 0)
        name = section.get("name") or ""
        content += "\n\n" + "#" * (depth + 2) + " " + name
        if description := (section.get("description") or ""):
            content += "\n\n" + description

        cases_markdown = [
            f"- [{c.get('title')}](https://{domain}/index.php?/cases/view/{c.get('id')})"
            for c in cases_data
            if c.get("section_id") == section.get("id")
        ]

        if cases_markdown:
            content += "\n\n**Test cases**:\n\n" + "\n".join(cases_markdown)
        else:
            content += "\n\nNo test cases."

    return content.strip()


def _testrail_project_runs_to_markdown(runs_data: list[dict[str, Any]]) -> str:
    if not runs_data:
        return "No test runs."
    return "\n".join("- " + _testrail_project_run_to_markdown(run) for run in runs_data)


def _testrail_project_run_to_markdown(run_data: dict[str, Any]) -> str:
    status = "completed" if run_data.get("is_completed") else "active"
    num_tests = (
        (run_data.get("passed_count") or 0)
        + (run_data.get("failed_count") or 0)
        + (run_data.get("blocked_count") or 0)
        + (run_data.get("untested_count") or 0)
    )
    created_on_raw = run_data.get("created_on")
    if not isinstance(created_on_raw, (float, int)):
        created_on_raw = 0
    created_on = datetime.fromtimestamp(float(created_on_raw), UTC)
    created_str = created_on.strftime("%Y-%m-%d")

    parts: list[str] = []
    if run_data.get("passed_count"):
        parts.append(f"{run_data['passed_count']} passed")
    if run_data.get("failed_count"):
        parts.append(f"{run_data['failed_count']} failed")
    if run_data.get("blocked_count"):
        parts.append(f"{run_data['blocked_count']} blocked")
    if run_data.get("untested_count"):
        parts.append(f"{run_data['untested_count']} untested")
    if run_data.get("retest_count"):
        parts.append(f"{run_data['retest_count']} to retest")

    if parts:
        return (
            f"{created_str} {status} run with {num_tests} tests: "
            + ", ".join(parts)
            + "."
        )
    else:
        return f"{status} empty run."


def _replace_attachments_by_urls(markdown: str, domain: str) -> str:
    paths = set(re.findall(r"\]\((index.php\?/attachments/get/\d+)\)", markdown))
    for path in paths:
        markdown = markdown.replace(
            f"]({path})",
            f"](https://{domain}/{path})",
        )
    return markdown
