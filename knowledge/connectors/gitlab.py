import contextlib
import dateutil.parser
import fnmatch
import json
import logging
import re
import yaml

from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Any, Literal, Never
from urllib.parse import quote, unquote

from base.api.documents import DocumentsReadResponse, Fragment
from base.core.exceptions import BadRequestError, UnavailableError
from base.core.values import parse_yaml_as
from base.resources.aff_body import AffBody
from base.resources.aff_collection import AffCollection
from base.resources.aff_plain import AffPlain
from base.resources.metadata import AffordanceInfo
from base.resources.relation import Relation, RelationParent
from base.strings.data import MimeType
from base.strings.file import REGEX_FILENAME, REGEX_FILEPATH, FileName, FilePath
from base.strings.resource import (
    ExternalUri,
    Observable,
    Realm,
    ResourceUri,
    RootReference,
    WebUrl,
)
from base.utils.markdown import markdown_split_code, strip_keep_indent

from knowledge.config import KnowledgeConfig
from knowledge.models.storage_metadata import Locator, MetadataDelta, ResourceView
from knowledge.models.storage_observed import BundleCollection, BundlePlain
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


class GitLabConnectorConfig(BaseModel, frozen=True):
    kind: Literal["gitlab"] = "gitlab"
    realm: Realm
    domain: str
    public_token: str

    def instantiate(self, context: KnowledgeContext) -> "GitLabConnector":
        return GitLabConnector(
            context=context,
            realm=self.realm,
            domain=self.domain,
            repositories={},
            public_token=self.public_token,
        )


##
## Context
##


GitLabRef = FilePath
"""
Expect tags, branches, and commit hashes to respect the `FilePath` regex.
"""


class Repository(BaseModel, frozen=True):
    """
    NOTE: We disallow the "_" character in group names, and use it to collapse
    sub-groups into a single file name in the resulting resource URI.
    """

    domain: str
    groups: list[FileName]
    project: FileName

    @staticmethod
    def from_web(url: WebUrl) -> "Repository | None":
        segment: str = url.path.split("/-/", 1)[0]
        if segment.endswith(("/activity", "/-")) or not re.fullmatch(
            REGEX_GITLAB_REPOSITORY_WEB_PATH, segment
        ):
            return None

        *url_groups, url_project = segment.split("/")
        if 1 <= len(url_groups) <= 4:  # noqa: PLR2004
            return Repository(
                domain=url.domain,
                groups=[FileName.decode(g) for g in url_groups],
                project=FileName.decode(url_project),
            )
        else:
            return None

    @staticmethod
    def from_uri(domain: str, uri: ResourceUri) -> "Repository | None":
        if uri.subrealm in ("commit", "file", "repository") and len(uri.path) >= 2:  # noqa: PLR2004
            uri_group, uri_project, *_path = uri.path
        elif uri.subrealm == "ref" and len(uri.path) >= 3:  # noqa: PLR2004
            _ref, uri_group, uri_project, *_path = uri.path
        else:
            return None

        return Repository(
            domain=domain,
            groups=[FileName.decode(g) for g in uri_group.split("_")],
            project=uri_project,
        )

    def as_web_prefix(self) -> WebUrl:
        return WebUrl(
            domain=self.domain,
            port=443,
            path=self.as_web_segment(),
            path_prefix=None,
            query_path=None,
            query=[],
            fragment="",
        )

    def as_web_segment(self) -> str:
        return "/".join([*self.groups, self.project])

    def as_uri_segment(self) -> list[FileName]:
        return [FileName.decode("_".join(self.groups)), self.project]

    def as_encoded(self) -> str:
        return quote(self.as_web_segment(), safe="")


@dataclass(kw_only=True, frozen=True)
class RepositoryMetadata:
    id: int
    visible: bool
    archived: bool
    default_branch: GitLabRef
    description: str
    updated_at: datetime | None

    @staticmethod
    async def load(
        context: KnowledgeContext,
        authorization: str,
        repository: Repository,
    ) -> "RepositoryMetadata":
        downloader = context.service(SvcDownloader)
        url = f"https://{repository.domain}/api/v4/projects/{repository.as_encoded()}"
        try:
            response = await downloader.documents_read_download(
                url=WebUrl.decode(url),
                authorization=authorization,
                headers={"accept": "application/json"},
                original=True,
            )
            data: dict[str, Any] = json.loads(response.text)
            return RepositoryMetadata.parse(data)
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception(
                    "Failed to read repository metadata: %s",
                    repository.as_web_segment(),
                )
            raise UnavailableError.new()  # noqa: B904

    @staticmethod
    def parse(data: dict[str, Any]) -> "RepositoryMetadata":
        return RepositoryMetadata(
            archived=data.get("archived") or False,
            default_branch=(
                FilePath.try_decode(data.get("default_branch", ""))
                or FilePath.decode("master")
            ),
            description=data.get("description") or "",
            id=data["id"],
            updated_at=(
                dateutil.parser.parse(updated_at)
                if (updated_at := data.get("updated_at"))
                else None
            ),
            visible=data.get("visibility") in ("public", "internal"),
        )


class RepositoryConfig(BaseModel, frozen=True):
    """
    Schema of the `nandam.yml` config file at the root of a repository.
    """

    branch: FilePath | None = None
    allowed: list[str] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    skipped_notify: list[str] = Field(default_factory=list)
    subprojects: dict[str, str | list[str]] = Field(default_factory=dict)

    @staticmethod
    async def load(
        context: KnowledgeContext,
        authorization: str,
        repository: Repository,
        ref: GitLabRef,
    ) -> "RepositoryConfig | None":
        downloader = context.service(SvcDownloader)
        url = (
            f"https://{repository.domain}/api/v4"
            f"/projects/{repository.as_encoded()}/repository"
            f"/files/nandam.yml/raw?ref={ref}"
        )
        try:
            response = await downloader.documents_read_download(
                url=WebUrl.decode(url),
                authorization=authorization,
                original=True,
            )
            return parse_yaml_as(RepositoryConfig, response.text)
        except ValueError:
            logger.exception(
                "Failed to parse repository config: %s",
                repository.as_web_segment(),
            )
            return None
        except Exception:
            return None


class GitLabCommit(BaseModel, frozen=True):
    full_id: GitLabRef
    short_id: GitLabRef
    updated_at: datetime

    @staticmethod
    def parse(data: dict[str, Any]) -> "GitLabCommit":
        return GitLabCommit(
            full_id=FilePath.decode(data["id"]),
            short_id=FilePath.decode(data["short_id"]),
            updated_at=dateutil.parser.parse(data["created_at"]),
        )


class GitLabBranch(BaseModel, frozen=True):
    name: GitLabRef
    commit_id: GitLabRef

    @staticmethod
    async def load(
        context: KnowledgeContext,
        authorization: str,
        repository: Repository,
    ) -> tuple[list["GitLabBranch"], list[GitLabCommit]]:
        downloader = context.service(SvcDownloader)
        url = (
            f"https://{repository.domain}/api/v4"
            f"/projects/{repository.as_encoded()}/repository/branches?per_page=100"
        )
        try:
            response = await downloader.documents_read_download(
                url=WebUrl.decode(url),
                authorization=authorization,
                original=True,
            )
            data: list[dict[str, Any]] = json.loads(response.text)
            return GitLabBranch.parse(data)
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception(
                    "Failed to read repository branches: %s",
                    repository.as_web_segment(),
                )
            raise UnavailableError.new()  # noqa: B904

    @staticmethod
    def parse(
        data: list[dict[str, Any]],
    ) -> tuple[list["GitLabBranch"], list[GitLabCommit]]:
        branches: list[GitLabBranch] = []
        commits: list[GitLabCommit] = []
        for branch_data in data:
            if branch_name := FilePath.try_decode(branch_data["name"]):
                commit = GitLabCommit.parse(branch_data["commit"])
                branch = GitLabBranch(name=branch_name, commit_id=commit.full_id)
                commits.append(commit)
                branches.append(branch)

        return branches, commits


@dataclass(kw_only=True)
class GitLabFile:
    path: FilePath
    subproject: FileName | None


@dataclass(kw_only=True)
class GitLabHandle:
    context: KnowledgeContext
    realm: Realm
    repository: Repository
    metadata: RepositoryMetadata
    config: RepositoryConfig
    branches: list[GitLabBranch]

    # Caches:
    _authorization: str
    _files: dict[GitLabRef, list[FilePath]]
    _file_metadata: dict[str, MetadataDelta | None]
    _commits: dict[GitLabRef, GitLabCommit | None]

    @staticmethod
    async def initialize(
        context: KnowledgeContext,
        realm: Realm,
        repository: Repository,
        authorization: str,
    ) -> "GitLabHandle":
        metadata = await RepositoryMetadata.load(context, authorization, repository)

        # Read the "nandam.yml" config file from the repository, and use it to
        # override the default branch when it disagrees with GitLab.
        config = await RepositoryConfig.load(
            context, authorization, repository, metadata.default_branch
        )
        if (
            config
            and config.branch
            and config.branch != metadata.default_branch
            and (
                better_config := await RepositoryConfig.load(
                    context, authorization, repository, config.branch
                )
            )
            and better_config.branch == config.branch
        ):
            metadata = RepositoryMetadata(
                id=metadata.id,
                visible=metadata.visible,
                archived=metadata.archived,
                default_branch=config.branch,  # Replace the default branch.
                description=metadata.description,
                updated_at=metadata.updated_at,
            )
            config = better_config
        config = config or RepositoryConfig()

        branches, commits = await GitLabBranch.load(context, authorization, repository)
        if not any(b.name == metadata.default_branch for b in branches):
            logger.error(
                "Repository %s has an invalid default branch %s",
                repository.as_web_segment(),
                metadata.default_branch,
            )
            raise UnavailableError.new()

        return GitLabHandle(
            context=context,
            realm=realm,
            repository=repository,
            metadata=metadata,
            config=config,
            branches=branches,
            _authorization=authorization,
            _files={},
            _file_metadata={},
            _commits={commit.full_id: commit for commit in commits},
        )

    #
    # URL helpers to avoid duplicating URL-building logic across methods
    #

    def _encoded_repository(self) -> str:
        return quote(self.repository.as_web_segment(), safe="")

    def _api_base(self) -> str:
        return (
            f"https://{self.repository.domain}/api/v4"
            f"/projects/{self._encoded_repository()}"
        )

    def _repo_endpoint(self, endpoint: str) -> str:
        base = self._api_base()
        return f"{base}/{endpoint}" if endpoint else base

    def _file_raw_url(self, ref: GitLabRef, path: list[FileName]) -> str:
        encoded_path = quote("/".join(path), safe="")
        return self._repo_endpoint(f"repository/files/{encoded_path}/raw?ref={ref}")

    async def files(self, ref: GitLabRef, prefix: list[FileName]) -> list[FilePath]:
        if ref not in self._files:
            try:
                self._files[ref] = await self._uncached_files(ref)
            except Exception:
                self._files[ref] = []  # `ref` not found.

        if prefix:
            joined_prefix = FilePath.new(prefix)
            return [f for f in self._files[ref] if f.is_child_or(joined_prefix)]
        else:
            return self._files[ref]

    async def find_mode(
        self,
        ref: GitLabRef,
        path: list[FileName],
    ) -> Literal["blob", "tree"] | None:
        files = await self.files(ref, [])
        if not files:
            return None
        if not path:
            return "tree"  # Root of the repository.

        joined_path = "/".join(path)
        if any(f == joined_path for f in files):
            return "blob"

        directories = {dir_path for f in files if (dir_path := f.rsplit("/", 1)[0])}
        if joined_path in directories:
            return "tree"
        else:
            return None

    def get_branch(self, ref: GitLabRef) -> GitLabBranch | None:
        for branch in self.branches:
            if branch.name == ref:
                return branch
        return None

    async def get_commit(self, ref: GitLabRef) -> GitLabCommit | None:
        if branch := self.get_branch(ref):
            ref = branch.commit_id
        if ref not in self._commits:
            self._commits[ref] = await self._uncached_get_commit(ref)
        return self._commits[ref]

    async def infer_file_metadata(
        self,
        ref: GitLabRef,
        path: list[FileName],
    ) -> MetadataDelta:
        file_key = "/".join([ref, *path])
        if file_key not in self._file_metadata:
            file_metadata = await self._uncached_infer_file_metadata(ref, path)
            self._file_metadata[file_key] = file_metadata

        if cached_metadata := self._file_metadata[file_key]:
            return cached_metadata
        else:
            raise UnavailableError.new()

    def infer_subproject(self, path: FilePath) -> FileName | None:  # noqa: C901, PLR0911
        if any(s in path for s in GITLAB_DEFAULT_SKIPPED_SUBSTR):
            return None
        if any(path.startswith(s) for s in GITLAB_DEFAULT_SKIPPED_ABSOLUTE):
            return None

        if not any(path.startswith(s) for s in self.config.allowed):
            if any(path.startswith(s) for s in self.config.skipped):
                return None
            if any(path.startswith(s) for s in self.config.skipped_notify):
                return None  # TODO: Handle "skipped".

            filename = path.rsplit("/", maxsplit=1)[-1]
            if not any(fnmatch.fnmatch(filename, p) for p in GITLAB_DEFAULT_ALLOWED):
                return None

        if self.config.subprojects:
            for subproject_name, subproject_path in self.config.subprojects.items():
                if not (subproject := FileName.try_decode(subproject_name)):
                    continue
                subproject_paths = (
                    [subproject_path]
                    if isinstance(subproject_path, str)
                    else subproject_path
                )
                if any(path.startswith(s) for s in subproject_paths):
                    return subproject

        return FileName.decode("root")

    def split_ref_and_path(
        self,
        mode: Literal["uri", "web"],
        ref_and_path: str,
    ) -> tuple[GitLabRef, list[FileName]]:
        # Check whether we are looking at a path on a branch, whose name may
        # contain "/".  Otherwise, assume that it is a tag or a commit, wwhere
        # "/" is not allowed in our scheme.
        ref: GitLabRef = FilePath.decode(ref_and_path.split("/", 1)[0])
        for branch in self.branches:
            branch_prefix = (
                branch.name.replace("/", "_") if mode == "uri" else branch.name
            )
            if ref_and_path == branch_prefix:
                return branch.name, []
            if ref_and_path.startswith(f"{branch_prefix}/"):
                ref = branch.name
                break

        # NOTE: Allow the empty path "", i.e., the project's root directory.
        ref_prefix = ref.replace("/", "_") if mode == "uri" else str(ref)
        path = ref_and_path.removeprefix(ref_prefix).removeprefix("/").removesuffix("/")
        if path:
            return ref, FilePath.decode(path).parts()
        else:
            return ref, []

    async def fetch_endpoint_json(
        self,
        endpoint: str,
    ) -> tuple[dict[str, str], Any]:
        downloader = self.context.service(SvcDownloader)
        url = self._repo_endpoint(endpoint)
        try:
            response = await downloader.documents_read_download(
                url=WebUrl.decode(url),
                authorization=self._authorization,
                original=True,
            )
            return response.headers, json.loads(response.text)
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("GitLabHandle.fetch_endpoint_json failed: %s", url)
            raise UnavailableError.new()  # noqa: B904

    async def read_file(
        self,
        ref: GitLabRef,
        path: list[FileName],
        original: bool,
    ) -> DocumentsReadResponse:
        downloader = self.context.service(SvcDownloader)
        url = self._file_raw_url(ref, path)
        try:
            return await downloader.documents_read_download(
                url=WebUrl.decode(url),
                authorization=self._authorization,
                original=original,
            )
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("GitLabHandle.read_file failed: %s", url)
            raise UnavailableError.new()  # noqa: B904

    ##
    ## Implementation
    ##

    async def _uncached_files(self, ref: GitLabRef) -> list[FilePath]:
        all_files: list[FilePath] = []
        page = 1
        while True:
            page_items: list[dict[str, Any]]
            response_headers, page_items = await self.fetch_endpoint_json(
                f"repository/tree?ref={quote(ref, safe='/')}&recursive=true&per_page=100&page={page}"
            )
            if not page_items:  # No more items
                break

            all_files.extend(
                file_path
                for item in page_items
                if item.get("type") == "blob"
                and (file_path := FilePath.try_decode(item.get("path")))
            )

            next_page_header = response_headers.get("x-next-page")
            if next_page_header and next_page_header.isdigit():
                page = int(next_page_header)
            else:  # No more pages
                break

        return all_files

    async def _uncached_get_commit(self, ref: GitLabRef) -> GitLabCommit | None:
        try:
            _, data = await self.fetch_endpoint_json(f"repository/commits/{ref}")
            return GitLabCommit.parse(data)
        except Exception:
            return None

    async def _uncached_infer_file_metadata(
        self,
        ref: GitLabRef,
        path: list[FileName],
    ) -> MetadataDelta | None:
        downloader = self.context.service(SvcDownloader)
        url = self._file_raw_url(ref, path)

        try:
            headers = await downloader.fetch_head(
                WebUrl.decode(url),
                {"Private-Token": self._authorization.removeprefix("Private-Token ")},
            )

            metadata = MetadataDelta()
            if (content_type := headers.get("content-type")) and (
                mime_type := MimeType.try_decode(content_type.split(";")[0])
            ):
                metadata = metadata.with_update(MetadataDelta(mime_type=mime_type))
            if last_commit_id := headers.get("x-gitlab-last-commit-id"):
                metadata = metadata.with_update(
                    MetadataDelta(revision_data=last_commit_id)
                )

            return metadata
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("GitLabHandle.infer_file_metadata failed: %s", url)
            return None


##
## Locators
##


REGEX_GITLAB_GROUP_WEB_PATH = r"[A-Za-z0-9\-]+(?:/[A-Za-z0-9\-]+)*"
REGEX_GITLAB_GROUP_SKN_NAME = r"[A-Za-z0-9\-]+(?:_[A-Za-z0-9\-]+)*"
REGEX_GITLAB_REPOSITORY_WEB_PATH = rf"{REGEX_GITLAB_GROUP_WEB_PATH}/{REGEX_FILENAME}"
REGEX_GITLAB_REPOSITORY_SKN_PATH = rf"{REGEX_GITLAB_GROUP_SKN_NAME}/{REGEX_FILENAME}"


class GitLabRepositoryLocator(Locator, frozen=True):
    kind: Literal["gitlab_repository"] = "gitlab_repository"
    repository: Repository

    @staticmethod
    def from_web(
        handle: GitLabHandle,
        url: WebUrl,
    ) -> "GitLabRepositoryLocator | None":
        locator = GitLabRepositoryLocator(
            realm=handle.realm,
            repository=handle.repository,
        )
        if url == handle.repository.as_web_prefix():
            return locator
        else:
            return None

    @staticmethod
    def from_uri(
        handle: GitLabHandle,
        uri: ResourceUri,
    ) -> "GitLabRepositoryLocator | None":
        locator = GitLabRepositoryLocator(
            realm=handle.realm,
            repository=handle.repository,
        )
        if uri == locator.resource_uri():
            return locator
        else:
            return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("repository"),
            path=self.repository.as_uri_segment(),
        )

    def content_url(self) -> WebUrl:
        return self.repository.as_web_prefix()

    def citation_url(self) -> WebUrl:
        return self.content_url()


class GitLabFileLocator(Locator, frozen=True):
    kind: Literal["gitlab_file"] = "gitlab_file"
    repository: Repository
    ref: GitLabRef
    mode: Literal["blob", "tree"]
    is_default_branch: bool
    path: list[FileName]

    @staticmethod
    def from_web(
        handle: GitLabHandle,
        url: WebUrl,
    ) -> "GitLabFileLocator | None":
        if url.domain != handle.repository.domain:
            return None

        mode: Literal["blob", "tree"]
        ref_and_path: str
        repo_segment = handle.repository.as_web_segment()
        if url.path.startswith(f"{repo_segment}/-/raw/"):
            mode = "blob"
            ref_and_path = url.path.removeprefix(f"{repo_segment}/-/raw/")
        elif url.path.startswith(f"{repo_segment}/-/blob/"):
            mode = "blob"
            ref_and_path = url.path.removeprefix(f"{repo_segment}/-/blob/")
        elif url.path.startswith(f"{repo_segment}/-/tree/"):
            mode = "tree"
            ref_and_path = url.path.removeprefix(f"{repo_segment}/-/tree/")
        else:
            return None

        # NOTE: Ignore paths with components that are not valid `FileName`,
        # i.e., those containing whitespace or special characters.  Check it
        # immediately, so the rest of the code can assume a valid path.
        if not FilePath.try_decode(ref_and_path):
            return None

        ref, path = handle.split_ref_and_path("web", ref_and_path)

        return GitLabFileLocator(
            realm=handle.realm,
            repository=handle.repository,
            ref=ref,
            mode=mode,
            is_default_branch=ref == handle.metadata.default_branch,
            path=path,
        )

    @staticmethod
    async def from_uri(
        handle: GitLabHandle,
        uri: ResourceUri,
    ) -> "GitLabFileLocator | None":
        if uri.realm != handle.realm:
            return None

        if (
            uri.subrealm == "file"
            and len(uri.path) >= 2  # noqa: PLR2004
            and f"{uri.path[0]}/{uri.path[1]}" == handle.repository.as_uri_segment()
        ):
            is_default_branch = True
            ref = handle.metadata.default_branch
            path = uri.path[2:]

        elif (
            uri.subrealm == "ref"
            and len(uri.path) >= 3  # noqa: PLR2004
            and f"{uri.path[0]}/{uri.path[1]}" == handle.repository.as_uri_segment()
        ):
            is_default_branch = False
            ref, path = handle.split_ref_and_path("uri", "/".join(uri.path[2:]))

        else:
            return None

        mode = await handle.find_mode(ref, path)
        if not mode:
            return None

        return GitLabFileLocator(
            realm=handle.realm,
            mode=mode,
            repository=handle.repository,
            ref=ref,
            is_default_branch=is_default_branch,
            path=path,
        )

    def resource_uri(self) -> ResourceUri:
        if self.is_default_branch:
            return ResourceUri(
                realm=self.realm,
                subrealm=FileName.decode("file"),
                path=[*self.repository.as_uri_segment(), *self.path],
            )
        else:
            ref = FileName.decode(self.ref.replace("/", "_"))
            return ResourceUri(
                realm=self.realm,
                subrealm=FileName.decode("ref"),
                path=[*self.repository.as_uri_segment(), ref, *self.path],
            )

    def content_url(self) -> WebUrl:
        content_mode = "raw" if self.mode == "blob" else self.mode
        path_suffix = "/" + "/".join(quote(f) for f in self.path) if self.path else ""
        return WebUrl.decode(
            f"{self.repository.as_web_prefix()}/-/{content_mode}/"
            f"{quote(self.ref, safe='/')}{path_suffix}",
        )

    def citation_url(self) -> WebUrl:
        path_suffix = "/" + "/".join(quote(f) for f in self.path) if self.path else ""
        return WebUrl.decode(
            f"{self.repository.as_web_prefix()}/-/{self.mode}/"
            f"{quote(self.ref, safe='/')}{path_suffix}",
        )


class GitLabCompareLocator(Locator, frozen=True):
    kind: Literal["gitlab_compare"] = "gitlab_compare"
    repository: Repository
    before: GitLabRef
    after: GitLabRef

    @staticmethod
    def from_web(
        handle: GitLabHandle,
        url: WebUrl,
    ) -> "GitLabCompareLocator | None":
        if url.domain != handle.repository.domain:
            return None

        path_prefix = f"{handle.repository.as_web_segment()}/-/compare/"
        if not url.path.startswith(path_prefix):
            return None

        compare_part = url.path.removeprefix(path_prefix)
        if "/" in compare_part or compare_part.count("...") != 1:
            return None

        before_str, after_str = compare_part.split("...")
        before = FilePath.try_decode(unquote(before_str))
        after = FilePath.try_decode(unquote(after_str))
        if not before or not after:
            return None

        return GitLabCompareLocator(
            realm=handle.realm,
            repository=handle.repository,
            before=before,
            after=after,
        )

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("compare"),
            path=[
                *self.repository.as_uri_segment(),
                FileName.decode(
                    f"{self.before.replace('/', '_')}_{self.after.replace('/', '_')}"
                ),
            ],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"{self.repository.as_web_prefix()}/-/compare/{quote(self.before)}...{quote(self.after)}",
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class GitLabCommitLocator(Locator, frozen=True):
    kind: Literal["gitlab_commit"] = "gitlab_commit"
    repository: Repository
    commit_id: GitLabRef

    @staticmethod
    def from_web(
        handle: GitLabHandle,
        url: WebUrl,
    ) -> "GitLabCommitLocator | None":
        if url.domain != handle.repository.domain:
            return None

        path_prefix = f"{handle.repository.as_web_segment()}/-/commit/"
        if not url.path.startswith(path_prefix):
            return None

        commit_part = url.path.removeprefix(path_prefix)
        if not re.fullmatch(r"[0-9a-f]{40}", commit_part):
            return None

        return GitLabCommitLocator(
            realm=handle.realm,
            repository=handle.repository,
            commit_id=FilePath.decode(commit_part),
        )

    @staticmethod
    def from_uri(
        handle: GitLabHandle,
        uri: ResourceUri,
    ) -> "GitLabCommitLocator | None":
        if (
            uri.realm == handle.realm
            and uri.subrealm == "commit"
            and len(uri.path) == 3  # noqa: PLR2004
            and f"{uri.path[0]}/{uri.path[1]}" == handle.repository.as_uri_segment()
        ):
            # TODO: `commit_id` will be "wrong" when it is a branch with "/",
            # since the translation in `resource_uri()` replaces them by "_".
            return GitLabCommitLocator(
                realm=handle.realm,
                repository=handle.repository,
                commit_id=FilePath.decode(uri.path[2]),
            )
        else:
            return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("commit"),
            path=[
                *self.repository.as_uri_segment(),
                FileName.decode(str(self.commit_id).replace("/", "_")),
            ],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"{self.repository.as_web_prefix()}/-/commit/{self.commit_id}",
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


AnyGitLabConnector = (
    GitLabRepositoryLocator
    | GitLabFileLocator
    | GitLabCompareLocator
    | GitLabCommitLocator
)


##
## Connector
##


@dataclass(kw_only=True)
class GitLabConnector(Connector):
    domain: str
    public_token: str
    repositories: dict[str, GitLabHandle | None]

    async def locator(  # noqa: PLR0911
        self,
        reference: RootReference,
    ) -> Locator | None:
        if isinstance(reference, WebUrl):
            if reference.domain != self.domain:
                return None

            repository = Repository.from_web(reference)
            if not repository:
                raise UnavailableError.new()

            # fmt: off
            handle = await self._acquire_handle(repository)
            locator = (
                GitLabRepositoryLocator.from_web(handle, reference)
                or GitLabFileLocator.from_web(handle, reference)
                or GitLabCompareLocator.from_web(handle, reference)
                or GitLabCommitLocator.from_web(handle, reference)
            )
            if not locator:
                raise UnavailableError.new()

            return locator
        elif isinstance(reference, ExternalUri):
            return None
        else:
            if reference.realm != self.realm:
                return None

            # If the repository cannot be inferred (common, e.g., for compare),
            # then rely on the resource history in storage.
            repository = Repository.from_uri(self.domain, reference)
            if not repository:
                return None

            # fmt: off
            handle = await self._acquire_handle(repository)
            locator = (
                GitLabRepositoryLocator.from_uri(handle, reference)
                or await GitLabFileLocator.from_uri(handle, reference)
                or GitLabCommitLocator.from_uri(handle, reference)
            )
            if not locator:
                return None

            return locator

    async def resolve(  # noqa: C901
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        assert isinstance(locator, AnyGitLabConnector)

        handle = await self._acquire_handle(locator.repository)
        repository_name = locator.repository.as_web_segment()

        metadata = MetadataDelta()
        name: str | None = None
        description: str | None = None
        revision: str | None = None
        affordances: list[AffordanceInfo] = []
        should_cache: bool = False

        if isinstance(locator, GitLabRepositoryLocator):
            should_cache = True
            name = f"Repository {repository_name}"
            description = handle.metadata.description or None
            if commit := await handle.get_commit(handle.metadata.default_branch):
                revision = commit.full_id
                # TODO: metadata.updated_at = commit.updated_at
            affordances = [AffordanceInfo(suffix=AffCollection.new())]

        elif isinstance(locator, GitLabFileLocator):
            path = "/".join(locator.path)
            if locator.mode == "blob":
                name = f"File {path} in repository {repository_name}"
                metadata = await handle.infer_file_metadata(locator.ref, locator.path)
                if (
                    locator.is_default_branch
                    and metadata.mime_type
                    and metadata.mime_type.mode() != "plain"
                ):
                    should_cache = True

                # TODO:
                # if (
                #     metadata.revision_data
                #     and (commit_id := FilePath.try_decode(metadata.revision_data))
                #     and (commit := await handle.get_commit(commit_id))
                # ):
                #     metadata.updated_at = commit.updated_at
                # else:
                #     raise UnavailableError.new()

                affordances = [AffordanceInfo(suffix=AffBody.new())]

                # Text files can be read (and possibly modified) by the client.
                mime_type = metadata.mime_type or MimeType.guess(locator.path[-1])
                if mime_type and mime_type.supports_plain():
                    affordances.append(AffordanceInfo(suffix=AffPlain.new()))

                # Files useful in Code Interpreter can be downloaded by the client.
                # TODO:
                # if mime_type and mime_type.mode() in ("image", "spreadsheet"):
                #     affordances.append(
                #         AffordanceInfo(suffix=AffFile.new(), mime_type=metadata.mime_type)
                #     )
            else:
                name = (
                    f"Folder {path} in repository {repository_name}"
                    if path
                    else f"Root folder in repository {repository_name}"
                )
                affordances.append(AffordanceInfo(suffix=AffCollection.new()))
            if not locator.is_default_branch:
                name += f" on {locator.ref}"

        elif isinstance(locator, GitLabCompareLocator):
            name = f"Compare {locator.before}...{locator.after} in repository {repository_name}"
            mime_type = MimeType.decode("text/markdown")

            # TODO:
            # if commit := await handle.get_commit(locator.before):
            #     metadata.created_at = commit.updated_at
            # else:
            #     raise UnavailableError.new()

            # TODO:
            # if commit := await handle.get_commit(locator.after):
            #     metadata.updated_at = commit.updated_at
            # else:
            #     raise UnavailableError.new()

            affordances = [AffordanceInfo(suffix=AffBody.new(), mime_type=mime_type)]

        elif isinstance(locator, GitLabCommitLocator):
            name = f"Commit {locator.commit_id} in repository {repository_name}"
            mime_type = MimeType.decode("text/markdown")

            # TODO:
            # if commit := await handle.get_commit(locator.commit_id):
            #     metadata.updated_at = commit.updated_at
            # else:
            #     raise UnavailableError.new()

            affordances = [AffordanceInfo(suffix=AffBody.new(), mime_type=mime_type)]

        else:
            _: Never = locator
            raise UnavailableError.new()

        # Generate a default name for the resource, but keep in mind that `read`
        # may override it, in which case, we prefer the latter.
        if cached and cached.metadata.name:
            name = None

        expired = bool(
            cached
            and revision is not None
            and cached.metadata.revision_data != revision
        )

        metadata = metadata.with_update(
            MetadataDelta(
                name=name,
                description=description,
                revision_data=revision,
                affordances=affordances,
            )
        )

        return ResolveResult(
            metadata=metadata,
            expired=[AffBody.new()] if expired else [],
            should_cache=should_cache,
        )

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyGitLabConnector)

        handle = await self._acquire_handle(locator.repository)
        if isinstance(locator, GitLabFileLocator) and locator.mode == "blob":
            file_metadata = await handle.infer_file_metadata(locator.ref, locator.path)
            if file_metadata.mime_type:
                mime_mode = file_metadata.mime_type.mode()
                if observable == AffPlain.new() and mime_mode not in (
                    "markdown",
                    "plain",
                ):
                    raise BadRequestError.observable(observable.as_suffix())
                # TODO:
                # elif observable == AffFile.new() and mime_mode != "spreadsheet":
                #     raise BadRequestError.observable(observable.as_suffix())

        match locator, observable:
            case (GitLabRepositoryLocator(), AffCollection()):
                return await _gitlab_read_repo_collection(handle, locator)
            case (GitLabFileLocator(), AffCollection()):
                return await _gitlab_read_file_collection(handle, locator)
            case (GitLabFileLocator(), AffBody()):
                return await _gitlab_read_file_body(handle, locator)
            # TODO:
            # case (GitLabFileLocator(), AffFile()):
            #     metadata, content = await _gitlab_read_file_file(handle, locator)
            case (GitLabFileLocator(), AffPlain()):
                return await _gitlab_read_file_plain(handle, locator)
            case (GitLabCompareLocator(), AffBody()):
                return await _gitlab_read_compare_body(handle, locator)
            case (GitLabCommitLocator(), AffBody()):
                return await _gitlab_read_commit_body(handle, locator)
            case _:
                raise BadRequestError.observable(observable.as_suffix())

    ##
    ## Implementation
    ##

    async def _acquire_handle(self, repository: Repository) -> GitLabHandle:
        handle_key = repository.as_web_segment()
        if handle_key not in self.repositories:
            # Initialize the handle by loading the repository metadata, to confirm
            # that the client is allowed to access the repository.  Otherwise, cache
            # the handle as `None` to always raises `UnavailableError`.
            try:
                self.repositories[handle_key] = await GitLabHandle.initialize(
                    context=self.context,
                    repository=repository,
                    realm=self.realm,
                    authorization=self._get_authorization(),
                )
            except Exception:
                self.repositories[handle_key] = None

        # Only "internal" repositories are allowed.
        # However, cache the repository handle, so its metadata need not be
        # fetched again within the same request.
        handle = self.repositories[handle_key]
        if not handle or not handle.metadata.visible:
            raise UnavailableError.new()

        return handle

    def _get_authorization(self) -> str:
        if private_token := self.context.creds.get(str(self.realm)):
            return f"Private-Token {private_token}"
        elif access_token := KnowledgeConfig.get(self.public_token):
            return f"Private-Token {access_token}"
        else:
            raise UnavailableError.new()


##
## Read - Repository
##


async def _gitlab_read_repo_collection(
    handle: GitLabHandle,
    locator: GitLabRepositoryLocator,
) -> ObserveResult:
    """
    When reading a GitLab repository as a collection, list all files that belong
    in one of its subprojects.  However, only the root folder is included in its
    relations, so that the files can be iteratively expanded from the repository
    root according to `relations_depth`.

    This allows the files of the repository to be ingested in Nightly jobs.
    """
    results = [
        GitLabFileLocator(
            realm=handle.realm,
            mode="blob",
            repository=handle.repository,
            ref=handle.metadata.default_branch,
            is_default_branch=True,
            path=f.parts(),
        )
        for f in await handle.files(handle.metadata.default_branch, [])
        if handle.infer_subproject(f)
    ]
    if not results:
        raise UnavailableError.new()

    root_folder_locator = GitLabFileLocator(
        realm=handle.realm,
        repository=handle.repository,
        ref=handle.metadata.default_branch,
        mode="tree",
        is_default_branch=True,
        path=[],
    )
    return ObserveResult(
        bundle=BundleCollection(
            uri=locator.resource_uri().child_affordance(AffCollection.new()),
            results=[loc.resource_uri() for loc in results],
        ),
        relations=[
            RelationParent(
                parent=locator.resource_uri(),
                child=root_folder_locator.resource_uri(),
            ),
        ],
        should_cache=False,
        option_relations_parent=False,
    )


##
## Read - File (Collection)
##


async def _gitlab_read_file_collection(
    handle: GitLabHandle,
    locator: GitLabFileLocator,
) -> ObserveResult:
    if locator.mode != "tree":
        raise UnavailableError.new()

    results = await _list_children_locators(
        handle,
        locator.ref,
        locator.path,
        locator.is_default_branch,
    )
    if not results:
        raise UnavailableError.new()

    return ObserveResult(
        bundle=BundleCollection(
            uri=locator.resource_uri().child_affordance(AffCollection.new()),
            results=[loc.resource_uri() for loc in results],
        ),
        should_cache=False,
        option_relations_parent=locator.is_default_branch,
    )


async def _list_children_locators(
    handle: GitLabHandle,
    ref: GitLabRef,
    base_path: list[FileName],
    is_default_branch: bool,
) -> list["GitLabFileLocator"]:
    all_files = await handle.files(ref, base_path)
    allowed_files = [f for f in all_files if handle.infer_subproject(f)]

    prefix_len = len(base_path)
    results: list[GitLabFileLocator] = []
    seen_folders: set[str] = set()
    seen_files: set[str] = set()

    for f in allowed_files:
        parts = f.parts()
        if base_path and parts[:prefix_len] != base_path:
            continue

        # Immediate file under the base path
        if len(parts) == prefix_len + 1:
            file_key = str(parts[prefix_len])
            if file_key in seen_files:
                continue
            results.append(
                GitLabFileLocator(
                    realm=handle.realm,
                    mode="blob",
                    repository=handle.repository,
                    ref=ref,
                    is_default_branch=is_default_branch,
                    path=parts[: prefix_len + 1],
                )
            )
            seen_files.add(file_key)

        # Folder under the base path
        elif len(parts) > prefix_len + 1:
            folder_name = parts[prefix_len]
            folder_key = str(folder_name)
            if folder_key in seen_folders:
                continue
            results.append(
                GitLabFileLocator(
                    realm=handle.realm,
                    mode="tree",
                    repository=handle.repository,
                    ref=ref,
                    is_default_branch=is_default_branch,
                    path=[*base_path, folder_name],
                )
            )
            seen_folders.add(folder_key)

    return results


##
## Read - File (Document)
##


async def _gitlab_read_file_body(
    handle: GitLabHandle,
    locator: GitLabFileLocator,
) -> ObserveResult:
    if locator.mode != "blob":
        raise UnavailableError.new()

    response = await handle.read_file(locator.ref, locator.path, original=False)

    metadata = MetadataDelta()
    mode = response.mode
    text = strip_keep_indent(response.text)
    relations: list[Relation] = []

    # Give special treatment to Markdown files:
    # - Override the metadata using the YAML frontmatter.
    # - Replace relative links with absolute GitLab URLs
    if locator.path[-1].endswith((".md", ".mdx")):
        mode = "markdown"
        metadata, relations = _process_markdown_frontmatter(text)
        text = _process_markdown_links(locator, text)

    return ObserveResult(
        bundle=Fragment(mode=mode, text=text, blobs=response.blobs),
        metadata=metadata,
        relations=relations,
        should_cache=response.mime_type.mode() in ("document", "media"),
        option_labels=locator.is_default_branch and mode != "plain",
        option_relations_link=locator.is_default_branch and mode == "markdown",
    )


async def _gitlab_read_file_plain(
    handle: GitLabHandle,
    locator: GitLabFileLocator,
) -> ObserveResult:
    if locator.mode != "blob":
        raise UnavailableError.new()

    response = await handle.read_file(locator.ref, locator.path, original=True)
    return ObserveResult(
        bundle=BundlePlain(
            uri=locator.resource_uri().child_affordance(AffPlain.new()),
            mime_type=response.mime_type,
            text=response.text,
        ),
        should_cache=False,
    )


def _process_markdown_frontmatter(content: str) -> tuple[MetadataDelta, list[Relation]]:
    """
    TODO: Handle relations.
    TODO: Handle citation URL and aliases.
    """
    mime_type = MimeType.decode("text/markdown")

    if not content.startswith("---\n"):
        return MetadataDelta(mime_type=mime_type), []
    parts = content[4:].split("\n---\n", 1)
    if len(parts) != 2:  # noqa: PLR2004
        return MetadataDelta(mime_type=mime_type), []

    name: str | None = None
    description: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    with contextlib.suppress(Exception):
        frontmatter = yaml.safe_load(parts[0])
        content = parts[1]

        name = frontmatter.get("title")
        description = frontmatter.get("description")
        if created_str := frontmatter.get("created_at"):
            with contextlib.suppress(Exception):
                created_at = dateutil.parser.parse(created_str)

    metadata = MetadataDelta(
        mime_type=mime_type,
        name=name,
        description=description,
        created_at=created_at,
        updated_at=updated_at,
    )
    return metadata, []


def _process_markdown_links(
    locator: GitLabFileLocator,
    content: str,
) -> str:
    # Find relative paths in markdown links.
    relative_paths: set[str] = set()
    for part_mode, part_text in markdown_split_code(content, split_exprs=True):
        if part_mode == "text":
            relative_paths.update(re.findall(rf"\]\(({REGEX_FILEPATH})\)", part_text))

    # Replace each relative path with absolute GitLab URL.
    base_url = locator.content_url()
    for relative_path in relative_paths:
        if absolute_url := base_url.try_join_href(relative_path):
            content = content.replace(f"]({relative_path})", f"]({absolute_url})")

    return content


##
## Read - Compare
##


async def _gitlab_read_compare_body(
    handle: GitLabHandle,
    locator: GitLabCompareLocator,
) -> ObserveResult:
    _, compare_data = await handle.fetch_endpoint_json(
        f"repository/compare?from={quote(locator.before)}&to={quote(locator.after)}"
    )
    if not compare_data.get("commits"):
        return ObserveResult(
            bundle=Fragment(mode="plain", text="No commits.", blobs={}),
            should_cache=False,
            option_labels=False,
            option_relations_link=False,
        )

    commits = compare_data["commits"]
    commits_text = "\n".join(
        f'<commit title="{commit["title"]}" author="{commit["author_name"]}" created_at="{commit["created_at"]}" />'
        for commit in compare_data.get("commits", [])
    )

    diffs_text = (
        "\n".join(
            diff_text
            for diff_data in compare_data.get("diffs", [])
            if (diff_text := _format_gitlab_diff(handle, diff_data))
        )
        or "No changes."
    )

    content = f"""<commits>
{commits_text}
</commits>
<diffs>
{diffs_text}
</diffs>"""

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=content, blobs={}),
        metadata=MetadataDelta(
            created_at=dateutil.parser.parse(commits[0]["created_at"]),
            updated_at=dateutil.parser.parse(commits[-1]["created_at"]),
        ),
        should_cache=False,
        option_labels=True,
        option_relations_link=False,
    )


##
## Read - Commit
##


async def _gitlab_read_commit_body(
    handle: GitLabHandle,
    locator: GitLabCommitLocator,
) -> ObserveResult:
    """Read a GitLab commit as a document."""
    _, commit_data = await handle.fetch_endpoint_json(
        f"repository/commits/{locator.commit_id}"
    )

    diffs_text = (
        "\n".join(
            diff_text
            for diff_data in commit_data.get("diffs", [])
            if (diff_text := _format_gitlab_diff(handle, diff_data))
        )
        or "No changes."
    )

    content = f"""\
<commit title="{commit_data["title"]}" author="{commit_data["author_name"]}" created_at="{commit_data["created_at"]}">
{diffs_text}
</commit>\
"""

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=content, blobs={}),
        metadata=MetadataDelta(
            created_at=dateutil.parser.parse(commit_data["created_at"]),
        ),
        should_cache=False,
        option_labels=False,
        option_relations_link=False,
    )


def _format_gitlab_diff(
    handle: GitLabHandle,
    diff_data: dict[str, Any],
) -> str | None:
    """Format a GitLab diff entry into plain text."""
    path = diff_data.get("new_path") or diff_data.get("old_path")
    if not path:
        return None

    if not diff_data.get("new_path"):
        return f'<file_diff path="{path}" change="deleted" />'

    elif not diff_data.get("old_path"):
        change = "created"
    elif diff_data.get("old_path") != diff_data.get("new_path"):
        change = f"renamed from {diff_data['old_path']}"
    else:
        change = "updated"

    diff_content = (
        "```\n" + re.sub(r"^```", "\\```", diff.strip().replace("\r\n", "\n")) + "\n```"
        if (diff := diff_data.get("diff"))
        else "Changes omitted."
    )
    if file_path := FilePath.try_decode(path):
        file_uri = f"ndk://{handle.realm}/file/{handle.repository.as_web_segment()}/{file_path}"
        return f'<file_diff uri="{file_uri}" change="{change}">\n{diff_content}\n</file_diff>'

    else:
        return (
            f'<file_diff path="{path}" change="{change}">\n{diff_content}\n</file_diff>'
        )


##
## Configuration
##


# Files that should be included in GitLabRepository contents.
GITLAB_DEFAULT_ALLOWED = [
    ".dockerignore",
    ".env.sample",
    ".gitignore",
    ".htaccess",
    "*.aidl",
    "*.bat",
    "*.c",
    "*.cmake",
    "*.cmd",
    "*.cpp",
    "*.cs",
    "*.csv",
    "*.csproj",
    "*.css",
    "*.env",
    "*.go",
    "*.gradle",
    "*.h",
    "*.hpp",
    "*.html",
    "*.iml",
    "*.java",
    "*.js",
    "*.json",
    "*.jsx",
    "*.kt",
    "*.less",
    "*.lua",
    "*.m",
    "*.md",
    "*.mdx",
    "*.mod",
    "*.mustache",
    "*.php",
    "*.pro",
    "*.properties",
    "*.pug",
    "*.py",
    "*.rb",
    "*.rs",
    "*.sass",
    "*.scss",
    "*.sh",
    "*.sln",
    "*.sql",
    "*.swift",
    "*.toml",
    "*.ts",
    "*.tsx",
    "*.txt",
    "*.xml",
    "*.yaml",
    "*.yml",
    "build.gradle",
    "docker.gradle",
    "Dockerfile*",
    "settings.gradle",
]

# Files that should be omitted from GitLabRepository contents.
GITLAB_DEFAULT_SKIPPED_SUBSTR = [
    "__pycache__/",
    ".git/",
    ".idea/",
    ".mypy_cache/",
    ".obsidian/",
    ".pytest_cache/",
    ".venv/",
    ".vscode/",
    "build/",
    "legacy/",
    "lib/",
    "node_modules/",
    "target/",
    "__init__.py",
    ".dockerignore",
    ".editorconfig",
    ".gitignore",
    ".min.js",
    "deno.lock",
    "package-lock.json",
    "poetry.lock",
    "nandam.yml",
    "tsconfig.json",
    "tslint.json",
    "workspace.code-workspace",
]

# Files that should be omitted from GitLabRepository contents when present at
# the root of the project.
GITLAB_DEFAULT_SKIPPED_ABSOLUTE = [
    "version.js",
]
