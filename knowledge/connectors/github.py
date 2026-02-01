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
from base.resources.aff_file import AffFile
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


class GitHubConnectorConfig(BaseModel):
    kind: Literal["github"] = "github"
    realm: Realm
    public_token: str | None

    def instantiate(self, context: KnowledgeContext) -> "GitHubConnector":
        return GitHubConnector(
            context=context,
            realm=self.realm,
            public_token=self.public_token,
            repositories={},
        )


##
## Context
##


GitHubRef = FilePath
"""
Expect tags, branches, and commit hashes to respect the `FilePath` regex.
"""


class GitHubRepository(BaseModel, frozen=True):
    """
    GitHub repository with owner and name.
    """

    owner: FileName
    repo: FileName

    @staticmethod
    def from_web(url: WebUrl) -> "GitHubRepository | None":
        # GitHub URLs are like: github.com/{owner}/{repo}
        # Remove trailing paths like /tree/main, /blob/main/file, etc.
        path_parts = url.path.strip("/").split("/")
        if len(path_parts) < 2:  # noqa: PLR2004
            return None

        owner_str, repo_str = path_parts[0], path_parts[1]

        # Validate owner and repo names
        owner = FileName.try_decode(owner_str)
        repo = FileName.try_decode(repo_str)

        if not owner or not repo:
            return None

        return GitHubRepository(owner=owner, repo=repo)

    @staticmethod
    def from_uri(uri: ResourceUri) -> "GitHubRepository | None":
        if (
            uri.subrealm in ("commit", "file", "repository") and len(uri.path) >= 2  # noqa: PLR2004
        ):
            uri_owner, uri_repo, *_path = uri.path
        elif uri.subrealm == "ref" and len(uri.path) >= 3:  # noqa: PLR2004
            _ref, uri_owner, uri_repo, *_path = uri.path
        else:
            return None

        return GitHubRepository(owner=uri_owner, repo=uri_repo)

    def as_web_prefix(self) -> WebUrl:
        return WebUrl(
            domain="github.com",
            port=443,
            path=self.as_web_segment(),
            path_prefix=None,
            query_path=None,
            query=[],
            fragment="",
        )

    def as_web_segment(self) -> str:
        return f"{self.owner}/{self.repo}"

    def as_uri_segment(self) -> list[FileName]:
        return [self.owner, self.repo]

    def as_encoded(self) -> str:
        return f"{self.owner}/{self.repo}"

    def is_writable(self) -> bool:
        return self.owner in ("knowledge",)


@dataclass(kw_only=True, frozen=True)
class RepositoryMetadata:
    id: int
    visible: bool
    archived: bool
    default_branch: GitHubRef
    description: str
    updated_at: datetime | None

    @staticmethod
    async def load(
        context: KnowledgeContext,
        authorization: str | None,
        repository: GitHubRepository,
    ) -> "RepositoryMetadata":
        downloader = context.service(SvcDownloader)
        url = f"https://api.github.com/repos/{repository.as_encoded()}"
        try:
            response = await downloader.documents_read_download(
                url=WebUrl.decode(url),
                authorization=authorization,
                headers={"accept": "application/vnd.github+json"},
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
                or FilePath.decode("main")
            ),
            description=data.get("description") or "",
            id=data["id"],
            updated_at=(
                dateutil.parser.parse(updated_at)
                if (updated_at := data.get("updated_at"))
                else None
            ),
            visible=not data.get("private", True),  # Public repos are visible
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
        repository: GitHubRepository,
        ref: GitHubRef,
    ) -> "RepositoryConfig | None":
        downloader = context.service(SvcDownloader)
        url = (
            f"https://api.github.com/repos/{repository.as_encoded()}"
            f"/contents/nandam.yml?ref={ref}"
        )
        try:
            response = await downloader.documents_read_download(
                url=WebUrl.decode(url),
                authorization=authorization,
                headers={"accept": "application/vnd.github.raw+json"},
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


class GitHubCommit(BaseModel, frozen=True):
    full_id: GitHubRef
    short_id: GitHubRef

    @staticmethod
    def parse(data: dict[str, Any]) -> "GitHubCommit":
        sha = data["sha"]
        return GitHubCommit(
            full_id=FilePath.decode(sha),
            short_id=FilePath.decode(sha[:7]),
        )


class GitHubBranch(BaseModel, frozen=True):
    name: GitHubRef
    commit_id: GitHubRef

    @staticmethod
    async def load(
        context: KnowledgeContext,
        authorization: str | None,
        repository: GitHubRepository,
    ) -> tuple[list["GitHubBranch"], list[GitHubCommit]]:
        downloader = context.service(SvcDownloader)
        url = (
            f"https://api.github.com/repos/{repository.as_encoded()}"
            f"/branches?per_page=100"
        )
        try:
            response = await downloader.documents_read_download(
                url=WebUrl.decode(url),
                authorization=authorization,
                headers={"accept": "application/vnd.github+json"},
                original=True,
            )
            data: list[dict[str, Any]] = json.loads(response.text)
            return GitHubBranch.parse(data)
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
    ) -> tuple[list["GitHubBranch"], list[GitHubCommit]]:
        branches: list[GitHubBranch] = []
        commits: list[GitHubCommit] = []
        for branch_data in data:
            if branch_name := FilePath.try_decode(branch_data["name"]):
                commit = GitHubCommit.parse(branch_data["commit"])
                branch = GitHubBranch(name=branch_name, commit_id=commit.full_id)
                commits.append(commit)
                branches.append(branch)

        return branches, commits


@dataclass(kw_only=True)
class GitHubFile:
    path: FilePath
    subproject: FileName | None


@dataclass(kw_only=True)
class GitHubHandle:
    context: KnowledgeContext
    realm: Realm
    repository: GitHubRepository
    metadata: RepositoryMetadata
    config: RepositoryConfig
    branches: list[GitHubBranch]

    # Caches:
    _authorization: str | None
    _files: dict[GitHubRef, list[FilePath]]
    _file_metadata: dict[str, MetadataDelta | None]
    _commits: dict[GitHubRef, GitHubCommit | None]

    @staticmethod
    async def initialize(
        context: KnowledgeContext,
        realm: Realm,
        repository: GitHubRepository,
        authorization: str | None,
    ) -> "GitHubHandle":
        metadata = await RepositoryMetadata.load(context, authorization, repository)

        branches, commits = await GitHubBranch.load(context, authorization, repository)
        if not any(b.name == metadata.default_branch for b in branches):
            logger.error(
                "Repository %s has an invalid default branch %s",
                repository.as_web_segment(),
                metadata.default_branch,
            )
            raise UnavailableError.new()

        return GitHubHandle(
            context=context,
            realm=realm,
            repository=repository,
            metadata=metadata,
            config=RepositoryConfig(),
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
        return self.repository.as_encoded()

    def _api_base(self) -> str:
        return f"https://api.github.com/repos/{self._encoded_repository()}"

    def _repo_endpoint(self, endpoint: str) -> str:
        base = self._api_base()
        return f"{base}/{endpoint}" if endpoint else base

    def _file_raw_url(self, ref: GitHubRef, path: list[FileName]) -> str:
        encoded_path = "/".join(path)
        return self._repo_endpoint(f"contents/{encoded_path}?ref={ref}")

    async def files(self, ref: GitHubRef, prefix: list[FileName]) -> list[FilePath]:
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
        ref: GitHubRef,
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

    def get_branch(self, ref: GitHubRef) -> GitHubBranch | None:
        for branch in self.branches:
            if branch.name == ref:
                return branch
        return None

    async def get_commit(self, ref: GitHubRef) -> GitHubCommit | None:
        if branch := self.get_branch(ref):
            ref = branch.commit_id
        if ref not in self._commits:
            self._commits[ref] = await self._uncached_get_commit(ref)
        return self._commits[ref]

    async def infer_file_metadata(
        self,
        ref: GitHubRef,
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

    def infer_subproject(  # noqa: C901, PLR0911
        self,
        path: FilePath,
    ) -> FileName | None:
        if any(s in path for s in GITHUB_DEFAULT_SKIPPED_SUBSTR):
            return None
        if any(path.startswith(s) for s in GITHUB_DEFAULT_SKIPPED_ABSOLUTE):
            return None

        if not any(path.startswith(s) for s in self.config.allowed):
            if any(path.startswith(s) for s in self.config.skipped):
                return None
            if any(path.startswith(s) for s in self.config.skipped_notify):
                return None  # TODO: Handle "skipped".

            filename = path.rsplit("/", maxsplit=1)[-1]
            if not any(fnmatch.fnmatch(filename, p) for p in GITHUB_DEFAULT_ALLOWED):
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
    ) -> tuple[GitHubRef, list[FileName]]:
        # Check whether we are looking at a path on a branch, whose name may
        # contain "/".  Otherwise, assume that it is a tag or a commit, wwhere
        # "/" is not allowed in our scheme.
        ref: GitHubRef = FilePath.decode(ref_and_path.split("/", 1)[0])
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
                headers={"accept": "application/vnd.github+json"},
                original=True,
            )
            return response.headers, json.loads(response.text)
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception("GitHubHandle.fetch_endpoint_json failed: %s", url)
            raise UnavailableError.new()  # noqa: B904

    async def read_file(
        self,
        ref: GitHubRef,
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
                logger.exception("GitHubHandle.read_file failed: %s", url)
            raise UnavailableError.new()  # noqa: B904

    ##
    ## Implementation
    ##

    async def _uncached_files(self, ref: GitHubRef) -> list[FilePath]:
        # First, resolve the ref to a commit SHA
        branch = self.get_branch(ref)
        commit_sha = branch.commit_id if branch else ref

        # Get the tree recursively using GitHub's Git Data API
        _, tree_data = await self.fetch_endpoint_json(
            f"git/trees/{commit_sha}?recursive=1"
        )

        return [
            file_path
            for item in tree_data.get("tree", [])
            if item.get("type") == "blob"
            and (file_path := FilePath.try_decode(item.get("path")))
        ]

    async def _uncached_get_commit(self, ref: GitHubRef) -> GitHubCommit | None:
        try:
            _, data = await self.fetch_endpoint_json(f"commits/{ref}")
            return GitHubCommit.parse(data)
        except Exception:
            return None

    async def _uncached_infer_file_metadata(
        self,
        ref: GitHubRef,
        path: list[FileName],
    ) -> MetadataDelta | None:
        try:
            # Get file info from GitHub API to get mime type and latest commit
            encoded_path = "/".join(path)
            _, file_data = await self.fetch_endpoint_json(
                f"contents/{encoded_path}?ref={ref}"
            )

            mime_type: MimeType | None = None
            revision: str | None = None

            # Infer mime type from file extension if not provided
            file_name = path[-1] if path else ""
            if "." in file_name:
                ext = file_name.rsplit(".", 1)[-1].lower()
                mime_map = {
                    "md": "text/markdown",
                    "txt": "text/plain",
                    "py": "text/x-python",
                    "js": "text/javascript",
                    "json": "application/json",
                    "html": "text/html",
                    "css": "text/css",
                    "yaml": "text/yaml",
                    "yml": "text/yaml",
                }
                if mime_str := mime_map.get(ext):
                    mime_type = MimeType.try_decode(mime_str)

            # Get the SHA from the file metadata
            if sha := file_data.get("sha"):
                revision = sha

            return MetadataDelta(mime_type=mime_type, revision_data=revision)
        except Exception:
            if KnowledgeConfig.verbose:
                logger.exception(
                    "GitHubHandle.infer_file_metadata failed for path: %s",
                    "/".join(path),
                )
            return None


##
## Locators
##


REGEX_GITHUB_OWNER_WEB_PATH = r"[A-Za-z0-9\-]+"
REGEX_GITHUB_REPOSITORY_WEB_PATH = rf"{REGEX_GITHUB_OWNER_WEB_PATH}/{REGEX_FILENAME}"


class GitHubRepositoryLocator(Locator, frozen=True):
    kind: Literal["github_repository"] = "github_repository"
    repository: GitHubRepository

    @staticmethod
    def from_web(
        handle: GitHubHandle,
        url: WebUrl,
    ) -> "GitHubRepositoryLocator | None":
        locator = GitHubRepositoryLocator(
            realm=handle.realm,
            repository=handle.repository,
        )
        if url == handle.repository.as_web_prefix():
            return locator
        else:
            return None

    @staticmethod
    def from_uri(
        handle: GitHubHandle,
        uri: ResourceUri,
    ) -> "GitHubRepositoryLocator | None":
        locator = GitHubRepositoryLocator(
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


class GitHubFileLocator(Locator, frozen=True):
    kind: Literal["github_file"] = "github_file"
    repository: GitHubRepository
    ref: GitHubRef
    mode: Literal["blob", "tree"]
    is_default_branch: bool
    path: list[FileName]

    @staticmethod
    def from_web(
        handle: GitHubHandle,
        url: WebUrl,
    ) -> "GitHubFileLocator | None":
        if url.domain != "github.com":
            return None

        mode: Literal["blob", "tree"]
        ref_and_path: str
        repo_segment = handle.repository.as_web_segment()

        # GitHub URL patterns: owner/repo/blob/ref/path or owner/repo/tree/ref/path
        if url.path.startswith(f"{repo_segment}/blob/"):
            mode = "blob"
            ref_and_path = url.path.removeprefix(f"{repo_segment}/blob/")
        elif url.path.startswith(f"{repo_segment}/tree/"):
            mode = "tree"
            ref_and_path = url.path.removeprefix(f"{repo_segment}/tree/")
        else:
            return None

        # NOTE: Ignore paths with components that are not valid `FileName`,
        # i.e., those containing whitespace or special characters.  Check it
        # immediately, so the rest of the code can assume a valid path.
        if not FilePath.try_decode(ref_and_path):
            return None

        ref, path = handle.split_ref_and_path("web", ref_and_path)

        return GitHubFileLocator(
            realm=handle.realm,
            repository=handle.repository,
            ref=ref,
            mode=mode,
            is_default_branch=ref == handle.metadata.default_branch,
            path=path,
        )

    @staticmethod
    async def from_uri(
        handle: GitHubHandle,
        uri: ResourceUri,
    ) -> "GitHubFileLocator | None":
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

        return GitHubFileLocator(
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
        # For blobs, GitHub uses the same blob URL (no raw endpoint needed for viewing)
        path_suffix = "/" + "/".join(quote(f) for f in self.path) if self.path else ""
        return WebUrl.decode(
            f"{self.repository.as_web_prefix()}/{self.mode}/"
            f"{quote(self.ref, safe='/')}{path_suffix}",
        )

    def citation_url(self) -> WebUrl:
        path_suffix = "/" + "/".join(quote(f) for f in self.path) if self.path else ""
        return WebUrl.decode(
            f"{self.repository.as_web_prefix()}/{self.mode}/"
            f"{quote(self.ref, safe='/')}{path_suffix}",
        )


class GitHubCompareLocator(Locator, frozen=True):
    kind: Literal["github_compare"] = "github_compare"
    repository: GitHubRepository
    before: GitHubRef
    after: GitHubRef

    @staticmethod
    def from_web(
        handle: GitHubHandle,
        url: WebUrl,
    ) -> "GitHubCompareLocator | None":
        if url.domain != "github.com":
            return None

        path_prefix = f"{handle.repository.as_web_segment()}/compare/"
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

        return GitHubCompareLocator(
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
            f"{self.repository.as_web_prefix()}/compare/{quote(self.before)}...{quote(self.after)}",
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


class GitHubCommitLocator(Locator, frozen=True):
    kind: Literal["github_commit"] = "github_commit"
    repository: GitHubRepository
    commit_id: GitHubRef

    @staticmethod
    def from_web(
        handle: GitHubHandle,
        url: WebUrl,
    ) -> "GitHubCommitLocator | None":
        if url.domain != "github.com":
            return None

        path_prefix = f"{handle.repository.as_web_segment()}/commit/"
        if not url.path.startswith(path_prefix):
            return None

        commit_part = url.path.removeprefix(path_prefix)
        if not re.fullmatch(r"[0-9a-f]{7,40}", commit_part):
            return None

        return GitHubCommitLocator(
            realm=handle.realm,
            repository=handle.repository,
            commit_id=FilePath.decode(commit_part),
        )

    @staticmethod
    def from_uri(
        handle: GitHubHandle,
        uri: ResourceUri,
    ) -> "GitHubCommitLocator | None":
        if (
            uri.realm == handle.realm
            and uri.subrealm == "commit"
            and len(uri.path) == 3  # noqa: PLR2004
            and f"{uri.path[0]}/{uri.path[1]}" == handle.repository.as_uri_segment()
        ):
            # TODO: `commit_id` will be "wrong" when it is a branch with "/",
            # since the translation in `resource_uri()` replaces them by "_".
            return GitHubCommitLocator(
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
            f"{self.repository.as_web_prefix()}/commit/{self.commit_id}",
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


AnyGitHubConnector = (
    GitHubRepositoryLocator
    | GitHubFileLocator
    | GitHubCompareLocator
    | GitHubCommitLocator
)


##
## Connector
##


@dataclass(kw_only=True)
class GitHubConnector(Connector):
    public_token: str | None
    repositories: dict[str, GitHubHandle | None]

    async def locator(  # noqa: PLR0911
        self,
        reference: RootReference,
    ) -> Locator | None:
        if isinstance(reference, WebUrl):
            if reference.domain != "github.com":
                return None

            repository = GitHubRepository.from_web(reference)
            if not repository:
                raise UnavailableError.new()

            # fmt: off
            handle = await self._acquire_handle(repository)
            locator = (
                GitHubRepositoryLocator.from_web(handle, reference)
                or GitHubFileLocator.from_web(handle,reference)
                or GitHubCompareLocator.from_web(handle, reference)
                or GitHubCommitLocator.from_web(handle,reference)
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
            repository = GitHubRepository.from_uri(reference)
            if not repository:
                return None

            # fmt: off
            handle = await self._acquire_handle(repository)
            locator = (
                GitHubRepositoryLocator.from_uri(handle, reference)
                or await GitHubFileLocator.from_uri(handle,reference)
                or GitHubCommitLocator.from_uri(handle, reference)
            )
            if not locator:
                return None

            return locator

    async def resolve(  # noqa: C901, PLR0912
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        assert isinstance(locator, AnyGitHubConnector)

        handle = await self._acquire_handle(locator.repository)
        repository_name = locator.repository.as_web_segment()

        metadata = MetadataDelta()
        name: str | None = None
        description: str | None = None
        revision: str | None = None
        affordances: list[AffordanceInfo] = []
        should_cache: bool = False

        if isinstance(locator, GitHubRepositoryLocator):
            should_cache = True
            name = f"Repository {repository_name}"
            description = handle.metadata.description or None
            if commit := await handle.get_commit(handle.metadata.default_branch):
                revision = commit.full_id
                # TODO: metadata.updated_at = commit.updated_at
            # TODO: Add "$body" affordance, where subprojects are sections?
            affordances = [AffordanceInfo(suffix=AffCollection.new())]

        elif isinstance(locator, GitHubFileLocator):
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

                if metadata.mime_type:
                    # Text files can be read (and possibly modified) by the client.
                    mime_mode = metadata.mime_type.mode()
                    # Spreadsheets can be downloaded by the client for Code Interpreter.
                    # TODO:
                    # if mime_mode == "spreadsheet":
                    #     affordances.append(
                    #         AffordanceInfo(suffix=AffFile.new(), mime_type=metadata.mime_type)
                    #     )
                    if mime_mode in ("markdown", "plain"):
                        # TODO:
                        # writable=locator.is_default_branch and locator.repository.is_writable()
                        affordances.append(AffordanceInfo(suffix=AffPlain.new()))
            else:
                name = (
                    f"Folder {path} in repository {repository_name}"
                    if path
                    else f"Root folder in repository {repository_name}"
                )
                affordances.append(AffordanceInfo(suffix=AffCollection.new()))
            if not locator.is_default_branch:
                name += f" on {locator.ref}"

        elif isinstance(locator, GitHubCompareLocator):
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

        elif isinstance(locator, GitHubCommitLocator):
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

    async def observe(  # noqa: C901
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyGitHubConnector)

        handle = await self._acquire_handle(locator.repository)
        if isinstance(locator, GitHubFileLocator) and locator.mode == "blob":
            file_metadata = await handle.infer_file_metadata(locator.ref, locator.path)
            if file_metadata.mime_type:
                mime_mode = file_metadata.mime_type.mode()
                if observable == AffPlain.new() and mime_mode not in (  # noqa: SIM114
                    "markdown",
                    "plain",
                ):
                    raise BadRequestError.observable(observable.as_suffix())
                elif observable == AffFile.new() and mime_mode != "spreadsheet":
                    raise BadRequestError.observable(observable.as_suffix())

        match locator, observable:
            case (GitHubRepositoryLocator(), AffCollection()):
                return await _github_read_repo_collection(handle, locator)
            case (GitHubFileLocator(), AffCollection()):
                return await _github_read_file_collection(handle, locator)
            case (GitHubFileLocator(), AffBody()):
                return await _github_read_file_body(handle, locator)
            # TODO:
            # case (GitHubFileLocator(), AffFile(path=[])):
            #     metadata, content = await _github_read_file_file(handle, locator)
            case (GitHubFileLocator(), AffPlain()):
                return await _github_read_file_plain(handle, locator)
            case (GitHubCompareLocator(), AffBody()):
                return await _github_read_compare_body(handle, locator)
            case (GitHubCommitLocator(), AffBody()):
                return await _github_read_commit_body(handle, locator)
            case _:
                raise BadRequestError.observable(observable.as_suffix())

    ##
    ## Implementation
    ##

    async def _acquire_handle(self, repository: GitHubRepository) -> GitHubHandle:
        handle_key = repository.as_web_segment()
        if handle_key not in self.repositories:
            # Initialize the handle by loading the repository metadata, to confirm
            # that the client is allowed to access the repository.  Otherwise, cache
            # the handle as `None` to always raises `UnavailableError`.
            try:
                self.repositories[handle_key] = await GitHubHandle.initialize(
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

    def _get_authorization(self) -> str | None:
        if authorization := self.context.get_bearer_authorization(
            self.realm, self.public_token
        ):
            return authorization[0]
        else:
            return None


##
## Read - Repository
##


async def _github_read_repo_collection(
    handle: GitHubHandle,
    locator: GitHubRepositoryLocator,
) -> ObserveResult:
    """
    When reading a GitHub repository as a collection, list all files that belong
    in one of its subprojects.  However, only the root folder is included in its
    relations, so that the files can be iteratively expanded from the repository
    root according to `relations_depth`.

    This allows the files of the repository to be ingested in Nightly jobs.
    """
    results = [
        GitHubFileLocator(
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

    root_folder_locator = GitHubFileLocator(
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


async def _github_read_file_collection(
    handle: GitHubHandle,
    locator: GitHubFileLocator,
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
    handle: GitHubHandle,
    ref: GitHubRef,
    base_path: list[FileName],
    is_default_branch: bool,
) -> list["GitHubFileLocator"]:
    all_files = await handle.files(ref, base_path)
    allowed_files = [f for f in all_files if handle.infer_subproject(f)]

    prefix_len = len(base_path)
    results: list[GitHubFileLocator] = []
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
                GitHubFileLocator(
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
                GitHubFileLocator(
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


async def _github_read_file_body(
    handle: GitHubHandle,
    locator: GitHubFileLocator,
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
    # - Replace relative links with absolute GitHub URLs
    if locator.path[-1].endswith((".md", ".mdx")):
        mode = "markdown"
        metadata, relations = _process_markdown_frontmatter(text)
        text = _process_markdown_links(locator, text)

    return ObserveResult(
        bundle=Fragment(mode=mode, text=text, blobs=response.blobs),
        metadata=metadata,
        relations=relations,
        should_cache=response.mime_type.mode() in ("document", "media"),
        option_labels=locator.is_default_branch and mode == "markdown",
        option_relations_link=locator.is_default_branch and mode == "markdown",
    )


async def _github_read_file_plain(
    handle: GitHubHandle,
    locator: GitHubFileLocator,
) -> ObserveResult:
    if locator.mode != "blob":
        raise UnavailableError.new()

    response = await handle.read_file(locator.ref, locator.path, original=True)
    return ObserveResult(
        bundle=BundlePlain(
            uri=locator.resource_uri().child_affordance(AffPlain.new()),
            mime_type=response.mime_type,
            text=response.text,
            # TODO: writable=locator.is_default_branch and locator.repository.is_writable(),
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


def _process_markdown_links(locator: GitHubFileLocator, content: str) -> str:
    # Find relative paths in markdown links.
    relative_paths: set[str] = set()
    for part_mode, part_text in markdown_split_code(content, split_exprs=True):
        if part_mode == "text":
            relative_paths.update(re.findall(rf"\]\(({REGEX_FILEPATH})\)", part_text))

    # Replace each relative path with absolute GitHub URL.
    base_url = locator.content_url()
    for relative_path in relative_paths:
        if absolute_url := base_url.try_join_href(relative_path):
            content = content.replace(f"]({relative_path})", f"]({absolute_url})")

    return content


##
## Read - Compare
##


async def _github_read_compare_body(
    handle: GitHubHandle,
    locator: GitHubCompareLocator,
) -> ObserveResult:
    _, compare_data = await handle.fetch_endpoint_json(
        f"compare/{quote(locator.before)}...{quote(locator.after)}"
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
        f'<commit title="{commit["commit"]["message"].splitlines()[0]}" author="{commit["commit"]["author"]["name"]}" created_at="{commit["commit"]["author"]["date"]}" />'
        for commit in commits
    )

    diffs_text = (
        "\n".join(
            diff_text
            for diff_data in compare_data.get("files", [])
            if (diff_text := _format_github_diff(handle, diff_data))
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
            created_at=dateutil.parser.parse(commits[0]["commit"]["author"]["date"]),
            updated_at=dateutil.parser.parse(commits[-1]["commit"]["author"]["date"]),
        ),
        should_cache=False,
        option_labels=True,
        option_relations_link=False,
    )


##
## Read - Commit
##


async def _github_read_commit_body(
    handle: GitHubHandle,
    locator: GitHubCommitLocator,
) -> ObserveResult:
    """Read a GitHub commit as a document."""
    _, commit_data = await handle.fetch_endpoint_json(f"commits/{locator.commit_id}")

    diffs_text = (
        "\n".join(
            diff_text
            for diff_data in commit_data.get("files", [])
            if (diff_text := _format_github_diff(handle, diff_data))
        )
        or "No changes."
    )

    commit_message = commit_data["commit"]["message"].splitlines()[0]
    author_name = commit_data["commit"]["author"]["name"]
    created_at = commit_data["commit"]["author"]["date"]

    content = f"""\
<commit title="{commit_message}" author="{author_name}" created_at="{created_at}">
{diffs_text}
</commit>\
"""

    return ObserveResult(
        bundle=Fragment(mode="markdown", text=content, blobs={}),
        metadata=MetadataDelta(
            created_at=dateutil.parser.parse(created_at),
        ),
        should_cache=False,
        option_labels=False,
        option_relations_link=False,
    )


def _format_github_diff(
    handle: GitHubHandle,
    diff_data: dict[str, Any],
) -> str | None:
    """Format a GitHub diff entry into plain text."""
    path = diff_data.get("filename")
    if not path:
        return None

    status = diff_data.get("status", "modified")

    if status == "removed":
        return f'<file_diff path="{path}" change="deleted" />'
    elif status == "added":
        change = "created"
    elif status == "renamed":
        previous_filename = diff_data.get("previous_filename", "")
        change = f"renamed from {previous_filename}"
    else:
        change = "updated"

    diff_content = (
        "```\n"
        + re.sub(r"^```", "\\```", patch.strip().replace("\r\n", "\n"))
        + "\n```"
        if (patch := diff_data.get("patch"))
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


# Files that should be included in GitHubRepository contents.
GITHUB_DEFAULT_ALLOWED = [
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

# Files that should be omitted from GitHubRepository contents.
GITHUB_DEFAULT_SKIPPED_SUBSTR = [
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
    "tsconfig.json",
    "tslint.json",
    "workspace.code-workspace",
]

# Files that should be omitted from GitHubRepository contents when present at
# the root of the project.
GITHUB_DEFAULT_SKIPPED_ABSOLUTE = [
    "version.js",
]
