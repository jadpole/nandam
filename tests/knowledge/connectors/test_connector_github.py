import pytest

from base.config import TEST_INTEGRATION
from base.models.content import ContentText
from base.resources.aff_body import AffBody, ObsBody
from base.resources.aff_collection import AffCollection, ObsCollection
from base.resources.aff_plain import AffPlain, ObsPlain
from base.strings.file import FileName, FilePath
from base.strings.resource import Realm, ResourceUri, WebUrl

from knowledge.connectors.github import (
    GitHubCommitLocator,
    GitHubCompareLocator,
    GitHubRepository,
    GitHubFileLocator,
    GitHubRepositoryLocator,
)

from tests.knowledge.utils_connectors import given_context, run_test_connector_full


##
## Repository
##


def test_github_repository_from_web():
    """Test parsing GitHub repository from web URL."""
    url = WebUrl.decode("https://github.com/GoodStartLabs/AI_Diplomacy")

    repo = GitHubRepository.from_web(url)
    assert repo is not None
    print(f"<repository>{repo.model_dump_json()}</repository>")
    assert repo.owner == "GoodStartLabs"
    assert repo.repo == "AI_Diplomacy"


def test_github_repository_from_web_with_suffix():
    """Test parsing GitHub repository URLs with path suffix."""
    url = WebUrl.decode(
        "https://github.com/GoodStartLabs/AI_Diplomacy/blob/main/README.md"
    )

    repo = GitHubRepository.from_web(url)
    assert repo is not None
    print(f"<repository>{repo.model_dump_json()}</repository>")
    assert repo.owner == "GoodStartLabs"
    assert repo.repo == "AI_Diplomacy"


def test_github_repository_from_web_invalid():
    """Test parsing invalid GitHub repository URLs."""
    url = WebUrl.decode("https://github.com/GoodStartLabs")
    repo = GitHubRepository.from_web(url)
    assert repo is None


@pytest.mark.parametrize(
    ("prefix", "suffix"),
    [
        ("repository", ""),
        ("ref/main", "README.md"),
        ("file", "README.md"),
        ("commit", "abc123"),
    ],
)
def test_github_repository_from_uri(prefix: str, suffix: str):
    """Test parsing GitHub repository from resource URI."""
    suffix = f"/{suffix}" if suffix else ""
    uri = ResourceUri.decode(
        f"ndk://github/{prefix}/GoodStartLabs/AI_Diplomacy{suffix}"
    )

    repo = GitHubRepository.from_uri(uri)
    assert repo is not None
    print(f"<repository>{repo.model_dump_json()}</repository>")
    assert repo.owner == "GoodStartLabs"
    assert repo.repo == "AI_Diplomacy"


def test_github_repository_from_uri_invalid():
    """Test parsing invalid GitHub repository from resource URI."""
    uri = ResourceUri.decode("ndk://github/file/onlyproject")
    repo = GitHubRepository.from_uri(uri)
    assert repo is None


##
## Locator
##


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_github_full_repository():
    locator = GitHubRepositoryLocator(
        realm=Realm.decode("github"),
        repository=GitHubRepository(
            owner=FileName.decode("GoodStartLabs"),
            repo=FileName.decode("AI_Diplomacy"),
        ),
    )
    assert str(locator.citation_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy"
    )
    assert str(locator.content_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://github.com/GoodStartLabs/AI_Diplomacy",
        resource_uri="ndk://github/repository/GoodStartLabs/AI_Diplomacy",
        observe=[AffCollection.new()],
        # NOTE: Relations are not persisted when should_cache=False.
        expected_resolve_name="Repository GoodStartLabs/AI_Diplomacy",
        expected_resolve_affordances=["self://$collection"],
        expected_load_locator=locator,
        expected_load_name="Repository GoodStartLabs/AI_Diplomacy",
        expected_load_mime_type=None,
    )

    # Collection content: all children should be listed, recursively.
    resource_uri = ResourceUri.decode(
        "ndk://github/repository/GoodStartLabs/AI_Diplomacy"
    )
    collection_content = resources.get_observation(
        resource_uri.child_observable(AffCollection.new())
    )
    assert collection_content is not None
    assert isinstance(collection_content, ObsCollection)
    results = sorted(str(r) for r in collection_content.results)
    print("\n".join(["<results>", *results, "</results>"]))

    assert "ndk://github/file/GoodStartLabs/AI_Diplomacy/README.md" in results
    assert (
        "ndk://github/file/GoodStartLabs/AI_Diplomacy/ai_diplomacy/game_logic.py"
        in results
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_github_full_file_blob_readme():
    locator = GitHubFileLocator(
        realm=Realm.decode("github"),
        repository=GitHubRepository(
            owner=FileName.decode("GoodStartLabs"),
            repo=FileName.decode("AI_Diplomacy"),
        ),
        ref=FilePath.decode("main"),
        mode="blob",
        is_default_branch=True,
        path=[FileName.decode("README.md")],
    )
    assert str(locator.citation_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/blob/main/README.md"
    )
    assert str(locator.content_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/blob/main/README.md"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://github.com/GoodStartLabs/AI_Diplomacy/blob/main/README.md",
        resource_uri="ndk://github/file/GoodStartLabs/AI_Diplomacy/README.md",
        observe=[AffBody.new()],
        expected_resolve_name="File README.md in repository GoodStartLabs/AI_Diplomacy",
        expected_resolve_affordances=["self://$body", "self://$plain"],
        expected_load_locator=locator,
        expected_load_name="File README.md in repository GoodStartLabs/AI_Diplomacy",
        expected_load_mime_type="text/markdown",
    )

    resource_uri = ResourceUri.decode(
        "ndk://github/file/GoodStartLabs/AI_Diplomacy/README.md"
    )
    content = resources.get_observation(resource_uri.child_observable(AffBody.new()))
    assert content is not None
    assert isinstance(content, ObsBody)
    assert isinstance(content.content, ContentText)
    content_text = content.content.as_str()
    print(f"<body>\n{content_text[:500]}\n</body>")
    assert content.description


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_github_full_file_blob_python():
    locator = GitHubFileLocator(
        realm=Realm.decode("github"),
        repository=GitHubRepository(
            owner=FileName.decode("GoodStartLabs"),
            repo=FileName.decode("AI_Diplomacy"),
        ),
        ref=FilePath.decode("main"),
        mode="blob",
        is_default_branch=True,
        path=[
            FileName.decode("ai_diplomacy"),
            FileName.decode("game_logic.py"),
        ],
    )
    assert str(locator.citation_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/blob/main/ai_diplomacy/game_logic.py"
    )
    assert str(locator.content_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/blob/main/ai_diplomacy/game_logic.py"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://github.com/GoodStartLabs/AI_Diplomacy/blob/main/ai_diplomacy/game_logic.py",
        resource_uri="ndk://github/file/GoodStartLabs/AI_Diplomacy/ai_diplomacy/game_logic.py",
        observe=[AffBody.new(), AffPlain.new()],
        expected_resolve_name="File ai_diplomacy/game_logic.py in repository GoodStartLabs/AI_Diplomacy",
        expected_resolve_affordances=["self://$body", "self://$plain"],
        expected_load_locator=locator,
        expected_load_name="File ai_diplomacy/game_logic.py in repository GoodStartLabs/AI_Diplomacy",
        expected_load_mime_type="text/x-python",
    )

    resource_uri = ResourceUri.decode(
        "ndk://github/file/GoodStartLabs/AI_Diplomacy/ai_diplomacy/game_logic.py"
    )
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentText)
    print(f"<body>\n{content_body.content.as_str()[:500]}\n</body>")

    content_plain = resources.get_observation(
        resource_uri.child_observable(AffPlain.new())
    )
    assert content_plain is not None
    assert isinstance(content_plain, ObsPlain)
    print(f"<plain>\n{content_plain.text[:500]}\n</plain>")


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_github_full_file_tree_folder():
    locator = GitHubFileLocator(
        realm=Realm.decode("github"),
        repository=GitHubRepository(
            owner=FileName.decode("GoodStartLabs"),
            repo=FileName.decode("AI_Diplomacy"),
        ),
        ref=FilePath.decode("main"),
        mode="tree",
        is_default_branch=True,
        path=[FileName.decode("ai_diplomacy")],
    )
    assert str(locator.citation_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/tree/main/ai_diplomacy"
    )
    assert str(locator.content_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/tree/main/ai_diplomacy"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://github.com/GoodStartLabs/AI_Diplomacy/tree/main/ai_diplomacy",
        resource_uri="ndk://github/file/GoodStartLabs/AI_Diplomacy/ai_diplomacy",
        observe=[AffCollection.new()],
        expected_resolve_name="Folder ai_diplomacy in repository GoodStartLabs/AI_Diplomacy",
        expected_resolve_affordances=["self://$collection"],
        # NOTE: Relations are not persisted when should_cache=False.
        expected_load_locator=locator,
        expected_load_name="Folder ai_diplomacy in repository GoodStartLabs/AI_Diplomacy",
        expected_load_mime_type=None,
    )

    # Collection content: only immediate files within ai_diplomacy
    resource_uri = ResourceUri.decode(
        "ndk://github/file/GoodStartLabs/AI_Diplomacy/ai_diplomacy"
    )
    collection_content = resources.get_observation(
        resource_uri.child_observable(AffCollection.new())
    )
    assert collection_content is not None
    assert isinstance(collection_content, ObsCollection)
    results = sorted(str(r) for r in collection_content.results)
    print("\n".join(["<results>", *results, "</results>"]))
    assert (
        "ndk://github/file/GoodStartLabs/AI_Diplomacy/ai_diplomacy/game_logic.py"
        in results
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_github_full_file_tree_root():
    locator = GitHubFileLocator(
        realm=Realm.decode("github"),
        repository=GitHubRepository(
            owner=FileName.decode("GoodStartLabs"),
            repo=FileName.decode("AI_Diplomacy"),
        ),
        ref=FilePath.decode("main"),
        mode="tree",
        is_default_branch=True,
        path=[],
    )
    assert str(locator.citation_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/tree/main"
    )
    assert str(locator.content_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/tree/main"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://github.com/GoodStartLabs/AI_Diplomacy/tree/main",
        resource_uri="ndk://github/file/GoodStartLabs/AI_Diplomacy",
        observe=[AffCollection.new()],
        # NOTE: Relations are not persisted when should_cache=False.
        expected_resolve_name="Root folder in repository GoodStartLabs/AI_Diplomacy",
        expected_resolve_affordances=["self://$collection"],
        expected_load_locator=locator,
        expected_load_name="Root folder in repository GoodStartLabs/AI_Diplomacy",
        expected_load_mime_type=None,
    )

    # Collection content at root: only one-level children
    resource_uri = ResourceUri.decode("ndk://github/file/GoodStartLabs/AI_Diplomacy")
    collection_content = resources.get_observation(
        resource_uri.child_observable(AffCollection.new())
    )
    assert collection_content is not None
    assert isinstance(collection_content, ObsCollection)
    results = sorted(str(r) for r in collection_content.results)
    print("\n".join(["<results>", *results, "</results>"]))

    assert "ndk://github/file/GoodStartLabs/AI_Diplomacy/ai_diplomacy" in results
    assert "ndk://github/file/GoodStartLabs/AI_Diplomacy/README.md" in results
    assert (
        "ndk://github/file/GoodStartLabs/AI_Diplomacy/ai_diplomacy/game_logic.py"
        not in results
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_github_full_compare():
    """Test comparing two commits in GitHub."""
    # Using tags or commits that exist in the repository
    locator = GitHubCompareLocator(
        realm=Realm.decode("github"),
        repository=GitHubRepository(
            owner=FileName.decode("GoodStartLabs"),
            repo=FileName.decode("AI_Diplomacy"),
        ),
        before=FilePath.decode("fe18f5ee524f95290a5ffb93b7d5398328c52a8b"),
        after=FilePath.decode("66d5f91225aff228c33c567ef01728ba85dd01c4"),
    )
    assert str(locator.citation_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/compare/fe18f5ee524f95290a5ffb93b7d5398328c52a8b...66d5f91225aff228c33c567ef01728ba85dd01c4"
    )
    assert str(locator.content_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/compare/fe18f5ee524f95290a5ffb93b7d5398328c52a8b...66d5f91225aff228c33c567ef01728ba85dd01c4"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://github.com/GoodStartLabs/AI_Diplomacy/compare/fe18f5ee524f95290a5ffb93b7d5398328c52a8b...66d5f91225aff228c33c567ef01728ba85dd01c4",
        resource_uri="ndk://github/compare/GoodStartLabs/AI_Diplomacy/fe18f5ee524f95290a5ffb93b7d5398328c52a8b_66d5f91225aff228c33c567ef01728ba85dd01c4",
        observe=[AffBody.new()],
        expected_resolve_name="Compare fe18f5ee524f95290a5ffb93b7d5398328c52a8b...66d5f91225aff228c33c567ef01728ba85dd01c4 in repository GoodStartLabs/AI_Diplomacy",
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="Compare fe18f5ee524f95290a5ffb93b7d5398328c52a8b...66d5f91225aff228c33c567ef01728ba85dd01c4 in repository GoodStartLabs/AI_Diplomacy",
        expected_load_mime_type=None,  # mime_type is on affordance, not resource attributes
    )

    resource_uri = ResourceUri.decode(
        "ndk://github/compare/GoodStartLabs/AI_Diplomacy/fe18f5ee524f95290a5ffb93b7d5398328c52a8b_66d5f91225aff228c33c567ef01728ba85dd01c4"
    )
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentText)
    content_text = content_body.content.as_str()
    print(f"<body>\n{content_text[:1000]}\n</body>")
    assert content_body.description
    assert "<commits>" in content_text
    assert "<diffs>" in content_text


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_github_full_commit():
    """Test reading a specific commit from GitHub."""
    # Using the first commit in the repository
    locator = GitHubCommitLocator(
        realm=Realm.decode("github"),
        repository=GitHubRepository(
            owner=FileName.decode("GoodStartLabs"),
            repo=FileName.decode("AI_Diplomacy"),
        ),
        commit_id=FilePath.decode("66d5f91225aff228c33c567ef01728ba85dd01c4"),
    )
    assert str(locator.citation_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/commit/66d5f91225aff228c33c567ef01728ba85dd01c4"
    )
    assert str(locator.content_url()) == (
        "https://github.com/GoodStartLabs/AI_Diplomacy/commit/66d5f91225aff228c33c567ef01728ba85dd01c4"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://github.com/GoodStartLabs/AI_Diplomacy/commit/66d5f91225aff228c33c567ef01728ba85dd01c4",
        resource_uri="ndk://github/commit/GoodStartLabs/AI_Diplomacy/66d5f91225aff228c33c567ef01728ba85dd01c4",
        observe=[AffBody.new()],
        expected_resolve_name="Commit 66d5f91225aff228c33c567ef01728ba85dd01c4 in repository GoodStartLabs/AI_Diplomacy",
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="Commit 66d5f91225aff228c33c567ef01728ba85dd01c4 in repository GoodStartLabs/AI_Diplomacy",
        expected_load_mime_type=None,  # mime_type is on affordance, not resource attributes
    )

    resource_uri = ResourceUri.decode(
        "ndk://github/commit/GoodStartLabs/AI_Diplomacy/66d5f91225aff228c33c567ef01728ba85dd01c4"
    )
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentText)
    content_text = content_body.content.as_str()
    print(f"<body>\n{content_text[:1000]}\n</body>")
    assert "<commit" in content_text
