import pytest

from base.config import TEST_INTEGRATION
from base.resources.aff_body import AffBody, ObsBody, ObsMedia
from base.strings.file import FileName
from base.strings.resource import ObservableUri, ResourceUri, WebUrl

from knowledge.connectors.public import ArXivLocator
from knowledge.services.storage import SvcStorageStub

from tests.knowledge.utils_connectors import given_context, run_test_connector_full


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="Integration tests disabled")
async def test_connector_public_arxiv():
    """
    NOTE: Checks that labels are correctly generated (via LLM) and injected.
    """
    web_url = WebUrl.decode("https://arxiv.org/abs/2303.11366v2")
    resource_uri = ResourceUri.decode("ndk://public/arxiv/2303.11366v2")
    dependency_uri = ResourceUri.decode(
        "ndk://github/repository/noahshinn024/reflexion"
    )

    context = given_context(stub_inference=False, stub_storage={})
    load_resources, resources = await run_test_connector_full(
        context=context,
        web_url=web_url,
        resource_uri=resource_uri,
        observe=[AffBody.new()],
        expected_resolve_name="2303.11366v2",
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=ArXivLocator(paper_id=FileName.decode("2303.11366v2")),
        # NOTE: Relation extraction requires links to be resolved to KnowledgeUris.
        expected_load_name="arXiv-2303.11366v2.tex",
        expected_load_mime_type="text/x-tex",
    )

    resource = resources.get_resource(resource_uri)
    assert resource is not None
    assert resource.attributes.description
    assert "Reflexion" in resource.attributes.description

    content = resources.get_observation(resource_uri.child_observable(AffBody.new()))
    assert content is not None
    assert isinstance(content, ObsBody)
    resource = resources.get_resource(resource_uri)
    assert resource is not None
    assert str(resource.attributes.citation_url) == "https://arxiv.org/abs/2303.11366v2"

    # Confirm that descriptions were generated for media.
    media = resources.get_observation(
        ObservableUri.decode(
            "ndk://public/arxiv/2303.11366v2/$media/figures/alfworld_failure.pdf"
        )
    )
    assert isinstance(media, ObsMedia)
    assert media.description
    assert "hallucination" in media.description.lower()

    # In "load" responses, links from the web pages are resolved by the service,
    # but they are not returned in the resources.
    assert load_resources.get_resource(dependency_uri) is None

    # In "read" responses, links from the web page are returned as dependencies
    # and a placeholder is created.
    read_dependency = resources.get_resource(dependency_uri)
    assert read_dependency is not None
    assert str(read_dependency.attributes.name) == "Repository noahshinn024/reflexion"
    assert str(read_dependency.attributes.citation_url) == (
        "https://github.com/noahshinn024/reflexion"
    )

    # The document content is ALWAYS cached for ArXiv papers.
    actual_storage_items = sorted(context.service(SvcStorageStub).items.keys())
    expected_storage_items = [
        "v1/observed/public+arxiv+2303.11366v2/body.yml",
        "v1/resource/github/repository/noahshinn024/reflexion.yml",
        "v1/resource/public/arxiv/2303.11366v2.yml",
    ]
    assert actual_storage_items == expected_storage_items
