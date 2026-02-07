import pytest

from base.config import TEST_INTEGRATION
from base.models.content import ContentText
from base.resources.aff_body import AffBody, ObsBody
from base.resources.relation import RelationParent
from base.strings.file import FileName
from base.strings.resource import Realm, ResourceUri

from knowledge.connectors.confluence import ConfluenceBlogLocator, ConfluencePageLocator
from knowledge.domain.refresh import execute_refresh

from tests.knowledge.utils_connectors import (
    given_context,
    run_test_connector_full,
    run_test_connector_resolve,
)


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_confluence_full_blog_by_id():
    """
    TODO: Extract embedded images in connector, then test here.
    """
    locator = ConfluenceBlogLocator(
        realm=Realm.decode("confluence"),
        domain="confluence.corp.stingraydigital.com",
        space_key=FileName.decode("AI"),
        posting_day=FileName.decode("2024-01-29"),
        page_id=FileName.decode("141591887"),
    )
    assert str(locator.citation_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=141591887"
    )
    assert str(locator.content_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=141591887"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=141591887",
        resource_uri="ndk://confluence/blog/AI/2024-01-29/141591887",
        observe=[AffBody.new()],
        expected_resolve_name=None,
        expected_resolve_citation_url="https://confluence.corp.stingraydigital.com/display/AI/2024/01/29/Large+Document+Storage+and+Retrieval+with+Vector+Search",
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="Large Document Storage and Retrieval with Vector Search",
        expected_load_mime_type=None,
    )

    resource_uri = ResourceUri.decode("ndk://confluence/blog/AI/2024-01-29/141591887")
    content = resources.get_observation(resource_uri.child_observable(AffBody.new()))
    assert content is not None
    assert isinstance(content, ObsBody)
    assert isinstance(content.content, ContentText)
    content_text = content.content.as_str()
    print(f"<body>\n{content_text}\n</body>")
    assert content.description
    assert "## **Abstract:**" in content_text

    # Code blocks are correctly extracted:
    assert (
        "The pre-promt used is the following\n\n```\n"
        '        "- Between the triple backticks bellow you will find a number of "\n'
        in content_text
    )
    # INDENTED code blocks inside others are not escaped.
    assert '        " ```{extracts}``` "' in content_text


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_confluence_resolve_blog_by_name():
    locator = ConfluenceBlogLocator(
        realm=Realm.decode("confluence"),
        domain="confluence.corp.stingraydigital.com",
        space_key=FileName.decode("AI"),
        posting_day=FileName.decode("2024-01-29"),
        page_id=FileName.decode("141591887"),
    )
    assert str(locator.citation_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=141591887"
    )
    assert str(locator.content_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=141591887"
    )

    context = given_context(stub_storage={})
    await run_test_connector_resolve(
        context=context,
        web_url=(
            "https://confluence.corp.stingraydigital.com/display/AI/2024/01/29/"
            "Large+Document+Storage+and+Retrieval+with+Vector+Search"
        ),
        expected_resource_uri="ndk://confluence/blog/AI/2024-01-29/141591887",
        expected_name=None,
        expected_citation_url="https://confluence.corp.stingraydigital.com/display/AI/2024/01/29/Large+Document+Storage+and+Retrieval+with+Vector+Search",
        expected_affordances=["self://$body"],
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_confluence_full_page_bi_by_id():
    locator = ConfluencePageLocator(
        realm=Realm.decode("confluence"),
        domain="confluence.corp.stingraydigital.com",
        space_key=FileName.decode("BI"),
        page_id=FileName.decode("8978580"),
    )
    assert str(locator.citation_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=8978580"
    )
    assert str(locator.content_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=8978580"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=8978580",
        resource_uri="ndk://confluence/page/BI/8978580",
        observe=[AffBody.new()],
        expected_resolve_name=None,
        expected_resolve_citation_url="https://confluence.corp.stingraydigital.com/display/BI/Business+Intelligence+Home",
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="Business Intelligence Home",
        expected_load_mime_type=None,
    )

    resource_uri = ResourceUri.decode("ndk://confluence/page/BI/8978580")
    content = resources.get_observation(resource_uri.child_observable(AffBody.new()))
    assert content is not None
    assert isinstance(content, ObsBody)
    assert isinstance(content.content, ContentText)
    content_text = content.content.as_str()
    print(f"<body>\n{content_text}\n</body>")
    assert content.description
    assert "**Definition of Business Intelligence:**" in content_text
    assert (
        "**[Introduction - Business Intelligence Developer](ndk://confluence/page/BI/33264777)**"
        in content_text
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_confluence_resolve_page_bi_by_name():
    locator = ConfluencePageLocator(
        realm=Realm.decode("confluence"),
        domain="confluence.corp.stingraydigital.com",
        space_key=FileName.decode("BI"),
        page_id=FileName.decode("8978580"),
    )
    assert str(locator.citation_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=8978580"
    )
    assert str(locator.content_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=8978580"
    )

    context = given_context(stub_storage={})
    await run_test_connector_resolve(
        context=context,
        web_url="https://confluence.corp.stingraydigital.com/display/BI/Business+Intelligence+Home",
        expected_resource_uri="ndk://confluence/page/BI/8978580",
        expected_name=None,
        expected_citation_url="https://confluence.corp.stingraydigital.com/display/BI/Business+Intelligence+Home",
        expected_affordances=["self://$body"],
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_confluence_full_page_mkt_by_id():
    locator = ConfluencePageLocator(
        realm=Realm.decode("confluence"),
        domain="confluence.corp.stingraydigital.com",
        space_key=FileName.decode("MKT"),
        page_id=FileName.decode("141603043"),
    )
    assert str(locator.citation_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=141603043"
    )
    assert str(locator.content_url()) == (
        "https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=141603043"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://confluence.corp.stingraydigital.com/pages/viewpage.action?pageId=141603043",
        resource_uri="ndk://confluence/page/MKT/141603043",
        observe=[AffBody.new()],
        expected_resolve_name=None,
        expected_resolve_citation_url="https://confluence.corp.stingraydigital.com/display/MKT/Stingray+Music+Overview",
        expected_resolve_affordances=["self://$body"],
        expected_load_relations=[
            # Relation "parent" from "Stingray Music Overview" to "Product Information".
            RelationParent(
                parent=ResourceUri.decode("ndk://confluence/page/MKT/141603027"),
                child=ResourceUri.decode("ndk://confluence/page/MKT/141603043"),
            ),
        ],
        expected_load_locator=locator,
        expected_load_name="Stingray Music Overview",
        expected_load_mime_type=None,
    )

    resource_uri = ResourceUri.decode("ndk://confluence/page/MKT/141603043")
    content = resources.get_observation(resource_uri.child_observable(AffBody.new()))
    assert content is not None
    assert isinstance(content, ObsBody)
    assert isinstance(content.content, ContentText)
    content_text = content.content.as_str()
    print(f"<body>\n{content_text}\n</body>")
    assert content.description
    # Check for content that indicates this is the right page
    assert "Premium" in content_text or "Stingray Music" in content_text


##
## Refresh
##


@pytest.mark.asyncio
@pytest.mark.skip("Connector.refresh tests disabled")
async def test_connector_confluence_refresh():
    context = given_context(stub_storage=None)
    results = await execute_refresh(context, [Realm.decode("confluence")], {})
    print("\n".join(str(r) for r in results))
    assert False  # noqa: B011, PT015
