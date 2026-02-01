import pytest

from base.config import TEST_INTEGRATION
from base.models.content import ContentBlob, ContentText
from base.resources.aff_body import AffBody, ObsBody
from base.strings.file import FileName
from base.strings.resource import Realm, ResourceUri

from knowledge.connectors.qatestrail import (
    QATestRailAttachmentLocator,
    QATestRailCaseLocator,
    QATestRailProjectLocator,
)

from tests.knowledge.utils_connectors import given_context, run_test_connector_full


def _realm() -> Realm:
    return Realm.decode("testrail")


def _domain() -> str:
    return "testrail.corp.stingraydigital.com"


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_testrail_full_project_overview():
    locator = QATestRailProjectLocator(
        realm=_realm(),
        domain=_domain(),
        project_id=FileName.decode("96"),
        suite_id=FileName.decode("4252"),
    )
    assert str(locator.citation_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/suites/view/4252"
    )
    assert str(locator.content_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/projects/overview/96"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://testrail.corp.stingraydigital.com/index.php?/projects/overview/96",
        resource_uri="ndk://testrail/project/96",
        observe=[AffBody.new()],
        expected_resolve_name=None,
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="AI Tools",
        expected_load_mime_type="text/markdown",
    )

    resource_uri = ResourceUri.decode("ndk://testrail/project/96")
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentText)
    content_text = content_body.content.as_str()
    print(f"<body>\n{content_text}\n</body>")
    assert content_body.description

    assert "# Test Suite" in content_text
    assert "## Sonata" in content_text
    assert "### Sonata (EN)" in content_text
    assert "The default persona" in content_text

    assert "#### Commands" in content_text
    assert "- [Command: settings](ndk://testrail/case/96/1034700)" in content_text
    assert "- [Command: debug error](ndk://testrail/case/96/1034702)" in content_text

    assert "# Test Runs (Last 28 Days)" in content_text


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_testrail_full_project_suite():
    locator = QATestRailProjectLocator(
        realm=_realm(),
        domain=_domain(),
        project_id=FileName.decode("96"),
        suite_id=FileName.decode("4252"),
    )
    assert str(locator.citation_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/suites/view/4252"
    )
    assert str(locator.content_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/projects/overview/96"
    )

    context = given_context(stub_storage={})
    _, _ = await run_test_connector_full(
        context=context,
        web_url="https://testrail.corp.stingraydigital.com/index.php?/suites/view/4252",
        resource_uri="ndk://testrail/project/96",
        observe=[AffBody.new()],
        expected_resolve_name=None,
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="AI Tools",
        expected_load_mime_type="text/markdown",
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_testrail_full_case_debug_error():
    locator = QATestRailCaseLocator(
        realm=_realm(),
        domain=_domain(),
        project_id=FileName.decode("96"),
        case_id=FileName.decode("1034702"),
    )
    assert str(locator.citation_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/cases/view/1034702"
    )
    assert str(locator.content_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/cases/view/1034702"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://testrail.corp.stingraydigital.com/index.php?/cases/view/1034702",
        resource_uri="ndk://testrail/case/96/1034702",
        observe=[AffBody.new()],
        expected_resolve_name=None,
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="Command: debug error",
        expected_load_mime_type="text/markdown",
    )

    resource_uri = ResourceUri.decode("ndk://testrail/case/96/1034702")
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentText)
    content_text = content_body.content.as_str()
    print(f"<body>\n{content_text}\n</body>")
    assert content_body.description

    assert "## Preconditions" in content_text

    assert "## Steps" in content_text
    assert '```\n/debug error "my error message"\n```' in content_text

    assert "## Expected Result" in content_text
    assert "![](ndk://testrail/attachment/27849)" in content_text


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_testrail_full_case_settings():
    locator = QATestRailCaseLocator(
        realm=_realm(),
        domain=_domain(),
        project_id=FileName.decode("96"),
        case_id=FileName.decode("1034700"),
    )
    assert str(locator.citation_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/cases/view/1034700"
    )
    assert str(locator.content_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/cases/view/1034700"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://testrail.corp.stingraydigital.com/index.php?/cases/view/1034700",
        resource_uri="ndk://testrail/case/96/1034700",
        observe=[AffBody.new()],
        expected_resolve_name=None,
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="Command: settings",
        expected_load_mime_type="text/markdown",
    )

    resource_uri = ResourceUri.decode("ndk://testrail/case/96/1034700")
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentText)
    content_text = content_body.content.as_str()
    print(f"<body>\n{content_text}\n</body>")
    assert content_body.description

    assert "## Steps" in content_text
    assert "## Expected Result" not in content_text

    assert "### Step 1" in content_text
    assert "```\nsettings\n```" in content_text
    assert (
        "**Expected Result**: The **Settings - Index** card is displayed:"
        in content_text
    )
    assert "![](ndk://testrail/attachment/29846)" in content_text


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_testrail_full_attachment():
    locator = QATestRailAttachmentLocator(
        realm=_realm(),
        domain=_domain(),
        attachment_id=FileName.decode("27849"),
    )
    assert str(locator.citation_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/attachments/get/27849"
    )
    assert str(locator.content_url()) == (
        "https://testrail.corp.stingraydigital.com/index.php?/attachments/get/27849"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        web_url=(
            "https://testrail.corp.stingraydigital.com/index.php?/attachments/get/27849"
        ),
        resource_uri="ndk://testrail/attachment/27849",
        observe=[AffBody.new()],
        context=context,
        expected_resolve_name=None,
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="27849",  # TODO: Extract the attachment name.
        expected_load_mime_type="image/png",
    )

    resource_uri = ResourceUri.decode("ndk://testrail/attachment/27849")
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentBlob)
    print(f"<body_description>\n{content_body.description}\n</body_description>")
    assert content_body.description
    assert content_body.content.mime_type == "image/webp"  # Should be optimized.
