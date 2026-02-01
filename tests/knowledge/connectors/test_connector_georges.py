import pytest

from base.config import TEST_INTEGRATION
from base.models.content import ContentBlob
from base.resources.aff_body import AffBody, ObsBody
from base.resources.aff_file import AffFile
from base.strings.file import FileName, FilePath
from base.strings.resource import ObservableUri, Realm

from knowledge.connectors.georges import (
    DalleImageLocator,
    FalImageLocator,
    OpenAIFileLocator,
)

from tests.knowledge.utils_connectors import (
    given_context,
    run_test_connector_full,
    run_test_connector_resolve,
)


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="Integration tests disabled")
async def test_connector_georges_resolve_dalle_image():
    """Test full flow for DALL-E image."""
    locator = DalleImageLocator(
        realm=Realm.decode("georges"),
        file_path=FilePath.decode(
            "private/org-ZqPqDHsb3141ej8sLSPCq2TG/user-fjRbyaktsSj41p1go1p30I7n/img-n8AFv3uCOXSqxudTpoybufGO.png"
        ),
    )
    assert str(locator.citation_url()) == (
        "https://oaidalleapiprodscus.blob.core.windows.net/"
        "private/org-ZqPqDHsb3141ej8sLSPCq2TG/user-fjRbyaktsSj41p1go1p30I7n/img-n8AFv3uCOXSqxudTpoybufGO.png"
    )
    assert str(locator.content_url()) == (
        "https://oaidalleapiprodscus.blob.core.windows.net/"
        "private/org-ZqPqDHsb3141ej8sLSPCq2TG/user-fjRbyaktsSj41p1go1p30I7n/img-n8AFv3uCOXSqxudTpoybufGO.png"
    )

    context = given_context(stub_storage={})
    await run_test_connector_resolve(
        context=context,
        web_url=(
            "https://oaidalleapiprodscus.blob.core.windows.net/"
            "private/org-ZqPqDHsb3141ej8sLSPCq2TG/user-fjRbyaktsSj41p1go1p30I7n/img-n8AFv3uCOXSqxudTpoybufGO.png"
        ),
        expected_resource_uri="ndk://georges/dalle/img-n8AFv3uCOXSqxudTpoybufGO.png",
        expected_name=None,
        expected_locator=locator,
        expected_description=None,
        expected_citation_url=(
            "https://oaidalleapiprodscus.blob.core.windows.net/"
            "private/org-ZqPqDHsb3141ej8sLSPCq2TG/user-fjRbyaktsSj41p1go1p30I7n/img-n8AFv3uCOXSqxudTpoybufGO.png"
        ),
        expected_affordances=["self://$body", "self://$file"],
    )


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="Integration tests disabled")
async def test_connector_georges_full_fal_image():
    """Test full flow for Fal image."""
    locator = FalImageLocator(
        realm=Realm.decode("georges"),
        category=FileName.decode("elephant"),
        filename=FileName.decode("OIUhnU24095TiMysqXcOd.png"),
    )
    assert str(locator.citation_url()) == (
        "https://fal.media/files/elephant/OIUhnU24095TiMysqXcOd.png"
    )
    assert str(locator.content_url()) == (
        "https://fal.media/files/elephant/OIUhnU24095TiMysqXcOd.png"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://fal.media/files/elephant/OIUhnU24095TiMysqXcOd.png",
        resource_uri="ndk://georges/fal/elephant/OIUhnU24095TiMysqXcOd.png",
        observe=[AffBody.new(), AffFile.new()],
        expected_resolve_name=None,
        expected_resolve_citation_url="https://fal.media/files/elephant/OIUhnU24095TiMysqXcOd.png",
        expected_resolve_affordances=["self://$body", "self://$file"],
        expected_load_locator=locator,
        expected_load_name="OIUhnU24095TiMysqXcOd.png",
        expected_load_mime_type="image/png",
    )

    content = resources.get_observation(
        ObservableUri.decode(
            "ndk://georges/fal/elephant/OIUhnU24095TiMysqXcOd.png/$body"
        )
    )
    assert content is not None
    assert isinstance(content, ObsBody)
    assert isinstance(content.content, ContentBlob)


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="Integration tests disabled")
@pytest.mark.skip("No valid OpenAI File example")
async def test_connector_georges_full_openai_file():
    """Test full flow for OpenAI file."""
    locator = OpenAIFileLocator(
        realm=Realm.decode("georges"),
        domain="georges.ai.stingray-private.com",
        file_id=FileName.decode("file-abc123xyz"),
    )
    assert locator.citation_url() is None
    assert str(locator.content_url()) == (
        "https://georges.ai.stingray-private.com/v1/files/file-abc123xyz/content"
    )

    context = given_context(stub_storage={})
    _, _ = await run_test_connector_full(
        context=context,
        web_url="https://georges.ai.stingray-private.com/v1/files/file-abc123xyz/content",
        resource_uri="ndk://georges/files/file-abc123xyz",
        observe=[AffBody.new(), AffFile.new()],
        expected_resolve_name=None,
        expected_resolve_citation_url="https://georges.ai.stingray-private.com/v1/files/file-abc123xyz/content",
        expected_resolve_affordances=["self://$body", "self://$file"],
        expected_load_locator=locator,
        expected_load_name="file-abc123xyz",
        expected_load_mime_type="text/plain",
    )
