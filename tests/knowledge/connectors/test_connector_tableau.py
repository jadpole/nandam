import pytest

from base.config import TEST_INTEGRATION
from base.models.content import ContentBlob
from base.resources.aff_body import AffBody, ObsBody
from base.strings.file import FileName
from base.strings.resource import ObservableUri, Realm

from knowledge.connectors.tableau import TableauViewLocator

from tests.knowledge.utils_connectors import given_context, run_test_connector_full


def _realm() -> Realm:
    return Realm.decode("tableau")


def _domain() -> str:
    return "tableau.corp.stingraydigital.com"


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="integration tests disabled")
async def test_connector_tableau_full_view_fragment():
    # NOTE: Choose a stable, public-ish dashboard which we can fetch with Basic auth.
    locator = TableauViewLocator(
        realm=_realm(),
        domain=_domain(),
        workbook=FileName.decode("SonataBenefitReport"),
        sheet=FileName.decode("Mainpage"),
    )
    assert str(locator.citation_url()) == (
        "https://tableau.corp.stingraydigital.com/views/SonataBenefitReport/Mainpage"
    )
    assert str(locator.content_url()) == (
        "https://tableau.corp.stingraydigital.com/views/SonataBenefitReport/Mainpage"
    )

    context = given_context(stub_storage={})
    _, resources = await run_test_connector_full(
        context=context,
        web_url="https://tableau.corp.stingraydigital.com/#/views/SonataBenefitReport/Mainpage",
        resource_uri="ndk://tableau/view/SonataBenefitReport/Mainpage",
        observe=[AffBody.new()],
        expected_resolve_name="SonataBenefitReport / Mainpage",
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=locator,
        expected_load_name="SonataBenefitReport / Mainpage",
        expected_load_mime_type=None,
    )

    content = resources.get_observation(
        ObservableUri.decode("ndk://tableau/view/SonataBenefitReport/Mainpage/$body")
    )
    assert content is not None
    assert isinstance(content, ObsBody)
    assert isinstance(content.content, ContentBlob)
