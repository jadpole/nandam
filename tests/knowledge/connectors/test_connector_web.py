import pytest

from base.config import TEST_INTEGRATION
from base.models.content import ContentText
from base.resources.aff_body import AffBody, ObsBody
from base.strings.resource import ResourceUri, WebUrl

from knowledge.connectors.web import WebLocator

from tests.knowledge.utils_connectors import given_context, run_test_connector_full


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="Integration tests disabled")
async def test_connector_web_full_html():
    web_url = WebUrl.decode("https://www.iana.org/help/example-domains")
    resource_uri = ResourceUri.decode(
        "ndk://www/iana.org/awugvcd9scar2kdlrdnk4ama1awj2wpkpmoabo1z"
    )

    storage_items = {}
    context = given_context(stub_inference=True, stub_storage=storage_items)
    _, resources = await run_test_connector_full(
        context=context,
        web_url=web_url,
        resource_uri=resource_uri,
        observe=[AffBody.new()],
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=WebLocator(url=web_url),
        expected_load_name="Example Domains",
        expected_load_mime_type="text/html",
    )

    resource = resources.get_resource(resource_uri)
    assert resource is not None
    assert resource.attributes.description == (
        "stub completion: <Source> # Example Domains As..."
    )

    # Web pages are ingested as a single body.
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentText)
    content_text = content_body.content.as_str()
    print(f"<body>\n{content_text}\n</body>")

    assert content_body.description == (
        "stub completion: <Source> # Example Domains As..."
    )
    assert (
        "# Example Domains\n\n"
        "As described in [RFC 2606](ndk://www/iana.org/mksrxphaqndehm5crkdaw5byesbnlbxuw4bwdivy) "
        "and [RFC 6761](ndk://www/iana.org/jipr8e8bcuoxv5lza8phj1w3kafwz5zqbhkhjb9y), "
        in content_text
    )

    # The metadata is cached to avoid re-generating the description.
    assert (
        "v1/resource/www/iana.org/awugvcd9scar2kdlrdnk4ama1awj2wpkpmoabo1z.yml"
        in storage_items
    )

    # The body is NOT cached for HTML pages.
    assert (
        "v1/observed/www/iana.org/awugvcd9scar2kdlrdnk4ama1awj2wpkpmoabo1z/body.yml"
        not in storage_items
    )

    # The aliases for the links in the page are cached.
    assert "v1/alias/7ii6qcimfrxja83xazt9hilbzwm7rwjclybasfgm.yml" in storage_items
    assert "v1/alias/js98nsvzlrr8trojkyrz8vozhujmhfwigbpe4ax7.yml" in storage_items
    assert "v1/alias/zadbq3na6woqqagmq2mmyrbahh2hvm5wwyoc4nan.yml" in storage_items
    assert "v1/alias/zhfej48emhesl9se9izejgwa28isstunwzfydbbi.yml" in storage_items


@pytest.mark.asyncio
@pytest.mark.skipif(not TEST_INTEGRATION, reason="Integration tests disabled")
async def test_connector_web_full_pdf():
    web_url = WebUrl.decode(
        "https://www.ams.org/journals/bull/1966-72-06/S0002-9904-1966-11654-3/S0002-9904-1966-11654-3.pdf"
    )
    resource_uri = ResourceUri.decode(
        "ndk://www/ams.org/kjuvhtrgnwgq00ueynxnn0xycyuoeaqea5c4jefa"
    )

    storage_items = {}
    context = given_context(stub_inference=True, stub_storage=storage_items)
    _, resources = await run_test_connector_full(
        context=context,
        web_url=web_url,
        resource_uri=resource_uri,
        observe=[AffBody.new()],
        expected_resolve_affordances=["self://$body"],
        expected_load_locator=WebLocator(url=web_url),
        expected_load_name="S0002-9904-1966-11654-3.pdf",
        expected_load_mime_type="application/pdf",
    )

    # Small PDFs are ingested as a single body.
    content_body = resources.get_observation(
        resource_uri.child_observable(AffBody.new())
    )
    assert content_body is not None
    assert isinstance(content_body, ObsBody)
    assert isinstance(content_body.content, ContentText)
    content_text = content_body.content.as_str()
    print(f"<body>\n{content_text}\n</body>")

    assert content_body.description
    assert content_text == (
        """\
2. F. P. Ramsey, *On a problem of formal logic*, Proc. London Math. Soc. (2) 30 (1930), 264-286.

DARTMOUTH COLLEGE

# COUNTEREXAMPLE TO EULER'S CONJECTURE ON SUMS OF LIKE POWERS

BY L. J. LANDER AND T. R. PARKIN

Communicated by J. D. Swift, June 27, 1966

A direct search on the CDC 6600 yielded

$$27^5 + 84^5 + 110^5 + 133^5 = 144^5$$

as the smallest instance in which four fifth powers sum to a fifth power. This is a counterexample to a conjecture by Euler [1] that at least  $n$   $n$ th powers are required to sum to an  $n$ th power,  $n > 2$ .

## REFERENCE

1. L. E. Dickson, *History of the theory of numbers*, Vol. 2, Chelsea, New York, 1952, p. 648.\
"""
    )

    # The metadata is cached to avoid re-generating the description.
    assert (
        "v1/resource/www/ams.org/kjuvhtrgnwgq00ueynxnn0xycyuoeaqea5c4jefa.yml"
        in storage_items
    )

    # The body is also cached for PDFs.
    assert (
        "v1/observed/www+ams.org+kjuvhtrgnwgq00ueynxnn0xycyuoeaqea5c4jefa/body.yml"
        in storage_items
    )
