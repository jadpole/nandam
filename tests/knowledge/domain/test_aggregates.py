import pytest

from base.models.content import ContentText
from base.models.rendered import Rendered
from base.resources.aff_body import (
    AffBody,
    ObsBody,
)
from base.resources.label import (
    AggregateDefinition,
    AggregateValue,
    LabelName,
    LabelValue,
)
from base.strings.resource import ResourceUri

from knowledge.domain.aggregates import (
    _build_aggregate_prompt,
    _generate_aggregates,
    _parse_aggregate_response,
)
from knowledge.models.storage_observed import BundleBody

from tests.knowledge.utils_connectors import given_context


##
## Unit Tests - Aggregate Response Parsing
##


def test_parse_aggregate_response_valid_json():
    """Test parsing a valid JSON aggregate response."""
    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary.",
        ),
        AggregateDefinition(
            name=LabelName.decode("category"),
            prompt="Determine the category.",
        ),
    ]

    response_json = '{"summary": "A test summary", "category": "technical"}'
    result = _parse_aggregate_response(response_json, definitions)

    assert len(result) == 2

    summary = next((r for r in result if str(r.name) == "summary"), None)
    assert summary is not None
    assert summary.value == "A test summary"

    category = next((r for r in result if str(r.name) == "category"), None)
    assert category is not None
    assert category.value == "technical"


def test_parse_aggregate_response_null_values():
    """Test that null values are skipped."""
    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary.",
        ),
    ]

    response_json = '{"summary": null}'
    result = _parse_aggregate_response(response_json, definitions)

    assert len(result) == 0


def test_parse_aggregate_response_invalid_json():
    """Test that invalid JSON returns empty list."""
    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary.",
        ),
    ]

    result = _parse_aggregate_response("not valid json", definitions)
    assert len(result) == 0


def test_parse_aggregate_response_extra_properties():
    """Test that extra properties not in definitions are ignored."""
    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary.",
        ),
    ]

    response_json = '{"summary": "A summary", "unknown_property": "ignored"}'
    result = _parse_aggregate_response(response_json, definitions)

    assert len(result) == 1
    assert result[0].name == LabelName.decode("summary")


def test_parse_aggregate_response_non_dict():
    """Test that non-dict responses return empty list."""
    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary.",
        ),
    ]

    result = _parse_aggregate_response('["not", "a", "dict"]', definitions)
    assert len(result) == 0


##
## Unit Tests - Aggregate Prompt Building
##


def test_build_aggregate_prompt_with_previous():
    """Test building prompt with previous aggregate values."""
    previous = [
        AggregateValue(name=LabelName.decode("summary"), value="Previous summary"),
    ]

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Test content."),
        sections=[],
        chunks=[],
    )

    rendered = Rendered.render_embeds([body_uri], [obs_body])
    labels: list[LabelValue] = []

    prompt = _build_aggregate_prompt(previous, rendered, labels)

    # Prompt should be a list with at least one string element.
    assert isinstance(prompt, list)
    assert len(prompt) > 0

    # Check that previous values are included.
    text_parts = [p for p in prompt if isinstance(p, str)]
    combined_text = " ".join(text_parts)
    assert "Previous summary" in combined_text


def test_build_aggregate_prompt_with_labels():
    """Test building prompt with resource labels."""
    previous: list[AggregateValue] = []

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Test content."),
        sections=[],
        chunks=[],
    )

    rendered = Rendered.render_embeds([body_uri], [obs_body])
    labels = [
        LabelValue(
            name=LabelName.decode("description"),
            target=body_uri,
            value="A test document description",
        ),
    ]

    prompt = _build_aggregate_prompt(previous, rendered, labels)

    # Check that labels are included.
    text_parts = [p for p in prompt if isinstance(p, str)]
    combined_text = " ".join(text_parts)
    assert "Labels for" in combined_text
    assert "description" in combined_text
    assert "A test document description" in combined_text


def test_build_aggregate_prompt_empty_previous():
    """Test building prompt with no previous values."""
    previous: list[AggregateValue] = []

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Test content."),
        sections=[],
        chunks=[],
    )

    rendered = Rendered.render_embeds([body_uri], [obs_body])
    labels: list[LabelValue] = []

    prompt = _build_aggregate_prompt(previous, rendered, labels)

    # Should still produce a valid prompt.
    assert isinstance(prompt, list)
    assert len(prompt) > 0

    # Should contain observation tags.
    text_parts = [p for p in prompt if isinstance(p, str)]
    combined_text = " ".join(text_parts)
    assert "<observations>" in combined_text
    assert "</observations>" in combined_text


##
## Integration Tests - Aggregate Generation
##


@pytest.mark.asyncio
async def test_generate_aggregates_empty_inputs():
    """Test _generate_aggregates with empty inputs."""
    context = given_context(stub_inference={}, stub_storage={})

    # Empty bundles.
    result = await _generate_aggregates(
        context=context,
        bundles=[],
        labels=[],
        definitions=[
            AggregateDefinition(
                name=LabelName.decode("summary"),
                prompt="Generate a summary.",
            ),
        ],
    )
    # Should return null values for each definition.
    assert len(result) == 1
    assert result[0].name == LabelName.decode("summary")
    assert result[0].value is None

    # Empty definitions.
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test content."),
    )
    result = await _generate_aggregates(
        context=context,
        bundles=[bundle],
        labels=[],
        definitions=[],
    )
    assert result == []


@pytest.mark.asyncio
async def test_generate_aggregates_single_bundle():
    """Test _generate_aggregates with a single bundle."""
    context = given_context(
        stub_inference={
            "summary": ["Generated summary of the document"],
        },
        stub_storage={},
    )

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test document content for aggregation."),
    )

    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary of all documents.",
        ),
    ]

    result = await _generate_aggregates(
        context=context,
        bundles=[bundle],
        labels=[],
        definitions=definitions,
    )

    assert len(result) == 1
    assert result[0].name == LabelName.decode("summary")
    assert result[0].value == "Generated summary of the document"


@pytest.mark.asyncio
async def test_generate_aggregates_multiple_definitions():
    """Test _generate_aggregates with multiple aggregate definitions."""
    context = given_context(
        stub_inference={
            "summary": ["Overall summary"],
            "category": ["technical"],
            "sentiment": ["positive"],
        },
        stub_storage={},
    )

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test document content."),
    )

    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary.",
        ),
        AggregateDefinition(
            name=LabelName.decode("category"),
            prompt="Determine the category.",
        ),
        AggregateDefinition(
            name=LabelName.decode("sentiment"),
            prompt="Analyze the sentiment.",
        ),
    ]

    result = await _generate_aggregates(
        context=context,
        bundles=[bundle],
        labels=[],
        definitions=definitions,
    )

    # Results include initial null placeholders and generated values.
    # Filter to get the non-null values.
    result_with_values = [r for r in result if r.value is not None]
    assert len(result_with_values) == 3

    summary = next((r for r in result if str(r.name) == "summary" and r.value), None)
    assert summary is not None
    assert summary.value == "Overall summary"

    category = next((r for r in result if str(r.name) == "category" and r.value), None)
    assert category is not None
    assert category.value == "technical"

    sentiment = next(
        (r for r in result if str(r.name) == "sentiment" and r.value), None
    )
    assert sentiment is not None
    assert sentiment.value == "positive"


@pytest.mark.asyncio
async def test_generate_aggregates_with_labels():
    """Test _generate_aggregates uses labels in the prompt."""
    context = given_context(
        stub_inference={
            "summary": ["Summary based on labels"],
        },
        stub_storage={},
    )

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test content."),
    )

    labels = [
        LabelValue(
            name=LabelName.decode("description"),
            target=body_uri,
            value="A detailed description of the document",
        ),
    ]

    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary using the document descriptions.",
        ),
    ]

    result = await _generate_aggregates(
        context=context,
        bundles=[bundle],
        labels=labels,
        definitions=definitions,
    )

    assert len(result) == 1
    assert result[0].value == "Summary based on labels"


@pytest.mark.asyncio
async def test_generate_aggregates_multiple_bundles():
    """Test _generate_aggregates with multiple bundles."""
    context = given_context(
        stub_inference={
            "summary": ["Combined summary of all documents"],
        },
        stub_storage={},
    )

    resource_uri_1 = ResourceUri.decode("ndk://test/realm/doc1")
    resource_uri_2 = ResourceUri.decode("ndk://test/realm/doc2")

    bundle_1 = BundleBody.make_single(
        resource_uri=resource_uri_1,
        text=ContentText.new_plain("Document 1 content."),
    )
    bundle_2 = BundleBody.make_single(
        resource_uri=resource_uri_2,
        text=ContentText.new_plain("Document 2 content."),
    )

    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary across all documents.",
        ),
    ]

    result = await _generate_aggregates(
        context=context,
        bundles=[bundle_1, bundle_2],
        labels=[],
        definitions=definitions,
    )

    assert len(result) == 1
    assert result[0].name == LabelName.decode("summary")
    assert result[0].value == "Combined summary of all documents"


@pytest.mark.asyncio
async def test_generate_aggregates_returns_aggregate_values():
    """Test that _generate_aggregates returns AggregateValue instances."""
    context = given_context(
        stub_inference={
            "summary": ["Test summary"],
        },
        stub_storage={},
    )

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test content."),
    )

    definitions = [
        AggregateDefinition(
            name=LabelName.decode("summary"),
            prompt="Generate a summary.",
        ),
    ]

    result = await _generate_aggregates(
        context=context,
        bundles=[bundle],
        labels=[],
        definitions=definitions,
    )

    assert len(result) == 1
    assert isinstance(result[0], AggregateValue)
    assert isinstance(result[0].name, LabelName)
    assert isinstance(result[0].value, str)
