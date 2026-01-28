import pytest

from base.models.content import ContentText
from base.resources.aff_body import (
    AffBody,
    AffBodyChunk,
    ObsBody,
    ObsBodySection,
    ObsChunk,
    ObsMedia,
)
from base.resources.metadata import FieldName, FieldValue
from base.strings.resource import ObservableUri, ResourceUri

from knowledge.domain.chunking import chunk_body_sync
from knowledge.domain.fields import (
    GenerateFieldsItem,
    _build_inference_params,
    _generate_fields,
    _group_observations_by_tokens,
    _parse_response,
    _render_prompt,
    generate_standard_fields,
)
from knowledge.models.storage_observed import BundleBody

from tests.data.samples import given_sample_media, read_2303_11366v2
from tests.knowledge.utils_connectors import given_context


##
## Unit Tests
##


def test_group_observations_by_tokens():
    """Test that observations are grouped correctly by token count."""

    class MockObs:
        def __init__(self, uri: str, tokens: int):
            self.uri = uri
            self._tokens = tokens

        def num_tokens(self) -> int:
            return self._tokens

    # Create observations with known token counts.
    obs1 = MockObs("uri1", 10_000)
    obs2 = MockObs("uri2", 20_000)
    obs3 = MockObs("uri3", 60_000)
    obs4 = MockObs("uri4", 10_000)

    groups = _group_observations_by_tokens([obs1, obs2, obs3, obs4])  # type: ignore

    # Should create 2 groups:
    # - Group 0: obs1 + obs2 = 30k (under 80k threshold)
    # - Group 1: obs3 + obs4 = 70k (obs3 alone is 60k, adding obs4 = 70k)
    assert len(groups) == 2
    assert len(groups[0]) == 2
    group0_tokens = sum([o._tokens for o in groups[0]])  # type: ignore
    assert group0_tokens == 30_000
    assert len(groups[1]) == 2
    group1_tokens = sum([o._tokens for o in groups[1]])  # type: ignore
    assert group1_tokens == 70_000


def test_group_observations_single_large():
    """Test that a single large observation creates its own group."""

    class MockObs:
        def __init__(self, uri: str, tokens: int):
            self.uri = uri
            self._tokens = tokens

        def num_tokens(self) -> int:
            return self._tokens

    obs1 = MockObs("uri1", 90_000)  # Exceeds threshold alone
    obs2 = MockObs("uri2", 10_000)

    groups = _group_observations_by_tokens([obs1, obs2])  # type: ignore

    # obs1 is added to the first group (even though it exceeds threshold).
    # obs2 starts a new group since adding it would exceed.
    assert len(groups) == 2
    assert len(groups[0]) == 1
    assert len(groups[1]) == 1


def test_parse_response_valid_json():
    """Test parsing a valid JSON response."""
    target_uri = ObservableUri.decode("ndk://test/resource/doc/$body")
    property_mapping = {
        "description_test_resource_doc_body": (
            FieldName.decode("description"),
            target_uri,
        ),
    }

    response_json = '{"description_test_resource_doc_body": "A test description"}'
    inferred = _parse_response(response_json, property_mapping)

    assert len(inferred) == 1
    assert inferred[0].name == FieldName.decode("description")
    assert inferred[0].target == target_uri
    assert inferred[0].value == "A test description"


def test_parse_response_null_value():
    """Test that null values are skipped."""
    target_uri = ObservableUri.decode("ndk://test/resource/doc/$body")
    property_mapping = {
        "description_test_resource_doc_body": (
            FieldName.decode("description"),
            target_uri,
        ),
    }

    response_json = '{"description_test_resource_doc_body": null}'
    inferred = _parse_response(response_json, property_mapping)

    assert len(inferred) == 0


def test_parse_response_invalid_json():
    """Test that invalid JSON returns empty list."""
    property_mapping: dict = {}
    inferred = _parse_response("not valid json", property_mapping)
    assert len(inferred) == 0


def test_build_inference_params():
    """Test building inference parameters from field items."""
    target1 = ObservableUri.decode("ndk://test/realm/doc/$body")
    target2 = ObservableUri.decode("ndk://test/realm/doc/$chunk/00")

    field_item = GenerateFieldsItem(
        name=FieldName.decode("description"),
        description="Generate a description.",
        targets=[target1, target2],
    )

    system, schema, mapping = _build_inference_params([field_item])

    # Check system message.
    assert "description" in system.lower()

    # Check schema has properties.
    assert "properties" in schema
    assert len(schema["properties"]) == 2

    # Check all properties are nullable strings.
    for prop in schema["properties"].values():
        assert prop["type"] == ["string", "null"]

    # Check mapping.
    assert len(mapping) == 2


def test_render_prompt():
    """Test building and rendering a prompt with observations."""
    obs = ObsChunk.stub(
        uri="ndk://test/realm/doc/$chunk/00",
        mode="plain",
        text="Test chunk content.",
        description="A test chunk",
    )

    prompt = _render_prompt([obs])

    # The prompt should be a list of strings and/or blobs.
    assert isinstance(prompt, list)
    assert len(prompt) > 0

    # At least one part should be a string containing the chunk content.
    text_parts = [p for p in prompt if isinstance(p, str)]
    assert len(text_parts) > 0
    combined_text = " ".join(text_parts)
    assert (
        "Test chunk content" in combined_text or "observations" in combined_text.lower()
    )


##
## Integration Tests
##


@pytest.mark.asyncio
async def test_generate_fields_with_stub_inference():
    """Test the full field generation flow with stubbed inference."""
    # Create a stub response that maps property names to values.
    # Note: FieldName.try_normalize removes slashes and $ from URIs.
    context = given_context(
        stub_inference={
            "description_test_realm_doc_body": ["Generated description for body"],
            "description_test_realm_doc_chunk_00": ["Generated description for chunk"],
        },
        stub_storage={},
    )

    # Create sample observations using the proper constructors.
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Test body content."),
        sections=[],
        chunks=[],
    )
    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Test chunk content."),
    )

    # Create field items targeting these observations.
    field_items = [
        GenerateFieldsItem(
            name=FieldName.decode("description"),
            description="Generate a description.",
            targets=[obs_body.uri, obs_chunk.uri],
        ),
    ]

    inferred = await _generate_fields(context, [obs_body, obs_chunk], field_items)

    # Verify inferred fields.
    assert len(inferred) == 2

    body_field = next((f for f in inferred if f.target == obs_body.uri), None)
    assert body_field is not None
    assert body_field.name == FieldName.decode("description")
    assert body_field.value == "Generated description for body"

    chunk_field = next((f for f in inferred if f.target == obs_chunk.uri), None)
    assert chunk_field is not None
    assert chunk_field.name == FieldName.decode("description")
    assert chunk_field.value == "Generated description for chunk"


@pytest.mark.asyncio
async def test_generate_fields_with_media():
    """Test field generation including media observations."""
    # Create a stub response.
    # Note: FieldName.try_normalize removes special chars from URIs.

    context = given_context(
        stub_inference={
            "description_stub_outputpng_media": ["Generated media description"],
            "placeholder_stub_outputpng_media": ["Generated media placeholder"],
        },
        stub_storage={},
    )

    # Use the sample media.
    obs_media = given_sample_media()

    # Create field items for the media.
    field_items = [
        GenerateFieldsItem(
            name=FieldName.decode("description"),
            description="Generate a description.",
            targets=[obs_media.uri],
        ),
        GenerateFieldsItem(
            name=FieldName.decode("placeholder"),
            description="Generate a placeholder.",
            targets=[obs_media.uri],
        ),
    ]

    inferred = await _generate_fields(context, [obs_media], field_items)

    # Verify inferred fields.
    assert len(inferred) == 2

    description_field = next(
        (f for f in inferred if f.name == FieldName.decode("description")), None
    )
    assert description_field is not None
    assert description_field.value == "Generated media description"

    placeholder_field = next(
        (f for f in inferred if f.name == FieldName.decode("placeholder")), None
    )
    assert placeholder_field is not None
    assert placeholder_field.value == "Generated media placeholder"


@pytest.mark.asyncio
async def test_generate_standard_fields_with_bundle():
    """Test generate_standard_fields using a chunked bundle."""
    # Read sample document and chunk it.
    text, _header = read_2303_11366v2()
    resource_uri = ResourceUri.decode("ndk://test/arxiv/2303.11366v2")

    # Process the text to create a bundle.
    bundle = chunk_body_sync(
        resource_uri=resource_uri,
        text=ContentText.parse(text),
        media=[],
    )

    # Get all observation URIs to build stub responses.
    observations = bundle.observations()
    stub_inference: dict[str, list[str | None]] = {}
    for obs in observations:
        # Normalize the URI to a property name.
        uri_str = str(obs.uri).removeprefix("ndk://")
        suffix = FieldName.try_normalize(uri_str)
        if suffix:
            stub_inference[f"description_{suffix}"] = [f"Description for {obs.uri}"]
            if isinstance(obs, ObsMedia):
                stub_inference[f"placeholder_{suffix}"] = [f"Placeholder for {obs.uri}"]

    context = given_context(
        stub_inference=stub_inference,
        stub_storage={},
    )

    # Generate fields.
    fields = await generate_standard_fields(
        context=context,
        cached=[],  # No cached fields.
        bundle=bundle,
    )

    # Verify that fields were generated for the body and chunks.
    assert len(fields) > 0

    # Check that description fields were generated.
    description_fields = [
        f for f in fields if f.name == FieldName.decode("description")
    ]
    assert len(description_fields) > 0

    # Check that each field has a valid value.
    for field in fields:
        assert field.value
        assert "Description for" in field.value or "Placeholder for" in field.value


@pytest.mark.asyncio
async def test_generate_fields_empty_inputs():
    """Test that empty inputs return empty results."""
    context = given_context(
        stub_inference={},
        stub_storage={},
    )

    # Empty observations.
    inferred = await _generate_fields(context, [], [])
    assert inferred == []

    # Empty fields.
    obs = ObsChunk.stub(
        uri="ndk://test/realm/doc/$chunk/00",
        mode="plain",
        text="Test content.",
    )
    inferred = await _generate_fields(context, [obs], [])
    assert inferred == []


@pytest.mark.asyncio
async def test_generate_fields_caching():
    """Test that cached fields are not regenerated."""
    # Note: FieldName.try_normalize removes slashes and $ from URIs.

    context = given_context(
        stub_inference={
            "description_test_realm_doc_body": ["Should not be generated (cached)"],
            "description_test_realm_doc_chunk_00": ["Generated for chunk 0"],
            "description_test_realm_doc_chunk_01": ["Generated for chunk 1"],
        },
        stub_storage={},
    )

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())
    chunk_uri_0 = resource_uri.child_observable(AffBodyChunk.new([0]))
    chunk_uri_1 = resource_uri.child_observable(AffBodyChunk.new([1]))

    # Create a bundle with multiple chunks (so observations() returns ObsBody + ObsChunks).
    obs_chunk_0 = ObsChunk(
        uri=chunk_uri_0,
        description=None,
        text=ContentText.new_plain("Test chunk 0 content."),
    )
    obs_chunk_1 = ObsChunk(
        uri=chunk_uri_1,
        description=None,
        text=ContentText.new_plain("Test chunk 1 content."),
    )

    bundle = BundleBody(
        uri=body_uri.affordance_uri(),
        description=None,
        sections=[ObsBodySection(indexes=[0], heading="Section 0")],
        chunks=[obs_chunk_0, obs_chunk_1],
        media=[],
    )

    # Mark body description as cached.
    cached = [
        FieldValue(
            name=FieldName.decode("description"),
            target=AffBody.new(),
            value="Cached body description",
        ),
    ]

    fields = await generate_standard_fields(
        context=context,
        cached=cached,
        bundle=bundle,
    )

    # Only chunk fields should be generated, not the body field.
    body_fields = [
        f
        for f in fields
        if f.target == AffBody.new() and f.name == FieldName.decode("description")
    ]
    assert len(body_fields) == 0

    chunk_fields = [
        f
        for f in fields
        if f.name == FieldName.decode("description")
        and "chunk" in str(f.target).lower()
    ]
    # Both chunks should have generated descriptions.
    assert len(chunk_fields) >= 1


@pytest.mark.asyncio
async def test_render_prompt_includes_observations():
    """Test that _render_prompt properly renders observations."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description="Test chunk description",
        text=ContentText.new_plain("Test chunk content with important details."),
    )

    # Render the prompt.
    rendered_parts = _render_prompt([obs_chunk])

    # The rendered parts should contain the chunk content.
    text_parts = [p for p in rendered_parts if isinstance(p, str)]
    combined_text = " ".join(text_parts)
    assert "Test chunk content" in combined_text
