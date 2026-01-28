import pytest

from base.api.knowledge import QueryField
from base.models.content import ContentText, PartLink
from base.resources.aff_body import (
    AffBody,
    AffBodyChunk,
    AffBodyMedia,
    AnyBodyObservableUri,
    ObsBody,
    ObsBodySection,
    ObsChunk,
    ObsMedia,
)
from base.resources.metadata import FieldName, FieldValue, ResourceField
from base.strings.resource import ObservableUri, ResourceUri

from knowledge.domain.chunking import chunk_body_sync
from knowledge.domain.fields import (
    GenerateFieldsItem,
    _build_inference_params,
    _explode_api_fields,
    _explode_api_field_forall,
    _explode_api_field_single,
    _generate_fields,
    _group_observations_by_tokens,
    _matches_api_field_filters,
    _parse_response,
    _render_prompt,
    generate_api_fields,
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
        ResourceField(
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


##
## API Fields - Unit Tests
##


def test_matches_api_field_filters_no_filters():
    """Test that observations match when no filters are set."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Test content."),
    )

    # Field with no prefixes and no targets matches everything.
    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],
        prefixes=None,
        targets=None,
    )

    assert _matches_api_field_filters(obs, field) is True


def test_matches_api_field_filters_with_matching_prefix():
    """Test that observations match with a matching prefix."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Test content."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],
        prefixes=["ndk://test/realm/"],
        targets=None,
    )

    assert _matches_api_field_filters(obs, field) is True


def test_matches_api_field_filters_with_non_matching_prefix():
    """Test that observations don't match with a non-matching prefix."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Test content."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],
        prefixes=["ndk://other/realm/"],
        targets=None,
    )

    assert _matches_api_field_filters(obs, field) is False


def test_matches_api_field_filters_with_matching_target():
    """Test that observations match when in targets list."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Test content."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],
        prefixes=None,
        targets=[chunk_uri],
    )

    assert _matches_api_field_filters(obs, field) is True


def test_matches_api_field_filters_with_non_matching_target():
    """Test that observations don't match when not in targets list."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri_0 = resource_uri.child_observable(AffBodyChunk.new([0]))
    chunk_uri_1 = resource_uri.child_observable(AffBodyChunk.new([1]))

    obs = ObsChunk(
        uri=chunk_uri_0,
        description=None,
        text=ContentText.new_plain("Test content."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],
        prefixes=None,
        targets=[chunk_uri_1],  # Different target
    )

    assert _matches_api_field_filters(obs, field) is False


def test_matches_api_field_filters_prefix_and_target():
    """Test filtering with both prefix and target set."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Test content."),
    )

    # Prefix matches but target doesn't.
    other_resource_uri = ResourceUri.decode("ndk://other/realm/doc")
    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],
        prefixes=["ndk://test/realm/"],
        targets=[other_resource_uri.child_observable(AffBodyChunk.new([0]))],
    )

    assert _matches_api_field_filters(obs, field) is False


def test_explode_api_field_forall_basic():
    """Test _explode_api_field_forall with basic observations."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Body content."),
        sections=[],
        chunks=[],
    )
    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Chunk content."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["body", "chunk"],
        prefixes=None,
        targets=None,
    )

    result: dict[FieldName, GenerateFieldsItem] = {}
    _explode_api_field_forall(
        cached=set(),
        observations=[obs_body, obs_chunk],
        field=field,
        result=result,
    )

    assert FieldName.decode("summary") in result
    item = result[FieldName.decode("summary")]
    assert item.targets is not None
    assert len(item.targets) == 2
    assert body_uri in item.targets
    assert chunk_uri in item.targets


def test_explode_api_field_forall_filters_by_type():
    """Test that _explode_api_field_forall filters by observation type."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Body content."),
        sections=[],
        chunks=[],
    )
    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Chunk content."),
    )

    # Field only targets chunks.
    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],  # Only chunks
        prefixes=None,
        targets=None,
    )

    result: dict[FieldName, GenerateFieldsItem] = {}
    _explode_api_field_forall(
        cached=set(),
        observations=[obs_body, obs_chunk],
        field=field,
        result=result,
    )

    assert FieldName.decode("summary") in result
    item = result[FieldName.decode("summary")]
    assert item.targets is not None
    assert len(item.targets) == 1
    assert chunk_uri in item.targets
    assert body_uri not in item.targets


def test_explode_api_field_forall_respects_cache():
    """Test that _explode_api_field_forall skips cached observations."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri_0 = resource_uri.child_observable(AffBodyChunk.new([0]))
    chunk_uri_1 = resource_uri.child_observable(AffBodyChunk.new([1]))

    obs_chunk_0 = ObsChunk(
        uri=chunk_uri_0,
        description=None,
        text=ContentText.new_plain("Chunk 0 content."),
    )
    obs_chunk_1 = ObsChunk(
        uri=chunk_uri_1,
        description=None,
        text=ContentText.new_plain("Chunk 1 content."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],
        prefixes=None,
        targets=None,
    )

    # Mark chunk 0 as cached.
    cached: set[tuple[FieldName, AnyBodyObservableUri]] = {
        (FieldName.decode("summary"), chunk_uri_0)
    }

    result: dict[FieldName, GenerateFieldsItem] = {}
    _explode_api_field_forall(
        cached=cached,
        observations=[obs_chunk_0, obs_chunk_1],
        field=field,
        result=result,
    )

    assert FieldName.decode("summary") in result
    item = result[FieldName.decode("summary")]
    assert item.targets is not None
    assert len(item.targets) == 1
    assert chunk_uri_1 in item.targets
    assert chunk_uri_0 not in item.targets


def test_explode_api_field_forall_with_prefix_filter():
    """Test that _explode_api_field_forall respects prefix filter."""
    resource_uri_1 = ResourceUri.decode("ndk://test/realm1/doc")
    resource_uri_2 = ResourceUri.decode("ndk://test/realm2/doc")
    chunk_uri_1 = resource_uri_1.child_observable(AffBodyChunk.new([0]))
    chunk_uri_2 = resource_uri_2.child_observable(AffBodyChunk.new([0]))

    obs_chunk_1 = ObsChunk(
        uri=chunk_uri_1,
        description=None,
        text=ContentText.new_plain("Chunk from realm1."),
    )
    obs_chunk_2 = ObsChunk(
        uri=chunk_uri_2,
        description=None,
        text=ContentText.new_plain("Chunk from realm2."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=["chunk"],
        prefixes=["ndk://test/realm1/"],  # Only realm1
        targets=None,
    )

    result: dict[FieldName, GenerateFieldsItem] = {}
    _explode_api_field_forall(
        cached=set(),
        observations=[obs_chunk_1, obs_chunk_2],
        field=field,
        result=result,
    )

    assert FieldName.decode("summary") in result
    item = result[FieldName.decode("summary")]
    assert item.targets is not None
    assert len(item.targets) == 1
    assert chunk_uri_1 in item.targets


def test_explode_api_field_single_basic():
    """Test _explode_api_field_single creates a global field."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri_0 = resource_uri.child_observable(AffBodyChunk.new([0]))
    chunk_uri_1 = resource_uri.child_observable(AffBodyChunk.new([1]))

    obs_chunk_0 = ObsChunk(
        uri=chunk_uri_0,
        description=None,
        text=ContentText.new_plain("Chunk 0 content."),
    )
    obs_chunk_1 = ObsChunk(
        uri=chunk_uri_1,
        description=None,
        text=ContentText.new_plain("Chunk 1 content."),
    )

    # Field with forall=None creates a single global field.
    field = QueryField(
        name=FieldName.decode("overall_summary"),
        description="Generate an overall summary.",
        forall=None,
        prefixes=None,
        targets=None,
    )

    result: dict[FieldName, GenerateFieldsItem] = {}
    _explode_api_field_single(
        cached=set(),
        observations=[obs_chunk_0, obs_chunk_1],
        field=field,
        result=result,
    )

    assert FieldName.decode("overall_summary") in result
    item = result[FieldName.decode("overall_summary")]
    assert item.targets is None
    assert item.result_target == chunk_uri_0  # First observation as representative


def test_explode_api_field_single_with_prefix_filter():
    """Test _explode_api_field_single respects prefix filter."""
    resource_uri_1 = ResourceUri.decode("ndk://test/realm1/doc")
    resource_uri_2 = ResourceUri.decode("ndk://test/realm2/doc")
    chunk_uri_1 = resource_uri_1.child_observable(AffBodyChunk.new([0]))
    chunk_uri_2 = resource_uri_2.child_observable(AffBodyChunk.new([0]))

    obs_chunk_1 = ObsChunk(
        uri=chunk_uri_1,
        description=None,
        text=ContentText.new_plain("Chunk from realm1."),
    )
    obs_chunk_2 = ObsChunk(
        uri=chunk_uri_2,
        description=None,
        text=ContentText.new_plain("Chunk from realm2."),
    )

    field = QueryField(
        name=FieldName.decode("realm2_summary"),
        description="Generate a summary for realm2.",
        forall=None,
        prefixes=["ndk://test/realm2/"],  # Only realm2
        targets=None,
    )

    result: dict[FieldName, GenerateFieldsItem] = {}
    _explode_api_field_single(
        cached=set(),
        observations=[obs_chunk_1, obs_chunk_2],
        field=field,
        result=result,
    )

    assert FieldName.decode("realm2_summary") in result
    item = result[FieldName.decode("realm2_summary")]
    assert item.targets is None
    assert item.result_target == chunk_uri_2  # First matching (realm2)


def test_explode_api_field_single_no_matches():
    """Test _explode_api_field_single with no matching observations."""
    resource_uri = ResourceUri.decode("ndk://test/realm1/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Chunk content."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=None,
        prefixes=["ndk://nonexistent/"],  # No match
        targets=None,
    )

    result: dict[FieldName, GenerateFieldsItem] = {}
    _explode_api_field_single(
        cached=set(),
        observations=[obs_chunk],
        field=field,
        result=result,
    )

    assert FieldName.decode("summary") not in result


def test_explode_api_field_single_respects_cache():
    """Test _explode_api_field_single skips if representative target is cached."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Chunk content."),
    )

    field = QueryField(
        name=FieldName.decode("summary"),
        description="Generate a summary.",
        forall=None,
        prefixes=None,
        targets=None,
    )

    # Mark the first (representative) observation as cached.
    cached: set[tuple[FieldName, AnyBodyObservableUri]] = {
        (FieldName.decode("summary"), chunk_uri)
    }

    result: dict[FieldName, GenerateFieldsItem] = {}
    _explode_api_field_single(
        cached=cached,
        observations=[obs_chunk],
        field=field,
        result=result,
    )

    assert FieldName.decode("summary") not in result


def test_explode_api_fields_mixed():
    """Test _explode_api_fields with both forall and single fields."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Body content."),
        sections=[],
        chunks=[],
    )
    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Chunk content."),
    )

    fields = [
        # Per-observation field.
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body", "chunk"],
            prefixes=None,
            targets=None,
        ),
        # Single global field.
        QueryField(
            name=FieldName.decode("overall_summary"),
            description="Generate an overall summary.",
            forall=None,
            prefixes=None,
            targets=None,
        ),
    ]

    items = _explode_api_fields(
        cached=set(),
        observations=[obs_body, obs_chunk],
        fields=fields,
    )

    assert len(items) == 2

    description_item = next(
        (i for i in items if i.name == FieldName.decode("description")), None
    )
    assert description_item is not None
    assert description_item.targets is not None
    assert len(description_item.targets) == 2

    summary_item = next(
        (i for i in items if i.name == FieldName.decode("overall_summary")), None
    )
    assert summary_item is not None
    assert summary_item.targets is None
    assert summary_item.result_target is not None


def test_build_inference_params_with_result_target():
    """Test _build_inference_params handles result_target for global fields."""
    target_uri = ObservableUri.decode("ndk://test/realm/doc/$body")

    field_item = GenerateFieldsItem(
        name=FieldName.decode("summary"),
        description="Generate a summary of the document.",
        targets=None,
        result_target=target_uri,
    )

    system, schema, mapping = _build_inference_params([field_item])

    # Global fields don't add a specific system message part (make_system returns None).
    # The base system message is still present.
    assert "knowledge extraction" in system.lower()

    # Check schema has the property (just the field name, not target-specific).
    assert "properties" in schema
    assert "summary" in schema["properties"]

    # Check mapping uses result_target.
    assert "summary" in mapping
    assert mapping["summary"] == (FieldName.decode("summary"), target_uri)


def test_build_inference_params_mixed_fields():
    """Test _build_inference_params with both targeted and global fields."""
    target1 = ObservableUri.decode("ndk://test/realm/doc1/$body")
    target2 = ObservableUri.decode("ndk://test/realm/doc2/$body")
    global_target = ObservableUri.decode("ndk://test/realm/doc1/$chunk/00")

    field_items = [
        # Targeted field.
        GenerateFieldsItem(
            name=FieldName.decode("description"),
            description="Generate a description.",
            targets=[target1, target2],
            result_target=None,
        ),
        # Global field.
        GenerateFieldsItem(
            name=FieldName.decode("overall_summary"),
            description="Generate an overall summary.",
            targets=None,
            result_target=global_target,
        ),
    ]

    _, schema, mapping = _build_inference_params(field_items)

    # Should have 3 properties: 2 for descriptions + 1 for summary.
    assert len(schema["properties"]) == 3

    # Check targeted field properties.
    assert "description_test_realm_doc1_body" in schema["properties"]
    assert "description_test_realm_doc2_body" in schema["properties"]

    # Check global field property.
    assert "overall_summary" in schema["properties"]

    # Check mappings.
    assert len(mapping) == 3
    assert "overall_summary" in mapping
    assert mapping["overall_summary"][1] == global_target


def test_generate_fields_item_make_system_global():
    """Test GenerateFieldsItem.make_system for global fields (no targets)."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())

    item = GenerateFieldsItem(
        name=FieldName.decode("summary"),
        description="Generate a comprehensive summary.",
        targets=None,
        result_target=body_uri,
    )

    system_part, properties = item.make_system()

    # Global fields don't have a system message part.
    assert system_part is None

    # Should have single property with field name.
    assert len(properties) == 1
    assert properties[0][0] == "summary"
    assert "comprehensive summary" in properties[0][1]


def test_generate_fields_item_make_system_targeted():
    """Test GenerateFieldsItem.make_system for targeted fields."""
    target1 = ObservableUri.decode("ndk://test/realm/doc/$body")
    target2 = ObservableUri.decode("ndk://test/realm/doc/$chunk/00")

    item = GenerateFieldsItem(
        name=FieldName.decode("description"),
        description="Generate a description.",
        targets=[target1, target2],
        result_target=None,
    )

    system_part, properties = item.make_system()

    # Targeted fields have a system message part.
    assert system_part is not None
    assert "description" in system_part
    assert str(target1) in system_part

    # Should have properties for each target.
    assert len(properties) == 2


##
## API Fields - Integration Tests
##


@pytest.mark.asyncio
async def test_generate_api_fields_empty_inputs():
    """Test generate_api_fields with empty inputs."""
    context = given_context(stub_inference={}, stub_storage={})

    # Empty bundles.
    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[],
        fields=[
            QueryField(
                name=FieldName.decode("summary"),
                description="Test",
                forall=["body"],
            )
        ],
    )
    assert result == []

    # Empty fields.
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test content."),
    )
    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle],
        fields=[],
    )
    assert result == []


@pytest.mark.asyncio
async def test_generate_api_fields_forall_single_bundle():
    """Test generate_api_fields with forall set on a single bundle."""
    context = given_context(
        stub_inference={
            "description_test_realm_doc_body": ["Generated body description"],
        },
        stub_storage={},
    )

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test document content."),
    )

    fields = [
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body"],
            prefixes=None,
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle],
        fields=fields,
    )

    assert len(result) == 1
    assert result[0].name == FieldName.decode("description")
    assert result[0].value == "Generated body description"


@pytest.mark.asyncio
async def test_generate_api_fields_forall_multiple_bundles():
    """Test generate_api_fields with forall set on multiple bundles."""
    context = given_context(
        stub_inference={
            "description_test_realm_doc1_body": ["Description for doc1"],
            "description_test_realm_doc2_body": ["Description for doc2"],
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

    fields = [
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body"],
            prefixes=None,
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle_1, bundle_2],
        fields=fields,
    )

    assert len(result) == 2

    doc1_field = next((f for f in result if "doc1" in str(f.target)), None)
    assert doc1_field is not None
    assert doc1_field.value == "Description for doc1"

    doc2_field = next((f for f in result if "doc2" in str(f.target)), None)
    assert doc2_field is not None
    assert doc2_field.value == "Description for doc2"


@pytest.mark.asyncio
async def test_generate_api_fields_single_global_field():
    """Test generate_api_fields with forall=None (single global field)."""
    context = given_context(
        stub_inference={
            "overall_summary": ["Combined summary of all documents"],
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

    fields = [
        QueryField(
            name=FieldName.decode("overall_summary"),
            description="Generate an overall summary of all documents.",
            forall=None,  # Single field, not per observation
            prefixes=None,
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle_1, bundle_2],
        fields=fields,
    )

    assert len(result) == 1
    assert result[0].name == FieldName.decode("overall_summary")
    assert result[0].value == "Combined summary of all documents"


@pytest.mark.asyncio
async def test_generate_api_fields_with_cached():
    """Test generate_api_fields respects cached values."""
    context = given_context(
        stub_inference={
            "description_test_realm_doc1_body": ["Should not be used (cached)"],
            "description_test_realm_doc2_body": ["Description for doc2"],
        },
        stub_storage={},
    )

    resource_uri_1 = ResourceUri.decode("ndk://test/realm/doc1")
    resource_uri_2 = ResourceUri.decode("ndk://test/realm/doc2")
    body_uri_1 = resource_uri_1.child_observable(AffBody.new())

    bundle_1 = BundleBody.make_single(
        resource_uri=resource_uri_1,
        text=ContentText.new_plain("Document 1 content."),
    )
    bundle_2 = BundleBody.make_single(
        resource_uri=resource_uri_2,
        text=ContentText.new_plain("Document 2 content."),
    )

    # Mark doc1 as cached.
    cached = [
        FieldValue(
            name=FieldName.decode("description"),
            target=body_uri_1,
            value="Cached description for doc1",
        ),
    ]

    fields = [
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body"],
            prefixes=None,
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=cached,
        bundles=[bundle_1, bundle_2],
        fields=fields,
    )

    # Only doc2 should have a generated field.
    assert len(result) == 1
    assert "doc2" in str(result[0].target)
    assert result[0].value == "Description for doc2"


@pytest.mark.asyncio
async def test_generate_api_fields_with_prefix_filter():
    """Test generate_api_fields with prefix filter."""
    context = given_context(
        stub_inference={
            "description_test_realm1_doc_body": ["Description for realm1"],
        },
        stub_storage={},
    )

    resource_uri_1 = ResourceUri.decode("ndk://test/realm1/doc")
    resource_uri_2 = ResourceUri.decode("ndk://test/realm2/doc")

    bundle_1 = BundleBody.make_single(
        resource_uri=resource_uri_1,
        text=ContentText.new_plain("Realm 1 document."),
    )
    bundle_2 = BundleBody.make_single(
        resource_uri=resource_uri_2,
        text=ContentText.new_plain("Realm 2 document."),
    )

    fields = [
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body"],
            prefixes=["ndk://test/realm1/"],  # Only realm1
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle_1, bundle_2],
        fields=fields,
    )

    # Only realm1 should have a generated field.
    assert len(result) == 1
    assert "realm1" in str(result[0].target)


@pytest.mark.asyncio
async def test_generate_api_fields_with_specific_targets():
    """Test generate_api_fields with specific targets filter."""
    resource_uri_1 = ResourceUri.decode("ndk://test/realm/doc1")
    resource_uri_2 = ResourceUri.decode("ndk://test/realm/doc2")
    body_uri_1 = resource_uri_1.child_observable(AffBody.new())

    context = given_context(
        stub_inference={
            "description_test_realm_doc1_body": ["Description for doc1"],
        },
        stub_storage={},
    )

    bundle_1 = BundleBody.make_single(
        resource_uri=resource_uri_1,
        text=ContentText.new_plain("Document 1 content."),
    )
    bundle_2 = BundleBody.make_single(
        resource_uri=resource_uri_2,
        text=ContentText.new_plain("Document 2 content."),
    )

    fields = [
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body"],
            prefixes=None,
            targets=[body_uri_1],  # Only doc1
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle_1, bundle_2],
        fields=fields,
    )

    # Only doc1 should have a generated field.
    assert len(result) == 1
    assert result[0].target == body_uri_1


@pytest.mark.asyncio
async def test_generate_api_fields_mixed_forall_and_single():
    """Test generate_api_fields with both forall and single fields."""
    context = given_context(
        stub_inference={
            "description_test_realm_doc1_body": ["Description for doc1"],
            "description_test_realm_doc2_body": ["Description for doc2"],
            "overall_summary": ["Combined summary"],
        },
        stub_storage={},
    )

    resource_uri_1 = ResourceUri.decode("ndk://test/realm/doc1")
    resource_uri_2 = ResourceUri.decode("ndk://test/realm/doc2")

    bundle_1 = BundleBody.make_single(
        resource_uri=resource_uri_1,
        text=ContentText.new_plain("Document 1."),
    )
    bundle_2 = BundleBody.make_single(
        resource_uri=resource_uri_2,
        text=ContentText.new_plain("Document 2."),
    )

    fields = [
        # Per-observation field.
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body"],
            prefixes=None,
            targets=None,
        ),
        # Global field.
        QueryField(
            name=FieldName.decode("overall_summary"),
            description="Generate an overall summary.",
            forall=None,
            prefixes=None,
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle_1, bundle_2],
        fields=fields,
    )

    # Should have 3 fields: 2 descriptions + 1 overall summary.
    assert len(result) == 3

    description_fields = [
        f for f in result if f.name == FieldName.decode("description")
    ]
    assert len(description_fields) == 2

    summary_fields = [
        f for f in result if f.name == FieldName.decode("overall_summary")
    ]
    assert len(summary_fields) == 1
    assert summary_fields[0].value == "Combined summary"


@pytest.mark.asyncio
async def test_generate_api_fields_with_multiple_chunks():
    """Test generate_api_fields with bundles containing multiple chunks."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri_0 = resource_uri.child_observable(AffBodyChunk.new([0]))
    chunk_uri_1 = resource_uri.child_observable(AffBodyChunk.new([1]))

    context = given_context(
        stub_inference={
            "description_test_realm_doc_body": ["Body description"],
            "description_test_realm_doc_chunk_00": ["Chunk 0 description"],
            "description_test_realm_doc_chunk_01": ["Chunk 1 description"],
        },
        stub_storage={},
    )

    obs_chunk_0 = ObsChunk(
        uri=chunk_uri_0,
        description=None,
        text=ContentText.new_plain("Chunk 0 content."),
    )
    obs_chunk_1 = ObsChunk(
        uri=chunk_uri_1,
        description=None,
        text=ContentText.new_plain("Chunk 1 content."),
    )

    bundle = BundleBody(
        uri=resource_uri.child_affordance(AffBody.new()),
        description=None,
        sections=[ObsBodySection(indexes=[0], heading="Section 0")],
        chunks=[obs_chunk_0, obs_chunk_1],
        media=[],
    )

    fields = [
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body", "chunk"],
            prefixes=None,
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle],
        fields=fields,
    )

    # Should have 3 fields: 1 body + 2 chunks.
    assert len(result) == 3

    body_field = next((f for f in result if "body" in str(f.target).lower()), None)
    assert body_field is not None

    chunk_fields = [f for f in result if "chunk" in str(f.target).lower()]
    assert len(chunk_fields) == 2


@pytest.mark.asyncio
async def test_generate_api_fields_media_observations():
    """Test generate_api_fields with media observations."""
    obs_media = given_sample_media()
    resource_uri = obs_media.uri.resource_uri()
    media_uri = resource_uri.child_observable(AffBodyMedia.new())

    # Build the property key that the stub inference expects.
    uri_str = str(media_uri).removeprefix("ndk://")
    suffix = FieldName.try_normalize(uri_str)

    context = given_context(
        stub_inference={
            f"placeholder_{suffix}": ["Generated placeholder for image"],
        },
        stub_storage={},
    )

    # Create a bundle with multiple chunks so media is not inlined.
    # (When there's only one chunk embedding one media, they get merged.)
    chunk_0_uri = resource_uri.child_observable(AffBodyChunk.new([0]))
    chunk_1_uri = resource_uri.child_observable(AffBodyChunk.new([1]))

    obs_chunk_0 = ObsChunk(
        uri=chunk_0_uri,
        description=None,
        text=ContentText.new([PartLink.new("embed", None, media_uri)]),
    )
    obs_chunk_1 = ObsChunk(
        uri=chunk_1_uri,
        description=None,
        text=ContentText.new_plain("Additional text content."),
    )

    bundle = BundleBody(
        uri=resource_uri.child_affordance(AffBody.new()),
        description=None,
        sections=[],
        chunks=[obs_chunk_0, obs_chunk_1],
        media=[
            ObsMedia(
                uri=media_uri,
                description=obs_media.description,
                placeholder=None,
                mime_type=obs_media.mime_type,
                blob=obs_media.blob,
            )
        ],
    )

    fields = [
        QueryField(
            name=FieldName.decode("placeholder"),
            description="Generate a placeholder for the media.",
            forall=["media"],
            prefixes=None,
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle],
        fields=fields,
    )

    # Should generate placeholder for media.
    placeholder_fields = [
        f for f in result if f.name == FieldName.decode("placeholder")
    ]
    assert len(placeholder_fields) == 1
    assert "placeholder" in placeholder_fields[0].value.lower()


@pytest.mark.asyncio
async def test_generate_api_fields_single_with_prefix_filter():
    """Test generate_api_fields single field with prefix filter."""
    resource_uri_1 = ResourceUri.decode("ndk://test/realm1/doc")
    resource_uri_2 = ResourceUri.decode("ndk://test/realm2/doc")

    context = given_context(
        stub_inference={
            "realm2_summary": ["Summary for realm2 only"],
        },
        stub_storage={},
    )

    bundle_1 = BundleBody.make_single(
        resource_uri=resource_uri_1,
        text=ContentText.new_plain("Realm 1 content."),
    )
    bundle_2 = BundleBody.make_single(
        resource_uri=resource_uri_2,
        text=ContentText.new_plain("Realm 2 content."),
    )

    fields = [
        QueryField(
            name=FieldName.decode("realm2_summary"),
            description="Generate a summary using only realm2 observations.",
            forall=None,  # Single field
            prefixes=["ndk://test/realm2/"],  # Only realm2
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle_1, bundle_2],
        fields=fields,
    )

    # Should generate one field, associated with the first realm2 observation.
    assert len(result) == 1
    assert result[0].name == FieldName.decode("realm2_summary")
    assert "realm2" in str(result[0].target)


@pytest.mark.asyncio
async def test_generate_api_fields_returns_field_values():
    """Test that generate_api_fields returns FieldValue instances."""
    context = given_context(
        stub_inference={
            "description_test_realm_doc_body": ["Test description"],
        },
        stub_storage={},
    )

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test content."),
    )

    fields = [
        QueryField(
            name=FieldName.decode("description"),
            description="Generate a description.",
            forall=["body"],
            prefixes=None,
            targets=None,
        ),
    ]

    result = await generate_api_fields(
        context=context,
        cached=[],
        bundles=[bundle],
        fields=fields,
    )

    assert len(result) == 1
    assert isinstance(result[0], FieldValue)
    assert isinstance(result[0].name, FieldName)
    assert result[0].target is not None
    assert isinstance(result[0].value, str)
