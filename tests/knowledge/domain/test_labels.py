import pytest

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
from base.resources.label import (
    AllowRule,
    LabelDefinition,
    LabelInfo,
    LabelName,
    LabelValue,
    ResourceFilters,
    ResourceLabel,
)
from base.strings.resource import ObservableUri, ResourceUri

from knowledge.domain.chunking import chunk_body_sync
from knowledge.domain.labels import (
    _build_inference_params,
    _explode_definitions,
    _filter_items_for_group,
    _group_observations_by_tokens,
    _LabelItem,
    _matches_observation_type,
    _parse_response,
    _render_prompt,
    _run_inference,
    generate_labels,
    generate_standard_labels,
)
from knowledge.models.storage_observed import BundleBody

from tests.data.samples import given_sample_media, read_2303_11366v2
from tests.knowledge.utils_connectors import given_context


##
## Unit Tests - Observation Grouping
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


##
## Unit Tests - Response Parsing
##


def test_parse_response_valid_json():
    """Test parsing a valid JSON response."""
    target_uri = ObservableUri.decode("ndk://test/resource/doc/$body")
    property_mapping = {
        "description_test_resource_doc_body": (
            LabelName.decode("description"),
            target_uri,
        ),
    }

    response_json = '{"description_test_resource_doc_body": "A test description"}'
    inferred = _parse_response(response_json, property_mapping)

    assert len(inferred) == 1
    assert inferred[0].name == LabelName.decode("description")
    assert inferred[0].target == target_uri
    assert inferred[0].value == "A test description"


def test_parse_response_null_value():
    """Test that null values are skipped."""
    target_uri = ObservableUri.decode("ndk://test/resource/doc/$body")
    property_mapping = {
        "description_test_resource_doc_body": (
            LabelName.decode("description"),
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


def test_parse_response_whitespace_only():
    """Test that whitespace-only values are skipped."""
    target_uri = ObservableUri.decode("ndk://test/resource/doc/$body")
    property_mapping = {
        "description_test_resource_doc_body": (
            LabelName.decode("description"),
            target_uri,
        ),
    }

    response_json = '{"description_test_resource_doc_body": "   "}'
    inferred = _parse_response(response_json, property_mapping)

    assert len(inferred) == 0


##
## Unit Tests - Observation Type Matching
##


def test_matches_observation_type_body():
    """Test matching body observations."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Body content."),
        sections=[],
        chunks=[],
    )

    assert _matches_observation_type(obs_body, ["body"]) is True
    assert _matches_observation_type(obs_body, ["chunk"]) is False
    assert _matches_observation_type(obs_body, ["media"]) is False
    assert _matches_observation_type(obs_body, ["body", "chunk"]) is True


def test_matches_observation_type_chunk():
    """Test matching chunk observations."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Chunk content."),
    )

    assert _matches_observation_type(obs_chunk, ["chunk"]) is True
    assert _matches_observation_type(obs_chunk, ["body"]) is False
    assert _matches_observation_type(obs_chunk, ["media"]) is False
    assert _matches_observation_type(obs_chunk, ["chunk", "media"]) is True


def test_matches_observation_type_media():
    """Test matching media observations."""
    obs_media = given_sample_media()

    assert _matches_observation_type(obs_media, ["media"]) is True
    assert _matches_observation_type(obs_media, ["body"]) is False
    assert _matches_observation_type(obs_media, ["chunk"]) is False
    assert _matches_observation_type(obs_media, ["body", "media"]) is True


##
## Unit Tests - Definition Expansion
##


def test_explode_definitions_basic():
    """Test expanding label definitions to label items."""
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

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("description"),
                forall=["body", "chunk"],
                prompt="Generate a description.",
            ),
        ),
    ]

    items = _explode_definitions(
        cached=set(),
        observations=[obs_body, obs_chunk],
        definitions=definitions,
    )

    assert len(items) == 1
    assert items[0].name == LabelName.decode("description")
    assert len(items[0].targets) == 2
    assert body_uri in items[0].targets
    assert chunk_uri in items[0].targets


def test_explode_definitions_filters_by_type():
    """Test that definitions filter by observation type."""
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

    # Definition only targets chunks.
    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("summary"),
                forall=["chunk"],  # Only chunks
                prompt="Generate a summary.",
            ),
        ),
    ]

    items = _explode_definitions(
        cached=set(),
        observations=[obs_body, obs_chunk],
        definitions=definitions,
    )

    assert len(items) == 1
    assert len(items[0].targets) == 1
    assert chunk_uri in items[0].targets
    assert body_uri not in items[0].targets


def test_explode_definitions_respects_cache():
    """Test that cached observations are skipped."""
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

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("summary"),
                forall=["chunk"],
                prompt="Generate a summary.",
            ),
        ),
    ]

    # Mark chunk 0 as cached.
    cached: set[tuple[LabelName, AnyBodyObservableUri]] = {
        (LabelName.decode("summary"), chunk_uri_0)
    }

    items = _explode_definitions(
        cached=cached,
        observations=[obs_chunk_0, obs_chunk_1],
        definitions=definitions,
    )

    assert len(items) == 1
    assert len(items[0].targets) == 1
    assert chunk_uri_1 in items[0].targets
    assert chunk_uri_0 not in items[0].targets


def test_explode_definitions_respects_resource_filters():
    """Test that resource filters are respected."""
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

    # Definition with filter blocking realm2.
    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("summary"),
                forall=["chunk"],
                prompt="Generate a summary.",
            ),
            filters=ResourceFilters(
                default="block",
                allowlist=[AllowRule(action="allow", prefix="ndk://test/realm1/")],
            ),
        ),
    ]

    items = _explode_definitions(
        cached=set(),
        observations=[obs_chunk_1, obs_chunk_2],
        definitions=definitions,
    )

    assert len(items) == 1
    assert len(items[0].targets) == 1
    assert chunk_uri_1 in items[0].targets


def test_explode_definitions_empty_forall():
    """Test that definitions with empty forall are skipped."""
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    body_uri = resource_uri.child_observable(AffBody.new())

    obs_body = ObsBody(
        uri=body_uri,
        description=None,
        content=ContentText.new_plain("Body content."),
        sections=[],
        chunks=[],
    )

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("summary"),
                forall=[],  # Empty - matches nothing
                prompt="Generate a summary.",
            ),
        ),
    ]

    items = _explode_definitions(
        cached=set(),
        observations=[obs_body],
        definitions=definitions,
    )

    assert len(items) == 0


def test_explode_definitions_multiple_definitions():
    """Test expanding multiple label definitions."""
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

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("description"),
                forall=["body", "chunk"],
                prompt="Generate a description.",
            ),
        ),
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("keywords"),
                forall=["body"],  # Only body
                prompt="Extract keywords.",
            ),
        ),
    ]

    items = _explode_definitions(
        cached=set(),
        observations=[obs_body, obs_chunk],
        definitions=definitions,
    )

    # Should have 2 items: description and keywords.
    assert len(items) == 2

    description_item = next(
        (i for i in items if i.name == LabelName.decode("description")), None
    )
    assert description_item is not None
    assert len(description_item.targets) == 2

    keywords_item = next(
        (i for i in items if i.name == LabelName.decode("keywords")), None
    )
    assert keywords_item is not None
    assert len(keywords_item.targets) == 1
    assert body_uri in keywords_item.targets


##
## Unit Tests - Item Filtering
##


def test_filter_items_for_group():
    """Test filtering label items for a specific observation group."""
    target1 = ObservableUri.decode("ndk://test/realm/doc1/$body")
    target2 = ObservableUri.decode("ndk://test/realm/doc2/$body")
    target3 = ObservableUri.decode("ndk://test/realm/doc3/$body")

    items = [
        _LabelItem(
            name=LabelName.decode("description"),
            description="Generate a description.",
            targets=[target1, target2, target3],
        ),
    ]

    # Group contains only target1 and target2.
    group_uris = {target1, target2}

    filtered = _filter_items_for_group(items, group_uris)

    assert len(filtered) == 1
    assert len(filtered[0].targets) == 2
    assert target1 in filtered[0].targets
    assert target2 in filtered[0].targets
    assert target3 not in filtered[0].targets


def test_filter_items_for_group_no_match():
    """Test filtering when no targets match the group."""
    target1 = ObservableUri.decode("ndk://test/realm/doc1/$body")
    target2 = ObservableUri.decode("ndk://test/realm/doc2/$body")

    items = [
        _LabelItem(
            name=LabelName.decode("description"),
            description="Generate a description.",
            targets=[target1],
        ),
    ]

    # Group doesn't contain target1.
    group_uris = {target2}

    filtered = _filter_items_for_group(items, group_uris)

    assert len(filtered) == 0


##
## Unit Tests - Inference Parameters
##


def test_build_inference_params():
    """Test building inference parameters from label items."""
    target1 = ObservableUri.decode("ndk://test/realm/doc/$body")
    target2 = ObservableUri.decode("ndk://test/realm/doc/$chunk/00")

    item = _LabelItem(
        name=LabelName.decode("description"),
        description="Generate a description.",
        targets=[target1, target2],
    )

    system, schema, mapping = _build_inference_params([item])

    # Check system message.
    assert "description" in system.lower()
    assert "knowledge extraction" in system.lower()

    # Check schema has properties.
    assert "properties" in schema
    assert len(schema["properties"]) == 2

    # Check all properties are nullable strings.
    for prop in schema["properties"].values():
        assert prop["type"] == ["string", "null"]

    # Check mapping.
    assert len(mapping) == 2


def test_build_inference_params_multiple_items():
    """Test building inference parameters from multiple label items."""
    target1 = ObservableUri.decode("ndk://test/realm/doc/$body")
    target2 = ObservableUri.decode("ndk://test/realm/doc/$chunk/00")

    items = [
        _LabelItem(
            name=LabelName.decode("description"),
            description="Generate a description.",
            targets=[target1],
        ),
        _LabelItem(
            name=LabelName.decode("keywords"),
            description="Extract keywords.",
            targets=[target1, target2],
        ),
    ]

    _, schema, mapping = _build_inference_params(items)

    # Should have 3 properties: 1 description + 2 keywords.
    assert len(schema["properties"]) == 3
    assert len(mapping) == 3


##
## Unit Tests - Label Item
##


def test_label_item_make_system():
    """Test _LabelItem.make_system generates correct output."""
    target1 = ObservableUri.decode("ndk://test/realm/doc/$body")
    target2 = ObservableUri.decode("ndk://test/realm/doc/$chunk/00")

    item = _LabelItem(
        name=LabelName.decode("description"),
        description="Generate a description.",
        targets=[target1, target2],
    )

    system_part, properties = item.make_system()

    # System part should contain the label name and description.
    assert "description" in system_part
    assert "Generate a description" in system_part
    assert str(target1) in system_part

    # Should have properties for each target.
    assert len(properties) == 2


##
## Unit Tests - Prompt Rendering
##


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


def test_render_prompt_includes_observations():
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
## Integration Tests - Inference
##


@pytest.mark.asyncio
async def test_run_inference_with_stub():
    """Test the inference flow with stubbed inference service."""
    # Create stub responses mapping property names to values.
    context = given_context(
        stub_inference={
            "description_test_realm_doc_body": ["Generated description for body"],
            "description_test_realm_doc_chunk_00": ["Generated description for chunk"],
        },
        stub_storage={},
    )

    # Create sample observations.
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

    # Create label items targeting these observations.
    items = [
        _LabelItem(
            name=LabelName.decode("description"),
            description="Generate a description.",
            targets=[obs_body.uri, obs_chunk.uri],
        ),
    ]

    inferred = await _run_inference(context, [obs_body, obs_chunk], items)

    # Verify inferred labels.
    assert len(inferred) == 2

    body_label = next((f for f in inferred if f.target == obs_body.uri), None)
    assert body_label is not None
    assert body_label.name == LabelName.decode("description")
    assert body_label.value == "Generated description for body"

    chunk_label = next((f for f in inferred if f.target == obs_chunk.uri), None)
    assert chunk_label is not None
    assert chunk_label.name == LabelName.decode("description")
    assert chunk_label.value == "Generated description for chunk"


@pytest.mark.asyncio
async def test_run_inference_with_media():
    """Test inference including media observations."""
    context = given_context(
        stub_inference={
            "description_stub_outputpng_media": ["Generated media description"],
            "placeholder_stub_outputpng_media": ["Generated media placeholder"],
        },
        stub_storage={},
    )

    # Use the sample media.
    obs_media = given_sample_media()

    # Create label items for the media.
    items = [
        _LabelItem(
            name=LabelName.decode("description"),
            description="Generate a description.",
            targets=[obs_media.uri],
        ),
        _LabelItem(
            name=LabelName.decode("placeholder"),
            description="Generate a placeholder.",
            targets=[obs_media.uri],
        ),
    ]

    inferred = await _run_inference(context, [obs_media], items)

    # Verify inferred labels.
    assert len(inferred) == 2

    description_label = next(
        (f for f in inferred if f.name == LabelName.decode("description")), None
    )
    assert description_label is not None
    assert description_label.value == "Generated media description"

    placeholder_label = next(
        (f for f in inferred if f.name == LabelName.decode("placeholder")), None
    )
    assert placeholder_label is not None
    assert placeholder_label.value == "Generated media placeholder"


@pytest.mark.asyncio
async def test_run_inference_empty_inputs():
    """Test that empty inputs return empty results."""
    context = given_context(
        stub_inference={},
        stub_storage={},
    )

    # Empty observations.
    inferred = await _run_inference(context, [], [])
    assert inferred == []

    # Empty items.
    obs = ObsChunk.stub(
        uri="ndk://test/realm/doc/$chunk/00",
        mode="plain",
        text="Test content.",
    )
    inferred = await _run_inference(context, [obs], [])
    assert inferred == []


##
## Integration Tests - generate_standard_labels
##


@pytest.mark.asyncio
async def test_generate_standard_labels_with_bundle():
    """Test generate_standard_labels using a chunked bundle."""
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
        suffix = LabelName.try_normalize(uri_str)
        if suffix:
            stub_inference[f"description_{suffix}"] = [f"Description for {obs.uri}"]
            if isinstance(obs, ObsMedia):
                stub_inference[f"placeholder_{suffix}"] = [f"Placeholder for {obs.uri}"]

    context = given_context(
        stub_inference=stub_inference,
        stub_storage={},
    )

    # Generate labels.
    labels = await generate_standard_labels(
        context=context,
        cached=[],
        bundle=bundle,
    )

    # Verify that labels were generated for the body and chunks.
    assert len(labels) > 0

    # Check that description labels were generated.
    description_labels = [
        f for f in labels if f.name == LabelName.decode("description")
    ]
    assert len(description_labels) > 0

    # Check that each label has a valid value.
    for label in labels:
        assert label.value
        assert "Description for" in label.value or "Placeholder for" in label.value


@pytest.mark.asyncio
async def test_generate_standard_labels_caching():
    """Test that cached labels are not regenerated."""
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
        ResourceLabel(
            name=LabelName.decode("description"),
            target=AffBody.new(),
            value="Cached body description",
        ),
    ]

    labels = await generate_standard_labels(
        context=context,
        cached=cached,
        bundle=bundle,
    )

    # Only chunk labels should be generated, not the body label.
    body_labels = [
        f
        for f in labels
        if f.target == AffBody.new() and f.name == LabelName.decode("description")
    ]
    assert len(body_labels) == 0

    chunk_labels = [
        f
        for f in labels
        if f.name == LabelName.decode("description")
        and "chunk" in str(f.target).lower()
    ]
    # Both chunks should have generated descriptions.
    assert len(chunk_labels) >= 1


##
## Integration Tests - generate_labels
##


@pytest.mark.asyncio
async def test_generate_labels_empty_inputs():
    """Test generate_labels with empty inputs."""
    context = given_context(stub_inference={}, stub_storage={})

    # Empty bundles.
    result = await generate_labels(
        context=context,
        cached=[],
        bundles=[],
        definitions=[
            LabelDefinition(
                info=LabelInfo(
                    name=LabelName.decode("summary"),
                    forall=["body"],
                    prompt="Test",
                )
            )
        ],
    )
    assert result == []

    # Empty definitions.
    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    bundle = BundleBody.make_single(
        resource_uri=resource_uri,
        text=ContentText.new_plain("Test content."),
    )
    result = await generate_labels(
        context=context,
        cached=[],
        bundles=[bundle],
        definitions=[],
    )
    assert result == []


@pytest.mark.asyncio
async def test_generate_labels_single_bundle():
    """Test generate_labels with a single bundle."""
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

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("description"),
                forall=["body"],
                prompt="Generate a description.",
            ),
        ),
    ]

    result = await generate_labels(
        context=context,
        cached=[],
        bundles=[bundle],
        definitions=definitions,
    )

    assert len(result) == 1
    assert result[0].name == LabelName.decode("description")
    assert result[0].value == "Generated body description"


@pytest.mark.asyncio
async def test_generate_labels_multiple_bundles():
    """Test generate_labels with multiple bundles."""
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

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("description"),
                forall=["body"],
                prompt="Generate a description.",
            ),
        ),
    ]

    result = await generate_labels(
        context=context,
        cached=[],
        bundles=[bundle_1, bundle_2],
        definitions=definitions,
    )

    assert len(result) == 2

    doc1_label = next((f for f in result if "doc1" in str(f.target)), None)
    assert doc1_label is not None
    assert doc1_label.value == "Description for doc1"

    doc2_label = next((f for f in result if "doc2" in str(f.target)), None)
    assert doc2_label is not None
    assert doc2_label.value == "Description for doc2"


@pytest.mark.asyncio
async def test_generate_labels_with_cached():
    """Test generate_labels respects cached values."""
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
        LabelValue(
            name=LabelName.decode("description"),
            target=body_uri_1,
            value="Cached description for doc1",
        ),
    ]

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("description"),
                forall=["body"],
                prompt="Generate a description.",
            ),
        ),
    ]

    result = await generate_labels(
        context=context,
        cached=cached,
        bundles=[bundle_1, bundle_2],
        definitions=definitions,
    )

    # Only doc2 should have a generated label.
    assert len(result) == 1
    assert "doc2" in str(result[0].target)
    assert result[0].value == "Description for doc2"


@pytest.mark.asyncio
async def test_generate_labels_with_resource_filter():
    """Test generate_labels with resource filter."""
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

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("description"),
                forall=["body"],
                prompt="Generate a description.",
            ),
            filters=ResourceFilters(
                default="block",
                allowlist=[AllowRule(action="allow", prefix="ndk://test/realm1/")],
            ),
        ),
    ]

    result = await generate_labels(
        context=context,
        cached=[],
        bundles=[bundle_1, bundle_2],
        definitions=definitions,
    )

    # Only realm1 should have a generated label.
    assert len(result) == 1
    assert "realm1" in str(result[0].target)


@pytest.mark.asyncio
async def test_generate_labels_multiple_definitions():
    """Test generate_labels with multiple label definitions."""
    context = given_context(
        stub_inference={
            "description_test_realm_doc_body": ["Body description"],
            "description_test_realm_doc_chunk_00": ["Chunk 0 description"],
            "keywords_test_realm_doc_body": ["keyword1, keyword2"],
        },
        stub_storage={},
    )

    resource_uri = ResourceUri.decode("ndk://test/realm/doc")
    chunk_uri = resource_uri.child_observable(AffBodyChunk.new([0]))

    obs_chunk = ObsChunk(
        uri=chunk_uri,
        description=None,
        text=ContentText.new_plain("Chunk content."),
    )

    bundle = BundleBody(
        uri=resource_uri.child_affordance(AffBody.new()),
        description=None,
        sections=[ObsBodySection(indexes=[0], heading="Section 0")],
        chunks=[obs_chunk],
        media=[],
    )

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("description"),
                forall=["body", "chunk"],
                prompt="Generate a description.",
            ),
        ),
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("keywords"),
                forall=["body"],  # Only body
                prompt="Extract keywords.",
            ),
        ),
    ]

    result = await generate_labels(
        context=context,
        cached=[],
        bundles=[bundle],
        definitions=definitions,
    )

    # Should have 3 labels: 2 descriptions (body + chunk) + 1 keywords (body only).
    assert len(result) == 3

    description_labels = [
        f for f in result if f.name == LabelName.decode("description")
    ]
    assert len(description_labels) == 2

    keyword_labels = [f for f in result if f.name == LabelName.decode("keywords")]
    assert len(keyword_labels) == 1
    assert "body" in str(keyword_labels[0].target).lower()


@pytest.mark.asyncio
async def test_generate_labels_with_media():
    """Test generate_labels with media observations."""
    obs_media = given_sample_media()
    resource_uri = obs_media.uri.resource_uri()
    media_uri = resource_uri.child_observable(AffBodyMedia.new())

    # Build the property key that the stub inference expects.
    uri_str = str(media_uri).removeprefix("ndk://")
    suffix = LabelName.try_normalize(uri_str)

    context = given_context(
        stub_inference={
            f"placeholder_{suffix}": ["Generated placeholder for image"],
        },
        stub_storage={},
    )

    # Create a bundle with multiple chunks so media is not inlined.
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

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("placeholder"),
                forall=["media"],
                prompt="Generate a placeholder for the media.",
            ),
        ),
    ]

    result = await generate_labels(
        context=context,
        cached=[],
        bundles=[bundle],
        definitions=definitions,
    )

    # Should generate placeholder for media.
    placeholder_labels = [
        f for f in result if f.name == LabelName.decode("placeholder")
    ]
    assert len(placeholder_labels) == 1
    assert "placeholder" in placeholder_labels[0].value.lower()


@pytest.mark.asyncio
async def test_generate_labels_returns_label_values():
    """Test that generate_labels returns LabelValue instances."""
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

    definitions = [
        LabelDefinition(
            info=LabelInfo(
                name=LabelName.decode("description"),
                forall=["body"],
                prompt="Generate a description.",
            ),
        ),
    ]

    result = await generate_labels(
        context=context,
        cached=[],
        bundles=[bundle],
        definitions=definitions,
    )

    assert len(result) == 1
    assert isinstance(result[0], LabelValue)
    assert isinstance(result[0].name, LabelName)
    assert result[0].target is not None
    assert isinstance(result[0].value, str)
