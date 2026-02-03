import pytest

from base.models.content import ContentText, PartLink
from base.resources.aff_body import (
    AffBody,
    AffBodyChunk,
    AffBodyMedia,
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
    _parse_labels_response,
    generate_labels,
    generate_standard_labels,
)
from knowledge.models.storage_observed import BundleBody

from tests.data.samples import given_sample_media, read_2303_11366v2
from tests.knowledge.utils_connectors import given_context


##
## Unit Tests - Response Parsing
##


def test_parse_response_valid_json():
    """Test parsing a valid JSON response."""
    target_uri = ObservableUri.decode("ndk://test/resource/doc/$body")
    property_mapping = {
        "description_test_resource_doc_body": (
            target_uri,
            LabelName.decode("description"),
        ),
    }

    response_json = '{"description_test_resource_doc_body": "A test description"}'
    inferred = _parse_labels_response(response_json, property_mapping)

    assert len(inferred) == 1
    assert inferred[0].name == LabelName.decode("description")
    assert inferred[0].target == target_uri
    assert inferred[0].value == "A test description"


def test_parse_response_null_value():
    """Test that null values are skipped."""
    target_uri = ObservableUri.decode("ndk://test/resource/doc/$body")
    property_mapping = {
        "description_test_resource_doc_body": (
            target_uri,
            LabelName.decode("description"),
        ),
    }

    response_json = '{"description_test_resource_doc_body": null}'
    inferred = _parse_labels_response(response_json, property_mapping)

    assert len(inferred) == 0


def test_parse_response_invalid_json():
    """Test that invalid JSON returns empty list."""
    property_mapping: dict = {}
    inferred = _parse_labels_response("not valid json", property_mapping)
    assert len(inferred) == 0


def test_parse_response_multiple_properties():
    """Test parsing response with multiple properties."""
    target_uri_body = ObservableUri.decode("ndk://test/resource/doc/$body")
    target_uri_chunk = ObservableUri.decode("ndk://test/resource/doc/$chunk/00")
    property_mapping = {
        "description_test_resource_doc_body": (
            target_uri_body,
            LabelName.decode("description"),
        ),
        "description_test_resource_doc_chunk_00": (
            target_uri_chunk,
            LabelName.decode("description"),
        ),
    }

    response_json = """{
        "description_test_resource_doc_body": "Body description",
        "description_test_resource_doc_chunk_00": "Chunk description"
    }"""
    inferred = _parse_labels_response(response_json, property_mapping)

    assert len(inferred) == 2

    body_label = next((f for f in inferred if f.target == target_uri_body), None)
    assert body_label is not None
    assert body_label.value == "Body description"

    chunk_label = next((f for f in inferred if f.target == target_uri_chunk), None)
    assert chunk_label is not None
    assert chunk_label.value == "Chunk description"


def test_parse_response_unknown_property():
    """Test that unknown properties are ignored."""
    target_uri = ObservableUri.decode("ndk://test/resource/doc/$body")
    property_mapping = {
        "description_test_resource_doc_body": (
            target_uri,
            LabelName.decode("description"),
        ),
    }

    response_json = """{
        "description_test_resource_doc_body": "Valid description",
        "unknown_property": "Should be ignored"
    }"""
    inferred = _parse_labels_response(response_json, property_mapping)

    assert len(inferred) == 1
    assert inferred[0].value == "Valid description"


def test_parse_response_non_dict():
    """Test that non-dict responses return empty list."""
    property_mapping: dict = {}
    inferred = _parse_labels_response('["not", "a", "dict"]', property_mapping)
    assert len(inferred) == 0


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
    """Test generate_labels with multiple label definitions.

    NOTE: Labels are generated for ALL observation URIs (body, chunks, media).
    For chunked documents, we only EMBED chunks (not body) in the prompt to avoid
    duplication, but we still generate labels for body, chunks, and media.
    """
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
