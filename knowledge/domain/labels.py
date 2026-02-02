import json
import logging

from dataclasses import dataclass
from functools import cache
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue

from base.core.values import parse_yaml_as
from base.models.content import ContentBlob, ContentText, PartLink, PartText, TextPart
from base.models.rendered import Rendered
from base.resources.aff_body import (
    AffBody,
    AnyBodyObservableUri,
    AnyObservationBody,
)
from base.resources.label import (
    LabelDefinition,
    LabelInfo,
    LabelName,
    LabelValue,
    ResourceLabel,
)
from base.strings.data import MimeType
from base.utils.sorted_list import bisect_insert

from knowledge.config import KnowledgeConfig
from knowledge.models.storage_observed import BundleBody
from knowledge.server.context import KnowledgeContext
from knowledge.services.inference import SvcInference

logger = logging.getLogger(__name__)


##
## Public API
##


async def generate_standard_labels(
    context: KnowledgeContext,
    cached: list[ResourceLabel],
    bundle: BundleBody,
) -> list[ResourceLabel]:
    """
    Generate labels for a single bundle using the standard configuration.

    This is a convenience wrapper around `generate_labels` that:
    - Uses the definitions from `labels_config()`
    - Converts relative `ResourceLabel` cache/output to/from absolute `LabelValue`
    """
    resource_uri = bundle.uri.resource_uri()

    # Convert cached ResourceLabel to LabelValue with absolute URIs.
    cached_values = [
        LabelValue(
            name=label.name,
            target=resource_uri.child_observable(label.target),  # type: ignore
            value=label.value,
        )
        for label in cached
    ]

    # Generate using the standard config.
    values = await generate_labels(
        context=context,
        cached=cached_values,
        bundles=[bundle],
        definitions=labels_config().labels,
    )

    # Convert LabelValue back to ResourceLabel with relative targets.
    return [
        ResourceLabel(
            name=value.name,
            target=value.target.suffix,
            value=value.value,
        )
        for value in values
        if value.target.resource_uri() == resource_uri
    ]


async def generate_labels(
    context: KnowledgeContext,
    cached: list[LabelValue],
    bundles: list[BundleBody],
    definitions: list[LabelDefinition],
) -> list[LabelValue]:
    """
    Generate label values for multiple resources based on label definitions.

    Each definition specifies which observation types to target (body, chunk, media)
    and the prompt used to generate the label value. One value is generated per
    matching observation.

    NOTE: We distinguish between:
    - Observations: what we generate labels for (body, chunks, media)
    - Embed targets: what we embed in the prompt (body OR chunks, not both)

    This avoids duplicating content in the prompt while still generating labels
    for all observation types.
    """
    if not definitions or not bundles:
        return []

    # Collect observations and embed targets from each bundle.
    all_observations: list[AnyObservationBody] = []
    all_embed_targets: list[AnyBodyObservableUri] = []

    for bundle in bundles:
        all_observations.extend(bundle.observations())  # type: ignore
        all_embed_targets.extend(_bundle_embed_targets(bundle))

    if not all_observations:
        return []

    # Get all observation URIs for label generation.
    all_observation_uris: list[AnyBodyObservableUri] = [
        obs.uri
        for obs in all_observations  # type: ignore
    ]

    # Build cached set for quick lookup.
    cached_set: set[tuple[LabelName, AnyBodyObservableUri]] = {
        (value.name, value.target)
        for value in cached  # type: ignore
    }

    # Convert definitions to inference items using ALL observation URIs.
    label_items = _explode_definitions(cached_set, all_observation_uris, definitions)
    if not label_items:
        return []

    # Run inference with embed targets for rendering.
    inferred = await _run_inference(
        context, all_observations, all_embed_targets, label_items
    )

    # Convert to LabelValue.
    return [
        LabelValue(
            name=item.name,
            target=item.target,  # type: ignore
            value=item.value,
        )
        for item in inferred
    ]


def _bundle_embed_targets(bundle: BundleBody) -> list[AnyBodyObservableUri]:
    """
    Determine what to embed in the prompt for a bundle.

    This avoids content duplication:
    - Single chunk, no sections: embed $body (content is inlined)
    - Multiple chunks or sections: embed $chunk URIs (not body, which would expand to chunks)

    NOTE: Media is NOT included here - chunks already embed their media.
    """
    resource_uri = bundle.uri.resource_uri()

    if not bundle.sections and len(bundle.chunks) == 1:
        # Single chunk body - embed the body itself.
        return [resource_uri.child_observable(AffBody.new())]
    else:
        # Multi-chunk body - embed the individual chunks.
        return [chunk.uri for chunk in bundle.chunks]


##
## Definition Expansion
##


@dataclass(kw_only=True)
class _LabelItem:
    """
    Intermediate representation of a label to generate.

    Groups multiple observation targets under a single label name/description,
    allowing the LLM to generate values for all targets in one call.
    """

    info: LabelInfo
    targets: list[AnyBodyObservableUri]


def _explode_definitions(
    cached: set[tuple[LabelName, AnyBodyObservableUri]],
    targets: list[AnyBodyObservableUri],
    definitions: list[LabelDefinition],
) -> list[_LabelItem]:
    """
    Convert label definitions to inference items.

    For each definition, finds all matching targets (based on type and filters)
    that are not already cached, and groups them into a _LabelItem.
    """
    items: dict[LabelName, _LabelItem] = {}

    for definition in definitions:
        info = definition.info

        # Skip definitions without target types.
        if not info.forall:
            continue

        for uri in targets:
            # Skip if already cached.
            if (info.name, uri) in cached:
                continue

            # Check resource filter.
            if not definition.filters.matches(uri.resource_uri()):
                continue

            # Check target type filter (body, chunk, media).
            if not info.matches_forall(uri.suffix):
                continue

            # Add to or create item.
            if not (item := items.get(info.name)):
                item = _LabelItem(info=info, targets=[])
                items[info.name] = item

            bisect_insert(item.targets, uri, key=str)

    return sorted(items.values(), key=lambda item: item.info.name)


##
## Inference
##


THRESHOLD_SPLIT_GROUP = 80_000
"""
Maximum tokens per inference group. Observations are batched to fit within this
limit based on their `num_tokens()` estimates.
"""

SUPPORTED_IMAGE_TYPES = [
    MimeType.decode("image/png"),
    MimeType.decode("image/jpeg"),
    MimeType.decode("image/webp"),
    MimeType.decode("image/heic"),
    MimeType.decode("image/heif"),
]


@dataclass(kw_only=True)
class _InferredLabel:
    """Result of label inference: a name, target observation, and generated value."""

    name: LabelName
    target: AnyBodyObservableUri
    value: str


async def _run_inference(
    context: KnowledgeContext,
    observations: list[AnyObservationBody],
    embed_targets: list[AnyBodyObservableUri],
    items: list[_LabelItem],
) -> list[_InferredLabel]:
    """
    Generate label values using the inference service.

    Args:
        observations: All observations (for embed resolution).
        embed_targets: URIs to embed in the prompt (body OR chunks, not both).
        items: Label items specifying what labels to generate.

    Embed targets are grouped by token count to fit within context windows.
    For each group, builds a prompt embedding the targets and requests labels
    for all matching observation URIs.
    """
    inference = context.service(SvcInference)

    # Build URI -> observation mapping for lookup.
    obs_by_uri: dict[AnyBodyObservableUri, AnyObservationBody] = {
        obs.uri: obs
        for obs in observations  # type: ignore
    }

    # Resolve embed targets to observations for token counting.
    embed_observations: list[AnyObservationBody] = [
        obs_by_uri[uri] for uri in embed_targets if uri in obs_by_uri
    ]

    # Group embed targets by token count.
    groups = _group_observations_by_tokens(embed_observations)
    results: list[_InferredLabel] = []

    for group in groups:
        # Get the resources represented by this group.
        group_resources = {obs.uri.resource_uri() for obs in group}

        # Find all label targets (from items) that belong to these resources.
        group_label_uris: set[AnyBodyObservableUri] = {
            uri
            for item in items
            for uri in item.targets
            if uri.resource_uri() in group_resources
        }

        # Filter items to include only targets in this group.
        group_items = _filter_items_for_group(items, group_label_uris)
        if not group_items:
            continue

        # Build inference parameters for ALL label targets in this group.
        system_message, response_schema, property_mapping = _build_inference_params(
            group_items
        )

        # Render the prompt: embed the group targets (body OR chunks).
        group_embed_uris = [obs.uri for obs in group]
        prompt = _render_prompt(group_embed_uris, observations)  # type: ignore

        # Call LLM and parse response.
        try:
            response_json = await inference.completion_json(
                system=system_message,
                response_schema=response_schema,
                prompt=prompt,
            )
            results.extend(_parse_response(response_json, property_mapping))
        except Exception:
            logger.exception("Failed to generate labels for observation group")

    return results


def _group_observations_by_tokens(
    observations: list[AnyObservationBody],
) -> list[list[AnyObservationBody]]:
    """Group observations into batches that fit within the token threshold."""
    groups: list[list[AnyObservationBody]] = []
    current_group: list[AnyObservationBody] = []
    current_tokens = 0

    for obs in observations:
        obs_tokens = obs.num_tokens() or 0

        # Start new group if adding this observation would exceed threshold.
        if current_tokens + obs_tokens > THRESHOLD_SPLIT_GROUP and current_group:
            groups.append(current_group)
            current_group = []
            current_tokens = 0

        current_group.append(obs)
        current_tokens += obs_tokens

    if current_group:
        groups.append(current_group)

    return groups


def _filter_items_for_group(
    items: list[_LabelItem],
    group_uris: set[AnyBodyObservableUri],
) -> list[_LabelItem]:
    """Filter label items to include only targets present in the group."""
    filtered: list[_LabelItem] = []

    for item in items:
        targets_in_group = [t for t in item.targets if t in group_uris]
        if targets_in_group:
            filtered.append(_LabelItem(info=item.info, targets=targets_in_group))

    return filtered


def _build_inference_params(
    items: list[_LabelItem],
) -> tuple[str, JsonSchemaValue, dict[str, tuple[LabelName, AnyBodyObservableUri]]]:
    """
    Build LLM inference parameters from label items.

    Returns:
        - system_message: Instructions for the LLM
        - response_schema: JSON schema for structured output
        - property_mapping: Maps property names to (label_name, target_uri)
    """
    system_parts: list[str] = [
        "You are a knowledge extraction assistant. Generate metadata labels for "
        "the provided observations.",
        "",
        "For each property in the response schema, generate an appropriate value "
        "based on the label description and observation content. Return null if "
        "the label cannot be inferred or if the previous value is accurate.",
    ]

    properties: dict[str, JsonSchemaValue] = {}
    property_mapping: dict[str, tuple[LabelName, AnyBodyObservableUri]] = {}

    for item in items:
        item_schema = item.info.as_schema()
        mapping: list[tuple[AnyBodyObservableUri, str]] = [
            (target, item.info.name.as_property(target)) for target in item.targets
        ]
        system_part_mapping = "\n".join(
            f"- {uri} -> {prop_name}" for uri, prop_name in mapping
        )
        system_parts.append(
            f"""\
## {item.info.name}

{item.info.prompt}

Mapping from URI to the corresponding property:
{system_part_mapping}\
""",
        )

        # Add properties to schema and mapping.
        for item_uri, item_property in mapping:
            properties[item_property] = {
                **item_schema,
                "description": f"{item.info.name} label for {item_uri}",
            }
            property_mapping[item_property] = (item.info.name, item_uri)

    response_schema: JsonSchemaValue = {
        "type": "object",
        "properties": properties,
        "required": list(properties.keys()),
        "additionalProperties": False,
    }

    return "\n\n".join(system_parts), response_schema, property_mapping


def _render_prompt(
    targets: list[AnyBodyObservableUri],
    observations: list[AnyObservationBody],
) -> list[str | ContentBlob]:
    """
    Render target URIs into a prompt for the LLM.

    Args:
        targets: URIs of observations to generate labels for.
        observations: All observations available for embed resolution.

    NOTE: For chunks ($chunk), wrap in a <document> tag with the parent $body URI
    to provide context. For bodies and media, embed directly.
    """
    parts: list[TextPart] = []
    for uri in targets:
        if uri.suffix.suffix_kind() == "chunk":
            # Wrap chunk in document context with body URI.
            body_uri = uri.resource_uri().child_observable(AffBody.new())
            parts.extend(PartText.xml_open("document", body_uri, []))
            parts.append(PartLink.new("embed", None, uri))
            parts.append(PartText.xml_close("document"))
        else:
            # Body or Media: embed directly.
            parts.append(PartLink.new("embed", None, uri))

    prompt = ContentText.new(parts)
    rendered = Rendered.render(prompt, observations)  # type: ignore
    return rendered.as_llm_inline(
        supports_media=SUPPORTED_IMAGE_TYPES,
        limit_media=20,
    )


def _parse_response(
    response_json: str,
    property_mapping: dict[str, tuple[LabelName, AnyBodyObservableUri]],
) -> list[_InferredLabel]:
    """Parse LLM response JSON and extract inferred labels."""
    results: list[_InferredLabel] = []

    try:
        response = json.loads(response_json)
        if not isinstance(response, dict):
            return []

        for property_name, value in response.items():
            if value is None or property_name not in property_mapping:
                continue

            label_name, target = property_mapping[property_name]
            if isinstance(value, str) and value.strip():
                results.append(
                    _InferredLabel(
                        name=label_name,
                        target=target,
                        value=value.strip(),
                    )
                )
    except json.JSONDecodeError:
        logger.warning("Failed to parse LLM response as JSON: %s", response_json[:100])

    return results


##
## Configuration
##


class LabelsConfig(BaseModel, frozen=True):
    labels: list[LabelDefinition]


@cache
def labels_config() -> LabelsConfig:
    try:
        config_yaml = KnowledgeConfig.cfg_path("labels.yml").read_text()
        config = parse_yaml_as(LabelsConfig, config_yaml)
        return LabelsConfig(
            labels=sorted(config.labels, key=lambda ld: ld.sort_key()),
        )
    except Exception:
        logger.error("Failed to read config: labels.yml")  # noqa: TRY400
        return LabelsConfig(
            labels=[
                LabelDefinition(
                    info=LabelInfo(
                        name=LabelName.decode("description"),
                        forall=["body", "chunk", "media"],
                        prompt="""\
Generate a concise, dense description of the $body, $chunk or $media.

Guidelines: \
- The description should be 2-3 sentences and no more than 50 words.
- The description should be highly dense and concise yet self-contained, i.e., \
easily understood without the Source. Make every word count.
- The description must allow the reader to infer what QUESTIONS they can answer \
using this Source, NOT give answers.

Audience: this description will be used by humans and tools to decide whether \
they should consult this Source to answer a given question. It should thus be \
exhaustive, so they can infer what information the Source contains.

For example:

- Given a Tableau visualization, the description should list its dimensions, \
metrics, and filters. Since it is dynamic, do NOT cite numbers, nor comment on \
visible trends. The description should remain relevant when the data changes, \
but the structure remains the same.\
""",
                    ),
                ),
                LabelDefinition(
                    info=LabelInfo(
                        name=LabelName.decode("placeholder"),
                        forall=["media"],
                        prompt="""\
Generate a dense, highly detailed placeholder for the $media. The raw data is \
replaced by this placeholder when an AI agent is unable to view it natively. \
It should therefore be a textual drop-in representation that contains ALL of \
the information in the original media.

For example:

- Given an image of a diagram on a whiteboard, the placeholder might be an \
equivalent MermaidJS diagram.\
""",
                    ),
                ),
            ],
        )
