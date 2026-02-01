import json
import logging

from dataclasses import dataclass
from functools import cache
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue
from typing import Literal

from base.core.values import parse_yaml_as
from base.models.content import ContentBlob, ContentText, PartLink
from base.models.rendered import Rendered
from base.resources.aff_body import (
    AnyBodyObservableUri,
    AnyObservationBody,
    ObsBody,
    ObsChunk,
    ObsMedia,
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
    """
    if not definitions or not bundles:
        return []

    # Collect all observations from all bundles.
    all_observations: list[AnyObservationBody] = []
    for bundle in bundles:
        all_observations.extend(bundle.observations())  # type: ignore

    if not all_observations:
        return []

    # Build cached set for quick lookup.
    cached_set: set[tuple[LabelName, AnyBodyObservableUri]] = {
        (value.name, value.target)
        for value in cached  # type: ignore
    }

    # Convert definitions to inference items.
    label_items = _explode_definitions(cached_set, all_observations, definitions)
    if not label_items:
        return []

    # Run inference.
    inferred = await _run_inference(context, all_observations, label_items)

    # Convert to LabelValue.
    return [
        LabelValue(
            name=item.name,
            target=item.target,  # type: ignore
            value=item.value,
        )
        for item in inferred
    ]


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

    name: LabelName
    description: str
    targets: list[AnyBodyObservableUri]

    def make_system(self) -> tuple[str, list[tuple[str, str]]]:
        """
        Build system message part and property definitions for this label.

        Returns:
            - system_message_part: Instructions for generating this label
            - properties: List of (property_name, property_description) tuples
        """
        # Build mapping from target URI to property name.
        observations_mapping: list[tuple[AnyBodyObservableUri, str, str]] = [
            (
                target,
                self.name.as_property(target),
                f"The {self.name} label for {target}",
            )
            for target in self.targets
        ]

        observations_mapping_part = "\n".join(
            f"- {uri} -> {prop_name}" for uri, prop_name, _ in observations_mapping
        )

        system_message_part = f"""\
## {self.name}

{self.description}

Generate for each observation and place in the corresponding property:
{observations_mapping_part}
"""

        properties = [
            (prop_name, prop_desc) for _, prop_name, prop_desc in observations_mapping
        ]

        return system_message_part, properties


def _explode_definitions(
    cached: set[tuple[LabelName, AnyBodyObservableUri]],
    observations: list[AnyObservationBody],
    definitions: list[LabelDefinition],
) -> list[_LabelItem]:
    """
    Convert label definitions to inference items.

    For each definition, finds all matching observations (based on type and filters)
    that are not already cached, and groups them into a _LabelItem.
    """
    items: dict[LabelName, _LabelItem] = {}

    for definition in definitions:
        info = definition.info

        # Skip definitions without target types.
        if not info.forall:
            continue

        for obs in observations:
            # Skip if already cached.
            if (info.name, obs.uri) in cached:
                continue

            # Check resource filter.
            if not definition.filters.matches(obs.uri.resource_uri()):
                continue

            # Check observation type filter.
            if not _matches_observation_type(obs, info.forall):
                continue

            # Add to or create item.
            if not (item := items.get(info.name)):
                item = _LabelItem(
                    name=info.name,
                    description=info.prompt,
                    targets=[],
                )
                items[info.name] = item

            bisect_insert(item.targets, obs.uri, key=str)

    return sorted(items.values(), key=lambda item: item.name)


def _matches_observation_type(
    obs: AnyObservationBody,
    forall: list[Literal["body", "chunk", "media"]],
) -> bool:
    """Check if an observation matches the target types."""
    return (
        (isinstance(obs, ObsBody) and "body" in forall)
        or (isinstance(obs, ObsChunk) and "chunk" in forall)
        or (isinstance(obs, ObsMedia) and "media" in forall)
    )


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
    items: list[_LabelItem],
) -> list[_InferredLabel]:
    """
    Generate label values using the inference service.

    Observations are grouped by token count to fit within context windows.
    For each group, builds a prompt and calls the LLM to generate values.
    """
    inference = context.service(SvcInference)
    groups = _group_observations_by_tokens(observations)
    results: list[_InferredLabel] = []

    for group in groups:
        group_uris = {obs.uri for obs in group}

        # Filter items to include only targets in this group.
        group_items = _filter_items_for_group(items, group_uris)
        if not group_items:
            continue

        # Build inference parameters.
        system_message, response_schema, property_mapping = _build_inference_params(
            group_items
        )

        # Render the prompt with observation content.
        prompt = _render_prompt(group)

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
            filtered.append(
                _LabelItem(
                    name=item.name,
                    description=item.description,
                    targets=targets_in_group,
                )
            )

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
    system_parts = [
        "You are a knowledge extraction assistant. Generate metadata labels for "
        "the provided observations.",
        "",
        "For each property in the response schema, generate an appropriate value "
        "based on the label description and observation content. Return null if "
        "the label cannot be inferred.",
        "",
    ]

    properties: dict[str, JsonSchemaValue] = {}
    property_mapping: dict[str, tuple[LabelName, AnyBodyObservableUri]] = {}

    for item in items:
        system_part, item_properties = item.make_system()
        system_parts.append(system_part)

        # Map each property to its target.
        for target in item.targets:
            property_name = item.name.as_property(target)
            property_mapping[property_name] = (item.name, target)

        # Add properties to schema.
        for property_name, property_description in item_properties:
            properties[property_name] = {
                "type": ["string", "null"],
                "description": property_description,
            }

    response_schema: JsonSchemaValue = {
        "type": "object",
        "properties": properties,
        "required": list(properties.keys()),
        "additionalProperties": False,
    }

    return "\n".join(system_parts), response_schema, property_mapping


def _render_prompt(observations: list[AnyObservationBody]) -> list[str | ContentBlob]:
    """
    Render observations into a prompt for the LLM.

    Creates embed links for each observation and renders them with content.
    """
    parts = [PartLink.new("embed", obs.description, obs.uri) for obs in observations]

    intro_text = (
        "Generate label values for the following observations. "
        "Analyze each observation carefully and provide appropriate values "
        "for the requested labels.\n\n"
    )

    prompt = ContentText.new(
        [
            ContentText.new_plain(intro_text).parts[0],
            *parts,
        ]
    )

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
