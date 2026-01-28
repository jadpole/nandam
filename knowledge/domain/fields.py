import json
import logging

from dataclasses import dataclass
from functools import cache
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue
from typing import Literal

from base.api.knowledge import QueryField
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
from base.resources.metadata import FieldName, FieldValue, ResourceField
from base.strings.data import MimeType
from base.utils.sorted_list import bisect_insert

from knowledge.config import KnowledgeConfig
from knowledge.models.storage_observed import BundleBody
from knowledge.server.context import KnowledgeContext
from knowledge.services.inference import SvcInference

logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class GenerateFieldsItem:
    name: FieldName
    description: str
    targets: list[AnyBodyObservableUri] | None
    result_target: AnyBodyObservableUri | None = None
    """
    When targets is None but result_target is set, this is a "global" field that
    uses all observations for context but associates the result with result_target.
    """

    def make_system(self) -> tuple[str | None, list[tuple[str, str]]]:
        """
        The return value has the form:
        ```
        (system_message_part, [(property_name, property_description), ...])
        ```
        """
        if not self.targets:
            return (None, [(str(self.name), self.description)])

        # (observable_uri, property_name, property_description)
        observations_mapping: list[tuple[AnyBodyObservableUri, str, str]] = [
            (
                target,
                f"{self.name}_{property_suffix}",
                f"The {self.name} field generated for {target}",
            )
            for target in self.targets
            if (
                property_suffix := FieldName.try_normalize(
                    str(target).removeprefix("ndk://")
                )
            )
        ]
        observations_mapping_part = "\n".join(
            f"- {observation_uri} -> {property_name}"
            for observation_uri, property_name, _ in observations_mapping
        )
        system_message_part = f"""\
## {self.name}

{self.description}

Should be generated for each of the following observations, and put in the \
property of the corresponding observation:
{observations_mapping_part}
"""

        properties: list[tuple[str, str]] = [
            (property_name, property_description)
            for _, property_name, property_description in observations_mapping
        ]
        return system_message_part, properties


##
## Generate - Standard
##


async def generate_standard_fields(
    context: KnowledgeContext,
    cached: list[ResourceField],
    bundle: BundleBody,
) -> list[ResourceField]:
    resource_uri = bundle.uri.resource_uri()
    observations: list[AnyObservationBody] = bundle.observations()  # type: ignore
    field_items = _explode_standard_fields(
        cached=[
            (f.name, resource_uri.child_observable(f.target))  # type: ignore
            for f in cached
        ],
        observations=observations,
    )
    inferred = await _generate_fields(context, observations, field_items)
    return [
        ResourceField(
            name=inferred.name,
            target=inferred.target.suffix,
            value=inferred.value,
        )
        for inferred in inferred
        if inferred.target.resource_uri() == resource_uri
    ]


def _explode_standard_fields(
    cached: list[tuple[FieldName, AnyBodyObservableUri]],
    observations: list[AnyObservationBody],
) -> list[GenerateFieldsItem]:
    fields: dict[FieldName, GenerateFieldsItem] = {}
    for field_config in fields_config().fields:
        if not field_config.forall:
            continue

        for obs in observations:
            if (field_config.name, obs.uri) in cached:
                continue  # No need to re-generate known fields.
            if field_config.prefixes is not None and not any(
                str(obs.uri).startswith(prefix) for prefix in field_config.prefixes
            ):
                continue  # The resource is not targeted.
            if (
                not (isinstance(obs, ObsBody) and "body" in field_config.forall)
                and not (isinstance(obs, ObsChunk) and "chunk" in field_config.forall)
                and not (isinstance(obs, ObsMedia) and "media" in field_config.forall)
            ):
                continue  # The observable is not targeted.

            if not (field_item := fields.get(field_config.name)):
                field_item = GenerateFieldsItem(
                    name=field_config.name,
                    description=field_config.description,
                    targets=[],
                )
                fields[field_config.name] = field_item

            assert field_item.targets is not None
            bisect_insert(field_item.targets, obs.uri, key=str)

    return sorted(fields.values(), key=lambda item: item.name)


##
## Generate - API
##


async def generate_api_fields(
    context: KnowledgeContext,
    cached: list[FieldValue],
    bundles: list[BundleBody],
    fields: list[QueryField],
) -> list[FieldValue]:
    """
    Generate custom field values for multiple resources.

    When `QueryField.forall` is set, generates one value per matching observation.
    When `QueryField.forall` is None, generates a single field using all matching
    targets (from `prefixes` or `targets`), associated with a representative target.
    """
    if not fields or not bundles:
        return []

    # Collect all observations from all bundles.
    all_observations: list[AnyObservationBody] = []
    for bundle in bundles:
        all_observations.extend(bundle.observations())  # type: ignore

    if not all_observations:
        return []

    # Build cached set for quick lookup.
    cached_set: set[tuple[FieldName, AnyBodyObservableUri]] = {
        (f.name, f.target)  # type: ignore
        for f in cached
    }

    # Explode fields into GenerateFieldsItem.
    field_items = _explode_api_fields(
        cached=cached_set,
        observations=all_observations,
        fields=fields,
    )

    # Call the common generation function.
    inferred = await _generate_fields(context, all_observations, field_items)

    # Convert InferredField to FieldValue.
    return [
        FieldValue(
            name=inferred_field.name,
            target=inferred_field.target,  # type: ignore
            value=inferred_field.value,
        )
        for inferred_field in inferred
    ]


def _explode_api_fields(
    cached: set[tuple[FieldName, AnyBodyObservableUri]],
    observations: list[AnyObservationBody],
    fields: list[QueryField],
) -> list[GenerateFieldsItem]:
    """
    Convert QueryField specifications to GenerateFieldsItem for inference.

    When `forall` is set, creates one target per matching observation.
    When `forall` is None, creates a single field with targets=None and
    result_target set to a representative observation.
    """
    result: dict[FieldName, GenerateFieldsItem] = {}

    for field in fields:
        if field.forall:
            _explode_api_field_forall(cached, observations, field, result)
        else:
            _explode_api_field_single(cached, observations, field, result)

    return sorted(result.values(), key=lambda item: item.name)


def _explode_api_field_forall(
    cached: set[tuple[FieldName, AnyBodyObservableUri]],
    observations: list[AnyObservationBody],
    field: QueryField,
    result: dict[FieldName, GenerateFieldsItem],
) -> None:
    """Generate one value per observation matching the criteria."""
    assert field.forall is not None

    for obs in observations:
        if (field.name, obs.uri) in cached:
            continue  # Already cached.

        if not _matches_api_field_filters(obs, field):
            continue

        # Check observation type.
        if (
            not (isinstance(obs, ObsBody) and "body" in field.forall)
            and not (isinstance(obs, ObsChunk) and "chunk" in field.forall)
            and not (isinstance(obs, ObsMedia) and "media" in field.forall)
        ):
            continue

        if not (field_item := result.get(field.name)):
            field_item = GenerateFieldsItem(
                name=field.name,
                description=field.description,
                targets=[],
            )
            result[field.name] = field_item

        assert field_item.targets is not None
        bisect_insert(field_item.targets, obs.uri, key=str)


def _explode_api_field_single(
    cached: set[tuple[FieldName, AnyBodyObservableUri]],
    observations: list[AnyObservationBody],
    field: QueryField,
    result: dict[FieldName, GenerateFieldsItem],
) -> None:
    """Generate a single field using all matching observations."""
    matching_obs = [
        obs for obs in observations if _matches_api_field_filters(obs, field)
    ]

    if not matching_obs:
        return

    # Use the first matching observation as the representative target.
    representative_target = matching_obs[0].uri

    # Check if already cached.
    if (field.name, representative_target) in cached:
        return

    # Create a global field item with result_target set.
    result[field.name] = GenerateFieldsItem(
        name=field.name,
        description=field.description,
        targets=None,
        result_target=representative_target,
    )


def _matches_api_field_filters(obs: AnyObservationBody, field: QueryField) -> bool:
    """Check if an observation matches the field's prefix and target filters."""
    # Check prefixes filter.
    if field.prefixes is not None and not any(
        str(obs.uri).startswith(prefix) for prefix in field.prefixes
    ):
        return False

    # Check targets filter.
    return not (
        field.targets is not None
        and str(obs.uri) not in [str(t) for t in field.targets]
    )


##
## Inference
##


THRESHOLD_SPLIT_GROUP = 80_000
"""
We group observations into groups of at most this many tokens, based on the
`num_tokens` method defined on `ObsBody`, `ObsChunk` and `ObsMedia`.
"""

SUPPORTED_IMAGE_TYPES = [
    MimeType.decode("image/png"),
    MimeType.decode("image/jpeg"),
    MimeType.decode("image/webp"),
    MimeType.decode("image/heic"),
    MimeType.decode("image/heif"),
]


@dataclass(kw_only=True)
class InferredField:
    name: FieldName
    target: AnyBodyObservableUri
    value: str


async def _generate_fields(
    context: KnowledgeContext,
    observations: list[AnyObservationBody],
    fields: list[GenerateFieldsItem],
) -> list[InferredField]:
    """
    Generate field values for observations using the inference service.

    Groups observations by token count to fit within context windows, then calls
    the LLM to generate field values for each group.
    """
    if not fields or not observations:
        return []

    # Group observations by token count.
    groups = _group_observations_by_tokens(observations)

    # Get the inference service.
    inference = context.service(SvcInference)

    # Process each group.
    all_inferred: list[InferredField] = []
    for group in groups:
        group_uris = {obs.uri for obs in group}

        # Filter fields to only include targets in this group.
        group_fields: list[GenerateFieldsItem] = []
        for field in fields:
            if field.targets is None:
                # Field applies to all observations.
                group_fields.append(field)
            else:
                # Filter targets to those in this group.
                filtered_targets = [t for t in field.targets if t in group_uris]
                if filtered_targets:
                    group_fields.append(
                        GenerateFieldsItem(
                            name=field.name,
                            description=field.description,
                            targets=filtered_targets,
                        )
                    )

        if not group_fields:
            continue

        # Build system message and response schema.
        system_message, response_schema, property_mapping = _build_inference_params(
            group_fields
        )

        # Build and render prompt with observations.
        prompt = _render_prompt(group)

        # Call inference service.
        try:
            response_json = await inference.completion_json(
                system=system_message,
                response_schema=response_schema,
                prompt=prompt,
            )

            # Parse response and extract inferred fields.
            inferred = _parse_response(response_json, property_mapping)
            all_inferred.extend(inferred)
        except Exception:
            logger.exception("Failed to generate fields for observation group")

    return all_inferred


def _group_observations_by_tokens(
    observations: list[AnyObservationBody],
) -> list[list[AnyObservationBody]]:
    """
    Group observations into batches that fit within the token threshold.
    """
    groups: list[list[AnyObservationBody]] = []
    current_group: list[AnyObservationBody] = []
    current_tokens: int = 0

    for obs in observations:
        obs_tokens = obs.num_tokens() or 0
        if current_tokens + obs_tokens > THRESHOLD_SPLIT_GROUP and current_group:
            groups.append(current_group)
            current_group = []
            current_tokens = 0
        current_group.append(obs)
        current_tokens += obs_tokens

    if current_group:
        groups.append(current_group)

    return groups


def _build_inference_params(
    fields: list[GenerateFieldsItem],
) -> tuple[str, JsonSchemaValue, dict[str, tuple[FieldName, AnyBodyObservableUri]]]:
    """
    Build system message, response schema, and property mapping from field items.

    Returns:
        - system_message: Instructions for the LLM
        - response_schema: JSON schema for structured output
        - property_mapping: Maps property names to (field_name, target_uri) tuples
    """
    system_parts: list[str] = [
        "You are a knowledge extraction assistant. Your task is to generate "
        "metadata fields for the provided observations.",
        "",
        "For each property in the response schema, generate an appropriate value "
        "based on the field description and the content of the corresponding "
        "observation. Return null if the field cannot be inferred.",
        "",
    ]

    properties: dict[str, JsonSchemaValue] = {}
    property_mapping: dict[str, tuple[FieldName, AnyBodyObservableUri]] = {}

    for field in fields:
        system_part, field_properties = field.make_system()
        if system_part:
            system_parts.append(system_part)

        # Build property to target mapping.
        # When targets is set, each target corresponds to a generated property name.
        # When targets is None but result_target is set, the property uses result_target.
        target_by_property: dict[str, AnyBodyObservableUri] = {}
        if field.targets:
            for target in field.targets:
                if property_suffix := FieldName.try_normalize(
                    str(target).removeprefix("ndk://")
                ):
                    property_name = f"{field.name}_{property_suffix}"
                    target_by_property[property_name] = target
        elif field.result_target:
            # Global field: use result_target for the property mapping.
            target_by_property[str(field.name)] = field.result_target

        for property_name, property_description in field_properties:
            # Create nullable string property.
            properties[property_name] = {
                "type": ["string", "null"],
                "description": property_description,
            }

            # Map property name to field and target.
            if property_name in target_by_property:
                property_mapping[property_name] = (
                    field.name,
                    target_by_property[property_name],
                )

    response_schema: JsonSchemaValue = {
        "type": "object",
        "properties": properties,
        "required": list(properties.keys()),
        "additionalProperties": False,
    }

    return "\n".join(system_parts), response_schema, property_mapping


def _render_prompt(observations: list[AnyObservationBody]) -> list[str | ContentBlob]:
    """
    Build and render a prompt with observations for the LLM to analyze.

    Returns a list of strings and blobs ready for the inference service.
    """
    # Create embed links for each observation.
    parts = [PartLink.new("embed", obs.description, obs.uri) for obs in observations]

    intro_text = (
        "Generate field values for the following observations. "
        "Analyze each observation carefully and provide appropriate values "
        "for the requested fields.\n\n"
    )

    prompt = ContentText.new(
        [
            ContentText.new_plain(intro_text).parts[0],
            *parts,
        ]
    )

    # Render the prompt with observations resolved.
    rendered = Rendered.render(prompt, observations)  # type: ignore
    return rendered.as_llm_inline(
        supports_media=SUPPORTED_IMAGE_TYPES,
        limit_media=20,
    )


def _parse_response(
    response_json: str,
    property_mapping: dict[str, tuple[FieldName, AnyBodyObservableUri]],
) -> list[InferredField]:
    """
    Parse the LLM response and extract inferred fields.
    """
    inferred: list[InferredField] = []

    try:
        response = json.loads(response_json)
        if not isinstance(response, dict):
            return []

        for property_name, value in response.items():
            if value is None or property_name not in property_mapping:
                continue

            field_name, target = property_mapping[property_name]
            if isinstance(value, str) and value.strip():
                inferred.append(
                    InferredField(
                        name=field_name,
                        target=target,
                        value=value.strip(),
                    )
                )
    except json.JSONDecodeError:
        logger.warning("Failed to parse LLM response as JSON: %s", response_json[:100])

    return inferred


##
## Configuration
##


class FieldConfig(BaseModel, frozen=True):
    name: FieldName
    forall: list[Literal["body", "chunk", "media"]]
    """
    Generates a key for each observation of a given kind.  (Must be non-empty.)
    """
    prefixes: list[str] | None = None
    """
    Generates a single field, but only using the information from these URIs.
    """
    description: str
    """
    The prompt used by the LLM to update this field.
    """

    def sort_key(self) -> str:
        return f"{self.name}/{','.join(self.forall)}/{','.join(self.prefixes or [])}"


class FieldsConfig(BaseModel, frozen=True):
    fields: list[FieldConfig]


@cache
def fields_config() -> FieldsConfig:
    try:
        config_yaml = KnowledgeConfig.cfg_path("fields.yml").read_text()
        config = parse_yaml_as(FieldsConfig, config_yaml)
        return FieldsConfig(
            fields=sorted(config.fields, key=FieldConfig.sort_key),
        )
    except Exception:
        logger.error("Failed to read config: fields.yml")  # noqa: TRY400
        return FieldsConfig(
            fields=[
                FieldConfig(
                    name=FieldName.decode("description"),
                    forall=["body", "chunk", "media"],
                    prefixes=None,
                    description="""\
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
                FieldConfig(
                    name=FieldName.decode("placeholder"),
                    forall=["media"],
                    prefixes=None,
                    description="""\
Generate a dense, highly detailed placeholder for the $media. The raw data is \
replaced by this placeholder when an AI agent is unable to view it natively. \
It should therefore be a textual drop-in representation that contains ALL of \
the information in the original media.

For example:

- Given an image of a diagram on a whiteboard, the placeholder might be an \
equivalent MermaidJS diagram.\
""",
                ),
            ],
        )
