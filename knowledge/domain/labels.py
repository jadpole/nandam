import asyncio
import json
import logging

from dataclasses import dataclass
from functools import cache
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue

from base.core.values import as_json, parse_yaml_as
from base.models.content import ContentBlob
from base.models.rendered import Rendered
from base.resources.aff_body import AnyBodyObservableUri
from base.resources.label import (
    LabelDefinition,
    LabelInfo,
    LabelName,
    LabelValue,
    LabelValues,
    ResourceLabel,
)
from base.utils.sorted_list import bisect_extend, bisect_insert, bisect_make

from knowledge.config import KnowledgeConfig
from knowledge.domain.gen import generate_property_schema, render_body_groups
from knowledge.models.storage_observed import BundleBody
from knowledge.server.context import KnowledgeContext
from knowledge.services.inference import SUPPORTED_IMAGE_BLOB_TYPES, SvcInference

logger = logging.getLogger(__name__)

THRESHOLD_SPLIT_GROUP = 80_000
"""
Maximum tokens per inference group. Observations are batched to fit within this
limit based on their `num_tokens()` estimates.
"""


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
    labels = await generate_labels(
        context=context,
        cached=[label.as_absolute(resource_uri) for label in cached],
        bundles=[bundle],
        definitions=labels_config().labels,
    )
    return [
        label.as_relative()
        for label in labels
        if label.target.resource_uri() == resource_uri
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

    cached_wrapped = LabelValues.new(cached)
    rendered_groups, _ = render_body_groups(bundles, THRESHOLD_SPLIT_GROUP)
    partial_requests = [
        _prepare_label_request(cached_wrapped, rendered, definitions)
        for rendered in rendered_groups
    ]

    relevant_labels: list[LabelInfo] = [
        definition.info
        for definition in definitions
        if any(definition.info.name in request.labels for request in partial_requests)
    ]
    label_schemas_tasks = [
        generate_property_schema(context, info.name, info.constraint)
        for info in relevant_labels
    ]
    label_schemas = dict(await asyncio.gather(*label_schemas_tasks))

    requests = [
        _LabelRequest(
            labels={
                label: (info, label_schemas[label], properties)
                for label, (info, properties) in request.labels.items()
            },
            properties=request.properties,
            rendered=request.rendered,
        )
        for request in partial_requests
    ]

    return await _generate_labels(context, cached_wrapped, requests)


##
## Request Construction
##


@dataclass(kw_only=True)
class _LabelRequest:
    labels: dict[LabelName, tuple[LabelInfo, JsonSchemaValue, list[str]]]
    """
    Labels that have at least one target in the group, along with the properties
    generated for this label.
    """
    properties: dict[str, tuple[AnyBodyObservableUri, LabelName]]
    """
    Mapping property name -> (uri, label).
    """
    rendered: Rendered
    """
    Rendered observations in a group.
    """


@dataclass(kw_only=True)
class _LabelRequestPartial:
    labels: dict[LabelName, tuple[LabelInfo, list[str]]]
    properties: dict[str, tuple[AnyBodyObservableUri, LabelName]]
    rendered: Rendered


def _prepare_label_request(
    cached: LabelValues,
    rendered: Rendered,
    definitions: list[LabelDefinition],
) -> _LabelRequestPartial:
    labels: dict[LabelName, tuple[LabelInfo, list[str]]] = {}
    properties: dict[str, tuple[AnyBodyObservableUri, LabelName]] = {}

    for definition in definitions:
        info = definition.info
        if not info.forall:
            continue  # Skip definitions without targets.

        label_properties: list[str] = []
        for uri in rendered.embeds:
            resource_uri = uri.resource_uri()
            if (
                cached.get(info.name, resource_uri, [uri.suffix])  # Already cached.
                or not definition.filters.matches(resource_uri)  # Not this resource.
                or not info.matches_forall(uri.suffix)  # Not this observation.
            ):
                continue

            property_name = info.name.as_property(uri)
            properties[property_name] = (uri, info.name)
            bisect_insert(label_properties, property_name, key=id)

        if label_properties:
            labels[info.name] = (info, label_properties)

    return _LabelRequestPartial(
        labels=labels,
        properties=properties,
        rendered=rendered,
    )


##
## Inference
##


GENERATE_LABELS_SYSTEM = """\
You are a knowledge extraction assistant. Generate metadata labels for the \
provided observations.

For each property in the response schema, generate an appropriate value based \
on the label description and observation content. \
Return null if the label cannot be inferred or if the previous value is accurate.\
"""


async def _generate_labels(
    context: KnowledgeContext,
    cached: LabelValues,  # noqa: ARG001
    requests: list[_LabelRequest],
) -> list[LabelValue]:
    """
    TODO: Determine which requests can be run in parallel, and which should be
    run sequentially, or run everything in parallel (batched), then join the
    label values that appear in more than one response.
    """
    results: list[LabelValue] = []
    for request in requests:
        response = await _generate_labels_once(context, results, request)
        bisect_extend(results, response, key=LabelValue.sort_key)
    return results


async def _generate_labels_once(
    context: KnowledgeContext,
    previous: list[LabelValue],
    request: _LabelRequest,
) -> list[LabelValue]:
    inference = context.service(SvcInference)

    system_message: list[str] = []
    response_properties: dict[str, JsonSchemaValue] = {}

    for label, (info, schema, properties) in request.labels.items():
        system_mapping: list[str] = []
        for property_name in properties:
            uri, _ = request.properties[property_name]
            response_properties[property_name] = {
                **schema,
                "description": f"{label} label for {uri}",
            }
            system_mapping.append(f"- {uri} -> {property_name}")

        system_mapping_str = "\n".join(system_mapping)
        system_message.append(
            f"## {label}\n\n{info.prompt}\n\n"
            f"Mapping from URI to the corresponding property:\n{system_mapping_str}"
        )

    relevant_previous = {
        property_name: label_value.value
        for label_value in previous
        if (property_name := label_value.name.as_property(label_value.target))
        and property_name in response_properties
    }
    prompt_prefix = (
        f"Existing properties:\n\n```json\n{as_json(relevant_previous, indent=2)}\n```"
    )

    prompt: list[str | ContentBlob] = request.rendered.as_llm_inline(
        supports_media=SUPPORTED_IMAGE_BLOB_TYPES,
    )
    prompt[0] = f"{prompt_prefix}\n\n<observations>\n{prompt[0]}"
    prompt[-1] = f"{prompt[-1]}\n</observations>"

    try:
        response_json = await inference.completion_json(
            system="\n\n".join([GENERATE_LABELS_SYSTEM, *system_message]),
            response_schema={
                "type": "object",
                "properties": response_properties,
                "required": list(response_properties.keys()),
                "additionalProperties": False,
            },
            prompt=prompt,
        )
    except Exception:
        if KnowledgeConfig.verbose:
            logger.exception("Failed to generate labels")
        return []

    return _parse_labels_response(response_json, request.properties)


def _parse_labels_response(
    response_json: str,
    property_mapping: dict[str, tuple[AnyBodyObservableUri, LabelName]],
) -> list[LabelValue]:
    """Parse LLM response JSON and extract inferred labels."""
    results: list[LabelValue] = []

    try:
        response = json.loads(response_json)
        if not isinstance(response, dict):
            if KnowledgeConfig.verbose:
                logger.warning("Invalid labels response: %s", response_json)
            return []
    except json.JSONDecodeError:
        if KnowledgeConfig.verbose:
            logger.warning("Invalid labels response: %s", response_json)
        return []

    for prop_name, prop_value in response.items():
        if prop_name not in property_mapping:
            if KnowledgeConfig.verbose:
                logger.warning(
                    "Generated unexpected label %s: %s",
                    prop_name,
                    json.dumps(prop_value),
                )
            continue
        if prop_value is None:
            continue

        target, label_name = property_mapping[prop_name]
        result = LabelValue(name=label_name, target=target, value=prop_value)
        bisect_insert(results, result, key=LabelValue.sort_key)

    return results


##
## Configuration
##


class LabelsConfig(BaseModel, frozen=True):
    labels: list[LabelDefinition]

    @staticmethod
    def defaults() -> "list[LabelDefinition]":
        return [
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
        ]


@cache
def labels_config() -> LabelsConfig:
    try:
        config_yaml = KnowledgeConfig.cfg_path("labels.yml").read_text()
        config = parse_yaml_as(LabelsConfig, config_yaml)
        return LabelsConfig(
            labels=bisect_make(
                [*LabelsConfig.defaults(), *config.labels],
                key=lambda ld: ld.sort_key(),
            ),
        )
    except Exception:
        logger.error("Failed to read config: labels.yml")  # noqa: TRY400
        return LabelsConfig(labels=LabelsConfig.defaults())
