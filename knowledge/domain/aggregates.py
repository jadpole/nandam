import asyncio
import json
import logging

from pydantic.json_schema import JsonSchemaValue

from base.models.content import ContentBlob
from base.resources.action import QueryAction
from base.resources.action import ResourcesLoadAction
from base.core.values import as_json
from base.models.rendered import Rendered
from base.resources.aff_body import AffBody
from base.resources.bundle import Resource
from base.resources.label import (
    AggregateDefinition,
    AggregateValue,
    AllowRule,
    LabelDefinition,
    LabelValue,
)
from base.utils.sorted_list import bisect_extend, bisect_find, bisect_make, bisect_union

from knowledge.domain.gen import generate_property_schema, render_body_groups
from knowledge.domain.labels import generate_labels
from knowledge.domain.query import execute_query_all
from knowledge.domain.resolve import scan_resources
from knowledge.models.storage_observed import BundleBody
from knowledge.server.context import KnowledgeContext
from knowledge.services.inference import SUPPORTED_IMAGE_BLOB_TYPES, SvcInference

logger = logging.getLogger(__name__)


async def scan_and_load_resources(
    context: KnowledgeContext,
    rules: list[list[AllowRule]],
) -> list[tuple[Resource, BundleBody]]:
    """
    Scan all resources that match the allowlist and load them with load_mode="none".
    """
    resource_uris = await scan_resources(context, rules)

    actions: list[QueryAction] = [
        ResourcesLoadAction(uri=uri, load_mode="none", observe=[AffBody.new()])
        for uri in resource_uris
    ]
    pending = await execute_query_all(context, actions)

    # Filter by label criteria using satisfied_by on the resource labels.
    # Labels are stored on individual Resource objects.
    return [
        (resource, bundle)
        for resource, bundles, _ in pending.build_all(context)
        if isinstance(resource, Resource)
        and (bundle := next((b for b in bundles if isinstance(b, BundleBody)), None))
    ]


async def generate_labels_and_aggregates(
    context: KnowledgeContext,
    req_labels: list[LabelDefinition],
    req_aggregates: list[AggregateDefinition],
) -> tuple[list[tuple[Resource, BundleBody]], list[LabelValue], list[AggregateValue]]:
    # Start by resolving all resources that may be relevant to calculate either
    # the labels or aggregates and whose "$body" is cached.
    scan_rules = [
        *[lab.filters.allowlist for lab in req_labels],
        *[agg.filters.allowlist for agg in req_aggregates],
    ]
    scanned = await scan_and_load_resources(context, scan_rules)
    if not scanned:
        return scanned, [], []

    # NOTE: Discard the cached value when a standard label's name appears in the
    # request custom labels (shadowing).
    cached: list[LabelValue] = [
        label.as_absolute(resource.uri)
        for resource, _ in scanned
        for label in resource.labels
        if not any(label.name == lab.info.name for lab in req_labels)
    ]

    # Generate custom labels using the provided definitions.
    custom_labels = await generate_labels(
        context=context,
        cached=cached,  # No cached values for custom labels.
        bundles=[
            bundle
            for resource, bundle in scanned
            if any(
                lab.filters.matches(resource.uri)
                and lab.filters.satisfied_by(resource.labels)
                for lab in req_labels
            )
        ],
        definitions=req_labels,
    )
    all_labels = bisect_union(cached, custom_labels, key=LabelValue.sort_key)

    # Group aggregate definitions that apply to the same bundles together, so
    # they can be sent as a single LLM request.
    agg_groups: dict[str, tuple[list[AggregateDefinition], list[BundleBody]]] = {}
    for agg in req_aggregates:
        aggregate_inputs = bisect_make(
            (
                resource.uri
                for resource, _ in scanned
                if agg.filters.matches(resource.uri)
                and agg.filters.satisfied_by(
                    [
                        label.as_relative()
                        for label in all_labels
                        if label.target.resource_uri() == resource.uri
                    ]
                )
            ),
            key=str,
        )
        agg_group_id = "/".join(str(uri) for uri in aggregate_inputs)

        if agg_group_id not in agg_groups:
            group_bundles = [
                pair[1]
                for uri in aggregate_inputs
                if (pair := bisect_find(scanned, uri, key=lambda p: str(p[0].uri)))
            ]
            agg_groups[agg_group_id] = ([], group_bundles)

        agg_groups[agg_group_id][0].append(agg)

    # Generate aggregate values for each group.
    aggregates: list[AggregateValue] = []
    for agg_definitions, agg_bundles in agg_groups.values():
        aggregates.extend(
            await _generate_aggregates(
                context=context,
                bundles=agg_bundles,
                definitions=agg_definitions,
            )
        )

    return scanned, custom_labels, aggregates


##
## Step 3: Generate Aggregates
##


THRESHOLD_AGGREGATE_GROUP = 80_000
"""
Maximum tokens per aggregate inference group. Bundles are batched to fit within
this limit based on their `num_tokens()` estimates.
"""

GENERATE_AGGREGATES_SYSTEM = """\
You are a knowledge aggregation assistant. Generate aggregate values that \
summarize the provided resources based on their content and labels.

For each aggregate property in the response schema, analyze all resources \
and generate an appropriate summary value. \
Return null if the aggregate cannot be determined or if the previous value is accurate.\
"""


async def _generate_aggregates(
    context: KnowledgeContext,
    bundles: list[BundleBody],
    definitions: list[AggregateDefinition],
) -> list[AggregateValue]:
    """
    Generate aggregate values across all bundles using the provided definitions.

    Each aggregate definition produces a single value that summarizes information
    across all matching resources. Groups are processed sequentially, allowing
    the agent to update aggregate state as it sees more information.
    """
    results: list[AggregateValue] = [
        AggregateValue(name=agg.name, value=agg.constraint.default_value())
        for agg in definitions
    ]

    if not definitions or not bundles:
        return results

    rendered_groups, _ = render_body_groups(bundles, THRESHOLD_AGGREGATE_GROUP)
    property_schemas_tasks = [
        generate_property_schema(context, agg.name, agg.constraint)
        for agg in definitions
    ]
    property_schemas = dict(await asyncio.gather(*property_schemas_tasks))
    response_schema = {
        "type": "object",
        "properties": {
            agg.name: {**property_schemas[agg.name], "description": agg.prompt}
            for agg in definitions
        },
        "required": [str(agg.name) for agg in definitions],
        "additionalProperties": False,
    }

    for rendered in rendered_groups:
        response = await _generate_aggregates_once(
            context, results, rendered, definitions, response_schema
        )
        bisect_extend(results, response, key=AggregateValue.sort_key)

    return results


async def _generate_aggregates_once(
    context: KnowledgeContext,
    previous: list[AggregateValue],
    rendered: Rendered,
    definitions: list[AggregateDefinition],
    response_schema: JsonSchemaValue,
) -> list[AggregateValue]:
    """Generate or update aggregate values for a single rendered group."""
    inference = context.service(SvcInference)

    try:
        response_json = await inference.completion_json(
            system="\n\n".join(
                [
                    GENERATE_AGGREGATES_SYSTEM,
                    *[f"## {agg.name}\n\n{agg.prompt}" for agg in definitions],
                ]
            ),
            response_schema=response_schema,
            prompt=_build_aggregate_prompt(previous, rendered),
        )
        return _parse_aggregate_response(response_json, definitions)
    except Exception as exc:
        logger.warning("Failed to generate aggregate values: %s", str(exc))
        return []


def _build_aggregate_prompt(
    previous: list[AggregateValue],
    rendered: Rendered,
) -> list[str | ContentBlob]:
    """
    Build a "user" prompt for aggregate generation.
    """
    # Include previous aggregate values if any.
    previous_values = {
        str(agg.name): agg.value for agg in previous if agg.value is not None
    }
    prompt_prefix = f"""\
Current aggregate values:

```json
{as_json(previous_values, indent=2)}
```\
"""

    # Build the main prompt using Rendered.as_llm_inline().
    prompt: list[str | ContentBlob] = rendered.as_llm_inline(
        supports_media=SUPPORTED_IMAGE_BLOB_TYPES,
    )
    prompt[0] = f"{prompt_prefix}\n\n<observations>\n{prompt[0]}"
    prompt[-1] = f"{prompt[-1]}\n</observations>"

    return prompt


def _parse_aggregate_response(
    response_json: str,
    definitions: list[AggregateDefinition],
) -> list[AggregateValue]:
    """Parse LLM response JSON and extract aggregate values."""
    try:
        response = json.loads(response_json)
        if not isinstance(response, dict):
            return []

        definition_map = {str(d.name): d for d in definitions}

        return [
            AggregateValue(name=definition_map[property_name].name, value=value)
            for property_name, value in response.items()
            if value is not None and property_name in definition_map
        ]
    except json.JSONDecodeError:
        logger.warning("Failed to parse aggregate response: %s", response_json[:100])
        return []
