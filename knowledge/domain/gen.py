from pydantic.json_schema import JsonSchemaValue

from base.models.rendered import Rendered
from base.resources.aff_body import AnyBodyObservableUri, AnyObservationBody
from base.resources.label import AnyLabelConstraint, EnumConstraint
from base.utils.sorted_list import bisect_make

from knowledge.models.storage_observed import BundleBody
from knowledge.server.context import KnowledgeContext


def render_body_groups(
    bundles: list[BundleBody],
    group_threshold_tokens: int,
) -> tuple[list[Rendered], list[AnyObservationBody]]:
    all_observations: list[AnyObservationBody] = bisect_make(
        (
            obs
            for bundle in bundles
            for obs in bundle.observations()  # type: ignore
        ),
        key=lambda obs: str(obs.uri),
    )
    root_uris: list[AnyBodyObservableUri] = []
    for bundle in bundles:
        if bundle.sections or len(bundle.chunks) > 1:
            root_uris.extend(chunk.uri for chunk in bundle.chunks)
        else:
            root_uris.append(bundle.body_uri())

    # Group bundles by token count for batched inference.
    rendered_groups = Rendered.render_groups(
        root_uris,
        list(all_observations),
        group_threshold_tokens=group_threshold_tokens,
    )

    return rendered_groups, all_observations


async def generate_property_schema(
    context: KnowledgeContext,  # noqa: ARG001
    name: str,
    constraint: AnyLabelConstraint | None,
) -> tuple[str, JsonSchemaValue]:
    """
    TODO: Build schema based on constraint; load variants from S3 when useful.
    """
    if not constraint:
        return name, {"type": ["string", "null"]}
    elif isinstance(constraint, EnumConstraint):
        return name, {"type": ["string", "null"], "enum": [*constraint.variants, None]}
    else:
        raise ValueError(f"Unexpected constraint: {constraint}")
