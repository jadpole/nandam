import asyncio
import logging

from collections.abc import Iterable
from dataclasses import dataclass
from timeit import default_timer

from base.models.context import NdCache
from base.resources.relation import Relation, RelationId
from base.strings.resource import ExternalUri, ResourceUri, RootReference
from base.utils.sorted_list import bisect_make

from knowledge.config import KnowledgeConfig
from knowledge.domain.storage import read_alias, read_resource_history, save_alias
from knowledge.models.storage_metadata import Locator, ResourceHistory
from knowledge.server import metrics
from knowledge.server.context import KnowledgeContext, ResolveResult

logger = logging.getLogger(__name__)

BATCH_SIZE_RESOLVE = 10
"""
The number of relations or dependencies that can be validated in parallel.
"""


@dataclass(kw_only=True)
class CacheResolve(NdCache):
    locators: dict[RootReference, Locator | None]
    resolves: dict[RootReference, ResolveResult | Exception]

    @classmethod
    def initialize(cls) -> "CacheResolve":
        return CacheResolve(locators={}, resolves={})


##
## Locator
##


async def try_infer_locator(
    context: KnowledgeContext,
    uri: RootReference,
) -> Locator | None:
    cache = context.cached(CacheResolve)

    # If `infer_locator` was already called with this URI in the same request,
    # then return the previous value (or failure) from the in-memory cache.
    if uri in cache.locators:
        return cache.locators[uri]

    try:
        # Otherwise, infer the Locator from the URI using the connectors.
        cached_resource: ResourceHistory | None = None
        locator: Locator
        if isinstance(uri, ResourceUri) and (
            cached_resource := await read_resource_history(context, uri)
        ):
            locator = cached_resource.merged().locator
        elif cached_locator := await read_alias(context, uri):
            locator = cached_locator
        else:
            start_time = default_timer()
            try:
                locator, _ = await context.locator(uri)
                metrics.track_locator_duration(
                    realm=locator.realm,
                    locator=locator,
                    duration_secs=default_timer() - start_time,
                )
            except Exception:
                metrics.track_locator_duration(
                    realm=uri.realm if isinstance(uri, ResourceUri) else None,
                    locator=None,
                    duration_secs=default_timer() - start_time,
                )
                raise

        # Cache the result in-memory for future calls in the same request.
        resource_uri = locator.resource_uri()
        cache.locators[resource_uri] = locator

        # If the URL was mapped to a Locator with no cached metadata, then define a
        # placeholder mapping from the resource URI to the Locator, which the client
        # may ingest "on-demand" via a subsequent "resources/load" request.
        if (
            not cached_resource
            and isinstance(uri, ExternalUri)
            and not await read_resource_history(context, resource_uri)
        ):
            await save_alias(context, resource_uri, locator)

        return locator
    except Exception:
        # NOTE: Disabled since it's TOO verbose.
        if KnowledgeConfig.verbose:
            logger.exception("Failed to infer locator: %s", uri)
        cache.locators[uri] = None
        return None


async def try_infer_locators(
    context: KnowledgeContext,
    uris: list[RootReference],
) -> dict[RootReference, Locator | None]:
    result: dict[RootReference, Locator | None] = {}
    for uri in uris:
        result[uri] = await try_infer_locator(context, uri)
    return result


##
## Resolve
##


async def resolve_locator(
    context: KnowledgeContext,
    locator: Locator,
) -> ResolveResult:
    cache = context.cached(CacheResolve)
    resource_uri = locator.resource_uri()
    if resource_uri in cache.resolves:
        cached_result = cache.resolves[resource_uri]
        if isinstance(cached_result, ResolveResult):
            return cached_result
        else:
            raise cached_result

    start_time = default_timer()
    try:
        connector = context.find_connector(locator)
        cached_resource = (
            h.merged()
            if (h := await read_resource_history(context, resource_uri))
            else None
        )
        resolve_result = await connector.resolve(locator, cached_resource)
        cache.resolves[resource_uri] = resolve_result

        metrics.track_resolve_duration(
            locator=locator,
            success=True,
            duration_secs=default_timer() - start_time,
        )
        return resolve_result
    except Exception as exc:
        if KnowledgeConfig.verbose:
            logger.exception("Failed to resolve locator: %s", locator.model_dump_json())

        cache.resolves[resource_uri] = exc
        metrics.track_resolve_duration(
            locator=locator,
            success=False,
            duration_secs=default_timer() - start_time,
        )
        raise


async def try_resolve_locator(
    context: KnowledgeContext,
    locator: Locator,
) -> ResolveResult | None:
    try:
        return await resolve_locator(context, locator)
    except Exception:
        return None


##
## Relation
##


async def try_infer_and_resolve_locator(
    context: KnowledgeContext,
    uri: RootReference,
) -> Locator | None:
    if not (locator := await try_infer_locator(context, uri)):
        return None
    if await try_resolve_locator(context, locator):
        return locator
    return None


async def try_infer_and_resolve_locators(
    context: KnowledgeContext,
    uris: Iterable[RootReference],
) -> dict[RootReference, Locator]:
    # Deduplicate URIs.
    uris = bisect_make(uris, key=str)

    # Resolve URIs in parallel.
    result: dict[RootReference, Locator] = {}
    for start_index in range(0, len(uris), BATCH_SIZE_RESOLVE):
        batch = uris[start_index : start_index + BATCH_SIZE_RESOLVE]
        tasks = [try_infer_and_resolve_locator(context, uri) for uri in batch]
        locators = await asyncio.gather(*tasks)

        for uri, locator in zip(batch, locators, strict=True):
            if locator:
                result[uri] = locator  # noqa: PERF403

    return result


async def try_resolve_relations(
    context: KnowledgeContext,
    origin: ResourceUri,
    relations: list[Relation],
) -> tuple[list[Relation], list[tuple[Locator, list[RelationId]]]]:
    locators = await try_infer_and_resolve_locators(
        context,
        (uri for relation in relations for uri in relation.get_nodes()),
    )

    valid_relations: list[Relation] = bisect_make(
        (
            relation
            for relation in relations
            if all(locators.get(node_uri) for node_uri in relation.get_nodes())
        ),
        key=lambda r: r.unique_id(),
    )

    valid_mapping: dict[ResourceUri, tuple[Locator, list[RelationId]]] = {}
    for relation in valid_relations:
        for node_uri in relation.get_nodes():
            if node_uri == origin:
                continue
            if existing := valid_mapping.get(node_uri):
                existing[1].append(relation.unique_id())
            else:
                valid_mapping[node_uri] = (locators[node_uri], [relation.unique_id()])

    valid_locators = sorted(valid_mapping.items(), key=lambda x: str(x[0]))
    return valid_relations, [locator_pair for _, locator_pair in valid_locators]
