import asyncio
import logging

from dataclasses import dataclass
from datetime import datetime, UTC
from timeit import default_timer

from base.core.exceptions import ApiError, ErrorInfo
from base.core.values import as_json
from base.resources.action import (
    LoadMode,
    QueryAction,
    QueryWriteAction,
    ResourcesAttachmentAction,
)
from base.resources.aff_body import AffBody
from base.resources.aff_collection import AffCollection, BundleCollection
from base.resources.bundle import ObservationError, Resources
from base.resources.metadata import ResourceInfo
from base.resources.observation import ObservationBundle
from base.strings.resource import ExternalUri, Observable, RootReference
from base.utils.sorted_list import bisect_insert, bisect_make

from knowledge.config import KnowledgeConfig
from knowledge.domain.ingestion import IngestedResult, ingest_observe_result
from knowledge.domain.resolve import (
    resolve_locator,
    try_infer_and_resolve_locators,
    try_infer_locator,
    try_resolve_relations,
)
from knowledge.domain.storage import (
    CacheStorage,
    list_relations,
    read_cached_bundle,
    read_resource_history,
    remove_alias,
    remove_relation,
    save_alias,
    save_cached_bundle,
    save_relation,
    save_resource_history,
)
from knowledge.models.pending import Dependency, PendingResult, PendingState
from knowledge.models.storage import (
    Locator,
    MetadataDelta,
    ResourceDelta,
    ResourceHistory,
    ResourceView,
)
from knowledge.server import metrics
from knowledge.server.context import (
    Connector,
    KnowledgeContext,
    ObserveResult,
    ResolveResult,
)

logger = logging.getLogger(__name__)

BATCH_SIZE_QUERY = 20
"""
The number of parallel actions that can be executed by the service for a single
request, capped to prevent it from being overwhelmed by too many requests and
crashing with an 'Out of Memory' error.
"""


async def execute_query_all(
    context: KnowledgeContext,
    actions: list[QueryAction],
) -> Resources:
    metrics.track_request()

    state = PendingState.new()
    write_actions = await _convert_query_actions(context, state, actions)
    for action in write_actions:
        await _execute_write(context, state, action)

    while batch := state.next_batch(BATCH_SIZE_QUERY):
        if KnowledgeConfig.verbose:
            print("READ BATCH:", as_json([b.locator for b in batch]))
        await _execute_reads(context, state, batch)

    return state.into_resources()


@dataclass(kw_only=True)
class QueryResult:
    metadata: MetadataDelta
    observed: list[IngestedResult]
    expired: list[Observable]
    errors: list[ObservationError]
    cached_bundles: list[ObservationBundle]
    should_cache: bool


##
## Result
##


async def _convert_query_actions(
    context: KnowledgeContext,
    state: PendingState,
    query_actions: list[QueryAction],
) -> list[QueryWriteAction]:
    """
    Convert `QueryAction` into `PendingRead` actions.
    Moreover, immediately resolve external URIs and add an error to `Resources`
    if they cannot be resolved.
    """
    write_actions: list[QueryWriteAction] = []

    for action in query_actions:
        # Resolve the requested URI into a `Locator`.
        uri = (
            action.uri
            if isinstance(action.uri, ExternalUri)
            else action.uri.resource_uri()
        )
        locator = await try_infer_locator(context, uri)
        if not locator:
            state.add_locator_unavailable(uri)
            continue

        state.add_action(locator, action)
        if isinstance(action, ResourcesAttachmentAction):
            write_actions.append(action)

    return write_actions


async def _handle_query_result(
    context: KnowledgeContext,
    state: PendingState,
    locator: Locator,
    refreshed_at: datetime,
    result: QueryResult,
) -> None:
    # Update the cache.
    new_history = await _save_resource(
        context=context,
        locator=locator,
        refreshed_at=refreshed_at,
        result=result,
    )

    # Add the resource and observations to the result.
    pending = state.result(locator)
    pending.update(
        resource=ResourceInfo(
            uri=locator.resource_uri(),
            attributes=new_history.all_attributes(),
            aliases=new_history.all_aliases(),
            affordances=new_history.all_affordances(),
        ),
        observed=[
            *[obs.bundle for obs in result.observed],
            *result.cached_bundles,
            *result.errors,
        ],
    )

    # Generate follow-up reads for the relations and dependencies.
    # NOTE: Only include relations that the user is allowed to see; discard
    # silently otherwise, to avoid leaking information.
    await _expand_relations(context, state, pending)

    # Extract dependencies from the requested new and cached bundles.
    # NOTE: Only include dependencies that the user is allowed to see; discard
    # silently otherwise, to avoid leaking information.
    for bundle in result.cached_bundles:
        await _expand_dependencies(context, state, pending, bundle)
    for obs in result.observed:
        await _expand_dependencies(context, state, pending, obs.bundle)


async def _save_resource(
    context: KnowledgeContext,
    locator: Locator,
    refreshed_at: datetime,
    result: QueryResult,
) -> ResourceHistory:
    resource_delta = ResourceDelta(
        refreshed_at=refreshed_at,
        locator=locator,
        metadata=result.metadata,
        expired=result.expired,
        observed=[obs.observed for obs in result.observed],
    )

    # Save the updated metadata (unless caching is disallowed).
    resource_uri = locator.resource_uri()
    old_history = await read_resource_history(context, resource_uri)
    if old_history:
        new_history = old_history.model_copy(deep=True)
        if new_history.update(resource_delta):
            await save_resource_history(context, new_history)
    else:
        new_history = ResourceHistory(history=[resource_delta])
        if result.should_cache or any(obs.should_cache for obs in result.observed):
            await save_resource_history(context, new_history)

            # Delete the alias from the resource URI to the Locator, since the
            # Locator can be read from the cached metadata.
            await remove_alias(context, resource_uri)
        else:
            context.cached(CacheStorage).resources[resource_uri] = new_history
            return new_history

    # Save the relevant bundles in the cache.
    for obs in result.observed:
        if obs.should_cache:
            await save_cached_bundle(context, obs.bundle)

    # Update aliases from the metadata.
    old_aliases = set(old_history.all_aliases()) if old_history else set()
    new_aliases = set(new_history.all_aliases())
    for created_alias in new_aliases:
        await save_alias(context, created_alias, locator)
    for deleted_alias in old_aliases - new_aliases:
        await remove_alias(context, deleted_alias)

    # Update relations from the metadata and observations.
    old_relations = (
        {r.unique_id(): r for r in old_history.all_relations()} if old_history else {}
    )
    new_relations = {r.unique_id(): r for r in new_history.all_relations()}
    old_relation_ids = set(old_relations.keys())
    new_relation_ids = set(new_relations.keys())
    for deleted_relation_id in old_relation_ids - new_relation_ids:
        await remove_relation(context, resource_uri, old_relations[deleted_relation_id])
    for created_relation_id in new_relation_ids - old_relation_ids:
        await save_relation(context, resource_uri, new_relations[created_relation_id])

    return new_history


async def _expand_relations(
    context: KnowledgeContext,
    state: PendingState,
    pending: PendingResult,
) -> None:
    if pending.relations_depth <= pending.request_expand_depth:
        return

    # Find all relations that the client is allowed to view.
    resource_uri = pending.locator.resource_uri()
    relations, relation_locators = await try_resolve_relations(
        context,
        origin=resource_uri,
        relations=await list_relations(context, resource_uri),
    )

    # Record valid relations in the state.
    for relation in relations:
        bisect_insert(state.relations, relation, key=lambda r: r.unique_id())

    # Load the metadata of related resources.
    for dep_locator, dep_relation_ids in relation_locators:
        dep_pending = state.result(dep_locator)
        dep_pending.update(
            request_expand_depth=pending.request_expand_depth - 1,
            request_expand_mode=pending.request_expand_mode,
            request_load_mode=pending.request_expand_mode,
        )
        for relation_id in dep_relation_ids:
            dep_pending.update(reason=relation_id)

    pending.update(relations_depth=pending.request_expand_depth)


async def _expand_dependencies(
    context: KnowledgeContext,
    state: PendingState,
    pending: PendingResult,
    bundle: ObservationBundle,
) -> None:
    observations = bundle.observations()
    unchecked_dependencies: list[RootReference] = [
        dep.root_uri()
        for observation in observations
        for dep in observation.dependencies()
    ]
    unchecked_embeds: list[RootReference] = [
        dep.root_uri() for observation in observations for dep in observation.embeds()
    ]

    locators = await try_infer_and_resolve_locators(
        context, [*unchecked_dependencies, *unchecked_embeds]
    )

    for dependency in unchecked_dependencies:
        if not (locator := locators.get(dependency)):
            continue
        dep_pending = state.result(locator)

        # When a collection's children are also collections, expand recursively,
        # treating it as a relation.
        if isinstance(bundle, BundleCollection):
            dep_pending.update(
                reason=Dependency(kind="collection", origin=bundle.uri.resource_uri()),
                request_expand_depth=pending.request_expand_depth - 1,
                request_expand_mode=pending.request_expand_mode,
                request_load_mode=pending.request_expand_mode,
                request_observe=[AffCollection.new()],
            )

        # Links that are not relations should NOT be expanded or auto-refreshed:
        # there is probably a good reason to omit those "link" relations.
        else:
            dep_pending.update(
                reason=Dependency(kind="link", origin=bundle.uri.resource_uri()),
                request_expand_depth=0,
                request_expand_mode="none",
                request_load_mode="none",
                request_observe=[],
            )

    for dependency in unchecked_embeds:
        if not (locator := locators.get(dependency)):
            continue

        # Embeds are NOT expanded recursively: we simply observe their "$body",
        # but we refresh it as though it were the origin.
        dep_pending = state.result(locator)
        dep_pending.update(
            reason=Dependency(kind="embed", origin=bundle.uri.resource_uri()),
            request_expand_depth=0,
            request_expand_mode=pending.request_expand_mode,
            request_load_mode=pending.request_load_mode,
            request_observe=[AffBody.new()],
        )


##
## Read
##


async def _execute_reads(
    context: KnowledgeContext,
    state: PendingState,
    pending: list[PendingResult],
) -> None:
    subtasks = [
        _execute_query(
            context=context,
            locator=p.locator,
            load_mode=p.request_load_mode,
            observe=p.missing_observe() or [],
        )
        for p in pending
    ]
    refreshed_at = datetime.now(UTC)
    for locator, result in await asyncio.gather(*subtasks):
        if isinstance(result, QueryResult):
            await _handle_query_result(context, state, locator, refreshed_at, result)
        else:
            state.add_error(locator, result)


async def _execute_query(
    context: KnowledgeContext,
    locator: Locator,
    load_mode: LoadMode,
    observe: list[Observable],
) -> tuple[Locator, QueryResult | ErrorInfo]:
    try:
        connector = context.find_connector(locator)
        resource_uri = locator.resource_uri()
        cached = (
            h.merged()
            if (h := await read_resource_history(context, resource_uri))
            else None
        )

        resolved = await resolve_locator(context, locator)

        observed, errors, cached_bundles = await _execute_query_observe(
            context=context,
            locator=locator,
            load_mode=load_mode,
            observe=observe,
            connector=connector,
            cached=cached,
            resolved=resolved,
        )

        ingested = await _execute_query_ingest(
            context=context,
            locator=locator,
            cached=cached,
            resolved=resolved,
            observed=observed,
            errors=errors,
            cached_bundles=cached_bundles,
        )

        return locator, ingested
    except Exception as exc:
        if KnowledgeConfig.verbose:
            logger.exception("Failed to execute query: %s", as_json(locator))
        return locator, ApiError.from_exception(exc).as_info()


async def _execute_query_observe(
    context: KnowledgeContext,
    locator: Locator,
    load_mode: LoadMode,
    observe: list[Observable],
    connector: Connector,
    cached: ResourceView | None,
    resolved: ResolveResult,
) -> tuple[list[ObserveResult], list[ObservationError], list[ObservationBundle]]:
    # Merge the resolved metadata delta into the cached metadata.
    if cached:
        metadata = cached.metadata.with_update(resolved.metadata)
        expired = bisect_make([*cached.expired, *resolved.expired], key=str)
    else:
        metadata = resolved.metadata
        expired = bisect_make(resolved.expired, key=str)

    # Read cached bundles.
    cached_observations: list[ObservationBundle] = []
    missing_observe: list[Observable] = []
    for observable in observe:
        if (
            load_mode != "force"
            and observable not in expired
            and (
                cached_bundle := await read_cached_bundle(
                    context,
                    locator.resource_uri(),
                    observable.affordance(),
                )
            )
        ):
            bisect_insert(
                cached_observations,
                cached_bundle,
                key=lambda obs: str(obs.uri.suffix),
            )
        else:
            bisect_insert(missing_observe, observable, key=str)

    supported = [affordance.suffix for affordance in metadata.affordances or []]
    already_observed = [obs.suffix for obs in cached.observed] if cached else []

    # If the "$body" affordance is supported, but expired or was never read,
    # then refresh it to generate descriptions and extract "link" relations,
    # even when it was not explicitly requested.
    aff_body = AffBody.new()
    if (
        load_mode != "none"
        and aff_body in supported
        and (aff_body in expired or aff_body not in already_observed)
    ):
        bisect_insert(missing_observe, aff_body, key=str)

    # If the "$collection" affordance is supported, but expired or was never
    # read, then refresh it to extract "parent" relations, even when it was not
    # explicitly requested.
    aff_collection = AffCollection.new()
    if (
        load_mode != "none"
        and aff_collection in supported
        and (aff_collection in expired or aff_collection not in already_observed)
    ):
        bisect_insert(missing_observe, aff_collection, key=str)

    # Observe the requested (or auto-refreshed) observables.
    observe_results: list[ObserveResult] = []
    observe_errors: list[ObservationError] = []
    for observable in missing_observe:
        start_time = default_timer()
        try:
            if observable not in supported:
                continue

            observe_result = await connector.observe(
                locator=locator,
                observable=observable,
                resolved=metadata,
            )
            observe_results.append(observe_result)

            metrics.track_read_duration(
                locator=locator,
                observable=observable,
                mime_type=observe_result.metadata.mime_type or metadata.mime_type,
                success=True,
                duration_secs=default_timer() - start_time,
            )
        except Exception as exc:
            metrics.track_read_duration(
                locator=locator,
                observable=observable,
                mime_type=metadata.mime_type,
                success=False,
                duration_secs=default_timer() - start_time,
            )
            observe_errors.append(
                ObservationError(
                    uri=locator.resource_uri().child_observable(observable),
                    error=ApiError.from_exception(exc).as_info(),
                )
            )

    return observe_results, observe_errors, cached_observations


async def _execute_query_ingest(
    context: KnowledgeContext,
    locator: Locator,
    cached: ResourceView | None,
    resolved: ResolveResult,
    observed: list[ObserveResult],
    errors: list[ObservationError],
    cached_bundles: list[ObservationBundle],
) -> QueryResult:
    # Record all newly expired observations that were not refreshed.
    new_expired: set[Observable] = set(resolved.expired)
    ingested: list[IngestedResult] = []
    metadata: MetadataDelta = cached.metadata if cached else MetadataDelta()
    metadata = metadata.with_update(resolved.metadata)

    for obs in observed:
        observable = (
            obs.bundle.uri.suffix
            if isinstance(obs.bundle, ObservationBundle)
            else AffBody.new()
        )
        new_expired.discard(observable)
        metadata = metadata.with_update(obs.metadata)

        start_time = default_timer()
        try:
            obs_ingested = await ingest_observe_result(
                context=context,
                resource_uri=locator.resource_uri(),
                cached=cached,
                metadata=metadata,
                observed=obs,
            )
            metadata = metadata.with_update(obs_ingested.metadata)
            bisect_insert(ingested, obs_ingested, key=lambda o: str(o.bundle.uri))

            metrics.track_ingestion_duration(
                locator=locator,
                observable=observable,
                success=False,
                duration_secs=default_timer() - start_time,
            )
        except Exception as exc:
            metrics.track_ingestion_duration(
                locator=locator,
                observable=observable,
                success=False,
                duration_secs=default_timer() - start_time,
            )
            bisect_insert(
                errors,
                ObservationError(
                    uri=locator.resource_uri().child_observable(observable),
                    error=ApiError.from_exception(exc).as_info(),
                ),
                key=lambda err: str(err.uri),
            )

    return QueryResult(
        metadata=metadata,
        observed=ingested,
        expired=sorted(new_expired, key=str),
        errors=errors,
        cached_bundles=cached_bundles,
        should_cache=resolved.should_cache,
    )


##
## Write
##


async def _execute_write(
    context: KnowledgeContext,
    state: PendingState,
    action: QueryWriteAction,
) -> None:
    """
    Call `_execute_query` for each write action sequentially, using the results
    to update the caches in Storage and the `RequestStatus`.
    """
    # TODO:
    # refreshed_at = datetime.now(UTC)
    # for action in actions:
    #     _, result = await _execute_query(context, action)
    #     await _execute_query_save(context, state, refreshed_at, action, result)
