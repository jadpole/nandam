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
from base.resources.aff_collection import AffCollection
from base.resources.bundle import ObservationError
from base.resources.label import ResourceLabel
from base.resources.metadata import ResourceInfo
from base.strings.resource import ExternalUri, Observable, ResourceUri, RootReference
from base.utils.sorted_list import bisect_insert, bisect_make

from knowledge.config import KnowledgeConfig
from knowledge.domain.ingestion import IngestedResult, ingest_observe_result
from knowledge.domain.labels import generate_standard_labels
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
from knowledge.models.pending import (
    Dependency,
    LabelRequest,
    LabelResult,
    PendingResult,
    PendingState,
)
from knowledge.models.storage_metadata import (
    Locator,
    MetadataDelta,
    ResourceDelta,
    ResourceHistory,
    ResourceView,
)
from knowledge.models.storage_observed import AnyBundle, BundleBody, BundleCollection
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
) -> PendingState:
    """
    Execute a batch of query actions and return the aggregated resources.

    The pipeline runs label generation in parallel with batch processing:
    1. Convert actions and execute writes (attachments)
    2. For each batch:
       a. Start label tasks for any pending requests (from previous batch)
       b. Process the batch (resolve → observe → ingest)
       c. Save completed label results to storage
    3. Wait for remaining label tasks, save final results
    4. Merge all labels and return resources
    """
    metrics.track_request()

    state = PendingState.new()
    write_actions = await _convert_query_actions(context, state, actions)
    for action in write_actions:
        await _execute_write(context, state, action)

    # Process batches with parallel label generation.
    while True:
        # Start label tasks for any pending requests from previous batch.
        _start_label_tasks(context, state)

        # Process completed label tasks and save to storage.
        await _process_completed_labels(context, state)

        # Process next batch (if any).
        if batch := state.next_batch(BATCH_SIZE_QUERY):
            if KnowledgeConfig.verbose:
                print("READ BATCH:", as_json([b.locator for b in batch]))
            await _execute_reads(context, state, batch)
        elif state.label_queue.has_work():
            # No more batches, but label tasks still running - yield.
            await asyncio.sleep(0)
        else:
            # No more batches and no label work - done.
            break

    # Wait for any remaining label tasks.
    await state.label_queue.wait_all()
    await _process_completed_labels(context, state)

    return state


@dataclass(kw_only=True)
class QueryResult:
    """
    Result of resolving and observing a single resource.

    Label handling:
    - `resolve_labels`: From ResolveResult, applied directly to the resource.
    - Observation labels are handled via the LabelQueue (provided or generated).
    """

    metadata: MetadataDelta
    observed: list[IngestedResult]
    resolve_labels: list[ResourceLabel]
    expired: list[Observable]
    errors: list[ObservationError]
    cached_bundles: list[AnyBundle]
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
    resource_uri = locator.resource_uri()

    # Update the cache.
    new_history = await _save_resource(
        context=context,
        locator=locator,
        refreshed_at=refreshed_at,
        result=result,
    )

    # Add the resource and observations to the result.
    # Cached labels from history are included immediately.
    # Labels from ResolveResult are also added directly.
    pending = state.result(locator)
    pending.update(
        resource=ResourceInfo(
            uri=resource_uri,
            attributes=new_history.all_attributes(),
            aliases=new_history.all_aliases(),
            affordances=new_history.all_affordances(),
        ),
        observed=[
            *[obs.bundle for obs in result.observed],
            *result.cached_bundles,
            *result.errors,
        ],
        labels=[*new_history.all_labels(), *result.resolve_labels],
    )

    # Do not expand the graph from nodes that were filtered out.
    if not context.filters.satisfied_by(new_history.all_labels()):
        return

    # Queue label generation for bundles that need it.
    # Labels are generated after all batches complete, allowing parallelization.
    # - `provided_labels` from connector are used directly
    # - `reset_labels` specify which cached labels to regenerate
    # - `needs_labels` indicates whether generation is needed
    cached_labels = new_history.all_labels()
    for obs in result.observed:
        if isinstance(obs.bundle, BundleBody) and (
            obs.needs_labels or obs.provided_labels
        ):
            state.label_queue.enqueue(
                resource_uri=resource_uri,
                bundle=obs.bundle,
                cached_labels=cached_labels,
                reset_labels=obs.reset_labels,
                provided_labels=obs.provided_labels,
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
    # NOTE: Labels are generated asynchronously via the LabelQueue.
    # They are NOT saved to the resource history during this phase.
    resource_delta = ResourceDelta(
        refreshed_at=refreshed_at,
        locator=locator,
        expired=result.expired,
        labels=[],
        metadata=result.metadata,
        observed=[obs.observed for obs in result.observed],
        reset_labels=[],  # TODO: Reset when structure changed.
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
    bundle: AnyBundle,
) -> None:
    unchecked_links: list[RootReference] = [
        dep.root_uri() for dep in bundle.dep_links()
    ]
    unchecked_embeds: list[RootReference] = [
        dep.root_uri() for dep in bundle.dep_embeds()
    ]

    locators = await try_infer_and_resolve_locators(
        context, [*unchecked_links, *unchecked_embeds]
    )

    for dependency in unchecked_links:
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

        # Merge the resolved metadata delta into the cached metadata.
        if cached:
            metadata = cached.metadata.with_update(resolved.metadata)
            expired = bisect_make([*cached.expired, *resolved.expired], key=str)
        else:
            metadata = resolved.metadata
            expired = bisect_make(resolved.expired, key=str)

        cached_bundles, missing_observe = await _execute_query_observe_cache(
            context=context,
            locator=locator,
            load_mode=load_mode,
            observe=observe,
            cached=cached,
            expired=expired,
            metadata=metadata,
        )

        observed, errors = await _execute_query_observe(
            context=context,
            locator=locator,
            observe=missing_observe,
            connector=connector,
            metadata=metadata,
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


async def _execute_query_observe_cache(
    context: KnowledgeContext,
    locator: Locator,
    load_mode: LoadMode,
    observe: list[Observable],
    cached: ResourceView | None,
    expired: list[Observable],
    metadata: MetadataDelta,
) -> tuple[list[AnyBundle], list[Observable]]:
    supported = [affordance.suffix for affordance in metadata.affordances or []]
    already_observed = [obs.suffix for obs in cached.observed] if cached else []

    # Read cached bundles.
    cached_observations: list[AnyBundle] = []
    missing_observe: list[Observable] = []
    for observable in observe:
        if (
            load_mode != "force"
            and (load_mode == "none" or observable not in expired)
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
        elif load_mode != "none" and observable in supported:
            bisect_insert(missing_observe, observable, key=str)

    # Do not observe if the cached labels do not match the filters.
    if cached and not context.filters.satisfied_by(cached.labels):
        return cached_observations, []

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

    return cached_observations, missing_observe


async def _execute_query_observe(
    context: KnowledgeContext,  # noqa: ARG001
    locator: Locator,
    observe: list[Observable],
    connector: Connector,
    metadata: MetadataDelta,
) -> tuple[list[ObserveResult], list[ObservationError]]:
    # Observe the requested (or auto-refreshed) observables.
    observe_results: list[ObserveResult] = []
    observe_errors: list[ObservationError] = []
    for observable in observe:
        start_time = default_timer()
        try:
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

    return observe_results, observe_errors


async def _execute_query_ingest(
    context: KnowledgeContext,
    locator: Locator,
    cached: ResourceView | None,
    resolved: ResolveResult,
    observed: list[ObserveResult],
    errors: list[ObservationError],
    cached_bundles: list[AnyBundle],
) -> QueryResult:
    """
    Ingest observed bundles into the internal representation.

    NOTE: Label generation is decoupled.  The `IngestedResult.needs_labels`
    flag indicates which bundles should be queued for label generation.
    """
    new_expired: set[Observable] = set(resolved.expired)
    ingested: list[IngestedResult] = []
    metadata: MetadataDelta = cached.metadata if cached else MetadataDelta()
    metadata = metadata.with_update(resolved.metadata)

    for obs in observed:
        observable = (
            obs.bundle.uri.suffix
            if isinstance(obs.bundle, AnyBundle)
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
                success=True,
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
        resolve_labels=resolved.labels.copy(),
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


##
## Labels
##


def _start_label_tasks(
    context: KnowledgeContext,
    state: PendingState,
) -> None:
    """
    Start label generation tasks for all pending requests.

    This is called before each batch to start label generation in parallel.
    """
    if not state.label_queue.pending:
        return

    if KnowledgeConfig.verbose:
        total_bundles = sum(
            len(req.bundles) for req in state.label_queue.pending.values()
        )
        print(
            f"LABEL START: {len(state.label_queue.pending)} resources, {total_bundles} bundles"
        )

    # Create a closure that captures context.
    async def generate(uri: ResourceUri, req: LabelRequest) -> list[ResourceLabel]:
        return await _generate_labels_for_request(context, uri, req)

    state.label_queue.start_pending(generate)


async def _process_completed_labels(
    context: KnowledgeContext,
    state: PendingState,
) -> None:
    """
    Process completed label tasks: merge into state and save to storage.

    This is called after each batch to process any completed label tasks.
    Non-blocking: only processes tasks that have already finished.
    """
    state.label_queue.collect_completed()

    if not state.label_queue.has_completed():
        return

    results = state.label_queue.take_completed()

    if KnowledgeConfig.verbose:
        print(f"LABEL DONE: {len(results)} resources")

    # Merge labels into state.
    state.label_queue.merge_into_state(state, results)

    # Save labels to storage.
    for result in results:
        await _save_labels_to_storage(context, result)


async def _save_labels_to_storage(
    context: KnowledgeContext,
    result: LabelResult,
) -> None:
    """
    Persist generated labels to the resource history in storage.

    Labels are saved as a new delta, so they're available on future requests.
    """
    all_labels = result.all_labels()
    if not all_labels:
        return

    resource_uri = result.resource_uri
    history = await read_resource_history(context, resource_uri)
    if not history:
        # Resource not in storage - labels will only be in memory.
        return

    # Create a delta with just the new labels.
    delta = ResourceDelta(
        refreshed_at=datetime.now(UTC),
        labels=all_labels,
        reset_labels=list(result.request.reset_labels),
    )

    # Update and save.
    if history.update(delta):
        await save_resource_history(context, history)


async def _generate_labels_for_request(
    context: KnowledgeContext,
    _resource_uri: ResourceUri,
    request: LabelRequest,
) -> list[ResourceLabel]:
    """
    Generate labels for all bundles in a request.

    Uses the effective cache (excluding labels marked for reset).
    Processes all bundles for the resource together.
    """
    # Use effective cache (excludes reset_labels).
    effective_cache = request.effective_cache()

    # Generate labels for all bundles.
    all_labels: list[ResourceLabel] = []
    for bundle in request.bundles:
        labels = await generate_standard_labels(
            context=context,
            cached=effective_cache,
            bundle=bundle,
        )
        for label in labels:
            bisect_insert(all_labels, label, key=ResourceLabel.sort_key)
        # Update effective cache with newly generated labels.
        effective_cache = [*effective_cache, *labels]

    return all_labels
