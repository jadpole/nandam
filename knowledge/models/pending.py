import asyncio

from collections.abc import Callable, Coroutine  # noqa: TC003
from dataclasses import dataclass, field
from typing import Any, Literal

from base.core.exceptions import ErrorInfo
from base.resources.action import (
    LoadMode,
    QueryAction,
    ResourcesAttachmentAction,
    ResourcesLoadAction,
    max_load_mode,
)
from base.resources.bundle import ObservationError, Resource, ResourceError, Resources
from base.resources.label import LabelName, ResourceLabel, ResourceLabels
from base.resources.metadata import ResourceInfo
from base.resources.observation import Observation
from base.resources.relation import Relation, RelationId
from base.strings.auth import ServiceId
from base.strings.resource import ExternalUri, Observable, ResourceUri, RootReference
from base.utils.sorted_list import bisect_insert

from knowledge.models.storage_metadata import Locator
from knowledge.models.storage_observed import AnyBundle, BundleBody
from knowledge.server.context import KnowledgeContext


@dataclass(kw_only=True)
class Dependency:
    kind: Literal["collection", "embed", "link"]
    origin: ResourceUri


PendingReason = QueryAction | RelationId | Dependency
PendingBuild = tuple[Resource | ResourceError, list[AnyBundle], list[ObservationError]]


@dataclass(kw_only=True)
class PendingResult:
    # Request:
    locator: Locator
    reason: list[PendingReason]
    request_expand_depth: int
    request_expand_mode: LoadMode
    request_load_mode: LoadMode
    request_observe: list[Observable]

    # Results:
    resource: ResourceInfo | ResourceError | None
    relations_depth: int
    observed: list[AnyBundle | ObservationError]
    labels: ResourceLabels

    @staticmethod
    def new(locator: Locator) -> "PendingResult":
        return PendingResult(
            locator=locator,
            reason=[],
            request_expand_depth=0,
            request_expand_mode="none",
            request_load_mode="none",
            request_observe=[],
            resource=None,
            relations_depth=0,
            observed=[],
            labels=ResourceLabels.new(),
        )

    def update(
        self,
        reason: PendingReason | None = None,
        request_expand_depth: int = 0,
        request_expand_mode: LoadMode = "none",
        request_load_mode: LoadMode = "none",
        request_observe: list[Observable] | None = None,
        resource: ResourceInfo | ResourceError | None = None,
        relations_depth: int = 0,
        observed: list[AnyBundle | ObservationError] | None = None,
        labels: list[ResourceLabel] | None = None,
    ) -> None:
        # Request:
        if reason:
            self.reason.append(reason)
        self.request_expand_depth = max(self.request_expand_depth, request_expand_depth)
        self.request_expand_mode = max_load_mode(
            self.request_expand_mode, request_expand_mode
        )
        self.request_load_mode = max_load_mode(
            self.request_load_mode, request_load_mode
        )
        if request_observe:
            for aff in request_observe:
                bisect_insert(self.request_observe, aff.root(), key=str)

        # Result:
        if resource:
            self.resource = resource
        self.relations_depth = max(self.relations_depth, relations_depth)
        if observed:
            self.observed.extend(observed)
        if labels:
            self.labels.extend(labels)

    def update_from_action(self, action: QueryAction) -> None:
        if isinstance(action, ResourcesAttachmentAction):
            pass  # No extra reads requirements for attachments.
        elif isinstance(action, ResourcesLoadAction):
            self.update(
                request_expand_depth=action.expand_depth,
                request_expand_mode=action.expand_mode,
                request_load_mode=action.load_mode,
                request_observe=action.observe,
            )

    def add_action(self, action: QueryAction) -> None:
        self.update_from_action(action)
        self.reason.append(action)

    def missing_observe(self) -> list[Observable] | None:
        if isinstance(self.resource, ResourceError):
            return None

        # NOTE: Since `ObservationBundle` and `ObservationError` use URIs with
        # suffix `Affordance` and `Observable`, respectively, we check both.
        missing_observe = [
            c
            for c in self.request_observe
            if (aff := c.affordance())
            and not any(obs.uri.suffix in (c, aff) for obs in self.observed)
        ]
        if missing_observe:
            return missing_observe
        elif not self.resource or self.relations_depth < self.request_expand_depth:
            return []
        else:
            return None

    def sort_key(self) -> tuple[Any, ...]:
        """
        In `RequestStatus.next_batch`, used to pick the next batch of reads
        with a REVERSED sort.  The greatest value is read first.

        - Critically, start with the greatest "expand_depth", expanding the
          relations graph breadth-first, and only once the graph was fully
          expanded do we read the remaining linked and embedded resources.
        - Then, start with resources that may cause a refresh.
        - Finally, read the resource URIs in reverse alphabetical order.
          This serves no purpose except consistency.
        """
        return (
            self.request_expand_depth,
            self.request_load_mode != "none",
            self.locator.resource_uri(),
        )


@dataclass(kw_only=True)
class PendingState:
    results: dict[ResourceUri, PendingResult]
    relations: list[Relation]
    locator_unavailable: list[RootReference]
    label_queue: "LabelQueue"

    @staticmethod
    def new() -> "PendingState":
        return PendingState(
            locator_unavailable=[],
            relations=[],
            results={},
            label_queue=LabelQueue(),
        )

    def result(self, locator: Locator) -> PendingResult:
        resource_uri = locator.resource_uri()
        if resource_uri not in self.results:
            self.results[resource_uri] = PendingResult.new(locator)
        return self.results[resource_uri]

    def add_action(self, locator: Locator, action: QueryAction) -> None:
        self.result(locator).add_action(action)

    def add_error(self, locator: Locator, error: ErrorInfo) -> None:
        self.result(locator).update(
            resource=ResourceError(uri=locator.resource_uri(), error=error),
        )

    def add_locator_unavailable(self, uri: RootReference) -> None:
        bisect_insert(self.locator_unavailable, uri, key=str)

    def next_batch(self, batch_size: int) -> list[PendingResult]:
        next_reads: list[PendingResult] = sorted(
            [
                pending_read
                for pending_read in self.results.values()
                if pending_read.missing_observe() is not None
            ],
            key=lambda r: r.sort_key(),
            reverse=True,
        )
        return next_reads[:batch_size]

    def build_one(
        self,
        context: KnowledgeContext,
        resource_uri: ResourceUri,
    ) -> PendingBuild | None:
        if (
            not (pending := self.results.get(resource_uri))
            or not pending.resource
            or not context.filters.matches(resource_uri)
            or not context.filters.satisfied_by(pending.labels.as_list())
        ):
            return None

        if isinstance(pending.resource, ResourceError):
            return pending.resource, [], []

        aliases = pending.resource.aliases.copy()
        for reason in pending.reason:
            if isinstance(reason, QueryAction) and isinstance(reason.uri, ExternalUri):
                bisect_insert(aliases, reason.uri, key=str)

        resource = Resource.new(
            uri=resource_uri,
            owner=ServiceId.decode("svc-knowledge"),
            attributes=pending.resource.attributes,
            aliases=aliases,
            affordances=pending.resource.affordances,
            labels=pending.labels,
            relations=(
                [
                    relation
                    for relation in self.relations
                    if resource_uri in relation.get_nodes()
                ]
                if pending.request_expand_depth > 0
                else None
            ),
        )

        bundles: list[AnyBundle] = []
        bundle_errors: list[ObservationError] = []
        for bundle in pending.observed:
            if bundle.uri.suffix not in pending.request_observe:
                continue
            if isinstance(bundle, ObservationError):
                bundle_errors.append(bundle)
            else:
                bundles.append(bundle)

        return resource, bundles, bundle_errors

    def build_all(
        self,
        context: KnowledgeContext,
    ) -> list[PendingBuild]:
        return [
            build
            for resource_uri in self.results
            if (build := self.build_one(context, resource_uri))
        ]

    def into_resources(self, context: KnowledgeContext) -> Resources:
        # TODO: ResourceError.uri: RemoteUri
        # for uri in self.locator_unavailable:
        #     resources.update(
        #         resources=[
        #             ResourceError(uri=uri, error=UnavailableError.new().as_info())
        #         ]
        #     )

        resources: list[Resource | ResourceError] = []
        observations: list[Observation | ObservationError] = []

        for resource_uri in self.results:
            if not (build := self.build_one(context, resource_uri)):
                continue

            # TODO: Aliases from the request.
            # if isinstance(status.request.uri, ExternalUri):
            #     resources.add_alias(resource_uri, status.request.uri)

            resource, bundles, bundle_errors = build
            resources.append(resource)
            if isinstance(resource, ResourceError):
                continue

            resource_labels = ResourceLabels.new(resource.labels)
            observations.extend(bundle_errors)
            observations.extend(
                obs.with_labels(resource_labels)
                for bundle in bundles
                for obs in bundle.observations()
            )

        result = Resources()
        result.update(resources=resources, observations=observations)
        return result


##
## Label Queue
##


@dataclass(kw_only=True)
class LabelRequest:
    """
    Label generation request for a single resource.

    Groups all bundles from the same resource together for efficient processing.
    Tracks which labels to reset (regenerate even if cached) vs. which were
    provided by the connector (use directly without generation).
    """

    bundles: list[BundleBody] = field(default_factory=list)
    cached_labels: list[ResourceLabel] = field(default_factory=list)
    reset_labels: set[LabelName] = field(default_factory=set)
    provided_labels: list[ResourceLabel] = field(default_factory=list)

    def effective_cache(self) -> list[ResourceLabel]:
        """
        Return cached labels, excluding those marked for reset.
        Labels to reset should be regenerated even when cached.
        """
        if not self.reset_labels:
            return self.cached_labels
        return [
            label for label in self.cached_labels if label.name not in self.reset_labels
        ]

    def all_labels(self, generated: list[ResourceLabel]) -> list[ResourceLabel]:
        """
        Combine provided and generated labels, sorted by key.
        Provided labels take precedence (are listed first for same key).
        """
        all_labels: list[ResourceLabel] = self.provided_labels.copy()
        for label in generated:
            bisect_insert(all_labels, label, key=ResourceLabel.sort_key)
        return all_labels


@dataclass(kw_only=True)
class LabelResult:
    """Result of label generation for a single resource."""

    resource_uri: ResourceUri
    request: LabelRequest
    generated: list[ResourceLabel]

    def all_labels(self) -> list[ResourceLabel]:
        """Combine provided and generated labels."""
        return self.request.all_labels(self.generated)


RunningTask = tuple[ResourceUri, LabelRequest, asyncio.Task[list[ResourceLabel]]]


@dataclass(kw_only=True)
class LabelQueue:
    """
    Queue for parallel label generation.

    Supports concurrent processing: label generation tasks run in parallel
    with batch processing.  The queue tracks:
    - `pending`: Requests waiting to be started
    - `running`: Active asyncio tasks
    - `completed`: Finished results ready to be saved/merged
    """

    pending: dict[ResourceUri, LabelRequest] = field(default_factory=dict)
    running: list[RunningTask] = field(default_factory=list)
    completed: list[LabelResult] = field(default_factory=list)

    def enqueue(
        self,
        resource_uri: ResourceUri,
        bundle: BundleBody,
        cached_labels: list[ResourceLabel],
        reset_labels: list[LabelName],
        provided_labels: list[ResourceLabel],
    ) -> None:
        """
        Add or merge a bundle into the pending queue.

        Multiple bundles from the same resource are grouped together.
        Reset labels and provided labels are accumulated.
        """
        if resource_uri in self.pending:
            req = self.pending[resource_uri]
            req.bundles.append(bundle)
            req.reset_labels.update(reset_labels)
            for label in provided_labels:
                bisect_insert(req.provided_labels, label, key=ResourceLabel.sort_key)
        else:
            self.pending[resource_uri] = LabelRequest(
                bundles=[bundle],
                cached_labels=cached_labels.copy(),
                reset_labels=set(reset_labels),
                provided_labels=sorted(provided_labels, key=ResourceLabel.sort_key),
            )

    def has_work(self) -> bool:
        """Check if there's any pending or running work."""
        return bool(self.pending) or bool(self.running)

    def has_completed(self) -> bool:
        """Check if there are completed results to process."""
        return bool(self.completed)

    def start_pending(
        self,
        generate_fn: "Callable[[ResourceUri, LabelRequest], Coroutine[Any, Any, list[ResourceLabel]]]",
    ) -> None:
        """
        Start async tasks for all pending requests.

        Moves requests from `pending` to `running`.
        """
        for resource_uri, request in self.pending.items():
            task = asyncio.create_task(generate_fn(resource_uri, request))
            self.running.append((resource_uri, request, task))
        self.pending.clear()

    def collect_completed(self) -> None:
        """
        Check running tasks and move completed ones to `completed`.

        Non-blocking: only collects tasks that have already finished.
        """
        still_running: list[RunningTask] = []

        for resource_uri, request, task in self.running:
            if task.done():
                try:
                    generated = task.result()
                    self.completed.append(
                        LabelResult(
                            resource_uri=resource_uri,
                            request=request,
                            generated=generated,
                        )
                    )
                except Exception:
                    # Log but don't fail - provided_labels still apply.
                    self.completed.append(
                        LabelResult(
                            resource_uri=resource_uri,
                            request=request,
                            generated=[],
                        )
                    )
            else:
                still_running.append((resource_uri, request, task))

        self.running = still_running

    async def wait_all(self) -> None:
        """
        Wait for all running tasks to complete.

        After this call, all results are in `completed`.
        """
        if not self.running:
            return

        tasks = [task for _, _, task in self.running]
        await asyncio.gather(*tasks, return_exceptions=True)
        self.collect_completed()

    def take_completed(self) -> list[LabelResult]:
        """Take all completed results for processing."""
        results = self.completed
        self.completed = []
        return results

    def merge_into_state(
        self,
        state: "PendingState",
        results: list[LabelResult],
    ) -> None:
        """
        Merge label results into the pending state.

        For each result:
        1. Add provided labels (from connector)
        2. Add generated labels (from LLM inference)
        """
        for result in results:
            if pending := state.results.get(result.resource_uri):
                pending.labels.extend(result.all_labels())
