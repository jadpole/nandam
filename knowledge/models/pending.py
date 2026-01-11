from dataclasses import dataclass
from typing import Any, Literal, Never

from base.core.exceptions import ErrorInfo
from base.resources.action import (
    LoadMode,
    QueryAction,
    ResourcesAttachmentAction,
    ResourcesLoadAction,
    ResourcesObserveAction,
    max_load_mode,
)
from base.resources.bundle import ObservationError, Resource, ResourceError, Resources
from base.resources.metadata import ResourceInfo
from base.resources.observation import Observation, ObservationBundle
from base.resources.relation import Relation, RelationId
from base.strings.auth import ServiceId
from base.strings.resource import ExternalUri, Observable, ResourceUri, RootReference
from base.utils.sorted_list import bisect_insert

from knowledge.models.storage import Locator


@dataclass(kw_only=True)
class Dependency:
    kind: Literal["collection", "embed", "link"]
    origin: ResourceUri


PendingReason = QueryAction | RelationId | Dependency


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
    observed: list[ObservationBundle | ObservationError]

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
        observed: list[ObservationBundle | ObservationError] | None = None,
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
        elif isinstance(action, ResourcesObserveAction):
            self.update(request_observe=[action.uri.suffix])
        else:
            _: Never = action
            raise NotImplementedError(f"unreachable: unknown action: {action.method}")

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

    @staticmethod
    def new() -> "PendingState":
        return PendingState(locator_unavailable=[], relations=[], results={})

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

    def into_resources(self) -> Resources:
        # TODO: ResourceError.uri: RemoteUri
        # for uri in self.locator_unavailable:
        #     resources.update(
        #         resources=[
        #             ResourceError(uri=uri, error=UnavailableError.new().as_info())
        #         ]
        #     )

        resources: list[Resource | ResourceError] = []
        observations: list[Observation | ObservationError] = []

        for resource_uri, pending in self.results.items():
            if not pending.resource:
                continue

            # TODO: Aliases from the request.
            # if isinstance(status.request.uri, ExternalUri):
            #     resources.add_alias(resource_uri, status.request.uri)

            if isinstance(pending.resource, ResourceError):
                resources.append(pending.resource)
                continue

            aliases = pending.resource.aliases.copy()
            for reason in pending.reason:
                if isinstance(reason, QueryAction) and isinstance(
                    reason.uri, ExternalUri
                ):
                    bisect_insert(aliases, reason.uri, key=str)

            # TODO: Keep only those that appear in `request_observe`.
            resources.append(
                Resource(
                    uri=resource_uri,
                    owner=ServiceId.decode("svc-knowledge"),
                    attributes=pending.resource.attributes,
                    aliases=aliases,
                    affordances=pending.resource.affordances,
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
            )

            for observed in pending.observed:
                if observed.uri.suffix not in pending.request_observe:
                    continue
                if isinstance(observed, ObservationBundle):
                    observations.extend(observed.observations())
                elif isinstance(observed, ObservationError):
                    observations.append(observed)

        result = Resources()
        result.update(resources=resources, observations=observations)
        return result
