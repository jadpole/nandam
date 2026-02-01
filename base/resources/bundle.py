from pydantic import BaseModel, Field
from typing import Literal, overload

from base.core.exceptions import ErrorInfo
from base.resources.aff_body import (
    AffBody,
    AffBodyChunk,
    AffBodyMedia,
    ObsBody,
    ObsChunk,
    ObsMedia,
)
from base.resources.aff_collection import AffCollection, ObsCollection
from base.resources.aff_file import AffFile, ObsFile
from base.resources.aff_plain import AffPlain, ObsPlain
from base.resources.label import ResourceLabel, ResourceLabels
from base.resources.metadata import (
    AffordanceInfo,
    AffordanceInfo_,
    ResourceAttrs_,
    ResourceAttrsUpdate_,
    ResourceAttrsUpdate,
    ResourceInfo,
)
from base.resources.observation import Observation, Observation_
from base.resources.relation import Relation_
from base.strings.auth import ServiceId
from base.strings.resource import (
    AffordanceUri,
    ExternalUri,
    KnowledgeUri,
    ObservableUri,
    Reference,
    ResourceUri,
)
from base.utils.sorted_list import bisect_find, bisect_insert, bisect_make


class Resource(BaseModel, frozen=True):
    kind: Literal["resource"] = "resource"
    uri: ResourceUri
    owner: ServiceId
    attributes: ResourceAttrs_
    aliases: list[ExternalUri] = Field(default_factory=list)
    affordances: list[AffordanceInfo_] = Field(default_factory=list)
    labels: list[ResourceLabel] = Field(default_factory=list)
    relations: list[Relation_] | None = None

    @staticmethod
    def new(
        *,
        uri: ResourceUri,
        owner: ServiceId,
        attributes: ResourceAttrs_,
        aliases: list[ExternalUri],
        affordances: list[AffordanceInfo_],
        labels: ResourceLabels,
        relations: list[Relation_] | None,
    ) -> "Resource":
        if not attributes.description and (
            value := labels.get_str("description", [AffBody.new()])
        ):
            attributes = attributes.model_copy(update={"description": value})

        return Resource(
            uri=uri,
            owner=owner,
            attributes=attributes,
            aliases=aliases,
            affordances=[aff.with_labels(labels) for aff in affordances],
            labels=labels.as_list(),
            relations=relations,
        )

    def info(self) -> ResourceInfo:
        return ResourceInfo(
            uri=self.uri,
            attributes=self.attributes,
            aliases=self.aliases,
            affordances=self.affordances,
        )


class ResourceUpdate(BaseModel, frozen=True):
    """
    - `attributes` replace only the affected fields.
    - `aliases` are added (union), preserving ordering.
    - `affordances` are completely replaced when given.
    - `labels` are added (union), preserving ordering.
    - `relations` are completely replaced when given.
    """

    attributes: ResourceAttrsUpdate_
    aliases: list[ExternalUri] | None = None
    affordances: list[AffordanceInfo_] | None = None
    labels: list[ResourceLabel] | None = None
    relations: list[Relation_] | None = None

    @staticmethod
    def diff(after: Resource, before: Resource) -> "ResourceUpdate":
        return ResourceUpdate(
            attributes=ResourceAttrsUpdate.diff(after.attributes, before.attributes),
            aliases=[alias for alias in after.aliases if alias not in before.aliases],
            affordances=(
                after.affordances
                if after.affordances is not None
                and after.affordances != before.affordances
                else None
            ),
            labels=[label for label in after.labels if label not in before.labels],
            relations=(
                after.relations
                if after.relations is not None and after.relations != before.relations
                else None
            ),
        )

    def apply(self, value: Resource) -> Resource:
        return Resource(
            uri=value.uri,
            owner=value.owner,
            attributes=self.attributes.apply(value.attributes),
            aliases=(
                bisect_make([*self.aliases, *value.aliases], key=str)
                if self.aliases
                else value.aliases
            ),
            affordances=(
                self.affordances if self.affordances is not None else value.affordances
            ),
            labels=(
                bisect_make([*self.labels, *value.labels], key=str)
                if self.labels
                else value.labels
            ),
            relations=self.relations if self.relations is not None else value.relations,
        )

    def is_empty(self) -> bool:
        return (
            self.attributes.is_empty()
            and not self.aliases
            and self.affordances is None
            and not self.labels
            and self.relations is None
        )


class ResourceError(BaseModel, frozen=True):
    kind: Literal["error"] = "error"
    uri: ResourceUri
    error: ErrorInfo


class ObservationError(BaseModel, frozen=True):
    kind: Literal["error"] = "error"
    uri: ObservableUri
    error: ErrorInfo


class Resources(BaseModel):
    resources: list[Resource | ResourceError] = Field(default_factory=list)
    observations: list[Observation_ | ObservationError] = Field(default_factory=list)

    ##
    ## Construction
    ##

    def update(
        self,
        *,
        resources: list[Resource | ResourceError] | None = None,
        observations: list[Observation | ObservationError] | None = None,
    ) -> None:
        """
        Add the resource to `resources` and the observations to `observations`.

        - When a resource already exists, merge its metadata.
        - When observations already exists for the updated root, discard all
          existing children (in case, e.g., the structure of "$body" changed).
        """
        for resource in resources or []:
            if (
                (existing := self.get_resource(resource.uri))
                and isinstance(resource, Resource)
                and isinstance(existing, Resource)
            ):
                merged = Resource(
                    uri=resource.uri,
                    owner=resource.owner,
                    attributes=ResourceAttrsUpdate.full(resource.attributes).apply(
                        existing.attributes
                    ),
                    aliases=bisect_make(
                        [*existing.aliases, *resource.aliases],
                        key=str,
                    ),
                    affordances=resource.affordances,
                    relations=(
                        resource.relations
                        if resource.relations is not None
                        else existing.relations
                    ),
                )
                bisect_insert(self.resources, merged, lambda r: str(r.uri))
            else:
                bisect_insert(self.resources, resource, lambda r: str(r.uri))

        if observations:
            replaced_roots = [
                observation.uri.root_observable_uri() for observation in observations
            ]
            new_observations = [
                observation
                for observation in self.observations
                if observation.uri.root_observable_uri() not in replaced_roots
            ]
            for observation in observations:
                bisect_insert(new_observations, observation, lambda o: str(o.uri))
            self.observations = new_observations

    ##
    ## Query
    ##

    def infer_resource_uri(self, reference: Reference) -> ResourceUri | None:
        if isinstance(reference, ExternalUri):
            return self.get_alias(reference)
        elif isinstance(reference, KnowledgeUri):
            return reference.resource_uri()
        else:
            return None  # Unreachable.

    def infer_knowledge_uri(self, reference: Reference) -> KnowledgeUri | None:
        if isinstance(reference, ExternalUri):
            return self.get_alias(reference)
        else:
            assert isinstance(reference, KnowledgeUri)
            return reference

    def get_alias(self, reference: ExternalUri) -> ResourceUri | None:
        for resource in self.resources:
            if isinstance(resource, Resource) and bisect_find(
                resource.aliases, str(reference), lambda a: str(a)
            ):
                return resource.uri
        return None

    def get_error(
        self,
        reference: Reference,
    ) -> ResourceError | ObservationError | None:
        if not (uri := self.infer_knowledge_uri(reference)):
            return None

        # Resource-level errors.
        resource = self.get_resource_or_error(uri.resource_uri())
        if not resource:
            return None  # No resource -> no observations either.
        if isinstance(resource, ResourceError):
            return resource
        if isinstance(uri, ResourceUri):
            return None

        # Observation-level errors.
        observation = self.get_observation_or_error(uri)
        if observation and isinstance(observation, ObservationError):
            return observation
        else:
            return None

    def get_resource(self, uri: ResourceUri) -> Resource | None:
        result = self.get_resource_or_error(uri)
        return result if isinstance(result, Resource) else None

    def get_resource_or_error(
        self,
        uri: ResourceUri,
    ) -> Resource | ResourceError | None:
        return bisect_find(self.resources, str(uri), lambda r: str(r.uri))

    def get_affordance(self, uri: AffordanceUri) -> AffordanceInfo | None:
        if resource := self.get_resource(uri.resource_uri()):
            return resource.info().get_affordance(uri.suffix)
        else:
            return None

    # fmt: off
    @overload
    def get_observation(self, uri: ResourceUri) -> ObsBody | ObsCollection | ObsPlain | None: ...
    @overload
    def get_observation(self, uri: ObservableUri[AffBody]) -> ObsBody | None: ...
    @overload
    def get_observation(self, uri: ObservableUri[AffBodyChunk]) -> ObsChunk | None: ...
    @overload
    def get_observation(self, uri: ObservableUri[AffBodyMedia]) -> ObsMedia | None: ...
    @overload
    def get_observation(self, uri: ObservableUri[AffCollection]) -> ObsCollection | None: ...
    @overload
    def get_observation(self, uri: ObservableUri[AffFile]) -> ObsFile | None: ...
    @overload
    def get_observation(self, uri: ObservableUri[AffPlain]) -> ObsPlain | None: ...
    @overload
    def get_observation(self, uri: KnowledgeUri) -> Observation | None: ...
    # fmt: on

    def get_observation(self, uri: KnowledgeUri) -> Observation | None:
        observation = self.get_observation_or_error(uri)
        return observation if isinstance(observation, Observation) else None

    def get_observation_or_error(
        self,
        uri: KnowledgeUri,
    ) -> Observation | ObservationError | None:
        # NOTE: Also covers `AffordanceUri` with an `Observable` suffix, since
        # they are converted into the same string.
        if isinstance(uri, AffordanceUri | ObservableUri):
            return bisect_find(self.observations, str(uri), lambda obs: str(obs.uri))

        # When a `ResourceUri` is embedded directly, pick the "best" default.
        # This occurs, for example, when an External URI is embedded.
        if (
            not isinstance(uri, ResourceUri)
            or not (resource := self.get_resource(uri))
            or not (affordances := resource.info().affordances)
        ):
            return None

        # NOTE: If the resource supports a "preferred" affordance (info exists),
        # but the observation has not been loaded, then return None rather than
        # continuing to the next fallback.
        defaults: list[tuple[str, str]] = [
            ("self://$body", "$body"),
            ("self://$plain", "$plain"),
            ("self://$collection", "$collection"),
        ]
        for default_aff, obs_suffix in defaults:
            if bisect_find(affordances, default_aff, lambda aff: str(aff.suffix)):
                return bisect_find(
                    self.observations, f"{uri}/{obs_suffix}", lambda obs: str(obs.uri)
                )

        return None
