from datetime import datetime
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    SerializeAsAny,
    WrapSerializer,
)
from typing import Annotated, Any

from base.core.unions import ModelUnion
from base.core.values import wrap_exclude_none
from base.resources.label import ResourceLabel
from base.resources.metadata import (
    AffordanceInfo,
    AffordanceInfo_,
    ObservationInfo_,
    ObservationSection,
    ResourceAttrs,
)
from base.resources.relation import Relation, Relation_
from base.strings.data import MimeType
from base.strings.resource import ExternalUri, Observable, Realm, ResourceUri, WebUrl
from base.utils.sorted_list import bisect_find, bisect_insert, bisect_make

from knowledge.models.exceptions import IngestionError


##
## Locator
##


class Locator(ModelUnion, frozen=True):
    model_config = ConfigDict(extra="allow", frozen=True)

    realm: Realm

    def resource_uri(self) -> ResourceUri:
        raise NotImplementedError("Subclasses must implement Locator.resource_uri")

    def content_uri(self) -> ExternalUri | None:
        return None

    def citation_url(self) -> WebUrl | None:
        return None


Locator_ = SerializeAsAny[Locator]


##
## History
##


class MetadataDelta(BaseModel, frozen=True):
    # ResourceInfo.attributes
    name: str | None = None
    mime_type: MimeType | None = None
    description: str | None = None
    citation_url: WebUrl | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revision_data: str | None = None
    revision_meta: str | None = None

    # Resource
    aliases: list[ExternalUri] | None = None
    affordances: list[AffordanceInfo_] | None = None
    relations: list[Relation_] | None = None

    def diff(self, before: "MetadataDelta") -> "MetadataDelta":
        return MetadataDelta(
            name=(
                self.name
                if self.name is not None and self.name != before.name
                else None
            ),
            mime_type=(
                self.mime_type
                if self.mime_type is not None and self.mime_type != before.mime_type
                else None
            ),
            description=(
                self.description
                if self.description is not None
                and self.description != before.description
                else None
            ),
            citation_url=(
                self.citation_url
                if self.citation_url is not None
                and self.citation_url != before.citation_url
                else None
            ),
            created_at=(
                self.created_at
                if self.created_at is not None and self.created_at != before.created_at
                else None
            ),
            updated_at=(
                self.updated_at
                if self.updated_at is not None and self.updated_at != before.updated_at
                else None
            ),
            revision_data=(
                self.revision_data
                if self.revision_data is not None
                and self.revision_data != before.revision_data
                else None
            ),
            revision_meta=(
                self.revision_meta
                if self.revision_meta is not None
                and self.revision_meta != before.revision_meta
                else None
            ),
            aliases=(
                self.aliases
                if self.aliases is not None and self.aliases != before.aliases
                else None
            ),
            affordances=(
                self.affordances
                if self.affordances is not None
                and self.affordances != before.affordances
                else None
            ),
            relations=(
                self.relations
                if self.relations is not None and self.relations != before.relations
                else None
            ),
        )

    def is_empty(self) -> bool:
        return (
            self.name is None
            and self.mime_type is None
            and self.description is None
            and self.citation_url is None
            and self.created_at is None
            and self.updated_at is None
            and self.revision_data is None
            and self.revision_meta is None
            and self.aliases is None
            and self.affordances is None
            and self.relations is None
        )

    def with_update(self, delta: "MetadataDelta") -> "MetadataDelta":
        return MetadataDelta(
            name=delta.name if delta.name is not None else self.name,
            mime_type=(
                delta.mime_type if delta.mime_type is not None else self.mime_type
            ),
            description=(
                delta.description if delta.description is not None else self.description
            ),
            citation_url=(
                delta.citation_url
                if delta.citation_url is not None
                else self.citation_url
            ),
            created_at=(
                delta.created_at if delta.created_at is not None else self.created_at
            ),
            updated_at=(
                delta.updated_at if delta.updated_at is not None else self.updated_at
            ),
            revision_data=(
                delta.revision_data
                if delta.revision_data is not None
                else self.revision_data
            ),
            revision_meta=(
                delta.revision_meta
                if delta.revision_meta is not None
                else self.revision_meta
            ),
            aliases=delta.aliases if delta.aliases is not None else self.aliases,
            affordances=(
                delta.affordances if delta.affordances is not None else self.affordances
            ),
            relations=(
                delta.relations if delta.relations is not None else self.relations
            ),
        )


class ObservedDelta(BaseModel, frozen=True):
    suffix: Observable
    info_mime_type: MimeType | None = None
    info_observations: list[ObservationInfo_] | None = None
    info_sections: list[ObservationSection] | None = None
    relations: list[Relation_] | None = None

    def diff(self, before: "ObservedDelta") -> "ObservedDelta":
        assert self.suffix == before.suffix
        return ObservedDelta(
            suffix=self.suffix,
            info_mime_type=(
                self.info_mime_type
                if self.info_mime_type is not None
                and self.info_mime_type != before.info_mime_type
                else None
            ),
            info_observations=(
                self.info_observations
                if self.info_observations is not None
                and self.info_observations != before.info_observations
                else None
            ),
            info_sections=(
                self.info_sections
                if self.info_sections is not None
                and self.info_sections != before.info_sections
                else None
            ),
            relations=(
                self.relations
                if self.relations is not None and self.relations != before.relations
                else None
            ),
        )

    def is_empty(self) -> bool:
        return (
            self.info_mime_type is None
            and self.info_observations is None
            and self.info_sections is None
            and self.relations is None
        )

    def with_update(self, delta: "ObservedDelta") -> "ObservedDelta":
        return ObservedDelta(
            suffix=delta.suffix,
            info_mime_type=(
                delta.info_mime_type
                if delta.info_mime_type is not None
                else self.info_mime_type
            ),
            info_observations=(
                delta.info_observations
                if delta.info_observations is not None
                else self.info_observations
            ),
            info_sections=(
                delta.info_sections
                if delta.info_sections is not None
                else self.info_sections
            ),
            relations=(
                delta.relations if delta.relations is not None else self.relations
            ),
        )


MetadataDelta_ = Annotated[MetadataDelta, WrapSerializer(wrap_exclude_none)]
ObservedDelta_ = Annotated[ObservedDelta, WrapSerializer(wrap_exclude_none)]


class ResourceDelta(BaseModel, frozen=True):
    refreshed_at: datetime
    locator: Locator_ | None = None
    """
    The locator of the resource.  Note that it will typically only be set when
    the resource is resolved for the first time, however, we support changes in
    the locator for the same resource URI.
    """
    expired: list[Observable] = Field(default_factory=list)
    """
    The root observations whose cache were expired by `Connector.resolve`, and
    should therefore be refreshed on the next read.  If this hasn't happened in
    the current request, then they are flagged here.
    """
    labels: list[ResourceLabel] = Field(default_factory=list)
    """
    The resource labels whose value was changed during ingestion.
    """
    metadata: MetadataDelta_ = Field(default_factory=MetadataDelta)
    """
    The resource attributes that were changed by `Connector.resolve` or `read`.
    """
    observed: list[ObservedDelta_] = Field(default_factory=list)
    """
    The root observations that were refreshed by `Connector.observe`.
    """
    reset_labels: bool = False
    """
    True when the structure of the "$body" has changed, such that every previous
    label value is no longer meaningful.
    """

    def is_empty(self) -> bool:
        return (
            self.locator is None
            and not self.expired
            and not self.labels
            and self.metadata.is_empty()
            and (
                not self.observed
                or all(observed.is_empty() for observed in self.observed)
            )
        )


class ResourceHistory(BaseModel):
    history: list[ResourceDelta]
    _cached: "ResourceView | None" = PrivateAttr(default=None)

    def update(self, delta: ResourceDelta) -> bool:
        if not self.history:
            if not delta.locator:
                raise IngestionError("missing locator in resource initialization")
            self.history.append(delta)
            return True

        delta = self.diff(delta)
        if not delta.is_empty():
            self.history.append(delta)
            self._cached = None
            return True
        else:
            return False

    def diff(self, delta: ResourceDelta) -> ResourceDelta:
        merged = self.merged()

        new_locator = (
            delta.locator if delta.locator and delta.locator != merged.locator else None
        )
        new_labels: list[ResourceLabel] = []
        for label in delta.labels:
            if (
                not (old_value := merged.get_label(label.name, label.target))
                or old_value != label.value
            ):
                bisect_insert(new_labels, label, key=ResourceLabel.sort_key)

        new_metadata = delta.metadata.diff(merged.metadata)
        new_expired = set(merged.expired)
        new_expired.update(delta.expired)
        new_observed: list[ObservedDelta] = []

        for obs_delta in delta.observed:
            new_expired.discard(obs_delta.suffix)
            if existing := next(
                (obs for obs in delta.observed if obs.suffix == obs_delta.suffix),
                None,
            ):
                new_obs_delta = obs_delta.diff(existing)
                if not new_obs_delta.is_empty():
                    bisect_insert(
                        new_observed,
                        new_obs_delta,
                        key=lambda x: str(x.suffix),
                    )
            else:
                bisect_insert(new_observed, obs_delta, key=lambda x: str(x.suffix))

        return ResourceDelta(
            refreshed_at=delta.refreshed_at,
            locator=new_locator,
            expired=sorted(new_expired, key=str),
            labels=new_labels,
            metadata=new_metadata,
            observed=new_observed,
        )

    ##
    ## Merged
    ##

    def all_affordances(self) -> list[AffordanceInfo]:
        merged = self.merged()
        affordances: list[AffordanceInfo] = []

        for affordance_info in merged.metadata.affordances or []:
            bisect_insert(affordances, affordance_info, key=lambda a: str(a.suffix))

        for observed in merged.observed:
            existing = (
                # fmt: off
                bisect_find(
                    affordances, str(observed.suffix), key=lambda info: str(info.suffix)
                )
                or AffordanceInfo(suffix=observed.suffix.affordance())
            )
            affordance_info = AffordanceInfo(
                suffix=observed.suffix.affordance(),
                mime_type=observed.info_mime_type or existing.mime_type,
                sections=observed.info_sections or existing.sections,
                observations=observed.info_observations or existing.observations,
            )
            # TODO:
            # if not affordance_info.description and (
            #     description := merged.get_label("description", observed.suffix)
            # ):
            #     description = None
            bisect_insert(affordances, affordance_info, key=lambda a: str(a.suffix))

        return affordances

    def all_aliases(self) -> list[ExternalUri]:
        return self.merged().metadata.aliases or []

    def all_attributes(self) -> ResourceAttrs:
        merged = self.merged()
        return ResourceAttrs(
            name=merged.metadata.name or merged.locator.resource_uri().path[-1],
            mime_type=merged.metadata.mime_type,
            description=merged.metadata.description,
            citation_url=merged.metadata.citation_url or merged.locator.citation_url(),
            created_at=merged.metadata.created_at,
            updated_at=merged.metadata.updated_at,
            revision_data=merged.metadata.revision_data,
            revision_meta=merged.metadata.revision_meta,
        )

    def all_labels(self) -> list[ResourceLabel]:
        merged = self.merged()
        return [
            ResourceLabel(name=label.name, target=label.target, value=label.value)
            for label in merged.labels
        ]

    def all_relations(self) -> list[Relation]:
        merged = self.merged()
        relations = bisect_make(
            merged.metadata.relations or [],
            key=lambda r: r.unique_id(),
        )
        for observed in merged.observed:
            for relation in observed.relations:
                bisect_insert(relations, relation, key=lambda r: r.unique_id())
        return relations

    def merged(self) -> "ResourceView":
        if not self._cached:
            self._cached = self._uncached_merged()
        return self._cached

    def _uncached_merged(self) -> "ResourceView":
        if not self.history:
            raise IngestionError("no history in cached resource")
        if not self.history[0].locator:
            raise IngestionError("no locator in cached resource")

        merged = ResourceView(
            locator=self.history[0].locator,
            expired=[],
            labels=[],
            metadata=MetadataDelta(),
            observed=[],
        )
        for delta in self.history:
            merged = merged.with_update(delta)

        return merged


##
## Merged View
##


class ObservedView(BaseModel, frozen=True):
    suffix: Observable
    info_mime_type: MimeType | None = None
    info_observations: list[ObservationInfo_] = Field(default_factory=list)
    """
    The metadata of observations that belong in `AffordanceInfo.observations`.
    """
    info_sections: list[ObservationSection] = Field(default_factory=list)
    relations: list[Relation_] = Field(default_factory=list)

    def with_update(self, delta: "ObservedDelta") -> "ObservedView":
        return ObservedView(
            suffix=delta.suffix,
            info_mime_type=(
                delta.info_mime_type
                if delta.info_mime_type is not None
                else self.info_mime_type
            ),
            info_observations=(
                delta.info_observations
                if delta.info_observations is not None
                else self.info_observations
            ),
            info_sections=(
                delta.info_sections
                if delta.info_sections is not None
                else self.info_sections
            ),
            relations=(
                delta.relations if delta.relations is not None else self.relations
            ),
        )


class ResourceView(BaseModel, frozen=True):
    locator: Locator
    expired: list[Observable]
    labels: list[ResourceLabel]
    """
    The latest value of each label generated from previous observations.
    """
    metadata: MetadataDelta_
    """
    Includes the latest metadata from `resolve`, merged with `observe` updates.
    """
    observed: list[ObservedView]

    def with_update(self, delta: ResourceDelta) -> "ResourceView":
        new_expired: set[Observable] = set(self.expired)
        new_expired.update(delta.expired)

        # Take the latest value of each labl.
        new_labels: list[ResourceLabel] = (
            [] if delta.reset_labels else self.labels.copy()
        )
        for new_label in delta.labels:
            bisect_insert(new_labels, new_label, key=ResourceLabel.sort_key)

        new_observed: list[ObservedView] = []
        for obs in self.observed:
            bisect_insert(new_observed, obs, key=lambda x: str(x.suffix))
        for obs_delta in delta.observed:
            new_expired.discard(obs_delta.suffix)
            if existing := next(
                (obs for obs in self.observed if obs.suffix == obs_delta.suffix),
                None,
            ):
                bisect_insert(
                    new_observed,
                    existing.with_update(obs_delta),
                    key=lambda x: str(x.suffix),
                )
            else:
                obs = ObservedView(
                    suffix=obs_delta.suffix,
                    info_mime_type=obs_delta.info_mime_type,
                    info_sections=obs_delta.info_sections or [],
                    info_observations=obs_delta.info_observations or [],
                    relations=obs_delta.relations or [],
                )
                bisect_insert(new_observed, obs, key=lambda x: str(x.suffix))

        return ResourceView(
            locator=delta.locator if delta.locator is not None else self.locator,
            expired=sorted(new_expired, key=str),
            labels=new_labels,
            metadata=self.metadata.with_update(delta.metadata),
            observed=new_observed,
        )

    def get_label(self, name: str, target: Observable) -> Any | None:
        return next(
            (f.value for f in self.labels if f.name == name and f.target == target),
            None,
        )
