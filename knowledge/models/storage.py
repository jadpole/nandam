from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, SerializeAsAny

from base.core.unions import ModelUnion
from base.resources.relation import Relation_
from base.strings.data import MimeType
from base.strings.resource import ExternalUri, Observable, Realm, ResourceUri, WebUrl
from base.resources.metadata import (
    AffordanceInfo_,
    ObservationInfo_,
    ObservationSection,
)


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


class ObservedDelta(BaseModel, frozen=True):
    suffix: Observable
    mime_type: MimeType | None = None
    description: str | None = None
    sections: list[ObservationSection] | None = None
    observations: list[ObservationInfo_] | None = None
    relations: list[Relation_] | None = None


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

    def with_update(
        self,
        name: str | None = None,
        mime_type: MimeType | None = None,
        description: str | None = None,
        citation_url: WebUrl | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        revision_data: str | None = None,
        revision_meta: str | None = None,
        aliases: list[ExternalUri] | None = None,
        affordances: list[AffordanceInfo_] | None = None,
        relations: list[Relation_] | None = None,
    ) -> "MetadataDelta":
        return MetadataDelta(
            name=name if name is not None else self.name,
            mime_type=mime_type if mime_type is not None else self.mime_type,
            description=description if description is not None else self.description,
            citation_url=(
                citation_url if citation_url is not None else self.citation_url
            ),
            created_at=created_at if created_at is not None else self.created_at,
            updated_at=updated_at if updated_at is not None else self.updated_at,
            revision_data=(
                revision_data if revision_data is not None else self.revision_data
            ),
            revision_meta=(
                revision_meta if revision_meta is not None else self.revision_meta
            ),
            aliases=aliases if aliases is not None else self.aliases,
            affordances=affordances if affordances is not None else self.affordances,
            relations=relations if relations is not None else self.relations,
        )


MetadataDelta_ = SerializeAsAny[MetadataDelta]


class ResourceDelta(BaseModel):
    refreshed_at: datetime
    locator: Locator_ | None = None
    """
    The locator of the resource.  Note that it will typically only be set when
    the resource is resolved for the first time, however, we support changes in
    the locator for the same resource URI.
    """
    metadata: MetadataDelta_ = Field(default_factory=MetadataDelta)
    """
    The metadata fields that were changed by `Connector.resolve` or `read`.
    """
    expired: list[Observable] = Field(default_factory=list)
    """
    The root observations whose cache were expired by `Connector.resolve`, and
    should therefore be refreshed on the next read.  If this hasn't happened in
    the current request, then they are flagged here.
    """
    observed: list[ObservedDelta] = Field(default_factory=list)
    """
    The root observations that were refreshed by `Connector.observe`.
    """


class ResourceHistory(BaseModel):
    history: list[ResourceDelta]
    _cached: "ResourceView | None" = PrivateAttr(default=None)


##
## Merged View
##


class ObservedView(BaseModel, frozen=True):
    suffix: Observable
    expired: bool
    mime_type: MimeType | None = None
    description: str | None = None
    sections: list[ObservationSection] = Field(default_factory=list)
    observations: list[ObservationInfo_] = Field(default_factory=list)
    relations: list[Relation_] = Field(default_factory=list)


class ResourceView(BaseModel, frozen=True):
    locator: Locator
    metadata: MetadataDelta
    observed: list[ObservedView] = Field(default_factory=list)
