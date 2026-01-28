from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, Field, WrapSerializer
from typing import Annotated, Any, Literal, Self

from base.core.strings import ValidatedStr, normalize_str
from base.core.values import wrap_exclude_none, wrap_exclude_none_or_empty
from base.models.content import PartHeading
from base.strings.data import MimeType
from base.strings.file import FileName
from base.strings.resource import (
    Affordance,
    ExternalUri,
    KnowledgeSuffix,
    KnowledgeUri,
    Observable,
    ResourceUri,
    WebUrl,
)
from base.utils.sorted_list import bisect_find, bisect_insert, bisect_make


##
## Affordance
##


class ObservationInfo(BaseModel, frozen=True):
    suffix: Observable
    num_tokens: int | None = None  # TODO: Remove?
    mime_type: MimeType | None = None
    description: str | None = None


ObservationInfo_ = Annotated[ObservationInfo, WrapSerializer(wrap_exclude_none)]


class ObservationSection(BaseModel, frozen=True):
    """
    Sections allow to organize observations of an affordance into a table of
    contents, notably, the list of "$chunk" of a "$body".  When a `heading` is
    present, the title of the section is displayed above its children.
    """

    type: str
    path: list[FileName]
    heading: str | None

    @staticmethod
    def new_body(indexes: list[int], heading: str | None) -> "ObservationSection":
        return ObservationSection(
            type="chunk",
            path=[FileName.decode(f"{index:02d}") for index in indexes],
            heading=heading,
        )

    def is_parent(self, suffix: Observable) -> bool:
        suffix_str = suffix.as_suffix()
        prefix_str = "/".join([f"${self.type}", *self.path])
        return suffix_str == prefix_str or suffix_str.startswith(f"{prefix_str}/")


class AffordanceInfo(BaseModel, frozen=True):
    suffix: Affordance
    mime_type: MimeType | None = None
    description: str | None = None
    sections: list[ObservationSection] = Field(default_factory=list)
    observations: list[ObservationInfo_] = Field(default_factory=list)

    def with_fields(self, fields: "FieldValues") -> "AffordanceInfo":
        aff_body = Observable.parse_suffix("$body")
        if not fields.fields or self.suffix != aff_body:
            return self

        result = self
        if not self.description and (
            new_description := fields.get_str("description", [aff_body])
        ):
            result = self.model_copy(update={"description": new_description})

        new_descriptions = {
            obs.suffix: value
            for obs in self.observations
            if not obs.description
            and (value := fields.get_str("description", [obs.suffix]))
        }
        if new_descriptions:
            new_observations = [
                obs.model_copy(update={"description": value})
                if (value := new_descriptions.get(obs.suffix))
                else obs
                for obs in self.observations
            ]
            result = result.model_copy(update={"observations": new_observations})

        return result

    def breadcrumbs_sections(self, suffix: Observable) -> list[PartHeading]:
        # Get headings for sections.
        return [
            PartHeading(level=len(section.path) + 1, text=section.heading)
            for section in self.sections
            if section.is_parent(suffix) and section.heading
        ]

    def breadcrumbs_index(self, suffix: Observable) -> PartHeading | None:
        """
        NOTE: The "index" is only meaningful for "$chunk" observations.
        """
        if (
            suffix.suffix_kind() != "chunk"
            or not suffix.path
            or not self.get_observation_info(suffix)
        ):
            return None

        # If there are multiple siblings in the section, add the index of the
        # observation among them, i.e., "part i/n" starting at 1.
        siblings = [
            obs.suffix
            for obs in self.observations
            if obs.suffix.suffix_kind() == "chunk"
            and obs.suffix.path
            and obs.suffix.path[:-1] == suffix.path[:-1]
            and obs.suffix.path[-1] != suffix.path[-1]
        ]
        if not siblings:
            return None

        item_suffixes = sorted([str(suffix), *(str(obs) for obs in siblings)])
        item_index = item_suffixes.index(str(suffix)) + 1
        item_heading = f"{item_index}/{len(item_suffixes)}"
        return PartHeading(level=len(suffix.path) + 1, text=item_heading)

    def get_observation_info(self, suffix: Observable) -> ObservationInfo | None:
        """
        TODO: Ensure sorting and use `bisect_find`?
        """
        observation = next(
            (c for c in self.observations if c.suffix == suffix),
            None,
        )
        if observation:
            return observation
        elif suffix == self.suffix:
            return ObservationInfo(
                suffix=suffix,
                num_tokens=None,
                mime_type=self.mime_type,
                description=self.description,
            )
        else:
            return None


AffordanceInfo_ = Annotated[AffordanceInfo, WrapSerializer(wrap_exclude_none_or_empty)]


##
## Resource - Attributes
##


class ResourceAttrs(BaseModel, frozen=True):
    name: str
    """
    The human-readable name of the source.  Either the title of the document or
    the file name (when no title is inferred).
    """
    mime_type: MimeType | None = None
    """
    The MIME type of the original file, which is often different from the format
    used by agents.
    """
    description: str | None = None
    """
    The description of the source, either read by the connector or generated by
    an LLM.  Explains *what* the source contains, to help the agent navigate the
    knowledge graph.
    """
    citation_url: WebUrl | None = None
    """
    A link that humans can follow to consult the original document.

    NOTE: Should NEVER be a signed URL: protected resources should require the
    user to log in when they click the link.
    """
    created_at: datetime | None = None
    """
    The timestamp at which the original document was created.
    Availability depends on the connector.
    """
    updated_at: datetime | None = None
    """
    The timestamp at which the original document was last modified, as of the
    last ingestion.  Availability depends on the connector.
    """
    revision_data: str | None = None
    """
    Unique identifier that changes when the content of a resource changes.
    When present, used to check whether the content should be refreshed instead
    of `updated_at`.

    Examples: SharePoint `cTag`, Git commit hash, Confluence revision.
    """
    revision_meta: str | None = None
    """
    Unique identifier that changes when the metadata of a resource changes.
    When present, used to check whether the metadata should be refreshed instead
    of `updated_at`.

    Examples: SharePoint `eTag`.

    NOTE: When `tag_data` changes, then the metadata is refreshed as well.
    """


class ResourceAttrsUpdate(BaseModel, frozen=True):
    name: str | None = None
    mime_type: MimeType | None = None
    description: str | None = None
    citation_url: WebUrl | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    revision_data: str | None = None
    revision_meta: str | None = None

    @staticmethod
    def full(after: ResourceAttrs) -> "ResourceAttrsUpdate":
        return ResourceAttrsUpdate(
            name=after.name,
            mime_type=after.mime_type,
            description=after.description,
            citation_url=after.citation_url,
            created_at=after.created_at,
            updated_at=after.updated_at,
            revision_data=after.revision_data,
            revision_meta=after.revision_meta,
        )

    @staticmethod
    def diff(after: ResourceAttrs, before: ResourceAttrs) -> "ResourceAttrsUpdate":
        return ResourceAttrsUpdate(
            name=after.name if after.name != before.name else None,
            mime_type=(
                after.mime_type
                if after.mime_type is not None and after.mime_type != before.mime_type
                else None
            ),
            description=(
                after.description
                if after.description is not None
                and after.description != before.description
                else None
            ),
            citation_url=(
                after.citation_url
                if after.citation_url is not None
                and after.citation_url != after.citation_url
                else None
            ),
            created_at=(
                after.created_at
                if after.created_at is not None
                and after.created_at != before.created_at
                else None
            ),
            updated_at=(
                after.updated_at
                if after.updated_at is not None
                and after.updated_at != before.updated_at
                else None
            ),
            revision_data=(
                after.revision_data
                if after.revision_data is not None
                and after.revision_data != before.revision_data
                else None
            ),
            revision_meta=(
                after.revision_meta
                if after.revision_meta is not None
                and after.revision_meta != before.revision_meta
                else None
            ),
        )

    def apply(self, value: ResourceAttrs) -> ResourceAttrs:
        return ResourceAttrs(
            name=self.name if self.name is not None else value.name,
            mime_type=self.mime_type or value.mime_type,
            description=(
                self.description if self.description is not None else value.description
            ),
            citation_url=self.citation_url or value.citation_url,
            created_at=self.created_at or value.created_at,
            updated_at=self.updated_at or value.updated_at,
            revision_data=(
                self.revision_data
                if self.revision_data is not None
                else value.revision_data
            ),
            revision_meta=(
                self.revision_meta
                if self.revision_meta is not None
                else value.revision_meta
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
        )


ResourceAttrs_ = Annotated[ResourceAttrs, WrapSerializer(wrap_exclude_none)]
ResourceAttrsUpdate_ = Annotated[ResourceAttrsUpdate, WrapSerializer(wrap_exclude_none)]


##
## Resource - Fields
##


class FieldName(ValidatedStr):
    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["description", "some_property"]

    @classmethod
    def _schema_regex(cls) -> str:
        return r"[a-z0-9]+(?:_[a-z0-9]+)*"

    @classmethod
    def normalize(cls, value: str) -> Self:
        if normalized := cls.try_normalize(value):
            return normalized
        else:
            raise ValueError(f"cannot normalize {cls.__name__}, got '{value}'")

    @classmethod
    def try_normalize(cls, value: str) -> Self | None:
        """
        Try to generate a field name from an arbitrary string, usually a title.
        Replaces accented characters with their ASCII equivalent.
        """
        value = value.lower()
        for c in (" ", "-", "/"):
            value = value.replace(c, "_")
        normalized = normalize_str(
            value,
            allowed_special_chars="_",
            remove_duplicate_chars="_",
            remove_prefix_chars="_",
            remove_suffix_chars="_",
            unquote_url=True,
        )

        # Reject non-ASCII file names.  Notably, fails to generate a filename
        # from the Kanji title of a web page or YouTube video.
        if normalized:
            return cls.decode(normalized)
        else:
            return None

    def as_observable_key(self, target: Observable) -> str:
        return f"{self}_{target}"


class FieldValue(BaseModel):
    name: FieldName
    target: Observable
    value: Any

    def sort_key(self) -> str:
        return f"{self.name}/{self.target}"


@dataclass(kw_only=True)
class FieldValues:
    fields: list[FieldValue]

    @staticmethod
    def new(fields: "FieldValues | list[FieldValue] | None" = None) -> "FieldValues":
        if isinstance(fields, FieldValues):
            return fields
        elif fields:
            return FieldValues(fields=bisect_make(fields, key=FieldValue.sort_key))
        else:
            return FieldValues(fields=[])

    def add(self, field: FieldValue) -> None:
        bisect_insert(self.fields, field, key=FieldValue.sort_key)

    def as_list(self) -> list[FieldValue]:
        return self.fields.copy()

    def extend(self, fields: list[FieldValue]) -> None:
        for field in fields:
            bisect_insert(self.fields, field, key=FieldValue.sort_key)

    def get(self, name: str, target: list[Observable]) -> FieldValue | None:
        for aff in target:
            value = bisect_find(self.fields, f"{name}/{aff}", key=FieldValue.sort_key)
            if value:
                return value
        return None

    def get_any(self, name: str, target: list[Observable]) -> Any | None:
        if f := self.get(name, target):
            return f.value
        else:
            return None

    def get_str(self, name: str, target: list[Observable]) -> str | None:
        if (f := self.get(name, target)) and isinstance(f.value, str):
            return str(f.value)
        else:
            return None


##
## Resource
##


class ResourceInfo(BaseModel, frozen=True):
    uri: ResourceUri
    attributes: ResourceAttrs_
    aliases: list[ExternalUri]
    affordances: list[AffordanceInfo_]

    def get_affordance(self, suffix: Affordance) -> AffordanceInfo | None:
        """
        TODO: Ensure sorting and use `bisect_find`.
        """
        return next(
            (c for c in self.affordances if c.suffix == suffix),
            None,
        )

    def get_observation_info(self, suffix: Observable) -> ObservationInfo | None:
        if not (affordance := self.get_affordance(suffix.affordance())):
            return None
        return affordance.get_observation_info(suffix)

    def cited(
        self,
        suffix: KnowledgeSuffix | None = None,
        excerpt: str | None = None,
    ) -> "CitedResource | None":
        """
        Examples of breadcrumbs + name given an Observable suffix:
        - "name / heading1 / heading2 / chunk 2/3" ($chunk with siblings)
        - "name / heading1 / heading2" ($chunk without siblings)
        - "name / chunk 2/3" (root $chunk with siblings)
        - "name / image.png" ($file/figures/image.png, $media/figures/image.png)
        - "name" ($body, root $chunk without siblings, $media)
        """
        if not suffix:
            return CitedResource(
                uri=self.uri,
                breadcrumbs=[],
                name=self.attributes.name,
                mime_type=self.attributes.mime_type,
                description=self.attributes.description,
                citation_url=self.attributes.citation_url,
                created_at=self.attributes.created_at,
                updated_at=self.attributes.updated_at,
                sensitivity=None,  # TODO: `ResourceAttrs.sensitivity`
                excerpt=excerpt,
            )

        elif (
            isinstance(suffix, Observable)
            and (affordance := self.get_affordance(suffix.affordance()))
            and (observation := affordance.get_observation_info(suffix))
        ):
            suffix_name = suffix.suffix_kind()
            breadcrumbs: list[PartHeading] = affordance.breadcrumbs_sections(suffix)
            breadcrumbs_index = affordance.breadcrumbs_index(suffix)
            breadcrumbs_name = PartHeading(level=1, text=self.attributes.name)
            if breadcrumbs_index:
                name = f"{suffix_name} {breadcrumbs_index.text}"
                breadcrumbs = [breadcrumbs_name, *breadcrumbs]
            elif breadcrumbs:
                name = breadcrumbs[-1].text
                breadcrumbs = [breadcrumbs_name, *breadcrumbs[:-1]]
            elif suffix.path and suffix_name != "chunk":
                name = str(observation.suffix.path[-1])
                breadcrumbs = [breadcrumbs_name]
            else:
                name = self.attributes.name

            return CitedResource(
                uri=self.uri.child_observable(suffix),
                breadcrumbs=[b.text for b in breadcrumbs],
                name=name,
                mime_type=(
                    observation.mime_type
                    or (
                        self.attributes.mime_type
                        if isinstance(suffix, Affordance)
                        else None
                    )
                ),
                description=(
                    observation.description
                    or (
                        self.attributes.description
                        if isinstance(suffix, Affordance)
                        else None
                    )
                ),
                citation_url=self.attributes.citation_url,
                created_at=self.attributes.created_at,
                updated_at=self.attributes.updated_at,
                sensitivity=None,  # TODO: `ResourceAttrs.sensitivity`
                excerpt=excerpt,
            )

        elif isinstance(suffix, Affordance) and (
            affordance := self.get_affordance(suffix)
        ):
            return CitedResource(
                uri=self.uri.child_affordance(suffix),
                breadcrumbs=[],
                name=self.attributes.name,
                mime_type=self.attributes.mime_type,
                description=self.attributes.description,
                citation_url=self.attributes.citation_url,
                created_at=self.attributes.created_at,
                updated_at=self.attributes.updated_at,
                sensitivity=None,  # TODO: `ResourceAttrs.sensitivity`
                excerpt=excerpt,
            )

        else:
            return None


class ResourceInfoUpdate(BaseModel, frozen=True):
    attributes: ResourceAttrsUpdate_ = Field(default_factory=ResourceAttrsUpdate)
    aliases: list[ExternalUri] = Field(default_factory=list)
    affordances: list[AffordanceInfo_] = Field(default_factory=list)

    @staticmethod
    def full(after: ResourceInfo) -> "ResourceInfoUpdate":
        return ResourceInfoUpdate(
            attributes=ResourceAttrsUpdate.full(after.attributes),
            aliases=after.aliases,
            affordances=after.affordances,
        )

    @staticmethod
    def diff(after: ResourceInfo, before: ResourceInfo) -> "ResourceInfoUpdate":
        return ResourceInfoUpdate(
            attributes=ResourceAttrsUpdate.diff(after.attributes, before.attributes),
            aliases=[alias for alias in after.aliases if alias not in before.aliases],
            affordances=[
                affordance
                for affordance in after.affordances
                if before.get_affordance(affordance.suffix) != affordance
            ],
        )

    def apply(self, value: ResourceInfo) -> ResourceInfo:
        new_affordances: list[AffordanceInfo] = []
        for affordance in value.affordances:
            bisect_insert(new_affordances, affordance, key=lambda x: str(x.suffix))
        for affordance in self.affordances:
            bisect_insert(new_affordances, affordance, key=lambda x: str(x.suffix))

        return ResourceInfo(
            uri=value.uri,
            attributes=self.attributes.apply(value.attributes),
            aliases=bisect_make([*self.aliases, *value.aliases], key=str),
            affordances=new_affordances,
        )

    def is_empty(self) -> bool:
        return self.attributes.is_empty() and not self.affordances


##
## Citation
##


class CitedResource(BaseModel, frozen=True):
    """
    The combined information from a resource's metadata and the observable used
    to render citations.

    NOTE: Microsoft Teams supports a maximum of 7 native citations per message.
    """

    uri: KnowledgeUri
    breadcrumbs: list[str]
    name: str
    mime_type: MimeType | None
    description: str | None
    citation_url: WebUrl | None
    created_at: datetime | None
    updated_at: datetime | None
    sensitivity: Literal["confidential"] | None
    excerpt: str | None
