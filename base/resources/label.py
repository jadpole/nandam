from dataclasses import dataclass
from pydantic import BaseModel, Field
from typing import Annotated, Any, Literal, Self

from base.core.strings import ValidatedStr, normalize_str
from base.strings.resource import Observable, ObservableUri, ResourceUri
from base.utils.sorted_list import bisect_find, bisect_insert, bisect_make


class LabelName(ValidatedStr):
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
        Try to generate a label name from an arbitrary string.
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

    def as_property(self, target: Observable | ObservableUri) -> str:
        property_suffix = LabelName.try_normalize(
            str(target).removeprefix("ndk://").removeprefix("self://")
        )
        return f"{self}_{property_suffix}"


##
## Absolute
##


class LabelValue(BaseModel, frozen=True):
    """
    Label with an absolute `ObservableUri`.
    """

    name: LabelName
    target: ObservableUri
    value: Any

    def as_relative(self) -> "ResourceLabel":
        return ResourceLabel(
            name=self.name,
            target=self.target.suffix,
            value=self.value,
        )

    def sort_key(self) -> str:
        return f"{self.name}/{self.target}"


class LabelValues(BaseModel):
    values: list[LabelValue]

    @staticmethod
    def new(
        labels: "LabelValues | list[LabelValue] | None" = None,
    ) -> "LabelValues":
        if isinstance(labels, LabelValues):
            return labels
        elif labels:
            return LabelValues(values=bisect_make(labels, key=LabelValue.sort_key))
        else:
            return LabelValues(values=[])

    def as_list(self) -> list[LabelValue]:
        return self.values.copy()

    def add(self, label: LabelValue) -> None:
        bisect_insert(self.values, label, key=LabelValue.sort_key)

    def extend(self, labels: list[LabelValue]) -> None:
        for label in labels:
            bisect_insert(self.values, label, key=LabelValue.sort_key)

    def get(
        self,
        name: str,
        uri: ResourceUri,
        target: list[Observable],
    ) -> LabelValue | None:
        """
        Return the first `target` observable within the resource labels of `uri`
        where a value exists.
        """
        uri_str = str(uri)
        for aff in target:
            aff_key = f"{name}/{uri_str}/{aff.as_suffix()}"
            value = bisect_find(self.values, aff_key, key=LabelValue.sort_key)
            if value is not None:
                return value
        return None

    def get_any(
        self,
        name: str,
        uri: ResourceUri,
        target: list[Observable],
    ) -> Any | None:
        if f := self.get(name, uri, target):
            return f.value
        else:
            return None

    def get_str(
        self,
        name: str,
        uri: ResourceUri,
        target: list[Observable],
    ) -> str | None:
        if (f := self.get(name, uri, target)) and isinstance(f.value, str):
            return str(f.value)
        else:
            return None


##
## Relative
##


class ResourceLabel(BaseModel, frozen=True):
    """
    Label with a relative `Observable` within a given `Resource`.
    """

    name: LabelName
    target: Observable
    value: Any

    def as_absolute(self, resource_uri: ResourceUri) -> LabelValue:
        return LabelValue(
            name=self.name,
            target=resource_uri.child_observable(self.target),
            value=self.value,
        )

    def sort_key(self) -> str:
        return f"{self.name}/{self.target}"


@dataclass(kw_only=True)
class ResourceLabels:
    values: list[ResourceLabel]

    @staticmethod
    def new(
        labels: "ResourceLabels | list[ResourceLabel] | None" = None,
    ) -> "ResourceLabels":
        if isinstance(labels, ResourceLabels):
            return labels
        elif labels:
            return ResourceLabels(
                values=bisect_make(labels, key=ResourceLabel.sort_key),
            )
        else:
            return ResourceLabels(values=[])

    def add(self, label: ResourceLabel) -> None:
        bisect_insert(self.values, label, key=ResourceLabel.sort_key)

    def as_list(self) -> list[ResourceLabel]:
        return self.values.copy()

    def extend(self, labels: list[ResourceLabel]) -> None:
        for label in labels:
            bisect_insert(self.values, label, key=ResourceLabel.sort_key)

    def get(self, name: str, target: list[Observable]) -> ResourceLabel | None:
        for aff in target:
            value = bisect_find(
                self.values, f"{name}/{aff}", key=ResourceLabel.sort_key
            )
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
## Filters
##


class AllowRule(BaseModel, frozen=True):
    action: Literal["allow", "block"]
    prefix: str

    @staticmethod
    def find_best(uri: ResourceUri, allowlist: list["AllowRule"]) -> "AllowRule | None":
        uri_str = str(uri)
        best: AllowRule | None = None
        for rule in allowlist:
            if not uri_str.startswith(rule.prefix):
                continue
            if best is None or len(rule.prefix) > len(best.prefix):
                best = rule
        return best

    @staticmethod
    def matches(
        uri: ResourceUri,
        default_action: Literal["allow", "block"],
        allowlist: list["AllowRule"],
    ) -> bool:
        action = default_action
        if best := AllowRule.find_best(uri, allowlist):
            action = best.action
        return action == "allow"


class LabelFilter(BaseModel, frozen=True):
    name: LabelName
    one_of: list[str] | None = None

    def satisfied_by(self, labels: list[ResourceLabel]) -> bool:
        values = [label.value for label in labels if label.name == self.name]
        if self.one_of is not None and not any(v in self.one_of for v in values):  # noqa: SIM103
            return False
        return True


class ResourceFilters(BaseModel, frozen=True):
    default: Literal["allow", "block"] = "allow"
    """
    Behaviour when no `allowlist` rule matches the URI.
    """
    allowlist: list[AllowRule] = Field(default_factory=list)
    """
    When empty, all URIs are valid (personal scope).
    When non-empty, only use information from "allow" URIs.

    NOTE: Useful as a privacy mechanism, by restricting which resources can be
    resolved within a request, when the access tokens have broader access than
    the scope in which the results will be displayed.
    """
    labels: list[LabelFilter] = Field(default_factory=list)
    """
    The existing labels that must match for the resource to be used.

    NOTE: Useful as a search mechanism, by returning only the resources relevant
    to a given underlying task.
    """

    def matches(self, uri: ResourceUri) -> bool:
        return AllowRule.matches(uri, self.default, self.allowlist)

    def satisfied_by(self, labels: list[ResourceLabel]) -> bool:
        return all(label_filter.satisfied_by(labels) for label_filter in self.labels)


##
## Definition
##


class EnumConstraint(BaseModel, frozen=True):
    name: Literal["enum"] = "enum"
    variants: list[str]


AnyLabelConstraint = EnumConstraint
AnyLabelConstraint_ = Annotated[AnyLabelConstraint, Field(discriminator="name")]


class LabelInfo(BaseModel, frozen=True):
    name: LabelName
    forall: list[Literal["body", "chunk", "media"]]
    """
    Generates a key for each observation of a given kind.
    """
    prompt: str
    """
    The prompt used by the LLM to update this field.
    """
    constraint: AnyLabelConstraint_ | None = None

    def matches_forall(self, observable: Observable) -> bool:
        return observable.suffix_kind() in self.forall

    def sort_key(self) -> str:
        return f"{self.name}/{','.join(self.forall)}"


class LabelDefinition(BaseModel, frozen=True):
    info: LabelInfo
    filters: ResourceFilters = Field(default_factory=ResourceFilters)

    def sort_key(self) -> str:
        return self.info.name


##
## Aggregate
##


class AggregateDefinition(BaseModel):
    name: LabelName
    prompt: str
    constraint: AnyLabelConstraint_ | None = None
    filters: ResourceFilters = Field(default_factory=ResourceFilters)

    def sort_key(self) -> str:
        return str(self.name)


class AggregateValue(BaseModel):
    name: LabelName
    value: Any

    def sort_key(self) -> str:
        return str(self.name)
