from typing import Literal

from base.models.content import ContentText, PartLink, PartText, TextPart
from base.resources.observation import Observation
from base.strings.resource import Affordance, Observable, Reference, ResourceUri

REGEX_SUFFIX_COLLECTION = r"\$collection"


##
## Suffix
##


class AffCollection(Affordance, Observable, frozen=True):
    @staticmethod
    def new() -> AffCollection:
        return AffCollection(path=[])

    @classmethod
    def suffix_kind(cls) -> str:
        return "collection"

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_SUFFIX_COLLECTION

    @classmethod
    def _suffix_examples(cls) -> list[str]:
        return ["$collection"]

    def affordance(self) -> AffCollection:
        return self


##
## Observation
##


class ObsCollection(Observation[AffCollection], frozen=True):
    kind: Literal["collection"] = "collection"
    results: list[ResourceUri]

    def dependencies(self) -> list[Reference]:
        return sorted(self.results, key=str)

    def info_attributes(self) -> list[tuple[str, str]]:
        attributes: list[tuple[str, str]] = []
        if self.results:
            attributes.append(("size", str(len(self.results))))
        return attributes

    def render_body(self) -> ContentText:
        parts: list[TextPart] = []
        parts.extend(PartText.xml_open("collection", self.uri))
        if self.results:
            for result in self.results:
                parts.append(PartText.new("- ", "\n", ""))
                parts.append(PartLink.new("markdown", None, result))
        else:
            parts.append(PartText.new("empty", "\n"))
        parts.append(PartText.xml_close("collection"))
        return ContentText.new(parts)
