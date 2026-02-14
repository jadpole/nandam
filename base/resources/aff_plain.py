"""
TODO: Affordance "$schema" that provides a JSON-Schema.
TODO: Affordance "$plain" has an optional "schema_uri".
"""

from typing import Literal

from base.models.content import ContentText, PartCode, PartText
from base.resources.observation import Observation
from base.strings.data import MimeType
from base.strings.resource import Affordance, Observable

REGEX_SUFFIX_PLAIN = r"\$plain"


##
## Suffix
##


class AffPlain(Affordance, Observable, frozen=True):
    @staticmethod
    def new() -> AffPlain:
        return AffPlain(path=[])

    @classmethod
    def suffix_kind(cls) -> str:
        return "plain"

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_SUFFIX_PLAIN

    @classmethod
    def _suffix_examples(cls) -> list[str]:
        return ["$plain"]

    def affordance(self) -> AffPlain:
        return self


##
## Observation
##


class ObsPlain(Observation[AffPlain], frozen=True):
    kind: Literal["plain"] = "plain"
    mime_type: MimeType | None
    text: str

    def info_attributes(self) -> list[tuple[str, str]]:
        attributes = super().info_attributes()
        if self.mime_type:
            attributes.append(("mimetype", str(self.mime_type)))
        return attributes

    def render_body(self) -> ContentText:
        attributes: list[tuple[str, str]] = []
        if self.mime_type:
            attributes.append(("mimetype", self.mime_type))
        return ContentText.new(
            [
                *PartText.xml_open("plain", self.uri, attributes),
                self.as_code(),
                PartText.xml_close("plain"),
            ]
        )

    def as_code(self) -> PartCode:
        language: str | None = None
        match str(self.mime_type):
            case "text/markdown" | "text/x-markdown":
                language = "markdown"
        return PartCode.new(self.text, language)
