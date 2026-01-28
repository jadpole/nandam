from pydantic import SerializeAsAny

from base.core.unions import ModelUnion
from base.models.content import ContentBlob, ContentText, PartText, TextPart
from base.resources.metadata import ResourceFields
from base.strings.resource import Observable, ObservableUri, Reference


class Observation[Obs: Observable](ModelUnion, frozen=True):
    uri: ObservableUri[Obs]
    description: str | None

    def with_fields(self, fields: ResourceFields) -> "Observation":
        if self.description is None and (
            value := fields.get_any("description", [self.uri.suffix])
        ):
            return self.model_copy(update={"description": value})
        else:
            return self

    def dependencies(self) -> list[Reference]:
        return []

    def embeds(self) -> list[Reference]:
        return []

    def info_attributes(self) -> list[tuple[str, str]]:
        attributes: list[tuple[str, str]] = []
        if value := self.description:
            attributes.append(("description", value))
        return attributes

    def render_info(self) -> list[TextPart]:
        return PartText.xml_open(
            tag=self.uri.suffix.suffix_kind(),
            uri=self.uri,
            attributes=self.info_attributes(),
            self_closing=True,
        )

    def render_body(self) -> ContentText | ContentBlob:
        """
        Render the observation in a format that can be understood by the LLM,
        which by default,
        """
        return ContentText.new(self.render_info())


Observation_ = SerializeAsAny[Observation]
