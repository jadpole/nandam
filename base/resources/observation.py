from pydantic import SerializeAsAny
from base.core.unions import ModelUnion
from base.models.content import ContentBlob, ContentText, PartText, TextPart
from base.resources.metadata import AffordanceInfo, ObservationInfo
from base.strings.resource import (
    Affordance,
    AffordanceUri,
    Observable,
    ObservableUri,
    Reference,
)


class Observation[Obs: Observable](ModelUnion, frozen=True):
    uri: ObservableUri[Obs]

    def dependencies(self) -> list[Reference]:
        return []

    def embeds(self) -> list[Reference]:
        return []

    def info(self) -> ObservationInfo:
        return ObservationInfo(
            suffix=self.uri.suffix,
            num_tokens=None,
            mime_type=None,
            description=None,
        )

    def info_attributes(self) -> list[tuple[str, str]]:
        attributes: list[tuple[str, str]] = []
        info = self.info()
        if value := info.mime_type:
            attributes.append(("mimetype", value))
        if value := info.description:
            attributes.append(("description", value))
        return []

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


class ObservationBundle[Aff: Affordance](ModelUnion, frozen=True):
    uri: AffordanceUri[Aff]

    def info(self) -> AffordanceInfo:
        return AffordanceInfo(suffix=self.uri.suffix)

    def observations(self) -> list[Observation]:
        raise NotImplementedError(
            "Subclasses must implement ObservationBundle.observations"
        )


Observation_ = SerializeAsAny[Observation]
ObservationBundle_ = SerializeAsAny[ObservationBundle]
