from dataclasses import dataclass
from pydantic import BaseModel

from base.models.content import (
    ContentBlob,
    ContentText,
    PartCode,
    PartHeading,
    PartLink,
    PartPageNumber,
    PartText,
    TextPart,
)
from base.resources.observation import Observation
from base.strings.data import MimeType
from base.strings.resource import Reference
from base.utils.markdown import strip_keep_indent
from base.utils.sorted_list import bisect_insert


class Rendered(BaseModel, frozen=True):
    text: list[TextPart]
    blobs: list[ContentBlob]

    @staticmethod
    def render(
        content: ContentText,
        observations: list[Observation],
    ) -> "Rendered":
        partial = RenderedPartial.new(observations)
        for part in content.parts:
            partial.render_part_mut(part)
        return Rendered(text=partial.text, blobs=partial.blobs)

    def as_llm_inline(
        self,
        supports_media: list[MimeType],
        limit_media: int | None,
    ) -> list[ContentBlob | str]:
        """
        NOTE: Embeds are NOT deduplicated.
        """
        result: list[ContentBlob | str] = []
        partial_text: list[TextPart] = []

        for part in self.text:
            if (
                isinstance(part, PartLink)
                and part.mode == "embed"
                and (embed := self.get_blob(part.href))
            ):
                if embed.mime_type in supports_media and (
                    limit_media is None or limit_media > 0
                ):
                    if partial_text:
                        if rendered_text := strip_keep_indent(
                            ContentText.new(partial_text).as_str()
                        ):
                            result.append(rendered_text)
                        partial_text = []

                    result.append(embed)
                    if limit_media is not None:
                        limit_media -= 1
                else:
                    partial_text.extend(embed.render_placeholder())
            else:
                partial_text.append(part)

        if partial_text and (
            rendered_text := strip_keep_indent(ContentText.new(partial_text).as_str())
        ):
            result.append(rendered_text)

        return result

    def as_llm_split(
        self,
        supports_media: list[MimeType],
        limit_media: int | None,
    ) -> tuple[str, list[ContentBlob]]:
        """
        NOTE: Embeds are automatically deduplicated.
        """
        text: list[TextPart] = []
        blobs: list[ContentBlob] = []

        for part in self.text:
            if (
                isinstance(part, PartLink)
                and part.mode == "embed"
                and (embed := self.get_blob(part.href))
            ):
                if embed.mime_type in supports_media and (
                    limit_media is None or limit_media > 0
                ):
                    text.append(part)
                    if not any(b.uri == embed.uri for b in blobs):
                        bisect_insert(blobs, embed, key=lambda b: str(b.uri))
                        if limit_media is not None:
                            limit_media -= 1
                else:
                    text.extend(embed.render_placeholder())
            else:
                text.append(part)

        return ContentText.new(text).as_str(), blobs

    def get_blob(self, href: Reference) -> ContentBlob | None:
        return next((embed for embed in self.blobs if embed.uri == href), None)


@dataclass(kw_only=True)
class RenderedPartial:
    text: list[TextPart]
    blobs: list[ContentBlob]
    available_obs: list[Observation]

    @staticmethod
    def new(observations: list[Observation]) -> "RenderedPartial":
        return RenderedPartial(text=[], blobs=[], available_obs=observations)

    def render_part_mut(self, part: TextPart) -> None:
        if isinstance(part, PartCode | PartHeading | PartPageNumber | PartText):
            self.text.append(part)
        elif part.mode == "embed" and (embed := self._get_embed(part.href)):
            self.render_embed_mut(embed, part.label)
        else:
            self._render_link_mut(part)

    def render_embed_mut(
        self,
        content: ContentText | ContentBlob,
        label: str | None = None,
    ) -> None:
        if isinstance(content, ContentBlob):
            self.text.append(PartLink.new("embed", label, content.uri))
            bisect_insert(self.blobs, content, key=lambda c: str(c.uri))
        else:
            # Recursively render embeds.
            for rendered_part in content.parts:
                self.render_part_mut(rendered_part)

    ##
    ## Helpers
    ##

    def _get_embed(self, href: Reference) -> ContentText | ContentBlob | None:
        if observation := self._get_observation(href):
            return observation.render_body()
        else:
            return None

    def _get_observation(self, href: Reference) -> Observation | None:
        if embed := next((obs for obs in self.available_obs if obs.uri == href), None):
            return embed
        else:
            return None

    def _render_link_mut(self, link: PartLink) -> None:
        self.text.append(link)
