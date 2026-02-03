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
from base.resources.aff_body import (
    AffBody,
    AffBodyChunk,
    AnyBodyObservableUri,
    ObsBody,
    ObsChunk,
    ObsMedia,
)
from base.resources.observation import Observation
from base.strings.data import MimeType
from base.strings.resource import ObservableUri, Reference
from base.utils.markdown import strip_keep_indent
from base.utils.sorted_list import bisect_find, bisect_insert, bisect_make


class Rendered(BaseModel, frozen=True):
    text: list[TextPart]
    blobs: list[ContentBlob]
    embedded: list[ObservableUri]

    @staticmethod
    def render(
        content: ContentText,
        observations: list[Observation],
    ) -> "Rendered":
        return Rendered.render_parts(content.parts, observations)

    @staticmethod
    def render_embeds(
        uris: list[ObservableUri],
        observations: list[Observation],
    ) -> "Rendered":
        """
        NOTE: When a "$body" is broken down into chunks, you can pass only the
        URIs of those chunks, but include the "$body" in the observations, since
        chunks in `uris` are automatically wrapped within a `<document>` tag.
        """
        parts: list[TextPart] = []
        extra_embedded: list[ObservableUri] = []

        parent_body: ObsBody | None = None
        parent_sections: list[list[int]] = []
        for uri in uris:
            obs = bisect_find(observations, str(uri), key=lambda o: str(o.uri))

            # When the embeded root observation is not found, then the link part
            # will simply be kept as-is in the result.
            if not obs:
                parts.append(PartLink.new("embed", None, uri))
                continue

            # Render `<document>` around chunks.
            new_parent_uri = (
                uri.resource_uri().child_observable(AffBody.new())
                if isinstance(uri.suffix, AffBodyChunk)
                else None
            )
            if new_parent_uri != (parent_body and parent_body.uri):
                if parent_body:
                    parts.append(PartText.xml_close("document"))
                    parent_body = None
                    parent_sections = []

                if new_parent_uri:
                    new_parent_body = bisect_find(
                        observations, str(new_parent_uri), key=lambda o: str(o.uri)
                    )
                    if new_parent_body and isinstance(new_parent_body, ObsBody):
                        parent_body = new_parent_body
                        parts.extend(PartText.xml_open("document", new_parent_uri, []))
                        extra_embedded.append(new_parent_uri)

            # Render the section headings from "$body" before a chunk.
            if parent_body and isinstance(obs, ObsChunk):
                chunk_headings, chunk_sections = parent_body.render_headings(
                    obs.uri.suffix.indexes(), parent_sections
                )
                parts.extend(chunk_headings)
                parent_sections.extend(chunk_sections)

            parts.append(PartLink.new("embed", None, uri))

        if parent_body:
            parts.append(PartText.xml_close("document"))

        return Rendered.render_parts(parts, observations, extra_embedded)

    @staticmethod
    def render_groups(
        uris: list[AnyBodyObservableUri],
        observations: list[Observation],
        group_threshold_tokens: int,
    ) -> "list[Rendered]":
        """
        Group the "root URIs" into groups that fit within the token threshold,
        then apply `render_embeds` for each group.
        """
        uris = bisect_make(uris, key=str)
        observations = sorted(observations, key=lambda obs: str(obs.uri))
        groups = Rendered._make_groups(uris, observations, group_threshold_tokens)
        return [
            Rendered.render_embeds(group_uris, observations) for group_uris in groups
        ]

    @staticmethod
    def _make_groups(
        uris: list[AnyBodyObservableUri],
        observations: list[Observation],
        group_threshold_tokens: int,
    ) -> "list[list[AnyBodyObservableUri]]":
        groups: list[list[AnyBodyObservableUri]] = []
        current_uris: list[AnyBodyObservableUri] = []
        current_tokens = 0

        for uri in uris:
            obs = bisect_find(observations, str(uri), key=lambda o: str(o.uri))
            if not obs:
                raise ValueError(f"RenderedGroup uri without observation: {uri}")
            if not isinstance(obs, ObsBody | ObsChunk | ObsMedia):
                raise ValueError(f"RenderedGroup uri with bad observation: {uri}")  # noqa: TRY004

            num_tokens = obs.num_tokens()
            if current_tokens + num_tokens > group_threshold_tokens and current_uris:
                groups.append(current_uris)
                current_uris = []
                current_tokens = 0

            current_uris.append(uri)
            current_tokens += num_tokens

        if current_uris:
            groups.append(current_uris)

        return groups

    @staticmethod
    def render_parts(
        parts: list[TextPart],
        observations: list[Observation],
        extra_embedded: list[ObservableUri] | None = None,
    ) -> "Rendered":
        partial = RenderedPartial.new(observations, extra_embedded)
        for part in parts:
            partial.render_part_mut(part)
        return Rendered(
            text=partial.text,
            blobs=partial.blobs,
            embedded=partial.embedded,
        )

    def as_llm_inline(
        self,
        supports_media: list[MimeType],
        limit_media: int | None,
    ) -> list[str | ContentBlob]:
        """
        NOTE: Embeds are NOT deduplicated.
        """
        result: list[str | ContentBlob] = []
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


##
## Intermedia Representation
##


@dataclass(kw_only=True)
class RenderedPartial:
    text: list[TextPart]
    blobs: list[ContentBlob]
    available_obs: list[Observation]
    embedded: list[ObservableUri]

    @staticmethod
    def new(
        observations: list[Observation],
        extra_embedded: list[ObservableUri] | None = None,
    ) -> "RenderedPartial":
        return RenderedPartial(
            text=[],
            blobs=[],
            available_obs=observations,
            embedded=bisect_make(extra_embedded, key=str) if extra_embedded else [],
        )

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
        if obs := next((obs for obs in self.available_obs if obs.uri == href), None):
            bisect_insert(self.embedded, obs.uri, key=str)
            return obs.render_body()
        else:
            return None

    def _render_link_mut(self, link: PartLink) -> None:
        self.text.append(link)
