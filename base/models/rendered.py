from dataclasses import dataclass
from typing import Annotated, Literal
from pydantic import BaseModel, Field

from base.models.content import ContentBlob, ContentText, PartLink, PartText, TextPart
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


class RenderedDocument(BaseModel, frozen=True):
    type: Literal["document"] = "document"
    uri: ObservableUri
    name: str
    label: str | None
    content: list[str | ContentBlob]

    def as_str(self) -> str:
        return (
            f'<document uri="{self.uri}" name="{self.name}" label="{self.label}">\n'
            + "\n\n".join(
                f"![]({part.uri})" if isinstance(part, ContentBlob) else part
                for part in self.content
            )
            + "\n</document>"
        )


RenderedBlock = ContentBlob | ContentText | RenderedDocument
RenderedBlock_ = Annotated[RenderedBlock, Field(discriminator="type")]


class Rendered(BaseModel, frozen=True):
    blocks: list[RenderedBlock_]
    embeds: list[ObservableUri]

    @staticmethod
    def text(content: ContentText | str) -> Rendered:
        if isinstance(content, str):
            content = ContentText.parse(content)
        return Rendered(blocks=[content], embeds=[])

    @staticmethod
    def plain(text: str) -> Rendered:
        content = ContentText.new_plain(text, "\n")
        return Rendered(blocks=[content], embeds=[])

    @staticmethod
    def render(
        content: ContentText,
        observations: list[Observation],
    ) -> Rendered:
        return Rendered.render_parts(content.parts, observations)

    @staticmethod
    def render_embeds(
        uris: list[ObservableUri],
        observations: list[Observation],
    ) -> Rendered:
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
    ) -> list[Rendered]:
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
    ) -> list[list[AnyBodyObservableUri]]:
        groups: list[list[AnyBodyObservableUri]] = []
        current_uris: list[AnyBodyObservableUri] = []
        current_tokens = 0

        for uri in uris:
            obs = bisect_find(observations, str(uri), key=lambda o: str(o.uri))
            if not obs:
                raise ValueError(f"Bad content group: URI without observation: {uri}")
            if not isinstance(obs, ObsBody | ObsChunk | ObsMedia):
                raise ValueError(f"Bad content group: URI with bad observation: {uri}")  # noqa: TRY004

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
    ) -> Rendered:
        partial = _PartialRendered.new(observations, extra_embedded)
        for part in parts:
            partial.render_part_mut(part)
        return partial.build()

    def as_llm_inline(  # noqa: C901, PLR0912
        self,
        supports_media: list[MimeType],
    ) -> list[str | ContentBlob]:
        """
        NOTE: Embeds are NOT deduplicated.
        """
        flattened: list[str | ContentBlob] = []
        for block in self.blocks:
            if isinstance(block, ContentBlob):
                if block.mime_type in supports_media:
                    flattened.append(block)
                else:
                    blob_text = ContentText.new(block.render_placeholder())
                    flattened.append(blob_text.as_str())
            elif isinstance(block, ContentText):
                flattened.append(block.as_str())
            elif isinstance(block, RenderedDocument):
                for part in block.content:
                    if isinstance(part, ContentBlob):
                        if part.mime_type in supports_media:
                            flattened.append(part)
                        else:
                            blob_text = ContentText.new(part.render_placeholder())
                            flattened.append(blob_text.as_str())
                    else:
                        flattened.append(part)

        result: list[str | ContentBlob] = []
        partial_text: list[str] = []

        for part in flattened:
            if isinstance(part, ContentBlob):
                if partial_text:
                    result.append(
                        "\n\n".join(strip_keep_indent(p) for p in partial_text)
                    )
                    partial_text = []
                result.append(part)
            else:
                partial_text.append(part)

        if partial_text:
            result.append("\n\n".join(strip_keep_indent(p) for p in partial_text))

        return result

    def as_llm_split(
        self,
        supports_media: list[MimeType],
    ) -> tuple[str, list[ContentBlob]]:
        result_text: list[str] = []
        result_blobs: list[ContentBlob] = []
        for part in self.as_llm_inline(supports_media):
            if isinstance(part, ContentBlob):
                bisect_insert(result_blobs, part, key=lambda p: str(p.uri))
                result_text.append(f"![]({part.uri})")
            else:
                result_text.append(part)

        return "\n\n".join(result_text), result_blobs

    def as_str(self) -> str:
        return "\n\n".join(
            f"![]({block.uri})" if isinstance(block, ContentBlob) else block.as_str()
            for block in self.blocks
        )


##
## Intermediate Representation
##


@dataclass(kw_only=True)
class _PartialText:
    parts: list[TextPart]


@dataclass(kw_only=True)
class _PartialDocument:
    content: list[_PartialText | ContentBlob]


@dataclass(kw_only=True)
class _PartialRendered:
    blocks: list[ContentBlob | RenderedDocument | _PartialText]
    embeds: list[ObservableUri]
    available: list[Observation]

    @staticmethod
    def new(
        observations: list[Observation],
        extra_embedded: list[ObservableUri] | None = None,
    ) -> _PartialRendered:
        return _PartialRendered(
            blocks=[],
            embeds=bisect_make(extra_embedded, key=str) if extra_embedded else [],
            available=observations,
        )

    def build(self) -> Rendered:
        return Rendered(
            blocks=[
                ContentText.new(block.parts)
                if isinstance(block, _PartialText)
                else block
                for block in self.blocks
            ],
            embeds=self.embeds,
        )

    def render_part_mut(self, part: TextPart) -> None:
        if (
            isinstance(part, PartLink)
            and part.mode == "embed"
            and (embed := self._get_embed(part.href))
        ):
            self.blocks.append(self.render_embed_mut(embed, part.label))
            return

        last_block: _PartialText
        if self.blocks and isinstance(self.blocks[-1], _PartialText):
            last_block = self.blocks[-1]
        else:
            last_block = _PartialText(parts=[])
            self.blocks.append(last_block)
        last_block.parts.append(part)

    def render_embed_mut(
        self,
        obs: Observation,
        label: str | None = None,
    ) -> ContentBlob | RenderedDocument:
        rendered = obs.render_body()
        if isinstance(rendered, ContentBlob):
            return rendered

        document_name = ""  # TODO: Get document name.
        document = _PartialDocument(content=[])
        for part in rendered.parts:
            self._render_document_part_mut(document, part)

        return RenderedDocument(
            uri=obs.uri,
            name=document_name,
            label=label,
            content=[
                ContentText.new(block.parts).as_str()
                if isinstance(block, _PartialText)
                else block
                for block in document.content
            ],
        )

    def _render_document_part_mut(
        self,
        document: _PartialDocument,
        part: TextPart,
    ) -> None:
        """
        NOTE: Documents do not support other documents as children: we flatten
        them into text interleaved with blobs.  All dependencies are tracked at
        the root `PartialRendered` level for use-cases that require it, such as
        generating labels.
        """
        if (
            isinstance(part, PartLink)
            and part.mode == "embed"
            and (embed := self._get_embed(part.href))
        ):
            rendered = embed.render_body()
            if isinstance(rendered, ContentBlob):
                document.content.append(rendered)
            else:
                for sub_part in rendered.parts:
                    self._render_document_part_mut(document, sub_part)
            return

        last_block: _PartialText
        if document.content and isinstance(document.content[-1], _PartialText):
            last_block = document.content[-1]
        else:
            last_block = _PartialText(parts=[])
            document.content.append(last_block)
        last_block.parts.append(part)

    ##
    ## Helpers
    ##

    def _get_embed(self, href: Reference) -> Observation | None:
        if obs := next((obs for obs in self.available if obs.uri == href), None):
            bisect_insert(self.embeds, obs.uri, key=str)
            return obs
        else:
            return None
