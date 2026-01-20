from pydantic import BaseModel
from typing import Literal

from base.config import IMAGE_TOKENS_ESTIMATE
from base.models.content import (
    ContentBlob,
    ContentText,
    PartHeading,
    PartLink,
    PartText,
    TextPart,
)
from base.resources.metadata import AffordanceInfo, ObservationInfo, ObservationSection
from base.resources.observation import Observation, ObservationBundle
from base.strings.data import DataUri, MimeType
from base.strings.file import FileName, FilePath, REGEX_FILENAME
from base.strings.resource import (
    Affordance,
    KnowledgeUri,
    Observable,
    ObservableUri,
    Reference,
    ResourceUri,
    WebUrl,
)
from base.utils.completion import estimate_tokens

REGEX_SUFFIX_BODY = r"\$body"
REGEX_SUFFIX_CHUNK = r"\$chunk(?:/[0-9]{2,})*"
REGEX_SUFFIX_MEDIA = rf"\$media(?:/{REGEX_FILENAME})*"


##
## Suffix
##


class AffBody(Affordance, Observable, frozen=True):
    @staticmethod
    def new() -> "AffBody":
        return AffBody(path=[])

    @classmethod
    def suffix_kind(cls) -> str:
        return "body"

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_SUFFIX_BODY

    @classmethod
    def _suffix_examples(cls) -> list[str]:
        return ["$body"]

    def affordance(self) -> "AffBody":
        return self


class AffBodyChunk(Observable, frozen=True):
    @staticmethod
    def new(indexes: list[int]) -> "AffBodyChunk":
        return AffBodyChunk(path=[FileName.decode(f"{index:02d}") for index in indexes])

    @classmethod
    def suffix_kind(cls) -> str:
        return "chunk"

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_SUFFIX_CHUNK

    @classmethod
    def _suffix_examples(cls) -> list[str]:
        return ["$chunk", "$chunk/01/02"]

    def indexes(self) -> list[int]:
        return [int(index.rstrip("0") or "0") for index in self.path]

    def affordance(self) -> AffBody:
        return AffBody.new()

    def root(self) -> "AffBody":
        return AffBody.new()


class AffBodyMedia(Observable, frozen=True):
    @staticmethod
    def new(path: FileName | FilePath | list[FileName] | None = None) -> "AffBodyMedia":
        if not path:
            path = []
        elif isinstance(path, FileName):
            path = [path]
        elif isinstance(path, FilePath):
            path = path.parts()
        return AffBodyMedia(path=path)

    @classmethod
    def suffix_kind(cls) -> str:
        return "media"

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_SUFFIX_MEDIA

    @classmethod
    def _suffix_examples(cls) -> list[str]:
        return ["$media", "$media/figure.png", "$media/figures/image.png"]

    def affordance(self) -> AffBody:
        return AffBody.new()

    def root(self) -> "AffBody":
        return AffBody.new()


##
## Observation
##


class ObsBodyChunk(BaseModel, frozen=True):
    """
    A summary of the '$chunk' observations that can be used to consult only the
    relevant parts of a large document.
    """

    indexes: list[int]
    description: str | None
    num_tokens: int

    def uri(self, root: KnowledgeUri) -> ObservableUri[AffBodyChunk]:
        return root.resource_uri().child_observable(AffBodyChunk.new(self.indexes))


class ObsBodySection(BaseModel, frozen=True):
    indexes: list[int]
    heading: str | None

    def indexes_str(self) -> str:
        return "/".join([f"{index:02d}" for index in self.indexes])


class ObsBody(Observation[AffBody], frozen=True):
    kind: Literal["body"] = "body"
    description: str | None
    content: ContentText | ContentBlob | None
    sections: list[ObsBodySection]
    chunks: list[ObsBodyChunk]

    def dependencies(self) -> list[Reference]:
        if self.content and isinstance(self.content, ContentText):
            return self.content.dep_links()
        else:
            return []

    def embeds(self) -> list[Reference]:
        if self.content and isinstance(self.content, ContentText):
            return self.content.dep_embeds()
        else:
            return [chunk.uri(self.uri) for chunk in self.chunks]

    def info(self) -> ObservationInfo:
        if self.content:
            return ObservationInfo(
                suffix=self.uri.suffix,
                num_tokens=(
                    IMAGE_TOKENS_ESTIMATE
                    if isinstance(self.content, ContentBlob)
                    else estimate_tokens(
                        self.content.as_str(), len(self.content.dep_embeds())
                    )
                ),
                mime_type=(
                    self.content.mime_type
                    if isinstance(self.content, ContentBlob)
                    else None
                ),
                description=self.description,
            )
        else:
            return ObservationInfo(
                suffix=self.uri.suffix,
                num_tokens=None,
                mime_type=None,
                description=self.description,
            )

    def render_info(self) -> list[TextPart]:
        """
        In placeholder-mode, render the document as a "table of contents": list
        the child sections and chunks, but do not render the content of chunks
        and include the chunk descriptions.

        NOTE: Given single chunk, expose only the "$body" URI, not "$chunk".
        """
        if self.content:
            attributes: list[tuple[str, str]] = []
            if self.description:
                attributes.append(("description", self.description))
            if isinstance(self.content, ContentText):
                return PartText.xml_open(
                    "document",
                    self.uri,
                    attributes,
                    self_closing=True,
                )
            else:
                attributes.append(("mimetype", self.content.mime_type))
                return PartText.xml_open(
                    "media",
                    self.uri,
                    attributes,
                    self_closing=True,
                )
        else:
            return self._render("info")

    def render_body(self) -> ContentText | ContentBlob:
        if self.content:
            if isinstance(self.content, ContentText):
                return ContentText.new(
                    [
                        *PartText.xml_open("document", self.uri, attributes=[]),
                        *self.content.parts,
                        PartText.xml_close("document"),
                    ]
                )
            else:
                return self.content
        else:
            return ContentText.new(self._render("body"))

    def _render(self, mode: Literal["body", "info"]) -> list[TextPart]:
        assert self.content is None
        result: list[TextPart] = []

        # Given a multi-chunk body:
        attributes: list[tuple[str, str]] = (
            [("description", self.description)]
            if mode == "info" and self.description
            else []
        )
        result.extend(PartText.xml_open("document", self.uri, attributes))

        included_sections: list[list[int]] = []
        for chunk in self.chunks:
            # Insert the headings for the parent sections.
            for section in self.sections:
                if section.indexes in included_sections:
                    continue
                num_indexes = len(section.indexes)
                if chunk.indexes[:num_indexes] == section.indexes:
                    included_sections.append(section.indexes)
                    if section.heading:
                        result.append(
                            PartHeading.new(level=num_indexes, text=section.heading)
                        )

            result.extend(
                [PartLink.new("embed", None, chunk.uri(self.uri))]
                if mode == "body"
                else PartText.xml_open(
                    "document-chunk",
                    self.uri,
                    attributes=(
                        [("description", chunk.description)]
                        if chunk.description
                        else []
                    ),
                    self_closing=True,
                )
            )

        result.append(PartText.xml_close("document"))
        return result


class ObsChunk(Observation[AffBodyChunk], frozen=True):
    kind: Literal["chunk"] = "chunk"
    description: str | None
    text: ContentText

    @staticmethod
    def new(
        uri: ObservableUri[AffBodyChunk],
        text: ContentText,
        description: str | None = None,
    ) -> "ObsChunk":
        return ObsChunk(uri=uri, description=description, text=text)

    @staticmethod
    def parse(
        uri: ObservableUri[AffBodyChunk],
        mode: Literal["data", "markdown", "plain"],
        text: str,
        description: str | None = None,
    ) -> "ObsChunk":
        parsed = (
            ContentText.new_plain(text)
            if mode == "plain"
            else ContentText.parse(text, mode=mode)
        )
        return ObsChunk(uri=uri, description=description, text=parsed)

    def dependencies(self) -> list[Reference]:
        return [
            href
            for href in self.text.dep_links()
            if not isinstance(href, KnowledgeUri)
            or href.resource_uri() != self.uri.resource_uri()
        ]

    def embeds(self) -> list[Reference]:
        return [
            href
            for href in self.text.dep_embeds()
            if not isinstance(href, KnowledgeUri)
            or href.resource_uri() != self.uri.resource_uri()
        ]

    def info(self) -> ObservationInfo:
        return ObservationInfo(
            suffix=self.uri.suffix,
            num_tokens=self.num_tokens(),
            mime_type=None,
            description=self.description,
        )

    def num_tokens(self) -> int:
        return estimate_tokens(self.text.as_str(), len(self.text.dep_embeds()))

    def infer_tag(self) -> Literal["document", "document-chunk"]:
        return "document-chunk" if self.uri.suffix.path else "document"

    def render_info(self) -> list[TextPart]:
        return PartText.xml_open(
            tag=self.infer_tag(),
            uri=self.uri,
            attributes=self.info_attributes(),
            self_closing=True,
        )

    def render_body(self) -> ContentText:
        tag = self.infer_tag()
        return ContentText.new(
            [
                *PartText.xml_open(tag, self.uri),
                *self.text.parts,
                PartText.xml_close(tag),
            ]
        )


class ObsMedia(Observation[AffBodyMedia], frozen=True):
    kind: Literal["media"] = "media"
    description: str | None
    placeholder: str | None
    mime_type: MimeType
    blob: str

    @staticmethod
    def stub(
        uri: str,
        blob: str | None = None,
        description: str | None = None,
        placeholder: str | None = None,
    ) -> "ObsMedia":
        if not uri.startswith("ndk://"):
            uri = f"ndk://stub/-/{uri}"
        if "/$media" not in uri:
            uri = f"{uri}/$media"

        mime_type, blob = DataUri.stub(blob).parts()

        return ObsMedia(
            uri=ObservableUri.decode(uri),
            description=description or "stub description",
            placeholder=placeholder or "stub placeholder",
            mime_type=mime_type,
            blob=blob,
        )

    def download_url(self) -> DataUri | WebUrl:
        if self.blob.startswith("https://"):
            return WebUrl.decode(self.blob)
        else:
            return DataUri.new(self.mime_type, self.blob)

    def info(self) -> ObservationInfo:
        return ObservationInfo(
            suffix=self.uri.suffix,
            num_tokens=IMAGE_TOKENS_ESTIMATE,
            mime_type=self.mime_type,
            description=self.description,
        )

    def as_blob(self) -> ContentBlob:
        return ContentBlob(
            uri=self.uri,
            placeholder=self.placeholder or self.description,
            mime_type=self.mime_type,
            blob=self.blob,
        )

    def as_link(self) -> PartLink:
        return PartLink.new("embed", self.description, self.uri)

    def render_body(self) -> ContentBlob:
        return self.as_blob()


##
## Bundle
##


class BundleBody(ObservationBundle[AffBody], frozen=True):
    kind: Literal["body"] = "body"
    description: str | None
    sections: list[ObsBodySection]
    chunks: list[ObsChunk]
    media: list[ObsMedia]

    @staticmethod
    def make_chunked(
        *,
        resource_uri: ResourceUri,
        description: str | None = None,
        sections: list[ObsBodySection],
        chunks: list[ObsChunk],
        media: list[ObsMedia] | None = None,
    ) -> "BundleBody":
        media = media or []
        return BundleBody(
            uri=resource_uri.child_affordance(AffBody.new()),
            description=description,
            sections=sorted(sections, key=lambda section: section.indexes_str()),
            chunks=sorted(chunks, key=lambda chunk: str(chunk.uri)),
            media=sorted(
                [m for m in media if any(m.uri in c.text.dep_embeds() for c in chunks)],
                key=lambda media: str(media.uri),
            ),
        )

    @staticmethod
    def make_single(
        *,
        resource_uri: ResourceUri,
        text: ContentText,
        media: list[ObsMedia] | None = None,
        description: str | None = None,
    ) -> "BundleBody":
        chunk_uri = resource_uri.child_observable(AffBodyChunk.new([]))
        result_chunk = ObsChunk.new(uri=chunk_uri, text=text, description=description)
        media = media or []
        return BundleBody(
            uri=resource_uri.child_affordance(AffBody.new()),
            description=None,
            sections=[],
            chunks=[result_chunk],
            media=sorted(
                [m for m in media if m.uri in result_chunk.text.dep_embeds()],
                key=lambda media: str(media.uri),
            ),
        )

    @staticmethod
    def make_media(
        *,
        resource_uri: ResourceUri,
        description: str | None = None,
        placeholder: str | None = None,
        mime_type: MimeType,
        blob: str,
    ) -> "BundleBody":
        media_uri = resource_uri.child_observable(AffBodyMedia.new())
        media = ObsMedia(
            uri=media_uri,
            description=description,
            placeholder=placeholder,
            mime_type=mime_type,
            blob=blob,
        )
        chunk = ObsChunk(
            uri=resource_uri.child_observable(AffBodyChunk.new([])),
            description=None,
            text=ContentText.new([PartLink.new("embed", None, media_uri)]),
        )
        return BundleBody(
            uri=resource_uri.child_affordance(AffBody.new()),
            description=None,
            sections=[],
            chunks=[chunk],
            media=[media],
        )

    def info(self) -> AffordanceInfo:
        """
        NOTE: Include the chunks in the affordance info, allowing agents to
        consult them directly (fewer tokens used), but omit media.
        """
        # Given only one chunk, reuse its description.
        # Given a pure "$media" body, prefer its description.
        if len(self.chunks) == 1 and not self.sections:
            description: str | None = self.chunks[0].description
            mime_type: MimeType | None = None
            if (
                len(self.media) == 1
                and self.media[0].description
                and len(self.chunks[0].text.parts) == 1
                and (only_media := self.chunks[0].text.parts[0])
                and isinstance(only_media, PartLink)
                and only_media.mode == "embed"
            ):
                description = self.media[0].description
                mime_type = self.media[0].mime_type

            return AffordanceInfo(
                suffix=self.uri.suffix,
                mime_type=mime_type,
                description=description,
            )

        return AffordanceInfo(
            suffix=self.uri.suffix,
            mime_type=None,
            description=self.description,
            sections=[
                ObservationSection(
                    type="chunk",
                    path=[FileName.decode(f"{index:02d}") for index in section.indexes],
                    heading=section.heading,
                )
                for section in self.sections
            ],
            observations=[obs.info() for obs in self.chunks],
        )

    def observations(self) -> list[Observation]:
        body_uri = ObservableUri.decode(str(self.uri))

        if not self.sections and len(self.chunks) == 1:
            only_chunk = self.chunks[0]
            if (
                len(self.media) == 1
                and len(only_chunk.text.parts) == 1
                and (only_embed := only_chunk.text.only_embed())
                and only_embed == self.media[0].uri
            ):
                only_media = self.media[0]
                return [
                    ObsBody(
                        uri=body_uri,
                        description=only_media.description or only_chunk.description,
                        content=ContentBlob(
                            uri=body_uri,
                            placeholder=only_media.placeholder
                            or only_media.description,
                            mime_type=only_media.mime_type,
                            blob=only_media.blob,
                        ),
                        sections=[],
                        chunks=[],
                    )
                ]

            else:
                return [
                    ObsBody(
                        uri=body_uri,
                        description=only_chunk.description,
                        content=only_chunk.text,
                        sections=[],
                        chunks=[],
                    ),
                    *self.media,
                ]

        obs_body = ObsBody(
            uri=body_uri,
            description=self.description,
            content=None,
            sections=self.sections,
            chunks=[
                ObsBodyChunk(
                    indexes=chunk.uri.suffix.indexes(),
                    description=chunk.description,
                    num_tokens=chunk.num_tokens(),
                )
                for chunk in self.chunks
            ],
        )
        return [obs_body, *self.chunks, *self.media]
