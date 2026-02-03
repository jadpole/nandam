from pydantic import BaseModel, Field, JsonValue
from typing import Annotated, Literal

from base.config import IMAGE_TOKENS_ESTIMATE
from base.models.content import (
    ContentBlob,
    ContentText,
    PartHeading,
    PartLink,
    PartText,
    TextPart,
)
from base.resources.label import ResourceLabels
from base.resources.observation import Observation
from base.strings.data import DataUri, MimeType
from base.strings.file import FileName, FilePath, REGEX_FILENAME
from base.strings.resource import (
    REGEX_RESOURCE_URI,
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


ANY_BODY_OBSERVABLE_SCHEMA: dict[str, JsonValue] = {
    "type": "string",
    "title": "AnyBodyObservableUri",
    "pattern": rf"^(?:{REGEX_SUFFIX_BODY}|{REGEX_SUFFIX_CHUNK}|{REGEX_SUFFIX_MEDIA})$",
    "description": "An observable within a $body.",
}
AnyObservableBody = AffBody | AffBodyChunk | AffBodyMedia
AnyObservableBody_ = Annotated[
    AnyObservableBody,
    Field(json_schema_extra=ANY_BODY_OBSERVABLE_SCHEMA),
]


ANY_BODY_OBSERVABLE_URI_SCHEMA: dict[str, JsonValue] = {
    "type": "string",
    "title": "AnyBodyObservableUri",
    "pattern": rf"^{REGEX_RESOURCE_URI}/(?:{REGEX_SUFFIX_BODY}|{REGEX_SUFFIX_CHUNK}|{REGEX_SUFFIX_MEDIA})$",
    "description": "An observable URI within a $body.",
}
AnyBodyObservableUri = (
    ObservableUri[AffBody] | ObservableUri[AffBodyChunk] | ObservableUri[AffBodyMedia]
)
AnyBodyObservableUri_ = Annotated[
    AnyBodyObservableUri,
    Field(json_schema_extra=ANY_BODY_OBSERVABLE_URI_SCHEMA),
]


##
## Observation
##

BUFFER_TOKENS_BODY = 40
BUFFER_TOKENS_SECTION = 10


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

    def with_labels(self, labels: ResourceLabels) -> "Observation":
        should_update: bool = False
        root_description: str | None = self.description

        if not root_description:
            root_affs: list[Observable] = []
            if not self.content:
                root_affs = [AffBody.new()]
            elif isinstance(self.content, ContentBlob):
                root_affs = [AffBody.new(), AffBodyChunk.new([]), AffBodyMedia.new()]
            else:  # ContentText
                root_affs = [AffBody.new(), AffBodyChunk.new([])]

            root_description = labels.get_any("description", root_affs)
            if root_description:
                should_update = True

        new_chunks: list[ObsBodyChunk] = []
        for chunk in self.chunks:
            if not chunk.description and (
                chunk_description := labels.get_any(
                    "description", [AffBodyChunk.new(chunk.indexes)]
                )
            ):
                new_chunks.append(
                    chunk.model_copy(update={"description": chunk_description})
                )
                should_update = True
            else:
                new_chunks.append(chunk)

        if should_update:
            return self.model_copy(update={"description": root_description})
        else:
            return self

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

    def info_attributes(self) -> list[tuple[str, str]]:
        attributes: list[tuple[str, str]] = super().info_attributes()
        if self.content and isinstance(self.content, ContentBlob):
            attributes.append(("mimetype", str(self.content.mime_type)))
        return attributes

    def num_tokens(self) -> int:
        if self.content:
            if isinstance(self.content, ContentBlob):
                return IMAGE_TOKENS_ESTIMATE
            else:
                return estimate_tokens(
                    self.content.as_str(), len(self.content.dep_embeds())
                )
        else:
            return (
                BUFFER_TOKENS_BODY
                + sum(chunk.num_tokens for chunk in self.chunks)
                + sum(
                    estimate_tokens(s.heading) + BUFFER_TOKENS_SECTION
                    for s in self.sections
                    if s.heading
                )
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
            chunk_headings, chunk_sections = self.render_headings(
                chunk.indexes, included_sections
            )
            result.extend(chunk_headings)
            included_sections.extend(chunk_sections)

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

    def render_headings(
        self,
        chunk_indexes: list[int],
        previous_sections: list[list[int]],
    ) -> tuple[list[TextPart], list[list[int]]]:
        result: list[TextPart] = []
        rendered_sections: list[list[int]] = []

        # Insert the headings for the parent sections.
        for section in self.sections:
            if section.indexes in previous_sections:
                continue
            num_indexes = len(section.indexes)
            if chunk_indexes[:num_indexes] == section.indexes:
                rendered_sections.append(section.indexes)
                if section.heading:
                    result.append(
                        PartHeading.new(level=num_indexes, text=section.heading)
                    )

        return result, rendered_sections


class ObsChunk(Observation[AffBodyChunk], frozen=True):
    kind: Literal["chunk"] = "chunk"
    text: ContentText

    @staticmethod
    def new(
        resource_uri: ResourceUri,
        indexes: list[int],
        text: ContentText,
        *,
        description: str | None = None,
    ) -> "ObsChunk":
        return ObsChunk(
            uri=resource_uri.child_observable(AffBodyChunk.new(indexes)),
            description=description,
            text=text,
        )

    @staticmethod
    def stub(
        uri: str,
        mode: Literal["data", "markdown", "plain"],
        text: str,
        description: str | None = None,
    ) -> "ObsChunk":
        return ObsChunk(
            uri=ObservableUri[AffBodyChunk].decode(uri),
            description=description,
            text=(
                ContentText.new_plain(text)
                if mode == "plain"
                else ContentText.parse(text, mode=mode)
            ),
        )

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
    placeholder: str | None
    mime_type: MimeType
    blob: str

    @staticmethod
    def new(
        resource_uri: ResourceUri,
        path: list[FileName],
        mime_type: MimeType,
        blob: str,
        *,
        description: str | None = None,
        placeholder: str | None = None,
    ) -> "ObsMedia":
        return ObsMedia(
            uri=resource_uri.child_observable(AffBodyMedia.new(path)),
            description=description,
            placeholder=placeholder,
            mime_type=mime_type,
            blob=blob,
        )

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

    def info_attributes(self) -> list[tuple[str, str]]:
        attributes: list[tuple[str, str]] = super().info_attributes()
        attributes.append(("mimetype", str(self.mime_type)))
        return attributes

    def num_tokens(self) -> int:
        return IMAGE_TOKENS_ESTIMATE

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


AnyObservationBody = ObsBody | ObsChunk | ObsMedia
