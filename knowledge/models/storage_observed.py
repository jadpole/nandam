from datetime import datetime
from pydantic import BaseModel, Field
from typing import Annotated, Literal

from base.models.content import ContentBlob, ContentText, PartLink
from base.resources.aff_body import (
    AffBody,
    AffBodyMedia,
    ObsBody,
    ObsBodyChunk,
    ObsBodySection,
    ObsChunk,
    ObsMedia,
)
from base.resources.aff_collection import AffCollection, ObsCollection
from base.resources.aff_file import AffFile, ObsFile
from base.resources.aff_plain import AffPlain, ObsPlain
from base.resources.metadata import (
    AffordanceInfo,
    FieldValue,
    ObservationInfo,
    ObservationSection,
)
from base.resources.observation import Observation
from base.strings.data import DataUri, MimeType
from base.strings.file import FileName
from base.strings.resource import (
    AffordanceUri,
    KnowledgeUri,
    Observable,
    ObservableUri,
    Reference,
    ResourceUri,
    WebUrl,
)
from base.utils.sorted_list import bisect_find, bisect_make


##
## Body
##


class BundleBody(BaseModel, frozen=True):
    kind: Literal["body"] = "body"
    uri: AffordanceUri[AffBody]
    description: str | None
    sections: list[ObsBodySection]
    chunks: list[ObsChunk]
    media: list[ObsMedia]

    def dep_embeds(self) -> list[Reference]:
        return bisect_make(
            (
                r
                for chunk in self.chunks
                for r in chunk.text.dep_embeds()
                if not isinstance(r, KnowledgeUri)
                or r.resource_uri() != self.uri.resource_uri()
            ),
            key=str,
        )

    def dep_links(self) -> list[Reference]:
        return bisect_make(
            (
                r
                for chunk in self.chunks
                for r in chunk.text.dep_links()
                if not isinstance(r, KnowledgeUri)
                or r.resource_uri() != self.uri.resource_uri()
            ),
            key=str,
        )

    @staticmethod
    def new(
        *,
        resource_uri: ResourceUri,
        description: str | None = None,
        sections: list[ObsBodySection],
        chunks: list[ObsChunk],
        media: list[ObsMedia],
    ) -> "BundleBody":
        if media:
            used_media: set[str] = {
                "/".join(r.suffix.path)
                for c in chunks
                for r in c.text.dep_embeds()
                if isinstance(r, KnowledgeUri)
                and r.suffix
                and r.suffix.suffix_kind() == "media"
                and r.resource_uri() == resource_uri
            }
            media = sorted(
                [m for m in media if "/".join(m.uri.suffix.path) in used_media],
                key=lambda m: str(m.uri.suffix),
            )

        return BundleBody(
            uri=resource_uri.child_affordance(AffBody.new()),
            description=description,
            sections=sorted(sections, key=lambda section: section.indexes_str()),
            chunks=sorted(chunks, key=lambda c: str(c.uri.suffix)),
            media=media,
        )

    @staticmethod
    def make_single(
        *,
        resource_uri: ResourceUri,
        text: ContentText,
        media: list[ObsMedia] | None = None,
        description: str | None = None,
    ) -> "BundleBody":
        return BundleBody.new(
            resource_uri=resource_uri,
            sections=[],
            chunks=[ObsChunk.new(resource_uri, [], text, description=description)],
            media=media or [],
        )

    @staticmethod
    def make_media(
        *,
        resource_uri: ResourceUri,
        mime_type: MimeType,
        blob: str,
        description: str | None = None,
        placeholder: str | None = None,
    ) -> "BundleBody":
        media_uri = resource_uri.child_observable(AffBodyMedia.new())
        body_text = ContentText.new([PartLink.new("embed", None, media_uri)])
        return BundleBody(
            uri=resource_uri.child_affordance(AffBody.new()),
            description=None,
            sections=[],
            chunks=[ObsChunk.new(resource_uri, [], body_text)],
            media=[
                ObsMedia.new(
                    resource_uri=resource_uri,
                    path=[],
                    mime_type=mime_type,
                    blob=blob,
                    description=description,
                    placeholder=placeholder,
                )
            ],
        )

    def info(self) -> AffordanceInfo:
        """
        NOTE: Include the chunks in the affordance info, allowing agents to
        consult them directly (fewer tokens used), but omit media.

        NOTE: Empty chunk descriptions will be populated using fields.
        """
        resource_uri = self.uri.resource_uri()
        mime_type: MimeType | None = None
        description: str | None = self.description

        # Given only one "$chunk", reuse its description.
        # Given a pure "$media" body, prefer the media description.
        if not self.sections and len(self.chunks) == 1:
            if not description and self.chunks[0].description:
                description = self.chunks[0].description
            if (
                len(self.media) == 1
                and (only_embed := self.chunks[0].text.only_embed())
                and (aff_media := AffBodyMedia.new(self.media[0].uri.suffix.path))
                and only_embed == resource_uri.child_observable(aff_media)
            ):
                mime_type = self.media[0].mime_type
                if not description and self.media[0].description:
                    description = self.media[0].description

            return AffordanceInfo(
                suffix=self.uri.suffix,
                mime_type=mime_type,
                description=description or self.description,
                sections=[],
                observations=[],
            )

        return AffordanceInfo(
            suffix=self.uri.suffix,
            mime_type=None,
            description=description,
            sections=[
                ObservationSection(
                    type="chunk",
                    path=[FileName.decode(f"{index:02d}") for index in section.indexes],
                    heading=section.heading,
                )
                for section in self.sections
            ],
            observations=[
                ObservationInfo(
                    suffix=chunk.uri.suffix,
                    num_tokens=chunk.num_tokens(),
                    mime_type=None,
                    description=chunk.description,
                )
                for chunk in self.chunks
            ],
        )

    def observations(self) -> list[Observation]:
        body_uri = self.uri.resource_uri().child_observable(self.uri.suffix)

        if (
            not self.sections
            and len(self.chunks) == 1
            and len(self.media) == 1
            and self.chunks[0].text.only_embed() == self.media[0].uri
        ):
            return [
                ObsBody(
                    uri=body_uri,
                    description=(
                        self.media[0].description
                        or self.chunks[0].description
                        or self.description
                    ),
                    content=ContentBlob(
                        uri=body_uri,
                        placeholder=self.media[0].placeholder,
                        mime_type=self.media[0].mime_type,
                        blob=self.media[0].blob,
                    ),
                    sections=[],
                    chunks=[],
                )
            ]

        obs_media = [
            ObsMedia(
                uri=media.uri,
                description=media.description,
                placeholder=media.placeholder,
                mime_type=media.mime_type,
                blob=media.blob,
            )
            for media in self.media
        ]

        if not self.sections and len(self.chunks) == 1:
            return [
                ObsBody(
                    uri=body_uri,
                    description=self.chunks[0].description or self.description,
                    content=self.chunks[0].text,
                    sections=[],
                    chunks=[],
                ),
                *obs_media,
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
        obs_chunks = [
            ObsChunk(
                uri=chunk.uri,
                description=chunk.description,
                text=chunk.text,
            )
            for chunk in self.chunks
        ]
        return [obs_body, *obs_chunks, *obs_media]


class BundleFields(BaseModel, frozen=True):
    fields: list[FieldValue]

    def get_str(self, name: str, targets: list[Observable]) -> str | None:
        for target in targets:
            field = bisect_find(
                self.fields, f"{name}/{target}", key=lambda f: f"{f.name}/{f.target}"
            )
            if field and field.value and isinstance(field.value, str):
                return field.value
        return None


##
## Collection
##


class BundleCollection(BaseModel, frozen=True):
    kind: Literal["collection"] = "collection"
    uri: AffordanceUri[AffCollection]
    results: list[ResourceUri]

    def dep_embeds(self) -> list[Reference]:
        return []

    def dep_links(self) -> list[Reference]:
        return list(self.results)

    def info(self) -> AffordanceInfo:
        return AffordanceInfo(suffix=self.uri.suffix)

    def observations(self) -> list[Observation]:
        return [
            ObsCollection(
                uri=ObservableUri.decode(str(self.uri)),
                description=None,
                results=self.results,
            )
        ]


##
## File
##


class BundleFile(BaseModel, frozen=True):
    kind: Literal["file"] = "file"
    uri: AffordanceUri[AffFile]
    description: str | None
    mime_type: MimeType | None
    expiry: datetime | None
    download_url: DataUri | WebUrl

    def dep_embeds(self) -> list[Reference]:
        return []

    def dep_links(self) -> list[Reference]:
        return []

    def info(self) -> AffordanceInfo:
        return AffordanceInfo(
            suffix=self.uri.suffix,
            mime_type=self.mime_type,
            description=self.description,
        )

    def observations(self) -> list[Observation]:
        return [
            ObsFile(
                uri=ObservableUri.decode(str(self.uri)),
                description=self.description,
                mime_type=self.mime_type,
                expiry=self.expiry,
                download_url=self.download_url,
            )
        ]


##
## Plain
##


class BundlePlain(BaseModel, frozen=True):
    kind: Literal["plain"] = "plain"
    uri: AffordanceUri[AffPlain]
    mime_type: MimeType | None
    text: str

    def dep_embeds(self) -> list[Reference]:
        return []

    def dep_links(self) -> list[Reference]:
        return []

    def info(self) -> AffordanceInfo:
        return AffordanceInfo(
            suffix=self.uri.suffix,
            mime_type=self.mime_type,
            description=None,
        )

    def observations(self) -> list[Observation]:
        return [
            ObsPlain(
                uri=ObservableUri.decode(str(self.uri)),
                description=None,
                mime_type=self.mime_type,
                text=self.text,
            )
        ]


##
## Union
##


AnyBundle = BundleBody | BundleCollection | BundleFile | BundlePlain
AnyBundle_ = Annotated[AnyBundle, Field(discriminator="kind")]
