import asyncio
import logging

from io import BytesIO
from PIL import Image
from pydantic import BaseModel

from base.api.documents import Fragment, FragmentUri
from base.models.content import ContentText, PartLink
from base.resources.aff_body import (
    AffBody,
    AffBodyChunk,
    AffBodyMedia,
    BundleBody,
    ObsBodySection,
    ObsChunk,
    ObsMedia,
)
from base.resources.aff_collection import BundleCollection
from base.resources.aff_file import AffFile, BundleFile
from base.resources.observation import ObservationBundle, ObservationBundle_
from base.resources.relation import Relation, Relation_, RelationLink, RelationParent
from base.strings.data import DataUri, MimeType
from base.strings.resource import (
    ExternalUri,
    KnowledgeUri,
    Observable,
    Reference,
    ResourceUri,
)
from base.utils.completion import estimate_tokens
from base.utils.sorted_list import bisect_insert, bisect_make

from knowledge.config import (
    IMAGE_MAX_SIDE_PX,
    IMAGE_MIN_SIDE_PX,
    IMAGE_PREFERRED_TYPE,
    IMAGE_MIME_TYPES,
)
from knowledge.domain.chunking import chunk_body
from knowledge.domain.resolve import try_infer_locators
from knowledge.models.context import KnowledgeContext, ObserveResult
from knowledge.models.exceptions import IngestionError
from knowledge.models.storage import (
    MetadataDelta,
    MetadataDelta_,
    ObservedDelta,
    ResourceView,
)
from knowledge.services.inference import SvcInference

logger = logging.getLogger(__name__)

NUM_PARALLEL_DESCRIPTIONS = 5
"""
The number of chunk descriptions that can be generated in parallel.

NOTE: Constrained by the GEORGES rate limits.  Since multiple documents may be
ingested in parallel, limits the number of concurrent descriptions on the same
document to avoid suffocating other requests.
"""

FRAGMENT_THRESHOLD = 800_000
"""
The maximum number of tokens in a "plain" or "data" source before we trim its
text content to `FRAGMENT_TRIMMED` in the "fragment" representation.

When this threshold is exceeded, LLMs can never view the full content of the
fragment directly.  Instead, it serves as a "preview" of the file before it is
fed to Code Interpreter.
"""

FRAGMENT_TRIMMED = 600_000
"""
This is the size of the "preview" of the file.  To access the full content, the
agent must feed the "raw file" to Code Interpreter or similar tools.
"""

SPREADSHEET_THRESHOLD = 40_000
"""
The maximum number of tokens in a spreadsheet before we split it into multiple
chunks, one per sheet.
"""

SHREADSHEET_CHUNK_TRIMMED = 20_000
"""
The maximum number of tokens preserved in a chunk from the document representing
a spreadsheet.  This allows Nandam to understand the format of the file, without
filling its context window.  Further analysis require Code Interpreter.
"""


def unittest_configure(
    num_parallel_descriptions: int = 5,
    fragment_threshold: int = 800_000,
    fragment_trimmed: int = 600_000,
    spreadsheet_threshold: int = 40_000,
    shreadsheet_chunk_trimmed: int = 20_000,
) -> None:
    global NUM_PARALLEL_DESCRIPTIONS  # noqa: PLW0603
    global FRAGMENT_THRESHOLD  # noqa: PLW0603
    global FRAGMENT_TRIMMED  # noqa: PLW0603
    global SPREADSHEET_THRESHOLD  # noqa: PLW0603
    global SHREADSHEET_CHUNK_TRIMMED  # noqa: PLW0603
    NUM_PARALLEL_DESCRIPTIONS = num_parallel_descriptions
    FRAGMENT_THRESHOLD = fragment_threshold
    FRAGMENT_TRIMMED = fragment_trimmed
    SPREADSHEET_THRESHOLD = spreadsheet_threshold
    SHREADSHEET_CHUNK_TRIMMED = shreadsheet_chunk_trimmed


##
## Bundle
##


class IngestedResult(BaseModel, frozen=True):
    metadata: MetadataDelta_
    bundle: ObservationBundle_
    observed: ObservedDelta
    derived: list[ObservationBundle_]
    should_cache: bool


async def ingest_observe_result(
    context: KnowledgeContext,
    resource_uri: ResourceUri,
    cached: ResourceView | None,
    metadata: MetadataDelta,
    observed: ObserveResult,
) -> IngestedResult:
    new_bundle: ObservationBundle
    new_derived: list[BundleFile]
    if isinstance(observed.bundle, Fragment):
        new_bundle, new_derived = await _ingest_fragment(
            context=context,
            resource_uri=resource_uri,
            mime_type=observed.metadata.mime_type,
            fragment=observed.bundle,
            should_cache=observed.should_cache,
        )
    else:
        new_bundle = observed.bundle
        new_derived = []

    # Generate descriptions for `DocumentBundle`.
    if observed.option_descriptions and isinstance(new_bundle, BundleBody):
        cached_descriptions: dict[Observable, str] = {}
        if new_bundle.description:
            cached_descriptions[new_bundle.uri.suffix] = new_bundle.description
        elif metadata.description:
            cached_descriptions[new_bundle.uri.suffix] = metadata.description

        if cached:
            for cached_obs in cached.observed:
                if cached_obs.description:
                    cached_descriptions[cached_obs.suffix] = cached_obs.description
                for cached_info in cached_obs.info_observations:
                    if cached_info.description:
                        cached_descriptions[cached_info.suffix] = (
                            cached_info.description
                        )

        new_bundle, new_derived = await _generate_descriptions_body(
            context=context,
            cached=cached_descriptions,
            bundle=new_bundle,
            derived=new_derived,
        )
        metadata = metadata.with_update(
            MetadataDelta(description=new_bundle.info().description)
        )

    bundle_info = new_bundle.info()
    observed_delta = ObservedDelta(
        suffix=new_bundle.uri.suffix,
        mime_type=bundle_info.mime_type,
        description=bundle_info.description,
        info_sections=bundle_info.sections,
        info_observations=bundle_info.observations,
        observations=[obs.info() for obs in new_bundle.observations()],
        relations=_ingest_observe_relations(
            new_bundle,
            observed.relations,
            observed.option_relations_parent,
            observed.option_relations_link,
        ),
    )

    return IngestedResult(
        metadata=metadata,
        bundle=new_bundle,
        observed=observed_delta,
        derived=sorted(new_derived, key=lambda b: str(b.uri)),
        should_cache=observed.should_cache,
    )


def _ingest_observe_relations(
    bundle: ObservationBundle_,
    observed_relations: list[Relation_],
    option_relations_parent: bool,
    option_relations_link: bool,
) -> list[Relation_]:
    resource_uri = bundle.uri.resource_uri()
    relations: list[Relation] = observed_relations.copy()

    # Record collection children as relations of type "parent".
    if option_relations_parent and isinstance(bundle, BundleCollection):
        child_hrefs: set[ResourceUri] = {
            child_uri.resource_uri() for child_uri in bundle.results
        }
        for href in child_hrefs:
            if any(href == n for r in relations for n in r.get_nodes()):
                continue  # Already recorded in another relation.
            relations.append(RelationParent(parent=resource_uri, child=href))

    # Record document links as relations of type "link" when they do are
    # not already recorded as other relations.
    if option_relations_link and isinstance(bundle, BundleBody):
        document_hrefs: set[ResourceUri] = {
            href.resource_uri()
            for chunk in bundle.chunks
            for href in chunk.render_body().dep_links()
            if isinstance(href, KnowledgeUri) and href.realm.create_backlinks()
        }
        for href in document_hrefs:
            if any(href == n for r in relations for n in r.get_nodes()):
                continue  # Already recorded in another relation.
            relations.append(RelationLink(source=resource_uri, target=href))

    return bisect_make(relations, key=lambda r: str(r.unique_id()))


##
## Fragment
##


async def _ingest_fragment(
    context: KnowledgeContext,
    resource_uri: ResourceUri,
    mime_type: MimeType | None,
    fragment: Fragment,
    should_cache: bool,
) -> tuple[BundleBody, list[BundleFile]]:
    if fragment.mode == "plain":
        markdown = _shorten_text(fragment.text, FRAGMENT_THRESHOLD, FRAGMENT_TRIMMED)
        text = ContentText.new_plain(markdown)
        bundle = BundleBody.make_single(resource_uri=resource_uri, text=text)
        return bundle, []

    elif mime_type and mime_type.mode() == "spreadsheet":
        bundle = await _ingest_spreadsheet(resource_uri, fragment.text)
        return bundle, []

    elif fragment.mode == "data":
        markdown = _shorten_text(fragment.text, FRAGMENT_THRESHOLD, FRAGMENT_TRIMMED)
        text = ContentText.parse(markdown, mode="data")
        text = await _ingest_links(context, text)
        bundle = BundleBody.make_single(resource_uri=resource_uri, text=text)
        return bundle, []

    # Only chunk a document when caching is enabled.
    markdown = (
        fragment.text
        if should_cache
        else _shorten_text(fragment.text, FRAGMENT_THRESHOLD, FRAGMENT_TRIMMED)
    )

    text: ContentText
    media: list[ObsMedia] = []
    files: list[BundleFile] = []
    if fragment.blobs:
        parsed = await _ingest_fragment_blobs(resource_uri, markdown, fragment.blobs)
        text, media, files = parsed
    else:
        text = ContentText.parse(markdown)

    if not should_cache:
        files = []

    text = await _ingest_links(context, text)
    bundle = (
        await chunk_body(resource_uri, text, media)
        if should_cache
        else BundleBody.make_single(resource_uri=resource_uri, text=text, media=media)
    )
    return bundle, sorted(files, key=lambda f: str(f.uri))


async def _ingest_fragment_blobs(
    resource_uri: ResourceUri,
    text: str,
    blobs: dict[FragmentUri, DataUri],
) -> tuple[ContentText, list[ObsMedia], list[BundleFile]]:
    return await asyncio.to_thread(
        _ingest_fragment_blobs_sync, resource_uri, text, blobs
    )


def _ingest_fragment_blobs_sync(  # noqa: C901, PLR0912
    resource_uri: ResourceUri,
    text: str,
    blobs: dict[FragmentUri, DataUri],
) -> tuple[ContentText, list[ObsMedia], list[BundleFile]]:
    # Start by finding and discarding meaningless blobs:
    # - Unused blobs which appear in the fragment object, yet are referenced
    #   nowhere in the fragment text.
    # - Repeated blobs are most likely, thumbnails or letterhead without useful
    #   information.  A blob is "repeated" when the same URI appears many times
    #   in the text, OR when the same data is reused for many URIs.
    unused_blob_urls: set[FragmentUri] = set()
    repeated_blob_urls: set[FragmentUri] = set()
    seen_data_uris: set[DataUri] = set()
    repeated_data_uris: set[DataUri] = set()
    for blob_uri, blob_data in blobs.items():
        num_occurrences = text.count(f"]({blob_uri})")
        if num_occurrences == 0:
            unused_blob_urls.add(blob_uri)
        elif num_occurrences > 1:
            repeated_blob_urls.add(blob_uri)
        if blob_data in repeated_data_uris:
            continue
        if blob_data in seen_data_uris:
            repeated_data_uris.add(blob_data)
            continue

        # NOTE: Special logic for unit tests:
        if blob_data not in (
            DataUri.stub(),
            DataUri.stub("file"),
            DataUri.stub("discard"),
        ):
            seen_data_uris.add(blob_data)

    # For performance, downsize large image blobs to the maximum size supported
    # by LLMs, use an efficient format, and discard meaningless blobs.
    # NOTE: Do not discard small images when the whole fragment is one image.
    image_fragment = "\n" not in text and len(blobs) == 1
    original_files: list[BundleFile] = []
    selected_blobs: dict[FragmentUri, DataUri] = {}
    for blob_uri, blob_data in blobs.items():
        if (
            blob_uri in unused_blob_urls
            or blob_uri in repeated_blob_urls
            or blob_data in repeated_data_uris
        ):
            continue

        blob_resized, original_file = _ingest_fragment_blob_sync(
            resource_uri, blob_uri, blob_data, image_fragment
        )
        if blob_resized:
            selected_blobs[blob_uri] = blob_resized
        if original_file:
            original_files.append(original_file)

    # `SelfUriBlob` of discarded blobs is replaced by an orphan anchor, so the
    # LLM sees its filename and the original figure caption (if any), providing
    # enough context for most cases.
    media: list[ObsMedia] = []
    for blob_uri in blobs:
        if data_uri := selected_blobs.get(blob_uri):
            absolute_uri = resource_uri.child_observable(
                AffBodyMedia.new(blob_uri.path())
            )
            blob_mime, blob_data = data_uri.parts()
            bisect_insert(
                media,
                ObsMedia(
                    uri=absolute_uri,
                    description=None,
                    placeholder=None,
                    mime_type=blob_mime,
                    blob=blob_data,
                ),
                key=lambda m: str(m.uri),
            )
            text = text.replace(f"]({blob_uri})", f"]({absolute_uri})")
        else:
            text = text.replace(f"]({blob_uri})", f"](#{blob_uri.path()})")

    # Save the selected, downscaled blobs in the fragment that will be ingested.
    return ContentText.parse(text), media, original_files


def _ingest_fragment_blob_sync(
    resource_uri: ResourceUri,
    blob_uri: FragmentUri,
    blob_data: DataUri,
    image_fragment: bool,
) -> tuple[DataUri | None, BundleFile | None]:
    """
    Discard images smaller than `MIN_IMAGE_SIDE_PX` and downscale large images
    to `MAX_IMAGE_SIDE_PX`.  See the bound definitions for more details.

    All images are converted into WEBP for performance and compatibility, which
    is reflected in `DataUri`, although in metadata, the original extension and
    MIME type are preserved.

    NOTE: Blobs that cannot be converted to WEBP images are discarded as well.
    NOTE: Do not discard small images when the whole fragment is one image.
    """
    mime_type, _ = blob_data.parts()
    if mime_type not in IMAGE_MIME_TYPES:
        return None, None  # Discard unsupported blobs.

    image = Image.open(BytesIO(blob_data.bytes()))
    width, height = image.size
    if (
        not image_fragment
        and (width < IMAGE_MIN_SIDE_PX or height < IMAGE_MIN_SIDE_PX)
        and blob_data not in (DataUri.stub(), DataUri.stub("file"))
    ):
        return None, None

    original_file = None
    if (
        width > IMAGE_MAX_SIDE_PX
        or height > IMAGE_MAX_SIDE_PX
        or blob_data == DataUri.stub("file")
    ):
        original_file = BundleFile(
            uri=resource_uri.child_affordance(AffFile.new(blob_uri.path())),
            mime_type=mime_type,
            description=None,
            download_url=blob_data,
            expiry=None,
        )

    if (
        mime_type == IMAGE_PREFERRED_TYPE
        and width <= IMAGE_MAX_SIDE_PX
        and height <= IMAGE_MAX_SIDE_PX
    ):
        return blob_data, None

    # Downscale the image to fit within a 1024x1024 box while preserving its
    # aspect ratio.
    image = image.convert("RGBA")
    aspect_ratio = width / height
    if width > IMAGE_MAX_SIDE_PX and width >= height:
        new_width = IMAGE_MAX_SIDE_PX
        new_height = int(new_width / aspect_ratio)
        image = image.resize((new_width, new_height))
    elif height > IMAGE_MAX_SIDE_PX and height > width:
        new_height = IMAGE_MAX_SIDE_PX
        new_width = int(new_height * aspect_ratio)
        image = image.resize((new_width, new_height))

    # Extract the bytes of the image as base64.
    buffered = BytesIO()
    image.save(buffered, format="webp", optimize=True)
    rescaled = DataUri.new(IMAGE_PREFERRED_TYPE, buffered.getvalue())

    return rescaled, original_file


##
## Body
##


async def _ingest_links(context: KnowledgeContext, text: ContentText) -> ContentText:
    hrefs = {p.href for p in text.parts_link() if isinstance(p.href, ExternalUri)}
    locators = await try_infer_locators(context, sorted(hrefs, key=str))
    replacements: dict[Reference, Reference] = {
        reference: uri.resource_uri() for reference, uri in locators.items() if uri
    }
    if not replacements:
        return text

    return ContentText(
        parts=[
            (
                PartLink.new(p.mode, p.label, replacements.get(p.href, p.href))
                if isinstance(p, PartLink)
                else p
            )
            for p in text.parts
        ],
        plain=None,
    )


async def _ingest_spreadsheet(
    resource_uri: ResourceUri,
    text: str,
) -> BundleBody:
    """
    Documents converts spreadsheets into CSV.  Multiple sheets are identified by
    Markdown-style headers (`##`), such that there is one "document section" per
    sheet and the Markdown headings become section headings.  Each sheet becomes
    a single "document chunk", and when large, is truncated individually.

    Since we typically extract meaningful insights from spreadsheets with code,
    their "document representation" is primarily used to understand their format
    and columns.  Therefore, we can trim them aggressively to avoid filling the
    context window.
    """
    # If the spreadsheet is small enough, even if it contains multiple sheets,
    # ingest it as a single chunk.
    is_single_sheet = not text.startswith("## ") or text.count("\n\n## ") == 0
    if estimate_tokens(text, 0) <= SPREADSHEET_THRESHOLD or is_single_sheet:
        trimmed = _shorten_text(text, SPREADSHEET_THRESHOLD, SHREADSHEET_CHUNK_TRIMMED)
        return BundleBody.make_single(
            resource_uri=resource_uri,
            text=ContentText.parse(trimmed, mode="data"),
        )

    # Split large multi-sheet spreadsheets into one chunk per sheet and trim
    # each chunk to provide a "preview" of each sheet.
    sections: list[ObsBodySection] = []
    chunks: list[ObsChunk] = []

    for index, chunk in enumerate(f"\n\n{text}".split("\n\n## ")[1:]):
        section_heading, chunk_text = chunk.split("\n", maxsplit=1)
        section_heading = section_heading.strip()
        sections.append(
            ObsBodySection(indexes=[index], heading=section_heading),
        )
        chunk_uri = resource_uri.child_observable(AffBodyChunk.new([index]))
        chunk_text = _shorten_text(chunk_text.strip(), SHREADSHEET_CHUNK_TRIMMED)
        chunks.append(
            ObsChunk.new(uri=chunk_uri, text=ContentText.parse(chunk_text, mode="data"))
        )

    return BundleBody.make_chunked(
        resource_uri=resource_uri,
        sections=sections,
        chunks=chunks,
    )


def _shorten_text(
    text: str,
    threshold_tokens: int,
    trimmed_max_tokens: int = 0,
) -> str:
    """
    LLM agents can consume large documents by breaking them down into chunks,
    but this is not easily possible with "data/plain" files or with "markdown"
    fragments from non-persisted sources.

    Therefore, we trim fragments that overflow `FRAGMENT_THRESHOLD` until they
    fit into `FRAGMENT_TRIMMED` and add a suffix:

    > ... (N lines omitted)
    """
    trimmed_max_tokens = trimmed_max_tokens or threshold_tokens
    num_tokens = estimate_tokens(text, 0)
    if num_tokens <= threshold_tokens:
        return text

    # Accumulate lines until we reach `max_tokens`.
    text_lines = text.splitlines(keepends=True)
    selected_lines: list[str] = []
    selected_tokens: int = 0
    for line in text_lines:
        line_tokens = estimate_tokens(line, 0)
        if selected_tokens + line_tokens > trimmed_max_tokens:
            break
        selected_lines.append(line)
        selected_tokens += line_tokens

    # If the first line is enough to overflow the limit, typically because the
    # complete fragment has one line (e.g., a big JSON object), then there is
    # no natural place to "split" it.
    if not selected_lines:
        raise IngestionError("The file is too large.")

    omitted_lines = len(text_lines) - len(selected_lines)
    return "".join(selected_lines).rstrip() + f"\n\n... ({omitted_lines} lines omitted)"


##
## Description
##


DESCRIPTION_MAX_INPUT_LENGTH = 60_000
"""
When the input document exceeds ~ 20k tokens (max fragment size), only keep the
first ~ 20k tokens.  Corresponds to roughly 60k characters.
"""

PROMPT_SUMMARY_COD = """\
Generate a concise, dense description of the Source.

Guidelines: \
The description should be 2-3 sentences and no more than 50 words. \
The description should be highly dense and concise yet self-contained, i.e., \
easily understood without the Source. Make every word count. \
The description must allow the reader to infer what QUESTIONS they can answer \
using this Source, NOT give answers.

Audience: this description will be used by humans and tools to decide whether \
they should consult this Source to answer a given question. It should thus be \
exhaustive, so they can infer what information the Source contains.

For example:

- Given a Tableau visualization, the description should list its dimensions, \
metrics, and filters. Since it is dynamic, do NOT cite numbers, nor comment on \
visible trends. The description should remain relevant when the data changes, \
but the structure remains the same.\
"""


async def _generate_descriptions_body(  # noqa: C901, PLR0912
    context: KnowledgeContext,
    cached: dict[Observable, str],
    bundle: BundleBody,
    derived: list[BundleFile],
) -> tuple[BundleBody, list[BundleFile]]:
    descriptions: dict[Observable, str] = {}

    # Generate descriptions of embedded media.
    pending_media: list[tuple[Observable, ContentText]] = []
    for media in bundle.media:
        if media.description:
            descriptions[media.uri.suffix] = media.description
        elif cached_description := cached.get(media.uri.suffix):
            descriptions[media.uri.suffix] = cached_description
        else:
            image_content = ContentText.new_embed(media.uri, None)
            pending_media.append((media.uri.suffix, image_content))
    descriptions.update(
        await _generate_descriptions_batch(context, pending_media, bundle.media)
    )

    # Generate descriptions of chunks.
    if (
        len(bundle.chunks) != 1
        or len(bundle.media) != 1
        or len(bundle.chunks[0].text.parts) != 1
        or not (only_part := bundle.chunks[0].text.parts[0])
        or not isinstance(only_part, PartLink)
        or only_part.mode != "embed"
    ):
        pending_chunks: list[tuple[Observable, ContentText]] = []
        for chunk in bundle.chunks:
            if chunk.description:
                descriptions[chunk.uri.suffix] = chunk.description
            elif cached_description := cached.get(chunk.uri.suffix):
                descriptions[chunk.uri.suffix] = cached_description
            else:
                pending_chunks.append((chunk.uri.suffix, chunk.text))
        descriptions.update(
            await _generate_descriptions_batch(context, pending_chunks, bundle.media)
        )

    # Reuse the media descriptions on derived files (original large images).
    for derived_file in derived:
        media_suffix = AffBodyMedia.new(derived_file.uri.suffix.path)
        if derived_file.description:
            descriptions[derived_file.uri.suffix] = derived_file.description
        elif cached_description := cached.get(derived_file.uri.suffix):
            descriptions[derived_file.uri.suffix] = cached_description
        elif media_description := cached.get(media_suffix):
            descriptions[derived_file.uri.suffix] = media_description

    # Generate the description for the full document by aggregating the
    # descriptions of its chunks.
    if bundle.description:
        descriptions[bundle.uri.suffix] = bundle.description
    elif cached_description := cached.get(bundle.uri.suffix):
        descriptions[bundle.uri.suffix] = cached_description
    elif len(bundle.chunks) > 1 and (
        body_description := await _generate_description_merged(
            context, descriptions, bundle.sections
        )
    ):
        descriptions[bundle.uri.suffix] = body_description

    new_body = BundleBody(
        uri=bundle.uri,
        description=descriptions.get(bundle.uri.suffix),
        sections=bundle.sections,
        chunks=[
            (
                chunk.model_copy(update={"description": description})
                if (description := descriptions.get(chunk.uri.suffix))
                else chunk
            )
            for chunk in bundle.chunks
        ],
        media=[
            (
                media.model_copy(update={"description": description})
                if (description := descriptions.get(media.uri.suffix))
                else media
            )
            for media in bundle.media
        ],
    )
    new_derived = [
        (
            derived_file.model_copy(update={"description": description})
            if (description := descriptions.get(derived_file.uri.suffix))
            else derived_file
        )
        for derived_file in derived
    ]
    return new_body, new_derived


async def _generate_descriptions_batch(
    context: KnowledgeContext,
    pending: list[tuple[Observable, ContentText]],
    media: list[ObsMedia],
) -> list[tuple[Observable, str]]:
    if not pending:
        return []

    if NUM_PARALLEL_DESCRIPTIONS > 1:
        results: list[tuple[Observable, str]] = []
        for start_index in range(0, len(pending), NUM_PARALLEL_DESCRIPTIONS):
            tasks = [
                _generate_description(context, pending_uri, pending_content, media)
                for pending_uri, pending_content in pending[
                    start_index : start_index + NUM_PARALLEL_DESCRIPTIONS
                ]
            ]
            for pending_uri, description in await asyncio.gather(*tasks):
                if description:
                    results.append((pending_uri, description))

        return results
    else:
        return [
            (pending_uri, description)
            for pending_uri, pending_content in pending
            if (
                gen_result := await _generate_description(
                    context, pending_uri, pending_content, media
                )
            )
            and (description := gen_result[1])
        ]


async def _generate_description_merged(
    context: KnowledgeContext,
    descriptions: dict[Observable, str],
    sections: list[ObsBodySection],
) -> str | None:
    summaries: list[str] = []
    included_sections: list[list[int]] = []
    descriptions_list = sorted(descriptions.items(), key=lambda item: str(item[0]))
    for observable, description in descriptions_list:
        if not isinstance(observable, AffBodyChunk):
            continue

        # Insert the headings for the parent sections.
        chunk_indexes = observable.indexes()
        for section in sections:
            if not section.heading or section.indexes in included_sections:
                continue
            num_indexes = len(section.indexes)
            if chunk_indexes[:num_indexes] == section.indexes:
                included_sections.append(section.indexes)
                summaries.append("#" * num_indexes + " " + section.heading)

        summaries.append(f"Summary: {description}")

    content = ContentText.new_plain("\n\n".join(summaries))
    _, output = await _generate_description(context, AffBody.new(), content, [])

    return output


class GeneratedDescription(BaseModel, frozen=True):
    description: str


async def _generate_description(
    context: KnowledgeContext,
    suffix: Observable,
    content: ContentText,
    media: list[ObsMedia],
) -> tuple[Observable, str | None]:
    """
    TODO: Generate longer 'placeholder' for media, alongside 'description'.
    """
    try:
        inference = context.service(SvcInference)
        text = _shorten_text(content.as_str(), DESCRIPTION_MAX_INPUT_LENGTH)
        prompt = f"<Source>\n{text}\n</Source>"
        response = await inference.completion_json(
            system=PROMPT_SUMMARY_COD,
            response_schema=GeneratedDescription,
            prompt=(
                ContentText.parse(prompt) if media else ContentText.new_plain(prompt)
            ),
            observations=sorted(media, key=lambda m: str(m.uri)),
        )
        description = (
            response.description
            if response and isinstance(response, GeneratedDescription)
            else response
        )
        return suffix, description
    except Exception:
        logger.exception("Failed to generate description")
        return suffix, None
