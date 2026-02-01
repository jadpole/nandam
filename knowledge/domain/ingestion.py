import asyncio
import logging

from io import BytesIO
from PIL import Image
from pydantic import BaseModel

from base.api.documents import Fragment, FragmentUri
from base.models.content import ContentText, PartLink
from base.resources.aff_body import AffBodyMedia, ObsBodySection, ObsChunk, ObsMedia
from base.resources.aff_file import AffFile
from base.resources.label import ResourceLabel
from base.resources.relation import Relation, Relation_, RelationLink, RelationParent
from base.strings.data import DataUri, MimeType
from base.strings.resource import ExternalUri, KnowledgeUri, Reference, ResourceUri
from base.utils.completion import estimate_tokens
from base.utils.sorted_list import bisect_insert, bisect_make

from knowledge.config import (
    IMAGE_MAX_SIDE_PX,
    IMAGE_MIN_SIDE_PX,
    IMAGE_PREFERRED_TYPE,
    IMAGE_MIME_TYPES,
)
from knowledge.domain.chunking import chunk_body
from knowledge.domain.labels import generate_standard_labels
from knowledge.domain.resolve import try_infer_locators
from knowledge.models.exceptions import IngestionError
from knowledge.models.storage_metadata import (
    MetadataDelta,
    MetadataDelta_,
    ObservedDelta,
    ResourceView,
)
from knowledge.models.storage_observed import (
    AnyBundle,
    AnyBundle_,
    BundleBody,
    BundleCollection,
    BundleFile,
)
from knowledge.server.context import KnowledgeContext, ObserveResult

logger = logging.getLogger(__name__)

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
    bundle: AnyBundle_
    labels: list[ResourceLabel]
    observed: ObservedDelta
    derived: list[AnyBundle_]
    should_cache: bool


async def ingest_observe_result(
    context: KnowledgeContext,
    resource_uri: ResourceUri,
    cached: ResourceView | None,
    metadata: MetadataDelta,
    observed: ObserveResult,
) -> IngestedResult:
    new_bundle: AnyBundle
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

    # Generate standard labels for `DocumentBundle`.
    # NOTE: `ResourceHistory.all_affordances` injects "description" from labels.
    # This avoids duplicates and allows refresh by `ResourceDelta.reset_labels`.
    # Think of descriptions *within* the bundle as "forced" by the connector.
    new_labels: list[ResourceLabel] = []
    if observed.option_labels and isinstance(new_bundle, BundleBody):
        new_labels = await generate_standard_labels(
            context=context,
            cached=cached.labels if cached else [],
            bundle=new_bundle,
        )

    bundle_info = new_bundle.info()
    observed_delta = ObservedDelta(
        suffix=new_bundle.uri.suffix,
        info_mime_type=bundle_info.mime_type,
        info_sections=bundle_info.sections,
        info_observations=bundle_info.observations,
        relations=_ingest_observe_relations(
            context,
            new_bundle,
            observed.relations,
            observed.option_relations_parent,
            observed.option_relations_link,
        ),
    )

    return IngestedResult(
        metadata=metadata,
        bundle=new_bundle,
        labels=new_labels,
        observed=observed_delta,
        derived=sorted(new_derived, key=lambda b: str(b.uri)),
        should_cache=observed.should_cache,
    )


def _ingest_observe_relations(
    context: KnowledgeContext,
    bundle: AnyBundle,
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
            for href in chunk.text.dep_links()
            if isinstance(href, KnowledgeUri) and context.should_backlink(href)
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

    image = Image.open(BytesIO(blob_data.as_bytes()))
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
        chunk_text = _shorten_text(chunk_text.strip(), SHREADSHEET_CHUNK_TRIMMED)
        chunks.append(
            ObsChunk.new(
                resource_uri=resource_uri,
                indexes=[index],
                text=ContentText.parse(chunk_text, mode="data"),
            )
        )

    return BundleBody.new(
        resource_uri=resource_uri,
        sections=sections,
        chunks=chunks,
        media=[],
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
