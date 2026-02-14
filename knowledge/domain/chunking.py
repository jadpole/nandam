import asyncio
import re

from dataclasses import dataclass

from base.resources.aff_body import ObsBodySection, ObsChunk, ObsMedia
from base.models.content import (
    ContentText,
    PartCode,
    PartHeading,
    PartLink,
    PartPageNumber,
    PartText,
    TextPart,
)
from base.strings.resource import ResourceUri
from base.utils.completion import estimate_tokens

from knowledge.models.storage_observed import BundleBody


CHUNKING_THRESHOLD_TOKENS = 20_000
"""
How many tokens are required in a body before we break it into multiple chunks.
"""

MAX_CHUNK_TOKENS = 8000
"""
Once we decide to chunk a body, how large the *text* of each chunk can be.

The "optimal" size is 10k per "batch of sources" sent to the LLM, to maximum
effectiveness on reminders between chunks.  We ignore headings and XML tags,
since the actual chunk size is not exactly equal to that limit.

Moreover, so that agents can rewrite chunks, we limit the chunk size to the
typicaly OUTPUT token limit of contemporary LLMs: 8192, including scaffolding,
or roughly 8k tokens for the chunk's text content.
"""


def unittest_configure(
    chunking_threshold_tokens: int = 20_000,
    max_chunk_tokens: int = 8000,
) -> None:
    global CHUNKING_THRESHOLD_TOKENS  # noqa: PLW0603
    global MAX_CHUNK_TOKENS  # noqa: PLW0603
    CHUNKING_THRESHOLD_TOKENS = chunking_threshold_tokens
    MAX_CHUNK_TOKENS = max_chunk_tokens


##
## Chunking
##


async def chunk_body(
    resource_uri: ResourceUri,
    text: ContentText,
    media: list[ObsMedia],
) -> BundleBody:
    """
    NOTE: Since chunking is a CPU-intensive operation, run it in another thread
    to avoid blocking the main event loop.
    """
    return await asyncio.to_thread(chunk_body_sync, resource_uri, text, media)


def chunk_body_sync(
    resource_uri: ResourceUri,
    text: ContentText,
    media: list[ObsMedia],
) -> BundleBody:
    # Documents under the threshold (excluding images) remain a single chunk.
    if estimate_tokens(text.as_str(), 0) <= CHUNKING_THRESHOLD_TOKENS:
        return BundleBody.make_single(resource_uri=resource_uri, text=text, media=media)

    # Break up the content into a hierarchy of headings and group these sections
    # into chunks typically smaller than `MAX_CHUNK_TOKENS`.
    parts = _split_chunk_parts(text)
    root_group = ChunkGroup.make_hierarchy(None, parts)
    root_opimized = _optimize_chunk_group(root_group)

    # Translate the hierarchy of chunks into a "body".
    output_sections: list[ObsBodySection] = []
    output_chunks: list[ObsChunk] = []
    _chunk_group_to_body(
        resource_uri,
        output_sections,
        output_chunks,
        root_opimized,
        parent_indexes=[],
        self_index=0,
    )

    return BundleBody.new(
        resource_uri=resource_uri,
        sections=output_sections,
        chunks=output_chunks,
        media=media,
    )


def _chunk_group_to_body(
    resource_uri: ResourceUri,
    output_sections: list[ObsBodySection],
    output_chunks: list[ObsChunk],
    chunk_group: ChunkGroup,
    parent_indexes: list[int],
    self_index: int,
) -> int:
    num_children: int = 0

    # When the section is merely a list of sub-groups...
    if chunk_group.groups and not chunk_group.heading:
        child_index = self_index
        for child_group in chunk_group.groups:
            num_group_children = _chunk_group_to_body(
                resource_uri,
                output_sections,
                output_chunks,
                child_group,
                parent_indexes,
                child_index,
            )
            num_children += num_group_children
            child_index += num_group_children

    # When the group is a section with multiple subgroups...
    elif chunk_group.heading and len(chunk_group.groups) > 1:
        section_indexes = [*parent_indexes, self_index]
        child_index = 0
        for child_group in chunk_group.groups:
            child_uris = _chunk_group_to_body(
                resource_uri,
                output_sections,
                output_chunks,
                child_group,
                section_indexes,
                child_index,
            )
            child_index += child_uris

        output_sections.append(
            ObsBodySection(
                indexes=section_indexes,
                heading=chunk_group.heading.text,
            )
        )
        num_children += 1

    # When the group is a single chunk...
    # TODO: Include breadcrumbs and `chunk_group.heading` in the chunk?
    elif chunk_group.chunks:
        output_chunks.append(
            ObsChunk.new(
                resource_uri,
                indexes=[*parent_indexes, self_index],
                text=chunk_group.render(),
            )
        )
        num_children += 1

    return num_children


##
## Chunk atoms
##


BUFFER_HEADING: int = 3
"""
Account for '#' prefix and newlines after the heading.
"""

BUFFER_PARAGRAPH: int = 1
"""
Account for newlines after the paragraph.
"""

REGEX_PARAGRAPH = re.compile(r"(?:^\s*\n){2,}")


@dataclass(kw_only=True)
class ChunkPart:
    parts: list[TextPart]
    num_tokens: int

    def as_heading(self) -> PartHeading | None:
        if len(self.parts) == 1 and isinstance(self.parts[0], PartHeading):
            return self.parts[0]
        else:
            return None


def _split_chunk_parts(text: ContentText) -> list[ChunkPart]:
    result: list[ChunkPart] = []
    partial_parts: list[TextPart] = []
    partial_tokens: int = 0

    def flush_partial() -> None:
        nonlocal result, partial_parts, partial_tokens
        if partial_parts:
            result.append(
                ChunkPart(
                    parts=partial_parts,
                    num_tokens=partial_tokens + BUFFER_PARAGRAPH,
                )
            )
            partial_parts = []
            partial_tokens = 0

    for part in text.parts:
        # Represent "naturally atomic" parts as their own chunks.
        if isinstance(part, PartCode | PartHeading | PartPageNumber) or (
            isinstance(part, PartLink) and part.mode == "embed"
        ):
            flush_partial()

            # NOTE: Do not consider embed tokens while optimizing the chunks.
            # The embeds DO matter in `ChunkPart.num_tokens`, however, so this
            # is only a heuristic to get compact chunks.
            part_tokens = (
                0
                if isinstance(part, PartLink)
                else estimate_tokens(part.as_str(), 0) + BUFFER_PARAGRAPH
            )
            result.append(ChunkPart(parts=[part], num_tokens=part_tokens))

        # Non-embed links are always part of the previous paragraph.
        elif isinstance(part, PartLink):
            partial_parts.append(part)
            partial_tokens += estimate_tokens(part.as_str(), 0)

        # Split plain text into paragraphs, so that we can group contiguous text
        # into the same `ChunkPart`.
        else:
            _: PartText = part
            if part.lsep in ("\n\n", "\n\n-force"):
                flush_partial()

            paragraphs = [p for p in REGEX_PARAGRAPH.split(part.text) if p.strip()]
            if len(paragraphs) < 2:  # noqa: PLR2004
                # Skip whitespace between embeds.
                # TODO: Move into `ContentText.new`?
                if not paragraphs and not part.text.strip():
                    continue
                # Keep as-is when there the text has at most one paragraph.
                # Notably, preserve whitespace between links (== 0).
                partial_parts.append(part)
                partial_tokens += estimate_tokens(part.as_str(), 0)
            else:
                partial_parts.append(PartText.new(paragraphs[0], part.lsep, "\n\n"))
                partial_tokens += estimate_tokens(paragraphs[0], 0)
                flush_partial()

                result.extend(
                    ChunkPart(
                        parts=[PartText.new(middle_paragraph, "\n\n")],
                        num_tokens=(
                            estimate_tokens(middle_paragraph, 0) + BUFFER_PARAGRAPH
                        ),
                    )
                    for middle_paragraph in paragraphs[1:-1]
                )

                partial_parts.append(PartText.new(paragraphs[-1], "\n\n", part.rsep))
                partial_tokens += estimate_tokens(paragraphs[-1], 0)

            if part.rsep in ("\n\n", "\n\n-force") and partial_parts:
                flush_partial()

    flush_partial()

    # TODO:
    # if partial_tokens > MAX_CHUNK_TOKENS:
    #     *heuristic_paragraphs, remaining = REGEX_PARAGRAPH.split(part.as_str())
    #     result.extend(_split_chunk_too_large(part.as_str()))

    return result


##
## Chunk hierarchy
##


@dataclass(kw_only=True)
class ChunkGroup:
    """
    A group of chunk parts and sub-groups, which cannot be split, but may be
    joined with other groups and whose children can be reorganized.  This allows
    us to think of combining chunks at a higher level.

    NOTE: MUST set EITHER `groups` OR `parts`, never both.
    """

    heading: PartHeading | None
    groups: list[ChunkGroup]
    chunks: list[ChunkPart]

    def __post_init__(self) -> None:
        if self.groups and self.chunks:
            raise ValueError("cannot set both children and parts in ChunkGroup")

    @property
    def num_tokens(self) -> int:
        heading_tokens = (
            estimate_tokens(self.heading.text, 0) + BUFFER_HEADING
            if self.heading
            else 0
        )
        return (
            heading_tokens
            + sum(g.num_tokens for g in self.groups)
            + sum(p.num_tokens for p in self.chunks)
        )

    @staticmethod
    def from_groups(
        heading: PartHeading | None,
        groups: list[ChunkGroup],
    ) -> ChunkGroup:
        return ChunkGroup(heading=heading, groups=groups, chunks=[])

    @staticmethod
    def from_chunks(
        heading: PartHeading | None,
        chunks: list[ChunkPart],
    ) -> ChunkGroup:
        return ChunkGroup(heading=heading, groups=[], chunks=chunks)

    @staticmethod
    def from_parts_bounded(
        heading: PartHeading | None,
        chunks: list[ChunkPart],
    ) -> ChunkGroup:
        """
        Pack parts into chunks up to MAX_CHUNK_TOKENS each.
        NOTE: Should only be called when `chunks` contains no headings.
        """
        if sum(p.num_tokens for p in chunks) < MAX_CHUNK_TOKENS:
            return ChunkGroup.from_chunks(heading, chunks)

        subgroups: list[ChunkGroup] = []
        partial_chunks: list[ChunkPart] = []
        partial_tokens: int = 0

        for part in chunks:
            if partial_tokens and partial_tokens + part.num_tokens > MAX_CHUNK_TOKENS:
                subgroups.append(ChunkGroup.from_chunks(None, partial_chunks))
                partial_chunks = []
                partial_tokens = 0

            partial_chunks.append(part)
            partial_tokens += part.num_tokens

        if partial_chunks:
            subgroups.append(ChunkGroup.from_chunks(None, partial_chunks))

        return ChunkGroup.from_groups(heading, subgroups)

    @staticmethod
    def make_hierarchy(
        heading: PartHeading | None,
        chunks: list[ChunkPart],
    ) -> ChunkGroup:
        heading_level = min(
            (h.level for chunk in chunks if (h := chunk.as_heading())),
            default=None,
        )

        # If there are no headings in `chunks`, then we group the chunks into
        # groups of the required size.
        if not heading_level:
            return ChunkGroup.from_parts_bounded(heading, chunks)

        # Otherwise, create one section per heading at the next level and keep
        # building the hierarchy recursively.
        group_children: list[ChunkGroup] = []
        section_heading: PartHeading | None = None
        section_parts: list[ChunkPart] = []

        def flush_section() -> None:
            nonlocal group_children, section_heading, section_parts
            if section_parts:
                group_children.append(
                    ChunkGroup.make_hierarchy(section_heading, section_parts)
                )
            elif section_heading:
                group_children.append(ChunkGroup.from_chunks(section_heading, []))
            section_heading = None
            section_parts = []

        for chunk in chunks:
            if (c_heading := chunk.as_heading()) and c_heading.level == heading_level:
                flush_section()
                section_heading = c_heading
                section_parts = []
            else:
                section_parts.append(chunk)

        flush_section()

        return ChunkGroup.from_groups(heading, group_children)

    @staticmethod
    def join(groups: list[ChunkGroup]) -> ChunkGroup:
        if len(groups) == 1:
            return groups[0]
        else:
            return ChunkGroup.from_chunks(
                None,
                [
                    chunk
                    for group in groups
                    for chunk in group.flatten(omit_heading=False)
                ],
            )

    def render(self) -> ContentText:
        parts: list[TextPart] = []
        for chunk in self.flatten(omit_heading=False):
            parts.extend(chunk.parts)
        return ContentText.new(parts=parts)

    def flatten(self, omit_heading: bool = False) -> list[ChunkPart]:
        flattened: list[ChunkPart] = []
        if self.heading and not omit_heading:
            flattened.append(
                ChunkPart(
                    parts=[self.heading],
                    num_tokens=estimate_tokens(self.heading.text, 0) + BUFFER_HEADING,
                )
            )
        for group in self.groups:
            flattened.extend(group.flatten(omit_heading=False))

        flattened.extend(self.chunks)
        return flattened

    def contains_section(self) -> bool:
        return bool(self.heading or any(g.contains_section() for g in self.groups))


##
## Chunk optimization
##


def _optimize_chunk_group(group: ChunkGroup) -> ChunkGroup:
    """
    Reorganize chunk groups to minimize chunks while preserving meaningful structure.

    After optimization, a group is in one of two states:
    - Flat chunk: `num_tokens <= MAX_CHUNK_TOKENS`, all content in `chunks`
    - Section: `num_tokens > MAX_CHUNK_TOKENS`, children in `groups`

    This invariant simplifies downstream processing: small groups are always
    flat chunks, large groups are always sections with children.
    """
    # Fits in one chunk → flatten everything (including nested structure).
    if group.num_tokens <= MAX_CHUNK_TOKENS:
        return ChunkGroup.from_chunks(group.heading, group.flatten(omit_heading=True))

    # Already flat or no sections to reorganize.
    if group.chunks or not group.contains_section():
        return group

    # Recursively optimize all subgroups, then pack small neighbors together.
    optimized = [_optimize_chunk_group(g) for g in group.groups]
    packed = _pack_neighboring_chunks(optimized)

    return ChunkGroup.from_groups(group.heading, packed)


def _pack_neighboring_chunks(groups: list[ChunkGroup]) -> list[ChunkGroup]:
    """
    Bin-pack neighboring small groups into combined chunks.

    After `_optimize_chunk_group`, each group is either:
    - A flat chunk (num_tokens <= MAX_CHUNK_TOKENS) → can be packed
    - A section (num_tokens > MAX_CHUNK_TOKENS) → must stay separate

    Small neighbors are merged until they'd exceed MAX_CHUNK_TOKENS.
    Large groups act as "barriers" that flush pending small groups.
    """
    result: list[ChunkGroup] = []
    pending: list[ChunkGroup] = []
    pending_tokens: int = 0

    def flush() -> None:
        nonlocal pending, pending_tokens
        if pending:
            result.append(ChunkGroup.join(pending))
            pending = []
            pending_tokens = 0

    for group in groups:
        is_small = group.num_tokens <= MAX_CHUNK_TOKENS

        if not is_small:
            flush()
            result.append(group)
        elif pending_tokens + group.num_tokens > MAX_CHUNK_TOKENS:
            flush()
            pending = [group]
            pending_tokens = group.num_tokens
        else:
            pending.append(group)
            pending_tokens += group.num_tokens

    flush()
    return result
