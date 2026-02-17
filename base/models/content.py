import base64
import re

from typing import Annotated, Any, Literal
from pydantic import BaseModel, Field, WrapSerializer

from base.strings.data import MimeType
from base.strings.resource import KnowledgeUri, Reference, REGEX_REFERENCE, WebUrl
from base.utils.markdown import (
    lstrip_newlines,
    markdown_normalize,
    markdown_split_code,
    strip_keep_indent,
)
from base.utils.sorted_list import bisect_make


##
## Blob
##


class ContentBlob(BaseModel, frozen=True):
    """
    Any binary content (images, audio, video, etc.) can be represented by this
    type.  When the format cannot be interpreted by the LLM (or too many blobs
    were provided), its `placeholder` can be used instead.

    NOTE: The `placeholder` is an alternative representation of the content, for
    example, an audio SRT or a detailed image description.
    """

    type: Literal["blob"] = "blob"
    uri: Reference
    placeholder: str | None
    mime_type: MimeType
    blob: str

    def render_placeholder(self) -> list[TextPart]:
        """
        When the MIME type is not supported by the LLM, or too many blobs were
        already included in the context, render a placeholder.
        """
        attributes: list[tuple[str, str]] = []
        if self.mime_type:
            attributes.append(("mimetype", self.mime_type))

        if not self.placeholder:
            return PartText.xml_open("blob", self.uri, attributes, self_closing=True)

        return [
            *PartText.xml_open("blob", self.uri, attributes),
            PartText.new(self.placeholder, "\n"),
            PartText.xml_close("blob"),
        ]

    def as_bytes(self) -> bytes | None:
        if self.blob.startswith("https://"):
            return None
        else:
            return base64.b64decode(self.blob)

    def download_url(self) -> str:
        if self.blob.startswith("https://"):
            return self.blob
        else:
            return f"data:{self.mime_type};base64,{self.blob}"


##
## Text - Parts
##


LinkMode = Literal["citation", "embed", "markdown", "plain"]
Sep = Literal["", "\n", "\n\n", "\n-force", "\n\n-force"]


class PartCode(BaseModel, frozen=True):
    """
    TODO: Support nested code blocks when given Markdown code:

    ```markdown
    First syntax:

    ````text
    converted to triple backticks in `PartCode.code`, but rendered as quadruple
    backticks in `PartCode.as_str()`.
    ````

    Second syntax:

    ~~~
    Kept as-is in `PartCode.code` and `PartCode.as_str()`.
    ~~~
    ```
    """

    type: Literal["code"] = "code"
    fence: Literal["```", "````", "~~~"]
    language: str | None
    code: str

    @staticmethod
    def new(
        code: str,
        language: str | None = None,
        fence: Literal["```", "````", "~~~"] | None = None,
    ) -> PartCode:
        # Pick the right fence, one that does not appear in the content.
        if fence is None:
            if "\n```" not in code:
                fence = "```"
            elif "\n~~~" not in code:
                fence = "~~~"
            else:
                raise ValueError("Cannot pick a valid fence for PartCode.")

        return PartCode(
            fence=fence,
            language=language,
            code=strip_keep_indent(code),
        )

    @staticmethod
    def parse(value: str) -> PartCode | None:
        value = strip_keep_indent(value)

        fence: Literal["```", "````", "~~~"]
        if value.startswith("````") and value.endswith("\n````"):
            fence = "````"
        elif value.startswith("```") and value.endswith("\n```"):
            fence = "```"
        elif value.startswith("~~~") and value.endswith("\n~~~"):
            fence = "~~~"
        else:
            return None

        language, code = (
            value.removeprefix(fence).removesuffix(fence).split("\n", maxsplit=1)
        )
        return PartCode.new(
            fence=fence,
            language=language.strip() or None,
            code=strip_keep_indent(code),
        )

    def separators(self) -> tuple[Sep, Sep]:
        return "\n\n", "\n\n"

    def as_str(self) -> str:
        language = self.language or ""
        return f"```{language}\n{self.code}\n```"


class PartHeading(BaseModel, frozen=True):
    type: Literal["heading"] = "heading"
    level: int
    text: str

    @staticmethod
    def new(level: int, text: str) -> PartHeading:
        return PartHeading(level=level, text=text)

    @staticmethod
    def parse(value: str) -> PartHeading:
        assert value.startswith("#")
        heading_marker, heading_text = value.split(" ", maxsplit=1)
        heading_level = len(heading_marker)
        assert 1 <= heading_level <= 6  # noqa: PLR2004
        return PartHeading(level=heading_level, text=heading_text.strip())

    def separators(self) -> tuple[Sep, Sep]:
        return "\n\n", "\n\n-force"

    def as_str(self) -> str:
        return "#" * self.level + " " + self.text


class PartLink(BaseModel, frozen=True):
    type: Literal["link"] = "link"
    mode: LinkMode
    label: str | None
    href: Reference

    @staticmethod
    def new(
        mode: LinkMode,
        label: str | None,
        href: Reference,
    ) -> PartLink:
        label = (
            " ".join(label.replace("[", "").replace("]", "").split()) if label else ""
        )
        return PartLink(mode=mode, label=label or None, href=href)

    @staticmethod
    def try_new(mode: LinkMode, label: str | None, href: str) -> PartLink | None:
        if not (reference := Reference.try_decode(href)):
            return None
        return PartLink.new(mode=mode, label=label, href=reference)

    @staticmethod
    def try_parse(value: str) -> PartLink | None:
        mode: LinkMode = "plain"
        label: str = ""
        href: str = ""

        value = value.strip()
        if value.startswith("[^") and value.endswith("]"):
            mode = "citation"
            href = value[2:-1]
            if "|" in href:
                href, label = href.split("|", 1)
        elif value.startswith("![") and value.endswith(")") and "](" in value:
            mode = "embed"
            label, href = value[2:-1].rsplit("](", 1)
        elif value.startswith("<") and value.endswith(">"):
            mode = "markdown"
            href = value[1:-1]
        elif value.startswith("[") and value.endswith(")") and "](" in value:
            mode = "markdown"
            label, href = value[1:-1].split("](", 1)
        else:
            href = value

        if not (reference := Reference.try_decode(href)):
            return None

        return PartLink.new(mode=mode, label=label, href=reference)

    @staticmethod
    def stub(mode: LinkMode, href: str, label: str | None = None) -> PartLink:
        if href and not href.startswith(("https://", "ndk://")):
            href = f"ndk://stub/-/{href}"
        return PartLink.new(mode=mode, label=label, href=Reference.decode(href))

    def separators(self) -> tuple[Sep, Sep]:
        if self.mode == "embed":
            return "\n\n", "\n\n"
        return "", ""

    def as_str(self) -> str:
        match self.mode:
            case "citation":
                return (
                    f"[^{self.href}|{self.label}]" if self.label else f"[^{self.href}]"
                )
            case "embed":
                return f"![{self.label or ''}]({self.href})"
            case "markdown":
                if self.label:
                    return f"[{self.label}]({self.href})"
                else:
                    return f"<{self.href}>"
            case "plain":
                return str(self.href)


class PartPageNumber(BaseModel, frozen=True):
    type: Literal["pagenumber"] = "pagenumber"
    page_number: int

    @staticmethod
    def parse(value: str) -> PartPageNumber:
        assert value.startswith("{")
        assert "}" in value
        page_number = value[1:].split("}", maxsplit=1)[0]
        return PartPageNumber(page_number=int(page_number))

    def separators(self) -> tuple[Sep, Sep]:
        return "\n\n-force", "\n\n-force"

    def as_str(self) -> str:
        page_num = str(self.page_number)
        return "{" + page_num + "}------------------------------------------------"


class PartText(BaseModel, frozen=True):
    type: Literal["text"] = "text"
    text: str
    lsep: Sep
    rsep: Sep

    @staticmethod
    def new(text: str, lsep: Sep = "", rsep: Sep | None = None) -> PartText:
        rsep = lsep if rsep is None else rsep
        return PartText(text=text, lsep=lsep, rsep=rsep)

    @staticmethod
    def xml_open(
        tag: str,
        uri: Reference | None = None,
        attributes: list[tuple[str, str]] | None = None,
        self_closing: bool = False,
    ) -> list[TextPart]:
        attributes = attributes or []
        attributes_str = " ".join(
            f'{key}="{clean_value}"'
            for key, value in attributes
            if (clean_value := " ".join(value.split()) if "\n" in value else value)
        )
        if attributes_str:
            attributes_str = " " + attributes_str
        suffix = f"{attributes_str} />" if self_closing else f"{attributes_str}>"

        if uri:
            return [
                PartText.new(f'<{tag} uri="', "\n", ""),
                PartLink(mode="plain", href=uri, label=None),
                PartText.new(f'"{suffix}', "", "\n-force"),
            ]
        else:
            return [
                PartText.new(f"<{tag}{suffix}", "\n", "\n-force"),
            ]

    @staticmethod
    def xml_close(tag: str) -> PartText:
        return PartText.new(f"</{tag}>", "\n-force", "\n")

    @staticmethod
    def separator(sep1: Sep, sep2: Sep | None = None) -> PartText:
        sep: Sep = max(sep1, sep2, key=len) if sep2 else sep1
        return PartText.new("", sep)

    def separators(self) -> tuple[Sep, Sep]:
        return self.lsep, self.rsep

    def as_str(self) -> str:
        return self.text


TextPart = PartCode | PartHeading | PartLink | PartPageNumber | PartText
TextPart_ = Annotated[TextPart, Field(discriminator="type")]


def _append_part_mut(parts: list[TextPart], part: TextPart) -> None:
    """
    Most content parts are added as-is.  However, when adding text, we try
    to merge it with the last textual item and join their `sep`.
    """
    if len(parts) == 0:
        parts.append(part)
        return

    prev_part: TextPart = parts[-1]
    sep: Sep = max(prev_part.separators()[1], part.separators()[0], key=len)

    # Merge consecutive text parts using their separators.
    if isinstance(part, PartText) and isinstance(prev_part, PartText):
        # NOTE: When the texts have no separator between them, keep all
        # whitespace.  Otherwise, remove:
        # - ALL trailing whitespace from `prev_part`,
        # - But only leading NEWLINES from `part`, to preserve spaces on the
        #   first line (e.g., indentation).
        if sep == "":
            prev_stripped = prev_part.text
            part_stripped = part.text
        else:
            prev_stripped = prev_part.text.rstrip()
            part_stripped = lstrip_newlines(part.text)

        # NOTE: Use "\n-force" to force a single newline between parts, which is
        # useful, e.g., when an "embed" link or a "code" part is wrapped between
        # XML tags.
        actual_sep = "\n" if sep == "\n-force" else sep
        parts[-1] = PartText.new(
            f"{prev_stripped}{actual_sep}{part_stripped}",
            prev_part.lsep,
            part.rsep,
        )
        return

    if isinstance(prev_part, PartText) and sep:
        parts[-1] = PartText.new(
            prev_part.text.rstrip(),
            prev_part.lsep,
            prev_part.rsep,
        )

    elif isinstance(part, PartText) and sep:
        part = PartText.new(
            lstrip_newlines(part.text),
            part.lsep,
            part.rsep,
        )

    parts.append(part)


##
## Content
##


class ContentText(BaseModel, frozen=True):
    """
    Any textual content can be represented by this type (usually as Markdown).
    This is the "parsed" content, where links and entities have been extracted,
    but where resources have not been embedded.

    NOTE: Ignores links from code blocks and expressions, allowing them to act
    as escapes in documents, prompts, and LLM completions.
    """

    type: Literal["text"] = "text"
    parts: list[TextPart_]
    plain: str | None

    def __bool__(self) -> bool:
        if len(self.parts) == 0:
            return False
        elif len(self.parts) > 1:
            return True
        else:
            return self.parts[0].type != "text" or self.parts[0].text != ""

    def as_compact(self, mode: Literal["markdown", "data"]) -> dict[str, Any]:
        return {"text": self.as_str(), "mode": "markdown"}

    def as_str(self, ignore_plain: bool = False) -> str:
        if self.plain and not ignore_plain:
            return self.plain

        content: str = ""
        prev_sep: Sep = ""

        for part in self.parts:
            part_text = part.as_str()
            part_lsep, part_rsep = part.separators()
            sep = max(prev_sep, part_lsep, key=len)
            actual_sep = sep.removesuffix("-force")
            if content and actual_sep:
                content += actual_sep
            content += part_text

            prev_sep = part_rsep

        return content

    ##
    ## Extension
    ##

    @staticmethod
    def new(
        parts: list[TextPart] | None = None,
        plain: str | None = None,
    ) -> ContentText:
        parts = parts or []

        result: list[TextPart] = []
        for part in parts:
            _append_part_mut(result, part)

        return ContentText(parts=result, plain=plain)

    @staticmethod
    def new_embed(uri: KnowledgeUri, label: str | None = None) -> ContentText:
        return ContentText(
            parts=[PartLink.new("embed", label, uri)],
            plain=f"![{label}]({uri})",
        )

    @staticmethod
    def new_plain(text: str, sep: Sep = "\n") -> ContentText:
        return ContentText(parts=[PartText.new(text, sep)], plain=text)

    @staticmethod
    def join(
        contents: list[ContentText],
        sep: Sep = "\n",
    ) -> ContentText:
        if not contents:
            return ContentText(parts=[], plain=None)

        result_parts: list[TextPart] = contents[0].parts.copy()
        for content in contents[1:]:
            if sep:
                _append_part_mut(result_parts, PartText.separator(sep))
            if content.parts:
                _append_part_mut(result_parts, content.parts[0])
                result_parts.extend(content.parts[1:])
            result_parts.extend(content.parts)

        return ContentText(parts=result_parts, plain=None)

    ##
    ## Parsing
    ##

    @staticmethod
    def parse(
        value: str,
        *,
        mode: Literal["data", "markdown"] = "markdown",
        default_link: Literal["markdown", "plain"] = "plain",
    ) -> ContentText:
        """
        Use mode "data" to parse non-Markdown textual content which may contain
        references, such as JSON or CSV files.

        If you instead wish to represent Markdown-style links, images, and code
        blocks (in which references are escaped), use mode "markdown".
        """
        value = value.replace("\r\n", "\n")
        parts: list[TextPart] = []

        if mode == "data":
            parts = _parse_plain_links(value, "plain")
        else:
            parts = _parse_markdown(value, default_link)

        return ContentText.new(parts, value)

    ##
    ## Queries
    ##

    def dep_links(self) -> list[Reference]:
        return bisect_make(
            [
                part.href
                for part in self.parts
                if isinstance(part, PartLink) and part.mode != "embed"
            ],
            key=str,
        )

    def dep_embeds(self) -> list[Reference]:
        return bisect_make(
            [
                part.href
                for part in self.parts
                if isinstance(part, PartLink) and part.mode == "embed"
            ],
            key=str,
        )

    def only_embed(self) -> Reference | None:
        return (
            self.parts[0].href
            if len(self.parts) == 1
            and isinstance(self.parts[0], PartLink)
            and self.parts[0].mode == "embed"
            else None
        )

    def parts_link(self) -> list[PartLink]:
        return [part for part in self.parts if isinstance(part, PartLink)]


def wrap_content_text_plain(value: Any, handler, info) -> Any:
    serialized = handler(value, info)
    if serialized.get("plain") is None and isinstance(value, ContentText):
        serialized["plain"] = value.as_str()
    return serialized


ContentText_ = Annotated[ContentText, WrapSerializer(wrap_content_text_plain)]


##
## Helpers
##


REGEX_MARKDOWN_HEADING = re.compile(r"\n(#+ .+|\{\d+\}\-{48})\n")
REGEX_MARKDOWN_LINK = re.compile(
    rf"(\"(?:{REGEX_REFERENCE})\""  # Quoted URL, e.g., in XML attribute.
    rf"|<(?:{REGEX_REFERENCE})>"  # Markdown link without label.
    rf"|!?\[[^\]]*\]\((?:{REGEX_REFERENCE})\)"  # Markdown image or link with (optional) label.
    rf"|\[\^(?:{REGEX_REFERENCE})(?:\|[^\]]+)?\]"  # Citation.
    r")"
)


def _parse_markdown(
    value: str,
    default_link: Literal["markdown", "plain"],
) -> list[TextPart]:
    parts: list[TextPart] = []
    partial_text: str = ""

    for part_type, part_text in markdown_split_code(value, True):
        match part_type:
            # Insert code blocks as-is, escaping references.
            case "code_block":
                parts.extend(_parse_markdown_text(partial_text.rstrip(), default_link))
                partial_text = ""

                code_block = PartCode.parse(part_text)
                assert code_block is not None
                parts.append(code_block)

            # Insert code expressions containing references as-is, escaping
            # them, but otherwise, add them to the partial text, to support
            # links whose label contains a code expression.
            case "code_expr":
                if _find_plain_references(part_text):
                    if partial_text:
                        parts.extend(_parse_markdown_text(partial_text, default_link))
                        partial_text = ""
                    parts.append(PartText.new(part_text))
                else:
                    partial_text += part_text

            # Extract references from non-code chunks of Markdown.
            case "text":
                partial_text += part_text

    parts.extend(_parse_markdown_text(partial_text.rstrip(), default_link))

    return parts


def _parse_markdown_text(  # noqa: C901
    chunk_markdown: str,
    default_link: Literal["markdown", "plain"],
) -> list[TextPart]:
    if not chunk_markdown:
        return []

    result: list[TextPart] = []
    chunk_markdown = markdown_normalize(chunk_markdown)
    if not strip_keep_indent(chunk_markdown):
        return []

    # Split such that even items are Markdown and odd items are potential links.
    # On `ContentText.append`, we keep the exising whitespace, unless the part
    # is an embed.
    markdown_and_headings: list[str] = REGEX_MARKDOWN_HEADING.split(
        f"\n\n{chunk_markdown}\n\n"
    )
    for i, text_or_heading in enumerate(markdown_and_headings):
        if i % 2 == 1:
            if text_or_heading.startswith("{"):
                result.append(PartPageNumber.parse(text_or_heading))
            elif text_or_heading.startswith("#"):
                result.append(PartHeading.parse(text_or_heading))
            continue

        section_text: str = text_or_heading
        if i == 0:
            section_text = lstrip_newlines(section_text)
        if i == len(markdown_and_headings) - 1:
            section_text = section_text.rstrip("\n")

        if not section_text.strip():
            continue

        markdown_and_links: list[str] = REGEX_MARKDOWN_LINK.split(section_text)
        for chunk_index, markdown_or_link in enumerate(markdown_and_links):
            # We try to parse link candidates, treating non-links or invalid
            # ones as plain text.
            if chunk_index % 2 == 1:
                result.extend(_parse_markdown_link(markdown_or_link))
                continue

            # We keep the whitespace around Markdown links, but not embeds.
            # Only discard completely empty (or only newlines) text blocks.
            if not strip_keep_indent(markdown_or_link):
                continue

            # We extract plain references, not wrapped in a special part format,
            # as `PartReference`.  They can then be handled by the caller.
            result.extend(_parse_plain_links(markdown_or_link, default_link))

    return result


def _parse_markdown_link(part: str) -> list[TextPart]:
    try:
        if parsed := PartLink.try_parse(part):
            # Edge-case: a link inside parentheses, e.g., "(see [label](href))".
            if isinstance(parsed, PartLink) and isinstance(parsed.href, WebUrl):
                href = str(parsed.href)
                clean_href = _clean_reference_str(href)
                if symbols_suffix := href.removeprefix(clean_href):
                    parsed = PartLink(
                        mode=parsed.mode,
                        label=parsed.label,
                        href=WebUrl.decode(clean_href),
                    )
                    return [parsed, PartText.new(symbols_suffix)]

            return [parsed]

        # NOTE: Since such links usually appear as HTML attributes, we treat
        # them as "reference" links.
        if (
            part.startswith('"')
            and part.endswith('"')
            and (reference := Reference.try_decode(part[1:-1]))
        ):
            return [
                PartText.new('"'),
                PartLink(mode="plain", label=None, href=reference),
                PartText.new('"'),
            ]

    except ValueError:
        pass  # just return the original text on invalid link candidates

    return [PartText.new(part)]


def _parse_plain_links(
    value: str,
    default_link: Literal["markdown", "plain"],
) -> list[TextPart]:
    parts: list[TextPart] = []

    remaining = value
    references = _find_plain_references(remaining)

    for reference in references:
        text_part, remaining = remaining.split(str(reference), maxsplit=1)
        if text_part:
            parts.append(PartText.new(text_part))
        parts.append(PartLink(mode=default_link, label=None, href=reference))

    if remaining:
        parts.append(PartText.new(remaining))

    return parts


def _find_plain_references(text: str) -> list[Reference]:
    """
    Get all well-formed references that appear in the text.

    The caller is responsible for:

    - Cleaning the argument by stripping parts of the text that should not be
      searched, such as code blocks, prior to calling this function.
    - Filtering the result to keep only references that they care about.

    NOTE: The reference is well-formed, but might not exist.

    NOTE: Repeated URLs are not deduplicated.  They are returned in the order of
    their appearance in the text, allowing the caller to extract them via:

    ```
    text_and_urls: list[AnyReference|str] = []
    remaining = text
    for reference in find_plain_references(text):
        text_part, remaining = remaining.split(str(reference), maxsplit=1)
        text_and_urls.append(text_part)
        text_and_urls.append(reference)
    if remaining:
        text_and_urls.append(remaining)
    ```
    """
    return [
        reference
        for reference_match in re.findall(REGEX_REFERENCE, text)
        if (clean_reference := _clean_reference_str(reference_match))
        and (reference := Reference.try_decode(clean_reference))
    ]


def _clean_reference_str(reference: str) -> str:
    """
    Remove trailing characters, such as commas or periods, from a reference
    candidate matched by the regex (since these naturally occur in text).
    """
    if reference.startswith("ndk://"):
        return reference.rstrip(".")
    else:
        reference = reference.rstrip("!$&(+,.:<>?")
        max_closing_parens = reference.count("(")
        while reference.count(")") > max_closing_parens and reference.endswith(")"):
            reference = reference[:-1]
        return reference
