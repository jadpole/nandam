import re

from html import unescape
from html2text import HTML2Text
from typing import Literal

from base.utils.sorted_list import bisect_insert


##
## Code blocks and expressions
##


REGEX_MARKDOWN_CODE_BLOCK = r"\n(```[A-Za-z0-9_\-]*\n.+?\n```)\n"
REGEX_MARKDOWN_CODE_EXPR = r"(`[^`\s][^`]*?[^`\s]`|`[^`\s]`)"

SplitCodeMode = Literal["code_block", "code_expr", "text"]


def markdown_split_code(
    markdown: str,
    split_exprs: bool,
) -> "list[tuple[SplitCodeMode, str]]":
    code_block_regex = re.compile(REGEX_MARKDOWN_CODE_BLOCK, re.DOTALL)
    code_expr_regex = re.compile(REGEX_MARKDOWN_CODE_EXPR)
    parts: list[tuple[SplitCodeMode, str]] = []

    # Add whitespace around the Markdown so the regex matches.
    if markdown.startswith("```"):
        markdown = f"\n{markdown}"
    if markdown.endswith("```"):
        markdown = f"{markdown}\n"

    # Add newlines between consecutive code blocks for proper splitting.
    markdown = markdown.replace("```\n```", "```\n\n```")

    # Split such that even index is "text" and odd is "code_blocks".
    text_and_code_blocks: list[str] = code_block_regex.split(markdown)
    for i, text_or_code_block in enumerate(text_and_code_blocks):
        # Insert code blocks as-is
        if i % 2 == 1:
            parts.append(("code_block", text_or_code_block))
            continue

        # Treat code blocks somewhat like embeds, stripping whitespace from
        # preceding and subsequent Markdown, but preserving indentation.
        if not text_or_code_block.strip():
            continue  # discard empty text blocks

        if not split_exprs:
            parts.append(("text", text_or_code_block))
            continue

        # Split such that even index is "text" and odd is "code_expr".
        text_and_code_exprs: list[str] = code_expr_regex.split(text_or_code_block)
        for j, text_or_code_expr in enumerate(text_and_code_exprs):
            # Unlike code blocks, we keep the whitespace around expressions
            # and only discard completely empty text chunks.
            if not text_or_code_expr:
                continue

            parts.append(
                ("code_expr", text_or_code_expr)
                if j % 2 == 1
                else ("text", text_or_code_expr)
            )

    return parts


def strip_keep_identation(text: str) -> str:
    return lstrip_newlines(text.rstrip())


def lstrip_newlines(text: str) -> str:
    while "\n" in text:
        first_line, remaining = text.split("\n", maxsplit=1)
        if not first_line or first_line.isspace():
            text = remaining
        else:
            break

    return text


##
## Normalize
##


def markdown_normalize(content: str) -> str:
    """
    Normalize the content of a Markdown document, so it can be safely used by
    downstream Nandam services and displayed by clients.

    - Completely remove emoji.
    - Replace `<img src="..." />` by `![](...)`, so images can easily be removed
      from LLM completions rendered on the frontend (for security).
    - Remove all other image tags (without "src"), since they may be adversarial
      attempts to leak information.

    NOTE: Ignore the `<img alt="..." />` attribute to keep the logic simple.

    NOTE: Attempt to preserve the original whitespace AROUND the content, so you
    may call `strip_keep_indentation` on the result if that isn't what you need.
    However, discard trailing whitespace INSIDE the content.
    """
    symbols = re.compile(
        "["
        "\U00002100-\U000027bf"  # lots of symbols
        "\U0001d800-\U0001ffff"  # emoji and unassigned
        "]+",
        flags=re.UNICODE,
    )
    content = symbols.sub(r"", content)  # strip symbols
    content = content.replace("\u0000", "??")  # NUL byte when cannot read glyph
    content = content.replace("\r\n", "\n")  # standardize newlines

    # Replace `<img>` tags by Markdown images.
    pattern_double = r'<img\s+[^>]*src="([^"]+)"[^>]*>'
    pattern_single = r"<img\s+[^>]*src='([^']+)'[^>]*>"
    content = re.sub(
        f"{pattern_double}|{pattern_single}",
        lambda m: f"![]({m.group(1)})",
        content,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    # Remove invalid `<img>` tags.
    pattern_adversarial = r"<img\s+[^>]*>"
    content = re.sub(
        pattern_adversarial,
        "",
        content,
        flags=re.IGNORECASE | re.MULTILINE,
    )

    # Trim trailing whitespace on each line, except for the last one.
    if "\n" in content:
        *lines, last_line = content.splitlines()
        if content.endswith("\n"):
            last_line += "\n"
        content = (
            "\n".join(line.rstrip() for line in lines) + f"\n{last_line}"
            if lines
            else last_line
        )

    # Remove empty headings and image style.
    content = re.sub(r"#{1,6}$", "", content, flags=re.MULTILINE)
    content = re.sub(r"\)\{.+\}$", ")", content, flags=re.MULTILINE)

    # Remove extra newlines.
    return re.sub(r"\n{3,}", "\n\n", content)


##
## Conversion from Microsoft Teams
##


class _CustomHTML2Text(HTML2Text):
    def __init__(self) -> None:
        super().__init__()
        self.body_width = 0  # prevent wrapping
        self.ignore_images = True
        self.pad_tables = False
        self.single_line_break = False
        self.ul_item_mark = "-"
        self.wrap_links = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs = [
            (attr, new_value)
            for attr, value in attrs
            if (new_value := self._filter_attribute(tag, attr, value))
        ]
        super().handle_starttag(tag, attrs)

    def _filter_attribute(self, tag: str, attr: str, value: str | None) -> str | None:
        if tag == "a" and attr == "title":
            return None

        return value


def markdown_from_msteams(
    text: str,
    remove_mentions: list[str] | None = None,
) -> tuple[str, list[str]]:
    """
    Translate the content of a Microsoft Teams message (HTML) into Markdown that
    can be parsed as `ContentText`.  If the user used Markdown by hand, preserve
    it, along with the formatting of code blocks.
    """
    remove_mentions = [m.lower() for m in remove_mentions or []]

    text = text.replace("\r\n", "\n")
    mentions: list[str] = []
    code_blocks = []

    # We do not want to run html2text on the content of code blocks, to maintain
    # the original spaces, etc.
    def make_placeholder(index):
        return "##$${{{index}}}$$##".replace("{index}", str(index))

    # Capture code blocks from HTML tags
    # TODO: Handle nested code blocks in Markdown.
    def handle_code_html(match):
        content = match.group(1)
        content = unescape(content.replace("&nbsp;", " "))
        placeholder = make_placeholder(len(code_blocks))
        code_blocks.append(f"```\n{content.strip()}\n```")
        return placeholder

    text = re.sub(
        r"<pre[^>]*><code[^>]*>(.*?)</code></pre>",
        handle_code_html,
        text,
        flags=re.DOTALL,
    )
    text = re.sub(
        r"<pre[^>]*>(.*?)</pre>",
        handle_code_html,
        text,
        flags=re.DOTALL,
    )

    # Capture code blocks from manual Markdown (<p>```[language]</p>)
    def handle_code_markdown(match):
        language = match.group(1)
        content = match.group(2)

        # Preserve new lines when they don't appear between <p>.
        # Should never be necessary in practice.
        content = content.replace("</p><p>", "</p>\n<p>")
        content = content.replace("<p>&nbsp;</p>", "")

        # Remove HTML tags in the code block (discarding any formatting) BEFORE
        # we unescape the HTML entities.
        content = re.sub(r"<[/a-zA-Z][^>]*>", "", content)

        content = unescape(content.replace("&nbsp;", " "))
        placeholder = make_placeholder(len(code_blocks))
        code_blocks.append(f"```{language}\n{content.strip()}\n```")
        return placeholder

    text = re.sub(
        r"<p>```([A-Za-z0-9_\-]*)\s*</p>(.+)<p>```\s*</p>",
        handle_code_markdown,
        text,
        flags=re.DOTALL,
    )

    # Remove mentions of bots, since "@Bot" is used to invoke Nandam in a
    # Microsoft Teams channel thread.
    def handle_mention(match):
        name = match.group(1).strip()
        bisect_insert(mentions, name, key=lambda n: n.lower())
        if any(name.lower() == m for m in remove_mentions):
            return ""
        else:
            return name

    text = re.sub(
        r'<span itemtype="http://schema.skype.com/Mention"[^>]*>([^<]*)</span>',
        handle_mention,
        text,
        flags=re.IGNORECASE,
    )

    # Convert HTML to Markdown
    h = _CustomHTML2Text()
    text = h.handle(text)

    # Replace the default '* * *' horizontal rule with '---'
    text = text.replace("\n\n* * *\n\n", "\n\n---\n\n")

    # Remove unnecessary vertical spacing
    text = re.sub(r"\n\s*\n", "\n\n", text)

    # At the very end, re-insert the content of code blocks.
    for i in range(len(code_blocks)):
        text = text.replace(make_placeholder(i), code_blocks[i])

    return strip_keep_identation(text), mentions
