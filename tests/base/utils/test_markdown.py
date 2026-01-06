import json
import pytest
import re

from base.core.values import as_json, as_yaml
from base.utils.markdown import (
    markdown_from_msteams,
    markdown_normalize,
    markdown_split_code,
    REGEX_MARKDOWN_CODE_BLOCK,
    REGEX_MARKDOWN_CODE_EXPR,
    SplitCodeMode,
)


def _run_markdown_split_code(
    markdown: str,
    split_exprs: bool,
    expected_output: list[tuple[SplitCodeMode, str]],
) -> None:
    actual_output = markdown_split_code(markdown, split_exprs)
    print(
        "Actual: "
        + as_yaml([{"mode": mode, "text": text} for mode, text in actual_output])
    )
    print(
        "Expected: "
        + as_yaml([{"mode": mode, "text": text} for mode, text in expected_output])
    )
    assert actual_output == expected_output


##
## markdown_split_code
##


def test_markdown_split_code_empty() -> None:
    """Test splitting an empty markdown string"""
    markdown = ""
    expected: list[tuple[SplitCodeMode, str]] = []
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_no_code() -> None:
    """Test splitting markdown with no code blocks or expressions"""
    markdown = "This is a simple text with no code blocks or expressions."
    expected: list[tuple[SplitCodeMode, str]] = [("text", markdown)]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_single_block() -> None:
    """Test splitting markdown with a single code block"""
    markdown = "Before\n```\ncode block\n```\nAfter"
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "Before"),
        ("code_block", "```\ncode block\n```"),
        ("text", "After"),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_multiple_blocks() -> None:
    """Test splitting markdown with multiple code blocks"""
    markdown = "First\n```\nblock 1\n```\nMiddle\n```\nblock 2\n```\nLast"
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "First"),
        ("code_block", "```\nblock 1\n```"),
        ("text", "Middle"),
        ("code_block", "```\nblock 2\n```"),
        ("text", "Last"),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_with_language() -> None:
    """Test splitting markdown with code blocks that have language specifiers"""
    markdown = "Text\n```python\nprint('Hello')\n```\nMore text"
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "Text"),
        ("code_block", "```python\nprint('Hello')\n```"),
        ("text", "More text"),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_with_expressions() -> None:
    """Test splitting markdown with code expressions"""
    markdown = "Text with `code expression` in it."
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "Text with "),
        ("code_expr", "`code expression`"),
        ("text", " in it."),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_with_expressions_disabled() -> None:
    """Test splitting markdown with code expressions but with split_exprs=False"""
    markdown = "Text with `code expression` in it."
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "Text with `code expression` in it.")
    ]
    _run_markdown_split_code(markdown, False, expected)


def test_markdown_split_code_blocks_and_expressions() -> None:
    """Test splitting markdown with both code blocks and expressions"""
    markdown = "Text with `expr` and\n```\ncode block\n```\nand `another expr`."
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "Text with "),
        ("code_expr", "`expr`"),
        ("text", " and"),
        ("code_block", "```\ncode block\n```"),
        ("text", "and "),
        ("code_expr", "`another expr`"),
        ("text", "."),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_consecutive_blocks() -> None:
    """Test splitting markdown with consecutive code blocks"""
    markdown = "```\nblock 1\n```\n```\nblock 2\n```"
    expected: list[tuple[SplitCodeMode, str]] = [
        ("code_block", "```\nblock 1\n```"),
        ("code_block", "```\nblock 2\n```"),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_consecutive_expressions() -> None:
    """Test splitting markdown with consecutive code expressions"""
    markdown = "Text `expr1``expr2` more"
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "Text "),
        ("code_expr", "`expr1`"),
        ("code_expr", "`expr2`"),
        ("text", " more"),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_starting_with_code() -> None:
    """Test splitting markdown that starts with a code block"""
    markdown = "```\nstarting code\n```\nText after"
    expected: list[tuple[SplitCodeMode, str]] = [
        ("code_block", "```\nstarting code\n```"),
        ("text", "Text after"),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_ending_with_code() -> None:
    """Test splitting markdown that ends with a code block"""
    markdown = "Text before\n```\nending code\n```"
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "Text before"),
        ("code_block", "```\nending code\n```"),
    ]
    _run_markdown_split_code(markdown, True, expected)


def test_markdown_split_code_complex_mixed() -> None:
    """Test splitting complex markdown with mixed code blocks and expressions"""
    markdown = """\
# Heading

Text with `inline code` and more text.

```python
def function():
    return "code block"
```

More text with `another` and `multiple` code expressions.

```
plain code block
```

Final paragraph.\
"""
    expected: list[tuple[SplitCodeMode, str]] = [
        ("text", "# Heading\n\nText with "),
        ("code_expr", "`inline code`"),
        ("text", " and more text.\n"),
        ("code_block", '```python\ndef function():\n    return "code block"\n```'),
        ("text", "\nMore text with "),
        ("code_expr", "`another`"),
        ("text", " and "),
        ("code_expr", "`multiple`"),
        ("text", " code expressions.\n"),
        ("code_block", "```\nplain code block\n```"),
        ("text", "\nFinal paragraph."),
    ]
    _run_markdown_split_code(markdown, True, expected)


##
## REGEX_MARKDOWN_CODE_BLOCK
##


def test_regex_markdown_code_block_pattern() -> None:
    """Test the regex pattern for matching markdown code blocks"""
    test_cases = [
        ("\n```\ncode\n```\n", True),
        ("\n```python\ncode\n```\n", True),
        ('\n```json\n{"key": "value"}\n```\n', True),
        ("```\nno newlines```", False),
        ("```\nneeds closing", False),
        ("needs opening\n```", False),
        ("plain text", False),
    ]

    regex = re.compile(REGEX_MARKDOWN_CODE_BLOCK, re.DOTALL)
    for text, should_match in test_cases:
        if should_match:
            assert regex.search(text), f"Should match: {text}"
        else:
            assert not regex.search(text), f"Should not match: {text}"


def test_regex_markdown_code_expr_pattern() -> None:
    """Test the regex pattern for matching markdown code expressions"""
    test_cases = [
        ("`code`", True),
        ("`c`", True),
        ("`multi word`", True),
        ("`code with @ special $ chars`", True),
        ("` leading space`", False),
        ("`trailing space `", False),
        ("no backticks", False),
        ("``", False),
        ("`", False),
    ]

    regex = re.compile(REGEX_MARKDOWN_CODE_EXPR)
    for text, should_match in test_cases:
        if should_match:
            assert regex.search(text), f"Should match: {text}"
        else:
            assert not regex.search(text), f"Should not match: {text}"


##
## markdown_normalize
##


def test_markdown_normalize_empty() -> None:
    """Test normalizing empty markdown"""
    markdown = ""
    expected = ""
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_simple() -> None:
    """Test normalizing simple markdown"""
    markdown = "Simple markdown with no special formatting."
    expected = "Simple markdown with no special formatting."
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_preserves_newlines_around() -> None:
    """Test normalizing markdown by stripping whitespace"""
    markdown = "\n\n  Text with leading and trailing whitespace.  \n\n"
    expected = "\n\n  Text with leading and trailing whitespace.\n\n"
    result = markdown_normalize(markdown)
    print(f"<actual>\n{as_json(result)}\n</actual>")
    assert result == expected


def test_markdown_normalize_line_trailing_whitespace_inside_content() -> None:
    """
    Test normalizing markdown by removing trailing whitespace on each line,
    except for the last line (in case it's followed, e.g., by a link).
    """
    markdown = "Line with trailing spaces.   \nSecond line.  "
    expected = "Line with trailing spaces.\nSecond line.  "
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_rn_newlines() -> None:
    """Test normalizing markdown with Windows newlines"""
    markdown = "Line 1.\r\nLine 2.\r\nLine 3."
    expected = "Line 1.\nLine 2.\nLine 3."
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_extra_newlines() -> None:
    """Test normalizing markdown with extra newlines"""
    markdown = "Paragraph 1.\n\n\n\nParagraph 2."
    expected = "Paragraph 1.\n\nParagraph 2."
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_empty_headings() -> None:
    """Test normalizing markdown with empty headings"""
    markdown = "# Heading\n## \n### Empty heading\n\n#### \n... and text!"
    expected = "# Heading\n\n### Empty heading\n\n... and text!"
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_image_style() -> None:
    """Test normalizing markdown by removing image style"""
    markdown = "![Image](path/to/image.png){width=100}"
    expected = "![Image](path/to/image.png)"
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_symbols() -> None:
    """Test normalizing markdown by removing emoji and symbols"""
    # Test with a few symbols from the defined ranges
    markdown = "Text with symbols: \u2103 \u2665 \U0001f600."
    expected = "Text with symbols:   ."
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_nul_bytes() -> None:
    """Test normalizing markdown by replacing NUL bytes"""
    markdown = "Text with null byte: \u0000."
    expected = "Text with null byte: ??."
    result = markdown_normalize(markdown)
    assert result == expected


def test_markdown_normalize_complex() -> None:
    """Test normalizing complex markdown with multiple issues"""
    markdown = """\
# Heading



Paragraph with trailing whitespace.
##

![Image](image.png){width=200 height=100}

* list item 1 \u2665
* list item 2 \u0000\
"""
    expected = """\
# Heading

Paragraph with trailing whitespace.

![Image](image.png)

* list item 1
* list item 2 ??\
"""
    result = markdown_normalize(markdown)
    assert result == expected


##
## HTML to Markdown
##


def _run_markdown_from_msteams(
    html_input: str,
    expected_output: str,
    remove_mentions: list[str] | None = None,
    expected_mentions: list[str] | None = None,
) -> None:
    expected_mentions = expected_mentions or []
    actual_output, actual_mentions = markdown_from_msteams(html_input, remove_mentions)
    print("Actual: " + json.dumps(actual_output.splitlines(), indent=2))
    print("Expected: " + json.dumps(expected_output.splitlines(), indent=2))
    assert actual_output == expected_output
    assert actual_mentions == expected_mentions


@pytest.mark.parametrize(
    ("html_input", "expected_output"),
    [
        ("<h1>Heading 1</h1>", "# Heading 1"),
        ("<h2>Heading 2</h2>", "## Heading 2"),
        ("<h3>Heading 3</h3>", "### Heading 3"),
        ("<h4>Heading 4</h4>", "#### Heading 4"),
        ("<h5>Heading 5</h5>", "##### Heading 5"),
        ("<h6>Heading 6</h6>", "###### Heading 6"),
    ],
)
def test_markdown_from_msteams_converts_headings(html_input, expected_output):
    _run_markdown_from_msteams(html_input, expected_output)


@pytest.mark.parametrize(
    ("html_input", "expected_output"),
    [
        (
            "<h1><em>Italic</em> and **bold**</h1>",
            "# _Italic_ and **bold**",
        ),
        (
            '<h2><a href="https://example.com">Link</a> in heading</h2>',
            "## [Link](https://example.com) in heading",
        ),
        (
            "<h3><code>Code</code> in <strong>[heading](url)</strong></h3>",
            "### `Code` in **[heading](url)**",
        ),
    ],
)
def test_markdown_from_msteams_converts_headings_with_formatting(
    html_input, expected_output
):
    _run_markdown_from_msteams(html_input, expected_output)


@pytest.mark.parametrize(
    ("html_input", "expected_output"),
    [
        (
            "https://example.com",
            "https://example.com",
        ),
        (
            '<a href="https://example.com">Example</a>',
            "[Example](https://example.com)",
        ),
        (
            # Discard the title
            '<a href="https://example.com" title="https://example.com">Example</a>',
            "[Example](https://example.com)",
        ),
        (
            # only inserts the link when its content is the href
            '<a href="https://example.com">https://example.com</a>',
            "<https://example.com>",
        ),
    ],
)
def test_markdown_from_msteams_converts_links(html_input, expected_output):
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_single_blockquote():
    html_input = "<blockquote><p>Single blockquote</p></blockquote>"
    expected_output = "> Single blockquote"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_multiple_blockquotes():
    html_input = (
        "<blockquote><p>First blockquote</p></blockquote>\n"
        "<blockquote><p>Second blockquote</p></blockquote>"
    )
    expected_output = "> First blockquote\n\n> Second blockquote"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_multiline_blockquotes():
    html_input = (
        "<blockquote>\n"
        "<p>Blockquote with <strong>bold</strong> and _italic_</p>\n"
        "<p>With a second paragraph</p>\n"
        "</blockquote>"
    )
    expected_output = (
        "> Blockquote with **bold** and _italic_\n> \n> With a second paragraph"
    )
    _run_markdown_from_msteams(html_input, expected_output)


# Test conversion of HTML horizontal rules to Markdown horizontal rules
@pytest.mark.parametrize(
    ("html_input", "expected_output"),
    [
        ("Hello<hr>world!", "Hello\n\n---\n\nworld!"),
        ("Hello<hr/>world!", "Hello\n\n---\n\nworld!"),
        ("Hello<HR>world!", "Hello\n\n---\n\nworld!"),  # case insensitive
    ],
)
def test_markdown_from_msteams_converts_hr(html_input, expected_output):
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_flat_ordered_list():
    html_input = (
        "<p>My list:</p><ol>\n<li>First</li>\n<li>Second</li>\n<li>Third</li>\n</ol>"
    )
    expected_output = "My list:\n\n  1. First\n  2. Second\n  3. Third"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_flat_unordered_list():
    html_input = (
        "<p>My list:</p><ul>\n<li>First</li>\n<li>Second</li>\n<li>Third</li>\n</ul>"
    )
    expected_output = "My list:\n\n  - First\n  - Second\n  - Third"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_nested_ordered_list():
    # fmt: off
    html_input = (
        "<p>My list:</p>"
        "<ol>"
        "<li>First<ol>"
        "<li>Second</li>"
        "<li>Third</li>"
        "</ol></li>"
        "</ol>"
    )
    expected_output = "My list:\n\n  1. First\n     1. Second\n     2. Third"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_nested_unordered_list():
    # fmt: off
    html_input = (
        "<p>My list:</p>"
        "<ul>"
        "<li>First<ul>"
        "<li>Second</li>"
        "<li>Third</li>"
        "</ul></li>"
        "</ul>"
    )
    expected_output = "My list:\n\n  - First\n    - Second\n    - Third"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_list_with_formatting():
    html_input = "<p>My list:</p><ul><li><strong>Bold Item</strong></li><li>_Italic Item_</li></ul>"
    expected_output = "My list:\n\n  - **Bold Item**\n  - _Italic Item_"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_removes_miscellaneous_html_inputs():
    html_input = "<div>Text <span>inside</span> div</div>"
    expected_output = "Text inside div"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_removes_mentions_bot():
    html_input = (
        '<p>Hello <span itemtype="http://schema.skype.com/Mention">Bot Name</span>!</p>'
    )
    expected_output = "Hello !"
    _run_markdown_from_msteams(
        html_input,
        expected_output,
        remove_mentions=["Bot Name"],
        expected_mentions=["Bot Name"],
    )


def test_markdown_from_msteams_keeps_mentions_user():
    html_input = '<p>Hello <span itemtype="http://schema.skype.com/Mention">User Name</span>!</p>'
    expected_output = "Hello User Name!"
    _run_markdown_from_msteams(
        html_input,
        expected_output,
        remove_mentions=["Bot Name"],
        expected_mentions=["User Name"],
    )


def test_markdown_from_msteams_decodes_html_entities():
    html_input = "Text with &amp; &lt;HTML&gt; entity"
    expected_output = "Text with & <HTML> entity"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_decodes_nbsp():
    html_input = "Here's the thing. &nbsp;I use two spaces."
    expected_output = "Here's the thing.  I use two spaces."
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_leaves_empty_string_unchanged():
    html_input = ""
    expected_output = ""
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_keeps_html_free_string_unchanged():
    html_input = "Just a plain string without HTML."
    expected_output = "Just a plain string without HTML."
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_keeps_markdown_only_string_unchanged():
    html_input = "Just a plain **Markdown** without _HTML_."
    expected_output = "Just a plain **Markdown** without _HTML_."
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_incomplete_tags():
    html_input = "<p>Paragraph with <b>bold</code>"
    expected_output = "Paragraph with **bold`"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_preserves_special_characters():
    html_input = "<p>Special characters: <, >, &, '\"', \"'</p>"
    expected_output = "Special characters: <, >, &, '\"', \"'"
    _run_markdown_from_msteams(html_input, expected_output)


##
## HTML to Markdown: code blocks
##


def test_markdown_from_msteams_converts_p_code():
    html_input = "<p><code>Single line code</code></p>"
    expected_output = "`Single line code`"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_p_code_preserving_html():
    # Teams automatically converts "<" and ">" into "&lt;" and "&gt;"
    html_input = "<p><code>Single _line_ &lt;strong&gt;code&lt;/strong&gt;</code></p>"
    expected_output = "`Single _line_ <strong>code</strong>`"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_pre_code():
    html_input = "<pre><code>First line\nSecond line</code></pre>"
    expected_output = "```\nFirst line\nSecond line\n```"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_pre_code_preserving_html():
    html_input = "<pre><code>First &lt;b&gt;line&lt;/b&gt;\nSecond _line_</code></pre>"
    expected_output = "```\nFirst <b>line</b>\nSecond _line_\n```"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_md_code_block():
    # Notice how we drop the <p>...</p> around each line, but nothing else.
    html_input = "<p>```</p><p>First line</p><p>Second line</p><p>```</p>"
    expected_output = "```\nFirst line\nSecond line\n```"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_md_code_block_trailing_whitespace():
    # Notice how we drop the <p>...</p> around each line, but nothing else.
    html_input = "<p>```  </p><p>First line</p><p>Second line</p><p>```  </p>"
    expected_output = "```\nFirst line\nSecond line\n```"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_md_code_block_lang():
    # Notice how we drop the <p>...</p> around each line, but nothing else.
    html_input = "<p>```python</p><p>First line</p><p>Second line</p><p>```</p>"
    expected_output = "```python\nFirst line\nSecond line\n```"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_md_code_block_preserving_html_without_p():
    # Notice how we drop the <p>...</p> around each line, but nothing else.
    # Teams automatically converts "<" and ">" into "&lt;" and "&gt;"
    html_input = (
        "<p>```</p><p>First &lt;b&gt;line&lt;/b&gt;</p><p>Second _line_</p><p>```</p>"
    )
    expected_output = "```\nFirst <b>line</b>\nSecond _line_\n```"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_md_code_block_preserving_html_without_p_lang():
    # Notice how we drop the <p>...</p> around each line, but nothing else.
    # Teams automatically converts "<" and ">" into "&lt;" and "&gt;"
    html_input = (
        "<p>```python</p>"
        "<p>First &lt;b&gt;line&lt;/b&gt;</p>"
        "<p>Second _line_</p>"
        "<p>```</p>"
    )
    expected_output = "```python\nFirst <b>line</b>\nSecond _line_\n```"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_inline_code():
    html_input = "<p>Why, <code>hello my world</code></p>"
    expected_output = "Why, `hello my world`"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_converts_inline_code_preserving_html():
    # Teams automatically converts "<" and ">" into "&lt;" and "&gt;"
    html_input = (
        "<p>Why, <code>hello _my_ &lt;strong&gt;world&lt;/strong&gt;</code></p>"
    )
    expected_output = "Why, `hello _my_ <strong>world</strong>`"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_convert_inline_markdown_code():
    html_input = "<p>Why, `hello my world`</p>"
    expected_output = "Why, `hello my world`"
    _run_markdown_from_msteams(html_input, expected_output)


def test_markdown_from_msteams_convert_inline_markdown_code_preserving_html():
    # Teams automatically converts "<" and ">" into "&lt;" and "&gt;"
    html_input = "<p>Why, `hello _my_ &lt;strong&gt;world&lt;/strong&gt;`</p>"
    expected_output = "Why, `hello _my_ <strong>world</strong>`"
    _run_markdown_from_msteams(html_input, expected_output)


##
## Edge cases in the wild
##


@pytest.mark.parametrize(
    ("html_input", "expected_output"),
    [
        (
            "\r\n".join(  # noqa: FLY002
                [
                    "<p>Just testing something. &nbsp;Please repeat as-is:</p>",
                    "<p> </p>",
                    "<h1>Title 1</h1>",
                    "<h2>Title 2</h2>",
                    "<h3>Title 3</h3>",
                    "<p>Paragraph with  spaces and  stuff.</p>",
                    "<ul>",
                    "<li>Item 1.</li><li>Item 2.</li></ul>",
                    "<p>And then:</p>",
                    "<ol>",
                    "<li>Item A.</li><li>Item B.</li></ol>",
                ]
            ),
            "\n".join(  # noqa: FLY002
                [
                    "Just testing something.  Please repeat as-is:",
                    "",
                    "# Title 1",
                    "",
                    "## Title 2",
                    "",
                    "### Title 3",
                    "",
                    "Paragraph with spaces and stuff.",
                    "",
                    "  - Item 1.",
                    "  - Item 2.",
                    "",
                    "And then:",
                    "",
                    "  1. Item A.",
                    "  2. Item B.",
                ]
            ),
        ),
    ],
)
def test_markdown_from_msteams_edge_cases(html_input, expected_output):
    _run_markdown_from_msteams(html_input, expected_output)
