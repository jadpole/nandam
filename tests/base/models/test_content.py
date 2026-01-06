from base.models.content import (
    _find_plain_references,
    ContentText,
    PartCode,
    PartHeading,
    PartLink,
    PartPageNumber,
    PartText,
    TextPart,
)
from base.resources.aff_body import AffBody, AffBodyChunk
from base.strings.resource import AffordanceUri, ObservableUri, ResourceUri, WebUrl


##
## ContentText.parse
##


def _run_content_parse_markdown(
    *,
    markdown: str,
    expected: list[TextPart],
    expected_text: str | None = None,
) -> None:
    if expected_text is None:
        expected_text = ContentText(parts=expected, plain=None).as_str()
    actual = ContentText.parse(markdown, default_link="plain")
    actual_text = actual.as_str(ignore_plain=True)

    expected_parts_str = "\n".join(str(part) for part in expected)
    actual_parts_str = "\n".join(str(part) for part in actual.parts)
    print(
        f"<expected_text>\n{expected_text}\n</expected_text>"
        + f"\n<actual_text>\n{actual_text}\n</actual_text>"
        + f"\n<expected_parts>\n{expected_parts_str}\n</expected_parts>"
        + f"\n<actual_parts>\n{actual_parts_str}\n</actual_parts>"
    )

    assert actual_text == expected_text
    assert actual.parts == expected


def test_content_parse_markdown_empty_content():
    _run_content_parse_markdown(
        markdown="",
        expected=[],
    )


def test_content_parse_markdown_plain_text():
    _run_content_parse_markdown(
        markdown="This is a plain text.",
        expected=[PartText.new("This is a plain text.")],
    )


def test_content_parse_markdown_web_url_plain():
    _run_content_parse_markdown(
        markdown="Visit https://example.com.",
        expected=[
            PartText.new("Visit "),
            PartLink.stub("plain", "https://example.com"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_web_url_quoted():
    _run_content_parse_markdown(
        markdown='Visit "https://example.com".',
        expected=[
            PartText.new('Visit "'),
            PartLink.stub("plain", "https://example.com"),
            PartText.new('".'),
        ],
    )


def test_content_parse_markdown_web_url_wrapped():
    _run_content_parse_markdown(
        markdown="Visit <https://example.com>.",
        expected=[
            PartText.new("Visit "),
            PartLink.stub("markdown", "https://example.com"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_web_url_with_label():
    _run_content_parse_markdown(
        markdown="Check out [Example](https://example.com).",
        expected=[
            PartText.new("Check out "),
            PartLink.stub("markdown", "https://example.com", "Example"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_web_url_with_parentheses():
    _run_content_parse_markdown(
        markdown="Check it out! (see [Example](https://example.com)).",
        expected=[
            PartText.new("Check it out! (see "),
            PartLink.stub("markdown", "https://example.com", "Example"),
            PartText.new(")."),
        ],
    )


def test_content_parse_markdown_web_url_image():
    _run_content_parse_markdown(
        markdown="Here is my figure: ![](https://example.com)\n\nIsn't it wonderful?",
        expected=[
            PartText.new("Here is my figure:"),
            PartLink.stub("embed", "https://example.com"),
            PartText.new("Isn't it wonderful?"),
        ],
    )


def test_content_parse_markdown_web_url_image_with_caption():
    _run_content_parse_markdown(
        markdown="Here is my figure: ![caption](https://example.com)\n\nIsn't it wonderful?",
        expected=[
            PartText.new("Here is my figure:"),
            PartLink.stub("embed", "https://example.com", "caption"),
            PartText.new("Isn't it wonderful?"),
        ],
    )


def test_content_parse_markdown_footnote_resource_uri():
    """
    NOTE: Also checks that it remembers the space before "[^" when parsing.
    """
    _run_content_parse_markdown(
        markdown="See this source [^ndk://sharepoint/SiteName/Documents/myfile.pdf].",
        expected=[
            PartText.new("See this source "),
            PartLink.stub("citation", "ndk://sharepoint/SiteName/Documents/myfile.pdf"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_footnote_affordance_uri():
    """
    NOTE: Also checks that it remembers the space before "[^" when parsing.
    """
    _run_content_parse_markdown(
        markdown="See this source [^ndk://sharepoint/SiteName/Documents/myfile.pdf/$file].",
        expected=[
            PartText.new("See this source "),
            PartLink.stub(
                "citation",
                "ndk://sharepoint/SiteName/Documents/myfile.pdf/$file",
            ),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_footnote_body_uri():
    """
    NOTE: Also checks that it remembers the space before "[^" when parsing.
    """
    _run_content_parse_markdown(
        markdown="See this source [^ndk://sharepoint/SiteName/Documents/myfile.pdf/$body].",
        expected=[
            PartText.new("See this source "),
            PartLink.stub(
                "citation",
                "ndk://sharepoint/SiteName/Documents/myfile.pdf/$body",
            ),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_footnote_observable_uri():
    """
    NOTE: Also checks that it remembers the space before "[^" when parsing.
    """
    _run_content_parse_markdown(
        markdown="See this source [^ndk://sharepoint/SiteName/Documents/myfile.pdf/$chunk/01/02].",
        expected=[
            PartText.new("See this source "),
            PartLink.stub(
                "citation",
                "ndk://sharepoint/SiteName/Documents/myfile.pdf/$chunk/01/02",
            ),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_footnote_web_url():
    _run_content_parse_markdown(
        markdown="See this source[^https://example.com].",
        expected=[
            PartText.new("See this source"),
            PartLink.stub("citation", "https://example.com"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_plain_resource_uri():
    _run_content_parse_markdown(
        markdown="See ndk://sharepoint/SiteName/Documents/myfile.pdf.",
        expected=[
            PartText.new("See "),
            PartLink.stub("plain", "ndk://sharepoint/SiteName/Documents/myfile.pdf"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_plain_observable_uri():
    _run_content_parse_markdown(
        markdown="See ndk://sharepoint/SiteName/Documents/myfile.pdf/$chunk/01/02.",
        expected=[
            PartText.new("See "),
            PartLink.stub(
                "plain",
                "ndk://sharepoint/SiteName/Documents/myfile.pdf/$chunk/01/02",
            ),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_quoted_resource_uri():
    _run_content_parse_markdown(
        markdown='See "ndk://sharepoint/SiteName/Documents/myfile.pdf".',
        expected=[
            PartText.new('See "'),
            PartLink.stub("plain", "ndk://sharepoint/SiteName/Documents/myfile.pdf"),
            PartText.new('".'),
        ],
    )


def test_content_parse_markdown_quoted_observable_uri():
    _run_content_parse_markdown(
        markdown='See "ndk://sharepoint/SiteName/Documents/myfile.pdf/$chunk/01/02".',
        expected=[
            PartText.new('See "'),
            PartLink.stub(
                "plain",
                "ndk://sharepoint/SiteName/Documents/myfile.pdf/$chunk/01/02",
            ),
            PartText.new('".'),
        ],
    )


def test_content_parse_markdown_linked_resource_uri():
    _run_content_parse_markdown(
        markdown="See <ndk://jira/issue/PROJ-123>.",
        expected=[
            PartText.new("See "),
            PartLink.stub("markdown", "ndk://jira/issue/PROJ-123"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_linked_labeled_resource_uri():
    _run_content_parse_markdown(
        markdown="Refer to [this ticket](ndk://jira/issue/PROJ-123).",
        expected=[
            PartText.new("Refer to "),
            PartLink.stub("markdown", "ndk://jira/issue/PROJ-123", "this ticket"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_embed_resource_uri():
    """
    NOTE: Preserves indentation, even though in this case, keeping the space
    before "Isn't it wonderful?" is not the obvious behavior.
    """
    _run_content_parse_markdown(
        markdown="Here is my figure: ![](ndk://onedrive/username_mycompany_com/Documents/image.jpg) Isn't it wonderful?",
        expected=[
            PartText.new("Here is my figure:"),
            PartLink.stub(
                "embed",
                "ndk://onedrive/username_mycompany_com/Documents/image.jpg",
            ),
            PartText.new(" Isn't it wonderful?"),
        ],
    )


def test_content_parse_markdown_embed_labeled_resource_uri():
    """
    NOTE: Preserves indentation, even though in this case, keeping the space
    before "Isn't it wonderful?" is not the obvious behavior.
    """
    _run_content_parse_markdown(
        markdown="Here is my figure: ![caption](ndk://onedrive/username_mycompany_com/Documents/image.jpg) Isn't it wonderful?",
        expected=[
            PartText.new("Here is my figure:"),
            PartLink.stub(
                "embed",
                "ndk://onedrive/username_mycompany_com/Documents/image.jpg",
                "caption",
            ),
            PartText.new(" Isn't it wonderful?"),
        ],
        expected_text=(
            "Here is my figure:"
            "\n\n![caption](ndk://onedrive/username_mycompany_com/Documents/image.jpg)"
            "\n\n Isn't it wonderful?"
        ),
    )


def test_content_parse_markdown_embed_labeled_observable_uri():
    """
    NOTE: Preserves indentation, even though in this case, keeping the space
    before "Isn't it wonderful?" is not the obvious behavior.
    """
    _run_content_parse_markdown(
        markdown="Here is my figure: ![caption](ndk://onedrive/username_mycompany_com/Documents/image.jpg/$media) Isn't it wonderful?",
        expected=[
            PartText.new("Here is my figure:"),
            PartLink.stub(
                "embed",
                "ndk://onedrive/username_mycompany_com/Documents/image.jpg/$media",
                "caption",
            ),
            PartText.new(" Isn't it wonderful?"),
        ],
        expected_text=(
            "Here is my figure:"
            "\n\n![caption](ndk://onedrive/username_mycompany_com/Documents/image.jpg/$media)"
            "\n\n Isn't it wonderful?"
        ),
    )


def test_content_parse_markdown_mixed():
    _run_content_parse_markdown(
        markdown=(
            "This is [Example](https://example.com) "
            "with <ndk://jira/issue/PROJ-123> "
            "and <https://example.org>."
        ),
        expected=[
            PartText.new("This is "),
            PartLink.stub("markdown", "https://example.com", "Example"),
            PartText.new(" with "),
            PartLink.stub("markdown", "ndk://jira/issue/PROJ-123"),
            PartText.new(" and "),
            PartLink.stub("markdown", "https://example.org"),
            PartText.new("."),
        ],
    )


def test_content_parse_markdown_excludes_code_blocks_and_expr():
    _run_content_parse_markdown(
        markdown="""
This is an <https://example.com>:
```lang
But this ndk://sharepoint/SiteName/Documents/file1.txt is omitted!
And so is ![this one](ndk://sharepoint/SiteName/Documents/file2.txt).
```
Same with `this ndk://sharepoint/SiteName/Documents/file1.txt` \
and `this [code link](ndk://sharepoint/SiteName/Documents/file2.txt)`.

However, ndk://sharepoint/SiteName/Documents/file1.txt is extracted.
Same with [*this* link](ndk://sharepoint/SiteName/Documents/file2.txt).
""".strip(),
        expected=[
            # Normal links are correctly extracted.
            PartText.new("This is an "),
            PartLink.stub("markdown", "https://example.com"),
            # However, they are not extracted from code blocks and expressions.
            PartText.new(":"),
            PartCode(
                fence="```",
                language="lang",
                code=(
                    "But this ndk://sharepoint/SiteName/Documents/file1.txt is omitted!\n"
                    "And so is ![this one](ndk://sharepoint/SiteName/Documents/file2.txt)."
                ),
            ),
            PartText.new(
                "Same with `this ndk://sharepoint/SiteName/Documents/file1.txt` "
                "and `this [code link](ndk://sharepoint/SiteName/Documents/file2.txt)`.\n\n"
                "However, "
            ),
            # Both file1.txt and file2.txt appears in a code block, then a code
            # expression, then an actual link.  We check that an extracted link
            # is not accidentally replaced in code expressions via split.
            PartLink.stub("plain", "ndk://sharepoint/SiteName/Documents/file1.txt"),
            PartText.new(" is extracted.\nSame with "),
            PartLink.stub(
                "markdown",
                "ndk://sharepoint/SiteName/Documents/file2.txt",
                "*this* link",
            ),
            PartText.new("."),
        ],
        expected_text="""
This is an <https://example.com>:

```lang
But this ndk://sharepoint/SiteName/Documents/file1.txt is omitted!
And so is ![this one](ndk://sharepoint/SiteName/Documents/file2.txt).
```

Same with `this ndk://sharepoint/SiteName/Documents/file1.txt` \
and `this [code link](ndk://sharepoint/SiteName/Documents/file2.txt)`.

However, ndk://sharepoint/SiteName/Documents/file1.txt is extracted.
Same with [*this* link](ndk://sharepoint/SiteName/Documents/file2.txt).
""".strip(),
    )


def test_content_parse_markdown_includes_code_expr_label():
    _run_content_parse_markdown(
        markdown="Notice how [`*this* link`](https://example.com) is extracted.",
        expected=[
            PartText.new("Notice how "),
            PartLink.stub("markdown", "https://example.com", "`*this* link`"),
            PartText.new(" is extracted."),
        ],
    )


def test_content_parse_markdown_with_headings_and_page_numbers():
    _run_content_parse_markdown(
        markdown="""
{0}------------------------------------------------

## Heading 1

This is an <https://example.com>:
```lang
But this ndk://sharepoint/SiteName/Documents/file1.txt is omitted!
And so is ![this one](ndk://sharepoint/SiteName/Documents/file2.txt).
```

### Heading 2

Same with `this ndk://sharepoint/SiteName/Documents/file1.txt` \
and `this [code link](ndk://sharepoint/SiteName/Documents/file2.txt)`.

{1}------------------------------------------------

### Heading 3

However, ndk://sharepoint/SiteName/Documents/file1.txt is extracted.
Same with [*this* link](ndk://sharepoint/SiteName/Documents/file2.txt).
And to confirm that `headings and page numbers` do not interfere with code \
expressions, let's try `ndk://sharepoint/SiteName/Documents/file2.txt` again.
""".strip(),
        expected=[
            PartPageNumber(page_number=0),
            PartHeading(level=2, text="Heading 1"),
            # Normal links are correctly extracted.
            PartText.new("This is an "),
            PartLink.stub("markdown", "https://example.com"),
            # However, they are not extracted from code blocks and expressions.
            PartText.new(":"),
            PartCode(
                fence="```",
                language="lang",
                code=(
                    "But this ndk://sharepoint/SiteName/Documents/file1.txt is omitted!\n"
                    "And so is ![this one](ndk://sharepoint/SiteName/Documents/file2.txt)."
                ),
            ),
            PartHeading(level=3, text="Heading 2"),
            PartText.new(
                "Same with `this ndk://sharepoint/SiteName/Documents/file1.txt` "
                "and `this [code link](ndk://sharepoint/SiteName/Documents/file2.txt)`."
            ),
            PartPageNumber(page_number=1),
            PartHeading(level=3, text="Heading 3"),
            # Both file1.txt and file2.txt appears in a code block, then a code
            # expression, then an actual link.  We check that an extracted link
            # is not accidentally replaced in code expressions via split.
            PartText.new("However, "),
            PartLink.stub("plain", "ndk://sharepoint/SiteName/Documents/file1.txt"),
            PartText.new(" is extracted.\nSame with "),
            PartLink.stub(
                "markdown",
                "ndk://sharepoint/SiteName/Documents/file2.txt",
                "*this* link",
            ),
            PartText.new(
                ".\nAnd to confirm that `headings and page numbers` do not interfere with code "
                "expressions, let's try `ndk://sharepoint/SiteName/Documents/file2.txt` again."
            ),
        ],
        expected_text="""
{0}------------------------------------------------

## Heading 1

This is an <https://example.com>:

```lang
But this ndk://sharepoint/SiteName/Documents/file1.txt is omitted!
And so is ![this one](ndk://sharepoint/SiteName/Documents/file2.txt).
```

### Heading 2

Same with `this ndk://sharepoint/SiteName/Documents/file1.txt` \
and `this [code link](ndk://sharepoint/SiteName/Documents/file2.txt)`.

{1}------------------------------------------------

### Heading 3

However, ndk://sharepoint/SiteName/Documents/file1.txt is extracted.
Same with [*this* link](ndk://sharepoint/SiteName/Documents/file2.txt).
And to confirm that `headings and page numbers` do not interfere with code \
expressions, let's try `ndk://sharepoint/SiteName/Documents/file2.txt` again.\
""".strip(),
    )


##
## ContentText.as_text
##


def test_content_as_text_embed_wrapped_in_xml_tags():
    content = ContentText.new(
        [
            PartText.new("Here is my figure:", "\n\n"),
            PartText.new("<figure>", "\n", "\n-force"),
            PartLink.stub(
                "embed",
                "ndk://onedrive/username_mycompany_com/Documents/image.jpg",
                "  caption with\n\twhitespace  to be removed ",
            ),
            PartText.new("</figure>", "\n-force", "\n"),
            PartText.new(" Isn't it wonderful?", "\n\n"),
        ]
    )
    actual = content.as_str()
    assert actual == (
        """\
Here is my figure:

<figure>
![caption with whitespace to be removed](ndk://onedrive/username_mycompany_com/Documents/image.jpg)
</figure>

 Isn't it wonderful?\
"""
    )


def test_content_as_text_code_wrapped_in_xml_tags():
    content = ContentText.new(
        [
            PartText.new("Here is my code:", "\n\n"),
            PartText.new("<code-block>", "\n", "\n-force"),
            PartCode.new(
                code="print('Hello, world!')",
                language="python",
            ),
            PartText.new("</code-block>", "\n-force", "\n"),
            PartText.new(" Isn't it wonderful?", "\n\n"),
        ]
    )
    actual = content.as_str()
    assert actual == (
        """\
Here is my code:

<code-block>
```python
print('Hello, world!')
```
</code-block>

 Isn't it wonderful?\
"""
    )


##
## _find_plain_references
##


def test_find_plain_references_on_hello_world():
    message = "hello, world!"
    actual = _find_plain_references(message)
    assert [str(r) for r in actual] == []


def test_find_plain_references_on_just_url():
    message = "https://arxiv.org/abs/1803.03453"
    actual = _find_plain_references(message)
    assert type(actual[0]) is WebUrl
    assert [str(r) for r in actual] == [
        "https://arxiv.org/abs/1803.03453",
    ]


def test_find_plain_references_on_list():
    message = (
        "compare https://arxiv.org/a, https://arxiv.org/b, and https://arxiv.org/c."
    )
    actual = _find_plain_references(message)
    assert [str(r) for r in actual] == [
        "https://arxiv.org/a",
        "https://arxiv.org/b",
        "https://arxiv.org/c",
    ]


def test_find_plain_references_on_markdown_link_affordanceuri():
    message = (
        "[Example](ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx/$body)"
    )
    actual = _find_plain_references(message)
    assert type(actual[0]) is ObservableUri  # NOTE: `ObservableUri` on conflict.
    assert type(actual[0].affordance_uri()) is AffordanceUri
    assert type(actual[0].suffix) is AffBody
    assert [str(r) for r in actual] == [
        "ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx/$body"
    ]


def test_find_plain_references_on_markdown_link_observableuri():
    message = "[Example](ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx/$chunk/01/02)"
    actual = _find_plain_references(message)
    assert type(actual[0]) is ObservableUri
    assert type(actual[0].suffix) is AffBodyChunk
    assert [str(r) for r in actual] == [
        "ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx/$chunk/01/02"
    ]


def test_find_plain_references_on_markdown_link_resourceuri():
    message = "[Example](ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx)"
    actual = _find_plain_references(message)
    assert type(actual[0]) is ResourceUri
    assert [str(r) for r in actual] == [
        "ndk://sharepoint/SiteName/SitePages/Loyalty-Program.aspx",
    ]


def test_find_plain_references_on_markdown_link_web():
    message = "[Example](https://example.com)"
    actual = _find_plain_references(message)
    assert [str(r) for r in actual] == [
        "https://example.com",
    ]


def test_find_plain_references_on_markdown_link_with_repeated_url():
    """
    It is the responsiblity of the caller to deduplicate when necessary.
    Some callers need the duplicates to split on all instances.
    """
    message = "[https://example.com](https://example.com)"
    actual = _find_plain_references(message)
    assert [str(r) for r in actual] == [
        "https://example.com",
        "https://example.com",
    ]


def test_find_plain_references_on_missing_word_break():
    message = "hellohttps://arxiv.org/a"
    actual = _find_plain_references(message)
    assert [str(r) for r in actual] == [
        "https://arxiv.org/a",
    ]


def test_find_plain_references_on_quoted_url():
    message = 'source: "https://arxiv.org/abs/1803.03453"'
    actual = _find_plain_references(message)
    assert [str(r) for r in actual] == [
        "https://arxiv.org/abs/1803.03453",
    ]


def test_find_plain_references_on_url_and_text():
    message = "https://arxiv.org/abs/1803.03453, what do you think?"
    actual = _find_plain_references(message)
    assert [str(r) for r in actual] == [
        "https://arxiv.org/abs/1803.03453",
    ]


def test_find_plain_references_removes_trailing_question_mark():
    message = "summarize https://arxiv.org/abs/1803.03453?"
    actual = _find_plain_references(message)
    assert [str(r) for r in actual] == [
        "https://arxiv.org/abs/1803.03453",
    ]
