import bs4
import pytest

from base.api.documents import DocOptions, HtmlOptions, TranscriptOptions
from base.strings.data import MimeType
from base.strings.file import FileName

from documents.extractors.html_page import HtmlPageExtractor, parse_title, parse_soup
from documents.models.pending import Downloaded, DownloadedData
from documents.models.processing import ExtractOptions


def _given_extract_options(
    original: bool = False,
    root_selector: str | None = None,
    ignore_selector: list[str] | None = None,
) -> ExtractOptions:
    return ExtractOptions(
        original=original,
        mime_type=None,
        doc=DocOptions(),
        html=HtmlOptions(
            root_selector=root_selector,
            ignore_selector=ignore_selector or [],
        ),
        transcript=TranscriptOptions(),
    )


##
## HtmlPageExtractor.match
##


def test_html_page_extractor_match_by_mime_type():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/html", text="<html></html>")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is True


def test_html_page_extractor_match_by_filename():
    extractor = HtmlPageExtractor()
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=FileName.decode("page.html"),
        charset="utf-8",
        data=b"<html></html>",
    )
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is True


def test_html_page_extractor_no_match_original_mode():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/html", text="<html></html>")
    options = _given_extract_options(original=True)
    assert extractor.match(downloaded, options) is False


def test_html_page_extractor_no_match_plain_text():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/plain", text="Hello")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is False


def test_html_page_extractor_no_match_pdf():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(mime_type="application/pdf", text="%PDF")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is False


##
## HtmlPageExtractor.extract
##


@pytest.mark.asyncio
async def test_html_page_extractor_extract_simple():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(
        url="https://example.com/page",
        mime_type="text/html",
        text="<html><head><title>Test</title></head><body><h1>Hello</h1><p>World</p></body></html>",
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert extracted.mode == "markdown"
    assert extracted.name == "Test"
    assert "Hello" in extracted.text
    assert "World" in extracted.text
    assert extracted.mime_type == MimeType.decode("text/html")


@pytest.mark.asyncio
async def test_html_page_extractor_extract_with_root_selector():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(
        url="https://example.com/page",
        mime_type="text/html",
        text="""
        <html>
            <body>
                <header>Header content</header>
                <main id="content">
                    <h1>Main Title</h1>
                    <p>Main paragraph</p>
                </main>
                <footer>Footer content</footer>
            </body>
        </html>
        """,
    )
    options = _given_extract_options(root_selector="#content")

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert "Main Title" in extracted.text
    assert "Main paragraph" in extracted.text
    # Header and footer should be excluded
    assert "Header content" not in extracted.text
    assert "Footer content" not in extracted.text


@pytest.mark.asyncio
async def test_html_page_extractor_extract_strips_script_and_style():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(
        url="https://example.com/page",
        mime_type="text/html",
        text="""
        <html>
            <head>
                <style>.hidden { display: none; }</style>
            </head>
            <body>
                <script>console.log('hello');</script>
                <h1>Visible Content</h1>
                <style>.more { color: red; }</style>
            </body>
        </html>
        """,
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert "Visible Content" in extracted.text
    assert "console.log" not in extracted.text
    assert "display: none" not in extracted.text
    assert ".hidden" not in extracted.text


@pytest.mark.asyncio
async def test_html_page_extractor_extract_strips_navigation():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(
        url="https://example.com/page",
        mime_type="text/html",
        text="""
        <html>
            <body>
                <nav>Navigation menu</nav>
                <header>Site header</header>
                <main>
                    <h1>Main Content</h1>
                </main>
                <footer>Site footer</footer>
            </body>
        </html>
        """,
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert "Main Content" in extracted.text
    # Navigation elements should be stripped
    assert "Navigation menu" not in extracted.text
    assert "Site header" not in extracted.text
    assert "Site footer" not in extracted.text


@pytest.mark.asyncio
async def test_html_page_extractor_extract_preserves_links():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(
        url="https://example.com/page",
        mime_type="text/html",
        text="""
        <html>
            <body>
                <p>Visit <a href="https://other.com/path">this link</a> for more info.</p>
            </body>
        </html>
        """,
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert "this link" in extracted.text
    assert "https://other.com/path" in extracted.text


@pytest.mark.asyncio
async def test_html_page_extractor_extract_relative_links():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(
        url="https://example.com/docs/page",
        mime_type="text/html",
        text="""
        <html>
            <body>
                <p>See <a href="/other-page">other page</a>.</p>
            </body>
        </html>
        """,
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    # Relative link should be resolved to absolute
    assert "https://example.com/other-page" in extracted.text


@pytest.mark.asyncio
async def test_html_page_extractor_extract_non_html_content():
    """When content is not well-formed HTML, lxml parses it anyway."""
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(
        mime_type="text/html",
        text="This is just plain text, not HTML.",
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    # lxml still parses plain text - it wraps it in html/body tags
    assert extracted.mode == "markdown"
    assert "This is just plain text, not HTML." in extracted.text


@pytest.mark.asyncio
async def test_html_page_extractor_extract_removes_empty_headings():
    extractor = HtmlPageExtractor()
    downloaded = Downloaded.stub_text(
        url="https://example.com/page",
        mime_type="text/html",
        text="""
        <html>
            <body>
                <h1></h1>
                <h2>   </h2>
                <h3>Real Heading</h3>
                <p>Content</p>
            </body>
        </html>
        """,
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert "Real Heading" in extracted.text
    assert "Content" in extracted.text


##
## parse_title
##


def test_parse_title_extracts_title():
    soup = bs4.BeautifulSoup(
        "<html><head><title>Page Title</title></head></html>",
        "lxml",
    )
    title = parse_title(soup)
    assert title == "Page Title"


def test_parse_title_strips_outer_whitespace():
    soup = bs4.BeautifulSoup(
        "<html><head><title>  Spaced  Title  </title></head></html>",
        "lxml",
    )
    title = parse_title(soup)
    # stripped_strings joins with single space, but preserves internal double spaces
    assert title == "Spaced  Title"


def test_parse_title_returns_none_when_missing():
    soup = bs4.BeautifulSoup(
        "<html><head></head><body></body></html>",
        "lxml",
    )
    title = parse_title(soup)
    assert title is None


##
## parse_soup
##


@pytest.mark.asyncio
async def test_parse_soup_basic():
    soup = bs4.BeautifulSoup(
        "<html><body><h1>Hello</h1><p>World</p></body></html>",
        "lxml",
    )
    content, blobs = await parse_soup(None, soup, None)

    assert "Hello" in content
    assert "World" in content
    assert blobs == {}


@pytest.mark.asyncio
async def test_parse_soup_with_root_selector():
    soup = bs4.BeautifulSoup(
        """
        <html>
            <body>
                <div id="wrapper">
                    <nav>Skip this</nav>
                    <div id="main">
                        <p>Only this content</p>
                    </div>
                </div>
            </body>
        </html>
        """,
        "lxml",
    )
    content, blobs = await parse_soup(None, soup, "#main")
    assert blobs == {}

    assert "Only this content" in content
    # Navigation should be present since we're selecting #main specifically
    # but the root selector limits what we parse
    assert "Skip this" not in content
