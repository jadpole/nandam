import pytest

from base.api.documents import DocOptions, HtmlOptions, TranscriptOptions
from base.strings.data import MimeType

from documents.extractors.plain_text import PlainTextExtractor
from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, DownloadedData
from documents.models.processing import ExtractOptions


def _given_extract_options() -> ExtractOptions:
    return ExtractOptions(
        original=False,
        mime_type=None,
        doc=DocOptions(),
        html=HtmlOptions(),
        transcript=TranscriptOptions(),
    )


##
## PlainTextExtractor.match
##


def test_plain_text_extractor_match_plain():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/plain", text="Hello")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is True


def test_plain_text_extractor_match_markdown():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/markdown", text="# Hello")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is True


def test_plain_text_extractor_no_match_csv():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/csv", text="a,b,c")
    options = _given_extract_options()
    # CSV has mode "spreadsheet", not "plain" or "markdown"
    assert extractor.match(downloaded, options) is False


def test_plain_text_extractor_match_html():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/html", text="<html></html>")
    options = _given_extract_options()
    # HTML has mode "plain" (starts with text/), but HtmlPageExtractor handles it first
    assert extractor.match(downloaded, options) is True


def test_plain_text_extractor_no_match_pdf():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(mime_type="application/pdf", text="%PDF")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is False


def test_plain_text_extractor_no_match_no_mime_type():
    extractor = PlainTextExtractor()
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        data=b"Hello",
    )
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is False


##
## PlainTextExtractor.extract
##


@pytest.mark.asyncio
async def test_plain_text_extractor_extract_plain():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/plain", text="Hello, world!")
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert extracted.mode == "plain"
    assert extracted.text == "Hello, world!"
    assert extracted.mime_type == MimeType.decode("text/plain")
    assert extracted.blobs == {}
    assert extracted.name is None
    assert extracted.path is None


@pytest.mark.asyncio
async def test_plain_text_extractor_extract_markdown():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(
        mime_type="text/markdown",
        text="# Title\n\nParagraph",
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert extracted.mode == "markdown"
    assert extracted.text == "# Title\n\nParagraph"
    assert extracted.mime_type == MimeType.decode("text/markdown")


@pytest.mark.asyncio
async def test_plain_text_extractor_extract_with_unicode():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(
        mime_type="text/plain",
        text="Héllo wörld 你好 🌍",
        charset="utf-8",
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert extracted.text == "Héllo wörld 你好 🌍"


@pytest.mark.asyncio
async def test_plain_text_extractor_extract_preserves_whitespace():
    extractor = PlainTextExtractor()
    downloaded = Downloaded.stub_text(
        mime_type="text/plain",
        text="Line 1\n\n\nLine 2\n\t\tIndented",
    )
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert extracted.text == "Line 1\n\n\nLine 2\n\t\tIndented"


@pytest.mark.asyncio
async def test_plain_text_extractor_extract_requires_mime_type():
    extractor = PlainTextExtractor()
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        data=b"Hello",
    )
    options = _given_extract_options()

    with pytest.raises(ExtractError, match="requires mime_type"):
        await extractor.extract(downloaded, options, user_id=None)
