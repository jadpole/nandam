import pytest

from base.api.documents import DocOptions, HtmlOptions, TranscriptOptions
from base.strings.data import MimeType

from documents.domain.processing import convert_document_response, run_extract
from documents.models.exceptions import DocumentsError
from documents.models.pending import Downloaded, DownloadedData, Extracted
from documents.models.processing import ExtractOptions

##
## convert_document_response
##


def test_convert_document_response_uses_extracted_name():
    downloaded = DownloadedData(
        url=None,
        response_headers={"x-custom": "value"},
        name="Downloaded Name",
        mime_type=MimeType.decode("text/html"),
        filename=None,
        charset="utf-8",
        data=b"test",
    )
    extracted = Extracted(
        mode="markdown",
        name="Extracted Name",
        path=None,
        mime_type=MimeType.decode("text/markdown"),
        blobs={},
        text="# Hello",
    )
    response = convert_document_response(downloaded, extracted)
    assert response.name == "Extracted Name"
    assert response.mode == "markdown"
    assert response.text == "# Hello"
    assert response.headers == {"x-custom": "value"}


def test_convert_document_response_fallback_to_downloaded_name():
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name="Downloaded Name",
        mime_type=MimeType.decode("text/html"),
        filename=None,
        charset="utf-8",
        data=b"test",
    )
    extracted = Extracted(
        mode="markdown",
        name=None,
        path=None,
        mime_type=MimeType.decode("text/markdown"),
        blobs={},
        text="# Hello",
    )
    response = convert_document_response(downloaded, extracted)
    assert response.name == "Downloaded Name"


def test_convert_document_response_fallback_to_unknown():
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        data=b"test",
    )
    extracted = Extracted(
        mode="plain",
        name=None,
        path=None,
        mime_type=MimeType.decode("text/plain"),
        blobs={},
        text="Hello",
    )
    response = convert_document_response(downloaded, extracted)
    assert response.name == "unknown"


def test_convert_document_response_normalizes_newlines():
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name="Test",
        mime_type=None,
        filename=None,
        charset="utf-8",
        data=b"test",
    )
    extracted = Extracted(
        mode="plain",
        name=None,
        path=None,
        mime_type=MimeType.decode("text/plain"),
        blobs={},
        text="Line 1\r\nLine 2\r\nLine 3",
    )
    response = convert_document_response(downloaded, extracted)
    assert response.text == "Line 1\nLine 2\nLine 3"


def test_convert_document_response_uses_extracted_mime_type():
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name="Test",
        mime_type=MimeType.decode("text/html"),
        filename=None,
        charset="utf-8",
        data=b"test",
    )
    extracted = Extracted(
        mode="markdown",
        name=None,
        path=None,
        mime_type=MimeType.decode("text/markdown"),
        blobs={},
        text="# Hello",
    )
    response = convert_document_response(downloaded, extracted)
    assert response.mime_type == MimeType.decode("text/markdown")


##
## run_extract
##


def _given_extract_options(
    original: bool = False,
    mime_type: MimeType | None = None,
) -> ExtractOptions:
    return ExtractOptions(
        original=original,
        mime_type=mime_type,
        doc=DocOptions(),
        html=HtmlOptions(),
        transcript=TranscriptOptions(),
    )


@pytest.mark.asyncio
async def test_run_extract_plain_text():
    downloaded = Downloaded.stub_text(
        mime_type="text/plain",
        text="Hello, world!",
    )
    options = _given_extract_options()
    extracted = await run_extract(downloaded, options, user_id=None)
    assert extracted.mode == "plain"
    assert extracted.text == "Hello, world!"


@pytest.mark.asyncio
async def test_run_extract_markdown():
    downloaded = Downloaded.stub_text(
        mime_type="text/markdown",
        text="# Hello\n\nWorld!",
    )
    options = _given_extract_options()
    extracted = await run_extract(downloaded, options, user_id=None)
    assert extracted.mode == "markdown"
    assert extracted.text == "# Hello\n\nWorld!"


@pytest.mark.asyncio
async def test_run_extract_html_page():
    downloaded = Downloaded.stub_text(
        url="https://example.com/page",
        mime_type="text/html",
        text="<html><head><title>Test Page</title></head><body><h1>Hello</h1><p>World</p></body></html>",
    )
    options = _given_extract_options()
    extracted = await run_extract(downloaded, options, user_id=None)
    assert extracted.mode == "markdown"
    assert "Hello" in extracted.text
    assert extracted.name == "Test Page"


@pytest.mark.asyncio
async def test_run_extract_html_original_mode():
    downloaded = Downloaded.stub_text(
        mime_type="text/html",
        filename="test.html",
        text="<html><body><p>Hello</p></body></html>",
    )
    # With original=True, HTML should not be converted to markdown
    options = _given_extract_options(original=True)
    extracted = await run_extract(downloaded, options, user_id=None)
    # Original mode should return the content as-is (plain text)
    assert extracted.mode == "plain"
    assert "<html>" in extracted.text


@pytest.mark.asyncio
async def test_run_extract_unknown_format_uses_unstructured():
    """Unknown formats fall back to the unstructured extractor, which may fail."""
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=MimeType.decode("application/octet-stream"),
        filename=None,
        charset=None,
        data=b"\x00\x01\x02\x03",
    )
    options = _given_extract_options()
    # The unstructured extractor catches unknown formats and raises ExtractError
    with pytest.raises(DocumentsError, match="unstructured"):
        await run_extract(downloaded, options, user_id=None)
