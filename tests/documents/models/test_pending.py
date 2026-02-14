import pytest
import tempfile
from pathlib import Path

from base.strings.data import MimeType
from base.strings.file import FileName
from base.strings.resource import WebUrl

from documents.models.pending import Downloaded, DownloadedData, DownloadedFile


##
## Downloaded.stub_text
##


def test_downloaded_stub_text_minimal():
    downloaded = Downloaded.stub_text(text="Hello, world!")
    assert downloaded.url is None
    assert downloaded.response_headers == {}
    assert downloaded.name is None
    assert downloaded.mime_type is None
    assert downloaded.filename is None
    assert downloaded.charset == "utf-8"
    assert downloaded.data == b"Hello, world!"


def test_downloaded_stub_text_with_options():
    downloaded = Downloaded.stub_text(
        url="https://example.com/file.txt",
        response_headers={"content-type": "text/plain"},
        name="My File",
        mime_type="text/plain",
        filename="file.txt",
        charset="utf-8",
        text="Hello, world!",
    )
    assert downloaded.url == WebUrl.decode("https://example.com/file.txt")
    assert downloaded.response_headers == {"content-type": "text/plain"}
    assert downloaded.name == "My File"
    assert downloaded.mime_type == MimeType.decode("text/plain")
    assert downloaded.filename == FileName.decode("file.txt")
    assert downloaded.charset == "utf-8"
    assert downloaded.data == b"Hello, world!"


##
## Downloaded.mime_type_forced
##


def test_downloaded_mime_type_forced_has_mime_type():
    downloaded = Downloaded.stub_text(mime_type="application/json", text="{}")
    assert downloaded.mime_type_forced() == MimeType.decode("application/json")


def test_downloaded_mime_type_forced_from_filename():
    downloaded = Downloaded.stub_text(filename="file.pdf", text="%PDF")
    assert downloaded.mime_type_forced() == MimeType.decode("application/pdf")


def test_downloaded_mime_type_forced_fallback_plain():
    downloaded = Downloaded.stub_text(text="Hello")
    assert downloaded.mime_type_forced() == MimeType.decode("text/plain")


##
## DownloadedData
##


def test_downloaded_data_open_bytes():
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        data=b"Test content",
    )
    with downloaded.open_bytes() as f:
        assert f.read() == b"Test content"


@pytest.mark.asyncio
async def test_downloaded_data_read_bytes_async():
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        data=b"Test content",
    )
    result = await downloaded.read_bytes_async()
    assert result == b"Test content"


def test_downloaded_data_read_text():
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        data="Héllo wörld".encode(),
    )
    assert downloaded.read_text() == "Héllo wörld"


@pytest.mark.asyncio
async def test_downloaded_data_read_text_async():
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        data="Héllo wörld".encode(),
    )
    result = await downloaded.read_text_async()
    assert result == "Héllo wörld"


##
## DownloadedFile
##


def test_downloaded_file_open_bytes():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as temp_file:
        temp_file.write(b"File content")
        temp_path = Path(temp_file.name)

    try:
        downloaded = DownloadedFile(
            url=None,
            response_headers={},
            name=None,
            mime_type=None,
            filename=None,
            charset="utf-8",
            tempfile_path=temp_path,
        )
        with downloaded.open_bytes() as f:
            assert f.read() == b"File content"
    finally:
        temp_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_downloaded_file_read_bytes_async():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as temp_file:
        temp_file.write(b"Async file content")
        temp_path = Path(temp_file.name)

    try:
        downloaded = DownloadedFile(
            url=None,
            response_headers={},
            name=None,
            mime_type=None,
            filename=None,
            charset="utf-8",
            tempfile_path=temp_path,
        )
        result = await downloaded.read_bytes_async()
        assert result == b"Async file content"
    finally:
        temp_path.unlink(missing_ok=True)  # noqa: ASYNC240


def test_downloaded_file_delete_tempfile():
    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as temp_file:
        temp_file.write(b"Content")
        temp_path = Path(temp_file.name)

    downloaded = DownloadedFile(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        tempfile_path=temp_path,
    )
    assert temp_path.exists()
    downloaded.delete_tempfile()
    assert not temp_path.exists()


def test_downloaded_file_delete_tempfile_missing_ok():
    # Create and immediately delete a file to get a valid but non-existent path
    with tempfile.NamedTemporaryFile(delete=True, suffix=".txt") as temp_file:
        temp_path = Path(temp_file.name)

    downloaded = DownloadedFile(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset="utf-8",
        tempfile_path=temp_path,
    )
    # Should not raise an error
    downloaded.delete_tempfile()
