import pytest
import base64

from io import BytesIO
from PIL import Image, UnidentifiedImageError

from base.api.documents import DocOptions, HtmlOptions, TranscriptOptions
from base.strings.data import MimeType

from documents.extractors.image import (
    ImageExtractor,
    image_bytes_as_data_uri,
    image_bytes_as_data_uri_sync,
)
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


def _create_test_image(
    image_format: str = "PNG",
    image_size: tuple[int, int] = (100, 100),
) -> bytes:
    """Create a simple test image in the specified format."""
    image = Image.new("RGB", image_size, color="red")
    buffer = BytesIO()
    image.save(buffer, format=image_format)
    return buffer.getvalue()


def _create_downloaded_image(
    image_bytes: bytes,
    mime_type: str = "image/png",
) -> DownloadedData:
    """Create a Downloaded object from image bytes."""
    return DownloadedData(
        url=None,
        response_headers={},
        name="test.png",
        mime_type=MimeType.decode(mime_type),
        filename=None,
        charset=None,
        data=image_bytes,
    )


##
## ImageExtractor.match
##


def test_image_extractor_match_png() -> None:
    extractor = ImageExtractor()
    image_bytes = _create_test_image("PNG")
    downloaded = _create_downloaded_image(image_bytes, "image/png")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is True


def test_image_extractor_match_jpeg() -> None:
    extractor = ImageExtractor()
    image_bytes = _create_test_image("JPEG")
    downloaded = _create_downloaded_image(image_bytes, "image/jpeg")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is True


def test_image_extractor_match_webp() -> None:
    extractor = ImageExtractor()
    image_bytes = _create_test_image("WEBP")
    downloaded = _create_downloaded_image(image_bytes, "image/webp")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is True


def test_image_extractor_no_match_text() -> None:
    extractor = ImageExtractor()
    downloaded = Downloaded.stub_text(mime_type="text/plain", text="Hello")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is False


def test_image_extractor_no_match_pdf() -> None:
    extractor = ImageExtractor()
    downloaded = Downloaded.stub_text(mime_type="application/pdf", text="%PDF")
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is False


def test_image_extractor_no_match_no_mime_type() -> None:
    extractor = ImageExtractor()
    downloaded = DownloadedData(
        url=None,
        response_headers={},
        name=None,
        mime_type=None,
        filename=None,
        charset=None,
        data=b"some bytes",
    )
    options = _given_extract_options()
    assert extractor.match(downloaded, options) is False


##
## ImageExtractor.extract
##


@pytest.mark.asyncio
async def test_image_extractor_extract_png() -> None:
    extractor = ImageExtractor()
    image_bytes = _create_test_image("PNG")
    downloaded = _create_downloaded_image(image_bytes, "image/png")
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert extracted.mode == "markdown"
    assert extracted.text == "![](self://~)"
    assert len(extracted.blobs) == 1
    assert extracted.mime_type == MimeType.decode("image/png")


@pytest.mark.asyncio
async def test_image_extractor_extract_jpeg() -> None:
    extractor = ImageExtractor()
    image_bytes = _create_test_image("JPEG")
    downloaded = _create_downloaded_image(image_bytes, "image/jpeg")
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    assert extracted.mode == "markdown"
    assert "self://~" in extracted.text
    assert len(extracted.blobs) == 1


@pytest.mark.asyncio
async def test_image_extractor_extract_preserves_blob() -> None:
    extractor = ImageExtractor()
    image_bytes = _create_test_image("PNG", image_size=(50, 50))
    downloaded = _create_downloaded_image(image_bytes, "image/png")
    options = _given_extract_options()

    extracted = await extractor.extract(downloaded, options, user_id=None)

    # Check that the blob contains valid image data
    blob_uri = next(iter(extracted.blobs.keys()))
    data_uri = extracted.blobs[blob_uri]
    mime, data = data_uri.parts()
    assert mime == MimeType.decode("image/png")
    # Decode and verify it's a valid image
    decoded_bytes = base64.b64decode(data)
    img = Image.open(BytesIO(decoded_bytes))
    assert img.size == (50, 50)


##
## image_bytes_as_data_uri
##


@pytest.mark.asyncio
async def test_image_bytes_as_data_uri_with_mime_type() -> None:
    image_bytes = _create_test_image("PNG")
    mime_type = MimeType.decode("image/png")

    result = await image_bytes_as_data_uri(image_bytes, mime_type)

    assert result is not None
    mime, _ = result.parts()
    assert mime == mime_type


@pytest.mark.asyncio
async def test_image_bytes_as_data_uri_infers_mime_type() -> None:
    image_bytes = _create_test_image("PNG")

    result = await image_bytes_as_data_uri(image_bytes, None)

    assert result is not None
    mime, _ = result.parts()
    assert "png" in str(mime).lower()


@pytest.mark.asyncio
async def test_image_bytes_as_data_uri_raises_for_invalid() -> None:
    # Pass non-image bytes with no mime type - should raise
    with pytest.raises(UnidentifiedImageError):
        await image_bytes_as_data_uri(b"not an image", None)


##
## image_bytes_as_data_uri_sync
##


def test_image_bytes_as_data_uri_sync_with_mime_type() -> None:
    image_bytes = _create_test_image("JPEG")
    mime_type = MimeType.decode("image/jpeg")

    result = image_bytes_as_data_uri_sync(image_bytes, mime_type)

    assert result is not None
    mime, _ = result.parts()
    assert mime == mime_type


def test_image_bytes_as_data_uri_sync_infers_from_pillow() -> None:
    image_bytes = _create_test_image("PNG")

    result = image_bytes_as_data_uri_sync(image_bytes, None)

    assert result is not None


def test_image_bytes_as_data_uri_sync_non_image_mime_type() -> None:
    # If mime_type is provided but not an image type, it should try Pillow
    image_bytes = _create_test_image("PNG")
    mime_type = MimeType.decode("application/octet-stream")

    result = image_bytes_as_data_uri_sync(image_bytes, mime_type)

    # Should still succeed by detecting via Pillow
    assert result is not None


def test_image_bytes_as_data_uri_sync_raises_for_invalid_bytes() -> None:
    # Invalid bytes should raise when no mime_type is provided
    with pytest.raises(UnidentifiedImageError):
        image_bytes_as_data_uri_sync(b"definitely not an image", None)
