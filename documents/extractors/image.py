import aiohttp
import asyncio
import pypdfium2

from io import BytesIO
from pathlib import Path
from PIL import Image

from base.api.documents import FragmentUri
from base.strings.data import DataUri, MimeType
from base.strings.resource import WebUrl

from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted
from documents.models.processing import ExtractOptions, Extractor


class ImageExtractor(Extractor):
    """
    When the user directly downloads an image file, we embed it in a Markdown
    file, so it becomes a "fragment" that can be sent to LLMs.

    The exception is when the "original" format is requested, where, we return
    the `"data:{content_type};base64,{data}"` URI directly.
    """

    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(downloaded.mime_type and downloaded.mime_type.mode() == "image")

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        image_bytes = await downloaded.read_bytes_async()
        image_data = await image_bytes_as_data_uri(image_bytes, downloaded.mime_type)
        if not image_data:
            raise ExtractError.fail("image", "cannot infer MIME type")

        image_uri = FragmentUri.singleton()
        return Extracted(
            mode="markdown",
            name=None,
            path=None,
            mime_type=image_data.parts()[0],
            blobs={image_uri: image_data},
            text=f"![]({image_uri})",
        )


async def image_bytes_as_data_uri(
    image_data: bytes,
    mime_type: MimeType | None,
) -> DataUri | None:
    return await asyncio.to_thread(image_bytes_as_data_uri_sync, image_data, mime_type)


def image_bytes_as_data_uri_sync(
    image_data: bytes,
    mime_type: MimeType | None,
) -> DataUri | None:
    """
    Convert the bytes into a Data URI as-is, without any processing.
    If no MIME type is provided, then it is inferred by Pillow.

    NOTE: Return `None` if the MIME type cannot be inferred.
    """
    if mime_type and mime_type.mode() == "image":
        return DataUri.new(mime_type, image_data)

    image = None
    try:
        image = Image.open(BytesIO(image_data))
        if image.format and (mime_str := Image.MIME.get(image.format)):
            return DataUri.new(MimeType(mime_str), image_data)

        return None
    finally:
        if image:
            image.close()


##
## Utils for other extractors
##


async def download_image_as_data_uri(url: WebUrl) -> DataUri | None:
    """
    Download an image from the Web, and convert it into a Data URI.

    NOTE: Return `None` if the image cannot be downloaded or if its MIME type
    cannot be inferred by Pillow.
    """
    try:
        async with aiohttp.ClientSession() as session:  # noqa: SIM117
            async with session.get(str(url)) as response:
                if response.status != 200:  # noqa: PLR2004
                    return None

                image_data = await response.read()
                return await image_bytes_as_data_uri(image_data, None)
    except Exception:
        return None


def load_image_as_data_uri_sync(file_path: Path) -> DataUri | None:
    """
    Load a local image and convert its to WEBP as a Data URI.
    Invoked for figures embedded in other documents, such as LaTeX archives.

    NOTE: Return `None` if the image cannot be opened or if its MIME type cannot
    be inferred by Pillow.
    """
    try:
        image_bytes = file_path.read_bytes()
        return image_bytes_as_data_uri_sync(image_bytes, None)
    except Exception:
        return None


def load_pdf_as_data_uri_sync(file_path: Path) -> DataUri | None:
    """
    Load a local PDF file and convert its first page to WEBP as a Data URI.
    Invoked for figures embedded in other documents, such as LaTeX archives.

    NOTE: Assumes that, when PDFs are embedded as figures, there is one page;
    otherwise, there's no way to map many images to a single `FragmentUri`.

    NOTE: Return `None` if the PDF cannot be opened, is empty, or cannot be
    converted into an image.
    """
    try:
        pdf = pypdfium2.PdfDocument(file_path)
        if len(pdf) == 0:
            raise ExtractError.fail("image", "empty PDF")

        image: Image.Image = pdf[0].render(scale=4).to_pil()
        image = image.convert("RGBA")

        buffered = BytesIO()
        image.save(buffered, format="webp", optimize=True)
        return DataUri.new(MimeType("image/webp"), buffered.getvalue())
    except Exception:
        return None
