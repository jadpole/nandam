import aiohttp
import json

from base.api.documents import FragmentUri
from base.strings.data import DataUri, MimeType
from base.strings.file import FilePath

from documents.config import DocumentsConfig
from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted, DownloadedFile
from documents.models.processing import ExtractOptions, Extractor


class PdfExtractor(Extractor):
    """
    Call the Datalab API to parse the PDF into a Markdown file, using OCR on
    images and extracting figures as blobs.
    """

    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(
            DocumentsConfig.datalab.api_key
            and downloaded.mime_type == "application/pdf",
        )

    async def extract(  # noqa: C901, PLR0912
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        assert DocumentsConfig.datalab.api_key

        if not isinstance(downloaded, DownloadedFile):
            raise ExtractError.fail("pdf", "requires DownloadedFile")

        try:
            async with aiohttp.ClientSession() as session:
                # Start a Datalab job to parse the PDF into Markdown.
                request_id: str
                with downloaded.tempfile_path.open("rb") as file:
                    form_data = aiohttp.FormData()
                    form_data.add_field("file", file)
                    form_data.add_field("langs", "en,fr")
                    form_data.add_field("output_format", "markdown")

                    # Apply document processing options
                    if options.doc.paginate:
                        form_data.add_field("paginate", "true")
                    if options.doc.disable_image_extraction:
                        form_data.add_field("disable_image_extraction", "true")
                    if options.doc.use_llm:
                        form_data.add_field("use_llm", "true")

                    # Build additional_config for Datalab
                    additional_config = {}
                    if options.doc.disable_links:
                        additional_config["disable_links"] = True
                    if options.doc.filter_blank_pages:
                        additional_config["filter_blank_pages"] = True
                    if additional_config:
                        form_data.add_field(
                            "additional_config", json.dumps(additional_config)
                        )

                    async with session.post(
                        "https://www.datalab.to/api/v1/marker",
                        headers={"X-API-Key": DocumentsConfig.datalab.api_key},
                        data=form_data,
                    ) as response:
                        response.raise_for_status()
                        data = await response.json()
                        request_id = data["request_id"]

                # Poll Datalab until the job is complete.
                markdown: str = ""
                images: dict[str, str] = {}
                while not markdown:
                    async with session.get(
                        f"https://www.datalab.to/api/v1/marker/{request_id}",
                        headers={"X-API-Key": DocumentsConfig.datalab.api_key},
                    ) as response:
                        response.raise_for_status()
                        data = await response.json()
                        if error := data.get("error"):
                            raise ExtractError.fail("pdf", f"Datalab error: {error}")
                        elif data["status"] == "complete":
                            markdown = data["markdown"]
                            images = data.get("images") or {}

                # Translate the output into the Documents format.
                blobs: dict[FragmentUri, DataUri] = {}
                for filename, data_base64 in images.items():
                    if not (media_type := MimeType.guess_from_bytes(data_base64)):
                        continue
                    image_uri = FragmentUri.new(FilePath.decode(filename))
                    blobs[image_uri] = DataUri.new(media_type, data_base64)
                    markdown = markdown.replace(f"]({filename})", f"]({image_uri})")

                return Extracted(
                    mode="markdown",
                    name=None,  # TODO
                    path=None,
                    mime_type=MimeType.decode("application/pdf"),
                    blobs=blobs,
                    text=markdown,
                )
        except ExtractError:
            raise
        except Exception as exc:
            raise ExtractError.fail("pdf", str(exc)) from exc
