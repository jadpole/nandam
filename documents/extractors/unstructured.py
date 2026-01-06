import asyncio

from unstructured.partition.auto import partition

from base.strings.data import MIME_TYPE_PLAIN

from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted
from documents.models.processing import ExtractOptions, Extractor


class UnstructuredExtractor(Extractor):
    """
    Uses the Unstructured library to parse the document types that were not
    matched to anything else.
    """

    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return True

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        return await asyncio.to_thread(_extract_unstructured_sync, downloaded)


def _extract_unstructured_sync(downloaded: Downloaded) -> Extracted:
    try:
        with downloaded.open_bytes() as file:
            elements = partition(file=file)
            content = "\n\n".join([str(el) for el in elements])
            return Extracted(
                mode="markdown",
                name=None,
                path=None,
                mime_type=downloaded.mime_type or MIME_TYPE_PLAIN,
                blobs={},
                text=content,
            )
    except Exception as exc:
        raise ExtractError.fail("unstructured", str(exc))  # noqa: B904
