from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted
from documents.models.processing import ExtractOptions, Extractor


class UnstructuredExtractor(Extractor):
    """
    Handle file types that were not matched by any other extractor.
    """

    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return True

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        raise ExtractError.fail("unstructured", "not supported")
