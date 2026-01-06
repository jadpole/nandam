from base.strings.data import MIME_TYPE_PLAIN

from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted
from documents.models.processing import ExtractOptions, Extractor


class PlainTextExtractor(Extractor):
    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(
            downloaded.mime_type
            and downloaded.mime_type.mode() in ("markdown", "plain")
        )

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        try:
            if not downloaded.mime_type:
                raise ExtractError.fail("plain_text", "requires mime_type")

            return Extracted(
                mode=(
                    "markdown" if downloaded.mime_type.mode() == "markdown" else "plain"
                ),
                name=None,
                path=None,
                mime_type=downloaded.mime_type or MIME_TYPE_PLAIN,
                blobs={},
                text=await downloaded.read_text_async(),
            )
        except ExtractError:
            raise
        except Exception as exc:
            raise ExtractError.fail("plain_text", str(exc)) from exc
