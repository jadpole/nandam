from pydantic import BaseModel, Field
from typing import Literal, Self

from base.api.utils import post_request
from base.config import BaseConfig
from base.core.exceptions import ApiError
from base.core.strings import ValidatedStr
from base.strings.auth import RequestId, UserId
from base.strings.data import DataUri, MimeType
from base.strings.file import FileName, FilePath, REGEX_FILEPATH
from base.strings.resource import WebUrl

from base.utils.markdown import strip_keep_indent

REGEX_FRAGMENT_URI = rf"self://(?:~|{REGEX_FILEPATH})"


##
## Model
##


FragmentMode = Literal["data", "markdown", "plain"]


class DocumentsApiError(ApiError):
    pass


class FragmentUri(ValidatedStr):
    @classmethod
    def singleton(cls) -> Self:
        return cls("self://~")

    @classmethod
    def new(cls, path: FilePath) -> Self:
        return cls(f"self://{path}")

    @classmethod
    def _parse(cls, v: str) -> Self:
        if v == "self://~":
            return cls("self://~")
        for part in v.removeprefix("self://").split("/"):
            FileName.decode_part(cls, v, part)
        return cls(v)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "self://figures/fig1.pdf",
            "self://image.png",
            "self://~",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_FRAGMENT_URI

    def path(self) -> list[FileName]:
        path_str = self.removeprefix("self://")
        return (
            [FileName.decode(part) for part in path_str.split("/")]
            if path_str != "~"
            else []
        )


class Fragment(BaseModel, frozen=True):
    mode: FragmentMode
    """
    The MIME type of the original file.
    """
    text: str
    """
    The textual content of the fragment, which may include links to or embeds of
    other resources, and must embed its blobs.
    """
    blobs: dict[FragmentUri, DataUri] = Field(default_factory=dict)
    """
    The content of blobs, which is binary data, usually images.

    The key will be used to construct an absolute `ObservableUri[AffBodyMedia]`.
    It must respect the following format:

    ```text
    self://{FilePath}
    ```

    The value is a Data URI with the format:

    ```text
    data:{media_type};base64,{data}
    ```

    All blobs must be embedded in the `text` using the syntax:

    ```text
    ![optional caption](self://{FilePath})
    ```
    """


##
## Read: just download the document, do not save tempfile
##


TranscriptFormat = Literal["original", "srt-dense", "srt-sparse", "text"]


class DocOptions(BaseModel):
    paginate: bool = False
    """
    Whether to paginate the output. If set to True, each page of the output
    will be separated by a horizontal rule that contains the page number.
    """
    disable_image_extraction: bool = False
    """
    Disable image extraction from the PDF. If use_llm is also set, then images
    will be automatically captioned.
    """
    use_llm: bool = False
    """
    Significantly improves accuracy by using an LLM to enhance tables, forms,
    inline math, and layout detection. Will increase latency.
    """
    disable_links: bool = False
    """
    Disable hyperlinks in the output (passed to Datalab additional_config).
    """
    filter_blank_pages: bool = False
    """
    Filter out blank pages from the output (passed to Datalab additional_config).
    """


class HtmlOptions(BaseModel):
    root_selector: str | None = None
    """
    When present, returns the contents of the element(s) that match these CSS
    selector and ignore everything else.
    """
    ignore_selector: list[str] = Field(default_factory=list)
    """
    When present, ignores all elements that match these CSS selectors.
    """


class TranscriptOptions(BaseModel):
    deduplicate: bool = True
    """
    Whether the segments of the SRT transcript should be deduplicated, since
    that is a common Whisper hallucination.

    By default, it is enabled, but in some situations (e.g., when analyzing the
    lyrics of a song, where repetitions are expected), it can be disabled.
    """
    format: TranscriptFormat | None = None
    """
    - "text" returns the transcript as a single text block (not SRT);
    - "srt-sparse" groups the SRT transcript into chunks, such that each one
      corresponds to 5 minutes of "real time" (i.e., keeping silences).
    - "srt-dense" groups the SRT transcript into denser chunks, such that each
      one corresponds to 5 minutes of "voice time" (i.e., ignoring silences).
    - "original" returns the Whisper SRT output as-is (one segment per line);

    NOTE: The default setting, given None, is "srt-dense", since it serves as a
    good default for most practical cases.
    """
    language: str | None = None
    """
    The optional language of the document, which can be used to improve the transcription.
    """


class DocumentsDownloadRequest(BaseModel):
    url: WebUrl
    """
    A public (or signed) URL to the document.
    """
    headers: dict[str, str] | None = None
    """
    Custom headers that should be sent in the HTTP request fetching the URL.
    """
    put_url: str | None = None
    """
    A signed PUT URL for AWS S3, which allows Documents to write the original
    file (unparsed) to the bucket, when it is "worth saving".  It is used for
    formats that tools or Code Interpreter may use later.

    TODO: The file is NOT encrypted.  Since the encryption key belongs in the
    Knowledge provisions, it cannot be read from Documents.  Supporting this
    requires passing the key as another argument.
    """
    original: bool = False
    """
    Whether Documents should perform post-processing on the file.

    For example, on web pages,

    - True returns the raw HTML of the page (treated like code).
    - False returns the text of the page as Markdown, with image as embeds.
    """
    mime_type: MimeType | None = None
    """
    The MIME type of the document, when it is known beforehand.
    """
    doc: DocOptions = Field(default_factory=DocOptions)
    """
    The options for document (PDF, Word, PowerPoint) processing.
    """
    html: HtmlOptions = Field(default_factory=HtmlOptions)
    """
    The options for "text/html" parsing, which when present, are applied in both
    "original" and "markdown" modes.
    """
    transcript: TranscriptOptions = Field(default_factory=TranscriptOptions)
    """
    The options for transcript generation from audio/video files.
    """


class DocumentsBlobRequest(BaseModel):
    name: str
    """
    The name of the file.
    """
    mime_type: MimeType | None
    """
    The MIME type of the file, to determine how to process its bytes.
    """
    blob: str
    """
    The file bytes encoded as base64.
    """
    original: bool = False
    """
    Whether Documents should perform post-processing on the file.

    For example, on web pages,

    - True returns the raw HTML of the page (treated like code).
    - False returns the text of the page as Markdown, with images as blobs.
    """
    doc: DocOptions = Field(default_factory=DocOptions)
    """
    The options for document (PDF, Word, PowerPoint) processing.
    """
    html: HtmlOptions = Field(default_factory=HtmlOptions)
    """
    The options for "text/html" parsing, which when present, are applied in both
    "original" and "markdown" modes.
    """
    transcript: TranscriptOptions = Field(default_factory=TranscriptOptions)
    """
    The options for transcript generation from audio/video files.
    """


class DocumentsReadResponse(BaseModel):
    # Metadata
    name: str
    """
    The name of the document, which might be extracted from the file or from the
    response headers.  When a human-friendly name cannot be obtained, defaults
    to the file name instead (from the response headers or the URL).
    """
    mime_type: MimeType
    """
    The MIME type of the original (raw) file.

    NOTE: This is not the MIME type of `text`, since the file is translated into
    Markdown or, at least, some textual representation is extracted.
    """
    headers: dict[str, str]
    """
    Some services (notably GitLab) include response headers that Knowledge cares
    about.  They are included here in an unstructured manner.
    """
    # Content
    mode: FragmentMode
    """
    The mode used to interpret `text` and `blobs`.
    """
    text: str
    """
    The textual content of the resource.

    NOTE: When the request URL points to an image, the resulting `text` is just
    the Markdown embed, `![](self://~)`, with the image data in `blobs`.
    """
    blobs: dict[FragmentUri, DataUri] = Field(default_factory=dict)
    """
    The content of blobs, which is binary data, usually images.

    The key will be used to construct an absolute `ObservableUri[AffBodyMedia]`.
    It must respect the following format:

    ```text
    self://{FilePath}
    ```

    The value is a Data URI with the format:

    ```text
    data:{media_type};base64,{data}
    ```

    All blobs must be embedded in the `text` using the syntax:

    ```text
    ![optional caption](self://{FilePath})
    ```

    The raw bytes of the image are returned as base64, with the full resolution,
    such that any post-processing is delegated to Knowledge.
    """

    @staticmethod
    def from_plain(
        name: str,
        headers: dict[str, str],
        text: str,
    ) -> "DocumentsReadResponse":
        return DocumentsReadResponse(
            name=name,
            mime_type=MimeType.decode("text/plain"),
            headers=headers,
            mode="plain",
            text=text.replace("\r\n", "\n"),
            blobs={},
        )

    @staticmethod
    def from_image(
        name: str,
        headers: dict[str, str],
        data: DataUri,
    ) -> "DocumentsReadResponse":
        mime_type = data.parts()[0]
        return DocumentsReadResponse(
            name=name,
            mime_type=mime_type,
            headers=headers,
            mode="markdown",
            text="![](self://~)",
            blobs={FragmentUri.singleton(): data},
        )

    def as_fragment(self) -> Fragment:
        return Fragment(
            mode=self.mode,
            text=strip_keep_indent(self.text),
            blobs=self.blobs,
        )


async def documents_blob(
    req: DocumentsBlobRequest,
    request_id: RequestId | None,
    user_id: UserId | None,
) -> DocumentsReadResponse:
    if not BaseConfig.api.documents_host:
        from documents.models.exceptions import DocumentsError  # noqa: PLC0415
        from documents.routers.read import post_blob  # noqa: PLC0415

        try:
            return await post_blob(
                req=req,
                x_request_id=request_id,
                x_user_id=user_id,
            )
        except DocumentsError as exc:
            raise DocumentsApiError.from_exception(exc) from exc

    return await post_request(
        endpoint=f"{BaseConfig.api.documents_host}/v1/blob",
        payload=req,
        type_exc=DocumentsApiError,
        type_resp=DocumentsReadResponse,
        request_id=request_id,
        user_id=user_id,
        authorization=None,
    )


async def documents_download(
    req: DocumentsDownloadRequest,
    authorization: str | None,
    request_id: RequestId | None,
    user_id: UserId | None,
) -> DocumentsReadResponse:
    if not BaseConfig.api.documents_host:
        from documents.models.exceptions import DocumentsError  # noqa: PLC0415
        from documents.routers.read import post_download  # noqa: PLC0415

        try:
            return await post_download(
                req=req,
                authorization=authorization,
                x_request_id=request_id,
                x_user_id=user_id,
            )
        except DocumentsError as exc:
            raise DocumentsApiError.from_exception(exc) from exc

    return await post_request(
        endpoint=f"{BaseConfig.api.documents_host}/v1/download",
        payload=req,
        type_exc=DocumentsApiError,
        type_resp=DocumentsReadResponse,
        request_id=request_id,
        user_id=user_id,
        authorization=authorization,
    )
