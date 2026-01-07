import base64
import logging
import tempfile

from fastapi import APIRouter, File, Header, UploadFile
from fastapi.responses import PlainTextResponse
from pathlib import Path
from typing import Annotated

from base.api.documents import (
    DocOptions,
    DocumentsBlobRequest,
    DocumentsDownloadRequest,
    DocumentsReadResponse,
    HtmlOptions,
    TranscriptFormat,
    TranscriptOptions,
)
from base.core.exceptions import ApiError
from base.strings.auth import RequestId, UserId
from base.strings.data import MIME_TYPES_USELESS, MimeType
from base.strings.file import FileName

from documents.domain.processing import (
    convert_document_response,
    run_download_and_extract,
    run_extract,
)
from documents.models.exceptions import DocumentsError, DownloadError
from documents.models.pending import DownloadedFile
from documents.models.processing import ExtractOptions

logger = logging.getLogger(__name__)
router = APIRouter(tags=["read"])


class MarkdownResponse(PlainTextResponse):
    media_type = "text/markdown"


@router.post("/v1/blob")
async def post_blob(
    req: DocumentsBlobRequest,
    x_request_id: Annotated[RequestId | None, Header()] = None,
    x_user_id: Annotated[str | None, Header()] = None,
) -> DocumentsReadResponse:
    download: DownloadedFile | None = None
    try:
        options = ExtractOptions(
            original=req.original,
            mime_type=req.mime_type,
            html=req.html,
            transcript=req.transcript,
            doc=req.doc,
        )

        downloaded = _save_blob_file(req.name, req.mime_type, req.blob)
        extracted = await run_extract(downloaded, options, x_request_id, x_user_id)
        return convert_document_response(downloaded, extracted)
    except ApiError:
        raise
    except Exception as exc:
        raise DocumentsError.from_exception(exc) from exc
    finally:
        if download:
            download.delete_tempfile()


@router.post("/v1/download")
async def post_download(
    req: DocumentsDownloadRequest,
    authorization: Annotated[str | None, Header()] = None,
    x_request_id: Annotated[RequestId | None, Header()] = None,
    x_user_id: Annotated[UserId | None, Header()] = None,
) -> DocumentsReadResponse:
    try:
        options = ExtractOptions(
            original=req.original,
            mime_type=req.mime_type,
            html=req.html,
            transcript=req.transcript,
            doc=req.doc,
        )

        return await run_download_and_extract(
            req.url,
            options,
            req.headers or {},
            authorization,
            x_request_id,
            x_user_id,
        )
    except ApiError:
        raise
    except Exception as exc:
        raise DocumentsError.from_exception(exc) from exc


@router.post("/v1/upload")
async def post_upload(
    file: Annotated[UploadFile, File()],
    x_original: Annotated[bool, Header()] = False,
    x_mime_type: Annotated[MimeType | None, Header()] = None,
    x_doc_paginate: Annotated[bool, Header()] = False,
    x_doc_disable_image_extraction: Annotated[bool, Header()] = False,
    x_doc_use_llm: Annotated[bool, Header()] = False,
    x_doc_disable_links: Annotated[bool, Header()] = False,
    x_doc_filter_blank_pages: Annotated[bool, Header()] = False,
    x_html_ignore_selector: Annotated[list[str] | None, Header()] = None,
    x_html_root_selector: Annotated[str | None, Header()] = None,
    x_transcript_deduplicate: Annotated[bool, Header()] = True,
    x_transcript_format: Annotated[TranscriptFormat | None, Header()] = None,
    x_request_id: Annotated[RequestId | None, Header()] = None,
    x_user_id: Annotated[str | None, Header()] = None,
) -> DocumentsReadResponse:
    download: DownloadedFile | None = None
    try:
        options = ExtractOptions(
            original=x_original,
            mime_type=x_mime_type,
            doc=DocOptions(
                paginate=x_doc_paginate,
                disable_image_extraction=x_doc_disable_image_extraction,
                use_llm=x_doc_use_llm,
                disable_links=x_doc_disable_links,
                filter_blank_pages=x_doc_filter_blank_pages,
            ),
            html=HtmlOptions(
                root_selector=x_html_root_selector,
                ignore_selector=x_html_ignore_selector or [],
            ),
            transcript=TranscriptOptions(
                deduplicate=x_transcript_deduplicate,
                format=x_transcript_format,
            ),
        )
        downloaded = await _save_uploaded_file(file)
        extracted = await run_extract(downloaded, options, x_request_id, x_user_id)
        return convert_document_response(downloaded, extracted)
    except ApiError:
        raise
    except Exception as exc:
        raise DocumentsError.from_exception(exc) from exc
    finally:
        if download:
            download.delete_tempfile()


def _save_blob_file(name: str, mime_type: MimeType | None, blob: str) -> DownloadedFile:
    filename = FileName.try_normalize(name)
    if not filename:
        raise DownloadError.bad_filename(name)

    with tempfile.NamedTemporaryFile(delete=False, suffix=filename.ext()) as temp_file:
        tempfile_path = Path(temp_file.name)
        temp_file.write(base64.b64decode(blob))

    return DownloadedFile(
        url=None,
        response_headers={},
        name=name,
        mime_type=mime_type,
        filename=filename,
        charset=None,
        tempfile_path=tempfile_path,
    )


async def _save_uploaded_file(file: UploadFile) -> DownloadedFile:
    filename = FileName.try_normalize(file.filename or "")
    if not filename:
        raise DownloadError.bad_filename(file.filename or "")

    with tempfile.NamedTemporaryFile(delete=False, suffix=filename.ext()) as temp_file:
        temp_file.write(await file.read())

    if file.content_type and file.content_type not in MIME_TYPES_USELESS:
        media_type = MimeType.decode(file.content_type)
    else:
        media_type = MimeType.guess(filename, False)

    return DownloadedFile(
        url=None,
        response_headers={},
        name=file.filename,
        mime_type=media_type,
        filename=filename,
        charset=None,
        tempfile_path=Path(temp_file.name),
    )


##
## Debug
##


@router.post("/debug/download", response_class=MarkdownResponse)
async def download_debug(
    req: DocumentsDownloadRequest,
    x_authorization: Annotated[str | None, Header()] = None,
    x_request_id: Annotated[RequestId | None, Header()] = None,
    x_user_id: Annotated[UserId | None, Header()] = None,
) -> str:
    extract = await post_download(req, x_authorization, x_request_id, x_user_id)
    return _format_debug(extract)


@router.post("/debug/upload", response_class=MarkdownResponse)
async def upload_debug(
    file: Annotated[UploadFile, File()],
    x_original: Annotated[bool, Header()] = False,
    x_mime_type: Annotated[MimeType | None, Header()] = None,
    x_doc_paginate: Annotated[bool, Header()] = False,
    x_doc_disable_image_extraction: Annotated[bool, Header()] = False,
    x_doc_use_llm: Annotated[bool, Header()] = False,
    x_doc_disable_links: Annotated[bool, Header()] = False,
    x_doc_filter_blank_pages: Annotated[bool, Header()] = False,
    x_html_ignore_selector: Annotated[list[str] | None, Header()] = None,
    x_html_root_selector: Annotated[str | None, Header()] = None,
    x_transcript_deduplicate: Annotated[bool, Header()] = True,
    x_transcript_format: Annotated[TranscriptFormat | None, Header()] = None,
    x_user_id: Annotated[str | None, Header()] = None,
) -> str:
    response = await post_upload(
        file=file,
        x_original=x_original,
        x_mime_type=x_mime_type,
        x_doc_paginate=x_doc_paginate,
        x_doc_disable_image_extraction=x_doc_disable_image_extraction,
        x_doc_use_llm=x_doc_use_llm,
        x_doc_disable_links=x_doc_disable_links,
        x_doc_filter_blank_pages=x_doc_filter_blank_pages,
        x_html_ignore_selector=x_html_ignore_selector,
        x_html_root_selector=x_html_root_selector,
        x_transcript_deduplicate=x_transcript_deduplicate,
        x_transcript_format=x_transcript_format,
        x_user_id=x_user_id,
    )
    return _format_debug(response)


def _format_debug(response: DocumentsReadResponse) -> str:
    blobs_list = "\n".join([f"- {name}" for name in response.blobs])
    headers_list = "\n".join(
        f"- {name}: {value}" for name, value in response.headers.items()
    )
    return f"""
---
Mode: {response.mode}
Name: {response.name}
MIME-Type: {response.mime_type}
Headers:
{headers_list or "  []"}
Blobs:
{blobs_list or "  []"}
---

{response.text}
""".strip()
