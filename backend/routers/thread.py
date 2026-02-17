import logging

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field
from typing import Annotated, Literal

from base.core.exceptions import ApiError, AuthorizationError
from base.server.auth import NdAuth
from base.strings.thread import ThreadCursor, ThreadUri

from backend.domain.thread import list_messages, unsafe_save_user_message
from backend.models.api_client import ClientAttachment
from backend.models.api_system import ActionResponse
from backend.models.workspace_thread import ThreadMessage_
from backend.services.kv_store import SvcKVStore
from backend.services.threads import SvcThreads

logger = logging.getLogger(__name__)
router = APIRouter(tags=["thread"])


class ApiThreadListRequest(BaseModel, frozen=True):
    source: ThreadUri | ThreadCursor


class ApiThreadListResponse(BaseModel, frozen=True):
    messages: list[ThreadMessage_]


@router.post("/api/thread/list")
async def post_api_thread_list(
    req: ApiThreadListRequest,
    authorization: Annotated[str | None, Header()] = None,
    x_authorization_client: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
    x_request_scope: Annotated[str | None, Header()] = None,
) -> ApiThreadListResponse:
    """
    TODO: Check workspace permissions.
    """
    try:
        _auth = NdAuth.from_headers(
            x_authorization_client=x_authorization_client,
            x_authorization_user=x_authorization_user or authorization,
            x_request_scope=x_request_scope,
        )

        kv_store = await SvcKVStore.initialize()
        threads = SvcThreads.initialize(req.source.workspace, kv_store)

        _, messages = await list_messages(threads, [req.source])
        return ApiThreadListResponse(messages=messages)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc


class ApiThreadSendRequest(BaseModel, frozen=True):
    thread_uri: ThreadUri
    message_format: Literal["markdown", "html"]
    message_text: str
    attachments: list[ClientAttachment] = Field(default_factory=list)


@router.post("/api/thread/send")
async def post_api_thread_send(
    req: ApiThreadSendRequest,
    authorization: Annotated[str | None, Header()] = None,
    x_authorization_client: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
    x_request_scope: Annotated[str | None, Header()] = None,
) -> ActionResponse:
    """
    TODO: Check workspace permissions.
    """
    try:
        auth = NdAuth.from_headers(
            x_authorization_client=x_authorization_client,
            x_authorization_user=x_authorization_user or authorization,
            x_request_scope=x_request_scope,
        )
        if not (sender := auth.validated_user_id()):
            raise AuthorizationError.unauthorized("authorized user required")

        kv_store = await SvcKVStore.initialize()
        threads = SvcThreads.initialize(req.thread_uri.workspace, kv_store)

        await unsafe_save_user_message(
            threads=threads,
            sender=sender,
            thread_uri=req.thread_uri,
            message_format=req.message_format,
            message_text=req.message_text,
            attachments=req.attachments,
        )
        return ActionResponse(success=True)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc
