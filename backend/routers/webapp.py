from collections.abc import AsyncIterable
import logging

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse
from typing import Annotated

from backend.models.workspace_thread import BotMessagePart_
from backend.server.workspace_api import api_send_request
from base.core.exceptions import ApiError, AuthorizationError
from base.core.values import as_json
from base.server.auth import AuthClientConfig, ClientAuth, NdAuth, UserAuth
from base.strings.auth import RequestId, UserId
from base.strings.scope import ScopePersonal
from base.strings.thread import ThreadUri

from backend.domain.thread import unsafe_save_user_message
from backend.models.api_client import (
    ChatbotStream,
    ChatbotStreamError,
    ChatbotStreamProgress,
    ChatbotStreamResult,
    RequestClientApp,
)
from backend.models.bot_persona import AnyPersona_
from backend.models.process_info import ToolDefinition
from backend.models.workspace_action import (
    WorkspaceChatbotSpawn,
    WorkspaceResponseReply,
)
from backend.server.context import RequestConfig
from backend.services.kv_store import SvcKVStore
from backend.services.threads import SvcThreads

logger = logging.getLogger(__name__)
router = APIRouter(tags=["webapp"])


class ApiWebappChatbotRequest(BaseModel, frozen=True):
    workspace: str
    thread: str
    bot: str
    tools: list[ToolDefinition] = Field(default_factory=list)
    persona: AnyPersona_ | None = None
    message: str
    recv_timeout: int = 1


@router.post("/api/webapp/chatbot")
async def post_api_webapp_chatbot(
    req: ApiWebappChatbotRequest,
    authorization: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
) -> EventSourceResponse:
    """
    TODO: Check workspace permissions.
    """
    try:
        auth_user = UserAuth.from_header(x_authorization_user or authorization)
        if not auth_user:
            raise AuthorizationError.unauthorized("authorized used required")

        auth = NdAuth(
            client=ClientAuth(config=AuthClientConfig.webapp_client()),
            user=auth_user,
            scope=ScopePersonal(user_id=UserId.teams(auth_user.user_id)),
            request_id=RequestId.new(),
            x_user_id=None,
        )
        client = RequestClientApp(
            workspace=req.workspace,
            thread=req.thread,
            bot=req.bot,
        )
        client_info = client.request_info(auth)

        assert client_info.default_thread
        thread_uri = ThreadUri.new(client_info.workspace, client_info.default_thread)

        # Add the message to the thread.
        kv_store = await SvcKVStore.initialize()
        await unsafe_save_user_message(
            SvcThreads.initialize(client_info.workspace, kv_store),
            sender=UserId.teams(auth_user.user_id),
            thread_uri=thread_uri,
            message_format="markdown",
            message_text=req.message,
            attachments=[],
        )

        request = WorkspaceChatbotSpawn(
            workspace=client_info.workspace,
            request=RequestConfig(auth=client_info.auth),
            bot_id=client_info.bot_id,
            persona=req.persona,
            threads=[thread_uri],
            tools=req.tools,
            recv_timeout=req.recv_timeout,
        )
        return EventSourceResponse(
            api_stream_chatbot(kv_store, request),
            media_type="text/event-stream",
        )
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc


##
## Execution
##


async def api_stream_chatbot(
    kv_store: SvcKVStore,
    request: WorkspaceChatbotSpawn,
) -> AsyncIterable[dict[str, str]]:
    idx = 0
    async for delta in api_spawn_chatbot(kv_store, request):
        yield {"event": delta.event, "id": str(idx), "data": as_json(delta)}
        idx += 1
    yield {"event": "end", "id": str(idx), "data": "[DONE]"}


async def api_spawn_chatbot(
    kv_store: SvcKVStore,
    request: WorkspaceChatbotSpawn,
) -> AsyncIterable[ChatbotStream]:
    """
    TODO: Check workspace permissions.
    """
    last_partial_reply: list[BotMessagePart_] = []
    try:
        async for resp in api_send_request(kv_store, request, request.recv_timeout * 2):
            if isinstance(resp, WorkspaceResponseReply):
                last_partial_reply = resp.reply
                if resp.status == "done":
                    yield ChatbotStreamResult(
                        reply=resp.reply,
                        actions=resp.actions,
                    )
                else:
                    yield ChatbotStreamProgress(
                        summary=resp.summary,
                        partial_reply=resp.reply,
                        actions=resp.actions,
                    )
    except Exception as exc:
        error = ApiError.from_exception(exc).as_info()
        yield ChatbotStreamError(
            error=error,
            partial_reply=last_partial_reply,
            actions=[],
        )
