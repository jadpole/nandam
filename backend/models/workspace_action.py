from datetime import UTC, datetime
from pydantic import Field
from typing import Annotated, Any, Literal

from backend.models.api_client import ClientAction
from backend.models.process_info import ToolDefinition
from backend.models.workspace_thread import BotMessagePart_
from base.core.exceptions import ErrorInfo
from base.core.unions import ModelUnion
from base.strings.auth import BotId
from base.strings.process import ProcessId, ProcessName, ProcessUri
from base.strings.scope import Workspace
from base.strings.thread import ThreadUri

from backend.models.bot_persona import AnyPersona_
from backend.models.process_history import (
    AnyProcessAction_,
    AnyProcessEvent_,
    ProcessHistoryItem_,
)
from backend.models.process_result import ProcessResult_
from backend.server.context import RequestConfig


##
## Request Message
##


class WorkspaceRequest(ModelUnion, frozen=True):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def get_workspace(self) -> Workspace:
        raise NotImplementedError(
            "Subclasses must implement WorkspaceBaseAction.get_workspace"
        )


class WorkspaceChatbotSpawn(WorkspaceRequest, frozen=True):
    kind: Literal["chatbot/spawn"] = "chatbot/spawn"
    workspace: Workspace
    request: RequestConfig
    bot_id: BotId
    persona: AnyPersona_ | None
    threads: list[ThreadUri]
    tools: list[ToolDefinition] = Field(default_factory=list)
    recv_timeout: int

    def get_workspace(self) -> Workspace:
        return self.workspace


# TODO:
# class WorkspaceProcessCustom(WorkspaceRequest, frozen=True):
#     kind: Literal["process/custom"] = "process/custom"
#     request: RequestConfig
#     process_uri: ProcessUri
#     name: ProcessName
#     arguments: dict[str, Any]
#
#     def get_workspace(self) -> Workspace:
#         return self.process_uri.workspace


class WorkspaceProcessSigkill(WorkspaceRequest, frozen=True):
    kind: Literal["process/sigkill"] = "process/sigkill"
    process_uri: ProcessUri

    def get_workspace(self) -> Workspace:
        return self.process_uri.workspace


# TODO: Remote and require using a custom context?
class WorkspaceProcessSpawn(WorkspaceRequest, frozen=True):
    kind: Literal["process/spawn"] = "process/spawn"
    workspace: Workspace
    request: RequestConfig
    process_id: ProcessId
    name: ProcessName
    arguments: dict[str, Any]

    def get_workspace(self) -> Workspace:
        return self.workspace


class WorkspaceProcessUpdate(WorkspaceRequest, frozen=True):
    kind: Literal["process/update"] = "process/update"
    process_uri: ProcessUri
    actions: list[AnyProcessAction_] = Field(default_factory=list)
    progress: list[dict[str, Any]] = Field(default_factory=list)
    result: ProcessResult_[Any] | None = None

    def get_workspace(self) -> Workspace:
        return self.process_uri.workspace


AnyWorkspaceRequest = (  # TODO: WorkspaceProcessCustom
    WorkspaceChatbotSpawn
    | WorkspaceProcessSigkill
    | WorkspaceProcessSpawn
    | WorkspaceProcessUpdate
)
AnyWorkspaceRequest_ = Annotated[AnyWorkspaceRequest, Field(discriminator="kind")]


##
## Response Stream Message
##


class WorkspaceResponse(ModelUnion, frozen=True):
    pass


class WorkspaceResponseReply(WorkspaceResponse, frozen=True):
    kind: Literal["reply"] = "reply"
    status: Literal["done", "provisional"]
    summary: str | None
    reply: list[BotMessagePart_]
    actions: list[ClientAction]


class WorkspaceResponseProgress(WorkspaceResponse, frozen=True):
    kind: Literal["progress"] = "progress"
    process_uri: ProcessUri
    events: list[AnyProcessEvent_] = Field(default_factory=list)
    history: list[ProcessHistoryItem_] = Field(default_factory=list)
    result: ProcessResult_[Any] | None = None


AnyWorkspaceResponse = WorkspaceResponseProgress | WorkspaceResponseReply
AnyWorkspaceResponse_ = Annotated[AnyWorkspaceResponse, Field(discriminator="kind")]


##
## Response Stream
##


class WorkspaceStreamClose(WorkspaceResponse, frozen=True):
    kind: Literal["close"] = "close"


class WorkspaceStreamError(WorkspaceResponse, frozen=True):
    kind: Literal["error"] = "error"
    error: ErrorInfo


class WorkspaceStreamValue(WorkspaceResponse, frozen=True):
    kind: Literal["value"] = "value"
    value: AnyWorkspaceResponse_


AnyWorkspaceStream = WorkspaceStreamClose | WorkspaceStreamError | WorkspaceStreamValue
AnyWorkspaceStream_ = Annotated[AnyWorkspaceStream, Field(discriminator="kind")]
