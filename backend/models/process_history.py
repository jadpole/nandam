from datetime import datetime, UTC
from pydantic import BaseModel, Field
from typing import Annotated, Any, Literal

from base.strings.process import ProcessId, ProcessName

from backend.models.process_result import ProcessResult_


##
## History - Actions
##


class ToolInvokeAction(BaseModel, frozen=True):
    kind: Literal["action/tool"] = "action/tool"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    process_id: ProcessId = Field(default_factory=ProcessId.generate)
    name: ProcessName
    arguments: dict[str, Any]


# TODO:
# class SendMessageAction(BaseModel, frozen=True):
#     kind: Literal["action/message"] = "action/message"
#     timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
#     sender: BotId
#     message_id: ThreadMessageId
#     content: list[BotMessagePart_]
#     provisional: bool = False


AnyProcessAction = ToolInvokeAction
AnyProcessAction_ = Annotated[AnyProcessAction, Field(discriminator="kind")]


##
## History - Events
##


class RestartEvent(BaseModel, frozen=True):
    kind: Literal["event/restart"] = "event/restart"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SigkillEvent(BaseModel, frozen=True):
    kind: Literal["event/sigkill"] = "event/sigkill"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SigtermEvent(BaseModel, frozen=True):
    kind: Literal["event/sigterm"] = "event/sigterm"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ToolResultEvent(BaseModel, frozen=True):
    kind: Literal["event/tool"] = "event/tool"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    process_id: ProcessId
    name: ProcessName
    result: ProcessResult_[Any]


AnyProcessEvent = RestartEvent | SigkillEvent | SigtermEvent | ToolResultEvent
AnyProcessEvent_ = Annotated[AnyProcessEvent, Field(discriminator="kind")]


##
## History
##


class ProcessProgress(BaseModel, frozen=True):
    kind: Literal["progress"] = "progress"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    progress: dict[str, Any]


ProcessHistoryItem = AnyProcessAction | AnyProcessEvent | ProcessProgress
ProcessHistoryItem_ = Annotated[ProcessHistoryItem, Field(discriminator="kind")]
