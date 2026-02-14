from pydantic import BaseModel, Field
from typing import Annotated, Any, Literal

from base.strings.process import ProcessName, ProcessUri

from backend.models.process_result import ProcessResult_


##
## History - Actions
##


class ProcessSpawnAction(BaseModel, frozen=True):
    kind: Literal["action/process_spawn"] = "action/process_spawn"
    process_uri: ProcessUri
    name: ProcessName
    arguments: dict[str, Any]


AnyProcessAction = ProcessSpawnAction
AnyProcessAction_ = Annotated[AnyProcessAction, Field(discriminator="kind")]


##
## History - Events
##


class ProcessResultEvent(BaseModel, frozen=True):
    kind: Literal["event/process_result"] = "event/process_result"
    process_uri: ProcessUri
    name: ProcessName
    result: ProcessResult_


AnyProcessEvent = ProcessResultEvent
AnyProcessEvent_ = Annotated[AnyProcessEvent, Field(discriminator="kind")]


##
## History
##


class ProcessProgress(BaseModel, frozen=True):
    kind: Literal["progress"] = "progress"
    progress: dict[str, Any]


ProcessHistoryItem = AnyProcessAction | AnyProcessEvent | ProcessProgress
ProcessHistoryItem_ = Annotated[ProcessHistoryItem, Field(discriminator="kind")]
