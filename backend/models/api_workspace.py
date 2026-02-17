from datetime import datetime
from pydantic import BaseModel
from typing import Any

from base.strings.process import ProcessName, ProcessUri

from backend.models.process_history import ProcessHistoryItem_
from backend.models.process_result import ProcessResult_


class ProcessStatus(BaseModel, frozen=True):
    process_uri: ProcessUri
    name: ProcessName
    created_at: datetime
    updated_at: datetime
    history: list[ProcessHistoryItem_]
    result: ProcessResult_[Any] | None
