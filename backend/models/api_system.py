from pydantic import BaseModel
from typing import Literal

from base.core.exceptions import ErrorInfo


class ActionResponse(BaseModel, frozen=True):
    success: Literal[True]


class ResponseStreamError(BaseModel, frozen=True):
    event: Literal["error"] = "error"
    error: ErrorInfo
