from pydantic import BaseModel
from typing import Literal


class ActionResponse(BaseModel, frozen=True):
    success: Literal[True]
