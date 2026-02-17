from pydantic import BaseModel
from typing import Literal

from base.core.exceptions import ApiError
from base.strings.process import ProcessName

from backend.models.process_info import ToolMode
from backend.server.context import ProcessInfo
from backend.services.tools_backend import BackendProcess


class EchoArguments(BaseModel):
    text: str


class EchoProgress(BaseModel):
    received_text: str


class EchoReturn(BaseModel):
    content: str


class Echo(BackendProcess[EchoArguments, EchoReturn]):
    kind: Literal["debug/echo"] = "debug/echo"
    name: ProcessName = ProcessName.decode("echo")
    mode: ToolMode = "internal"

    @classmethod
    def _info(cls) -> ProcessInfo:
        return ProcessInfo(
            description="Return the input text as-is to test the tools system.",
            fluent_icon_name="Megaphone",
            mui_icon_name="Campaign",
        )

    @classmethod
    def progress_types(cls) -> tuple[type[BaseModel], ...]:
        return (EchoProgress,)

    async def _run(self) -> EchoReturn:
        await self._on_progress(EchoProgress(received_text=self.arguments.text))
        if self.arguments.text.startswith("ERROR: "):
            raise ApiError(self.arguments.text.removeprefix("ERROR: "), code=400)
        return EchoReturn(content=self.arguments.text)
