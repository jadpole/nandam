import contextlib
from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any

from base.models.context import NdService
from base.strings.auth import ServiceId

from backend.models.process_result import ProcessFailure, ProcessSuccess
from backend.server.context import NdProcess, NdTool, ToolsProvider, WorkspaceContext

SVC_BACKEND_TOOLS = ServiceId.decode("svc-backend-tools")


class BackendProcess[Arg: BaseModel = Any, Ret: BaseModel = Any](NdProcess[Arg, Ret]):
    owner: ServiceId = SVC_BACKEND_TOOLS

    async def on_spawn(self) -> None:
        with contextlib.suppress(Exception):
            await self._run_with_scaffolding()

    async def _run_with_scaffolding(self) -> Ret:
        """
        Utility method to invoke the tool with the given (typed) arguments and
        await the (typed) result, which can be used in workflows.
        """
        try:
            return_value = await self._run()
            self.result = ProcessSuccess(value=return_value)
            await self.on_save()
            return return_value
        except Exception as exc:
            self.result = ProcessFailure.from_exception(exc)
            await self.on_save()
            raise

    async def _run(self) -> Ret:
        raise NotImplementedError("Subclasses must implement NdProcess._run")


@dataclass(kw_only=True)
class SvcBackendTools(NdService, ToolsProvider):
    service_id: ServiceId = SVC_BACKEND_TOOLS

    @staticmethod
    def process_types() -> list[type[NdProcess]]:
        # fmt: off
        from backend.tools.debug.echo import Echo  # noqa: PLC0415
        return [
            # Debug:
            Echo,
        ]

    def list_tools(self, parent: NdProcess | WorkspaceContext) -> list[NdTool]:
        return [
            tool
            for process_type in self.process_types()
            if (tool := process_type.tool(context=parent))
        ]
