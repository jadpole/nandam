from typing import Any, Literal

from base.strings.auth import ServiceId
from base.strings.process import ProcessName

from backend.models.process_info import ToolMode
from backend.server.context import NdProcess

OWNER_CUSTOM = ServiceId.decode("svc-custom")
TOOL_NAME_CUSTOM = ProcessName.decode("custom")


class ProcessCustom(NdProcess[Any, Any]):
    kind: Literal["custom"] = "custom"
    owner: ServiceId = OWNER_CUSTOM
    name: ProcessName = TOOL_NAME_CUSTOM
    mode: ToolMode = "custom"

    async def on_spawn(self) -> None:
        """
        No-op: everything is handled by the client and context.
        """
