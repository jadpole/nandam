from pydantic import BaseModel, Field
from typing import Annotated, Literal

from base.strings.process import ProcessName
from base.strings.resource import ResourceUri

from backend.server.context import NdTool


##
## Persona - Chat
##


class CapabilityTools(BaseModel, frozen=True):
    kind: Literal["tools"] = "tools"
    action: Literal["disable", "enable"]
    tools: list[ProcessName]


AnyBotCapability = CapabilityTools
AnyBotCapability_ = Annotated[AnyBotCapability, Field(discriminator="kind")]


class ChatPersona(BaseModel, frozen=True):
    """
    Chatbot persona that runs using the Completions API.
    """

    agent: Literal["chat"] = "chat"
    system_message: str
    model: str
    temperature: float
    resources: list[ResourceUri]
    capabilities: list[AnyBotCapability_]

    def filter_tool(self, tool: NdTool) -> bool:
        enabled = tool.default_enabled
        for capability in self.capabilities:
            if (
                isinstance(capability, CapabilityTools)
                and tool.name in capability.tools
            ):
                enabled = capability.action == "enable"
        return enabled

    def filter_tools(self, tools: list[NdTool]) -> list[NdTool]:
        return [tool for tool in tools if self.filter_tool(tool)]


AnyPersona = ChatPersona
AnyPersona_ = Annotated[AnyPersona, Field(discriminator="agent")]


# TODO:
# class Persona(BaseModel, frozen=True):
#     name: str
#     description: str
#     config: AnyPersona_
