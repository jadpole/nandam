from typing import Literal
from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue

from base.strings.auth import ServiceId
from base.strings.process import ProcessName


ToolMode = Literal["production", "internal", "experimental", "custom"]


class ToolDefinition(BaseModel):
    # System-side configuration:
    name: ProcessName
    """
    The machine name of the tool seen by LLMs.
    """
    description: str
    """
    The LLM-optimized description of the tool.
    """
    arguments_schema: JsonSchemaValue
    """
    The JSON-Schema of the arguments, visible by the LLM and used to validate
    the arguments when the tool is invoked.
    """
    progress_schema: JsonSchemaValue | None
    return_schema: JsonSchemaValue | None

    # User-side configuration:
    human_name: str | None = None
    human_description: str | None = None
    fluent_icon_name: str | None = None
    mui_icon_name: str | None = None


class ToolInfo(BaseModel):
    # System-side configuration:
    owner: ServiceId
    name: ProcessName
    mode: ToolMode
    description: str
    arguments_schema: JsonSchemaValue
    progress_schema: JsonSchemaValue | None
    return_schema: JsonSchemaValue | None

    # User-side configuration:
    human_name: str
    human_description: str
    fluent_icon_name: str | None
    mui_icon_name: str | None

    @staticmethod
    def new(
        owner: ServiceId,
        mode: ToolMode,
        definition: ToolDefinition,
    ) -> ToolInfo:
        return ToolInfo(
            owner=owner,
            name=definition.name,
            mode=mode,
            description=definition.description,
            arguments_schema=definition.arguments_schema,
            progress_schema=definition.progress_schema,
            return_schema=definition.return_schema,
            human_name=(
                definition.human_name
                or " ".join(
                    word[0].upper() + word[1:]
                    for word in definition.name.split("_")
                    if word
                )
            ),
            human_description=(
                definition.human_description
                or " ".join(
                    word[0].upper() + word[1:]
                    for word in definition.description.split()[:50]
                    if word
                )
            ),
            fluent_icon_name=definition.fluent_icon_name,
            mui_icon_name=definition.mui_icon_name,
        )
