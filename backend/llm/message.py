import yaml

from pydantic import BaseModel
from pydantic.json_schema import JsonSchemaValue
from typing import Any, Literal, Self

from backend.llm.model_info import LlmModelInfo
from base.core.unions import ModelUnion
from base.core.values import as_json, as_yaml
from base.models.content import ContentText, PartText
from base.strings.auth import AgentId, ServiceId
from base.strings.process import ProcessId, ProcessName
from base.utils.markdown import strip_keep_indent
from base.utils.sorted_list import bisect_insert

from backend.models.process_status import ProcessResult, ProcessResult_, ProcessSuccess
from backend.server.context import NdProcess


##
## Tools
##


class LlmTool(BaseModel):
    name: str
    description: str
    arguments_schema: JsonSchemaValue

    def as_openai(self) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.arguments_schema,
            },
        }


class LlmToolCall(BaseModel):
    process_id: ProcessId | None
    name: ProcessName
    arguments: dict[str, Any]

    @staticmethod
    def stub(
        tool_name: str,
        tool_call_id: str,
        tool_arguments: dict[str, Any],
    ) -> "LlmToolCall":
        return LlmToolCall(
            process_id=ProcessId.stub(tool_call_id),
            name=ProcessName.decode(tool_name),
            arguments=tool_arguments,
        )

    @staticmethod
    def from_list(tool_calls: Any) -> "list[LlmToolCall]":
        # When the LLM forgets to wrap a tool call in a list, do so.
        if isinstance(tool_calls, dict):
            tool_calls = [tool_calls]
        if not isinstance(tool_calls, list) or not all(
            isinstance(call, dict) for call in tool_calls
        ):
            raise ValueError("bad tool-calls: expected list of {name, arguments}")

        return [LlmToolCall.from_dict(tool_call) for tool_call in tool_calls]

    @staticmethod
    def from_dict(tool_call: dict[str, Any]) -> "LlmToolCall":
        if not (tool_name := tool_call.get("name")):
            raise ValueError("bad tool-calls: missing name")
        if not (tool_arguments := tool_call.get("arguments")):
            raise ValueError(f"bad '{tool_name}' tool call: missing arguments")
        if extra_keys := set(tool_call.keys()) - {"name", "arguments"}:
            extra_str = ", ".join(sorted(extra_keys))
            raise ValueError(f"bad '{tool_name}' tool call: extra keys: {extra_str}")
        if not (process_name := ProcessName.try_decode(tool_name)):
            raise ValueError(f"bad '{tool_name}' tool call: invalid name")
        return LlmToolCall(
            process_id=None,
            name=process_name,
            arguments=tool_arguments,
        )


##
## History Parts
##


class LlmPart(ModelUnion, frozen=True):
    @classmethod
    def tag(cls) -> str:
        return cls.model_fields["kind"].default

    @classmethod
    def get_system_instructions(cls, process: NdProcess) -> str | None:
        return None

    @classmethod
    def parse_body(cls, value: str) -> Self | None:
        return None

    def render_xml(self) -> ContentText | None:
        raise NotImplementedError("Subclasses must implement LlmPart.render_xml")

    def render_client(self) -> ContentText | None:
        return None

    def render_debug(self) -> str:
        return "<unknown />"

    def as_str(self) -> str:
        return rendered.as_str() if (rendered := self.render_xml()) else ""


class LlmInvalid(LlmPart, frozen=True):
    kind: Literal["invalid"] = "invalid"
    error: str
    completion: str

    def render_xml(self) -> ContentText:
        return ContentText.new_plain(self.completion, "\n")

    def render_error(self) -> ContentText:
        return ContentText.new_plain(
            f"<error>\n{self.error}\n</error>\n"
            f"<completion>\n{self.completion}\n</completion>",
            "\n",
        )

    def render_debug(self) -> str:
        return f"<invalid>\n{self.render_error().as_str()}\n</invalid>"


class LlmText(LlmPart, frozen=True):
    kind: Literal["text"] = "text"
    sender: AgentId | None
    content: ContentText

    @classmethod
    def prompt(cls, sender: AgentId, value: str) -> "LlmText":
        return LlmText(
            sender=sender,
            content=ContentText.parse(value, default_link="markdown"),
        )

    @classmethod
    def parse_body(cls, value: str) -> "LlmText":
        return LlmText(
            sender=None,
            content=ContentText.parse(value, default_link="markdown"),
        )

    def render_xml(self) -> ContentText:
        return self.content

    def render_client(self) -> ContentText:
        return self.content

    def render_debug(self) -> str:
        return f'<text sender="{self.sender}">\n{self.render_xml().as_str()}\n</text>'


class LlmThink(LlmPart, frozen=True):
    kind: Literal["think"] = "think"
    text: str
    signature: str | None = None

    @staticmethod
    def stub(text: str | None = None) -> "LlmThink":
        return LlmThink(
            text=text or "some thought",
            signature="iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC",
        )

    @classmethod
    def parse_body(cls, value: str) -> "LlmThink":
        return LlmThink(text=strip_keep_indent(value), signature=None)

    def render_xml(self) -> ContentText | None:
        return None  # Hidden unless supported natively.

    def render_client(self) -> ContentText | None:
        return None

    def render_debug(self) -> str:
        return f'<think signature="{bool(self.signature)}">\n{self.text}\n</think>'


class LlmToolCalls(LlmPart, frozen=True):
    kind: Literal["tool-calls"] = "tool-calls"
    calls: list[LlmToolCall]

    @classmethod
    def parse_body(cls, value: str) -> "LlmToolCalls":
        try:
            tools_dict = yaml.safe_load(strip_keep_indent(value))
            return LlmToolCalls(calls=LlmToolCall.from_list(tools_dict))
        except Exception:
            raise ValueError("bad tool-calls: malformed YAML")  # noqa: B904

    def render_xml(self) -> ContentText:
        tools_value: list[dict[str, Any]] = [
            {"name": str(tool_call.name), "arguments": tool_call.arguments}
            for tool_call in self.calls
        ]
        # TODO: Useful when representing user actions (e.g., in a UI) as tool calls.
        # if self.sender:
        #     tools_value = [{"from_user": str(self.sender), **t} for t in tools_value]

        tools_xml = f"<tool-calls>\n{as_yaml(tools_value)}\n</tool-calls>"
        return ContentText.parse(tools_xml, mode="data")

    def render_debug(self) -> str:
        return self.render_xml().as_str()


class LlmToolResult(LlmPart, frozen=True):
    kind: Literal["tool-result"] = "tool-result"
    sender: ServiceId | None
    process_id: ProcessId
    name: ProcessName
    result: ProcessResult_

    @staticmethod
    def stub(
        tool_name: str,
        tool_call_id: str,
        tool_result: ProcessResult | dict[str, Any],
    ) -> "LlmToolResult":
        return LlmToolResult(
            sender=ServiceId.stub("tool"),
            process_id=ProcessId.stub(tool_call_id),
            name=ProcessName.decode(tool_name),
            result=(
                tool_result
                if isinstance(tool_result, ProcessResult)
                else ProcessSuccess(value=tool_result)
            ),
        )

    def render_xml(self) -> ContentText:
        result = self.result.as_text()
        return ContentText.new(
            [
                PartText.new("<tool-result>", "\n"),
                PartText.new(f"<name>{self.name}</name>", "\n"),
                *result.parts,
                PartText.new("</tool-result>", "\n"),
            ]
        )

    def render_debug(self) -> str:
        return self.render_xml().as_str()


##
## Semantics
##


def system_instructions(
    info: LlmModelInfo,
    *,
    mermaid: bool = True,
    tags: list[type[LlmPart]] | list[str] | None = None,
    tips: bool = True,
    tools: bool = True,
) -> str:
    tags = tags or []
    supports_tags = sorted(
        tag
        for p in tags
        if (tag := p if isinstance(p, str) else p.tag())
        and tag not in ("text", "invalid", "think", "tool-calls")
    )
    if info.supports_think == "deepseek":
        bisect_insert(supports_tags, "think", key=lambda t: t)
    if tools and not info.supports_tools:
        bisect_insert(supports_tags, "tool-calls", key=lambda t: t)

    system: list[str] = []
    system.append(system_instructions_response(supports_tags))
    system.append(system_instructions_resources(info.knowledge_cutoff))
    if mermaid:
        system.append(system_instructions_mermaid())

    if tips:
        system.append(
            """\
<tips>
When necessary, Nandam should call tools before sending its final answer. \
The user sees neither tool calls nor tool results. \
Nandam's final answer should therefore be self-contained and give the user the \
necessary context and citations. \
</tips>\
"""
        )

    return "\n".join(system)


def system_instructions_response(tags: list[str]) -> str:
    instructions = """\
<response_info>
Nandam responds to the user in beautiful Markdown, using the minimum formatting \
appropriate to make the response clear and readable. It avoids over-formatting, \
e.g., with elements like bold emphasis, headers, lists, and bullet points. \

It provides thorough responses to more complex and open-ended questions or to \
anything where a long response is requested, but concise responses to simpler \
questions and tasks. \
All else being equal, it tries to give the most correct and concise answer it \
can to the user's message and offers to elaborate if further information may \
be helpful.
When answering technical questions, Nandam provides motivating examples. \
When brainstorming, Nandam tries to improve on the user's ideas and critiques \
them constructively.

Nandam wraps code in code blocks using triple-backticks and does not explain \
or break down the code unless the user requests it.

Everything is interpreted as literal in `code expressions` and \

```
code blocks
```\
"""

    if literal_escapes := ", ".join(
        esc for tag in tags for esc in (f"`<{tag}>`", f"`</{tag}>`")
    ):
        instructions += f"\n\nThe following strings MUST be escaped when meant literally: {literal_escapes}"

    instructions += "\n</response_info>"
    return instructions


def system_instructions_resources(knowledge_cutoff: str | None) -> str:
    instructions = """\
All resources, affordances, and observations are uniquely identified by a \
Knowledge URI. A resource URI has the form "ndk://RESOURCE-PATH".

Resources have "affordances", methods of interacting with it, that tools may \
require, of the form "ndk://RESOURCE-PATH/$AFFORDANCE". For example,

- The "$body" affordance means that its content can be consulted.
- The "$collection" affordance means that it is some sort of "folder" or "tag" \
that can be browsed to find other resources.
- The "$file" affordance means that it can be downloaded, e.g., for analysis \
or manipulation using code.
- The "$plain" affordance means that the file can be read as text, copied to \
an artifact and potentially edited, e.g., code, YAML configurations, or the \
raw HTML/Markdown that was parsed into "$body".

Some affordances may be further dissected into "observations" of the form \
"ndk://RESOURCE-PATH/$AFFORDANCE/$OBSERVATION". \
For example, the "$body" of a large document might be split into "chunks" and \
"media" items that can be consulted individually.

Nandam can use Knowledge URIs to:

- Call tools that take resources as arguments \
(with their schema indicating which kinds of URIs are supported).
- Cite its sources using the [^$CITED_URI] syntax. \
Example (one source): "The answer is x[^ndk://cited/resource/path]."
Example (many sources): "The answer is x[^ndk://first/cited/resource][^ndk://second/cited/resource]."
- Create descriptive links: \
Example (email with subject): "[Re: Project Update](ndk://linked/email/path)". \
Example (document with name): "[Annual Report 2024.pdf](ndk://linked/doc/path)". \
Example (no metadata): "<ndk://linked/resource/path>".

Important: Nandam MUST cite every resource used to produce the response \
EXACTLY ONCE, following the most salient claim.

Reminder: These URIs should NEVER be escaped in backticks or code blocks, \
unless you intend to display them as-is.
"""

    if knowledge_cutoff:
        instructions += f"""
Nandam's knowledge base was last updated on {knowledge_cutoff}.
It answers questions about events prior to and after {knowledge_cutoff} the way \
a highly informed individual in {knowledge_cutoff} would if they were talking \
to someone from the above date, and can let the human know this when relevant.
"""

    instructions += """
When a response depends on internal company information or on facts beyond its \
knowledge cutoff, Nandam should find the required information proactively by \
consulting external resources, then provide relevant citations. \
Nandam uses the available resources and tools to find the relevant information \
before answering instead of making assumptions.\
"""
    return f"<resources_info>\n{instructions}\n</resources_info>"


def system_instructions_mermaid() -> str:
    return """\
<mermaid_info>
Nandam can draw Mermaid diagrams by using the syntax:

```mermaid
$DIAGRAM_CODE
```

The following diagram types are supported in $DIAGRAM_CODE: \
flowchart, sequenceDiagram, classDiagram, stateDiagram-v2, erDiagram, journey, \
gantt, pie, quadrantChart, requirementDiagram, gitGraph, zenuml, xychart, \
block, packet, kanban.
</mermaid_info>\
"""


def system_instructions_tools_xml(tools: list[LlmTool]) -> str:
    """
    When the LLM does not support tool calls natively, but the request provides
    a list of tools, this provides an XML-based alternative that will be parsed
    into `LlmToolCalls`.
    """
    if not tools:
        return ""

    tool_names = ", ".join(tool.name for tool in tools)
    tool_definitions = "\n".join(
        line
        for tool in tools
        for line in [
            "<tool>",
            f"<name>{tool.name}</name>",
            f"<description>{tool.description}</description>",
            f"<arguments-jsonschema>{as_json(tool.arguments_schema)}</arguments-jsonschema>",
            "</tool>",
        ]
    )
    return f"""\
<tools_info>
Nandam can use tools by ending its response with a `<tool-calls>` block that \
contains one or more tool calls:

<tool-calls>
- name: $TOOL_NAME
  arguments:
    $TOOL_ARGS
...
</tool-calls>

Valid $TOOL_NAME values: {tool_names}
$TOOL_ARGS is the tool arguments as YAML, respecting `<arguments-jsonschema>`.

Tool results are provided in a sequence of `<tool-result>` blocks:

<tool-result>
<name>$TOOL_NAME</name>
$RESULT
</tool-result>

For example:

<tool-calls>
- name: web_search
  arguments:
    question: "Who is the current president of France?"
- name: ask_docs
  arguments:
    question: |-
      Multiline string arguments...
      ... are easier to format with a block scalar.
    sources:
      - $variable_1
      - $variable_2
</tool-calls>

Will be answered, in the next "user message", by:

<tool-result>
<name>web_search</name>
...
</tool-result>
<tool-result>
<name>ask_docs</name>
...
</tool-result>
</tools_info>
<available_tools>
{tool_definitions}
</available_tools>\
"""
