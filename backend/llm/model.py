import asyncio
import cerebras.cloud.sdk
import json
import logging
import openai

from collections.abc import Callable, Coroutine
from google.genai.errors import APIError as GeminiAPIError
from pydantic import BaseModel, Field, TypeAdapter
from pydantic.json_schema import JsonSchemaValue
from typing import Any, Literal, NotRequired, TypedDict, Unpack

from base.core.exceptions import StoppedError
from base.core.schema import as_jsonschema
from base.models.content import ContentBlob, ContentText
from base.strings.data import MimeType
from base.strings.process import ProcessId, ProcessName
from base.utils.completion import estimate_tokens, split_xml
from base.utils.markdown import lstrip_newlines, strip_keep_indent

from backend.config import BackendConfig
from backend.llm.message import (
    LlmInvalid,
    LlmPart,
    LlmText,
    LlmThink,
    LlmTool,
    LlmToolCall,
    LlmToolCalls,
    system_instructions_tools_xml,
)
from backend.llm.model_info import (
    LlmModelInfo,
    LlmModelStatus,
    LlmThinkMode,
    LlmToolsMode,
)
from backend.models.exceptions import LlmError
from backend.server.context import NdProcess

logger = logging.getLogger(__name__)

RETRY_DELAY_SECS = [2, 30, 60] if BackendConfig.is_kubernetes() else [30]


##
## Request
##


LlmCallback = Callable[[list[LlmPart]], Coroutine[Any, Any, Any]]
LlmSessionMode = Literal["disabled", "done", "pending"]


class LlmModelArgs[S](TypedDict):
    process: NdProcess
    callback: NotRequired[LlmCallback | None]
    state: NotRequired[S | None]
    system: str | None
    messages: list[LlmPart]
    max_tokens: NotRequired[int | None]
    response_schema: NotRequired[JsonSchemaValue | None]
    stop: NotRequired[list[str] | None]
    temperature: NotRequired[float | None]
    tools: NotRequired[list[LlmTool] | None]
    tool_choice: NotRequired[ProcessName | None]
    xml_hallucinations: NotRequired[list[str] | None]
    xml_sections: NotRequired[list[type[LlmPart]] | None]


class LlmModelArgsJson[S](TypedDict):
    process: NdProcess
    callback: NotRequired[LlmCallback | None]
    state: NotRequired[S | None]
    system: str | None
    messages: list[LlmPart]
    max_tokens: NotRequired[int | None]
    response_schema: NotRequired[JsonSchemaValue | None]
    temperature: NotRequired[float | None]


##
## Implementation
##


class LlmInvalidToolCall(BaseModel):
    id: str | None
    name: str
    arguments: str
    error: str

    def as_invalid(self) -> LlmInvalid:
        call_json = '{"name": "' + self.name + '", "arguments": ' + self.arguments + "}"
        return LlmInvalid(
            error=self.error,
            completion=f"<tool-calls>\n{call_json}\n</tool-calls>",
        )


class LlmPartialToolCall(BaseModel):
    # TODO: mode: Literal["function", "custom"]
    id: str | None
    name: str
    arguments: str

    def build(self, final: bool) -> list[LlmToolCall]:
        # TODO:
        # if self.mode == "custom":
        #     return [
        #         LlmToolCall(
        #             process_id=ProcessId.from_native(self.id),
        #             name=ProcessName.decode(self.name),
        #             arguments={"content": self.arguments},
        #         )
        #     ]

        arguments = json.loads(self.arguments)

        # Some OpenAI models (e.g., GPT-4o) hallucinate a "parallel" tool
        # when calling more than one function.  The function name is then
        # stored as `recipient_name = "functions.{function_name}"`.
        if self.name == "multi_tool_use.parallel":
            return [
                LlmToolCall(
                    process_id=ProcessId.generate() if final else None,
                    name=ProcessName.decode(tool_name),
                    arguments=tool_use["parameters"],
                )
                for tool_use in arguments["tool_uses"]
                if (tool_name := tool_use["recipient_name"].rsplit(".", maxsplit=1)[-1])
            ]

        # Note that we do not stream tool calls to the user, since they
        # will see the `Tool.invocation_notif` very soon.
        process_id = None
        if self.id:
            process_id = ProcessId.from_native(self.id)
        elif final:
            process_id = ProcessId.generate()
        return [
            LlmToolCall(
                process_id=process_id,
                name=ProcessName.decode(self.name),
                arguments=arguments,
            )
        ]


class LlmNativeCompletion(BaseModel):
    answer: str
    thoughts: list[LlmThink]
    tool_calls: list[LlmToolCall] = Field(default_factory=list)
    tool_errors: list[LlmInvalidToolCall] = Field(default_factory=list)

    @staticmethod
    def parse(
        answer: str,
        thoughts: list[LlmThink],
        tool_calls: list[LlmPartialToolCall],
        *,
        final: bool,
        supports_think: LlmThinkMode | None,
    ) -> "LlmNativeCompletion":
        # Convert gpt-oss reasoning when incorrectly parsed by TogetherAI.
        if supports_think == "gpt-oss":
            if answer.startswith("<|channel|>analysis<|message|>"):
                if "<|end|>" in answer:
                    answer = answer.removeprefix("<|channel|>analysis<|message|>")
                    thinking_text, answer = answer.split("<|end|>", 1)
                    thoughts = [*thoughts, LlmThink(text=thinking_text, signature=None)]
                    answer = answer.removeprefix(
                        "<|start|>assistant<|channel|>final<|message|>"
                    )
                    answer = answer.removeprefix("<|start|>")
                    answer = answer.removeprefix("<|call|>")
                else:
                    answer = ""
            elif answer.startswith("analysis") and "assistantfinal" in answer:
                answer = answer.removeprefix("analysis")
                thinking_text, answer = answer.split("assistantfinal", 1)
                thoughts = [*thoughts, LlmThink(text=thinking_text, signature=None)]

        # Some models, notably DeepSeek and GLM, include their thinking in the
        # content part of their completion.  Extract it.
        if supports_think == "deepseek" and answer.startswith("<think>"):
            thinking_text, answer = (
                answer.split("</think>", maxsplit=1)
                if "</think>" in answer
                else (answer, "")
            )
            answer = lstrip_newlines(answer)
            thinking_text = strip_keep_indent(thinking_text.removeprefix("<think>"))
            thoughts = [*thoughts, LlmThink(text=thinking_text, signature=None)]

        valid_tool_calls: list[LlmToolCall] = []
        invalid_tool_calls: list[LlmInvalidToolCall] = []

        for partial_call in tool_calls:
            try:
                valid_tool_calls.extend(partial_call.build(final))
            except Exception as exc:
                if not final:
                    continue
                invalid_tool_calls.append(
                    LlmInvalidToolCall(
                        id=partial_call.id,
                        name=partial_call.name,
                        arguments=partial_call.arguments,
                        error=str(exc),
                    )
                )

        return LlmNativeCompletion(
            answer=answer,
            thoughts=thoughts,
            tool_calls=valid_tool_calls,
            tool_errors=invalid_tool_calls,
        )

    def render_debug(self) -> str:
        # TODO: Do we want to log the parsed completion in "blue"?
        # completion_log = "\n".join(
        #     part.render_debug()
        #     for part in self._parse_completion(
        #         native_completion, xml_sections, xml_hallucinations
        #     )
        # )
        parts = [p.render_debug() for p in self.thoughts]
        if self.answer:
            parts.append(self.answer)
        if self.tool_calls:
            parts.append(LlmToolCalls(calls=self.tool_calls).render_debug())
        return "\n\n".join(parts)


##
## Model
##


class LlmModel[P, S: BaseModel, U](BaseModel):
    # Interface
    name: str
    status: LlmModelStatus
    description: str
    color: str

    # Model
    native_name: str
    knowledge_cutoff: str | None = None
    supports_media: list[MimeType] = Field(default_factory=list)
    supports_stop: bool = False
    supports_stream: bool = True
    supports_think: LlmThinkMode | None = None
    supports_tools: LlmToolsMode | None = None

    # Limits
    limit_tokens_total: int
    limit_tokens_response: int
    limit_tokens_recent: int | None = None
    limit_media: int = 0

    # Settings
    reasoning_effort: Literal["high", "medium", "low"] | None = None

    def info(self) -> LlmModelInfo:
        return LlmModelInfo(
            name=self.name,
            status=self.status,
            description=self.description,
            color=self.color,
            native_name=self.native_name,
            knowledge_cutoff=self.knowledge_cutoff,
            supports_media=self.supports_media,
            supports_think=self.supports_think,
            supports_tools=self.supports_tools,
            limit_tokens_total=self.limit_tokens_total,
            limit_tokens_response=self.limit_tokens_response,
            limit_tokens_recent=self.limit_tokens_recent,
            limit_media=self.limit_media,
        )

    @classmethod
    def state_type(cls) -> type[BaseModel]:
        raise NotImplementedError("Subclasses must implement LlmModel.state_type")

    ##
    ## Implementation
    ##

    def _convert_system(
        self,
        process: NdProcess,
        system: str | None,
        tools: list[LlmTool],
        xml_sections: list[type[LlmPart]],
    ) -> str | None:
        system_parts: list[str] = []
        if system:
            system_parts.append(system)

        # When the LLM does not support tool calls natively, but the request
        # demands it, provide an XML-based alternative.
        if not self.supports_tools and tools:
            system_parts.append(system_instructions_tools_xml(tools))

        system_parts.extend(
            xml_system
            for xml_section in xml_sections
            if (xml_system := xml_section.get_system_instructions(process))
        )

        return "\n".join(system_parts) or None

    def _count_blob_tokens(self, blob: ContentBlob) -> int:
        return estimate_tokens("", 1)

    def _count_text_tokens(self, text: ContentText | str) -> int:
        return estimate_tokens(text.as_str() if isinstance(text, ContentText) else text)

    def _get_completion_params(self, **kwargs: Unpack[LlmModelArgs[S]]) -> P:
        raise NotImplementedError(
            "Subclasses must implement LlmModel._get_completion_params"
        )

    async def _get_completion_result(
        self,
        callback: LlmCallback | None,
        params: P,
        xml_sections: list[type[LlmPart]],
        xml_hallucinations: list[str],
    ) -> tuple[LlmNativeCompletion, U]:
        raise NotImplementedError(
            "Subclasses must implement LlmModel._get_completion_result"
        )

    async def _update_state(
        self,
        request: LlmModelArgs[S],
        update: U,
        completion: LlmNativeCompletion,
        parsed: list[LlmPart] | None,
    ) -> S:
        raise NotImplementedError("Subclasses must implement LlmModel._update_status")

    ##
    ## Completion
    ##

    async def get_completion(
        self,
        **kwargs: Unpack[LlmModelArgs[S]],
    ) -> tuple[list[LlmPart], S]:
        completion, update = await self._get_completion_with_retry(**kwargs)
        parsed = self._parse_completion(
            completion,
            kwargs.get("xml_sections"),
            kwargs.get("xml_hallucinations"),
        )
        new_state = await self._update_state(kwargs, update, completion, parsed)
        return parsed, new_state

    async def get_completion_json[T: BaseModel](
        self,
        type_: type[T],
        **kwargs: Unpack[LlmModelArgsJson[S]],
    ) -> tuple[T, S]:
        # Inject the expected JSON-Schema into the request, unless the caller
        # has provided one (typically, equivalent, with better descriptions).
        if not kwargs.get("response_schema"):
            kwargs = kwargs.copy()
            kwargs["response_schema"] = as_jsonschema(type_)

        completion, update = await self._get_completion_with_retry(**kwargs)  # type: ignore
        answer = strip_keep_indent(completion.answer)
        if not answer:
            raise LlmError.empty_completion()

        # Claude-Sonnet 4.5 in "stream" mode uses an `index` to differentiate
        # between the "reply" (human-readable) and the JSON object.
        if "\n\n" in answer:
            answer = answer.rsplit("\n\n", 1)[1]

        try:
            parsed = TypeAdapter(type_).validate_json(answer)
        except Exception as exc:
            raise LlmError.bad_completion(f"invalid JSON: {exc}", answer)  # noqa: B904

        request: LlmModelArgs[S] = kwargs  # type: ignore
        new_state = await self._update_state(request, update, completion, None)
        return parsed, new_state

    async def get_completion_native(
        self,
        **kwargs: Unpack[LlmModelArgs[S]],
    ) -> tuple[LlmNativeCompletion, S]:
        completion, update = await self._get_completion_with_retry(**kwargs)
        new_state = await self._update_state(kwargs, update, completion, None)
        return completion, new_state

    async def get_completion_text(
        self,
        **kwargs: Unpack[LlmModelArgs[S]],
    ) -> tuple[str, S]:
        completion, update = await self._get_completion_with_retry(**kwargs)
        answer = strip_keep_indent(completion.answer)
        if not answer:
            raise LlmError.empty_completion()

        new_state = await self._update_state(kwargs, update, completion, [])
        return answer, new_state

    async def _get_completion_with_retry(
        self,
        **kwargs: Unpack[LlmModelArgs[S]],
    ) -> tuple[LlmNativeCompletion, U]:
        # NOTE: Since the conversion might be CPU-intensive on long histories,
        # 1. Run it in a thread to avoid blocking the main event loop;
        # 2. Only run the conversion once, reusing it on network errors.
        params = await asyncio.to_thread(self._get_completion_params, **kwargs)

        network_errors = 0
        while True:
            try:
                return await self._get_completion_result(
                    kwargs.get("callback"),
                    params,
                    kwargs.get("xml_sections") or [],
                    kwargs.get("xml_hallucinations") or [],
                )
            except StoppedError:
                raise
            except Exception as exc:
                # TODO: Also retry on timeout, socket closed.
                # Retry on rate limit, but raise other errors immediately.
                if network_errors < len(RETRY_DELAY_SECS) and (
                    isinstance(
                        exc, cerebras.cloud.sdk.RateLimitError | openai.RateLimitError
                    )
                    or (
                        isinstance(exc, GeminiAPIError)
                        and exc.code == 429  # noqa: PLR2004
                    )
                    or "overloaded" in str(exc).lower()
                ):
                    if BackendConfig.verbose:
                        logger.warning("Retrying after LLM error: %s", str(exc))
                    else:
                        logger.warning("Retrying after LLM error")

                    await asyncio.sleep(RETRY_DELAY_SECS[network_errors])
                    network_errors += 1
                else:
                    raise LlmError.network_error(exc) from exc

    def _parse_completion(
        self,
        completion: LlmNativeCompletion,
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> list[LlmPart]:
        parts: list[LlmPart] = []

        if completion.thoughts:
            parts.extend(completion.thoughts)

        if completion.answer:
            parts.extend(
                self._parse_answer(completion.answer, xml_sections, xml_hallucinations)
            )

        if completion.tool_calls:
            parts.append(LlmToolCalls(calls=completion.tool_calls))

        if completion.tool_errors:
            parts.extend(call.as_invalid() for call in completion.tool_errors)

        return parts

    def _parse_answer(
        self,
        completion: str,
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> list[LlmPart]:
        xml_hallucinations = xml_hallucinations or []
        xml_sections = xml_sections.copy() if xml_sections else []
        if LlmText not in xml_sections:
            xml_sections.append(LlmText)
        if not self.supports_tools and LlmToolCalls not in xml_sections:
            xml_sections.append(LlmToolCalls)

        if len(xml_sections) > 1:
            xml_tags = tuple(s.tag() for s in xml_sections)
            xml_parts = split_xml(completion, xml_tags, LlmText.tag())
        else:
            xml_parts = [(LlmText.tag(), completion)]

        result: list[LlmPart] = []
        for part_tag, part_text in xml_parts:
            part_type = next((s for s in xml_sections if part_tag == s.tag()), LlmText)
            try:
                if part_parsed := part_type.parse_body(part_text):
                    result.append(part_parsed)
            except Exception as exc:
                part_completion = (
                    f"<{part_tag}>{part_text}</{part_tag}>"
                    if part_tag != LlmText.tag()
                    else part_text
                )
                result.append(LlmInvalid(error=str(exc), completion=part_completion))

        return result
