import base64

from google import genai
from openai.types.chat import (
    ChatCompletionAssistantMessageParam,
    ChatCompletionContentPartParam,
    ChatCompletionMessageParam,
    ChatCompletionToolMessageParam,
    ChatCompletionUserMessageParam,
)
from pydantic import BaseModel, Field, PrivateAttr
from typing import Annotated, Any, Literal

from base.core.values import as_json, as_value
from base.models.content import ContentBlob, ContentText
from base.strings.auth import AgentId, ServiceId, UserId
from base.strings.process import ProcessId, ProcessName
from base.utils.completion import estimate_tokens

from backend.llm.message import (
    LlmPart,
    LlmText,
    LlmThink,
    LlmToolCall,
    LlmToolCalls,
    LlmToolResult,
)
from backend.llm.model_info import LlmModelInfo
from backend.models.exceptions import LlmError
from backend.models.process_status import ProcessFailure, ProcessSuccess
from backend.server.context import NdProcess

TOKENS_BUFFER_TOOL_CALL = 20
TOKENS_EXPIRED_TOOL_RESULT = 40


ContentMode = Literal["temp", "required", "optional"]
RenderMode = Literal["current", "history", "legacy"]


class LlmUserContent(BaseModel):
    mode: ContentMode
    content: list[str | ContentBlob]

    def count_tokens(self, mode: RenderMode) -> int:
        if _should_keep_content(self.mode, mode):
            text = "\n\n".join(c for c in self.content if isinstance(c, str))
            num_blobs = sum(1 for c in self.content if isinstance(c, ContentBlob))
            return estimate_tokens(text, num_blobs)
        else:
            return 0


class LlmBotContent(BaseModel):
    mode: ContentMode
    content: str

    def count_tokens(self, mode: RenderMode) -> int:
        return (
            estimate_tokens(self.content)
            if _should_keep_content(self.mode, mode)
            else 0
        )


class LlmHistoryUser(BaseModel):
    sender: AgentId
    role: Literal["user"] = "user"
    contents: list[LlmUserContent]

    def clean_content(self, mode: RenderMode) -> list[str | ContentBlob]:
        return [
            part
            for content in self.contents
            if _should_keep_content(content.mode, mode)
            for part in content.content
        ]

    def count_tokens(self, mode: RenderMode) -> int:
        return sum(content.count_tokens(mode) for content in self.contents)


class LlmHistoryTool(BaseModel):
    role: Literal["tool"] = "tool"
    process_id: ProcessId
    name: str
    result: dict[str, Any]
    is_error: bool

    def clean_result(self, mode: RenderMode) -> dict[str, Any]:
        if mode == "legacy" and not self.is_error:
            return {"expired": "This tool result has expired to free context."}
        else:
            return self.result

    def count_tokens(self, mode: RenderMode) -> int:
        num_tokens = estimate_tokens(as_json(self.clean_result(mode)))
        return num_tokens + TOKENS_BUFFER_TOOL_CALL


class LlmHistoryBot(BaseModel):
    role: Literal["bot"] = "bot"
    thoughts: list[LlmThink]
    contents: list[LlmBotContent]
    tool_calls: list[LlmToolCall]

    def clean_content(self, mode: RenderMode) -> str:
        return "\n\n".join(
            content.content
            for content in self.contents
            if _should_keep_content(content.mode, mode)
        )

    def count_tokens(self, mode: RenderMode) -> int:
        thought_tokens = (
            sum(estimate_tokens(think.text) for think in self.thoughts)
            if mode != "legacy"
            else 0
        )
        text_tokens = sum(content.count_tokens(mode) for content in self.contents)
        tool_tokens = sum(estimate_tokens(as_json(call)) for call in self.tool_calls)
        return thought_tokens + text_tokens + tool_tokens


LlmHistoryMessage = LlmHistoryUser | LlmHistoryTool | LlmHistoryBot
LlmHistoryMessage_ = Annotated[LlmHistoryMessage, Field(discriminator="role")]


class LlmHistoryRun(BaseModel):
    messages: list[LlmHistoryMessage_]
    num_tokens: int
    num_tokens_legacy: int


class LlmHistory(BaseModel):
    """
    The messages sent to any Completions-style API that accepts the complete
    conversation history as a parameter.

    TODO: Split messages into "history" and "current".
    """

    model_info: LlmModelInfo
    history: list[LlmHistoryRun]
    current: list[LlmHistoryMessage_]
    pending_media: list[ContentBlob]
    pending_tools: list[tuple[ProcessId, ProcessName]]
    _process: NdProcess | None = PrivateAttr(default=None)

    @staticmethod
    def new(model_info: LlmModelInfo, process: NdProcess) -> "LlmHistory":
        history = LlmHistory(
            model_info=model_info,
            history=[],
            current=[],
            pending_media=[],
            pending_tools=[],
        )
        history._process = process
        return history

    def reuse(self, model_info: LlmModelInfo, process: NdProcess) -> "LlmHistory":
        # Proprietary LLMs rely on "thought signatures" to recall reasoning, so
        # they must match.
        if (
            model_info.supports_think in ("anthropic", "gemini")
            and model_info.supports_think != self.model_info.supports_think
        ):
            raise LlmError.incompatible_model(
                self.model_info.name, model_info.name, "reasoning mismatch"
            )

        # Native tool calls and results cannot be sent to models that do not
        # support them (although we could add a one-way conversion logic).
        if not model_info.supports_tools and self.model_info.supports_tools:
            raise LlmError.incompatible_model(
                self.model_info.name, model_info.name, "native tools mismatch"
            )

        history = self.model_copy(deep=True, update={"model_info": model_info})
        history._process = process  # noqa: SLF001
        return history

    def flush_task(self) -> None:
        """
        When a new prompt is added to the conversation history, or after a given
        task was completed in long agentic workflows, flush "current" messages
        into the "history".  This allows the system to:

        1. Drop "temp" contents (extra agent scaffolding such as reminders).
        2. Drop old "optional" contents to minimize the context.

        NOTE: Although "flush_task" can be called manually, the system will
        automatically invoke it when a message is received from a user.
        """
        self.flush_pending()
        self.history.append(
            LlmHistoryRun(
                messages=self.current,
                num_tokens=sum(
                    message.count_tokens("history") for message in self.current
                ),
                num_tokens_legacy=sum(
                    message.count_tokens("legacy") for message in self.current
                ),
            )
        )
        self.current = []

    def flush_pending(self) -> None:
        """
        Before sending an LLM request, inject results for pending tools into the
        context, followed by pending media (from tool results), ensuring that:
        - The LLM has the context required to produce the next completion.
        - The tool-call structure expected by the LLM is respected.

        NOTE: Although "flush_pending" can be called manually, the system will
        automatically invoke it before an "assistant" or "user" message.

        NOTE: Gemini models natively support late tool results, and therefore,
        only flush pending tools when supports_tools == "openai".
        """
        if self.pending_tools and self.model_info.supports_tools == "openai":
            pending_result = ProcessSuccess(
                value={"content": "The tool is still running."},
            )
            for process_id, tool_name in self.pending_tools:
                early_result = LlmToolResult(
                    sender=ServiceId.decode("svc-llm-tools"),
                    process_id=process_id,
                    name=tool_name,
                    result=pending_result,
                )
                self._add_part_tool(early_result)

        if self.pending_media:
            sender = ServiceId.decode("svc-llm-tools")
            media_content = [
                "<tool-result-embeds>",
                *self.pending_media,
                "</tool-result-embeds>",
            ]
            self._add_part_user_content(sender, "optional", media_content)
            self.pending_media = []

    def _render_text_inline(self, content: ContentText) -> list[str | ContentBlob]:
        if self._process and content.dep_embeds():
            return self._process.render_content(content).as_llm_inline(
                supports_media=self.model_info.supports_media,
                limit_media=None,
            )
        else:
            return [content.as_str()]

    def _render_text_split(self, content: ContentText) -> tuple[str, list[ContentBlob]]:
        if self._process and content.dep_embeds():
            return self._process.render_content(content).as_llm_split(
                supports_media=self.model_info.supports_media,
                limit_media=None,
            )
        else:
            return content.as_str(), []

    ##
    ## Append
    ##

    def add_part(
        self,
        llm_part: LlmPart,
    ) -> None:
        if isinstance(llm_part, LlmText) and llm_part.sender:
            if isinstance(llm_part.sender, UserId):
                self.flush_task()
            else:
                self.flush_pending()
            self._add_part_user(llm_part)
        elif isinstance(llm_part, LlmToolResult):
            self._add_part_tool(llm_part)
        else:
            self.flush_pending()
            self._add_part_bot(llm_part)

    def _add_part_user(
        self,
        llm_part: LlmText,
    ) -> None:
        assert llm_part.sender

        content = llm_part.render_xml()
        if not content or not content.parts:
            return

        self._add_part_user_content(
            sender=llm_part.sender,
            mode="temp" if isinstance(llm_part.sender, ServiceId) else "required",
            content=self._render_text_inline(content),
        )

    def _add_part_user_content(
        self,
        sender: AgentId,
        mode: ContentMode,
        content: list[str | ContentBlob],
    ) -> None:
        wrapped = LlmUserContent(mode=mode, content=content)
        if (
            self.current
            and (prev_message := self.current[-1])
            and isinstance(prev_message, LlmHistoryUser)
            and prev_message.sender == sender
        ):
            prev_message.contents.append(wrapped)
        else:
            self.current.append(LlmHistoryUser(sender=sender, contents=[wrapped]))

    def _add_part_tool(self, llm_part: LlmToolResult) -> None:
        expected_tool = any(pid == llm_part.process_id for pid, _ in self.pending_tools)
        if expected_tool:
            self.pending_tools = [
                t for t in self.pending_tools if t[0] != llm_part.process_id
            ]

        if (
            self.model_info.supports_tools not in ("openai", "gemini")
            or not expected_tool
        ):
            content = llm_part.render_xml()
            self._add_part_user_content(
                sender=ServiceId.decode("svc-llm-tools"),
                mode="optional",  # TODO: Collapse representation instead.
                content=self._render_text_inline(content),
            )
            return

        is_error: bool = False
        result_value: dict[str, Any] = {}

        if isinstance(llm_part.result, ProcessSuccess):
            result_value, content = llm_part.result.as_split()
            if content:
                content_text, content_blobs = self._render_text_split(content)
                self.pending_media.extend(content_blobs)
                result_value["content"] = content_text
        else:
            is_error = True
            result_value = (
                as_value(llm_part.result.error)
                if isinstance(llm_part.result, ProcessFailure)
                else {"code": 500, "message": llm_part.result.error_message()}
            )

        self.current.append(
            LlmHistoryTool(
                process_id=llm_part.process_id,
                name=llm_part.name,
                result=result_value,
                is_error=is_error,
            )
        )

    def _add_part_bot(self, llm_part: LlmPart) -> None:
        converted_thoughts: list[LlmThink] = []
        converted_contents: list[LlmBotContent] = []
        converted_tool_calls: list[LlmToolCall] = []

        if isinstance(llm_part, LlmThink):
            if self.model_info.supports_think in ("anthropic", "gemini"):
                converted_thoughts.append(llm_part)

            elif self.model_info.supports_think in ("deepseek", "gpt-oss"):
                # GPT-OSS models directly prepend the reasoning to the content,
                # whereas DeepSeek and GLM models use `<think>` tags.
                thought_text = (
                    llm_part.text
                    if self.model_info.supports_think == "gpt-oss"
                    else f"<think>{llm_part.text}</think>"
                )
                converted_contents.append(
                    LlmBotContent(mode="optional", content=thought_text)
                )

        elif isinstance(llm_part, LlmToolCalls):
            for tool_call in llm_part.calls:
                assert tool_call.process_id, "process ID required in history"
                self.pending_tools.append((tool_call.process_id, tool_call.name))

            if self.model_info.supports_tools in ("openai", "gemini"):
                converted_tool_calls.extend(llm_part.calls)
            elif (rendered := llm_part.render_xml()) and rendered.parts:
                converted_contents.append(
                    LlmBotContent(mode="required", content=rendered.as_str())
                )

        elif (rendered := llm_part.render_xml()) and rendered.parts:
            converted_contents.append(
                LlmBotContent(mode="required", content=rendered.as_str())
            )

        self._add_part_bot_content(
            thoughts=converted_thoughts,
            contents=converted_contents,
            tool_calls=converted_tool_calls,
        )

    def _add_part_bot_content(
        self,
        thoughts: list[LlmThink],
        contents: list[LlmBotContent],
        tool_calls: list[LlmToolCall],
    ) -> None:
        if (
            self.current
            and (prev_message := self.current[-1])
            and isinstance(prev_message, LlmHistoryBot)
        ):
            prev_message.thoughts.extend(thoughts)
            prev_message.contents.extend(contents)
            prev_message.tool_calls.extend(tool_calls)
        else:
            self.current.append(
                LlmHistoryBot(
                    thoughts=thoughts,
                    contents=contents,
                    tool_calls=tool_calls,
                )
            )

    ##
    ## Render - LiteLLM
    ##

    def render_litellm(
        self,
        limit_media: int,
    ) -> list[ChatCompletionMessageParam]:
        self.flush_pending()

        converted: list[ChatCompletionMessageParam] = []
        total_tokens: int = 0

        for message in reversed(self.current):
            message_tokens = message.count_tokens("current")
            total_tokens += message_tokens
            if total_tokens > self.model_info.limit_tokens_request():
                raise LlmError.context_limit_exceeded()

            converted_message, used_media = self._render_litellm_message(
                message, "current", limit_media
            )
            converted.append(converted_message)
            limit_media -= used_media

        mode: Literal["history", "legacy"] = "history"
        for run in reversed(self.history):
            if total_tokens + run.num_tokens > self.model_info.limit_tokens_request():
                break
            if (
                mode == "history"
                and self.model_info.limit_tokens_recent
                and total_tokens + run.num_tokens > self.model_info.limit_tokens_recent
            ):
                mode = "legacy"

            total_tokens += (
                run.num_tokens_legacy if mode == "legacy" else run.num_tokens
            )

            for message in reversed(run.messages):
                converted_message, used_media = self._render_litellm_message(
                    message, mode, limit_media
                )
                converted.append(converted_message)
                limit_media -= used_media

        return list(reversed(converted))

    def _render_litellm_message(
        self,
        message: LlmHistoryMessage,
        mode: RenderMode,
        limit_media: int,
    ) -> tuple[ChatCompletionMessageParam, int]:
        if isinstance(message, LlmHistoryUser):
            return self._render_litellm_user(message, mode, limit_media)
        elif isinstance(message, LlmHistoryTool):
            return self._render_litellm_tool(message, mode), 0
        else:
            return self._render_litellm_bot(message, mode), 0

    def _render_litellm_user(
        self,
        message: LlmHistoryUser,
        mode: RenderMode,
        limit_media: int,
    ) -> tuple[ChatCompletionUserMessageParam, int]:
        used_media: int = 0
        partial_text: str = ""
        converted: list[ChatCompletionContentPartParam] = []

        for part in message.clean_content(mode):
            if isinstance(part, ContentBlob):
                if (
                    part.mime_type in self.model_info.supports_media
                    and used_media < limit_media
                ):
                    # NOTE: Wrap `<blob>`, so the LLM can use tools on the blob.
                    partial_text = f'{partial_text.rstrip()}\n<blob uri="{part.uri}">'
                    converted.append({"type": "text", "text": partial_text})
                    converted.append(
                        {"type": "image_url", "image_url": {"url": part.download_url()}}
                    )
                    partial_text = "</blob>\n"
                    used_media += 1
                else:
                    placeholder = ContentText.new(part.render_placeholder()).as_str()
                    if partial_text:
                        partial_text = f"{partial_text.rstrip()}\n\n"
                    partial_text += f"{placeholder}\n\n"
            else:
                partial_text += part

        # TODO: "name": str(message.sender) ?
        if used_media:
            if partial_text:
                converted.append({"type": "text", "text": partial_text.rstrip()})
            return {"role": "user", "content": converted}, used_media
        else:
            assert not converted
            return {"role": "user", "content": partial_text.rstrip()}, 0

    def _render_litellm_tool(
        self,
        message: LlmHistoryTool,
        mode: RenderMode,
    ) -> ChatCompletionToolMessageParam:
        assert self.model_info.supports_tools == "openai"
        return {
            "role": "tool",
            "tool_call_id": message.process_id.as_native_openai(),
            "content": (
                as_json({"error": message.result})
                if message.is_error
                else as_json(message.clean_result(mode))
            ),
        }

    def _render_litellm_bot(
        self,
        message: LlmHistoryBot,
        mode: RenderMode,
    ) -> ChatCompletionAssistantMessageParam:
        converted: ChatCompletionAssistantMessageParam = {"role": "assistant"}

        if message.thoughts and self.model_info.supports_think == "anthropic":
            assert len(message.thoughts) == 1
            thought = message.thoughts[0]

            if self.model_info.supports_think == "anthropic":
                thinking_block = {"type": "thinking"}
                if thought.text:
                    thinking_block["thinking"] = thought.text
                if thought.signature:
                    thinking_block["signature"] = thought.signature
                converted["thinking_blocks"] = [thinking_block]  # type: ignore

        if message.contents:
            converted["content"] = message.clean_content(mode)

        if message.tool_calls:
            assert self.model_info.supports_tools == "openai"
            converted["tool_calls"] = [
                {
                    "type": "function",
                    "id": tool_call.process_id.as_native_openai(),
                    "function": {
                        "name": tool_call.name,
                        "arguments": as_json(tool_call.arguments),
                    },
                }
                for tool_call in message.tool_calls
                if tool_call.process_id
            ]

        return converted

    ##
    ## Render - Gemini
    ##

    def render_gemini(
        self,
        limit_media: int,
    ) -> list[genai.types.Content]:
        self.flush_pending()

        converted: list[genai.types.Content] = []
        total_tokens: int = 0

        for message in reversed(self.current):
            message_tokens = message.count_tokens("current")
            total_tokens += message_tokens
            if total_tokens > self.model_info.limit_tokens_request():
                raise LlmError.context_limit_exceeded()

            converted_message, used_media = self._render_gemini_message(
                message, "current", limit_media
            )
            limit_media -= used_media

            if converted and converted_message.role == converted[-1].role:
                assert converted_message.parts is not None
                assert converted[-1].parts is not None
                converted[-1].parts = [
                    *converted_message.parts,
                    *converted[-1].parts,
                ]
            else:
                converted.append(converted_message)

        mode: Literal["history", "legacy"] = "history"
        for run in reversed(self.history):
            if total_tokens + run.num_tokens > self.model_info.limit_tokens_request():
                break
            if (
                mode == "history"
                and self.model_info.limit_tokens_recent
                and total_tokens + run.num_tokens > self.model_info.limit_tokens_recent
            ):
                mode = "legacy"

            total_tokens += (
                run.num_tokens_legacy if mode == "legacy" else run.num_tokens
            )

            for message in reversed(run.messages):
                converted_message, used_media = self._render_gemini_message(
                    message, mode, limit_media
                )
                limit_media -= used_media
                if converted and converted_message.role == converted[-1].role:
                    assert converted_message.parts is not None
                    assert converted[-1].parts is not None
                    converted[-1].parts = [
                        *converted_message.parts,
                        *converted[-1].parts,
                    ]
                else:
                    converted.append(converted_message)

        return list(reversed(converted))

    def _render_gemini_message(
        self,
        message: LlmHistoryMessage,
        mode: RenderMode,
        limit_media: int,
    ) -> tuple[genai.types.Content, int]:
        if isinstance(message, LlmHistoryUser):
            rendered, used_media = self._render_gemini_user(message, mode, limit_media)
            return genai.types.Content(role="user", parts=rendered), used_media

        elif isinstance(message, LlmHistoryTool):
            rendered = self._render_gemini_tool(message, mode)
            return genai.types.Content(role="user", parts=rendered), 0

        else:
            rendered = self._render_gemini_bot(message, mode)
            return genai.types.Content(role="model", parts=rendered), 0

    def _render_gemini_user(
        self,
        message: LlmHistoryUser,
        mode: RenderMode,
        limit_media: int,
    ) -> tuple[list[genai.types.Part], int]:
        used_media: int = 0
        partial_text: str = ""
        converted: list[genai.types.Part] = []

        for part in message.clean_content(mode):
            if isinstance(part, ContentBlob):
                if (
                    part.mime_type in self.model_info.supports_media
                    and used_media < limit_media
                    and (blob_bytes := part.as_bytes())
                ):
                    # NOTE: Wrap `<blob>`, so the LLM can use tools on the blob.
                    partial_text = f'{partial_text.rstrip()}\n<blob uri="{part.uri}">'
                    converted.append(genai.types.Part(text=partial_text))
                    converted.append(
                        genai.types.Part(
                            inline_data=genai.types.Blob(
                                mime_type=part.mime_type,
                                data=blob_bytes,
                            )
                        )
                    )
                    partial_text = "</blob>\n"
                    used_media += 1
                else:
                    placeholder = ContentText.new(part.render_placeholder()).as_str()
                    if partial_text:
                        partial_text = f"{partial_text.rstrip()}\n\n"
                    partial_text += f"{placeholder}\n\n"
            else:
                partial_text += part

        if used_media:
            if partial_text:
                converted.append(genai.types.Part(text=partial_text.rstrip()))
            return converted, used_media
        else:
            assert not converted
            return [genai.types.Part(text=partial_text.rstrip())], 0

    def _render_gemini_tool(
        self,
        message: LlmHistoryTool,
        mode: RenderMode,
    ) -> list[genai.types.Part]:
        assert self.model_info.supports_tools == "gemini"
        return [
            genai.types.Part(
                function_response=genai.types.FunctionResponse(
                    id=message.process_id.as_native_gemini(),
                    name=str(message.name),
                    response=(
                        {"error": message.result}
                        if message.is_error
                        else {"output": message.clean_result(mode)}
                    ),
                )
            )
        ]

    def _render_gemini_bot(
        self,
        message: LlmHistoryBot,
        mode: RenderMode,
    ) -> list[genai.types.Part]:
        thought_signature: bytes | None = None
        converted: list[genai.types.Part] = []

        for thought in message.thoughts:
            if thought.signature and self.model_info.supports_think == "gemini":
                thought_signature = base64.b64decode(thought.signature)

        if content := message.clean_content(mode):
            converted.append(
                genai.types.Part(
                    text=content,
                    thought_signature=thought_signature,
                )
            )
            thought_signature = None

        for tool_call in message.tool_calls:
            converted.append(
                genai.types.Part(
                    function_call=genai.types.FunctionCall(
                        id=(
                            tool_call.process_id.as_native_gemini()
                            if tool_call.process_id
                            else None
                        ),
                        name=str(tool_call.name),
                        args=tool_call.arguments,
                    ),
                    thought_signature=thought_signature,
                )
            )
            thought_signature = None

        return converted


def _should_keep_content(mode: ContentMode, render_mode: RenderMode) -> bool:
    return (
        mode == "required"
        or (mode == "optional" and render_mode != "legacy")
        or (mode == "temp" and render_mode == "current")
    )
