import anthropic

from dataclasses import dataclass
from pydantic import BaseModel, PrivateAttr
from termcolor import colored
from typing import Any, Unpack

from base.core.values import as_json

from backend.config import BackendConfig
from backend.llm.history import LlmHistory
from backend.llm.message import LlmPart, LlmThink
from backend.llm.model import (
    LlmCallback,
    LlmModel,
    LlmModelArgs,
    LlmNativeCompletion,
    LlmPartialToolCall,
)
from backend.models.exceptions import LlmError

MODELS_USING_DEVELOPER = ("gpt-", "o1", "o3", "o4", "together_ai/openai/")
"""
These models should use the new "developer" role for system messages, rather
than the legacy "system" role.
"""
REQUEST_TIMEOUT = 300  # 5 minutes
STREAM_TOKEN_THRESHOLD = 40
"""
When streaming, instead of sending updates after each token, wait until at least
this many tokens have been generated, unless `force_send` is set, indicating the
end of a section (thinking block, tool call, etc.)
"""


@dataclass(kw_only=True)
class LlmAnthropicParams:
    params: Any
    partial_history: LlmHistory
    new_messages: list[LlmPart]


class LlmAnthropicState(BaseModel):
    history: LlmHistory

    def append_part(self, part: LlmPart) -> None:
        pass


@dataclass(kw_only=True)
class LlmAnthropicUpdate:
    new_history: LlmHistory


def init_anthropic_client() -> anthropic.AsyncAnthropic:
    if BackendConfig.llm.anthropic_api_key:
        return anthropic.AsyncAnthropic(
            api_key=BackendConfig.llm.anthropic_api_key,
        )
    elif BackendConfig.llm.router_api_base and BackendConfig.llm.router_api_key:
        return anthropic.AsyncAnthropic(
            api_key=BackendConfig.llm.router_api_key,
            base_url=f"{BackendConfig.llm.router_api_base}/anthropic",
        )
    else:
        raise LlmError("LlmAnthropic requires LLM_ANTHROPIC_API_KEY")


class LlmAnthropic(LlmModel[LlmAnthropicParams, LlmAnthropicState, LlmAnthropicUpdate]):
    _client: anthropic.AsyncAnthropic = PrivateAttr(
        default_factory=init_anthropic_client
    )

    @classmethod
    def state_type(cls) -> type[LlmAnthropicState]:
        return LlmAnthropicState

    def _get_completion_params(  # noqa: C901
        self,
        **kwargs: Unpack[LlmModelArgs[LlmAnthropicState]],
    ) -> LlmAnthropicParams:
        assert self.supports_tools in (None, "openai")
        assert self.supports_think in (None, "anthropic")

        # Convert the messages into the Anthropic format.
        model_info = self.info()
        history = (
            state.history.reuse(model_info, kwargs["process"])
            if (state := kwargs.get("state"))
            else LlmHistory.new(model_info, kwargs["process"])
        )
        for part in kwargs["messages"]:
            history.add_part(part)

        messages: list[anthropic.types.MessageParam] = history.render_anthropic(
            limit_media=self.limit_media
        )

        extra_headers = kwargs["process"].llm_headers()
        params: dict[str, Any] = {
            "max_tokens": self.limit_tokens_response,
            "messages": messages,
            "metadata": anthropic.types.MetadataParam(
                user_id=extra_headers.get("x-georges-user-id"),
            ),
            "model": self.native_name,
            "output_config": {},
            "extra_body": {},
            "extra_headers": {} if BackendConfig.llm.gemini_api_key else extra_headers,
            "timeout": REQUEST_TIMEOUT,
        }

        if system_text := self._convert_system(
            process=kwargs["process"],
            system=kwargs.get("system"),
            tools=kwargs.get("tools") or [],
            xml_sections=kwargs.get("xml_sections") or [],
        ):
            params["system"] = system_text

        # Parameters that should be Omit when false.
        if self.supports_stream and kwargs.get("callback"):
            params["stream"] = True
        if (value_stop := kwargs.get("stop")) and self.supports_stop:
            params["stop_sequences"] = value_stop

        # Reasoning limits.
        # TODO: Support "xhigh" reasoning effort?
        if self.supports_think == "anthropic":
            match self.reasoning_effort:
                case "high":
                    reasoning_tokens = 48_000
                case "medium":
                    reasoning_tokens = 24_000
                case _:
                    reasoning_tokens = 12_000
            params["thinking"] = {
                "type": "enabled",
                "budget_tokens": reasoning_tokens,
            }
            # TODO: Forward reasoning effort to LLM?
            # if self.native_name == "claude-opus-4-6":
            #     params["output_config"]["effort"] = self.reasoning_effort

        if (value := kwargs.get("temperature")) is not None and not self.supports_think:
            params["temperature"] = value

        # Native tools.
        if self.supports_tools == "openai" and (tools := kwargs.get("tools")):
            tool_choice = kwargs.get("tool_choice") or "auto"
            params["tools"] = [tool.as_anthropic() for tool in tools]
            params["tool_choice"] = (
                {"type": tool_choice}
                if tool_choice in ("auto", "none")
                else {
                    "type": "tool",
                    "name": tool_choice,
                    "disable_parallel_tool_use": True,
                }
            )

        # Response format.
        if response_schema := kwargs.get("response_schema"):
            params["output_config"]["format"] = {
                "type": "json_schema",
                "schema": anthropic.transform_schema(response_schema),
            }

        return LlmAnthropicParams(
            params=params,
            partial_history=history,
            new_messages=kwargs["messages"],
        )

    async def _update_state(
        self,
        request: LlmModelArgs[LlmAnthropicState],
        update: LlmAnthropicUpdate,
        completion: LlmNativeCompletion,
        parsed: list[LlmPart] | None,
    ) -> LlmAnthropicState:
        if parsed is None:
            parsed = self._parse_completion(
                completion,
                request.get("xml_sections"),
                request.get("xml_hallucinations"),
            )

        for part in parsed:
            update.new_history.add_part(part)

        return LlmAnthropicState(history=update.new_history)

    ##
    ##  Completion
    ##

    async def _get_completion_result(
        self,
        callback: LlmCallback | None,
        params: LlmAnthropicParams,
        xml_sections: list[type[LlmPart]],
        xml_hallucinations: list[str],
    ) -> tuple[LlmNativeCompletion, LlmAnthropicUpdate]:
        if BackendConfig.verbose >= 4:  # noqa: PLR2004
            prompt_log = "\n".join(as_json(m) for m in params.params["messages"])
            print(colored(f"<messages>\n{prompt_log}\n</messages>", "blue"))
        elif BackendConfig.verbose >= 3:  # noqa: PLR2004
            prompt_log = "\n".join(m.render_debug() for m in params.new_messages)
            print(colored(f"<messages>\n{prompt_log}\n</messages>", "blue"))

        completion = await self._client.messages.create(**params.params)
        if params.params.get("stream"):
            result = await self._consume_completion_stream(
                callback, completion, xml_sections, xml_hallucinations
            )
        else:
            result = await self._consume_completion_batch(
                callback, completion, xml_sections, xml_hallucinations
            )

        return result, LlmAnthropicUpdate(new_history=params.partial_history)

    async def _consume_completion_batch(
        self,
        callback: LlmCallback | None,
        completion: anthropic.types.Message,
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> LlmNativeCompletion:
        answer: str = ""
        tool_calls: list[LlmPartialToolCall] = []
        thinking: LlmThink | None = None

        for content in completion.content:
            # TODO: "server_tool_use", "web_search_tool_result".
            if content.type == "thinking":
                thinking = LlmThink(
                    text=content.thinking,
                    signature=content.signature,
                )
            elif content.type == "redacted_thinking":
                thinking = LlmThink(text="", signature=content.data)
            elif content.type == "text":
                # TODO: Handle native citations.
                answer += content.text
            elif content.type == "tool_use":
                tool_calls.append(
                    LlmPartialToolCall(
                        id=content.id,
                        name=content.name,
                        arguments=as_json(content.input),
                    )
                )

        native_completion = LlmNativeCompletion.parse(
            answer=answer,
            thoughts=[thinking] if thinking else [],
            tool_calls=tool_calls,
            final=True,
            supports_think=self.supports_think,
        )

        if BackendConfig.verbose >= 2:  # noqa: PLR2004
            print(colored(f"\n{native_completion.render_debug()}\n", "green"))

        if callback:
            await callback(
                self._parse_completion(
                    native_completion, xml_sections, xml_hallucinations
                )
            )

        return native_completion

    async def _consume_completion_stream(  # noqa: C901, PLR0912, PLR0915
        self,
        callback: LlmCallback | None,
        completion: anthropic.AsyncStream[anthropic.types.RawMessageStreamEvent],
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> LlmNativeCompletion:
        if BackendConfig.verbose >= 2:  # noqa: PLR2004
            print()

        answer: list[str] = []
        thinking_signature: str | None = None
        thinking_text: list[str] = []
        tool_calls: list[LlmPartialToolCall] = []

        chunk_text: str = ""
        prev_block: str | None = None

        async for chunk in completion:  # type: ignore
            force_send: bool = False

            if chunk.type == "content_block_start":
                # Close blocks:
                if prev_block == "thinking":
                    force_send = True
                    chunk_text += "\n</think>\n\n"
                elif prev_block in ("text", "tool_use"):
                    chunk_text += "\n"

                # Open blocks:
                if chunk.content_block.type == "thinking":
                    prev_block = "thinking"
                    chunk_text += "<think>\n"
                    thinking_text.append("")
                    if chunk.content_block.signature:
                        thinking_signature = chunk.content_block.signature
                    if chunk.content_block.thinking:
                        thinking_text[-1] += chunk.content_block.thinking

                elif (
                    chunk.content_block.type == "redacted_thinking"
                    and chunk.content_block.data
                ):
                    prev_block = "redacted_thinking"
                    thinking_signature = chunk.content_block.data
                    chunk_text += "<think>\nREDACTED\n</think>\n"

                elif chunk.content_block.type == "text":
                    prev_block = "text"
                    answer.append(chunk.content_block.text)
                    chunk_text += chunk.content_block.text

                elif chunk.content_block.type == "tool_use":
                    if prev_block != "tool_use":
                        chunk_text += "<tool-calls>"
                    prev_block = "tool_use"

                    arguments_json = (
                        as_json(chunk.content_block.input)
                        if chunk.content_block.input
                        else ""
                    )
                    tool_calls.append(
                        LlmPartialToolCall(
                            id=chunk.content_block.id,
                            name=chunk.content_block.name,
                            arguments=arguments_json,
                        )
                    )
                    chunk_text += f"\n- name: {chunk.content_block.name}\n  arguments: "
                    if arguments_json:
                        chunk_text += arguments_json

            # Append delta:
            if chunk.type == "content_block_delta":
                if chunk.delta.type == "thinking_delta":
                    thinking_text[-1] += chunk.delta.thinking
                    chunk_text += chunk.delta.thinking
                elif chunk.delta.type == "signature_delta":
                    thinking_signature = chunk.delta.signature
                elif chunk.delta.type == "text_delta":
                    answer[-1] += chunk.delta.text
                    chunk_text += chunk.delta.text
                elif chunk.delta.type == "input_json_delta":
                    tool_calls[-1].arguments += chunk.delta.partial_json

            if force_send or len(chunk_text) >= STREAM_TOKEN_THRESHOLD:
                if BackendConfig.verbose >= 2 and chunk_text:  # noqa: PLR2004
                    print(colored(chunk_text, "green"), end="")
                chunk_text = ""

                if callback:
                    native_completion = LlmNativeCompletion.parse(
                        answer="\n\n".join(answer),
                        thoughts=(
                            [
                                LlmThink(
                                    text="\n\n".join(thinking_text),
                                    signature=thinking_signature,
                                )
                            ]
                            if thinking_text or thinking_signature
                            else []
                        ),
                        tool_calls=tool_calls,
                        final=False,
                        supports_think=self.supports_think,
                    )
                    await callback(
                        self._parse_completion(
                            native_completion, xml_sections, xml_hallucinations
                        )
                    )

        if tool_calls:
            chunk_text += "\n</tool-calls>"
        if BackendConfig.verbose >= 2:  # noqa: PLR2004
            print(colored(f"{chunk_text}\n", "green"))

        native_completion = LlmNativeCompletion.parse(
            answer="\n\n".join(answer),
            thoughts=(
                [
                    LlmThink(
                        text="\n\n".join(thinking_text),
                        signature=thinking_signature,
                    )
                ]
                if thinking_text or thinking_signature
                else []
            ),
            tool_calls=tool_calls,
            final=True,
            supports_think=self.supports_think,
        )

        if callback:
            await callback(
                self._parse_completion(
                    native_completion, xml_sections, xml_hallucinations
                )
            )

        return native_completion


def _safe_openai_message(content: str) -> str:
    """
    We "escape" special tokens to prevent an exception.
    Side effect: messages are not exactly the same as the original.
    """
    return content.replace("<|endoftext|>", "<||endoftext||>")
