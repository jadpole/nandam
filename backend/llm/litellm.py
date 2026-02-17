import openai

from dataclasses import dataclass
from openai.types.chat import (
    ChatCompletion,
    ChatCompletionChunk,
    ChatCompletionMessageFunctionToolCall,
    ChatCompletionMessageParam,
)
from pydantic import BaseModel, PrivateAttr
from termcolor import colored
from typing import Any, Unpack

from base.core.schema import clean_jsonschema
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
class LlmLiteParams:
    params: Any
    partial_history: LlmHistory
    new_messages: list[LlmPart]


class LlmLiteState(BaseModel):
    history: LlmHistory | None = None

    def append_part(self, part: LlmPart) -> None:
        pass


@dataclass(kw_only=True)
class LlmLiteUpdate:
    new_history: LlmHistory


def init_litellm_client() -> openai.AsyncClient:
    if BackendConfig.llm.router_api_base and BackendConfig.llm.router_api_key:
        return openai.AsyncClient(
            api_key=BackendConfig.llm.router_api_key,
            base_url=f"{BackendConfig.llm.router_api_base}/v1",
        )
    else:
        raise LlmError("LlmLite requires LLM_ROUTER_API_*")


class LlmLite(LlmModel[LlmLiteParams, LlmLiteState, LlmLiteUpdate]):
    _client: openai.AsyncClient = PrivateAttr(default_factory=init_litellm_client)

    @classmethod
    def state_type(cls) -> type[LlmLiteState]:
        return LlmLiteState

    def _get_completion_params(  # noqa: C901, PLR0912
        self,
        **kwargs: Unpack[LlmModelArgs[LlmLiteState]],
    ) -> LlmLiteParams:
        assert self.supports_tools in (None, "openai")
        assert self.supports_think in (None, "anthropic", "deepseek", "hidden")

        # Convert the messages into the LiteLLM format.
        model_info = self.info()
        history = (
            state.history.reuse(model_info)
            if (state := kwargs.get("state")) and state.history
            else LlmHistory.new(model_info)
        )
        for part in kwargs["messages"]:
            history.add_part(part)

        messages = history.render_litellm(limit_media=self.limit_media)
        if system_text := self._convert_system(
            process=kwargs["process"],
            system=kwargs.get("system"),
            tools=kwargs.get("tools") or [],
            xml_sections=kwargs.get("xml_sections") or [],
        ):
            system_text = _safe_openai_message(system_text)
            system_message: ChatCompletionMessageParam
            if self.native_name.startswith(MODELS_USING_DEVELOPER):
                system_message = {"role": "developer", "content": system_text}
            else:
                system_message = {"role": "system", "content": system_text}

            messages.insert(0, system_message)

        params: dict[str, Any] = {
            "extra_body": {},
            "extra_headers": kwargs["process"].llm_headers(),
            "model": self.native_name,
            "messages": messages,
            "timeout": REQUEST_TIMEOUT,
        }

        # Parameters that should be NOT_GIVEN when false.
        if self.supports_stream and kwargs.get("callback"):
            params["stream"] = True
        if (value_stop := kwargs.get("stop")) and self.supports_stop:
            params["stop"] = value_stop

        if (value := kwargs.get("max_tokens")) and not self.supports_think:
            params["max_tokens"] = value
        elif self.supports_think == "anthropic":
            params["max_tokens"] = self.limit_tokens_response

        if (value := kwargs.get("temperature")) is not None and not self.supports_think:
            params["temperature"] = value

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
            params["extra_body"]["thinking"] = {
                "type": "enabled",
                "budget_tokens": reasoning_tokens,
            }
        elif self.supports_think == "hidden":
            params["reasoning_effort"] = self.reasoning_effort or "minimal"

        # Native tools.
        if self.supports_tools == "openai" and (tools := kwargs.get("tools")):
            tool_choice = kwargs.get("tool_choice") or "auto"
            params["tools"] = [tool.as_litellm() for tool in tools]
            params["tool_choice"] = (
                tool_choice
                if tool_choice in ("auto", "none")
                else {"type": "function", "function": {"name": tool_choice}}
            )

        # Response format.
        if response_schema := kwargs.get("response_schema"):
            params["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "answer_schema",
                    "strict": True,
                    "schema": clean_jsonschema(response_schema),
                },
            }

        return LlmLiteParams(
            params=params,
            partial_history=history,
            new_messages=kwargs["messages"],
        )

    async def _update_state(
        self,
        request: LlmModelArgs[LlmLiteState],
        update: LlmLiteUpdate,
        completion: LlmNativeCompletion,
        parsed: list[LlmPart] | None,
    ) -> LlmLiteState:
        if parsed is None:
            parsed = self._parse_completion(
                completion,
                request.get("xml_sections"),
                request.get("xml_hallucinations"),
            )

        for part in parsed:
            update.new_history.add_part(part)

        return LlmLiteState(history=update.new_history)

    ##
    ##  Completion
    ##

    async def _get_completion_result(
        self,
        callback: LlmCallback | None,
        params: LlmLiteParams,
        xml_sections: list[type[LlmPart]],
        xml_hallucinations: list[str],
    ) -> tuple[LlmNativeCompletion, LlmLiteUpdate]:
        if BackendConfig.verbose >= 4:  # noqa: PLR2004
            prompt_log = "\n".join(as_json(m) for m in params.params["messages"])
            print(colored(f"<messages>\n{prompt_log}\n</messages>", "blue"))
        elif BackendConfig.verbose >= 3:  # noqa: PLR2004
            prompt_log = "\n".join(m.render_debug() for m in params.new_messages)
            print(colored(f"<messages>\n{prompt_log}\n</messages>", "blue"))

        completion = await self._client.chat.completions.create(**params.params)
        if params.params.get("stream"):
            result = await self._consume_completion_stream(
                callback, completion, xml_sections, xml_hallucinations
            )
        else:
            result = await self._consume_completion_batch(
                callback, completion, xml_sections, xml_hallucinations
            )

        return result, LlmLiteUpdate(new_history=params.partial_history)

    async def _consume_completion_batch(
        self,
        callback: LlmCallback | None,
        completion: ChatCompletion,
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> LlmNativeCompletion:
        answer: str = ""
        tool_calls: list[LlmPartialToolCall] = []
        thinking: LlmThink | None = None

        response = completion.choices[0].message

        if hasattr(response, "content") and (chunk_content := response.content):
            answer = chunk_content

        # Native "anthropic" thinking.
        if hasattr(response, "thinking_blocks") and (
            thinking_blocks := response.thinking_blocks  # type: ignore
        ):
            if len(thinking_blocks) > 1:
                raise LlmError.bad_completion("multiple thinking blocks", None)

            thinking = LlmThink(
                text=thinking_blocks[0]["thinking"],
                signature=thinking_blocks[0].get("signature"),
            )

        # Native "openai" tool calls.
        if hasattr(response, "tool_calls") and response.tool_calls:
            for tool_call in response.tool_calls:
                if isinstance(tool_call, ChatCompletionMessageFunctionToolCall):
                    tool_calls.append(  # noqa: PERF401
                        LlmPartialToolCall(
                            id=tool_call.id,
                            name=tool_call.function.name,
                            arguments=tool_call.function.arguments,
                        )
                    )
                # TODO:
                # elif isinstance(tool_call, ChatCompletionMessageCustomToolCall):
                #     tool_calls.append(
                #         LlmPartialToolCall(
                #             mode="custom",
                #             id=tool_call.id,
                #             name=tool_call.custom.name,
                #             arguments=tool_call.custom.input,
                #         )
                #     )

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
            callback(
                self._parse_completion(
                    native_completion, xml_sections, xml_hallucinations
                )
            )

        return native_completion

    async def _consume_completion_stream(  # noqa: C901, PLR0912, PLR0915
        self,
        callback: LlmCallback | None,
        completion: openai.AsyncStream[ChatCompletionChunk],
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> LlmNativeCompletion:
        if BackendConfig.verbose >= 2:  # noqa: PLR2004
            print()

        answer: str = ""
        thinking_signature: str | None = None
        thinking_text: str = ""
        tool_calls: list[LlmPartialToolCall] = []

        chunk_text: str = ""
        answer_index: int = 1

        async for chunk in completion:  # type: ignore
            force_send: bool = False

            # Anthropic: thinking blocks.
            if (
                chunk.choices
                and chunk.choices[0].delta
                and hasattr(chunk.choices[0].delta, "thinking_blocks")
                and (thinking := chunk.choices[0].delta.thinking_blocks)  # type: ignore
            ):
                if len(thinking) > 1:
                    raise LlmError.bad_completion("multiple thinking blocks", None)
                if not thinking_text:
                    chunk_text += "<think>\n"
                if chunk_thinking := thinking[0].get("thinking"):
                    chunk_text += chunk_thinking
                    thinking_text += chunk_thinking
                if chunk_signature := thinking[0].get("signature"):
                    thinking_signature = chunk_signature

            # Stream answer.
            if (
                chunk.choices
                and chunk.choices[0].delta
                and hasattr(chunk.choices[0].delta, "content")
                and (chunk_content := chunk.choices[0].delta.content)
            ):
                if thinking_text and not answer:
                    chunk_text += "\n</think>\n\n"
                    force_send = True

                # When generating a JSON response, the LLM will sometimes put a
                # reply at index=1, then the JSON object at index=2.  Insert two
                # newlines as a separator for different indexes.
                if not answer and chunk.choices[0].index:
                    answer_index = chunk.choices[0].index
                elif chunk.choices[0].index and chunk.choices[0].index > answer_index:
                    answer_index = chunk.choices[0].index
                    answer += "\n\n"
                answer += chunk_content
                chunk_text += chunk_content

            # Perplexity: the full message generated thus far is sent in each chunk,
            # rather than only the last generated token(s).
            elif hasattr(chunk, "message") and (
                chunk_message := chunk.message["content"]  # type: ignore
            ):
                if chunk_message.startswith(answer):
                    chunk_text += chunk_message[len(answer) :]
                else:
                    chunk_text += " ... "  # Show progress without repeating chunks.
                answer = chunk_message

            # Native "openai" tool calls.
            if (
                chunk.choices
                and chunk.choices[0].delta
                and hasattr(chunk.choices[0].delta, "tool_calls")
                and (chunk_calls := chunk.choices[0].delta.tool_calls)
            ):
                if not tool_calls:
                    chunk_text += "\n<tool-calls>"

                for chunk_call in chunk_calls:
                    if chunk_call.id:
                        if tool_calls:
                            force_send = True
                        if chunk_call.function and chunk_call.function.name:
                            # TODO: Support 'custom tools' when streaming.
                            tool_calls.append(
                                LlmPartialToolCall(
                                    id=chunk_call.id,
                                    name=chunk_call.function.name,
                                    arguments="",
                                )
                            )
                            chunk_text += (
                                f"\n- name: {chunk_call.function.name}\n  arguments: "
                            )
                        else:
                            raise LlmError.bad_completion(
                                "Cannot create partial tool call without a name", None
                            )

                    if chunk_call.function and (
                        chunk_arguments := chunk_call.function.arguments
                    ):
                        if tool_calls:
                            tool_calls[-1].arguments += chunk_arguments
                            chunk_text += chunk_arguments

                        # NOTE: Sometimes, claude-sonnet-thinking chunks include an empty
                        # tool call without an ID (arguments='{}', name=None).
                        # Just ignore these -- but *do* raise an error if it ever tries to
                        # add real arguments to a non-existent tool call (unreachable).
                        elif chunk_arguments != "{}":
                            raise LlmError.bad_completion(
                                "Cannot add arguments without partial tool call.", None
                            )

            if force_send or len(chunk_text) >= STREAM_TOKEN_THRESHOLD:
                if BackendConfig.verbose >= 2 and chunk_text:  # noqa: PLR2004
                    print(colored(chunk_text, "green"), end="")
                chunk_text = ""

                if callback:
                    native_completion = LlmNativeCompletion.parse(
                        answer=answer,
                        thoughts=(
                            [LlmThink(text=thinking_text, signature=thinking_signature)]
                            if thinking_text or thinking_signature
                            else []
                        ),
                        tool_calls=tool_calls,
                        final=False,
                        supports_think=self.supports_think,
                    )
                    callback(
                        self._parse_completion(
                            native_completion, xml_sections, xml_hallucinations
                        )
                    )

        if tool_calls:
            chunk_text += "\n</tool-calls>"
        if BackendConfig.verbose >= 2:  # noqa: PLR2004
            print(colored(f"{chunk_text}\n", "green"))

        native_completion = LlmNativeCompletion.parse(
            answer=answer,
            thoughts=(
                [LlmThink(text=thinking_text, signature=thinking_signature)]
                if thinking_text or thinking_signature
                else []
            ),
            tool_calls=tool_calls,
            final=True,
            supports_think=self.supports_think,
        )

        if callback:
            callback(
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
