import logging

from cerebras.cloud.sdk import AsyncCerebras, AsyncStream
from cerebras.cloud.sdk.types.chat import ChatCompletion
from cerebras.cloud.sdk.types.chat.chat_completion import (
    ChatChunkResponse,
    ChatCompletionResponse,
)
from dataclasses import dataclass
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

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 300  # 5 minutes
STREAM_TOKEN_THRESHOLD = 40
"""
When streaming, instead of sending updates after each token, wait until at least
this many tokens have been generated, unless `force_send` is set, indicating the
end of a section (thinking block, tool call, etc.)
"""


@dataclass(kw_only=True)
class LlmCerebrasParams:
    params: Any
    new_history: LlmHistory
    new_messages: list[LlmPart]


class LlmCerebrasState(BaseModel):
    history: LlmHistory

    def append_part(self, part: LlmPart) -> None:
        pass


@dataclass(kw_only=True)
class LlmCerebrasUpdate:
    new_history: LlmHistory


def init_cerebras_client() -> AsyncCerebras:
    return AsyncCerebras(
        api_key=BackendConfig.llm.cerebras_api_key,
        base_url=BackendConfig.llm.cerebras_api_base,
    )


class LlmCerebras(LlmModel[LlmCerebrasParams, LlmCerebrasState, LlmCerebrasUpdate]):
    _client: AsyncCerebras = PrivateAttr(default_factory=init_cerebras_client)

    @classmethod
    def state_type(cls) -> type[LlmCerebrasState]:
        return LlmCerebrasState

    def _get_completion_params(  # noqa: C901
        self,
        **kwargs: Unpack[LlmModelArgs[LlmCerebrasState]],
    ) -> LlmCerebrasParams:
        assert self.supports_tools in (None, "openai")
        assert self.supports_think in (None, "deepseek", "gpt-oss")

        # Convert the messages into the Cerebras format.
        model_info = self.info()
        history = (
            state.history.reuse(model_info, kwargs["process"])
            if (state := kwargs.get("state"))
            else LlmHistory.new(model_info, kwargs["process"])
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
            system_text = safe_openai_message(system_text)
            messages.insert(0, {"role": "system", "content": system_text})

        params: dict[str, Any] = {
            "extra_body": {},
            "model": self.native_name,
            "messages": messages,
            "temperature": 1.0 if self.native_name.startswith("gpt-oss-") else 0.6,
            "timeout": REQUEST_TIMEOUT,
        }

        # Parameters that should be NOT_GIVEN when false.
        if kwargs.get("callback") and self.supports_stream:
            params["stream"] = True
        if (value_stop := kwargs.get("stop")) and self.supports_stop:
            params["stop"] = value_stop[:4]  # API supports up to 4 items.

        if (value := kwargs.get("max_tokens")) and not self.supports_think:
            params["max_tokens"] = value
        if (value := kwargs.get("temperature")) is not None and not self.supports_think:
            params["temperature"] = value

        # Reasoning limits.
        if self.supports_think:
            params["reasoning_format"] = "parsed"
            if self.supports_think == "gpt-oss" and self.reasoning_effort:
                params["reasoning_effort"] = self.reasoning_effort
        elif self.native_name.startswith("zai-glm-"):
            params["disable_reasoning"] = True

        # Native tools.
        if self.supports_tools == "openai" and (tools := kwargs.get("tools")):
            params["parallel_tool_calls"] = True
            params["tools"] = [
                {
                    "type": "function",
                    "function": {
                        "name": tool.name,
                        "strict": True,
                        "description": tool.description,
                        "parameters": clean_jsonschema(
                            tool.arguments_schema,
                            disallow_examples=True,
                            disallow_pattern=True,
                        ),
                    },
                }
                for tool in tools
            ]

        # Response format.
        if response_schema := kwargs.get("response_schema"):
            params["response_format"] = {
                "type": "json_schema",
                "strict": True,
                "json_schema": clean_jsonschema(
                    response_schema,
                    disallow_examples=True,
                    disallow_pattern=True,
                ),
            }

        return LlmCerebrasParams(
            params=params,
            new_history=history,
            new_messages=kwargs["messages"],
        )

    async def _update_state(
        self,
        request: LlmModelArgs[LlmCerebrasState],
        update: LlmCerebrasUpdate,
        completion: LlmNativeCompletion,
        parsed: list[LlmPart] | None,
    ) -> LlmCerebrasState:
        if parsed is None:
            parsed = self._parse_completion(
                completion,
                request.get("xml_sections"),
                request.get("xml_hallucinations"),
            )

        for part in parsed:
            update.new_history.add_part(part)

        return LlmCerebrasState(history=update.new_history)

    ##
    ## Completion
    ##

    async def _get_completion_result(
        self,
        callback: LlmCallback | None,
        params: LlmCerebrasParams,
        xml_sections: list[type[LlmPart]],
        xml_hallucinations: list[str],
    ) -> tuple[LlmNativeCompletion, LlmCerebrasUpdate]:
        if BackendConfig.verbose >= 4:  # noqa: PLR2004
            prompt_log = "\n".join(as_json(m) for m in params.params["messages"])
            print(colored(f"<messages>\n{prompt_log}\n</messages>", "blue"))
        elif BackendConfig.verbose >= 3:  # noqa: PLR2004
            prompt_log = "\n".join(m.render_debug() for m in params.new_messages)
            print(colored(f"<messages>\n{prompt_log}\n</messages>", "blue"))

        completion = await self._client.chat.completions.create(**params.params)
        if params.params.get("stream"):
            assert isinstance(completion, AsyncStream)
            result = await self._consume_completion_stream(
                callback, completion, xml_sections, xml_hallucinations
            )
        else:
            assert isinstance(completion, ChatCompletionResponse)
            result = await self._consume_completion_batch(
                callback, completion, xml_sections, xml_hallucinations
            )

        return result, LlmCerebrasUpdate(new_history=params.new_history)

    async def _consume_completion_batch(
        self,
        callback: LlmCallback | None,
        completion: ChatCompletionResponse,
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> LlmNativeCompletion:
        answer: str = ""
        tool_calls: list[LlmPartialToolCall] = []
        thinking: LlmThink | None = None

        response = completion.choices[0].message

        if response.content:
            answer = response.content

        if response.reasoning:
            thinking = LlmThink(text=response.reasoning, signature=None)

        if response.tool_calls:
            tool_calls.extend(
                LlmPartialToolCall(
                    id=tool_call.id,
                    name=tool_call.function.name,
                    arguments=tool_call.function.arguments,
                )
                for tool_call in response.tool_calls
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
        completion: AsyncStream[ChatCompletion],
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> LlmNativeCompletion:
        if BackendConfig.verbose >= 2:  # noqa: PLR2004
            print()

        answer: str = ""
        thinking_text: str = ""
        tool_calls: list[LlmPartialToolCall] = []

        chunk_text: str = ""

        chunk: ChatChunkResponse
        async for chunk in completion:  # type: ignore
            force_send: bool = False

            if not chunk.choices:
                continue
            if not (delta := chunk.choices[0].delta):
                continue

            if delta.reasoning:
                if not thinking_text:
                    chunk_text += "<think>\n"
                thinking_text += delta.reasoning
                chunk_text += delta.reasoning

            if delta.content:
                if thinking_text and not answer:
                    force_send = True
                    chunk_text += "\n</think>\n\n"
                answer += delta.content
                chunk_text += delta.content

            if delta.tool_calls:
                if not tool_calls:
                    chunk_text += "\n<tool-calls>"

                for chunk_call in delta.tool_calls:
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
                            [LlmThink(text=thinking_text, signature=None)]
                            if thinking_text
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
            answer=answer,
            thoughts=(
                [LlmThink(text=thinking_text, signature=None)] if thinking_text else []
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


def safe_openai_message(content: str) -> str:
    """
    We "escape" special tokens to prevent an exception.
    Side effect: messages are not exactly the same as the original.
    """
    return content.replace("<|endoftext|>", "<||endoftext||>")
