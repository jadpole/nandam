import base64

from dataclasses import dataclass
from google import genai
from pydantic import BaseModel
from termcolor import colored
from typing import Unpack

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

REQUEST_TIMEOUT = 300  # 5 minutes
STREAM_TOKEN_THRESHOLD = 40
"""
When streaming, instead of sending updates after each token, wait until at least
this many tokens have been generated, unless `force_send` is set, indicating the
end of a section (thinking block, tool call, etc.)
"""


@dataclass(kw_only=True)
class LlmGeminiParams:
    client: genai.Client
    config: genai.types.GenerateContentConfig
    contents: list[genai.types.Content]
    model: str
    new_history: LlmHistory
    new_messages: list[LlmPart]


class LlmGeminiState(BaseModel):
    history: LlmHistory

    def append_part(self, part: LlmPart) -> None:
        pass


@dataclass(kw_only=True)
class LlmGeminiUpdate:
    new_history: LlmHistory


class LlmGemini(LlmModel[LlmGeminiParams, LlmGeminiState, LlmGeminiUpdate]):
    @classmethod
    def state_type(cls) -> type[LlmGeminiState]:
        return LlmGeminiState

    def _get_completion_params(
        self,
        **kwargs: Unpack[LlmModelArgs[LlmGeminiState]],
    ) -> LlmGeminiParams:
        assert self.supports_tools in (None, "gemini")
        assert self.supports_think in (None, "gemini")

        # NOTE: When going through the LLM Router, remove the prefix and pass the
        # the GCP-compatible model name in `extra_body`.
        if BackendConfig.llm.gemini_api_key:
            client = genai.Client(
                api_key=BackendConfig.llm.gemini_api_key,
                http_options=genai.types.HttpOptions(
                    api_version="v1alpha",
                ),
            )
        elif BackendConfig.llm.router_api_base and BackendConfig.llm.router_api_key:
            client = genai.Client(
                api_key=BackendConfig.llm.router_api_key,
                http_options=genai.types.HttpOptions(
                    base_url=f"{BackendConfig.llm.router_api_base}/gemini",
                    api_version="v1alpha",
                    extra_body={"model": self.native_name},
                    headers=kwargs["process"].llm_headers(),
                ),
            )
        else:
            raise LlmError("LlmGemini requires LLM_GEMINI_API_KEY")

        # Convert the messages into the Gemini format.
        model_info = self.info()
        history = (
            state.history.reuse(model_info)
            if (state := kwargs.get("state"))
            else LlmHistory.new(model_info)
        )
        for part in kwargs["messages"]:
            history.add_part(part)

        messages = history.render_gemini(limit_media=self.limit_media)
        config = genai.types.GenerateContentConfig(
            system_instruction=self._convert_system(
                process=kwargs["process"],
                system=kwargs.get("system"),
                tools=kwargs.get("tools") or [],
                xml_sections=kwargs.get("xml_sections") or [],
            ),
            response_json_schema=kwargs.get("response_schema") or None,
            response_mime_type=(
                "application/json" if kwargs.get("response_schema") else None
            ),
            tools=(
                [
                    genai.types.Tool(
                        function_declarations=[
                            genai.types.FunctionDeclaration(
                                name=tool.name,
                                description=tool.description,
                                parameters_json_schema=tool.arguments_schema,
                            )
                        ]
                    )
                    for tool in tools
                ]
                if (tools := kwargs.get("tools"))
                else None
            ),
            stop_sequences=kwargs.get("stop") or None,
        )

        if (value := kwargs.get("max_tokens")) and not self.supports_think:
            config.max_output_tokens = value

        if (value := kwargs.get("temperature")) is not None and not self.supports_think:
            config.temperature = value
        else:
            config.temperature = 1.0

        # NOTE: reasoning_effort "medium" -> "high" as per the docs.
        if self.reasoning_effort:
            config.thinking_config = genai.types.ThinkingConfig(
                include_thoughts=True,
                thinking_level=(
                    genai.types.ThinkingLevel.HIGH
                    if self.reasoning_effort != "low"
                    else genai.types.ThinkingLevel.LOW
                ),
            )

        return LlmGeminiParams(
            client=client,
            config=config,
            contents=messages,
            model=self.native_name,
            new_history=history,
            new_messages=kwargs["messages"],
        )

    async def _update_state(
        self,
        request: LlmModelArgs[LlmGeminiState],
        update: LlmGeminiUpdate,
        completion: LlmNativeCompletion,
        parsed: list[LlmPart] | None,
    ) -> LlmGeminiState:
        if parsed is None:
            parsed = self._parse_completion(
                completion,
                request.get("xml_sections"),
                request.get("xml_hallucinations"),
            )

        for part in parsed:
            update.new_history.add_part(part)

        return LlmGeminiState(history=update.new_history)

    ##
    ## Completion
    ##

    async def _get_completion_result(
        self,
        callback: LlmCallback | None,
        params: LlmGeminiParams,
        xml_sections: list[type[LlmPart]],
        xml_hallucinations: list[str],
    ) -> tuple[LlmNativeCompletion, LlmGeminiUpdate]:
        if BackendConfig.verbose >= 4:  # noqa: PLR2004
            prompt_log = "\n".join(gemini_debug_content(m) for m in params.contents)
            print(colored(f"<messages>\n{prompt_log}\n</messages>", "blue"))
        elif BackendConfig.verbose >= 3:  # noqa: PLR2004
            prompt_log = "\n".join(m.render_debug() for m in params.new_messages)
            print(colored(f"<messages>\n{prompt_log}\n</messages>", "blue"))

        if self.supports_stream and callback:
            completion = await self._get_completion_stream(
                callback, params, xml_sections, xml_hallucinations
            )
        else:
            completion = await self._get_completion_batch(
                callback, params, xml_sections, xml_hallucinations
            )

        return completion, LlmGeminiUpdate(new_history=params.new_history)

    async def _get_completion_batch(  # noqa: C901
        self,
        callback: LlmCallback | None,
        params: LlmGeminiParams,
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> LlmNativeCompletion:
        response = await params.client.aio.models.generate_content(
            model=params.model,
            contents=list(params.contents),
            config=params.config,
        )

        answer: str = ""
        tool_calls: list[LlmPartialToolCall] = []
        thoughts: list[LlmThink] = []

        # Handle candidates
        if response.candidates:
            candidate = response.candidates[0]
            if candidate.content and candidate.content.parts:
                for part in candidate.content.parts:
                    if part.thought:
                        thoughts.append(
                            LlmThink(
                                text=part.text or "",
                                signature=None,
                            )
                        )
                    elif part.text:
                        answer += part.text

                    if part.function_call:
                        tool_call = part.function_call
                        if not tool_call.name:
                            continue

                        tool_calls.append(
                            LlmPartialToolCall(
                                id=tool_call.id,
                                name=tool_call.name,
                                arguments=as_json(tool_call.args or {}),
                            )
                        )

                    if part.thought_signature:
                        signature = base64.b64encode(part.thought_signature).decode(
                            "utf-8"
                        )
                        if thoughts:
                            thoughts[-1] = LlmThink(
                                text=thoughts[-1].text,
                                signature=signature,
                            )
                        else:
                            thoughts.append(LlmThink(text="", signature=signature))

        native_completion = LlmNativeCompletion.parse(
            answer=answer,
            thoughts=thoughts,
            tool_calls=tool_calls,
            final=True,
            supports_think=self.supports_think,
        )

        if BackendConfig.verbose >= 2:  # noqa: PLR2004
            print(colored(f"\n{native_completion.render_debug()}\n", "green"))

        if callback:
            parsed_completion = self._parse_completion(
                native_completion, xml_sections, xml_hallucinations
            )
            await callback(parsed_completion)

        return native_completion

    async def _get_completion_stream(  # noqa: C901, PLR0912
        self,
        callback: LlmCallback | None,
        params: LlmGeminiParams,
        xml_sections: list[type[LlmPart]] | None,
        xml_hallucinations: list[str] | None,
    ) -> LlmNativeCompletion:
        answer = ""
        thoughts: list[LlmThink] = []
        tool_calls: list[LlmPartialToolCall] = []

        chunk_text: str = ""

        async for chunk in await params.client.aio.models.generate_content_stream(
            model=params.model,
            contents=list(params.contents),
            config=params.config,
        ):
            force_send = False

            if chunk.candidates:
                candidate = chunk.candidates[0]
                if candidate.content and candidate.content.parts:
                    for part in candidate.content.parts:
                        if part.thought and part.text:
                            thought = LlmThink(text=part.text.strip(), signature=None)
                            thoughts.append(thought)
                            chunk_text += (
                                "<think>\n" + part.text.strip() + "\n</think>\n\n"
                            )
                            force_send = True
                            continue

                        if part.text:
                            answer += part.text
                            chunk_text += part.text

                        if part.function_call:
                            tool_call = part.function_call
                            if not tool_call.name:
                                continue

                            if not tool_calls:
                                chunk_text += "\n<tool-calls>"

                            tool_calls.append(
                                LlmPartialToolCall(
                                    id=tool_call.id,
                                    name=tool_call.name,
                                    arguments=as_json(tool_call.args or {}),
                                )
                            )
                            chunk_text += f"\n- name: {tool_call.name}\n  arguments: {as_json(tool_call.args or {})}"
                            force_send = True

                        if part.thought_signature:
                            signature = base64.b64encode(part.thought_signature).decode(
                                "utf-8"
                            )
                            if thoughts:
                                thoughts[-1] = LlmThink(
                                    text=thoughts[-1].text,
                                    signature=signature,
                                )
                            else:
                                thoughts.append(LlmThink(text="", signature=signature))

            if force_send or len(chunk_text) >= STREAM_TOKEN_THRESHOLD:
                if BackendConfig.verbose >= 2 and chunk_text:  # noqa: PLR2004
                    print(colored(chunk_text, "green"), end="")
                chunk_text = ""

                if callback:
                    native_completion = LlmNativeCompletion.parse(
                        answer=answer,
                        thoughts=thoughts,
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
            thoughts=thoughts,
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


def gemini_debug_content(content: genai.types.Content) -> str:
    return "\n".join(
        [
            f'<gemini-message role="{content.role}">',
            *[gemini_debug_part(part) for part in content.parts or []],
            "</gemini-message>",
        ]
    )


def gemini_debug_part(part: genai.types.Part) -> str:
    rendered: list[str] = []
    if part.thought_signature:
        rendered.append("<gemini-thought-signature />")
    if part.text:
        rendered.append(f"<gemini-text>{part.text}</gemini-text>")
    if part.inline_data:
        rendered.append(
            f'<gemini-inline-data mimetype="{part.inline_data.mime_type}" />'
        )
    if part.function_call:
        rendered.append(
            f"<gemini-tool-call>{as_json(part.function_call)}</gemini-tool-call>"
        )
    if part.function_response:
        rendered.append(f"<gemini-tool>{as_json(part.function_response)}</gemini-tool>")
    return "\n".join(rendered)
