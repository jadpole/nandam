import weakref

from dataclasses import dataclass
from pydantic import BaseModel
from typing import Any

from base.models.content import ContentText, PartText, TextPart
from base.models.context import NdService
from base.models.rendered import Rendered
from base.resources.metadata import ResourceInfo
from base.strings.auth import AgentId, BotId, ServiceId
from base.strings.process import ProcessId, ProcessName
from base.strings.scope import Workspace

from backend.data.llm_models import get_llm_by_name
from backend.domain.api_chatbot import bot_default_persona
from backend.llm.message import LlmPart, LlmTool, LlmToolResult, LlmUserMessage
from backend.llm.model import LlmCallback, LlmModel
from backend.models.bot_persona import AnyPersona
from backend.models.bot_state import BotState
from backend.models.process_result import ProcessResult, RenderedResult
from backend.models.workspace_thread import (
    MessageText,
    ThreadMessage,
    ThreadMessageBot,
    ThreadMessageUser,
)
from backend.server.context import NdProcess, WorkspaceContext
from backend.services.kv_store import SvcKVStore

SVC_LLM = ServiceId.decode("svc-llm")

REGEX_LLM_HISTORY_ID = r"llm-[a-z0-9]{20}"
NUM_CHARS_LLM_HISTORY_ID = 20
NUM_CHARS_LLM_HISTORY_SUFFIX = 8


@dataclass(kw_only=True)
class SvcLlm(NdService):
    service_id: ServiceId = SVC_LLM
    kv_store: SvcKVStore
    workspace: Workspace

    @staticmethod
    def initialize(context: WorkspaceContext) -> SvcLlm:
        return SvcLlm(
            kv_store=context.service(SvcKVStore),
            workspace=context.workspace,
        )

    async def acquire(
        self,
        process: NdProcess,
        bot_state: BotState,
        persona: AnyPersona | None,
    ) -> LlmProxy:
        assert process.process_uri.workspace == self.workspace
        persona = (
            persona
            or bot_state.persona
            or await bot_default_persona(self.kv_store, self.workspace)
        )
        return LlmProxy.new(
            process=process,
            bot_id=bot_state.bot_id,
            persona=persona,
            llm_state=bot_state.llm_state,
        )

    async def llm_by_name(self, model: str) -> LlmModel:
        return get_llm_by_name(model)

    async def oneshot_completion(
        self,
        *,
        process: NdProcess,
        model: str,
        callback: LlmCallback | None = None,
        system: str | None,
        messages: list[LlmPart],
        max_tokens: int | None = None,
        stop: list[str] | None = None,
        temperature: float | None = None,
        tools: list[LlmTool] | None = None,
        tool_choice: ProcessName | None = None,
        xml_hallucinations: list[str] | None = None,
        xml_sections: list[type[LlmPart]] | None = None,
    ) -> list[LlmPart]:
        completion, _ = await get_llm_by_name(model).get_completion(
            process=process,
            callback=callback,
            system=system,
            messages=messages,
            max_tokens=max_tokens,
            stop=stop,
            temperature=temperature,
            tools=tools,
            tool_choice=tool_choice,
            xml_hallucinations=xml_hallucinations,
            xml_sections=xml_sections,
        )
        return completion

    async def oneshot_completion_json[T: BaseModel](
        self,
        type_: type[T],
        *,
        process: NdProcess,
        model: str,
        callback: LlmCallback | None = None,
        system: str | None,
        messages: list[LlmPart],
        max_tokens: int | None = None,
        temperature: float | None = None,
    ) -> T:
        completion, _ = await get_llm_by_name(model).get_completion_json(
            type_=type_,
            process=process,
            callback=callback,
            system=system,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return completion


# TODO:
# @dataclass(kw_only=True)
# class SvcLlmStub(SvcLlm): ...


@dataclass(kw_only=True)
class LlmProxy[P, S: BaseModel, U]:
    process: NdProcess
    bot_id: BotId
    persona: AnyPersona
    llm_model: LlmModel[P, S, U]
    llm_state: S
    pending: list[LlmPart]

    @staticmethod
    def new(
        process: NdProcess,
        bot_id: BotId,
        persona: AnyPersona,
        llm_state: dict[str, Any] | None,
    ) -> LlmProxy[P, S, U]:
        llm_model = get_llm_by_name(persona.model)
        state_type: type[S] = llm_model.state_type()
        return LlmProxy(
            process=weakref.proxy(process),
            bot_id=bot_id,
            persona=persona,
            llm_model=llm_model,
            llm_state=(
                state_type.model_validate(llm_state) if llm_state else state_type()
            ),
            pending=[],
        )

    def add_message(
        self,
        sender: AgentId,
        content: Rendered | ContentText | str,
    ) -> None:
        if isinstance(content, str):
            content = ContentText.parse(content)
        if isinstance(content, ContentText):
            content = Rendered.render(content, self.process.cached_observations())
        self.pending.append(LlmUserMessage(sender=sender, content=content))

    def add_tool_result(
        self,
        sender: ServiceId,
        process_id: ProcessId,
        name: ProcessName,
        result: RenderedResult | ProcessResult,
    ) -> None:
        if isinstance(result, ProcessResult):
            result = RenderedResult.render(result, self.process.cached_observations())
        self.pending.append(
            LlmToolResult(
                sender=sender,
                process_id=process_id,
                name=name,
                result=result,
            )
        )

    def add_thread_message(self, message: ThreadMessage) -> None:
        if isinstance(message, ThreadMessageUser) and (
            content_user := self._render_thread_message_user(message)
        ):
            self.add_message(message.sender, content_user)
        elif (
            isinstance(message, ThreadMessageBot)
            and message.sender != self.bot_id
            and (content_bot := self._render_thread_message_bot(message))
        ):
            self.add_message(message.sender, content_bot)

    def _render_thread_message_user(self, message: ThreadMessageUser) -> ContentText:
        # TODO: Include profile information.
        parts: list[TextPart] = [PartText.new("<user-message>", "\n")]

        if message.content and message.content.parts:
            parts.extend(
                [
                    PartText.new("<content>", "\n"),
                    *message.content.parts,
                    PartText.new("</content>", "\n"),
                ]
            )
        if message.attachments:
            parts.extend(
                [
                    PartText.new("<attachments>", "\n"),
                    *[
                        att_part
                        for att in message.attachments
                        for att_part in self._render_thread_message_attachment(att)
                    ],
                    PartText.new("</attachments>", "\n"),
                ]
            )

        if message.attachment_errors:
            parts.extend(
                [
                    PartText.new("<attachment-errors>", "\n"),
                    *[
                        PartText.new(
                            f'<attachment-error name="{name}">{error.message}</attachment-error>',
                            "\n",
                        )
                        for name, error in message.attachment_errors.items()
                    ],
                    PartText.new("</attachment-errors>", "\n"),
                ]
            )

        parts.append(PartText.new("</user-message>", "\n"))
        return ContentText.new(parts)

    def _render_thread_message_attachment(
        self,
        resource: ResourceInfo,
    ) -> list[TextPart]:
        resource_attrs: list[tuple[str, str]] = []
        if resource.attributes.name:
            resource_attrs.append(
                ("name", resource.attributes.name),
            )
        if resource.attributes.mime_type:
            resource_attrs.append(
                ("mimetype", resource.attributes.mime_type),
            )
        if resource.attributes.description:
            resource_attrs.append(
                ("description", resource.attributes.description),
            )
        if resource.attributes.created_at:
            resource_attrs.append(
                ("created", resource.attributes.created_at.isoformat()),
            )
        if resource.attributes.updated_at:
            resource_attrs.append(
                ("updated", resource.attributes.updated_at.isoformat()),
            )

        if not resource.affordances:
            return PartText.xml_open(
                "resource",
                resource.uri,
                resource_attrs,
                self_closing=True,
            )

        result: list[TextPart] = PartText.xml_open(
            "resource",
            resource.uri,
            resource_attrs,
            self_closing=False,
        )

        for _affordance in resource.affordances:
            pass  # TODO

        result.append(PartText.xml_close("resource"))
        return result

    def _render_thread_message_bot(
        self,
        message: ThreadMessageBot,
    ) -> ContentText | None:
        last_text = None
        for part in message.content:
            if isinstance(part, MessageText) and part.text:
                last_text = part.text
        return last_text

    async def get_completion(
        self,
        *,
        callback: LlmCallback | None = None,
        max_tokens: int | None = None,
        stop: list[str] | None = None,
        system: str | None,
        temperature: float | None = None,
        tools: list[LlmTool] | None = None,
        tool_choice: ProcessName | None = None,
        xml_hallucinations: list[str] | None = None,
        xml_sections: list[type[LlmPart]] | None = None,
    ) -> list[LlmPart]:
        completion, new_state = await self.llm_model.get_completion(
            process=self.process,
            callback=callback,
            state=self.llm_state,
            system=system,
            messages=self.pending,
            max_tokens=max_tokens,
            stop=stop,
            temperature=temperature,
            tools=tools,
            tool_choice=tool_choice,
            xml_hallucinations=xml_hallucinations,
            xml_sections=xml_sections,
        )
        self.llm_state = new_state
        self.pending = []
        return completion

    async def get_completion_json[T: BaseModel](
        self,
        type_: type[T],
        *,
        callback: LlmCallback | None = None,
        max_tokens: int | None = None,
        system: str | None,
        temperature: float | None = None,
    ) -> T:
        completion, new_state = await self.llm_model.get_completion_json(
            type_,
            process=self.process,
            callback=callback,
            state=self.llm_state,
            max_tokens=max_tokens,
            messages=self.pending,
            system=system,
            temperature=temperature,
        )
        self.llm_state = new_state
        self.pending = []
        return completion
