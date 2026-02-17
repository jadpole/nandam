import asyncio
from dataclasses import dataclass
import logging

from pydantic import BaseModel, PrivateAttr
from typing import Any, Literal

from base.core.exceptions import ApiError, StoppedError
from base.core.values import as_json
from base.strings.auth import BotId
from base.strings.process import ProcessId, ProcessName, ProcessUri
from base.strings.thread import ThreadCursor, ThreadUri

from backend.domain.api_chatbot import bot_acquire
from backend.domain.thread import list_messages
from backend.llm.message import (
    LlmPart,
    LlmPart_,
    LlmText,
    LlmTool,
    LlmToolCall,
    LlmToolCalls,
    LlmToolResult,
    system_instructions,
)
from backend.models.bot_persona import AnyPersona_
from backend.models.bot_state import BotState
from backend.models.process_info import ToolMode
from backend.models.process_result import ProcessStopped, RenderedResult
from backend.models.workspace_thread import (
    BotMessagePart,
    BotMessagePart_,
    MessageText,
    MessageTool,
)
from backend.server.context import NdProcess, NdTool, ProcessInfo, ProcessListener
from backend.services.client_reply import SvcClientReply
from backend.services.kv_store import SvcKVStore
from backend.services.llm import LlmProxy, SvcLlm
from backend.services.threads import SvcThreads
from backend.services.tools_backend import BackendProcess

logger = logging.getLogger(__name__)

MAX_COMPLETIONS = 5


class ChatbotArguments(BaseModel):
    bot_id: BotId
    persona: AnyPersona_ | None
    thread_uris: list[ThreadUri]


class ChatbotProgressCompletion(BaseModel, frozen=True):
    step: Literal["completion"] = "completion"
    original: list[LlmPart_]
    corrected: list[LlmPart_]
    committed: list[BotMessagePart_]


class ChatbotReturn(BaseModel):
    reply: list[BotMessagePart_]


@dataclass(kw_only=True)
class BotChatState:
    bot: BotState
    llm: LlmProxy
    new_cursors: list[ThreadCursor]
    reply: list[BotMessagePart]


@dataclass(kw_only=True)
class BotChatToolCall:
    tool: NdTool
    uri: ProcessUri
    arguments: dict[str, Any]


class BotChat(BackendProcess[ChatbotArguments, ChatbotReturn]):
    kind: Literal["bot/chat"] = "bot/chat"
    name: ProcessName = ProcessName.decode("bot_chat")
    mode: ToolMode = "production"
    _done: asyncio.Event = PrivateAttr(default_factory=asyncio.Event)
    _stop: asyncio.Event = PrivateAttr(default_factory=asyncio.Event)

    @classmethod
    def _info(cls) -> ProcessInfo:
        return ProcessInfo(
            description="Chat with the assistant in a thread.",
            human_name="Chatbot",
            human_description="Conversational assistant",
        )

    async def on_sigterm(self) -> None:
        self._stop.set()
        await self._done.wait()
        await self._on_update(result=ProcessStopped(reason="stopped"))

    @classmethod
    def progress_types(cls) -> tuple[type[BaseModel], ...]:
        return (ChatbotProgressCompletion,)

    async def _run(self) -> ChatbotReturn:
        # TODO:
        # self._put_summary(
        #     "**Downloading attachments...**"
        #     if self.arguments.attachments
        #     else "**Remembering...**",
        # )
        try:
            state = await self._lifecycle_before()

            for step in range(MAX_COMPLETIONS):
                force_answer = step == MAX_COMPLETIONS - 1
                tool_calls = await self._run_step_reply(
                    force_answer=force_answer,
                    state=state,
                )
                if not tool_calls:
                    break
                await self._run_step_tools(
                    state=state,
                    tool_calls=tool_calls,
                )

            return ChatbotReturn(reply=state.reply.copy())
        finally:
            self._done.set()
            if client := self.get_service(SvcClientReply):
                client.send_done()

    async def _lifecycle_before(self) -> BotChatState:
        threads = self.service(SvcThreads)
        llm = self.service(SvcLlm)
        bot = await bot_acquire(
            self.service(SvcKVStore),
            self.ctx().workspace,
            self.arguments.bot_id,
        )
        llm_proxy = await llm.acquire(self, bot, self.arguments.persona)

        # Add new thread messages as LLM inputs.
        new_cursors, new_messages = await list_messages(
            threads,
            list(self.arguments.thread_uris),
        )
        for message in new_messages:
            llm_proxy.add_thread_message(message)

        return BotChatState(
            bot=bot,
            llm=llm_proxy,
            new_cursors=new_cursors,
            reply=[],
        )

    async def _lifecycle_after(self, state: BotChatState) -> None:
        """
        TODO: Save agent state.
        """

    async def _run_step_reply(
        self,
        *,
        force_answer: bool,
        state: BotChatState,
    ) -> list[BotChatToolCall]:
        """
        TODO: Replace "corrected" state in LLM history?
        """
        self._put_summary("Thinking...")

        llm_system, tools = await self._make_system(state)
        llm_tools = [
            LlmTool(
                name=tool.name,
                description=tool.description,
                arguments_schema=tool.arguments_schema,
            )
            for tool in tools
        ]
        # TODO:
        # if force_answer:
        # state.llm.add_message("Reached steps limit: please provide a final answer.")

        completion = await state.llm.get_completion(
            callback=self._put_reply,
            system=llm_system,
            temperature=state.llm.persona.temperature,
            tools=llm_tools if not force_answer else [],
        )
        corrected, tool_calls = self._auto_correct(tools, completion)
        committed = self._render_reply(corrected)
        await self._on_progress(
            ChatbotProgressCompletion(
                original=completion,
                corrected=corrected,
                committed=committed,
            )
        )
        self._commit_reply(committed)
        return tool_calls

    async def _run_step_tools(
        self,
        *,
        state: BotChatState,
        tool_calls: list[BotChatToolCall],
    ) -> None:
        client = self.get_service(SvcClientReply)

        process: NdProcess
        tool_processes: list[NdProcess] = []
        tool_listeners: list[ProcessListener] = []
        for tool_call in tool_calls:
            process = await self.ctx().spawn_tool(
                None, tool_call.tool, tool_call.uri, tool_call.arguments
            )
            tool_processes.append(process)
            tool_listeners.append(self.ctx().listener(process.process_uri))

        # TODO: Wait for tool results in parallel.
        # TODO: Put tool results in `BotChatState.reply`.
        tools_done = asyncio.gather(
            *[ls.wait_result() for ls in tool_listeners],
            return_exceptions=True,
        )
        stop_requested = asyncio.create_task(self._stop.wait())
        done, _ = await asyncio.wait(
            {tools_done, stop_requested},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if tools_done not in done:
            tools_done.cancel()
        if stop_requested not in done:
            stop_requested.cancel()

        tool_results: list[LlmToolResult] = []
        for tool_process in tool_processes:
            tool_call_id = tool_process.process_uri.process_id
            if not tool_process.result:
                continue
            if client:
                client.put_tool_result(tool_call_id, tool_process.result)

            tool_result = RenderedResult.render(
                tool_process.result, self.cached_observations()
            )
            tool_results.append(
                LlmToolResult(
                    sender=tool_process.owner,
                    process_id=tool_call_id,
                    name=tool_process.name,
                    result=tool_result,
                )
            )

        for tool_result in tool_results:
            state.llm.add_tool_result(
                sender=tool_result.sender,
                process_id=tool_result.process_id,
                name=tool_result.name,
                result=tool_result.result,
            )

    def _auto_correct(
        self,
        tools: list[NdTool],
        completion: list[LlmPart],
    ) -> tuple[list[LlmPart], list[BotChatToolCall]]:
        tool_calls: list[BotChatToolCall] = []

        llm_tool_calls: list[LlmToolCall] = [
            tool_call
            for part in completion
            if isinstance(part, LlmToolCalls)
            for tool_call in part.calls
        ]
        for tool_call in llm_tool_calls:
            tool = next((t for t in tools if t.name == tool_call.name), None)
            if not tool:
                raise ApiError(f"TODO: Invalid tool call: {as_json(tool_call)}")

            tool_call_id = tool_call.process_id or ProcessId.generate()
            tool_call_uri = self.process_uri.child(tool_call_id)
            tool_calls.append(
                BotChatToolCall(
                    tool=tool,
                    uri=tool_call_uri,
                    arguments=tool_call.arguments,
                )
            )

        return completion, tool_calls

    async def _make_system(self, state: BotChatState) -> tuple[str, list[NdTool]]:
        system_prompt: list[str] = []
        system_prompt.append(
            system_instructions(
                info=state.llm.llm_model.info(),
                mermaid=True,
                tips=True,
                tools=True,
            )
        )

        available_tools = self.available_tools()
        selected_tools = sorted(
            (tool for tool in available_tools if state.llm.persona.filter_tool(tool)),
            key=lambda tool: tool.name,
        )

        return "\n".join(system_prompt), selected_tools

    ##
    ## Reply
    ##

    def _commit_reply(self, reply: list[BotMessagePart]) -> None:
        client = self.get_service(SvcClientReply)
        if not client:
            return

        client.put_reply(reply)
        if self._stop.is_set():
            raise StoppedError("stopped")

    def _put_reply(self, completion: list[LlmPart]) -> None:
        client = self.get_service(SvcClientReply)
        if not client:
            return

        rendered = self._render_reply(completion)
        client.put_reply(rendered)
        if self._stop.is_set():
            raise StoppedError("stopped")

    def _put_summary(self, summary: str) -> None:
        client = self.get_service(SvcClientReply)
        if not client:
            return

        client.put_summary(summary)
        if self._stop.is_set():
            raise StoppedError("stopped")

    def _render_reply(self, completion: list[LlmPart]) -> list[BotMessagePart]:
        rendered: list[BotMessagePart] = []
        for idx, part in enumerate(completion):
            if isinstance(part, LlmText):
                rendered.append(MessageText(text=part.content))
            elif isinstance(part, LlmToolCall):
                rendered.append(
                    MessageTool(
                        process_id=part.process_id or ProcessId.temp(str(idx)),
                        name=part.name,
                        arguments=part.arguments,
                    )
                )
        return rendered
