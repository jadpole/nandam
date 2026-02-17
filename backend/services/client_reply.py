"""
SvcClient - Client-Provided Tools
==================================

When a web client sends a message, it may declare tools that the chatbot can
invoke.  These tools are forwarded to the client as SSE events, executed
client-side, and the results sent back.

`SvcClient` implements `ToolsProvider` so the chatbot can discover and call
client tools alongside server-side tools.
"""

import asyncio
import logging
import weakref

from dataclasses import dataclass
from pydantic import BaseModel, Field
from typing import Literal

from base.core.exceptions import BadRequestError
from base.core.unique_id import unique_id_random
from base.models.context import NdService
from base.strings.auth import ServiceId
from base.strings.process import ProcessId

from backend.domain.workspace import WorkspaceStore
from backend.models.api_client import ClientAction
from backend.models.process_info import ToolDefinition, ToolMode
from backend.models.process_result import ProcessResult
from backend.models.workspace_thread import BotMessagePart, BotMessagePart_, MessageTool
from backend.server.context import (
    NdProcess,
    NdTool,
    ServiceConfig,
    ToolsProvider,
    WorkspaceContext,
)

logger = logging.getLogger(__name__)


class ClientReplyProcess(NdProcess):
    """
    A process that delegates execution to the web client via `SvcClient`.
    """

    kind: Literal["client-reply"] = "client-reply"
    mode: ToolMode = "custom"

    async def on_spawn(self) -> None:
        client = self.ctx().service(SvcClientReply, self.owner)
        store = WorkspaceStore.new(self)
        secret = await store.register_process(self)
        client.send_action(
            ClientAction(secret=secret, name=self.name, arguments=self.arguments)
        )


class SvcClientReplyConfig(ServiceConfig, frozen=True):
    kind: Literal["client"] = "client"
    tools: list[ToolDefinition]
    reply_id: str = Field(default_factory=unique_id_random)

    def service_id(self) -> ServiceId:
        return ServiceId.new("client-reply", self.reply_id)

    def initialize(self, parent: NdProcess | WorkspaceContext) -> SvcClientReply:
        if not isinstance(parent, NdProcess):
            raise BadRequestError.new("SvcClientReply requires parent process")
        return SvcClientReply.initialize(
            context=parent.ctx(),
            service_id=self.service_id(),
            tools=self.tools,
        )


class ClientProvisionalReply(BaseModel, frozen=True):
    summary: str | None
    reply: list[BotMessagePart_]
    actions: list[ClientAction]


##
## Service
##


@dataclass(kw_only=True)
class SvcClientReply(NdService, ToolsProvider):
    context: WorkspaceContext
    tools: list[ToolDefinition]
    summary: str | None = None
    reply_provisional: list[BotMessagePart_]
    reply_committed: list[BotMessagePart_]
    pending_actions: list[ClientAction]
    event_done: asyncio.Event
    event_flush: asyncio.Event

    @staticmethod
    def initialize(
        context: WorkspaceContext,
        service_id: ServiceId,
        tools: list[ToolDefinition],
    ) -> SvcClientReply:
        return SvcClientReply(
            service_id=service_id,
            context=weakref.proxy(context),
            tools=tools,
            summary=None,
            reply_provisional=[],
            reply_committed=[],
            pending_actions=[],
            event_done=asyncio.Event(),
            event_flush=asyncio.Event(),
        )

    def list_tools(self, parent: NdProcess | WorkspaceContext) -> list[NdTool]:
        """
        Build NdTool instances for each client-provided tool definition.
        Client tools use a special `ClientToolProcess` that delegates execution
        back to the web client.
        """
        return [
            NdTool.from_definition(
                process=ClientReplyProcess,
                owner=self.service_id,
                mode="custom",
                definition=tool_def,
                default_enabled=True,
            )
            for tool_def in self.tools
        ]

    def pull(self) -> ClientProvisionalReply:
        return ClientProvisionalReply(
            summary=self.summary,
            reply=[*self.reply_committed, *self.reply_provisional],
            actions=self.pull_actions(),
        )

    def pull_actions(self) -> list[ClientAction]:
        actions = self.pending_actions.copy()
        self.pending_actions.clear()
        return actions

    def send_done(self) -> None:
        self.event_done.set()
        self.event_flush.set()

    def send_action(self, action: ClientAction) -> None:
        self.pending_actions.append(action)
        self.event_flush.set()

    def put_summary(self, summary: str) -> None:
        self.summary = summary

    def put_reply(self, reply: list[BotMessagePart]) -> None:
        self.reply_provisional = [*reply]

    def put_tool_result(
        self,
        process_id: ProcessId,
        result: ProcessResult,
    ) -> None:
        replacement: tuple[int, BotMessagePart] | None = None
        for idx, part in enumerate(self.reply_committed):
            if isinstance(part, MessageTool) and part.process_id == process_id:
                replacement = (
                    idx,
                    MessageTool(
                        process_id=process_id,
                        name=part.name,
                        arguments=part.arguments,
                        result=result.untyped(),
                    ),
                )
                break

        if replacement:
            idx, new_part = replacement
            self.reply_committed[idx] = new_part

        self.event_flush.set()

    def commit_reply(
        self,
        reply: list[BotMessagePart],
        provisional: list[BotMessagePart] | None = None,
    ) -> None:
        self.reply_committed.extend(reply)
        self.reply_provisional = provisional or []
        self.event_flush.set()
