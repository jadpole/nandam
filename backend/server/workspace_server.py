import asyncio
import logging
import time

from dataclasses import dataclass

from base.core.exceptions import ApiError, ErrorInfo
from base.server.status import app_status, with_timeout_event
from base.strings.auth import ServiceId
from base.strings.process import ProcessId, ProcessUri
from base.strings.scope import Workspace

from backend.domain.workspace import WorkspaceStore
from backend.models.exceptions import BadToolError, ProcessNotFoundError
from backend.models.process_history import ProcessProgress
from backend.models.workspace_action import (
    AnyWorkspaceRequest,
    AnyWorkspaceStream,
    WorkspaceChatbotSpawn,
    WorkspaceProcessSigkill,
    WorkspaceProcessSpawn,
    WorkspaceProcessUpdate,
    WorkspaceResponseProgress,
    WorkspaceResponseReply,
    WorkspaceStreamClose,
    WorkspaceStreamError,
    WorkspaceStreamValue,
)
from backend.server.context import (
    NdProcess,
    RequestContext,
    ProcessListener,
    ToolsProvider,
    WorkspaceContext,
)
from backend.services.client_reply import SvcClientReply
from backend.services.kv_store import KVLock, SvcKVStore
from backend.services.llm import SvcLlm
from backend.services.threads import SvcThreads
from backend.services.tools_backend import SvcBackendTools
from backend.tools.bot.chat import BotChat, ChatbotArguments

logger = logging.getLogger(__name__)

# fmt: off
KEY_WORKSPACE_LOCK = "workspace:lock:{workspace}"                           # KVLock
# fmt: on

LOCK_TIMEOUT_SECS = 120
LOCK_REFRESH_SECS = 60
POLL_INTERVAL_SECS = 10

# TODO:
# CONTEXT_TIMEOUT = timedelta(minutes=10)
# """
# After 10 minutes without any active processes, shutdown the workspace.
# """

RUNNING_WORKSPACES: dict[Workspace, WorkspaceServer] = {}


@dataclass(kw_only=True)
class WorkspaceServer:
    lock: KVLock
    context: WorkspaceContext
    _execution_task: asyncio.Task | None = None

    @staticmethod
    async def try_acquire(
        kv_store: SvcKVStore,
        workspace: Workspace,
    ) -> WorkspaceServer | None:
        if workspace in RUNNING_WORKSPACES:
            return RUNNING_WORKSPACES[workspace]

        key_lock = KEY_WORKSPACE_LOCK.format(workspace=workspace.as_kv_path())
        lock = await kv_store.acquire_lock(key_lock, LOCK_TIMEOUT_SECS)
        if not lock:
            logger.warning("Failed to acquire lock for workspace %s", str(workspace))
            return None

        logger.warning("Starting workspace server for %s", str(workspace))
        context = WorkspaceContext.new(workspace=workspace)
        server = WorkspaceServer(context=context, lock=lock)
        RUNNING_WORKSPACES[workspace] = server
        context = await server.initialize(kv_store)
        server._execution_task = asyncio.create_task(server._execution_loop())
        return RUNNING_WORKSPACES[workspace]

    async def initialize(self, kv_store: SvcKVStore) -> None:
        """
        TODO: Restart logic on unfinished SIGTERMed processes.
        """
        self.context.add_service(kv_store)
        self.context.add_service(SvcBackendTools())
        self.context.add_service(SvcLlm.initialize(self.context))
        self.context.add_service(
            SvcThreads.initialize(self.context.workspace, kv_store)
        )

    async def _execution_loop(self) -> None:
        """
        NOTE: The lock is acquired for
        """
        store = WorkspaceStore.new(self.context)
        try:
            last_refresh = time.time()
            while app_status() <= "ok":
                if time.time() - last_refresh > LOCK_REFRESH_SECS:
                    await self.lock.refresh()
                    last_refresh = time.time()

                # TODO:
                # last_update = max(p.updated_at for p in self.context.processes.values())
                # if datetime.now(UTC) - last_update > CONTEXT_TIMEOUT:
                #     break

                req_pair = await store.recv_request(self.context, POLL_INTERVAL_SECS)
                if not req_pair:
                    continue

                request, response = req_pair
                await self.on_remote_action(request, response)
        finally:
            logger.warning("Shutting down workspace: %s", str(self.context.workspace))
            await self.context.send_sigterm()
            await self.lock.release()

    async def on_remote_action(
        self,
        request: AnyWorkspaceRequest,
        response: asyncio.Queue[AnyWorkspaceStream],
    ) -> None:
        match request:
            # TODO:
            # case WorkspaceProcessCustom():
            #     pass
            case WorkspaceChatbotSpawn():
                await self.on_request_chatbot_spawn(request, response)
            case WorkspaceProcessSigkill():
                await self.on_request_process_sigkill(request, response)
            case WorkspaceProcessSpawn():
                await self.on_request_process_spawn(request, response)
            case WorkspaceProcessUpdate():
                await self.on_request_process_update(request, response)
            case _:
                error = ErrorInfo.new(
                    code=400,
                    message=f"Bad Request: unexpected variant {type(request).__name__}",
                )
                await response.put(WorkspaceStreamError(error=error))
                await response.put(WorkspaceStreamClose())

    async def on_request_chatbot_spawn(
        self,
        request: WorkspaceChatbotSpawn,
        response: asyncio.Queue[AnyWorkspaceStream],
    ) -> None:
        """
        NOTE: The API MUST have already checked the workspace permissions.
        """
        try:
            service_id = ServiceId.new("client-reply", None)
            svc_client_reply = SvcClientReply.initialize(
                context=self.context,
                service_id=service_id,
                tools=request.tools,
            )
            request_ctx = RequestContext(
                auth=request.request.auth,
                services=[svc_client_reply],
            )
            process_uri = ProcessUri.root(self.context.workspace, ProcessId.generate())
            process: NdProcess = BotChat(
                process_uri=process_uri,
                owner=service_id,
                arguments=ChatbotArguments(
                    bot_id=request.bot_id,
                    persona=request.persona,
                    thread_uris=request.threads,
                ),
            )
            await self.context.spawn(request_ctx, process)
            listener = self.context.listener(process_uri)
        except Exception as exc:
            error = ApiError.from_exception(exc).as_info()
            await response.put(WorkspaceStreamError(error=error))
            await response.put(WorkspaceStreamClose())
            return

        self.context.create_task(
            self._poll_spawned_chatbot(
                process,
                listener,
                svc_client_reply,
                response,
                request.recv_timeout,
            )
        )

    async def _poll_spawned_chatbot(
        self,
        process: NdProcess,
        listener: ProcessListener,
        svc_client_reply: SvcClientReply,
        response: asyncio.Queue[AnyWorkspaceStream],
        recv_timeout: int,
    ) -> None:
        try:
            while not process.result:
                await with_timeout_event(svc_client_reply.event_flush, recv_timeout)
                if listener.has_result.is_set():
                    break
                await response.put(
                    WorkspaceStreamValue(
                        value=WorkspaceResponseReply(
                            status="provisional",
                            summary=svc_client_reply.summary,
                            reply=[
                                *svc_client_reply.reply_committed,
                                *svc_client_reply.reply_provisional,
                            ],
                            actions=svc_client_reply.pull_actions(),
                        ),
                    )
                )

            await response.put(
                WorkspaceStreamValue(
                    value=WorkspaceResponseReply(
                        status="done",
                        summary=None,
                        reply=[*svc_client_reply.reply_committed],
                        actions=svc_client_reply.pull_actions(),
                    ),
                )
            )
        except Exception as exc:
            error = ApiError.from_exception(exc).as_info()
            await response.put(WorkspaceStreamError(error=error))
        finally:
            await response.put(WorkspaceStreamClose())

    async def on_request_process_sigkill(
        self,
        request: WorkspaceProcessSigkill,
        response: asyncio.Queue[AnyWorkspaceStream],
    ) -> None:
        try:
            await self.context.send_sigkill(request.process_uri)
        except Exception as exc:
            error = ApiError.from_exception(exc).as_info()
            await response.put(WorkspaceStreamError(error=error))
        finally:
            await response.put(WorkspaceStreamClose())

    async def on_request_process_spawn(
        self,
        request: WorkspaceProcessSpawn,
        response: asyncio.Queue[AnyWorkspaceStream],
    ) -> None:
        """
        NOTE: The API MUST have already checked the workspace permissions.
        """
        try:
            tool = next(
                (
                    tool
                    for provider in self.context.services_implementing(ToolsProvider)
                    for tool in provider.list_tools(self.context)
                    if tool.name == request.name
                ),
                None,
            )
            if not tool:
                raise BadToolError.not_found(request.name)

            process_uri = ProcessUri.root(self.context.workspace, request.process_id)
            process: NdProcess = await self.context.spawn_tool(
                request=RequestContext(auth=request.request.auth, services=[]),
                tool=tool,
                process_uri=process_uri,
                arguments=request.arguments,
            )
            listener = self.context.listener(process_uri)
        except Exception as exc:
            error = ApiError.from_exception(exc).as_info()
            await response.put(WorkspaceStreamError(error=error))
            await response.put(WorkspaceStreamClose())
            return

        self.context.create_task(
            self._poll_spawned_process(process, listener, response)
        )

    async def _poll_spawned_process(
        self,
        process: NdProcess,
        listener: ProcessListener,
        response: asyncio.Queue[AnyWorkspaceStream],
    ) -> None:
        try:
            while not process.result:
                if not await listener.wait_progress():
                    continue
                await response.put(
                    WorkspaceStreamValue(
                        value=WorkspaceResponseProgress(
                            process_uri=process.process_uri,
                            events=list(process.get_pending_events()),
                            history=process.history,
                            result=process.result.untyped() if process.result else None,
                        ),
                    )
                )
        except Exception as exc:
            error = ApiError.from_exception(exc).as_info()
            await response.put(WorkspaceStreamError(error=error))
        finally:
            await response.put(WorkspaceStreamClose())

    async def on_request_process_update(
        self,
        request: WorkspaceProcessUpdate,
        response: asyncio.Queue[AnyWorkspaceStream],
    ) -> None:
        """
        NOTE: The API should have already checked the workspace and process
        permissions, and validated the progress and result.
        """
        try:
            if not (process := self.context.processes.get(request.process_uri)):
                raise ProcessNotFoundError.from_uri(request.process_uri)

            await process._on_update(  # noqa: SLF001
                history=[*[ProcessProgress(progress=p) for p in request.progress]],
                result=request.result,
            )
            await self.context.send_actions(request.process_uri, request.actions)
        except Exception as exc:
            error = ApiError.from_exception(exc).as_info()
            await response.put(WorkspaceStreamError(error=error))
        finally:
            await response.put(WorkspaceStreamClose())
