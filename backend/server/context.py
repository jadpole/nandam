import asyncio
import jsonschema
import weakref

from collections.abc import Coroutine
from dataclasses import dataclass
from pydantic import BaseModel, PrivateAttr
from pydantic.json_schema import JsonSchemaValue
from typing import Any

from base.core.exceptions import BadRequestError, StoppedError
from base.models.content import ContentText
from base.models.context import NdContext
from base.models.rendered import Rendered
from base.resources.bundle import Resources
from base.resources.observation import Observation
from base.server.auth import NdAuth
from base.server.status import assert_is_alive, with_timeout
from base.strings.auth import RequestId
from base.strings.process import ProcessName, ProcessUri
from base.strings.scope import Workspace

from backend.models.api_client import RequestInfo
from backend.models.exceptions import (
    BadProcessError,
    BadToolError,
    ProcessNotFoundError,
)
from backend.models.process_status import ProcessResult, ProcessStatus, ProcessStopped
from backend.models.tool_info import ToolInfo
from backend.services.kv_store import EXP_MONTH, EXP_WEEK, SvcKVStore

# fmt: off
KEY_PROCESS_EXECUTOR = "process:executor:{process_uri}"     # NdProcess
KEY_PROCESS_STATUS = "process:status:{process_uri}"         # ProcessStatus
# fmt: on


##
## Workspace
##


@dataclass(kw_only=True)
class WorkspaceContext(NdContext):
    requests: dict[RequestId, "RequestContext"]
    resources: Resources
    workspace: Workspace

    @staticmethod
    def new(*, workspace: Workspace) -> "WorkspaceContext":
        return WorkspaceContext(
            caches=[],
            services=[],
            resources=Resources(),
            requests={},
            workspace=workspace,
        )

    async def on_sigterm(self) -> None:
        sigterm_tasks = [
            process.on_sigterm()
            for request in self.requests.values()
            for process in request.processes.values()
        ]
        await asyncio.gather(*sigterm_tasks, return_exceptions=True)


##
## Request
##


@dataclass(kw_only=True)
class ProcessListener:
    uri: ProcessUri
    has_progress: asyncio.Event
    has_result: asyncio.Event

    async def wait_progress(self, timeout: float | None = None) -> bool:
        await with_timeout(asyncio.create_task(self.has_progress.wait()), timeout)
        if self.has_progress.is_set():
            self.has_progress.clear()
            return True
        else:
            assert_is_alive()  # Raise StoppedError when received SIGTERM.
            return False

    async def wait_result(self) -> None:
        await with_timeout(asyncio.create_task(self.has_result.wait()))
        if not self.has_result.is_set():
            raise StoppedError.timeout()  # Received SIGTERM.


@dataclass(kw_only=True)
class RequestContext:
    workspace: WorkspaceContext
    auth: NdAuth
    process_listeners: dict[ProcessUri, list[ProcessListener]]
    """
    Listeners for the progress and result of processes spawned by the request.
    """
    process_statuses: dict[ProcessUri, ProcessStatus]
    """
    The statuses of all processes spawned by the request.
    """
    processes: dict[ProcessUri, "NdProcess"]
    """
    The executors of all processes spawned by the request.
    """
    tasks: set[asyncio.Task]
    """
    The background tasks spawned by the request, typically executing processes.
    """

    @staticmethod
    def create(
        workspace: WorkspaceContext,
        request_info: RequestInfo,
    ) -> "RequestContext":
        if request_info.auth.scope != workspace.workspace.scope:
            raise BadRequestError.new("request scope incompatible with workspace")
        if request_info.auth.request_id in workspace.requests:
            raise BadRequestError.new("request already exists in workspace")

        context = RequestContext(
            workspace=weakref.proxy(workspace),
            auth=request_info.auth,
            process_listeners={},
            process_statuses={},
            processes={},
            tasks=set(),
        )
        workspace.requests[request_info.auth.request_id] = context
        return context

    async def create_task[R](self, coro: Coroutine[Any, Any, R]) -> asyncio.Task[R]:
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    ##
    ## Process Status
    ##

    def listener(self, process_uri: ProcessUri) -> ProcessListener:
        if not (process := self.process_statuses.get(process_uri)):
            raise ProcessNotFoundError.from_uri(process_uri)

        new_listener = ProcessListener(
            uri=process_uri,
            has_progress=asyncio.Event(),
            has_result=asyncio.Event(),
        )
        if process.result:
            new_listener.has_result.set()

        self.process_listeners.setdefault(process_uri, []).append(new_listener)
        return new_listener

    def get_status(self, process_uri: ProcessUri) -> ProcessStatus:
        if status := self.try_get_status(process_uri):
            return status
        else:
            raise ProcessNotFoundError.from_uri(process_uri)

    def try_get_status(self, process_uri: ProcessUri) -> ProcessStatus | None:
        if status := self.process_statuses.get(process_uri):
            return status.model_copy(deep=True)
        else:
            return None

    async def refresh_status(self, process_uri: ProcessUri) -> ProcessStatus | None:
        kv_store = self.workspace.service(SvcKVStore)
        key_status = KEY_PROCESS_STATUS.format(process_uri=process_uri.as_kv_path())
        status = await kv_store.get(key_status, ProcessStatus)
        if status:
            self.set_status(status)
        return status

    def set_status(self, status: ProcessStatus) -> None:
        if status == self.try_get_status(status.process_uri):
            return  # Nothing changed, so do not notify listeners.

        self.process_statuses[status.process_uri] = status.model_copy(deep=True)

        if not (listeners := self.process_listeners.get(status.process_uri)):
            return
        for listener in listeners:
            listener.has_progress.set()
        if status.result:
            for listener in listeners:
                listener.has_result.set()


##
## Process
##


class NdProcess(BaseModel):
    """
    NOTE: Subclases of `NdProcess` must define a field:

    ```
    kind: Literal["kind"] = "kind"
    ```

    Where "kind" is unique for the subclass and will be used to deserialize the
    process into the correct variant.  This allows different kinds of processes
    to have definitions.

    TODO: Decode into subclasses based on `kind`.

    TODO: Methods `on_sigterm` and `on_restart` that runs when the Backend is
    restarted by k8s, allowing long-running processes to gracefully shutdown or
    continue running after the restart.
    """

    _request: RequestContext = PrivateAttr()
    process_uri: ProcessUri
    name: ProcessName

    ##
    ## Interface - Internal
    ##

    def get_arguments(self) -> dict[str, Any]:
        raise NotImplementedError("Subclasses must implement NdProcess.get_arguments")

    def get_arguments_schema(self) -> JsonSchemaValue | None:
        return None

    async def on_spawn(self) -> None:
        raise NotImplementedError("Subclasses must implement NdProcess.on_spawn")

    async def on_restart(self) -> None:
        raise BadRequestError.new(f"{type(self).__name__}.on_restart not supported")

    async def on_sigterm(self) -> None:
        """
        By default, when the Backend restarts, stop the ongoing process.
        If you want to support `on_restart`, then you must override this method
        to store the information necessary to restart.
        """
        result = ProcessStopped(reason="stopped")
        await self._send_update([], result)

    async def spawn(self) -> None:
        # Initialize the process, then run it in the background.
        await self._spawn_before_task()
        await self._request.create_task(self.on_spawn())

    async def _spawn_before_task(self) -> None:
        kv_store = self._request.workspace.service(SvcKVStore)
        if await self._request.refresh_status(self.process_uri):
            raise BadProcessError.duplicate(self.process_uri)

        # Do not store the process definition or status on invalid arguments.
        # NOTE: Some processes, e.g., those with a Pydantic model for arguments,
        # may not require a second validation.
        arguments = self.get_arguments()
        if arguments_schema := self.get_arguments_schema():
            try:
                jsonschema.validate(arguments, arguments_schema)
            except Exception as exc:
                raise BadToolError.bad_arguments(self.name, str(exc)) from exc

        # Store the process definition, to allow interacting with it from within
        # other replicas, e.g., in API calls or to restart a crashed process.
        process_part = self.process_uri.as_kv_path()
        key_executor = KEY_PROCESS_EXECUTOR.format(process_uri=process_part)
        await kv_store.set_one(key_executor, self, ex=EXP_WEEK)

        # Store the initial status of the process.
        status = ProcessStatus.new(
            request_id=self._request.auth.request_id,
            process_uri=self.process_uri,
            name=self.name,
            arguments=arguments,
        )
        await self._save_status(status)

    ##
    ## Interface - Internal
    ##

    async def send_progress(self, progress: Any) -> None:
        progress = progress if isinstance(progress, list) else [progress]
        progress = [p.model_dump() if isinstance(p, BaseModel) else p for p in progress]
        await self._send_update(progress=progress, result=None)

    ##
    ## Helpers - LLM
    ##

    def render_content(self, text: ContentText) -> Rendered:
        """
        Render the content, expanding embeds recursively using the observations
        that are already cached in the workspace.
        """
        observations = [
            obs
            for obs in self._request.workspace.resources.observations
            if isinstance(obs, Observation)
        ]
        return Rendered.render(text, observations)

    def llm_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {
            "x-georges-user-id": self._request.auth.tracking_user_id(),
            "x-georges-task-id": str(self._request.auth.request_id),
            "x-georges-task-type": self.llm_task_type(),
        }
        if value := self.llm_task_entity_id():
            headers["x-georges-task-entity-id"] = str(value)
        return headers

    def llm_task_entity_id(self) -> str | None:
        return None

    def llm_task_type(self) -> str:
        return str(self.name)

    ##
    ## Helpers - Status
    ##

    async def _save_status(self, status: ProcessStatus) -> None:
        kv_store = self._request.workspace.service(SvcKVStore)
        if status == self._request.try_get_status(status.process_uri):
            return  # Nothing changed, so do not notify listeners.

        process_part = status.process_uri.as_kv_path()
        key_status = KEY_PROCESS_STATUS.format(process_uri=process_part)
        await kv_store.set_one(key_status, status, ex=EXP_MONTH)

        self._request.set_status(status)

    async def _send_update(
        self,
        progress: list[dict[str, Any]],
        result: ProcessResult | None,
    ) -> ProcessStatus:
        status = self._request.get_status(self.process_uri)
        status.update_mut(progress, result)
        await self._save_status(status)
        return status


##
## Tool
##


@dataclass(kw_only=True)
class NdTool:
    """
    NOTE: Subclases of `NdProcess` must define a field:

    ```
    kind: Literal["kind"] = "kind"
    ```

    Where "kind" is unique for the subclass and will be used to deserialize the
    process into the correct variant.  This allows different kinds of processes
    to have definitions.

    TODO: Decode into subclasses based on `kind`.
    """

    def info(self, context: RequestContext) -> ToolInfo | None:
        """
        Return information about the tool, used for LLMs and schema validation.
        NOTE: Returns `None` when the tool is not available.
        """
        raise NotImplementedError("Subclasses must implement NdTool.info")

    async def spawn(
        self,
        context: RequestContext,
        process_uri: ProcessUri,
        arguments: dict[str, Any],
    ) -> NdProcess:
        raise NotImplementedError("Subclasses must implement NdTool.spawn")


##
## Providers
##


class ToolsProvider:
    def list_tools(self, context: RequestContext) -> list[NdTool]:
        raise NotImplementedError("Subclasses must implement ToolsProvider.list_tools")
