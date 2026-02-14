import asyncio
import contextlib
import jsonschema
import logging
import weakref

from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import datetime, UTC
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    SerializeAsAny,
    TypeAdapter,
)
from pydantic_core import PydanticUndefined
from pydantic.json_schema import JsonSchemaValue
from typing import Any, Generic, Self, TypeVar, Union

from base.core.exceptions import ApiError, BadRequestError, ErrorInfo
from base.core.unions import ModelUnionMut
from base.core.values import as_value
from base.models.context import NdContext
from base.resources.observation import Observation
from base.server.auth import NdAuth
from base.server.status import with_timeout
from base.strings.auth import ServiceId
from base.strings.process import ProcessId, ProcessName, ProcessUri
from base.strings.scope import Workspace

from backend.models.exceptions import (
    BadProcessError,
    BadToolError,
    ProcessNotFoundError,
)
from backend.models.process_history import ProcessHistoryItem_, ProcessProgress
from backend.models.process_result import (
    ProcessError,
    ProcessFailure,
    ProcessResult,
    ProcessResult_,
    ProcessStopped,
    ProcessSuccess,
)
from backend.models.tool_info import ToolDefinition, ToolInfo, ToolMode
from backend.server.resources import CacheResources
from backend.services.kv_store import EXP_MONTH, SvcKVStore

logger = logging.getLogger(__name__)

# fmt: off
KEY_PROCESS_INFO = "process:info:{process_uri}"             # NdProcess
# fmt: on

RUNNING_WORKSPACES: dict[Workspace, WorkspaceContext] = {}


##
## Workspace
##


@dataclass(kw_only=True)
class WorkspaceRequest:
    auth: NdAuth


@dataclass(kw_only=True)
class ProcessListener:
    uri: ProcessUri
    has_progress: asyncio.Event
    has_result: asyncio.Event

    def notify(self, with_result: bool) -> None:
        self.has_progress.set()
        if with_result:
            self.has_result.set()

    async def wait_progress(self, timeout: float | None = None) -> bool:
        await with_timeout(asyncio.create_task(self.has_progress.wait()), timeout)
        recent_progress = self.has_progress.is_set()
        self.has_progress.clear()
        return recent_progress

    async def wait_result(self, timeout: float | None = None) -> bool:
        await with_timeout(asyncio.create_task(self.has_result.wait()), timeout)
        return self.has_result.is_set()


@dataclass(kw_only=True)
class WorkspaceContext(NdContext):
    process_listeners: dict[ProcessUri, list[ProcessListener]]
    """
    Listeners for the progress and result of processes spawned by the request.
    """
    processes: dict[ProcessUri, NdProcess]
    """
    The executors of all processes spawned by the request.
    """
    requests: dict[ProcessUri, WorkspaceRequest]
    """
    The settings of each request by the Process URI that spawned it.
    """
    tasks: set[asyncio.Task]
    """
    The background tasks spawned by the request, typically executing processes.
    """
    workspace: Workspace

    @staticmethod
    def new(*, workspace: Workspace) -> WorkspaceContext:
        return WorkspaceContext(
            caches=[],
            services=[],
            process_listeners={},
            processes={},
            requests={},
            tasks=set(),
            workspace=workspace,
        )

    ##
    ## Signals
    ##

    async def on_sigterm(self) -> None:
        sigterm_tasks = [process.on_sigterm() for process in self.processes.values()]
        await asyncio.gather(*sigterm_tasks, return_exceptions=True)

    ##
    ## Tasks
    ##

    def create_task[R](self, coro: Coroutine[Any, Any, R]) -> asyncio.Task[R]:
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _on_task_done(self, task: asyncio.Task) -> None:
        self.tasks.discard(task)
        try:
            task.result()
        except Exception:
            logger.exception("Workspace %s task failed", self.workspace)

    ##
    ## Process
    ##

    def get_request(self, process_uri: ProcessUri) -> WorkspaceRequest | None:
        while True:
            if request := self.requests.get(process_uri):
                return request
            if not process_uri.parent_ids:
                return None

            *next_parent_ids, next_process_id = process_uri.parent_ids
            process_uri = ProcessUri(
                workspace=process_uri.workspace,
                parent_ids=next_parent_ids,
                process_id=next_process_id,
            )

    def listener(self, process_uri: ProcessUri) -> ProcessListener:
        if not (process := self.processes.get(process_uri)):
            raise ProcessNotFoundError.from_uri(process_uri)

        new_listener = ProcessListener(
            uri=process_uri,
            has_progress=asyncio.Event(),
            has_result=asyncio.Event(),
        )
        self.process_listeners.setdefault(process_uri, []).append(new_listener)

        if process.result:
            new_listener.has_result.set()

        return new_listener

    async def spawn_tool[P: NdProcess](
        self,
        tool: NdTool[P],
        process_uri: ProcessUri,
        arguments: Any,
    ) -> P:
        process = tool._build(process_uri=process_uri, arguments=arguments)  # noqa: SLF001
        await self.spawn(process)
        return process

    async def spawn(self, process: NdProcess) -> None:
        await self._prepare_spawn(process)
        self.create_task(process.on_spawn())

    async def restart(self, process: NdProcess) -> None:
        await self._prepare_spawn(process)
        self.create_task(process.on_restart())

    async def _prepare_spawn(self, process: NdProcess) -> None:
        if process.process_uri.workspace != self.workspace:
            raise BadRequestError.new("process URI incompatible with workspace")
        if process.process_uri in self.processes:
            raise BadRequestError.new("process URI already exists in workspace")
        if process.result:
            raise BadRequestError.new("process already has a result")

        process._ctx = weakref.proxy(self)  # noqa: SLF001
        self.processes[process.process_uri] = process
        await process.on_save()


##
## Process
##


_ALREADY_WARNED: list[str] = []


@dataclass(kw_only=True)
class ProcessInfo:
    description: str | None = None
    human_name: str | None = None
    human_description: str | None = None
    fluent_icon_name: str | None = None
    mui_icon_name: str | None = None


class NdProcess[Arg: BaseModel = Any, Ret: BaseModel = Any](ModelUnionMut):
    """
    NOTE: Subclasses of `NdProcess` must define a field:

    ```
    kind: Literal["kind"] = "kind"
    ```

    Where "kind" is unique for the subclass and will be used to deserialize the
    process into the correct variant.  This allows different kinds of processes
    to have definitions.

    NOTE: Subclasses of `NdProcess` must

    TODO: Methods `on_sigterm` and `on_restart` that runs when the Backend is
    restarted by k8s, allowing long-running processes to gracefully shutdown or
    continue running after the restart.
    """

    model_config = ConfigDict(extra="allow")

    _ctx: WorkspaceContext = PrivateAttr(default=None)  # type: ignore

    process_uri: ProcessUri = Field(frozen=True)
    """
    The unique ID of the process and of the corresponding Context.
    """
    owner: ServiceId = Field(frozen=True)
    """
    The service ID that owns the process.
    """
    name: ProcessName = Field(frozen=True)
    """
    The machine name of the process, i.e., of the corresponding agent or tool.
    """
    mode: ToolMode = Field(frozen=True)
    """
    The category of process, for tracking errors.
    """

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC), frozen=True)
    """
    The time at which the process was spawned, though not necessarily when it
    started running.
    """
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    """
    The time of the last heartbeat, progress, or when the result was generated.
    Used to decide whether to set `result` to `ProcessExpired`: when an active
    process had no heartbeats for at least 10 minutes.
    """

    arguments: SerializeAsAny[Arg] = Field(frozen=True)
    """
    The arguments that were passed to the process.
    """
    history: list[ProcessHistoryItem_] = Field(default_factory=list)
    """
    A view on the internal state of the process for debugging and auditing.
    Changes as the process runs, sometimes destructively.
    """
    result: ProcessResult_[Ret] | None = None
    """
    The final output of the process.  Assigned once, when it completes.
    When `result is None`, the process is still pending / ongoing.
    """

    ##
    ## Tool
    ##

    @classmethod
    def tool(
        cls,
        *,
        context: NdProcess | WorkspaceContext | None = None,
        owner: ServiceId | None = None,
        name: ProcessName | None = None,
        mode: ToolMode | None = None,
        description: str | None = None,
        human_name: str | None = None,
        human_description: str | None = None,
        fluent_icon_name: str | None = None,
        mui_icon_name: str | None = None,
    ) -> NdTool[Self] | None:
        info = cls._info()
        return NdTool(
            process=cls,
            owner=cls._make_tool_field_req("owner", owner),
            name=cls._make_tool_field_req("name", name),
            mode=cls._make_tool_field_req("mode", mode),
            description=description or info.description or "",
            arguments_schema=cls._arguments_schema(context),
            progress_schema=cls._progress_schema(context),
            return_schema=cls._return_schema(context),
            human_name=human_name or info.human_name,
            human_description=human_description or info.human_description,
            fluent_icon_name=fluent_icon_name or info.fluent_icon_name,
            mui_icon_name=mui_icon_name or info.mui_icon_name,
        )

    @classmethod
    def _info(cls) -> ProcessInfo:
        return ProcessInfo()

    @classmethod
    def _arguments_schema(
        cls,
        context: NdProcess | WorkspaceContext | None,
    ) -> JsonSchemaValue:
        type_arg = cls.arguments_type()
        if isinstance(type_arg, type) and issubclass(type_arg, BaseModel):
            return type_arg.model_json_schema()
        else:
            return {"type": "object"}

    @classmethod
    def _progress_schema(
        cls,
        context: NdProcess | WorkspaceContext | None,
    ) -> JsonSchemaValue:
        if progress_types := cls.progress_types():
            return TypeAdapter(Union[*progress_types]).json_schema()
        else:
            return {"type": "object"}

    @classmethod
    def _return_schema(
        cls,
        context: NdProcess | WorkspaceContext | None,
    ) -> JsonSchemaValue:
        type_ret = cls.arguments_type()
        if isinstance(type_ret, type) and issubclass(type_ret, BaseModel):
            return type_ret.model_json_schema()
        else:
            return {"type": "object"}

    @classmethod
    def _get_default(cls, field_name: str) -> Any | None:
        field = cls.model_fields.get(field_name)
        if not field:
            return None
        if field.default == PydanticUndefined:
            return None
        return field.default

    @classmethod
    def _make_tool_field[T](cls, field_name: str, override: T | None) -> T | None:
        default_value = cls._get_default(field_name)
        if (
            override is not None
            and default_value is not None
            and override != default_value
        ):
            raise BadToolError.bad_info(
                cls,
                f"incompatible {cls.__name__} field {field_name}: {default_value} -> {override}",
            )
        elif override is not None:
            return override
        else:
            return default_value

    @classmethod
    def _make_tool_field_req[T](cls, field_name: str, override: T | None) -> T:
        value = cls._make_tool_field(field_name, override)
        if value is None:
            raise BadToolError.bad_info(
                cls, f"missing {cls.__name__} field {field_name}"
            )
        return value

    ##
    ## Internals - Generics
    ##

    @classmethod
    def _generics(cls) -> tuple[type[Arg] | None, type[Ret] | None]:
        cursor = cls
        while cursor:
            cursor_name = cursor.__name__
            if cursor_name != "NdProcess" and not cursor_name.startswith("NdProcess["):
                cursor = cursor.__base__
                continue

            types = cursor.__pydantic_generic_metadata__["args"]
            if len(types) == 0:
                return None, None
            elif len(types) == 1:
                return types[0], None
            else:
                return types[0], types[1]

        raise ValueError("Cannot find NdProcess generics")

    @classmethod
    def arguments_type(cls) -> type[Arg] | None:
        return cls._generics()[0]

    @classmethod
    def progress_types(cls) -> tuple[type[BaseModel], ...]:
        return ()

    @classmethod
    def return_type(cls) -> type[Ret] | None:
        return cls._generics()[1]

    def _validate_result(
        self,
        result: dict[str, Any] | ErrorInfo | Exception | ProcessResult | Ret,
        *,
        name: ProcessName,
    ) -> ProcessSuccess[Ret] | ProcessError:
        # Error results:
        if isinstance(result, ErrorInfo):
            return ProcessFailure.from_info(result)
        if isinstance(result, Exception):
            return ProcessFailure.from_exception(result)
        if isinstance(result, ProcessError):
            return result

        value: dict[str, Any] | Ret = (
            result.value if isinstance(result, ProcessSuccess) else result
        )
        return ProcessSuccess(value=self._validate_return(value, name=name))

    def _validate_return(
        self,
        result: dict[str, Any] | Ret,
        *,
        name: ProcessName,
    ) -> Ret:
        """
        Given a Ret parameter, validate using Pydantic.
        Otherwise, validate using JSON-Schema and return a `dict[str, Any]`.
        """
        try:
            if type_ret := self.return_type():
                if isinstance(result, type_ret):
                    return result
                else:
                    return type_ret.model_validate(result)
            else:
                value = as_value(result)
                jsonschema.validate(value, self._return_schema(self.ctx()))
                return value
        except BadToolError:
            raise
        except Exception as exc:
            raise BadToolError.bad_return(name, str(exc)) from exc

    ##
    ## Internals - Proxy
    ##

    def ctx(self) -> WorkspaceContext:
        if not self._ctx:
            raise BadProcessError.invalid_context(self.process_uri, "no context")
        return self._ctx

    def _request(self) -> WorkspaceRequest:
        if request := self.ctx().get_request(self.process_uri):
            return request
        else:
            raise BadProcessError.invalid_context(self.process_uri, "no request")

    ##
    ## Signals
    ##

    async def on_save(self) -> None:
        ctx = self.ctx()
        kv_store = ctx.service(SvcKVStore)

        self.updated_at = datetime.now(UTC)
        key_process_info = KEY_PROCESS_INFO.format(
            process_uri=self.process_uri.as_kv_path()
        )
        await kv_store.set_one(key_process_info, self, ex=EXP_MONTH)

        if listeners := ctx.process_listeners.get(self.process_uri):
            for listener in listeners:
                listener.notify(self.result is not None)

    async def on_spawn(self) -> None:
        with contextlib.suppress(Exception):
            await self._invoke()

    async def on_restart(self) -> None:
        raise BadRequestError.new(f"{type(self).__name__}.on_restart not supported")

    async def on_sigkill(self) -> None:
        """
        Sent when the process is killed by the client.
        Can be overridden to stop gracefully.
        """
        self.result = ProcessStopped(reason="stopped")
        await self.on_save()

    async def on_sigterm(self) -> None:
        """
        By default, kill the ongoing process when the Backend restarts.
        NOTE: If you want to support `on_restart`, then you must override this
        method to store the information necessary to restart.
        """
        await self.on_sigkill()

    ##
    ## State
    ##

    async def wait(self) -> ProcessResult_[Ret]:
        listener = self.ctx().listener(self.process_uri)
        await listener.wait_result()
        return self.result or ProcessStopped(reason="timeout")  # Server restarted.

    async def spawn_child(
        self,
        tool: NdTool,
        process_id: ProcessId,
        arguments: dict[str, Any],
    ) -> NdProcess:
        process = tool.build_child(self, process_id, arguments)
        await self._ctx.spawn(process)
        return process

    async def _on_progress(self, progress: Any) -> None:
        progress = progress if isinstance(progress, list) else [progress]
        await self._on_update(
            history=[ProcessProgress(progress=as_value(p)) for p in progress],
        )

    async def _on_update(
        self,
        history: list[ProcessHistoryItem_] | None = None,
        result: ProcessResult | Exception | Ret | dict[str, Any] | None = None,
    ) -> None:
        if self.result:
            raise BadProcessError.update_after_result(self.process_uri)
        if history:
            self.history.extend(history)
        if result:
            self.result = self._validate_result(result, name=self.name)
        await self.on_save()

    ##
    ## Invoke
    ##

    async def _invoke(self) -> Ret:
        """
        Utility method to invoke the tool with the given (typed) arguments and
        await the (typed) result, which can be used in workflows.
        """
        try:
            return_value = await self._run()
            self.result = ProcessSuccess(value=return_value)
            await self.on_save()
            return return_value
        except Exception as exc:
            self.result = ProcessFailure.from_exception(exc)
            await self.on_save()
            raise

    async def _run(self) -> Ret:
        raise NotImplementedError("Subclasses must implement NdProcess._run")

    ##
    ## Helpers - LLM
    ##

    def cached_observations(self) -> list[Observation]:
        return [
            obs
            for obs in self.ctx().cached(CacheResources).resources.observations
            if isinstance(obs, Observation)
        ]

    def llm_headers(self) -> dict[str, str]:
        auth = self._request().auth
        headers: dict[str, str] = {
            "x-georges-user-id": auth.tracking_user_id(),
            "x-georges-task-id": str(auth.request_id),
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
## Tool
##


P = TypeVar("P", covariant=True, bound=NdProcess)  # noqa: PLC0105


@dataclass(kw_only=True)
class NdTool(Generic[P]):  # noqa: UP046
    """
    A tool is a "process factory".
    """

    process: type[P]

    owner: ServiceId
    name: ProcessName
    mode: ToolMode
    description: str
    arguments_schema: JsonSchemaValue
    progress_schema: JsonSchemaValue
    return_schema: JsonSchemaValue
    human_name: str | None = None
    human_description: str | None = None
    fluent_icon_name: str | None = None
    mui_icon_name: str | None = None

    def info(self) -> ToolInfo:
        return ToolInfo.new(
            owner=self.owner,
            mode=self.mode,
            definition=ToolDefinition(
                name=self.name,
                description=self.description,
                arguments_schema=self.arguments_schema,
                progress_schema=self.progress_schema,
                return_schema=self.return_schema,
                human_name=self.human_name,
                human_description=self.human_description,
                fluent_icon_name=self.fluent_icon_name,
                mui_icon_name=self.mui_icon_name,
            ),
        )

    def build_root(
        self,
        context: WorkspaceContext,
        process_id: ProcessId,
        arguments: Any,
    ) -> P:
        try:
            return self._build(
                process_uri=ProcessUri.root(context.workspace, process_id),
                arguments=arguments,
            )
        except ApiError:
            raise
        except Exception as exc:
            raise BadToolError.bad_arguments(self.name, str(exc)) from exc

    def build_child(
        self,
        parent: NdProcess,
        process_id: ProcessId,
        arguments: Any,
    ) -> P:
        try:
            return self._build(
                process_uri=parent.process_uri.child(process_id),
                arguments=arguments,
            )
        except ApiError:
            raise
        except Exception as exc:
            raise BadToolError.bad_arguments(self.name, str(exc)) from exc

    def _build(
        self,
        *,
        process_uri: ProcessUri,
        arguments: Any,
    ) -> P:
        return self.process(
            process_uri=process_uri,
            owner=self.owner,
            name=self.name,
            mode=self.mode,
            arguments=self._validate_arguments(arguments),
        )

    def _validate_arguments(self, arguments: Any) -> Any:
        """
        Given an Arg parameter, validate using Pydantic.
        Otherwise, validate using JSON-Schema and return a `dict[str, Any]`.
        """
        try:
            if type_arg := self.process.arguments_type():
                if isinstance(arguments, type_arg):
                    return arguments
                else:
                    return type_arg.model_validate(arguments)
            else:
                arguments = as_value(arguments)
                jsonschema.validate(arguments, self.arguments_schema)
                return arguments  # type: ignore
        except Exception as exc:
            raise BadToolError.bad_arguments(self.name, str(exc)) from exc
