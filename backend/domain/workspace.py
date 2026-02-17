import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import datetime
from typing import Self

from pydantic import BaseModel

from backend.models.api_client import ClientAction
from base.core.exceptions import ApiError, ServiceError
from base.core.strings import ValidatedStr
from base.core.unique_id import unique_id_from_datetime
from base.strings.auth import ServiceId
from base.strings.process import ProcessUri
from base.strings.remote import RemoteProcessSecret, RemoteServiceSecret
from base.strings.scope import Workspace

from backend.models.api_workspace import ProcessStatus
from backend.models.exceptions import ProcessNotFoundError
from backend.models.workspace_action import (
    AnyWorkspaceRequest,
    AnyWorkspaceRequest_,
    AnyWorkspaceResponse,
    AnyWorkspaceStream,
    AnyWorkspaceStream_,
    WorkspaceStreamClose,
    WorkspaceStreamError,
    WorkspaceStreamValue,
)
from backend.models.workspace_state import RegisteredProcess, RegisteredService
from backend.server.context import (
    KEY_PROCESS_STATE,
    NdProcess,
    ServiceConfig,
    WorkspaceContext,
)
from backend.services.kv_store import EXP_TEN_MINUTES, EXP_WORKDAY, SvcKVStore

# fmt: off
KEY_MAPPING_PROCESS = "remote:bysecret:process:{secret}"                # RegisteredProcess
KEY_MAPPING_SERVICE = "remote:bysecret:service:{secret}"                # tuple[Workspace, ServiceId]
KEY_REMOTE_SERVICE = "remote:{workspace}:service:{service_id}"          # RegisteredService
KEY_REMOTE_SERVICES = "remote:{workspace}:service"                      # SET of ServiceId
KEY_SERVICE_ACTIONS = "workspace:{workspace}:actions:{service_id}"      # LIST of ClientAction
KEY_WORKSPACE_REQUEST = "workspace:{workspace}:request"                 # LIST of WrappedWorkspaceRequest
KEY_WORKSPACE_RESPONSE = "workspace:{workspace}:response:{channel_id}"  # LIST of WorkspaceResponse
KEY_WORKSPACE_BOTS = "workspace:{workspace}:bot"                        # LIST of WorkspaceResponse
KEY_WORKSPACE_BOT = "workspace:{workspace}:bot:{bot_id}"                # LIST of BotState
# fmt: on

NUM_CHARS_WORKSPACE_CHANNEL_ID = 36
REGEX_WORKSPACE_CHANNEL_ID = r"wch-[a-z0-9]{36}"


class WorkspaceChannelId(ValidatedStr):
    @classmethod
    def generate(cls, timestamp: datetime | None = None) -> Self:
        suffix = unique_id_from_datetime(timestamp, NUM_CHARS_WORKSPACE_CHANNEL_ID)
        return cls(f"wch-{suffix}")

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["wch-9e7xc00123456789abcdef0123456789abcd"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_WORKSPACE_CHANNEL_ID


class WrappedWorkspaceRequest(BaseModel, frozen=True):
    channel_id: WorkspaceChannelId
    request: AnyWorkspaceRequest_


@dataclass(kw_only=True)
class WorkspaceStore:
    kv_store: SvcKVStore
    workspace: Workspace

    @staticmethod
    def new(context: NdProcess | WorkspaceContext) -> WorkspaceStore:
        if isinstance(context, NdProcess):
            context = context.ctx()
        return WorkspaceStore(
            kv_store=context.service(SvcKVStore),
            workspace=context.workspace,
        )

    ##
    ## Service
    ## TODO: api_poll_service
    ##

    async def api_register_service(self, config: ServiceConfig) -> RemoteServiceSecret:
        """
        TODO: Send message to create service, await result with ID?
        """
        try:
            service_id = config.service_id()
            key_service_info = KEY_REMOTE_SERVICE.format(
                workspace=self.workspace.as_kv_path(),
                service_id=str(service_id),
            )
            if await self.kv_store.exists(key_service_info):
                raise ServiceError.duplicate(
                    str(service_id), type(config), type(config)
                )

            secret_key = RemoteServiceSecret.generate()
            registered = RegisteredService(
                workspace=self.workspace,
                config=config,
                secret_key=secret_key,
            )

            key_mapping = KEY_MAPPING_SERVICE.format(secret=str(secret_key))
            key_services = KEY_REMOTE_SERVICES.format(
                workspace=self.workspace.as_kv_path()
            )

            mapping = (self.workspace, service_id)
            await self.kv_store.sadd(key_services, str(service_id))
            await self.kv_store.set_one(key_mapping, mapping, ex=EXP_WORKDAY)
            await self.kv_store.set_one(key_service_info, registered, ex=EXP_WORKDAY)

            return secret_key
        except ApiError:
            raise
        except Exception as exc:
            raise ServiceError.bad_config(type(config), str(exc)) from exc

    async def resolve_service(self, service_id: ServiceId) -> RegisteredService:
        key_service_info = KEY_REMOTE_SERVICE.format(
            workspace=self.workspace.as_kv_path(),
            service_id=str(service_id),
        )
        registered = await self.kv_store.get(key_service_info, RegisteredService)
        if not registered:
            raise ServiceError.remote()
        return registered

    async def send_action(
        self,
        service_id: ServiceId,
        action: ClientAction,
    ) -> None:
        key_service_actions = KEY_SERVICE_ACTIONS.format(
            workspace=self.workspace.as_kv_path(),
            service_id=str(service_id),
        )
        await self.kv_store.rpush(key_service_actions, action, ex=EXP_WORKDAY)

    async def recv_action(
        self,
        service_id: ServiceId,
        timeout: int,
    ) -> ClientAction | None:
        key_service_actions = KEY_SERVICE_ACTIONS.format(
            workspace=self.workspace.as_kv_path(),
            service_id=str(service_id),
        )
        return await self.kv_store.blpop(
            key_service_actions,
            ClientAction,
            timeout=timeout,
        )

    ##
    ## Process
    ##

    async def api_process_status(self, process_uri: ProcessUri) -> ProcessStatus:
        key_process_state = KEY_PROCESS_STATE.format(
            process_uri=process_uri.as_kv_path(),
        )
        process = await self.kv_store.get(key_process_state, NdProcess)
        if not process:
            raise ProcessNotFoundError.from_uri(process_uri)

        return ProcessStatus(
            process_uri=process.process_uri,
            name=process.name,
            created_at=process.created_at,
            updated_at=process.updated_at,
            history=process.history,
            result=process.result,
        )

    async def register_process(self, process: NdProcess) -> RemoteProcessSecret:
        secret_key = RemoteProcessSecret.generate()
        key_remote_process_by_key = KEY_MAPPING_PROCESS.format(secret=str(secret_key))
        info = RegisteredProcess(
            process_uri=process.process_uri,
            secret_key=secret_key,
            name=process.name,
            created_at=process.created_at,
            arguments_schema=process._arguments_schema(process),  # noqa: SLF001
            progress_schema=process._progress_schema(process),  # noqa: SLF001
            return_schema=process._return_schema(process),  # noqa: SLF001
        )
        await self.kv_store.set_one(key_remote_process_by_key, info, ex=EXP_WORKDAY)
        return secret_key

    ##
    ## Message
    ##

    async def api_send_request(
        self,
        request: AnyWorkspaceRequest,
        recv_timeout: int = 1,
    ) -> AsyncGenerator[AnyWorkspaceResponse]:
        channel_id = WorkspaceChannelId.generate()
        wrapped = WrappedWorkspaceRequest(channel_id=channel_id, request=request)

        key_request = KEY_WORKSPACE_REQUEST.format(
            workspace=self.workspace.as_kv_path(),
        )
        key_response = KEY_WORKSPACE_RESPONSE.format(
            workspace=self.workspace.as_kv_path(),
            channel_id=str(channel_id),
        )

        await self.kv_store.lpush(key_request, wrapped, ex=EXP_TEN_MINUTES)

        resp: AnyWorkspaceStream
        while True:
            resp = await self.kv_store.blpop(
                key_response,
                AnyWorkspaceStream_,  # type: ignore
                timeout=recv_timeout,
            )
            match resp:
                case None:
                    continue
                case WorkspaceStreamClose():
                    break
                case WorkspaceStreamError():
                    raise ApiError.from_info(resp.error)
                case WorkspaceStreamValue():
                    yield resp.value

    async def recv_request(
        self,
        context: WorkspaceContext,
        recv_timeout: int = 10,
    ) -> tuple[AnyWorkspaceRequest, asyncio.Queue[AnyWorkspaceStream]] | None:
        assert context.workspace == self.workspace

        key_request = KEY_WORKSPACE_REQUEST.format(
            workspace=self.workspace.as_kv_path(),
        )
        wrapped = await self.kv_store.blpop(
            key_request,
            WrappedWorkspaceRequest,  # type: ignore
            timeout=recv_timeout,
        )
        if not wrapped:
            return None

        queue = asyncio.Queue()
        context.create_task(self._send_responses(wrapped.channel_id, queue))
        return wrapped.request, queue

    async def _send_responses(
        self,
        channel_id: WorkspaceChannelId,
        queue: asyncio.Queue[AnyWorkspaceResponse],
    ) -> None:
        key_response = KEY_WORKSPACE_RESPONSE.format(
            workspace=self.workspace.as_kv_path(),
            channel_id=str(channel_id),
        )
        while True:
            resp = await queue.get()
            await self.kv_store.lpush(key_response, resp, ex=EXP_TEN_MINUTES)
            if resp.kind == "close":
                break


async def _api_resolve_service_id(
    kv_store: SvcKVStore,
    secret: RemoteServiceSecret,
) -> tuple[Workspace, ServiceId]:
    key_mapping = KEY_MAPPING_SERVICE.format(secret=str(secret))
    mapping = await kv_store.get(key_mapping, tuple[Workspace, ServiceId])
    if not mapping:
        raise ServiceError.remote()
    return mapping


async def api_resolve_service(
    kv_store: SvcKVStore,
    secret: RemoteServiceSecret,
) -> RegisteredService:
    workspace, service_id = await _api_resolve_service_id(kv_store, secret)
    store = WorkspaceStore(kv_store=kv_store, workspace=workspace)
    return await store.resolve_service(service_id)


async def api_service_recv_action(
    kv_store: SvcKVStore,
    secret: RemoteServiceSecret,
    timeout: int,
) -> ClientAction | None:
    workspace, service_id = await _api_resolve_service_id(kv_store, secret)
    store = WorkspaceStore(kv_store=kv_store, workspace=workspace)
    return await store.recv_action(service_id, timeout)


async def api_resolve_process(
    kv_store: SvcKVStore,
    secret: RemoteProcessSecret,
) -> RegisteredProcess:
    key = KEY_MAPPING_PROCESS.format(secret=str(secret))
    info = await kv_store.get(key, RegisteredProcess)
    if not info:
        raise ProcessNotFoundError.remote()
    return info
