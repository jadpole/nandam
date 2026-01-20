from typing import Any, Literal

from base.resources.bundle import ObservationError, Resource, ResourceError
from base.resources.observation import Observation
from base.server.auth import NdAuth
from base.strings.process import ProcessName, ProcessUri
from base.strings.scope import Workspace

from backend.models.api_client import RequestInfo
from backend.models.process_status import ProcessResult_, ProcessSuccess
from backend.server.context import NdProcess, RequestContext, WorkspaceContext
from backend.services.kv_store import SvcKVStoreMemory


class StubProcess(NdProcess):
    kind: Literal["stub"] = "stub"
    arguments: dict[str, Any]
    progress: list[dict[str, Any]]
    result: ProcessResult_

    def get_arguments(self) -> dict[str, Any]:
        return self.arguments

    async def on_spawn(self) -> None:
        for progress in self.progress:
            await self._send_update([progress], None)
        await self._send_update([], self.result)


def given_context(
    kvstore_items: dict[str, Any] | None = None,
    observations: list[Observation | ObservationError] | None = None,
    resources: list[Resource | ResourceError] | None = None,
) -> tuple[WorkspaceContext, RequestContext]:
    """
    TODO: Use observations.
    """
    if kvstore_items is None:
        kvstore_items = {}

    workspace = Workspace.stub_internal()
    ctx_workspace = WorkspaceContext.new(
        workspace=workspace,
    )
    ctx_workspace.add_service(SvcKVStoreMemory(items=kvstore_items))

    ctx_request = RequestContext.create(
        workspace=ctx_workspace,
        request_info=RequestInfo(
            auth=NdAuth.stub(),
            workspace=workspace,
            workspace_name="Unit Test",
            thread=None,
        ),
    )

    if resources or observations:
        ctx_workspace.resources.update(resources=resources, observations=observations)

    return ctx_workspace, ctx_request


def given_headless_process(
    name: str = "stub_process",
    arguments: dict[str, Any] | None = None,
    progress: list[dict[str, Any]] | None = None,
    result: ProcessResult_ | None = None,
    kvstore_items: dict[str, Any] | None = None,
    observations: list[Observation | ObservationError] | None = None,
    resources: list[Resource | ResourceError] | None = None,
) -> NdProcess:
    ctx_workspace, ctx_request = given_context(
        kvstore_items=kvstore_items,
        observations=observations,
        resources=resources,
    )
    ctx_request.workspace = ctx_workspace  # NOTE: Do not use weakref.proxy()

    process = StubProcess(
        process_uri=ProcessUri.stub(),
        name=ProcessName.decode(name),
        arguments=arguments or {"example": "argument"},
        progress=progress or [],
        result=result or ProcessSuccess(value={"example": "value"}),
    )
    process._request = ctx_request  # NOTE: Do not use weakref.proxy()
    return process
