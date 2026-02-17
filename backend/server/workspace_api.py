from collections.abc import AsyncIterable

from backend.domain.workspace import WorkspaceStore
from backend.models.workspace_action import AnyWorkspaceRequest, AnyWorkspaceResponse
from backend.server.workspace_server import WorkspaceServer
from backend.services.kv_store import SvcKVStore


async def api_send_request(
    kv_store: SvcKVStore,
    request: AnyWorkspaceRequest,
    recv_timeout: int = 1,
) -> AsyncIterable[AnyWorkspaceResponse]:
    workspace = request.get_workspace()
    store = WorkspaceStore(kv_store=kv_store, workspace=workspace)

    # If no worker exists for the workspace, start it.
    await WorkspaceServer.try_acquire(kv_store, workspace)

    async for resp in store.api_send_request(request, recv_timeout):
        yield resp
