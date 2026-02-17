import logging

from fastapi import APIRouter, Header
from pydantic import BaseModel
from typing import Annotated

from base.core.exceptions import ApiError
from base.server.auth import NdAuth
from base.strings.remote import RemoteServiceSecret
from base.strings.scope import Workspace

from backend.domain.workspace import WorkspaceStore, api_service_recv_action
from backend.models.api_client import ClientAction
from backend.server.context import ServiceConfig
from backend.services.kv_store import SvcKVStore

logger = logging.getLogger(__name__)
router = APIRouter(tags=["workspace"])


##
## Custom
##


# TODO:
# class ApiWorkspaceCustomRequest(BaseModel, frozen=True):
#     workspace: Workspace
#     tools: list[ToolDefinition]
#
# class ApiWorkspaceCustomResponse(BaseModel, frozen=True):
#     secret_key: RemoteProcessSecret
#
# @router.post("/api/workspace/custom")
# async def post_api_workspace_custom(
#     req: ApiWorkspaceCustomRequest,
#     authorization: Annotated[str | None, Header()] = None,
#     x_authorization_client: Annotated[str | None, Header()] = None,
#     x_authorization_user: Annotated[str | None, Header()] = None,
#     x_request_scope: Annotated[str | None, Header()] = None,
# ) -> ApiWorkspaceCustomResponse:
#     """
#     TODO: Check workspace permissions.
#     """
#     try:
#         auth = NdAuth.from_headers(
#             x_authorization_client=x_authorization_client,
#             x_authorization_user=x_authorization_user or authorization,
#             x_request_scope=x_request_scope,
#         )
#         if not auth.validated_user_id():
#             raise AuthorizationError.unauthorized("authorization required")
#
#         kv_store = await SvcKVStore.initialize()
#         client = WorkspaceStore(kv_store=kv_store, workspace=request.workspace)
#         secret_key = await client.custom_spawn()
#         return ApiWorkspaceCustomResponse(
#             secret_key=secret_key,
#         )
#         raise NotImplementedError("WorkspaceStore.custom_spawn")
#     except ApiError:
#         raise
#     except Exception as exc:
#         raise ApiError.from_exception(exc) from exc


##
## Service
##


class ApiWorkspaceSvcPullRequest(BaseModel, frozen=True):
    secret_key: RemoteServiceSecret
    timeout: int = 10


class ApiWorkspaceSvcPullResponse(BaseModel, frozen=True):
    action: ClientAction | None


@router.post("/api/workspace/svc-pull")
async def post_api_workspace_svc_pull(
    req: ApiWorkspaceSvcPullRequest,
) -> ApiWorkspaceSvcPullResponse:
    """
    TODO: Heartbeat.
    """
    try:
        kv_store = await SvcKVStore.initialize()
        action = await api_service_recv_action(kv_store, req.secret_key, req.timeout)
        return ApiWorkspaceSvcPullResponse(action=action)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc


class ApiWorkspaceSvcRegisterRequest(BaseModel, frozen=True):
    workspace: Workspace
    config: ServiceConfig


class ApiWorkspaceSvcRegisterResponse(BaseModel, frozen=True):
    secret_key: RemoteServiceSecret


@router.post("/api/workspace/svc-register")
async def post_api_workspace_svc_register(
    req: ApiWorkspaceSvcRegisterRequest,
    authorization: Annotated[str | None, Header()] = None,
    x_authorization_client: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
    x_request_scope: Annotated[str | None, Header()] = None,
) -> ApiWorkspaceSvcRegisterResponse:
    """
    TODO: Check workspace permissions.
    """
    try:
        _auth = NdAuth.from_headers(
            x_authorization_client=x_authorization_client,
            x_authorization_user=x_authorization_user or authorization,
            x_request_scope=x_request_scope,
        )
        store = WorkspaceStore(
            kv_store=await SvcKVStore.initialize(),
            workspace=req.workspace,
        )

        secret_key = await store.api_register_service(req.config)
        return ApiWorkspaceSvcRegisterResponse(secret_key=secret_key)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc
