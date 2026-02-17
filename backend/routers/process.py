import logging
import jsonschema

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field
from typing import Annotated, Any

from base.core.exceptions import ApiError, AuthorizationError
from base.server.auth import NdAuth
from base.strings.process import ProcessUri
from base.strings.remote import RemoteProcessSecret
from base.strings.scope import ScopeInternal

from backend.domain.workspace import WorkspaceStore, api_resolve_process
from backend.models.api_system import ActionResponse
from backend.models.api_workspace import ProcessStatus
from backend.models.exceptions import BadToolError
from backend.models.process_history import AnyProcessAction_
from backend.models.process_result import ProcessResult_, ProcessSuccess
from backend.models.workspace_action import WorkspaceProcessUpdate
from backend.server.workspace_api import api_send_request
from backend.services.kv_store import SvcKVStore

logger = logging.getLogger(__name__)
router = APIRouter(tags=["process"])


##
## Status
##


class ApiProcessStatusRequest(BaseModel, frozen=True):
    process_uri: ProcessUri


class ApiProcessStatusResponse(BaseModel, frozen=True):
    summary: ProcessStatus


@router.post("/api/process/status")
async def post_api_process_status(
    request: ApiProcessStatusRequest,
    authorization: Annotated[str | None, Header()] = None,
    x_authorization_client: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
    x_request_scope: Annotated[str | None, Header()] = None,
) -> ApiProcessStatusResponse:
    """
    TODO: Check workspace permissions.
    """
    try:
        auth = NdAuth.from_headers(
            x_authorization_client=x_authorization_client,
            x_authorization_user=x_authorization_user or authorization,
            x_request_scope=x_request_scope,
        )
        if (
            not isinstance(request.process_uri.workspace.scope, ScopeInternal)
            and request.process_uri.workspace.scope != auth.scope
        ):
            raise AuthorizationError.forbidden("process in incorrect scope")

        store = WorkspaceStore(
            kv_store=await SvcKVStore.initialize(),
            workspace=request.process_uri.workspace,
        )
        return ApiProcessStatusResponse(
            summary=await store.api_process_status(request.process_uri),
        )
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc


##
## Update
##


class ApiProcessUpdateRequest(BaseModel, frozen=True):
    secret_key: RemoteProcessSecret
    actions: list[AnyProcessAction_] = Field(default_factory=list)
    progress: list[dict[str, Any]] = Field(default_factory=list)
    result: ProcessResult_[Any] | None = None


@router.post("/api/process/update")
async def post_api_process_update(req: ApiProcessUpdateRequest) -> ActionResponse:
    try:
        kv_store = await SvcKVStore.initialize()
        process = await api_resolve_process(kv_store, req.secret_key)

        if process.progress_schema:
            for progress in req.progress:
                try:
                    jsonschema.validate(progress, process.progress_schema)
                except Exception as exc:
                    raise BadToolError.bad_progress(process.name, str(exc)) from exc

        if (
            process.return_schema
            and req.result
            and isinstance(req.result, ProcessSuccess)
        ):
            try:
                jsonschema.validate(req.result.value, process.return_schema)
            except Exception as exc:
                raise BadToolError.bad_return(process.name, str(exc)) from exc

        kv_store = await SvcKVStore.initialize()
        request = WorkspaceProcessUpdate(
            process_uri=process.process_uri,
            actions=req.actions,
            progress=req.progress,
            result=req.result,
        )
        async for _ in api_send_request(kv_store, request):
            pass  # Only expecting "close" or "error".

        return ActionResponse(success=True)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc
