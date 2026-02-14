import logging

from fastapi import APIRouter, Header
from typing import Annotated

from base.api.knowledge import (
    KnowledgeQueryRequest,
    KnowledgeQueryResponse,
    KnowledgeScanRequest,
    KnowledgeScanResponse,
)
from base.core.exceptions import ApiError
from base.resources.action import QueryAction, ResourcesLoadAction
from base.resources.bundle import Resource

from knowledge.domain.query import execute_query_all
from knowledge.domain.resolve import scan_resources
from knowledge.server.request import initialize_context

logger = logging.getLogger(__name__)
router = APIRouter(tags=["query"])


##
## Query API - Generic
##


@router.post("/v1/query")
async def post_v1_query(
    req: KnowledgeQueryRequest,
    x_authorization_client: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
    x_request_id: Annotated[str | None, Header()] = None,
) -> KnowledgeQueryResponse:
    try:
        context = await initialize_context(
            settings=req.settings,
            x_authorization_client=x_authorization_client,
            x_authorization_user=x_authorization_user,
            x_request_id=x_request_id,
        )
        pending = await execute_query_all(context, req.actions)
        results = pending.into_resources(context)
        return KnowledgeQueryResponse(results=results)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc


@router.post("/v1/scan")
async def post_v1_scan(
    req: KnowledgeScanRequest,
    x_authorization_client: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
    x_request_id: Annotated[str | None, Header()] = None,
) -> KnowledgeScanResponse:
    try:
        context = await initialize_context(
            settings=req.settings,
            x_authorization_client=x_authorization_client,
            x_authorization_user=x_authorization_user,
            x_request_id=x_request_id,
        )

        resource_uris = await scan_resources(context, [req.rules])
        actions: list[QueryAction] = [
            ResourcesLoadAction(uri=uri, load_mode=req.load_mode)
            for uri in resource_uris
        ]

        pending = await execute_query_all(context, actions)
        results = pending.into_resources(context)
        return KnowledgeScanResponse(
            resources=[r for r in results.resources if isinstance(r, Resource)],
        )
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc
