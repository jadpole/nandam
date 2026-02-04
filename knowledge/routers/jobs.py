import logging

from fastapi import APIRouter, Header
from typing import Annotated

from base.api.knowledge import KnowledgeRefreshRequest, KnowledgeRefreshResponse
from base.core.exceptions import ApiError

from knowledge.domain.refresh import execute_refresh
from knowledge.server.request import initialize_context

logger = logging.getLogger(__name__)
router = APIRouter(tags=["jobs"])


##
## Query API - Generic
##


@router.post("/v1/jobs/refresh")
async def post_v1_jobs_refresh(
    req: KnowledgeRefreshRequest,
    x_authorization_client: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
    x_request_id: Annotated[str | None, Header()] = None,
) -> KnowledgeRefreshResponse:
    try:
        context = await initialize_context(
            settings=req.settings,
            x_authorization_client=x_authorization_client,
            x_authorization_user=x_authorization_user,
            x_request_id=x_request_id,
        )
        results = await execute_refresh(context, req.realms, req.previous)
        return KnowledgeRefreshResponse(
            refresh_id=context.refresh_id,
            uris=results,
        )
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc
