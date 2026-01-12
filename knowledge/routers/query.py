"""
Endpoints used exclusively by Sonata Backend.
As we deploy more clients, we will explode these endpoints into clearer modules.
"""

import logging
from typing import Annotated

from fastapi import APIRouter, Header

from base.api.knowledge import KnowledgeQueryRequest
from base.core.exceptions import ApiError
from base.resources.bundle import Resources

from knowledge.domain.query import execute_query_all
from knowledge.server.request import initialize_context

logger = logging.getLogger(__name__)
router = APIRouter(tags=["query"])


##
## Query API - Generic
##


@router.post("/v1/query")
async def post_v1_query(
    req: KnowledgeQueryRequest,
    authorization: Annotated[str | None, Header()] = None,
    x_authorization: Annotated[str | None, Header()] = None,
) -> Resources:
    try:
        context = await initialize_context(
            authorization=x_authorization or authorization,
            settings=req.settings,
        )
        return await execute_query_all(context, req.actions)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc
