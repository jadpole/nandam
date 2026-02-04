import logging

from fastapi import APIRouter, Header
from typing import Annotated

from base.api.knowledge import KnowledgeAggregateRequest, KnowledgeAggregateResponse
from base.core.exceptions import ApiError

from knowledge.domain.aggregates import generate_labels_and_aggregates
from knowledge.server.request import initialize_context

logger = logging.getLogger(__name__)
router = APIRouter(tags=["tools"])


##
## Query API - Aggregate
##


@router.post("/v1/tools/aggregate")
async def post_v1_tools_aggregate(
    req: KnowledgeAggregateRequest,
    x_authorization_client: Annotated[str | None, Header()] = None,
    x_authorization_user: Annotated[str | None, Header()] = None,
    x_request_id: Annotated[str | None, Header()] = None,
) -> KnowledgeAggregateResponse:
    try:
        context = await initialize_context(
            settings=req.settings,
            x_authorization_client=x_authorization_client,
            x_authorization_user=x_authorization_user,
            x_request_id=x_request_id,
        )

        _, labels, aggregates = await generate_labels_and_aggregates(
            context=context,
            req_labels=req.labels,
            req_aggregates=req.aggregates,
        )

        return KnowledgeAggregateResponse(labels=labels, aggregates=aggregates)
    except ApiError:
        raise
    except Exception as exc:
        raise ApiError.from_exception(exc) from exc
