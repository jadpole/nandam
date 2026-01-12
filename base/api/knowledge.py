from pydantic import BaseModel, Field
from typing import Literal

from base.api.utils import post_request
from base.config import BaseConfig
from base.core.exceptions import ApiError
from base.resources.action import QueryAction_
from base.resources.bundle import Resources


class KnowledgeApiError(ApiError):
    pass


class KnowledgeSettings(BaseModel, frozen=True):
    creds: dict[str, str] = Field(
        default_factory=dict,
        examples=[{"gitlab": "gplat-xxxxxxxxxxxxxxxxxxxx"}],
    )
    """
    Credentials used by the Knowledge connectors, overriding the defaults.
    The key is the Realm of the connector and the value is the Authorization.
    """
    prefix_rules: list[tuple[str, Literal["allow", "block"]]] = Field(
        default_factory=list,
        examples=[
            ("ndk://microsoft/", "block"),
            ("ndk://microsoft/sharepoint-MsSiteId/", "allow"),
        ],
    )
    """
    In "block" mode, "ndk://" prefixes for which `resolve` should fail.

    Useful when called in a public or semi-public scope where we must omit the
    URIs that might leak confidential information from the results.
    """


##
## Query
##


class KnowledgeQueryRequest(BaseModel, frozen=True):
    settings: KnowledgeSettings = Field(default_factory=KnowledgeSettings)
    """
    The settings for the current Query request.
    """
    actions: list[QueryAction_]
    """
    The actions that should be performed in the current Query request.
    """


async def knowledge_query(req: KnowledgeQueryRequest) -> Resources:
    if not BaseConfig.api.knowledge_host:
        from knowledge.models.exceptions import KnowledgeError
        from knowledge.routers.query import post_v1_query

        try:
            return await post_v1_query(req=req)
        except KnowledgeError as exc:
            raise KnowledgeApiError.from_exception(exc) from exc

    return await post_request(
        endpoint=f"{BaseConfig.api.knowledge_host}/v1/query",
        payload=req,
        type_exc=KnowledgeApiError,
        type_resp=Resources,
        timeout_secs=580.0,  # 20 seconds under the nginx timeout (10 minutes).
    )
