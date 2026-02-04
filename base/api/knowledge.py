from datetime import datetime
from pydantic import BaseModel, Field

from base.api.utils import post_request
from base.config import BaseConfig
from base.core.exceptions import ApiError
from base.core.strings import ValidatedStr
from base.core.unique_id import unique_id_from_datetime
from base.resources.action import QueryAction_
from base.resources.bundle import Resources
from base.resources.label import (
    AggregateDefinition,
    AggregateValue,
    LabelDefinition,
    LabelValue,
    ResourceFilters,
)
from base.strings.resource import Realm, ResourceUri


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
    filters: ResourceFilters = Field(default_factory=ResourceFilters)
    """
    Useful to omit private resources in Knowledge results that will be displayed
    in a public or semi-public scope when "creds" provide wide access.
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
        from knowledge.models.exceptions import KnowledgeError  # noqa: PLC0415
        from knowledge.routers.query import post_v1_query  # noqa: PLC0415

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


##
## Refresh
##


REFRESH_EXAMPLE_REALMS = [
    Realm.decode("confluence"),
    Realm.decode("microsoft-org"),
]


class KnowledgeRefreshId(ValidatedStr):
    @staticmethod
    def generate(timestamp: datetime | None = None) -> "KnowledgeRefreshId":
        return KnowledgeRefreshId(unique_id_from_datetime(timestamp, 32))

    @classmethod
    def _schema_regex(cls) -> str:
        return r"refresh-[a-z0-9]{32}"

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["refresh-9e7xc00123456789abcdef0123456789"]


class KnowledgeRefreshRequest(BaseModel, frozen=True):
    settings: KnowledgeSettings = Field(default_factory=KnowledgeSettings)
    realms: list[Realm] = Field(default_factory=list, examples=[REFRESH_EXAMPLE_REALMS])
    previous: dict[Realm, KnowledgeRefreshId] = Field(default_factory=dict)


class KnowledgeRefreshResponse(BaseModel, frozen=True):
    refresh_id: KnowledgeRefreshId
    uris: list[ResourceUri]


##
## Aggregate
##


class KnowledgeAggregateRequest(BaseModel, frozen=True):
    settings: KnowledgeSettings
    labels: list[LabelDefinition]
    aggregates: list[AggregateDefinition]


class KnowledgeAggregateResponse(BaseModel, frozen=True):
    labels: list[LabelValue]
    aggregates: list[AggregateValue]


async def knowledge_aggregate(
    req: KnowledgeAggregateRequest,
) -> KnowledgeAggregateResponse:
    if not BaseConfig.api.knowledge_host:
        from knowledge.models.exceptions import KnowledgeError  # noqa: PLC0415
        from knowledge.routers.tools import post_v1_tools_aggregate  # noqa: PLC0415

        try:
            return await post_v1_tools_aggregate(req=req)
        except KnowledgeError as exc:
            raise KnowledgeApiError.from_exception(exc) from exc

    return await post_request(
        endpoint=f"{BaseConfig.api.knowledge_host}/v1/tools/aggregate",
        payload=req,
        type_exc=KnowledgeApiError,
        type_resp=KnowledgeAggregateResponse,
        timeout_secs=580.0,  # 20 seconds under the nginx timeout (10 minutes).
    )
