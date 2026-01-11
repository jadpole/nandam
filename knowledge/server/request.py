import logging

from functools import cache
from pydantic import BaseModel, Field
from typing import Annotated

from base.api.knowledge import KnowledgeSettings
from base.core.values import parse_yaml_as
from base.strings.auth import AuthKeycloak, RequestId

from knowledge.config import KnowledgeConfig
from knowledge.connectors.confluence import ConfluenceConnectorConfig
from knowledge.connectors.github import GitHubConnectorConfig
from knowledge.connectors.jira import JiraConnectorConfig
from knowledge.connectors.public import PublicConnector
from knowledge.connectors.qatestrail import QATestRailConnectorConfig
from knowledge.connectors.tableau import TableauConnectorConfig
from knowledge.connectors.web import WebConnector
from knowledge.models.context import KnowledgeContext
from knowledge.services.downloader import SvcDownloader
from knowledge.services.storage import SvcStorage

# TODO: from knowledge.services.inference import SvcInference

logger = logging.getLogger(__name__)


async def initialize_context(
    *,
    authorization: AuthKeycloak | str | None = None,
    request_id: RequestId | None = None,
    settings: KnowledgeSettings | None = None,
) -> KnowledgeContext:
    auth = (
        authorization
        if authorization and isinstance(authorization, AuthKeycloak)
        else AuthKeycloak.from_header(authorization)
    )
    settings = settings or KnowledgeSettings()
    context = KnowledgeContext.new(
        auth=auth,
        settings=settings,
        request_id=request_id,
        request_timestamp=None,
    )

    # Services
    context.add_service(SvcDownloader.initialize(context))
    # TODO: context.add_service(SvcInference.initialize(context))
    context.add_service(SvcStorage.initialize())

    # Connectors
    for connector_config in read_connectors_config().connectors:
        context.add_connector(connector_config.instantiate(context))
    # TODO: TempConnector(context=context)
    context.connectors.append(PublicConnector(context=context))
    context.connectors.append(WebConnector(context=context))

    return context


##
## Configuration
##


AnyConnectorConfig = (
    ConfluenceConnectorConfig
    | GitHubConnectorConfig
    | JiraConnectorConfig
    | QATestRailConnectorConfig
    | TableauConnectorConfig
)


class ConnectorsConfig(BaseModel):
    connectors: list[Annotated[AnyConnectorConfig, Field(discriminator="kind")]]

    @staticmethod
    def read() -> "ConnectorsConfig":
        return parse_yaml_as(
            ConnectorsConfig, KnowledgeConfig.cfg_path("connectors.yml").read_text()
        )


@cache
def read_connectors_config() -> ConnectorsConfig:
    try:
        return parse_yaml_as(
            ConnectorsConfig,
            KnowledgeConfig.cfg_path("connectors.yml").read_text(),
        )
    except Exception:
        logger.exception("Failed to read config: connectors.yml")
        return ConnectorsConfig(connectors=[])
