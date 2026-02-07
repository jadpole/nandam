import logging
import weakref

from functools import cache
from pydantic import BaseModel, Field
from typing import Annotated

from base.api.knowledge import KnowledgeSettings
from base.core.values import parse_yaml_as
from base.server.auth import NdAuth

from knowledge.config import KnowledgeConfig
from knowledge.connectors.confluence import ConfluenceConnectorConfig
from knowledge.connectors.georges import GeorgesConnectorConfig
from knowledge.connectors.github import GitHubConnectorConfig
from knowledge.connectors.gitlab import GitLabConnectorConfig
from knowledge.connectors.jira import JiraConnectorConfig
from knowledge.connectors.microsoft_my import MicrosoftMyConnectorConfig
from knowledge.connectors.microsoft_org import MicrosoftOrgConnectorConfig
from knowledge.connectors.public import PublicConnector
from knowledge.connectors.qatestrail import QATestRailConnectorConfig
from knowledge.connectors.tableau import TableauConnectorConfig
from knowledge.connectors.web import WebConnector
from knowledge.server.context import Connector, KnowledgeContext
from knowledge.services.downloader import SvcDownloader
from knowledge.services.inference import SvcInference
from knowledge.services.storage import SvcStorage

logger = logging.getLogger(__name__)


async def initialize_context(
    *,
    settings: KnowledgeSettings | None = None,
    x_authorization_client: str | None = None,
    x_authorization_user: str | None = None,
    x_request_id: str | None = None,
    x_request_scope: str | None = None,
) -> KnowledgeContext:
    auth = NdAuth.from_headers(
        x_authorization_client=x_authorization_client,
        x_authorization_user=x_authorization_user,
        x_request_id=x_request_id,
        x_request_scope=x_request_scope,
    )
    settings = settings or KnowledgeSettings()
    context = KnowledgeContext.new(
        auth=auth,
        request_timestamp=None,
        settings=settings,
    )

    # Services
    context.add_service(SvcDownloader.initialize(context))
    context.add_service(SvcInference.initialize(context))
    context.add_service(SvcStorage.initialize())

    # Connectors
    for connector in initialize_connectors(context):
        context.add_connector(connector)

    return context


def initialize_connectors(context: KnowledgeContext) -> list[Connector]:
    connectors: list[Connector] = [
        connector
        for connector_config in read_connectors_config().connectors
        if (connector := connector_config.instantiate(context))
    ]
    connectors.append(PublicConnector(context=weakref.proxy(context)))
    connectors.append(WebConnector(context=weakref.proxy(context)))
    return connectors


##
## Configuration
##


AnyConnectorConfig = (
    ConfluenceConnectorConfig
    | GeorgesConnectorConfig
    | GitHubConnectorConfig
    | GitLabConnectorConfig
    | JiraConnectorConfig
    | MicrosoftMyConnectorConfig
    | MicrosoftOrgConnectorConfig
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
