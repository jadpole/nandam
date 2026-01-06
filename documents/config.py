import logging
import os

from base.config import BaseConfig

logger = logging.getLogger(__name__)


class DatalabConfig:
    api_key: str | None = os.getenv("DOCUMENTS_DATALAB_API_KEY")


class DomainsConfig:
    confluence: list[str] = (
        ds.split(",") if (ds := os.getenv("DOCUMENTS_CONFLUENCE_DOMAINS")) else []
    )
    tableau: list[str] = (
        ds.split(",") if (ds := os.getenv("DOCUMENTS_TABLEAU_DOMAINS")) else []
    )


class ScrapflyConfig:
    api_key: str | None = os.getenv("DOCUMENTS_SCRAPFLY_API_KEY")
    disabled_domains: list[str] = (
        ds.split(",")
        if (ds := os.getenv("DOCUMENTS_SCRAPFLY_DISABLED_DOMAINS"))
        else []
    )
    disabled_suffixes: list[str] = (
        ds.split(",")
        if (ds := os.getenv("DOCUMENTS_SCRAPFLY_DISABLED_SUFFIXES"))
        else []
    )


class SSLConfig:
    disabled: list[str] = (
        ds.split(",") if (ds := os.getenv("DOCUMENTS_SSL_DISABLED_DOMAINS")) else []
    )
    legacy: list[str] = (
        ds.split(",") if (ds := os.getenv("DOCUMENTS_SSL_LEGACY_DOMAINS")) else []
    )


class WebServerConfig:
    port: int = int(os.getenv("PORT", "8010"))


class DocumentsConfig(BaseConfig):
    datalab = DatalabConfig()
    domains = DomainsConfig()
    scrapfly = ScrapflyConfig()
    ssl = SSLConfig()
    web_server = WebServerConfig()
