import logging
import os

from functools import cached_property
from pathlib import Path

from base.config import BaseConfig

logger = logging.getLogger(__name__)


class RedisConfig:
    host: str | None = os.getenv("REDIS_HOST")
    password: str | None = os.getenv("REDIS_PASSWORD")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    ssl_ca_certs = "/etc/ca-bundles/rds-combined-ca-bundle.pem"
    ssl_cert_reqs = "required"
    socket_connect_timeout = 10
    socket_timeout = 10

    @cached_property
    def ssl(self) -> bool:
        path = Path(self.ssl_ca_certs)
        has_certs = path.exists()

        if has_certs:
            logger.info("SSL certs found: using SSL for Redis connection.")
        else:
            logger.info("SSL certs not found: not using SSL for Redis connection.")

        return has_certs

    def client_config(self) -> dict:
        return {
            "host": self.host,
            "password": self.password,
            "port": self.port,
            "ssl": self.ssl,
            "ssl_ca_certs": self.ssl_ca_certs,
            "ssl_cert_reqs": self.ssl_cert_reqs,
            "socket_connect_timeout": self.socket_connect_timeout,
            "socket_timeout": self.socket_timeout,
        }


class TeamsAppConfig:
    app_id: str | None = os.getenv("TEAMS_APP_ID")
    client_id: str | None = os.getenv("MICROSOFT_APP_ID")
    client_secret: str | None = os.getenv("MICROSOFT_APP_PASSWORD")
    tenant_id: str | None = os.getenv("MICROSOFT_TENANT_ID")
    app_type: str | None = os.getenv("MICROSOFT_APP_TYPE", "MultiTenant")
    graph_endpoint: str | None = os.getenv(
        "GRAPH_ENDPOINT",
        "https://graph.microsoft.com/v1.0",
    )
    webhook_notification_url: str | None = os.getenv("WEBHOOK_NOTIFICATION_URL")
    webhook_client_state: str | None = os.getenv("WEBHOOK_CLIENT_STATE")
    feedback_webhook_url: str | None = os.getenv("TEAMS_FEEDBACK_WEBHOOK_URL")


class WebAppConfig:
    directory: str | None = os.getenv("WEBAPP_BUILD_DIR")


class WebServerConfig:
    port = int(os.getenv("PORT", "8000"))


class BackendConfig(BaseConfig):
    redis = RedisConfig()
    teams_app = TeamsAppConfig()
    web_app = WebAppConfig()
    web_server = WebServerConfig()
