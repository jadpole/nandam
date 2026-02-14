import dotenv
import logging
import os

from pathlib import Path
from segment import analytics
from typing import Any

logger = logging.getLogger(__name__)

dotenv.load_dotenv(override=True)


IMAGE_TOKENS_ESTIMATE = 1600
"""
A rough estimate of how many tokens are contained in an image.

NOTE: Use the maximum as a conservative estimate to avoid context overflows,
based on the following data points:

- 765 tokens for a 1024x1024 image on GPT-4;
- 1120 tokens for a media_resolution_high image on Gemini 3.
- 1600 tokens for a 1092x1092 image on Claude 3.
"""

TEST_INTEGRATION = bool(os.getenv("TEST_INTEGRATION"))
"""
- In Bash: TEST_INTEGRATION=true pytest
- In Powershell: $env:TEST_INTEGRATION=true; pytest
"""

TEST_LLM = bool(os.getenv("TEST_LLM"))
"""
- In Bash: TEST_LLM=true pytest
- In Powershell: $env:TEST_LLM=true; pytest
"""


class AnalyticsConfig:
    enabled: bool
    verbose: bool

    def __init__(self) -> None:
        host = os.getenv("SEGMENT_HOST") or None
        write_key = os.getenv("SEGMENT_WRITE_KEY", "")
        self.enabled = bool(host and write_key and not os.getenv("SEGMENT_DISABLE"))
        self.verbose = int(os.getenv("DEBUG_VERBOSE") or "0") > 0

        analytics.host = host
        analytics.write_key = write_key
        analytics.on_error = lambda error, _: logger.warning(
            "Error sending analytics: %s", error
        )
        if self.verbose:
            analytics.debug = True
        if not self.enabled:
            analytics.send = False


class ApiConfig:
    alerts_host: str | None = os.getenv("NANDAM_ALERTS_HOST") or None
    backend_host: str | None = os.getenv("NANDAM_BACKEND_HOST") or None
    documents_host: str | None = os.getenv("NANDAM_DOCUMENTS_HOST") or None
    knowledge_host: str | None = os.getenv("NANDAM_KNOWLEDGE_HOST") or None


class AuthConfig:
    internal_secret: str | None = os.getenv("NANDAM_AUTH_INTERNAL_SECRET") or None
    """
    A 64-char hexadecimal string used to sign internal JWTs.  When missing,
    - In Kubernetes, disables internal JWTs.
    - In local development, encodes the payload as base64 without encryption.
    """
    keycloak_audience: list[str] = (
        aud.split(",")
        if (aud := os.getenv("NANDAM_AUTH_KEYCLOAK_AUDIENCE", ""))
        else []
    )
    """
    A comma-separated list of audiences (Azure Subscription IDs, UUID format)
    used in Keycloak JWTs.  When missing, disables Keycloak authentication.
    """
    keycloak_tenant_id = os.getenv("NANDAM_AUTH_KEYCLOAK_TENANT_ID") or None
    """
    The Microsoft Azure tenant ID used to validate Keycloak JWTs.
    """


class DebugConfig:
    auth_user_email = os.getenv("DEBUG_AUTH_USER_EMAIL")
    auth_user_id = os.getenv("DEBUG_AUTH_USER_ID")
    auth_user_name = os.getenv("DEBUG_AUTH_USER_NAME")
    storage_root = os.getenv("DEBUG_STORAGE_ROOT")


class LlmConfig:
    anthropic_api_key = os.getenv("LLM_ANTHROPIC_API_KEY") or None
    cerebras_api_key = os.getenv("LLM_CEREBRAS_API_KEY") or None
    gemini_api_key = os.getenv("LLM_GEMINI_API_KEY") or None
    openai_api_key = os.getenv("LLM_OPENAI_API_KEY") or None
    router_api_base = os.getenv("LLM_ROUTER_API_BASE") or None
    router_api_key = os.getenv("LLM_ROUTER_API_KEY") or None


class BaseConfig:
    # Kubernetes
    cfg_root: Path = Path(os.getenv("CONFIG_PATH", "config")).resolve()
    environment = os.getenv("ENVIRONMENT", "local")
    version = os.getenv("VERSION", "development")

    # Logging
    verbose = max(0, min(4, int(os.getenv("DEBUG_VERBOSE") or "0")))
    """
    - When empty or <= 0, verbose logs are disabled.
    - When >= 1, enable debugging logs.
    - When >= 2, also enable logging of LLM completions.
    - When >= 3, also enable logging of LLM payloads.
    - When >= 4, also enable logging of LLM system and history.
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    analytics = AnalyticsConfig()
    api = ApiConfig()
    auth = AuthConfig()
    debug = DebugConfig()
    llm = LlmConfig()

    extra: dict[str, str] = {}

    @classmethod
    def is_kubernetes(cls) -> bool:
        return cls.environment in ("prod", "test", "dev")

    @classmethod
    def is_prod(cls) -> bool:
        return cls.environment in ("prod",)  # noqa: FURB171

    @classmethod
    def get(cls, key: Any) -> str:
        if not key or not isinstance(key, str):
            return ""
        if key not in cls.extra:
            cls.extra[key] = os.getenv(key, "")
        return cls.extra[key]

    @classmethod
    def cfg_path(cls, relative_path: str) -> Path:
        return cls.cfg_root / relative_path
