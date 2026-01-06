import dotenv
import logging
import os

from segment import analytics

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
        self.verbose = bool(os.getenv("DEBUG_VERBOSE"))

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
    documents_host: str | None = os.getenv("NANDAM_DOCUMENTS_HOST") or None
    knowledge_host: str | None = os.getenv("NANDAM_KNOWLEDGE_HOST") or None


class AzureConfig:
    audience = os.getenv("MICROSOFT_AUDIENCE", "").split(",")
    client_id = os.getenv("MICROSOFT_CLIENT_ID", "")
    client_secret = os.getenv("MICROSOFT_CLIENT_SECRET", "")
    tenant_id = os.getenv("MICROSOFT_TENANT_ID", "")


class DebugConfig:
    auth_user_email = os.getenv("DEBUG_AUTH_USER_EMAIL")
    auth_user_id = os.getenv("DEBUG_AUTH_USER_ID")
    auth_user_name = os.getenv("DEBUG_AUTH_USER_NAME")
    storage_root = os.getenv("DEBUG_STORAGE_ROOT")


class LlmConfig:
    gateway_api_base: str = os.getenv("LLM_GATEWAY_API_BASE", "").removesuffix("/")
    gateway_api_key: str = os.getenv("LLM_GATEWAY_API_BASE", "")


class BaseConfig:
    # Kubernetes
    environment = os.getenv("ENVIRONMENT", "local")
    version = os.getenv("VERSION", "development")

    # Logging
    verbose = bool(os.getenv("DEBUG_VERBOSE"))  # any nonempty string
    log_level = logging.DEBUG if verbose else logging.INFO

    analytics = AnalyticsConfig()
    api = ApiConfig()
    azure = AzureConfig()
    debug = DebugConfig()
    llm = LlmConfig()

    @classmethod
    def is_kubernetes(cls) -> bool:
        return cls.environment in ("prod", "test", "dev")

    @classmethod
    def is_prod(cls) -> bool:
        return cls.environment in ("prod",)
