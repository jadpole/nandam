import logging

from typing import Literal

from base.core.values import as_json

from backend.llm.cerebras import LlmCerebras
from backend.llm.gemini import LlmGemini
from backend.llm.litellm import LlmLite
from backend.llm.model import LlmModel
from backend.llm.model_info import (
    CLAUDE_BLOB_TYPES,
    GEMINI_BLOB_TYPES,
    OPENAI_BLOB_TYPES,
)
from backend.models.exceptions import BadPersonaError

logger = logging.getLogger(__name__)


##
## Anthropic
##


ANTHROPIC_CLAUDE_HAIKU = LlmLite(
    name="claude-haiku",
    status="stable",
    description="Reasoning: 2/5, Speed: 5/5, Conversations length: 3/5",
    color="#ad552f",
    native_name="anthropic/claude-haiku-4-5-20251001",
    knowledge_cutoff="February 2025",
    supports_media=CLAUDE_BLOB_TYPES,
    supports_stop=True,
    supports_think="anthropic",
    supports_tools="openai",
    limit_tokens_total=180_000,  # actual limit ~ 200k
    limit_tokens_response=64_000,
    limit_tokens_recent=100_000,
    limit_media=20,  # actual limit unknown
    reasoning_effort="medium",
)

ANTHROPIC_CLAUDE_OPUS = LlmLite(
    name="claude-opus",
    status="stable",
    description="Reasoning: 5/5, Speed: 2/5, Conversations length: 3/5",
    color="#ad552f",
    native_name="anthropic/claude-opus-4-5-20251101",
    knowledge_cutoff="March 2025",
    supports_media=CLAUDE_BLOB_TYPES,
    supports_stop=True,
    supports_think="anthropic",
    supports_tools="openai",
    limit_tokens_total=180_000,  # actual limit ~ 200k
    limit_tokens_response=64_000,
    limit_tokens_recent=100_000,
    limit_media=20,  # actual limit unknown
    reasoning_effort="medium",
)

ANTHROPIC_CLAUDE_SONNET = LlmLite(
    name="claude-sonnet",
    status="stable",
    description="Reasoning: 4/5, Speed: 3/5, Conversations length: 3/5",
    color="#ad552f",
    native_name="anthropic/claude-sonnet-4-5-20250929",
    knowledge_cutoff="January 2025",
    supports_media=CLAUDE_BLOB_TYPES,
    supports_stop=True,
    supports_think="anthropic",
    supports_tools="openai",
    limit_tokens_total=180_000,  # actual limit ~ 200k
    limit_tokens_response=64_000,
    limit_tokens_recent=100_000,
    limit_media=20,  # actual limit unknown
    reasoning_effort="medium",
)


##
## Cerebras
##


CEREBRAS_GPT_OSS = LlmCerebras(
    name="gpt-oss",
    status="experimental",
    description="Reasoning: 2/5, Speed: 5/5, Conversations length: 3/5",
    color="#1fb8cd",
    native_name="gpt-oss-120b",
    knowledge_cutoff="May 2024",
    supports_media=[],
    supports_stop=True,
    supports_think="gpt-oss",
    supports_tools="openai",
    limit_tokens_total=128_000,
    limit_tokens_response=40_000,
    limit_tokens_recent=60_000,
    reasoning_effort="high",
)

CEREBRAS_GPT_OSS_FAST = LlmCerebras(
    name="gpt-oss-fast",
    status="experimental",
    description="Reasoning: 2/5, Speed: 5/5, Conversations length: 3/5",
    color="#1fb8cd",
    native_name="gpt-oss-120b",
    knowledge_cutoff="May 2024",
    supports_media=[],
    supports_stop=True,
    supports_think="gpt-oss",
    supports_tools="openai",
    limit_tokens_total=128_000,
    limit_tokens_response=40_000,
    limit_tokens_recent=60_000,
    reasoning_effort="low",
)

CEREBRAS_ZAI_GLM = LlmCerebras(
    name="zai-glm",
    status="experimental",
    description="Reasoning: 4/5, Speed: 5/5, Conversations length: 3/5",
    color="#1fb8cd",
    native_name="zai-glm-4.7",
    knowledge_cutoff="May 2024",
    supports_media=[],
    supports_stop=True,
    supports_think="deepseek",
    supports_tools="openai",
    limit_tokens_total=128_000,
    limit_tokens_response=40_000,
    limit_tokens_recent=60_000,
    reasoning_effort="high",  # not configurable
)

CEREBRAS_ZAI_GLM_FAST = LlmCerebras(
    name="zai-glm-fast",
    status="experimental",
    description="Reasoning: 4/5, Speed: 5/5, Conversations length: 3/5",
    color="#1fb8cd",
    native_name="zai-glm-4.7",
    knowledge_cutoff="May 2024",
    supports_media=[],
    supports_stop=True,
    supports_think=None,  # disable
    supports_tools="openai",
    limit_tokens_total=128_000,
    limit_tokens_response=40_000,
    limit_tokens_recent=60_000,
)


##
## Google VertexAI
##


GOOGLE_GEMINI_FLASH = LlmGemini(
    name="gemini-flash",
    status="stable",
    description="Reasoning: 2/5, Speed: 5/5, Conversations length: 5/5",
    color="#ad552f",
    native_name="google/gemini-3-flash-preview",
    knowledge_cutoff="January 2025",
    supports_media=GEMINI_BLOB_TYPES,
    supports_stop=True,
    supports_think="gemini",
    supports_tools="gemini",
    limit_tokens_total=1_000_000,  # actual limit = 1,048,576
    limit_tokens_response=65_536,
    limit_tokens_recent=120_000,
    limit_media=20,  # actual limit ~ 3600
    reasoning_effort="high",
)

GOOGLE_GEMINI_FLASH_LITE = LlmGemini(
    name="gemini-flash-lite",
    status="stable",
    description="Reasoning: 2/5, Speed: 5/5, Conversations length: 5/5",
    color="#ad552f",
    native_name="google/gemini-2.5-flash-lite",
    knowledge_cutoff="January 2025",
    supports_media=GEMINI_BLOB_TYPES,
    supports_stop=True,
    supports_think=None,  # Disabled in small models for efficiency.
    supports_tools="gemini",
    limit_tokens_total=1_000_000,  # actual limit = 1,048,576
    limit_tokens_response=65_536,
    limit_tokens_recent=120_000,
    limit_media=20,  # actual limit ~ 3600
)

GOOGLE_GEMINI_PRO = LlmGemini(
    name="gemini-pro",
    status="experimental",
    description="Reasoning: 5/5, Speed: 3/5, Conversations length: 5/5",
    color="#ad552f",
    native_name="google/gemini-3-pro-preview",
    knowledge_cutoff="January 2025",
    supports_media=GEMINI_BLOB_TYPES,
    supports_stop=True,
    supports_think="gemini",
    supports_tools="gemini",
    limit_tokens_total=1_000_000,  # actual limit = 1,048,576
    limit_tokens_response=65_536,
    limit_tokens_recent=120_000,
    limit_media=20,  # actual limit ~ 3600
    reasoning_effort="high",
)


##
## OpenAI
##


OPENAI_GPT_5 = LlmLite(
    name="gpt-5",
    status="stable",
    description="Reasoning: 5/5, Speed: 4/5, Conversations length: 4/5",
    color="#f30b87",
    native_name="gpt-5.2",
    knowledge_cutoff="August 2025",
    supports_media=OPENAI_BLOB_TYPES,
    supports_stop=False,
    supports_think="hidden",
    supports_tools="openai",
    limit_tokens_total=400_000,
    limit_tokens_response=128_000,
    limit_tokens_recent=100_000,
    limit_media=20,
    reasoning_effort="medium",
)

OPENAI_GPT_5_MINI = LlmLite(
    name="gpt-5-mini",
    status="stable",
    description="Reasoning: 3/5, Speed: 5/5, Conversations length: 4/5",
    color="#f30b87",
    native_name="gpt-5-mini",
    knowledge_cutoff="May 2024",
    supports_media=OPENAI_BLOB_TYPES,
    supports_stop=False,
    supports_think="hidden",
    supports_tools="openai",
    limit_tokens_total=400_000,
    limit_tokens_response=128_000,
    limit_tokens_recent=100_000,
    limit_media=20,
    reasoning_effort="medium",
)

OPENAI_O3 = LlmLite(
    name="o3",
    status="stable",
    description="Reasoning: 5/5, Speed: 1/5, Conversations length: 3/5",
    color="#f30b87",
    native_name="o3-2025-04-16",
    knowledge_cutoff="May 2024",
    supports_media=OPENAI_BLOB_TYPES,
    supports_stop=False,
    supports_think="hidden",  # Not included in response.
    supports_tools="openai",
    limit_tokens_total=200_000,
    limit_tokens_response=100_000,
    limit_tokens_recent=60_000,
    limit_media=20,
    reasoning_effort="medium",
)

OPENAI_O4_MINI = LlmLite(
    name="o4-mini",
    status="stable",
    description="Reasoning: 4/5, Speed: 2/5, Conversations length: 3/5",
    color="#f30b87",
    native_name="o4-mini-2025-04-16",
    knowledge_cutoff="May 2024",
    supports_media=OPENAI_BLOB_TYPES,
    supports_stop=False,
    supports_think="hidden",  # Not included in response.
    supports_tools="openai",
    limit_tokens_total=200_000,
    limit_tokens_response=100_000,
    limit_tokens_recent=60_000,
    limit_media=20,
    reasoning_effort="medium",
)


##
## Loader
##


LlmModelName = Literal[
    # Anthropic
    "claude-haiku",
    "claude-opus",
    "claude-sonnet",
    # Cerebras
    "gpt-oss",
    "gpt-oss-fast",
    "zai-glm",
    "zai-glm-fast",
    # Gemini
    "gemini-flash",
    "gemini-flash-lite",
    "gemini-pro",
    # OpenAI
    "gpt-5",
    "gpt-5-mini",
    "o3",
    "o4-mini",
]

LLM_MODELS: list[LlmModel] = [
    # Anthropic
    ANTHROPIC_CLAUDE_HAIKU,
    ANTHROPIC_CLAUDE_OPUS,
    ANTHROPIC_CLAUDE_SONNET,
    # Cerebras
    CEREBRAS_GPT_OSS,
    CEREBRAS_GPT_OSS_FAST,
    CEREBRAS_ZAI_GLM,
    CEREBRAS_ZAI_GLM_FAST,
    # Gemini
    GOOGLE_GEMINI_FLASH,
    GOOGLE_GEMINI_FLASH_LITE,
    GOOGLE_GEMINI_PRO,
    # OpenAI
    OPENAI_GPT_5,
    OPENAI_GPT_5_MINI,
    OPENAI_O3,
    OPENAI_O4_MINI,
]

AVAILABLE_LLM_MODELS: dict[str, LlmModel] = {model.name: model for model in LLM_MODELS}


def get_llm_by_name(name: str) -> LlmModel:
    if llm := AVAILABLE_LLM_MODELS.get(name):
        return llm
    else:
        raise BadPersonaError.unknown_model(name)


def get_available_llm_models() -> dict[str, LlmModel]:
    """Get a list of all available models."""
    return AVAILABLE_LLM_MODELS


async def refresh_available_llm_models() -> dict[str, LlmModel]:
    global AVAILABLE_LLM_MODELS  # noqa: PLW0603
    model_availability: dict[str, list[str]] = {"available": [], "not_available": []}
    new_available_llm_models: dict[str, LlmModel] = {}

    for model in LLM_MODELS:
        # TODO: if await model.check_availability():
        new_available_llm_models[model.name] = model

    logger.info("Model availability: %s", as_json(model_availability))

    AVAILABLE_LLM_MODELS = new_available_llm_models
    return AVAILABLE_LLM_MODELS
