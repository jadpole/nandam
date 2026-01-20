from pydantic import BaseModel
from typing import Literal

from base.strings.data import MimeType


LlmModelStatus = Literal["experimental", "legacy", "stable"]

LlmThinkMode = Literal["anthropic", "deepseek", "gemini", "gpt-oss", "hidden"]
"""
- "anthropic" uses "thinking_blocks".
- "deepseek" expects "<think>" in request, but response may parse "reasoning".
- "gemini" uses "thinking_config" and "thoughtSignature".
- "gpt-oss" expects plain text in request, but response may parse "reasoning".
- "hidden" does not receive thoughts in the ChatCompletion response.
"""

LlmToolsMode = Literal["gemini", "openai"]
"""
Indicates how to render native tool calls and results in the LLM API request.
"""


class LlmModelInfo(BaseModel, frozen=True):
    # Interface
    name: str
    status: LlmModelStatus
    description: str
    color: str

    # Model
    native_name: str
    knowledge_cutoff: str | None = None
    supports_media: list[MimeType]
    supports_think: LlmThinkMode | None
    supports_tools: LlmToolsMode | None

    # Limits
    limit_tokens_total: int
    limit_tokens_response: int
    limit_tokens_recent: int | None
    limit_media: int = 0

    def limit_tokens_request(self) -> int:
        """
        When the conversation history goes beyond this limit, older items will
        be truncated.
        """
        return self.limit_tokens_total - self.limit_tokens_response


##
## Constants
##


CLAUDE_BLOB_TYPES: list[MimeType] = [
    MimeType.decode("image/gif"),
    MimeType.decode("image/jpeg"),
    MimeType.decode("image/png"),
    MimeType.decode("image/webp"),
]

GEMINI_BLOB_TYPES: list[MimeType] = [
    MimeType.decode("image/png"),
    MimeType.decode("image/jpeg"),
    MimeType.decode("image/webp"),
    MimeType.decode("image/heic"),
    MimeType.decode("image/heif"),
]

OPENAI_BLOB_TYPES: list[MimeType] = [
    MimeType.decode("image/gif"),
    MimeType.decode("image/jpeg"),
    MimeType.decode("image/png"),
    MimeType.decode("image/webp"),
]
