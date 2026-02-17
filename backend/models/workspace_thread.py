"""
Thread Models
=============

A "thread" is a conversation scoped within a workspace, identified by a
`ThreadUri` of the form ``nkt://{scope}/{workspace_suffix}/{thread_id}``.

The data is stored in two separate KV structures:

- **ThreadInfo** — lightweight metadata (URI, timestamps).  Stored as a single
  JSON value so it can be read cheaply.
- **ThreadMessage** — individual messages, stored in a Redis LIST so new
  messages can be appended via RPUSH without rewriting the whole thread.

A "cursor" points to the last message a consumer has seen and has the form
``nkt://{scope}/{workspace_suffix}/{thread_id}/{last_message_id}``.

Message roles:
- "user": a message from the human user.
- "bot": a message from the assistant / chatbot.

The model is designed to be extended later with additional roles (e.g. "system",
"tool") or richer content types.
"""

from datetime import UTC, datetime
from pydantic import BaseModel, Field
from typing import Annotated, Any, Literal

from base.core.exceptions import ErrorInfo
from base.core.unions import ModelUnion
from base.models.content import ContentText_
from base.resources.metadata import ResourceInfo
from base.strings.auth import AgentId, BotId, UserId
from base.strings.process import ProcessId, ProcessName
from base.strings.thread import ThreadMessageId, ThreadUri

from backend.models.process_result import ProcessResult_


##
## Thread
##


class ThreadInfo(BaseModel):
    """Lightweight metadata about a thread.  Does NOT contain messages."""

    uri: ThreadUri
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    message_count: int = 0

    def touch(self) -> None:
        """Bump `updated_at` to now and increment the message count."""
        self.updated_at = datetime.now(UTC)
        self.message_count += 1


##
## Message
##


class MessageText(BaseModel, frozen=True):
    kind: Literal["text"] = "text"
    think: ContentText_ | None = None
    text: ContentText_


class MessageTool(BaseModel, frozen=True):
    kind: Literal["tool"] = "tool"
    process_id: ProcessId
    name: ProcessName
    arguments: dict[str, Any]
    result: ProcessResult_[Any] | None = None


BotMessagePart = MessageText | MessageTool
BotMessagePart_ = Annotated[BotMessagePart, Field(discriminator="kind")]


class BaseThreadMessage(ModelUnion, frozen=True):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    message_id: ThreadMessageId = Field(default_factory=ThreadMessageId.generate)
    sender: AgentId


class ThreadMessageBot(BaseThreadMessage, frozen=True):
    role: Literal["bot"] = "bot"
    sender: BotId
    content: list[BotMessagePart_]


class ThreadMessageUser(BaseThreadMessage, frozen=True):
    role: Literal["user"] = "user"
    sender: UserId
    content: ContentText_ | None
    attachments: list[ResourceInfo]
    attachment_errors: dict[str, ErrorInfo]


ThreadMessage = ThreadMessageBot | ThreadMessageUser
ThreadMessage_ = Annotated[ThreadMessage, Field(discriminator="role")]
