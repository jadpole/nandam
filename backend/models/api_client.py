from pydantic import BaseModel, Field
from typing import Annotated, Any, Literal

from base.core.exceptions import BadRequestError, ErrorInfo
from base.server.auth import NdAuth
from base.strings.auth import BotId
from base.strings.data import DataUri
from base.strings.file import FileName
from base.strings.remote import RemoteProcessSecret
from base.strings.resource import WebUrl
from base.strings.scope import (
    ScopeInternal,
    ScopeMsGroup,
    ScopePersonal,
    ScopePrivate,
    Workspace,
)
from base.strings.thread import ThreadId

from backend.models.workspace_thread import BotMessagePart_


##
## Request
##


class RequestClientApp(BaseModel):
    client: Literal["app"] = "app"
    workspace: str
    thread: str
    bot: str

    def request_info(self, auth: NdAuth) -> ClientInfo:
        release = auth.client.config.release
        workspace: Workspace

        match auth.scope:
            case ScopeInternal():
                workspace = auth.scope.workspace(release, self.workspace)
            case ScopeMsGroup():
                workspace = auth.scope.workspace(self.workspace)
            case ScopePersonal():
                workspace = auth.scope.workspace(release, self.workspace)
            case ScopePrivate():
                workspace = auth.scope.workspace(self.workspace)
            case _:
                raise BadRequestError.new(f"scope {auth.scope} not supported by app")

        return ClientInfo(
            auth=auth,
            workspace=workspace,
            workspace_name=self.workspace,
            default_thread=ThreadId.conversation(workspace, self.thread),
            bot_name=self.bot,
            bot_id=BotId.new(self.bot, self.thread),
        )


class ClientInfo(BaseModel, frozen=True):
    auth: NdAuth
    workspace: Workspace
    workspace_name: str
    default_thread: ThreadId | None
    bot_name: str
    bot_id: BotId


class ClientAttachment(BaseModel, frozen=True):
    content_url: WebUrl
    """
    The `content_url` used to infer the source URL.
    """
    download_url: DataUri | WebUrl
    """
    A temporary signed URL used to download `content_url`, then discarded.
    Access is henceforth granted to participants in the conversation.
    """
    name: str
    """
    The `FileName` of the attachment (without format validation).
    - Used by Knowledge when `download_url` is a `DataUri` or when a name cannot
      be inferred from the downloaded file.
    - Used by Nandam Backend to provide better error messages.
    """


##
## Response
##


TeamsCardColor = Literal["default", "accent", "good", "warning"]


class ClientAction(BaseModel, frozen=True):
    """
    An action that is expected to run on the client.
    When an `id` is provided, the client is expected to send a result to the
    backend via its API (and optionally, progress updates).
    """

    secret: RemoteProcessSecret | None
    name: str
    arguments: dict[str, Any]

    @staticmethod
    def attach_image(filename: FileName, data_uri: DataUri) -> ClientAction:
        return ClientAction(
            secret=None,  # No response expected.
            name="attach_image",
            arguments={"filename": str(filename), "data_uri": data_uri},
        )

    @staticmethod
    def attach_notif(
        title: str | None,
        text: str,
        color: TeamsCardColor = "default",
    ) -> ClientAction:
        return ClientAction(
            secret=None,  # No response expected.
            name="attach_notif",
            arguments={"title": title, "text": text, "color": color},
        )

    @staticmethod
    def attach_text(title: str, text: str) -> ClientAction:
        return ClientAction(
            secret=None,  # No response expected.
            name="attach_text",
            arguments={"title": title, "text": text},
        )


##
## Response Stream
##


class ChatbotStreamError(BaseModel, frozen=True):
    event: Literal["error"] = "error"
    error: ErrorInfo
    partial_reply: list[BotMessagePart_]
    actions: list[ClientAction]


class ChatbotStreamProgress(BaseModel, frozen=True):
    event: Literal["progress"] = "progress"
    summary: str | None
    partial_reply: list[BotMessagePart_]
    actions: list[ClientAction]


class ChatbotStreamResult(BaseModel, frozen=True):
    event: Literal["result"] = "result"
    reply: list[BotMessagePart_]
    actions: list[ClientAction]


ChatbotStream = ChatbotStreamError | ChatbotStreamProgress | ChatbotStreamResult
ChatbotStream_ = Annotated[ChatbotStream, Field(discriminator="event")]
