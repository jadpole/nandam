from pydantic import BaseModel
from typing import Any, Literal

from base.core.exceptions import BadRequestError
from base.server.auth import NdAuth
from base.strings.data import DataUri
from base.strings.file import FileName
from base.strings.process import RemoteId
from base.strings.resource import WebUrl
from base.strings.scope import (
    ScopeInternal,
    ScopeMsGroup,
    ScopePersonal,
    ScopePrivate,
    Workspace,
)


##
## Workspace
##


class RequestClientApp(BaseModel):
    client: Literal["app"] = "app"
    workspace: str | None = None
    workspace_name: str | None = None
    thread: str | None = None

    def request_info(self, auth: NdAuth) -> RequestInfo:
        release = auth.client.config.release
        workspace: Workspace
        workspace_name: str = self.workspace_name or self.workspace or ""

        match auth.scope:
            case ScopeInternal():
                workspace = auth.scope.workspace(release, self.workspace)
                if not workspace_name:
                    workspace_name = f"{release} app"

            case ScopeMsGroup():
                if self.workspace and (
                    workspace_id := FileName.try_normalize(self.workspace)
                ):
                    workspace = auth.scope.workspace(workspace_id)
                else:
                    raise BadRequestError.new("invalid workspace ID in scope msgroup")

            case ScopePersonal():
                workspace = auth.scope.workspace(release, self.workspace)
                if not workspace_name:
                    workspace_name = f"{release} x {auth.scope.user_id}"

            case ScopePrivate():
                workspace = auth.scope.workspace(self.workspace)
                if not workspace_name:
                    workspace_name = (
                        f"{release} x {auth.user.user_name}"
                        if auth.user
                        else f"{release} app"
                    )

            case _:
                raise BadRequestError.new(f"scope {auth.scope} not supported by app")

        return RequestInfo(
            auth=auth,
            workspace=workspace,
            workspace_name=workspace_name,
            thread=self.thread,
        )


##
## Request
##


class RequestInfo(BaseModel, frozen=True):
    auth: NdAuth
    workspace: Workspace
    workspace_name: str
    thread: str | None


class ClientAttachment(BaseModel):
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


class ClientAction(BaseModel):
    """
    An action that is expected to run on the client.
    When an `id` is provided, the client is expected to send a result to the
    backend via its API (and optionally, progress updates).
    """

    id: RemoteId | None
    name: str
    arguments: dict[str, Any]

    @staticmethod
    def attach_image(filename: FileName, data_uri: DataUri) -> ClientAction:
        return ClientAction(
            id=None,  # No response expected.
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
            id=None,  # No response expected.
            name="attach_notif",
            arguments={"title": title, "text": text, "color": color},
        )

    @staticmethod
    def attach_text(title: str, text: str) -> ClientAction:
        return ClientAction(
            id=None,  # No response expected.
            name="attach_text",
            arguments={"title": title, "text": text},
        )
