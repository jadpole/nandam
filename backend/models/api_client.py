from pydantic import BaseModel
from typing import Any, Literal

from base.core.exceptions import AuthorizationError, BadRequestError
from base.server.auth import ClientAuth, NdAuth
from base.strings.auth import RequestId, UserId
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

from backend.models.microsoft_teams import (
    TeamsChannelReference,
    TeamsConversationReference,
    TeamsUserReference,
)


##
## Workspace
##


class RequestClientApp(BaseModel):
    client: Literal["app"] = "app"
    workspace: str | None = None
    workspace_name: str | None = None
    thread: str | None = None

    def request_info(self, auth: NdAuth) -> "RequestInfo":
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


class RequestClientTeams(BaseModel):
    client: Literal["msteams"] = "msteams"
    bot_name: str
    """
    The Microsoft Teams bot that received the request, used to configure the
    Persona and to route subsequent async messages.
    """
    channel: TeamsChannelReference | None = None
    """
    The channel reference, used to infer `MsTeamScope.site_id` when present.
    """
    conversation: TeamsConversationReference
    """
    The Microsoft Teams conversation reference, used to infer the scope and the
    conversation ID.
    """
    participants: list[TeamsUserReference]
    """
    The participants of the conversation, used to configure access to the scope.
    """
    reply_activity_id: str | None = None
    """
    The activity ID of the reply from the bot, used to get the process details
    when the user sends feedback.
    """
    timezone: str | None = None
    """
    The timezone of the user according to the Microsoft Teams client.
    """

    def request_info(self, auth_client: ClientAuth) -> "RequestInfo":
        # Since Teams requests lack most authorization headers, confirm that the
        # request was sent from a recognized client via the auth witness.
        release = auth_client.config.release
        if not release.is_teams_client():
            raise BadRequestError.new(f"Teams does not support release {release}")

        # Every participant in a Teams conversation must have a valid user ID.
        self.participant_user_ids()

        user_id = self.conversation.user_id()
        conversation_id = self.conversation.conversation_id()

        workspace: Workspace
        workspace_name: str | None = None
        thread: str | None = None

        if self.conversation.is_personal():
            scope = ScopePersonal(user_id=user_id)
            workspace = scope.workspace(release, None)
            workspace_name = f"{self.bot_name} x {self.conversation.user_name()}"
        elif self.channel:
            scope = ScopeMsGroup(group_id=self.channel.group_id)
            workspace = scope.workspace(self.channel.channel_id)
            workspace_name = (
                str(self.channel.channel_name)
                if self.channel.channel_name
                else "General"
            )
            thread = conversation_id
        else:
            scope = ScopePrivate.generate(release, conversation_id)
            workspace = scope.workspace(None)
            workspace_name = "Microsoft Teams Chat"
            if self.participants:
                workspace_name += ": " + ", ".join(
                    sorted(p.name for p in self.participants)
                )

        auth = NdAuth(
            client=auth_client,
            user=None,
            scope=scope,
            request_id=RequestId.new(),
            x_user_id=user_id.uuid(),
        )

        return RequestInfo(
            auth=auth,
            workspace=workspace,
            workspace_name=workspace_name,
            thread=thread,
        )

    def participant_user_ids(self) -> list[UserId]:
        participant_ids = [
            user_id
            for p in self.participants
            if (user_id := UserId.try_decode(f"user-{p.aad_object_id}"))
        ]
        if len(participant_ids) == len(self.participants):
            return participant_ids
        else:
            raise AuthorizationError.unauthorized("invalid participant ID(s)")


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
    def attach_image(filename: FileName, data_uri: DataUri) -> "ClientAction":
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
    ) -> "ClientAction":
        return ClientAction(
            id=None,  # No response expected.
            name="attach_notif",
            arguments={"title": title, "text": text, "color": color},
        )

    @staticmethod
    def attach_text(title: str, text: str) -> "ClientAction":
        return ClientAction(
            id=None,  # No response expected.
            name="attach_text",
            arguments={"title": title, "text": text},
        )
