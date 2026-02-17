import contextlib

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from typing import Literal

from base.core.exceptions import AuthorizationError, BadRequestError
from base.server.auth import ClientAuth, NdAuth
from base.strings.auth import BotId, RequestId, UserHandle, UserId
from base.strings.microsoft import (
    MsChannelId,
    MsChannelName,
    MsGroupId,
    MsTeamsBotId,
    MsTeamsUserId,
)
from base.strings.scope import (
    ScopeMsGroup,
    ScopePersonal,
    ScopePrivate,
    Workspace,
)
from base.strings.thread import ThreadId

from backend.models.api_client import ClientInfo


class MsTeamsChannelReference(BaseModel):
    """
    When the message is sent from a Teams channel, includes the details of the
    site and channel.

    TODO: Include group name and use to generate workspace name?
    """

    group_id: MsGroupId
    """
    The `addGroupId` of the Teams channel, used by `ask_index`.
    For example: "c92caf77-8136-43df-b94a-c98d16d99130".
    """
    channel_id: MsChannelId
    """
    The `teamsChannelId` of a Teams channel.
    For example: "19:0123456789abcdef0123456789abcdef@thread.tacv2".
    """
    channel_name: MsChannelName | None
    """
    The name of the Teams channel.  When missing, the channel is "General".
    For example: "My Channel Name".
    """


class MsTeamsConversationReference(BaseModel):
    """
    See https://learn.microsoft.com/en-us/javascript/api/botframework-schema/conversationreference?view=botbuilder-ts-latest

    NOTE: Should ONLY be used with `Release.teams_client()`.

    NOTE: Corresponds to `ConversationReference` in the Bot Framework SDK, and
    therefore, its fields use camel case.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        from_attributes=True,
        serialize_by_alias=True,
        validate_by_name=True,
    )

    activity_id: str | None = None
    bot: dict[str, str]
    channel_id: Literal["msteams"]
    conversation: dict[str, str]
    locale: str | None = None
    service_url: str
    user: dict[str, str]

    def bot_id(self) -> MsTeamsBotId:
        if value := MsTeamsBotId.try_decode(self.bot.get("id")):
            return value
        else:
            raise BadRequestError.new("cannot infer bot ID")

    def bot_name(self) -> str:
        if value := self.bot.get("name"):
            return value
        else:
            raise BadRequestError.new("cannot infer bot name")

    def conversation_id(self) -> str:
        if value := self.conversation.get("id"):
            return value
        else:
            raise BadRequestError.new("cannot infer conversation ID")

    def is_personal(self) -> bool:
        return self.conversation.get("conversationType") == "personal"

    def teams_user_id(self) -> MsTeamsUserId:
        if value := MsTeamsUserId.try_decode(self.user.get("id")):
            return value
        else:
            raise BadRequestError.new("cannot infer Teams user ID")

    def user_id(self) -> UserId:
        with contextlib.suppress(ValueError):
            if value := self.user.get("aadObjectId"):
                return UserId.teams(value)
        raise BadRequestError.new("cannot infer Active Directory user ID")

    def user_name(self) -> str:
        if value := self.user.get("name"):
            return value
        else:
            raise BadRequestError.new("cannot infer user name")

    def user_email(self) -> str:
        raise NotImplementedError()  # TODO


class MsTeamsUserReference(BaseModel):
    """
    The details of the Teams user that sent the message.
    Used to generate `MsPersonalScope` from `user_principal_name`.

    NOTE: Corresponds to `TeamsChannelAccount` in the Bot Framework SDK, and
    therefore, its fields use camel case.
    """

    model_config = ConfigDict(
        alias_generator=to_camel,
        from_attributes=True,
        serialize_by_alias=True,
        validate_by_name=True,
    )

    id: str
    name: str
    aad_object_id: str
    given_name: str
    surname: str
    email: str
    user_principal_name: str
    tenant_id: str
    user_role: str

    def user_id(self) -> UserId:
        return UserId.decode(f"user-{self.aad_object_id}")

    def user_handle(self) -> UserHandle:
        return UserHandle.decode(self.user_principal_name.split("@")[0])


class RequestClientMsTeams(BaseModel):
    client: Literal["msteams"] = "msteams"
    bot_name: str
    """
    The Microsoft Teams bot that received the request, used to configure the
    Persona and to route subsequent async messages.
    """
    channel: MsTeamsChannelReference | None = None
    """
    The channel reference, used to infer `MsTeamScope.site_id` when present.
    """
    conversation: MsTeamsConversationReference
    """
    The Microsoft Teams conversation reference, used to infer the scope and the
    conversation ID.
    """
    participants: list[MsTeamsUserReference]
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

    def request_info(self, auth_client: ClientAuth) -> ClientInfo:
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
        default_thread: str | None = None

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
            default_thread = ThreadId.conversation(workspace, conversation_id)
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

        return ClientInfo(
            auth=auth,
            workspace=workspace,
            workspace_name=workspace_name,
            default_thread=default_thread,
            bot_name=self.bot_name,
            bot_id=BotId.new(self.bot_name, default_thread),
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
