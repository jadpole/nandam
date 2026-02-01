import contextlib
from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from typing import Literal

from base.core.exceptions import BadRequestError
from base.strings.auth import UserHandle, UserId
from base.strings.microsoft import (
    MsChannelId,
    MsChannelName,
    MsGroupId,
    MsTeamsBotId,
    MsTeamsUserId,
)


class TeamsChannelReference(BaseModel):
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


class TeamsConversationReference(BaseModel):
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


class TeamsUserReference(BaseModel):
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
