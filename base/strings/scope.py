from typing import Literal

from base.config import BaseConfig
from base.core.strings import StructStr, ValidatedStr
from base.core.unique_id import unique_id_from_str
from base.strings.auth import UserId
from base.strings.data import REGEX_UUID
from base.strings.microsoft import MsGroupId, REGEX_MS_GROUP_ID

REGEX_RELEASE = r"[a-z]+(?:\-[a-z]+)*-(?:dev|prod|stage|test)(?:\-[a-z]+)*"

REGEX_SCOPE_INTERNAL = r"internal"
REGEX_SCOPE_MSGROUP = rf"msgroup-{REGEX_MS_GROUP_ID}"
REGEX_SCOPE_PERSONAL = rf"personal-{REGEX_UUID}"
REGEX_SCOPE_PRIVATE = r"private-[a-z0-9]{36}"
REGEX_SCOPE = (
    rf"{REGEX_SCOPE_INTERNAL}"
    rf"|{REGEX_SCOPE_MSGROUP}"
    rf"|{REGEX_SCOPE_PERSONAL}"
    rf"|{REGEX_SCOPE_PRIVATE}"
)

NUM_CHARS_PRIVATE_ID = 36


##
## Client
##


class Release(ValidatedStr):
    """
    The name of a Concourse service release.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["ai-exporter-prod", "local-dev", "nandam-teams-prod"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_RELEASE

    @staticmethod
    def teams_client() -> "Release":
        return Release.decode(f"nandam-teams-{BaseConfig.environment}")

    def is_teams_client(self) -> bool:
        return self.startswith("nandam-teams-")


##
## Scope
##


ScopeType = Literal[
    "internal",
    "msgroup",
    "personal",
    "private",
]


class Scope(StructStr, frozen=True):
    """
    NOTE: Never instantiated directly, but instead, parsing returns a subclass.
    Therefore, all subclasses MUST define `type: Literal` with a default value,
    which is used as the string prefix.
    """

    type: str

    @staticmethod
    def find_subclass_by_type(scope_type: str) -> "type[Scope]":
        for subclass in Scope.__subclasses__():
            if subclass.model_fields["type"].default == scope_type:
                return subclass

        raise ValueError(f"unknown scope type '{scope_type}'")

    @classmethod
    def _parse(cls, v: str) -> "Scope":
        type_str = v.split("-", 1)[0]
        return cls.find_subclass_by_type(type_str)._parse(v)  # noqa: SLF001

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "internal",
            "msgroup-00000000-0000-0000-0000-000000000000",  # MsChannelId
            "personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8",  # UserId
            "private-0123456789abcdefghijklmnopqrstuvwxyz",  # HASH(Release)
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_SCOPE

    def _serialize(self) -> str:
        raise NotImplementedError("Scope._serialize is called via subclasses")


class ScopeInternal(Scope, frozen=True):
    """
    The conversation occurs inside an "app" that isn't a classic Nandam client,
    such as a cronjob or an internal application where everything is public.

    The user's identity might be verified (when an authorization is provided),
    but access is limited to internal resources and workspace attachments.
    """

    type: Literal["internal"] = "internal"

    @classmethod
    def _parse(cls, v: str) -> "ScopeInternal":
        return ScopeInternal()

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["internal"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_SCOPE_INTERNAL

    def _serialize(self) -> str:
        return "internal"


class ScopeMsGroup(Scope, frozen=True):
    """
    The conversation occurs in a Microsoft Teams channel.

    The user's identity is verified, but access is not privileged and limited to
    public resources and those within the SharePoint site.
    """

    type: Literal["msgroup"] = "msgroup"
    group_id: MsGroupId

    @classmethod
    def _parse(cls, v: str) -> "ScopeMsGroup":
        group_id_str = v.removeprefix("msgroup-")
        group_id = MsGroupId.decode_part(cls, v, group_id_str)
        return ScopeMsGroup(group_id=group_id)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["msgroup-00000000-0000-0000-0000-000000000000"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_SCOPE_MSGROUP

    def _serialize(self) -> str:
        return f"msgroup-{self.group_id}"


class ScopePersonal(Scope, frozen=True):
    """
    The conversation occurs in a privileged scope, visible to a single user,
    either in their personal chat with Nandam or in some other client where
    threads are isolated.

    Files attached to the conversation are stored in the user's OneDrive.
    """

    type: Literal["personal"] = "personal"
    user_id: UserId

    @classmethod
    def _parse(cls, v: str) -> "ScopePersonal":
        user_id_str = v.removeprefix("personal-")
        user_id = UserId.decode_part(cls, v, f"user-{user_id_str}")
        return ScopePersonal(user_id=user_id)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_SCOPE_PERSONAL

    def _serialize(self) -> str:
        return f"personal-{self.user_id.uuid()}"


class ScopePrivate(Scope, frozen=True):
    """
    The conversation occurs in a Microsoft Teams chat or in a multi-participants
    workspace within an application that calls Nandam.

    The user's identity is verified, but access is not privileged and limited to
    internal resources and workspace attachments.

    The processes can only be audited by the participants of the chat.
    """

    type: Literal["private"] = "private"
    chat_id: str

    @staticmethod
    def generate(release: Release, group_key: str) -> "ScopePrivate":
        chat_id = unique_id_from_str(
            f"{release}-{group_key}",
            num_chars=NUM_CHARS_PRIVATE_ID,
            salt=f"scope-private-{BaseConfig.environment}",
        )
        return ScopePrivate(chat_id=chat_id)

    @classmethod
    def _parse(cls, v: str) -> "ScopePrivate":
        return ScopePrivate(chat_id=v.removeprefix("private-"))

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["private-0123456789abcdefghijklmnopqrstuvwxyz"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_SCOPE_PRIVATE

    def _serialize(self) -> str:
        return f"private-{self.chat_id}"
