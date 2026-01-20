from typing import Literal, Self

from base.config import BaseConfig
from base.core.strings import StructStr
from base.core.unique_id import unique_id_from_str
from base.strings.auth import REGEX_RELEASE, Release, UserId
from base.strings.data import REGEX_UUID
from base.strings.file import FileName
from base.strings.microsoft import MsChannelId, MsGroupId, REGEX_MS_GROUP_ID

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

REGEX_SUFFIX_DEFAULT = rf"default(?:-{REGEX_RELEASE})?"
REGEX_SUFFIX_CHANNEL = r"channel-[a-z0-9]{36}"
REGEX_WORKSPACE_SUFFIX = rf"{REGEX_SUFFIX_DEFAULT}|{REGEX_SUFFIX_CHANNEL}"

NUM_CHARS_PRIVATE_ID = 36
NUM_CHARS_CHANNEL_ID = 36


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
        raise NotImplementedError("Subclasses must implement Scope._serialize")


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

    def workspace(
        self,
        release: Release,
        channel: str | None,
    ) -> "WorkspaceDefault | WorkspaceChannel":
        return (
            WorkspaceChannel.generate(self, f"{release}-{channel}")
            if channel
            else WorkspaceDefault.new(self, release)
        )


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

    def workspace(self, channel_id: FileName | MsChannelId) -> "WorkspaceChannel":
        if isinstance(channel_id, MsChannelId):
            channel_id = channel_id.as_filename()
        return WorkspaceChannel(scope=self, channel_id=channel_id)


class ScopePersonal(Scope, frozen=True):
    """
    The conversation occurs in a privileged scope, visible to a single user,
    either in their personal chat with Nandam or in some other client where
    all information is accessible.
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

    def workspace(
        self,
        release: Release,
        channel_name: str | None,
    ) -> "WorkspaceDefault | WorkspaceChannel":
        return (
            WorkspaceChannel.generate(self, f"{release}-{channel_name}")
            if channel_name
            else WorkspaceDefault.new(self, release)
        )


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

    def workspace(
        self,
        channel_name: str | None,
    ) -> "WorkspaceChannel | WorkspaceDefault":
        return (
            WorkspaceChannel.generate(self, f"{self.chat_id}-{channel_name}")
            if channel_name
            else WorkspaceDefault.new(self, None)
        )


##
## Workspace
##


class Workspace(StructStr, frozen=True):
    """
    The unique ID of a workspace, prefixed by its scope.

    The workspace suffix one of:

    - "default" for scopes where a single "base" conversation exists, such as
      "personal" or "private".
    - "channel-" followed by a hash for scopes with multiple conversations, such
      as "msgroup" or "internal".
    """

    type: str
    scope: Scope

    @staticmethod
    def stub_internal() -> "Workspace":
        return Workspace.decode("ndw://internal/default-unit-test")

    @staticmethod
    def find_subclass_by_type(suffix_type: str) -> "type[Workspace] | None":
        for subclass in Workspace.__subclasses__():
            if subclass.model_fields["type"].default == suffix_type:
                return subclass
        return None

    @classmethod
    def _parse(cls, v: str) -> "Workspace":
        scope_str, suffix_str = v.removeprefix("ndw://").split("/", 1)
        scope = Scope.decode_part(cls, v, scope_str)

        suffix_type = suffix_str.split("-", 1)[0]
        suffix_cls = Workspace.find_subclass_by_type(suffix_type)
        if not suffix_cls:
            raise ValueError(f"invalid {cls.__name__}: unknown suffix, got '{v}'")

        return suffix_cls._suffix_parse(scope, suffix_str)  # noqa: SLF001

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "ndw://internal/default-ai-exporter-prod",
            "ndw://msgroup-00000000-0000-0000-0000-000000000000/channel-0123456789abcdefghijklmnopqrstuvwxyz",
            "ndw://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/default",
            "ndw://private-0123456789abcdefghijklmnopqrstuvwxyz/default",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return rf"ndw://(?:{REGEX_SCOPE})/(?:{cls._suffix_regex()})"

    def _serialize(self) -> str:
        return f"ndw://{self.scope}/{self.as_suffix()}"

    @classmethod
    def _suffix_parse(cls, scope: Scope, suffix: str) -> Self:
        raise NotImplementedError("Subclasses must implement Workspace._suffix_parse")

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_WORKSPACE_SUFFIX

    def as_suffix(self) -> str:
        raise NotImplementedError("Subclasses must implement Workspace.as_suffix")


class WorkspaceDefault(Workspace, frozen=True):
    type: Literal["default"] = "default"
    release: Release | None

    @staticmethod
    def new(scope: Scope, release: Release | None) -> "WorkspaceDefault":
        if release and (
            isinstance(scope, ScopePrivate)
            or (isinstance(scope, ScopePersonal) and release.is_teams_client())
        ):
            release = None

        if not isinstance(scope, ScopeInternal | ScopePersonal | ScopePrivate):
            suffix = f"default-{release}" if release else "default"
            raise ValueError(  # noqa: TRY004
                f"invalid WorkspaceDefault: scope {scope} not supported, given 'ndw://{scope}/{suffix}'"
            )

        if isinstance(scope, ScopeInternal) and not release:
            raise ValueError(
                "invalid WorkspaceDefault: scope internal requires a release"
            )
        return WorkspaceDefault(scope=scope, release=release)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "ndw://internal/default-ai-exporter-prod",
            "ndw://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/default",
            "ndw://private-0123456789abcdefghijklmnopqrstuvwxyz/default",
        ]

    @classmethod
    def _suffix_parse(cls, scope: Scope, suffix: str) -> "WorkspaceDefault":
        release = (
            Release.decode_part(cls, suffix, suffix.removeprefix("default-"))
            if suffix.startswith("default-")
            else None
        )
        return WorkspaceDefault.new(scope, release)

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_SUFFIX_DEFAULT

    def as_suffix(self) -> str:
        return f"default-{self.release}" if self.release else "default"


class WorkspaceChannel(Workspace, frozen=True):
    type: Literal["channel"] = "channel"
    channel_id: FileName

    @staticmethod
    def generate(scope: Scope, value: str) -> "WorkspaceChannel":
        channel_id = unique_id_from_str(
            value,
            num_chars=NUM_CHARS_CHANNEL_ID,
            salt=f"nandam-workspace-channel-{BaseConfig.environment}",
        )
        return WorkspaceChannel(scope=scope, channel_id=FileName.decode(channel_id))

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "ndw://internal/channel-0123456789abcdefghijklmnopqrstuvwxyz",
            "ndw://msgroup-00000000-0000-0000-0000-000000000000/channel-0123456789abcdefghijklmnopqrstuvwxyz",
        ]

    @classmethod
    def _suffix_parse(cls, scope: Scope, suffix: str) -> "WorkspaceChannel":
        channel_id = FileName.decode_part(cls, suffix, suffix.removeprefix("channel-"))
        return WorkspaceChannel(scope=scope, channel_id=channel_id)

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_SUFFIX_CHANNEL

    def as_suffix(self) -> str:
        return f"channel-{self.channel_id}"
