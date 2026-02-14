import base64

from datetime import datetime
from hashlib import sha256
from typing import Self

from base.config import BaseConfig
from base.core.strings import ValidatedStr, normalize_str
from base.core.unique_id import unique_id_from_datetime
from base.strings.data import REGEX_UUID

REGEX_RELEASE = r"[a-z]+(?:\-[a-z]+)*-(?:dev|prod|stage|test)(?:\-[a-z]+)*"

REGEX_APP_ID = rf"app-{REGEX_RELEASE}"
REGEX_BOT_ID = r"bot(?:-[a-z0-9]+)+"
REGEX_SERVICE_ID = r"svc(?:-[a-z0-9]+)+"
REGEX_USER_ID = rf"user-{REGEX_UUID}"
REGEX_AGENT_ID = rf"{REGEX_BOT_ID}|{REGEX_SERVICE_ID}|{REGEX_USER_ID}"

REGEX_USER_HANDLE = r"[A-Za-z\.]+"
STUB_USER_ID_PREFIX = "user-00000000-0000-0000-0000-"


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
    def stub() -> Release:
        return Release.decode("unit-test")

    @staticmethod
    def local_dev() -> Release:
        return Release.decode("local-dev")

    @staticmethod
    def teams_client() -> Release:
        return (
            Release.decode(f"nandam-teams-{BaseConfig.environment}")
            if BaseConfig.is_kubernetes()
            else Release.decode("nandam-teams-dev")
        )

    def is_local_dev(self) -> bool:
        return self == Release.decode("local-dev")

    def is_teams_client(self) -> bool:
        return self == Release.teams_client()


##
## Agent
##


class AgentId(ValidatedStr):
    @classmethod
    def _parse(cls, v: str) -> AgentId:
        if v.startswith("bot-"):
            return BotId.decode(v)
        elif v.startswith("svc-"):
            return ServiceId.decode(v)
        elif v.startswith("user-"):
            return UserId.decode(v)
        else:
            raise ValueError(f"invalid AgentId: unknown prefix, got '{v}'")

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "app-ai-exporter-prod",
            "app-nandam-teams-dev",
            "bot-nandam",
            "svc-backend-tools",
            "user-00000000-0000-0000-0000-000000000000",  # testing
            "user-54916b77-a320-4496-a8f6-f4ce7ab46fc8",  # jpelletier
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_AGENT_ID


class BotId(AgentId):
    @classmethod
    def _parse(cls, v: str) -> Self:
        return cls(v)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["bot-nandam"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_BOT_ID

    @staticmethod
    def new(name: str) -> BotId:
        suffix = normalize_str(
            name,
            allowed_special_chars="-",
            other_replacements={" ": "-", ".": "-", "_": "-"},
            remove_prefix_chars="-",
            remove_suffix_chars="-",
        )
        return BotId.decode(f"bot-{suffix}")

    @staticmethod
    def stub(name: str = "") -> BotId:
        suffix = normalize_str(
            name,
            allowed_special_chars="-",
            other_replacements={" ": "-", ".": "-", "_": "-"},
            remove_prefix_chars="-",
            remove_suffix_chars="-",
        )
        return BotId.decode(f"bot-stub-{suffix}" if suffix else "bot-stub")

    def is_stub(self) -> bool:
        return self == "bot-stub" or self.startswith("bot-stub-")


class ServiceId(AgentId):
    @classmethod
    def _parse(cls, v: str) -> Self:
        return cls(v)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "svc-backend-tools",
            "svc-stub",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_SERVICE_ID

    @staticmethod
    def new(name: str) -> ServiceId:
        suffix = normalize_str(
            name,
            allowed_special_chars="-",
            other_replacements={" ": "-", ".": "-", "_": "-"},
            remove_prefix_chars="-",
            remove_suffix_chars="-",
        )
        if not suffix or suffix == "stub" or suffix.startswith("stub-"):
            raise ValueError(f"invalid ServiceId name: got stub: '{name}'")
        return ServiceId.decode(f"svc-{suffix}")

    @staticmethod
    def stub(suffix: str = "") -> ServiceId:
        return ServiceId.decode(f"svc-stub-{suffix}" if suffix else "svc-stub")

    def is_stub(self) -> bool:
        return self == "svc-stub" or self.startswith("svc-stub-")


class UserId(AgentId):
    """
    The Active Directory Object ID of an employee, prefixed by "user-".
    """

    @classmethod
    def _parse(cls, v: str) -> Self:
        return cls(v)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "user-00000000-0000-0000-0000-000000000000",
            "user-54916b77-a320-4496-a8f6-f4ce7ab46fc8",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_USER_ID

    @staticmethod
    def stub(user_handle: UserHandle | None = None) -> UserId:
        if user_handle:
            salted = f"stub-user-id:{user_handle}"
            digest = sha256(salted.encode()).hexdigest()
            suffix = digest[:12]
        else:
            suffix = "0" * 12

        return UserId.decode(STUB_USER_ID_PREFIX + suffix)

    @staticmethod
    def teams(user_uuid: str) -> UserId:
        value = f"user-{user_uuid}"
        if value.startswith(STUB_USER_ID_PREFIX):
            raise ValueError(f"invalid UserId: expected Teams, got stub: '{user_uuid}'")
        return UserId.decode(f"user-{user_uuid}")

    def is_stub(self) -> bool:
        return self.startswith(STUB_USER_ID_PREFIX)

    def is_teams(self) -> bool:
        return not self.is_stub()

    def uuid(self) -> str:
        return self.removeprefix("user-")


class UserHandle(ValidatedStr):
    """
    The username of an employee, i.e., their corporate email prefix.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["jpelletier"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_USER_HANDLE

    @staticmethod
    def stub(suffix: str = "") -> UserHandle:
        return UserHandle.decode(f"stub.{suffix}" if suffix else "stub")

    def is_stub(self) -> bool:
        return self == "stub" or self.startswith("stub.")

    def is_teams(self) -> bool:
        return not self.is_stub()


##
## Headers
##


class RequestId(ValidatedStr):
    @staticmethod
    def new(timestamp: datetime | None = None) -> RequestId:
        suffix = unique_id_from_datetime(timestamp, num_chars=24)
        return RequestId.decode(f"request-{suffix}")

    @staticmethod
    def stub(suffix: str = "") -> RequestId:
        if len(suffix) > 20:  # noqa: PLR2004
            raise ValueError(f"invalid RequestId.stub suffix: got '{suffix}'")
        else:
            suffix = suffix.zfill(20)
        return RequestId.decode(f"request-stub{suffix}")

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["request-9e7xc00123456789abcdef01"]

    @classmethod
    def _schema_regex(cls) -> str:
        return r"request-[a-z0-9]{24}"


def authorization_basic_credentials(username: str, password: str) -> str:
    credentials_base64 = base64.b64encode(f"{username}:{password}".encode()).decode()
    return f"Basic {credentials_base64}"


def parse_basic_credentials(value: str | None) -> tuple[str, str] | None:
    try:
        if not value or not value.startswith("Basic "):
            return None  # expected Basic header

        creds_base64 = value.removeprefix("Basic ")
        creds_str = base64.b64decode(creds_base64).decode("utf-8")
        if ":" not in creds_str:
            return None  # Basic header does no contain username:password

        username, password = creds_str.split(":", maxsplit=1)
        return username, password
    except ValueError:
        return None
