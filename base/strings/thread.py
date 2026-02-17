from datetime import datetime
from typing import Self

from backend.config import BackendConfig
from base.core.strings import StructStr, ValidatedStr
from base.core.unique_id import unique_id_from_datetime, unique_id_from_str
from base.strings.scope import REGEX_SCOPE, REGEX_WORKSPACE_SUFFIX, Workspace


NUM_CHARS_THREAD_ID = 24
NUM_CHARS_MESSAGE_ID = 28

REGEX_THREAD_ID = r"thread-[a-z0-9]{24,}"
REGEX_THREAD_MESSAGE_ID = r"msg-[a-z0-9]{28,}"
REGEX_THREAD_URI = (
    rf"nkt://(?:{REGEX_SCOPE})/(?:{REGEX_WORKSPACE_SUFFIX})/(?:{REGEX_THREAD_ID})"
)
REGEX_THREAD_CURSOR = (
    rf"nkt://(?:{REGEX_SCOPE})/(?:{REGEX_WORKSPACE_SUFFIX})"
    rf"/(?:{REGEX_THREAD_ID})/(?:{REGEX_THREAD_MESSAGE_ID})"
)


##
## Identifiers
##


class ThreadId(ValidatedStr):
    """Unique ID for a thread, time-ordered.  Prefixed by ``thread-``."""

    @classmethod
    def conversation(cls, workspace: Workspace, conversation_id: str) -> Self:
        unique_id = unique_id_from_str(
            f"{workspace}/{conversation_id}",
            NUM_CHARS_THREAD_ID,
            salt=f"backend-thread-{BackendConfig.environment}",
        )
        return cls(f"thread-{unique_id}")

    @classmethod
    def generate(cls, timestamp: datetime | None = None) -> Self:
        suffix = unique_id_from_datetime(timestamp, NUM_CHARS_THREAD_ID)
        return cls(f"thread-{suffix}")

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["thread-9e7xc0000123456789abcdef"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_THREAD_ID


class ThreadMessageId(ValidatedStr):
    """Unique ID for a message within a thread, time-ordered.  Prefixed by ``msg-``."""

    @classmethod
    def generate(cls, timestamp: datetime | None = None) -> Self:
        suffix = unique_id_from_datetime(timestamp, NUM_CHARS_MESSAGE_ID)
        return cls(f"msg-{suffix}")

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["msg-9e7xc00123456789abcdef012345"]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_THREAD_MESSAGE_ID


##
## URIs
##


class ThreadUri(StructStr, frozen=True):
    """
    The URI of a thread:  nkt://{scope}/{workspace_suffix}/{thread_id}
    """

    workspace: Workspace
    thread_id: ThreadId

    @staticmethod
    def new(workspace: Workspace, thread_id: ThreadId | None = None) -> ThreadUri:
        return ThreadUri(
            workspace=workspace,
            thread_id=thread_id or ThreadId.generate(),
        )

    @classmethod
    def _parse(cls, v: str) -> ThreadUri:
        path = v.removeprefix("nkt://")
        scope_str, suffix_str, thread_str = path.split("/", 2)
        workspace_str = f"ndw://{scope_str}/{suffix_str}"
        return ThreadUri(
            workspace=Workspace.decode_part(cls, v, workspace_str),
            thread_id=ThreadId.decode_part(cls, v, thread_str),
        )

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "nkt://internal/default-unit-test/thread-9e7xc0000123456789abcdef",
            "nkt://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/default/thread-9e7xc00123456789abcdef012345",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_THREAD_URI

    def _serialize(self) -> str:
        scope_str = str(self.workspace.scope)
        suffix_str = self.workspace.as_suffix()
        return f"nkt://{scope_str}/{suffix_str}/{self.thread_id}"

    def as_kv_path(self) -> str:
        scope_str = str(self.workspace.scope)
        suffix_str = self.workspace.as_suffix()
        return f"{scope_str}/{suffix_str}/{self.thread_id}"

    def cursor(self, last_message_id: ThreadMessageId) -> ThreadCursor:
        return ThreadCursor(
            workspace=self.workspace,
            thread_id=self.thread_id,
            last_message_id=last_message_id,
        )


class ThreadCursor(StructStr, frozen=True):
    """
    A cursor into a thread:
        nkt://{scope}/{workspace_suffix}/{thread_id}/{last_message_id}

    Allows fetching only messages newer than `last_message_id`.
    """

    workspace: Workspace
    thread_id: ThreadId
    last_message_id: ThreadMessageId

    @classmethod
    def _parse(cls, v: str) -> ThreadCursor:
        path = v.removeprefix("nkt://")
        scope_str, suffix_str, thread_str, message_str = path.split("/", 3)
        workspace_str = f"ndw://{scope_str}/{suffix_str}"
        return ThreadCursor(
            workspace=Workspace.decode_part(cls, v, workspace_str),
            thread_id=ThreadId.decode_part(cls, v, thread_str),
            last_message_id=ThreadMessageId.decode_part(cls, v, message_str),
        )

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "nkt://internal/default-unit-test/thread-9e7xc0000123456789abcdef/msg-9e7xc00123456789abcdef012345",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_THREAD_CURSOR

    def _serialize(self) -> str:
        scope_str = str(self.workspace.scope)
        suffix_str = self.workspace.as_suffix()
        return f"nkt://{scope_str}/{suffix_str}/{self.thread_id}/{self.last_message_id}"

    def thread_uri(self) -> ThreadUri:
        return ThreadUri(workspace=self.workspace, thread_id=self.thread_id)
