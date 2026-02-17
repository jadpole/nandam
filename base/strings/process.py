from datetime import datetime
from typing import Self

from base.core.strings import StructStr, ValidatedStr
from base.core.unique_id import BASE36_CHARS, unique_id_from_datetime
from base.strings.file import FileName
from base.strings.scope import REGEX_SCOPE, REGEX_WORKSPACE_SUFFIX, Workspace

NUM_CHARS_PROCESS_ID = 24
REGEX_PROCESS_ID = r"[a-zA-Z0-9]{24,}"
REGEX_PROCESS_NAME = r"[a-z][a-z0-9]+(?:_[a-z0-9]+)*"
REGEX_PROCESS_URI = (
    rf"ndp://(?:{REGEX_SCOPE})/(?:{REGEX_WORKSPACE_SUFFIX})(?:/{REGEX_PROCESS_ID})+"
)


##
## Identifiers
##


class ProcessName(ValidatedStr):
    """
    The unique ID of a process protocol.

    Used in the `ProcessProtocol` definition and in `ProcessStatus` to attribute
    each process status to a given protocol.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "ask_docs",
            "generate_image",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_PROCESS_NAME


class ProcessId(ValidatedStr):
    """
    The unique ID of a process across all of Nandam.

    It is used, notably, to represent tool call IDs.  To be OpenAI-compatible,
    when generating an internal process ID, we produce a 24 characters ID, but
    contrary to OpenAI, this ID is time-ordered so that, when listing processes,
    they appear in order.

    However, to provide maximum flexibility, tool call IDs from other providers
    can also be converted back and forth into a process ID.

    NOTE: As a quick survey of the supported providers:
    - OpenAI uses 24 alphanumeric characters;
    - Google uses 32 hexadecimal characters (a UUID);
    - Anthropic uses 23 alphanumeric characters.
    """

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_PROCESS_ID

    @staticmethod
    def from_native(tool_call_id: str) -> ProcessId:
        cleaned = (
            tool_call_id.removeprefix("call_")
            .removeprefix("toolu_vrtx_")  # Claude on GCP
            .removeprefix("toolu_")  # Claude API
            .replace("-", "")
        )
        if len(cleaned) < NUM_CHARS_PROCESS_ID:
            cleaned += "0" * (NUM_CHARS_PROCESS_ID - len(cleaned))
        return ProcessId.decode(cleaned)

    @classmethod
    def generate(cls, timestamp: datetime | None = None) -> Self:
        """
        Generate a unique ID, using the current timestamp for time ordering if
        none is provided.

        NOTE: Although `UniqueId` is case-sensitive, supporting IDs from other
        services, the generated ID is always lowercase.
        """
        return cls(unique_id_from_datetime(timestamp, NUM_CHARS_PROCESS_ID))

    @staticmethod
    def stub(suffix: str = "") -> ProcessId:
        suffix_len = 4
        remaining = suffix_len - len(suffix)
        assert remaining >= 0
        if remaining:
            suffix = "0" * remaining + suffix

        return ProcessId.decode("00000000000000000000" + suffix)

    @staticmethod
    def temp(suffix: str = "") -> ProcessId:
        suffix_len = 4
        remaining = suffix_len - len(suffix)
        assert remaining >= 0
        if remaining:
            suffix = "0" * remaining + suffix

        return ProcessId.decode("00000000000000000000" + suffix)

    def as_native_gemini(self) -> str:
        """
        NOTE: When using GCP for inference, the prefix include "vrtx" (Vertex).
        """
        # Insert missing values in a deterministic way.
        num_chars = 32
        corrected = (
            self + "0" * (num_chars - len(self))
            if len(self) < num_chars
            else self[:num_chars]
        )

        # Replace non-hex characters in a deterministic way by looping back.
        hex_chars = "0123456789abcdef"
        corrected = "".join(
            hex_chars[BASE36_CHARS.index(c) % 16] for c in corrected.lower()
        )

        # Format as a UUID.
        return "-".join(
            [
                corrected[0:8],
                corrected[8:12],
                corrected[12:16],
                corrected[16:20],
                corrected[20:32],
            ]
        )

    def as_native_openai(self) -> str:
        num_chars = 24
        corrected = (
            self + "0" * (num_chars - len(self))
            if len(self) < num_chars
            else self[:num_chars]
        )
        return f"call_{corrected}"

    def as_native_vertexai(self) -> str:
        """
        NOTE: When using GCP for inference, the prefix include "vrtx" (Vertex).
        """
        num_chars = 23
        corrected = (
            self + "0" * (num_chars - len(self))
            if len(self) < num_chars
            else self[:num_chars]
        )
        return f"toolu_vrtx_{corrected}"


##
## URI
##


class ProcessUri(StructStr, frozen=True):
    """
    The URI of a process, which includes:

    - The workspace in which the process runs;
    - The chain of IDs for the parent processes that eventually spawned it;
    - The unique ID of the process as its last path component.
    """

    workspace: Workspace
    parent_ids: list[ProcessId]
    process_id: ProcessId

    @staticmethod
    def root(workspace: Workspace, process_id: ProcessId) -> ProcessUri:
        return ProcessUri(
            workspace=workspace,
            parent_ids=[],
            process_id=process_id,
        )

    @staticmethod
    def stub(suffix: str = "") -> ProcessUri:
        process_id = ProcessId.stub(suffix)
        return ProcessUri.decode(f"ndp://internal/default-unit-test/{process_id}")

    @classmethod
    def _parse(cls, v: str) -> ProcessUri:
        process_path = v.removeprefix("ndp://")
        scope_str, suffix_str, *parent_strs, process_str = process_path.split("/")
        workspace_str = f"ndw://{scope_str}/{suffix_str}"
        return ProcessUri(
            workspace=Workspace.decode_part(cls, v, workspace_str),
            parent_ids=[ProcessId.decode_part(cls, v, p) for p in parent_strs],
            process_id=ProcessId.decode_part(cls, v, process_str),
        )

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "ndp://internal/default-ai-exporter-prod/9e7xc0000123456789abcdef",
            "ndp://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/default/9e7xc0000123456789abcdef",
            "ndp://private-0123456789abcdefghijklmnopqrstuvwxyz/channel-0123456789abcdefghij/9e7xc0000123456789abcdef/9e7xd0fedcba987654321000",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_PROCESS_URI

    def _serialize(self) -> str:
        return "ndp://" + self.as_kv_path()

    def as_filenames(self) -> list[FileName]:
        return [
            FileName.decode(part)
            for part in [
                str(self.workspace.scope),
                self.workspace.as_suffix(),
                *self.parent_ids,
                self.process_id,
            ]
        ]

    def as_kv_path(self) -> str:
        return "/".join(
            [
                str(self.workspace.scope),
                self.workspace.as_suffix(),
                *self.parent_ids,
                self.process_id,
            ]
        )

    def parent(self) -> ProcessUri | None:
        if not self.parent_ids:
            return None
        return ProcessUri(
            workspace=self.workspace,
            parent_ids=self.parent_ids[:-1],
            process_id=self.parent_ids[-1],
        )

    def child(self, child_process_id: ProcessId) -> ProcessUri:
        return ProcessUri(
            workspace=self.workspace,
            parent_ids=[*self.parent_ids, self.process_id],
            process_id=child_process_id,
        )

    def is_child_or(self, parent_or_self: Workspace | ProcessUri) -> bool:
        if isinstance(parent_or_self, Workspace):
            return self.workspace == parent_or_self
        else:
            self_str = str(self)
            parent_str = str(parent_or_self)
            return self_str == parent_str or self_str.startswith(f"{parent_str}/")
