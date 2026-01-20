import contextlib
import copy

from datetime import datetime, UTC
from pydantic import BaseModel, Field, PrivateAttr
from typing import Annotated, Any, Literal

from base.core.exceptions import ApiError, StoppedError, ErrorInfo
from base.core.values import as_json, as_yaml
from base.models.content import ContentText, PartText, TextPart
from base.strings.auth import RequestId
from base.strings.process import ProcessUri, ProcessName
from base.strings.resource import Reference

from backend.models.exceptions import BadProcessError


##
## Result
##


class ProcessSuccess(BaseModel, frozen=True):
    type: Literal["success"] = "success"
    value: dict[str, Any]

    def as_str(self) -> str:
        return self.as_text().as_str()

    def get_content(self) -> ContentText | None:
        if not (content := self.value.get("content")):
            return None

        if isinstance(content, str):
            if self.value.get("content_mode") in ("data", "markdown"):
                return ContentText.parse(content, mode=self.value["content_mode"])
            else:
                return ContentText.new_plain(content)
        elif isinstance(content, dict):
            with contextlib.suppress(Exception):
                return ContentText.model_validate(content)

        return None

    def as_dict(self) -> tuple[dict[str, Any], list[Reference]]:
        value, content = self.as_split()
        references: list[Reference] = []
        if content:
            value["content"] = content.as_str()
            references.extend(content.dep_embeds())
        return value, references

    def as_split(self) -> tuple[dict[str, Any], ContentText | None]:
        value = copy.deepcopy(self.value)
        if content := self.get_content():
            raw_content = value.pop("content", None)
            if isinstance(raw_content, str):
                value.pop("content_mode", None)
        return value, content

    def as_text(self) -> ContentText:
        value, content = self.as_split()
        rendered: list[TextPart] = []

        if value:
            rendered.append(PartText.new(f"<value>\n{as_yaml(value)}\n</value>", "\n"))

        if content:
            rendered.append(PartText.new("<content>", "\n", "\n-force"))
            rendered.extend(content.parts)
            rendered.append(PartText.new("</content>", "\n-force", "\n"))

        return ContentText.new(parts=rendered)


class ProcessStopped(BaseModel, frozen=True):
    type: Literal["stopped"] = "stopped"
    reason: Literal["stopped", "timeout"]

    def error_message(self) -> str:
        match self.reason:
            case "stopped":
                return "Stopped: the user or runtime cancelled the task"
            case "timeout":
                return "Timeout: the task did not produce a result"

    def as_str(self) -> str:
        return f"<error>\n{self.error_message()}\n</error>"

    def as_text(self) -> ContentText:
        return ContentText.new_plain(self.as_str(), "\n")


class ProcessFailure(BaseModel, frozen=True):
    type: Literal["failure"] = "failure"
    error: ErrorInfo
    _exception: Exception | None = PrivateAttr(default=None)

    @staticmethod
    def from_exception(exc: Exception) -> "ProcessFailure | ProcessStopped":
        if isinstance(exc, StoppedError):
            return ProcessStopped(reason=exc.reason)
        else:
            result = ProcessFailure(error=ApiError.from_exception(exc).as_info())
            return result.model_copy(update={"_exception": exc})

    def error_message(self) -> str:
        message = self.error.message or f"Runtime Error: {self.error.data.stacktrace}"
        if extra := self.error.data.extra:
            message += f"\nDetails: {as_json(extra)}"
        return message

    def as_str(self) -> str:
        return f"<error>\n{self.error_message()}\n</error>"

    def as_text(self) -> ContentText:
        return ContentText.new_plain(self.as_str(), "\n")


ProcessResult = ProcessSuccess | ProcessStopped | ProcessFailure
ProcessResult_ = Annotated[ProcessResult, Field(discriminator="type")]


##
## Status
##


class ProcessStatus(BaseModel):
    """
    Serialized as YAML when displayed in the auditing UI.

    # Expiry detection

    Active processes are stored in a Redis SET and checked periodically via a
    cronjob in `ai-nightly`.  When `updated_at > 10m ago` on an active process,
    it indicates that the process was killed, and it is marked `ProcessExpired`.
    """

    request_id: RequestId
    """
    The unique ID of the request that instantiated the process and provides the
    necessary permission witnesses.
    """
    process_uri: ProcessUri
    """
    The unique ID of the process and of the corresponding Context.
    """
    name: ProcessName
    """
    The machine name of the process, i.e., of the corresponding agent or tool.
    """
    created_at: datetime
    """
    The time at which the process was spawned, though not necessarily when it
    started running.
    """
    updated_at: datetime
    """
    The time of the last heartbeat, or when the result was generated.
    Used to decide whether to set `result` to `ProcessExpired`: when an active
    process had no heartbeats for at least 10 minutes.
    """

    arguments: dict[str, Any]
    """
    The arguments that were passed to the process.
    """
    progress: list[dict[str, Any]]
    """
    A view on the internal state of the process for debugging and auditing.
    Changes as the process runs, sometimes destructively.
    """
    result: ProcessResult_ | None
    """
    The final output of the process.  Assigned once, when it completes.
    When `result is None`, the process is still pending / ongoing.
    """

    @staticmethod
    def new(
        request_id: RequestId,
        process_uri: ProcessUri,
        name: ProcessName,
        arguments: dict[str, Any],
        *,
        created_at: datetime | None = None,
    ) -> "ProcessStatus":
        return ProcessStatus(
            request_id=request_id,
            process_uri=process_uri,
            name=name,
            created_at=created_at or datetime.now(UTC),
            updated_at=datetime.now(UTC),
            arguments=arguments,
            progress=[],
            result=None,
        )

    def update_mut(
        self,
        progress: list[dict[str, Any]],
        result: ProcessResult | None,
    ) -> None:
        if self.result:
            raise BadProcessError.update_after_result(self.process_uri)

        self.updated_at = datetime.now(UTC)
        self.progress.extend(progress)
        self.result = result
