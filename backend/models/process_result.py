import contextlib
import copy

from pydantic import BaseModel, Field, PrivateAttr, SerializeAsAny
from typing import Annotated, Any, Literal

from base.core.exceptions import ApiError, StoppedError, ErrorInfo
from base.core.values import as_json, as_value, as_yaml
from base.models.content import ContentText, PartText, TextPart
from base.models.rendered import Rendered
from base.resources.observation import Observation


##
## Result
##


class ProcessSuccess[Ret: BaseModel = Any](BaseModel, frozen=True):
    type: Literal["success"] = "success"
    value: SerializeAsAny[Ret]

    def get_content(self) -> ContentText | None:
        content: Any = None
        if isinstance(self.value, BaseModel) and hasattr(self.value, "content"):
            content = self.value.content  # type: ignore
        elif isinstance(self.value, dict):
            content = self.value.get("content")

        if not content:
            return None
        elif isinstance(content, ContentText):
            return content
        elif isinstance(content, dict):
            with contextlib.suppress(Exception):
                return ContentText.model_validate(content)
        elif isinstance(content, str):
            return ContentText.parse(content)

        return None

    def as_dict(self) -> dict[str, Any]:
        if isinstance(self.value, BaseModel):
            return as_value(self.value)
        elif isinstance(self.value, dict):
            return copy.deepcopy(self.value)
        else:
            raise TypeError(f"ProcessSuccess.as_dict: bad value: {self.value}")

    def as_split(self) -> tuple[dict[str, Any], ContentText | None]:
        value = self.as_dict()
        content = self.get_content()
        if content:
            value.pop("content", None)
        return value, content

    def as_str(self) -> str:
        return self.as_text().as_str()

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

    def as_exception(self) -> Exception:
        return StoppedError.new(self.reason)

    def as_str(self) -> str:
        return f"<error>\n{self.error_message()}\n</error>"

    def as_text(self) -> ContentText:
        return ContentText.new_plain(self.as_str(), "\n")


class ProcessFailure(BaseModel, frozen=True):
    type: Literal["failure"] = "failure"
    error: ErrorInfo
    _exception: Exception | None = PrivateAttr(default=None)

    @staticmethod
    def from_info(error: ErrorInfo) -> ProcessFailure | ProcessStopped:
        if (
            error.code == 418  # noqa: PLR2004
            and (reason := error.message.lower())
            and reason in ("stopped", "timeout")
        ):
            return ProcessStopped(reason=reason)
        else:
            return ProcessFailure(error=error)

    @staticmethod
    def from_exception(exc: Exception) -> ProcessFailure | ProcessStopped:
        if isinstance(exc, StoppedError):
            return ProcessStopped(reason=exc.reason)
        else:
            result = ProcessFailure(error=ApiError.from_exception(exc).as_info())
            return result.model_copy(update={"_exception": exc})

    def as_exception(self) -> Exception:
        if self._exception:
            return self._exception
        else:
            return ApiError(
                message=self.error.message,
                code=self.error.code,
                error_guid=self.error.data.error_guid,
                error_kind=self.error.data.error_kind,
                extra_data=copy.deepcopy(self.error.data.extra),
                extra_stacktrace=self.error.data.stacktrace,
            )

    def error_message(self) -> str:
        message = self.error.message or f"Runtime Error: {self.error.data.stacktrace}"
        if extra := self.error.data.extra:
            message += f"\nDetails: {as_json(extra)}"
        return message

    def as_str(self) -> str:
        return f"<error>\n{self.error_message()}\n</error>"

    def as_text(self) -> ContentText:
        return ContentText.new_plain(self.as_str(), "\n")


ProcessError = ProcessStopped | ProcessFailure
ProcessResult = ProcessSuccess | ProcessError
type ProcessResult_[Ret: BaseModel] = Annotated[
    ProcessSuccess[Ret] | ProcessError,
    Field(discriminator="type"),
]


##
## Rendered
##


class RenderedResult(BaseModel, frozen=True):
    is_error: bool
    value: dict[str, Any]
    content: Rendered | None = None

    @staticmethod
    def render(
        result: ProcessResult,
        observations: list[Observation],
    ) -> RenderedResult:
        if isinstance(result, ProcessFailure):
            value = {"code": result.error.code, "message": result.error.message}
            if extra := result.error.data.extra:
                value.update(extra)
            return RenderedResult(is_error=True, value=value, content=None)

        if isinstance(result, ProcessStopped):
            value = {"message": result.error_message()}
            return RenderedResult(is_error=True, value=value, content=None)

        content: Rendered | None = None
        value, content_text = result.as_split()
        if content_text:
            content = Rendered.render(content_text, observations)

        return RenderedResult(is_error=False, value=value, content=content)
