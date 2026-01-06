import contextlib
import copy
import json
import traceback
import uuid

from fastapi import HTTPException, Response
from pydantic import BaseModel
from typing import Any, get_args, Literal, Self


##
## Data
##


ApiErrorKind = Literal["action", "normal", "retryable", "runtime"]
"""
The kind of error, which is used by the client to determine how to display
the error to the user (if at all).

- "action" errors are voluntary (e.g., user cancelled a task).
- "normal" errors are expected (e.g., '404 Not Found').
- "retryable" errors are temporary (e.g., '429 Too Many Requests').
- "runtime" errors are unexpected: they should be displayed as-is to the user
  and, once they agree, forwarded to the maintainers for debugging.
"""


class ErrorData(BaseModel, frozen=True):
    error_guid: str
    """
    The unique ID of the error, included in the error message displayed to the
    user and logged to Kibana to assist debugging.
    """
    error_kind: ApiErrorKind
    """
    The kind of error, to inform how it is displayed by the client.
    """
    extra: dict[str, Any]
    """
    Extra information about the error, to assist debugging.
    """
    stacktrace: str
    """
    The stacktrace of the original exception.
    """


class ErrorInfo(BaseModel, frozen=True):
    """
    An MCP-compatible representation of an error, to standardize error handling
    across Nandam services.
    """

    code: int
    """
    The HTTP status code of the error (default is 500).
    """
    message: str
    """
    The human-readable error message.
    """
    data: ErrorData
    """
    Standard information about the error to assist debugging.
    """

    @staticmethod
    def new(
        code: int,
        message: str,
        error_guid: str | None = None,
        error_kind: ApiErrorKind | None = None,
        extra: dict[str, Any] | None = None,
        stacktrace: str | None = None,
    ) -> "ErrorInfo":
        return ErrorInfo(
            code=code,
            message=str(message),
            data=ErrorData(
                error_guid=error_guid or str(uuid.uuid4()),
                error_kind=(
                    error_kind
                    if error_kind and error_kind in get_args(ApiErrorKind)
                    else "runtime"
                ),
                extra=extra or {},
                stacktrace=stacktrace or "",
            ),
        )


##
## Exception
##


class ApiError(Exception):
    code: int | None = None
    """
    See `ErrorData.code`.
    """
    error_guid: str
    """
    See `ErrorInfo.error_guid`.
    """
    error_kind: ApiErrorKind = "runtime"
    """
    How the error should be displayed to end-users when delivered to the client.
    For example, whether to display 'retry prompt' or 'report error' buttons.
    """
    extra_data: dict[str, Any] | None = None
    """
    Extra data about the error, to assist debugging.

    NOTE: Only defined when re-wrapping another `ApiError`.

    NOTE: `ErrorInfo.data.extra` contains the result of `as_info_extra`, which
    merges this value.
    """
    extra_stacktrace: str | None = None
    """
    Additional stacktrace that comes, not from this exception, but instead, from
    the details of an HTTP response when available.

    NOTE: Only useful when using `raise` without the `from` keyword.
    """
    include_stacktrace: bool = True
    """
    Whether the stacktrace should be included in the `ErrorInfo` object.

    NOTE: When `False`, then `extra_stacktrace` is ignored as well.
    """

    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        error_guid: str | None = None,
        error_kind: ApiErrorKind | None = None,
        extra_data: dict[str, Any] | None = None,
        extra_stacktrace: str | None = None,
        include_stacktrace: bool | None = None,
    ) -> None:
        super().__init__(message)
        self.error_guid = error_guid or str(uuid.uuid4())

        if code is not None:
            self.code = code
        if error_kind is not None:
            self.error_kind = error_kind
        if extra_data:
            self.extra_data = extra_data
        if extra_stacktrace:
            self.extra_stacktrace = str(extra_stacktrace).rstrip()
        if include_stacktrace is not None:
            self.include_stacktrace = include_stacktrace

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        *,
        copy_data: bool = True,
        copy_stacktrace: bool = False,
    ) -> Self:
        """
        Wrap an arbitrary exception into an `ApiError` (or subclass), so it can
        be handled in a standard way across the application.

        NOTE: Keep exceptions of subclasses as-is.
        """
        if isinstance(exc, cls):
            return exc
        elif isinstance(exc, ApiError):
            # When rewrapping a Nandam exception, reuse the original GUID.
            return cls(
                str(exc),
                code=cls.code or exc.code,
                error_guid=exc.error_guid,
                extra_data=exc.build_extra(redacted=False) if copy_data else None,
                extra_stacktrace=exc.build_stacktrace() if copy_stacktrace else None,
            )
        elif isinstance(exc, HTTPException):
            return cls.from_http(exc.status_code, exc.detail)
        else:
            return cls(f"Internal Server Error: {exc}")

    @classmethod
    def from_http(cls, status_code: int, response: Any) -> Self:
        """
        Convert an HTTP response from a third-party service into an `ApiError`.
        Start by assuming that it is a Nandam-style error from `as_http_response`
        and extract the relevant fields; otherwise, treat it generically.
        """
        # Attempt to parse the response body as JSON.
        if isinstance(response, str):
            with contextlib.suppress(Exception):
                response = json.loads(response)

        # Unwrap the `HTTPException` response format:
        # { "detail": { "error": "message", ... } }
        if isinstance(response, dict) and (detail := response.get("detail")):
            response = detail

        error_message = str(response)
        error_guid: str | None = None
        extra_data: dict[str, Any] | None = None
        extra_stacktrace: str | None = None

        if isinstance(response, dict):
            if error := response.get("error"):
                error_message = str(error)

            # Nandam format: { "error": str, "data": { ... } }
            if (data := response.get("data")) and isinstance(data, dict):
                error_guid = data.get("error_guid")
                extra_data = data.get("extra")
                extra_stacktrace = data.get("stacktrace")

            # Other format: { "error": str, "error_guid": str }
            else:
                error_guid = response.get("error_guid")

        return cls(
            error_message,
            code=cls.code or status_code,
            error_guid=error_guid,
            extra_data=extra_data,
            extra_stacktrace=extra_stacktrace,
        )

    def as_info(self, *, redacted: bool = False) -> ErrorInfo:
        return ErrorInfo.new(
            code=self.code or 500,
            message=str(self),
            error_guid=self.error_guid,
            error_kind=self.error_kind,
            extra=self.build_extra(redacted=redacted),
            stacktrace=self.build_stacktrace(),
        )

    def build_extra(self, redacted: bool) -> dict[str, Any]:
        return copy.deepcopy(self.extra_data) or {}

    def build_stacktrace(self) -> str:
        if self.include_stacktrace:
            stacktrace = "\n".join(traceback.format_exception(self)).rstrip()
            return (
                f"{stacktrace}\n\n---\n\n{self.extra_stacktrace}"
                if self.extra_stacktrace
                else stacktrace
            )
        else:
            return ""

    def as_http_detail(self) -> dict[str, Any]:
        error_info = self.as_info()
        return {
            "error": error_info.message,
            "data": error_info.data.model_dump(),
        }

    def as_http_exception(self) -> HTTPException:
        """
        NOTE: When converting an `ApiError` into an HTTP exception,

        - Apply FastAPI conventions, by putting the message in `detail.error`;
        - Apply MCP conventions, by putting extra information in `detail.data`;
        - Then use the "code" of the error as the HTTP status code.
        """
        return HTTPException(
            status_code=self.code or 500,
            detail=self.as_http_detail(),
        )

    def as_http_response(self) -> Response:
        """
        NOTE: When converting an `ApiError` into an HTTP response,

        - Apply FastAPI conventions, by putting the message in `detail.error`;
        - Apply MCP conventions, by putting extra information in `detail.data`;
        - Then use the "code" of the error as the HTTP status code.
        """
        return Response(
            content={"detail": self.as_http_detail()},
            status_code=self.code or 500,
            media_type="application/json",
        )


##
## Basics
##


class AuthorizationError(ApiError):
    """Raised when the process roles do not allow it to perform an action."""

    code: int | None = 403
    error_kind: ApiErrorKind = "normal"
    include_stacktrace: bool = False

    @staticmethod
    def forbidden(message: str) -> "AuthorizationError":
        return AuthorizationError(f"Forbidden: {message}", code=403)

    @staticmethod
    def unauthorized(message: str) -> "AuthorizationError":
        return AuthorizationError(f"Unauthorized: {message}", code=401)


class BadRequestError(ApiError):
    code: int | None = 400
    error_kind: ApiErrorKind = "normal"
    include_stacktrace: bool = True

    @staticmethod
    def new(reason: str) -> "BadRequestError":
        return BadRequestError(f"Bad Request: {reason}")

    @staticmethod
    def capability(suffix: str) -> "BadRequestError":
        return BadRequestError(f"Bad Request: unsupported capability: {suffix}")


class IntegrationError(ApiError):
    """Raised when trying to use an Integration that does not exist."""

    code: int | None = 500
    error_kind: ApiErrorKind = "runtime"
    include_stacktrace: bool = True

    @staticmethod
    def bad_connector(realm: str, message: str) -> "IntegrationError":
        return IntegrationError(
            f"Internal Server Error: bad connector '{realm}': {message}"
        )

    @staticmethod
    def duplicate(name: str, type_before: type, type_after: type) -> "IntegrationError":
        return IntegrationError(
            f"Internal Server Error: duplicate integration '{name}' "
            f"with types {type_before.__name__} -> {type_after.__name__}"
        )

    @staticmethod
    def not_found(name: str, type_: type) -> "IntegrationError":
        return IntegrationError(
            f"Internal Server Error: missing integration '{name}' "
            f"with type {type_.__name__}"
        )


class LlmError(ApiError):
    """
    Raised when the LLM fails to generate a response or when it generates a
    response that does not match the expected format.

    For example, raised when the Content Management Policy of Azure is triggered
    by the prompt or the generated response.
    """

    code: int | None = 500
    error_kind: ApiErrorKind = "retryable"
    include_stacktrace: bool = True
    completion: str | None = None

    def build_extra(self, redacted: bool) -> dict[str, Any]:
        result = super().build_extra(redacted=redacted)
        if not redacted and self.completion:
            result["completion"] = self.completion
        return result

    @staticmethod
    def content_policy() -> "LlmError":
        return LlmError(
            "The response was filtered due to the prompt triggering the vendor's "
            "content management policy. Please modify your prompt and retry.",
        )

    @staticmethod
    def bad_completion(reason: str, completion: str | None) -> "LlmError":
        error = LlmError(f"Malformed completion: {reason}")
        error.completion = completion
        return error

    @staticmethod
    def bad_request(reason: str) -> "LlmError":
        return LlmError(
            f"Malformed request: {reason}",
            error_kind="runtime",
        )

    @staticmethod
    def empty_completion() -> "LlmError":
        return LlmError("Malformed completion: empty response")

    @staticmethod
    def network_error(exc: Exception) -> "LlmError":
        return LlmError(f"Unexpected LLM error: {exc}")


class StoppedError(ApiError):
    """
    Raised when a process is cancelled by the user (using the "stop" command) or
    by the system (e.g., the service needs to shutdown).
    """

    code: int | None = 418
    error_kind: ApiErrorKind = "action"
    include_stacktrace: bool = False
    reason: Literal["stopped", "timeout"]

    @staticmethod
    def new(reason: Literal["stopped", "timeout"]) -> "StoppedError":
        error = StoppedError(f"{reason.upper()}")
        error.reason = reason
        return error

    @staticmethod
    def stopped() -> "StoppedError":
        return StoppedError.new("stopped")

    @staticmethod
    def timeout() -> "StoppedError":
        return StoppedError.new("timeout")


class UnavailableError(ApiError):
    """
    Raised when a resource cannot be loaded (either not found or forbidden).
    To avoid leaking information about which resources exist, we omit both why
    the error occurred and its stacktrace.
    """

    code: int | None = 404
    error_kind: ApiErrorKind = "normal"
    include_stacktrace: bool = False

    @staticmethod
    def new() -> "UnavailableError":
        return UnavailableError("Not Found: unavailable")

    @staticmethod
    def cache() -> "UnavailableError":
        return UnavailableError(
            "Not Found: unavailable",
            error_guid="00000000-0000-0000-0000-000000000000",
        )

    @staticmethod
    def stub() -> "UnavailableError":
        return UnavailableError(
            "Not Found: unavailable",
            error_guid="00000000-0000-0000-0000-ffffffffffff",
        )
