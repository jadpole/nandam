from typing import Any

from fastapi import HTTPException

from base.core.exceptions import (
    ApiError,
    ApiErrorKind,
    BadRequestError,
    LlmError,
    UnavailableError,
)
from base.core.values import as_yaml


##
## ApiError.__init__
##


def _run_api_error_check(
    error: ApiError,
    *,
    message: str,
    code: int,
    error_kind: ApiErrorKind,
    extra: dict[str, Any],
) -> None:
    error_info = error.as_info()
    print(f"<error_info>\n{as_yaml(error_info)}\n</error_info>")

    assert str(error) == message
    assert error.error_guid  # generated UUIDv4

    assert error_info.code == code
    assert error_info.message == message
    assert error_info.data.error_guid == error.error_guid
    assert error_info.data.error_kind == error_kind
    assert error_info.data.extra == extra or {}


def test_api_error_init_base_simple():
    error = ApiError("error message")
    assert error.extra_data is None
    assert error.extra_stacktrace is None

    _run_api_error_check(
        error,
        message="error message",
        code=500,
        error_kind="runtime",
        extra={},
    )
    assert error.as_info().data.stacktrace == (
        "base.core.exceptions.ApiError: error message"
    )


def test_api_error_init_base_extra():
    custom_guid = "test-guid"
    error = ApiError(
        "error message",
        code=418,
        error_guid=custom_guid,
        extra_data={"field": "value"},
        extra_stacktrace="extra stacktrace",
    )
    assert error.error_guid == custom_guid
    assert error.extra_data == {"field": "value"}
    assert error.extra_stacktrace == "extra stacktrace"

    _run_api_error_check(
        error,
        message="error message",
        code=418,
        error_kind="runtime",
        extra={"field": "value"},
    )
    assert error.as_info().data.stacktrace == (
        "base.core.exceptions.ApiError: error message\n\n---\n\nextra stacktrace"
    )


def test_api_error_init_bad_request():
    error = BadRequestError.new("some reason")
    _run_api_error_check(
        error,
        message="Bad Request: some reason",
        code=400,
        error_kind="normal",
        extra={},
    )
    assert error.as_info().data.stacktrace == (
        "base.core.exceptions.BadRequestError: Bad Request: some reason"
    )


def test_api_error_init_llm():
    error = LlmError.bad_completion("some reason", "some completion")
    _run_api_error_check(
        error,
        message="Malformed completion: some reason",
        code=500,
        error_kind="retryable",
        extra={"completion": "some completion"},
    )
    assert error.as_info().data.stacktrace == (
        "base.core.exceptions.LlmError: Malformed completion: some reason"
    )


def test_api_error_init_unavailable():
    error = UnavailableError.new()
    _run_api_error_check(
        error,
        message="Not Found: unavailable",
        code=404,
        error_kind="normal",
        extra={},
    )
    assert error.as_info().data.stacktrace == ""


##
## ApiError.from_exception
##


def _run_api_error_from_exception(
    source: Exception,
    target: type[ApiError],
) -> ApiError:
    try:
        try:
            raise source
        except Exception as exc:
            raise target.from_exception(exc) from exc
    except ApiError as exc:
        return exc


def test_api_error_from_exception_value_error():
    error = _run_api_error_from_exception(
        ValueError("Invalid value"),
        ApiError,
    )
    stacktrace = error.as_info().data.stacktrace

    _run_api_error_check(
        error,
        message="Internal Server Error: Invalid value",
        code=500,
        error_kind="runtime",
        extra={},
    )
    assert (
        "ValueError: Invalid value"
        "\n\n\nThe above exception was the direct cause of the following exception:"
        "\n\n\nTraceback (most recent call last):" in stacktrace
    )
    assert stacktrace.endswith(
        "base.core.exceptions.ApiError: Internal Server Error: Invalid value"
    )


def test_api_error_from_exception_http_exception():
    error = _run_api_error_from_exception(
        HTTPException(status_code=403, detail="Forbidden"),
        ApiError,
    )
    stacktrace = error.as_info().data.stacktrace

    _run_api_error_check(
        error,
        message="Forbidden",
        code=403,
        error_kind="runtime",
        extra={},
    )
    assert "HTTPException: 403: Forbidden" in stacktrace


def test_api_error_from_exception_self():
    original = UnavailableError.new()
    error = _run_api_error_from_exception(original, UnavailableError)
    print(f"<error_info>\n{as_yaml(error.as_info())}\n</error_info>")
    assert original == error


def test_api_error_from_exception_subclass():
    original = UnavailableError.new()
    error = _run_api_error_from_exception(original, ApiError)
    print(f"<error_info>\n{as_yaml(error.as_info())}\n</error_info>")
    assert original == error


##
## ApiError.as_http_exception
##


def test_from_http_after_as_http_exception():
    original = UnavailableError.new()
    print(f"<original_info>\n{as_yaml(original.as_info())}\n</original_info>")

    http_exc = original.as_http_exception()
    error = ApiError.from_http(http_exc.status_code, {"detail": http_exc.detail})
    stacktrace = error.as_info().data.stacktrace
    _run_api_error_check(
        error,
        message="Not Found: unavailable",
        code=404,
        error_kind="runtime",
        extra={},
    )

    # NOTE: We preserve the error GUID.
    # NOTE: We preserve the original status code (not overridden by ApiError).
    # NOTE: We correctly discarded the original stacktrace (UnavailableError),
    # but still preserved the new error's stacktrace (ApiError), as expected.
    assert error.error_guid == original.error_guid
    assert stacktrace == "base.core.exceptions.ApiError: Not Found: unavailable"


def test_from_http_after_as_http_exception_same_class():
    original = LlmError.bad_completion("some reason", "some completion")
    print(f"<original_info>\n{as_yaml(original.as_info())}\n</original_info>")

    http_exc = original.as_http_exception()
    error = LlmError.from_http(http_exc.status_code, {"detail": http_exc.detail})
    stacktrace = error.as_info().data.stacktrace
    _run_api_error_check(
        error,
        message="Malformed completion: some reason",
        code=500,
        error_kind="retryable",
        extra={"completion": "some completion"},
    )

    # NOTE: We preserve the error GUID.
    # NOTE: The stacktrace from both services (client and server) are preserved.
    assert error.error_guid == original.error_guid
    assert (
        stacktrace
        == """\
base.core.exceptions.LlmError: Malformed completion: some reason

---

base.core.exceptions.LlmError: Malformed completion: some reason\
"""
    )


def test_from_http_after_as_http_exception_disjoint_class():
    original = LlmError.bad_completion("some reason", "some completion")
    print(f"<original_info>\n{as_yaml(original.as_info())}\n</original_info>")

    http_exc = original.as_http_exception()
    error = UnavailableError.from_http(
        http_exc.status_code, {"detail": http_exc.detail}
    )
    stacktrace = error.as_info().data.stacktrace
    _run_api_error_check(
        error,
        message="Malformed completion: some reason",
        code=404,
        error_kind="normal",
        extra={"completion": "some completion"},
    )

    # NOTE: We preserve the error GUID and message.
    # NOTE: We use the status code to 404 (overridden by UnavailableError).
    # NOTE: The stacktrace from both services (client and server) are omitted
    # when configured in the wrapper (UnavailableError).
    assert error.error_guid == original.error_guid
    assert stacktrace == ""
