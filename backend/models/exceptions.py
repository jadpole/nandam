from typing import Any

from base.core.exceptions import ApiError, ApiErrorKind


class BackendError(ApiError):
    """Base class for all custom exceptions in the Backend."""


class BadPersonaError(BackendError):
    """Raised when the user misconfigures a Persona."""

    code: int | None = 400
    error_kind: ApiErrorKind = "normal"

    @staticmethod
    def unknown_agent(name: str) -> "BadPersonaError":
        return BadPersonaError(f"Unknown agent: {name}")

    @staticmethod
    def unknown_model(name: str) -> "BadPersonaError":
        return BadPersonaError(f"Unknown model: {name}")

    @staticmethod
    def unknown_persona(persona_id: str) -> "BadPersonaError":
        return BadPersonaError(f"Unknown persona: {persona_id}")

    @staticmethod
    def unsupported_field(agent: str, field: str) -> "BadPersonaError":
        return BadPersonaError(f"Cannot use the {agent} agent with {field}")


class BadToolError(BackendError):
    """Raised when the tool is not found."""

    code: int | None = 400
    error_kind: ApiErrorKind = "action"

    @staticmethod
    def bad_arguments(name: str, reason: str) -> "BadToolError":
        return BadToolError(f"Bad Request: invalid arguments for tool {name}: {reason}")

    @staticmethod
    def bad_progress(name: str, reason: str) -> "BadToolError":
        return BadToolError(f"Bad Request: invalid progress for tool {name}: {reason}")

    @staticmethod
    def bad_return(name: str, reason: str) -> "BadToolError":
        return BadToolError(f"Bad Request: invalid return for tool {name}: {reason}")

    @staticmethod
    def duplicate(name: str) -> "BadToolError":
        return BadToolError(f"Bad Request: duplicate tool: {name}")

    @staticmethod
    def not_found(name: str) -> "BadToolError":
        return BadToolError(f"Bad Request: unknown tool: {name}")


class BadProcessError(BackendError):
    """Raised when trying to read a process status that does not exist."""

    code: int | None = 500
    error_kind: ApiErrorKind = "runtime"

    @staticmethod
    def duplicate(process_uri: Any) -> "BadProcessError":
        return BadProcessError(
            f"Internal Server Error: duplicate process: {process_uri}"
        )

    @staticmethod
    def invalid_status(process_uri: Any) -> "BadProcessError":
        return BadProcessError(
            f"Internal Server Error: invalid process status: {process_uri}"
        )

    @staticmethod
    def update_after_result(process_uri: Any) -> "BadProcessError":
        return BadProcessError(
            f"Internal Server Error: process status updated after result: {process_uri}"
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
    def context_limit_exceeded() -> "LlmError":
        return LlmError.bad_request("context limit exceeded")

    @staticmethod
    def empty_completion() -> "LlmError":
        return LlmError("Malformed completion: empty response")

    @staticmethod
    def incompatible_model(
        model_before: str,
        model_after: str,
        reason: str,
    ) -> "LlmError":
        return LlmError(
            f"Cannot use {model_before} history in {model_after} request: {reason}"
        )

    @staticmethod
    def network_error(exc: Exception) -> "LlmError":
        return LlmError(f"Unexpected LLM error: {exc}")


class ProcessNotFoundError(BackendError):
    """Raised when trying to read a process status that does not exist."""

    code: int | None = 404
    error_kind: ApiErrorKind = "normal"

    @staticmethod
    def from_uri(process_uri: Any) -> "ProcessNotFoundError":
        return ProcessNotFoundError(f"Not Found: unknown process: {process_uri}")

    @staticmethod
    def remote() -> "ProcessNotFoundError":
        return ProcessNotFoundError("Not Found: unknown process: invalid remote ID")

    @staticmethod
    def remote_expired() -> "ProcessNotFoundError":
        return ProcessNotFoundError("Not Found: unknown process: expired remote ID")


class UserNotFoundError(BackendError):
    """
    Raised when trying to read or edit the profile of a user whose ID has no
    corresponding Profile.  Should never occur in practice: it is a bug.
    """

    code: int | None = 404
    error_kind: ApiErrorKind = "normal"

    @staticmethod
    def new() -> "UserNotFoundError":
        return UserNotFoundError(
            "Unable to read your profile. "
            "It should be created the first time you send a message to Nandam."
        )
