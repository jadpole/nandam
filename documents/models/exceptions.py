from base.core.exceptions import ApiError


class DocumentsError(ApiError):
    """Base class for all custom exceptions in the project."""


class DownloadError(DocumentsError):
    """Raised when the download fails."""

    include_stacktrace: bool = False

    @staticmethod
    def bad_filename(filename: str) -> DownloadError:
        return DownloadError(
            f"Internal Server Error: failed to normalize filename: {filename}",
            code=404,
        )

    @staticmethod
    def bad_response(
        source: str,
        status_code: int,
        reason: str | None = None,
    ) -> DownloadError:
        message = f"Failed to download file from {source}: status code {status_code}"
        if reason:
            message += f": {reason}"
        return DownloadError(
            message,
            code=status_code,
        )

    @staticmethod
    def forbidden(reason: str) -> DownloadError:
        return DownloadError(
            f"Forbidden: {reason}",
            code=403,
        )

    @staticmethod
    def network(reason: str) -> DownloadError:
        return DownloadError(
            f"Bad Gateway: network error: {reason}",
            code=502,
        )

    @staticmethod
    def unauthorized(auth_prefix: str) -> DownloadError:
        return DownloadError(
            f"Unauthorized: missing authorization: {auth_prefix}",
            code=401,
        )

    @staticmethod
    def unexpected(reason: str) -> DownloadError:
        return DownloadError(
            f"Internal Server Error: unexpected error: {reason}",
            code=500,
            include_stacktrace=True,
        )

    @staticmethod
    def youtube(reason: str) -> DownloadError:
        return DownloadError(
            f"Not Found: failed to download YouTube video: {reason}",
            code=404,
        )


class ExtractError(DocumentsError):
    """Raised when the extract fails."""

    @staticmethod
    def expected_transcript(content_type) -> ExtractError:
        return ExtractError(
            f"Bad Request: expected audio or video file, got {content_type}",
            code=400,
        )

    @staticmethod
    def fail(method: str, reason: str) -> ExtractError:
        return ExtractError(
            f"Internal Server Error: failed to parse with {method}: {reason}",
            code=500,
        )

    @staticmethod
    def security_violation(archive_type: str, reason: str) -> ExtractError:
        return ExtractError(
            f"Forbidden: {archive_type} archive contains malicious content: {reason}",
            code=403,
        )

    @staticmethod
    def unexpected(reason: str) -> ExtractError:
        return ExtractError(
            f"Internal Server Error: unexpected extract error: {reason}",
            code=500,
        )
