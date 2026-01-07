from base.core.exceptions import ApiError, ApiErrorKind


class KnowledgeError(ApiError):
    """Base class for all custom exceptions in the project."""


class DownloadError(KnowledgeError):
    """
    The source could not be downloaded, but not because of a permission error.
    Therefore, we forward the error message to the client for debugging.
    """

    code: int | None = None
    error_kind: ApiErrorKind = "retryable"


class IngestionError(KnowledgeError):
    """
    The source was correctly downloaded, but failed to be ingested.
    Either the chunking or the metadata generation failed.
    """

    code: int | None = 500
    error_kind: ApiErrorKind = "retryable"

    def __init__(self, details: str) -> None:
        super().__init__(f"Ingestion failed: {details}")
