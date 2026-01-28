# Exception Handling

Nandam uses a structured exception system that provides consistent error responses across all services, compatible with both FastAPI conventions and MCP (Model Context Protocol) standards.

## Error Response Format

All errors produce a JSON response with this structure:

```json
{
  "detail": {
    "error": "Human-readable error message",
    "data": {
      "error_guid": "uuid-for-debugging",
      "error_kind": "normal|retryable|runtime|action",
      "extra": { ... },
      "stacktrace": "..."
    }
  }
}
```

The `error_guid` is logged to Kibana and displayed to users, enabling support to quickly locate error details.

## Error Kinds

The `error_kind` field tells clients how to handle the error:

| Kind | Description | User Action |
|------|-------------|-------------|
| `action` | Voluntary (e.g., user cancelled) | None needed |
| `normal` | Expected (e.g., 404 Not Found) | Fix input and retry |
| `retryable` | Temporary (e.g., 429 Too Many Requests) | Wait and retry |
| `runtime` | Unexpected bug | Report to maintainers |

## ApiError Base Class

All custom exceptions extend `ApiError`, which provides:

```python
class ApiError(Exception):
    code: int | None = None           # HTTP status code
    error_guid: str                   # Unique error ID
    error_kind: ApiErrorKind          # How to display to user
    include_stacktrace: bool = True   # Whether to include in response
```

### Key Methods

- `as_info()` → `ErrorInfo`: Structured error for logging/responses
- `as_http_exception()` → `HTTPException`: For FastAPI handlers
- `as_http_response()` → `Response`: Direct JSON response
- `from_exception(exc)`: Wrap arbitrary exceptions
- `from_http(status_code, response)`: Parse HTTP error responses

## Standard Exception Classes

### AuthorizationError (403)
Authentication or permission failures. No stacktrace (to avoid leaking info).

```python
raise AuthorizationError.forbidden("Cannot access this resource")
raise AuthorizationError.unauthorized("Invalid token")
```

### BadRequestError (400)
Client errors due to invalid input.

```python
raise BadRequestError.new("Missing required field 'name'")
```

### ServiceError (500)
Internal configuration errors (wrong service type, missing service, etc.).

```python
raise ServiceError.not_found("storage", SvcStorage)
raise ServiceError.bad_type("cache", expected=RedisCache, actual=MemoryCache)
```

### StoppedError (418)
Process was cancelled by user or system. Used for graceful shutdown.

```python
raise StoppedError.stopped()   # User cancelled
raise StoppedError.timeout()   # System timeout
```

### UnavailableError (404)
Resource not found or forbidden. Intentionally vague to avoid leaking information about what resources exist.

```python
raise UnavailableError.new()
```

## FastAPI Integration

Each service registers an exception handler:

```python
@app.exception_handler(ApiError)
def api_error_handler(_request: Request, exc: ApiError) -> Response:
    logger.error(as_json({"api_error": exc.as_info(redacted=True)}))
    return exc.as_http_response()
```

This ensures all `ApiError` exceptions produce consistent JSON responses.

## Propagation Across Services

When one service calls another and receives an error:

1. Parse the response using `ApiError.from_http(status_code, response_body)`
2. The original `error_guid` is preserved for tracing
3. The original stacktrace is included in `extra_stacktrace`

This allows debugging across service boundaries while maintaining a single error ID.

## ErrorInfo Model

The `ErrorInfo` model represents errors in a structured, serializable format:

```python
class ErrorInfo(BaseModel, frozen=True):
    code: int           # HTTP status code
    message: str        # Human-readable message
    data: ErrorData     # Debugging information
```

Used for:
- Logging errors to monitoring systems
- Including error details in API responses
- Passing error context between services
