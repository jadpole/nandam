# Server Primitives

The `base/server` module provides shared infrastructure for all Nandam services: authentication, lifecycle management, metrics, and status signaling.

## Service Lifecycle

### BaseLifespan

Each service extends `BaseLifespan` to manage startup and shutdown:

```python
@dataclass(kw_only=True)
class BaseLifespan:
    background_tasks: list[asyncio.Task]

    async def on_startup(self, app_name: str) -> None:
        # Configure logging
        # Start background initialization
        ...

    async def on_shutdown(self) -> None:
        # Signal tasks to stop
        # Wait for graceful completion
        # Send terminated signal
        ...
```

Subclasses override:
- `_handle_startup_background()`: Initialize connections, caches, etc.
- `_handle_shutdown_background()`: Clean up resources

### Status Signals

Services communicate their state via signals:

| Signal | State | Meaning |
|--------|-------|---------|
| — | `loading` | Service is starting up |
| `send_ready()` | `ok` | Ready to receive requests |
| `send_sigterm()` | `stopping` | Graceful shutdown initiated |
| `send_terminated()` | `terminated` | Ready to be removed |

Kubernetes probes use these states:

```python
def status_code_live(self) -> int:
    # "loading", "ok", "stopping" → 200 (keep alive)
    # "terminated" → 500 (remove pod)

def status_code_ready(self) -> int:
    # "ok" → 200 (accept traffic)
    # "loading", "stopping" → 503 (prefer other replicas)
    # "terminated" → 500 (remove pod)
```

### Graceful Shutdown

Background tasks check `app_status()` and exit when stopping:

```python
async def background_worker():
    while await loop_if_alive(delay_secs=10.0):
        # Process work
        ...
```

The `with_timeout` helper cancels operations when the service stops:

```python
result = await with_timeout(task, timeout=30.0)
if isinstance(result, StoppedError):
    return  # Service is shutting down
```

## Authentication

### NdAuth

Combines client and user authentication:

```python
class NdAuth(BaseModel, frozen=True):
    client: ClientAuth      # Which service/app is calling
    user: UserAuth | None   # Which user (if authenticated)
    scope: Scope            # Access scope
    request_id: RequestId   # For tracing
```

### Client Authentication

Services authenticate using Basic auth in `X-Authorization-Client`:

```
X-Authorization-Client: Basic base64(release:secret)
```

The `release` identifies the calling service (e.g., `teams-client`, `local-dev`).

Each client has configured capabilities:
- `supports_keycloak`: Whether user tokens are required/trusted
- `supports_internal`: Whether internal scope is allowed
- `supports_personal`: Whether personal scope is allowed
- `supports_private`: Whether private scope is allowed

### User Authentication

Users authenticate via JWT in `X-Authorization-User`:

```
X-Authorization-User: Bearer <jwt>
```

Two JWT types:
- **Keycloak (RS256)**: External users via Azure AD
- **Internal (HS256)**: Service-to-service forwarding

### Scopes

Scopes control resource access:

| Scope | Description |
|-------|-------------|
| `ScopeInternal` | Service-level access (no user) |
| `ScopePrivate` | Isolated namespace per client+key |
| `ScopePersonal` | User's personal resources |
| `ScopeMsGroup` | Microsoft 365 group resources |

The scope determines which resources a request can access and is validated against client capabilities.

## Metrics

### MetricsMiddleware

Collects HTTP request metrics compatible with Prometheus:

```python
REQUEST_METRIC = Histogram(
    "http_request_duration_seconds",
    "duration histogram of http responses",
    labelnames=["method", "path", "status_code"],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)
```

Metrics are exposed at `/metrics` for Prometheus scraping.

### Custom Metrics

Services can define additional metrics using `prometheus_client`:

```python
from prometheus_client import Counter, Histogram

DOCUMENTS_PROCESSED = Counter(
    "documents_processed_total",
    "Number of documents processed",
    labelnames=["connector"],
)
```

## Logging

### Configuration

Logging is configured per-service in `on_startup`:

```python
setup_logging(
    app_name="knowledge",
    log_level=KnowledgeConfig.log_level,
)
```

- **Kubernetes**: JSON format for log aggregation
- **Local**: Colored text format for development

### Log Routing

- `WARNING` and above → stderr
- `DEBUG` to `INFO` → stdout (when enabled)

This allows Kubernetes to separate error logs from info logs.

## Request Context

### Headers

Standard headers for cross-service communication:

| Header | Purpose |
|--------|---------|
| `X-Authorization-Client` | Service authentication |
| `X-Authorization-User` | User authentication |
| `X-Request-Id` | Request tracing |
| `X-Request-Scope` | Access scope override |
| `X-User-Id` | User ID (trusted clients only) |

### Forwarding Auth

When making downstream requests, forward authentication:

```python
headers = auth.as_headers()
# Returns: {
#   "x-authorization-client": "Basic ...",
#   "x-authorization-user": "Bearer ...",
#   "x-request-id": "...",
# }
```

## Health Endpoints

Standard endpoints for Kubernetes:

```
GET /health/live   → {"status": "ok", "version": "1.0.0"}
GET /health/ready  → {"status": "ok", "version": "1.0.0"}
GET /metrics       → Prometheus metrics
```

## Service Pattern

A typical service setup:

```python
app = FastAPI(
    title="Nandam Knowledge",
    version=KnowledgeConfig.version,
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)
app.add_middleware(CORSMiddleware, ...)

app.include_router(kubernetes.router)  # Health endpoints
app.include_router(query.router)       # Business logic

@app.exception_handler(ApiError)
def api_error_handler(_request: Request, exc: ApiError) -> Response:
    logger.error(as_json({"api_error": exc.as_info(redacted=True)}))
    return exc.as_http_response()
```
