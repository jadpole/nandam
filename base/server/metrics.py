import re

from fastapi import FastAPI
from http import HTTPStatus
from prometheus_client import generate_latest, Histogram
from starlette.requests import Request
from starlette.types import Message, Receive, Scope, Send
from timeit import default_timer


class MetricsMiddleware:
    """
    NOTE: For compatibility with our existing dashboards, we provide default
    metrics for HTTP requests matching `express-prometheus-middleware` (NPM)
    instead of using `prometheus-fastapi-instrumentator`.
    """

    app: FastAPI

    def __init__(self, app: FastAPI):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        request = Request(scope)
        start_time = default_timer()

        path = self._get_path(request)
        status_code = 500

        async def send_wrapper(message: Message) -> None:
            if message["type"] == "http.response.start":
                nonlocal status_code
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:  # noqa: TRY203
            raise
        finally:
            if isinstance(status_code, HTTPStatus):
                status_code = status_code.value
            duration = max(default_timer() - start_time, 0)
            REQUEST_METRIC.labels(
                method=request.method,
                path=path,
                status_code=str(status_code),
            ).observe(duration)

    def _get_path(self, request: Request) -> str:
        path = request.url.path

        if re.match(r"/profile/(.*?)", path):
            path = "/profile/#secret"

        return path


def latest_metrics() -> bytes:
    return generate_latest()


REQUEST_METRIC = Histogram(
    "http_request_duration_seconds",
    "duration histogram of http responses labeled with: status_code, method, path",
    labelnames=["method", "path", "status_code"],
    buckets=[0.003, 0.03, 0.1, 0.3, 1.5, 10, float("inf")],
)
