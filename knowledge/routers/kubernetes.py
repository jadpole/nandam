from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST

from base.server.metrics import latest_metrics
from base.server.status import HealthResponse

router = APIRouter(tags=["kubernetes"])


@router.get("/", response_model=HealthResponse)
async def get_kubernetes_live() -> JSONResponse:
    """
    A simple health check that returns the version.

    Returns 200 OK unless the service is ready to be removed by Kubernetes,
    represented by app status "terminated".
    """
    response = HealthResponse.new()
    return JSONResponse(
        content=response.model_dump(),
        status_code=response.status_code_live(),
    )


@router.get("/ready", response_model=HealthResponse)
async def get_kubernetes_ready() -> JSONResponse:
    """
    A simple health check that returns the version.

    Returns 200 OK when the service is ready to serve requests, i.e., it is
    ready and did not receive SIGTERM from Kubernetes.
    """
    response = HealthResponse.new()
    return JSONResponse(
        content=response.model_dump(),
        status_code=response.status_code_ready(),
    )


@router.get("/metrics")
def get_kubernetes_metrics(_: Request) -> Response:
    """Endpoint that serves Prometheus metrics."""
    resp = Response(content=latest_metrics())
    resp.headers["Content-Type"] = CONTENT_TYPE_LATEST
    return resp
