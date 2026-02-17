import logging
import uvicorn

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from base.core.exceptions import ApiError
from base.core.values import as_json

from backend.config import BackendConfig
from backend.server.lifespan import lifespan
from backend.server.metrics import MetricsMiddleware
from backend.routers import kubernetes, process, thread, webapp, workspace

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Nandam Backend",
    version=BackendConfig.version,
    lifespan=lifespan,
)
app.add_middleware(MetricsMiddleware)  # type: ignore
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

if BackendConfig.web_app.directory:
    app.mount(
        "/app",
        StaticFiles(directory=BackendConfig.web_app.directory, html=True),
        name="webapp",
    )

app.include_router(kubernetes.router)
app.include_router(process.router)
app.include_router(thread.router)
app.include_router(webapp.router)
app.include_router(workspace.router)


@app.exception_handler(ApiError)
def api_error_handler(_request: Request, exc: ApiError) -> Response:
    logger.error(as_json({"api_error": exc.as_info(redacted=True)}))
    return exc.as_http_response()


if __name__ == "__main__":
    uvicorn.run(
        "backend.app:app",
        host="0.0.0.0",
        port=BackendConfig.web_server.port,
        reload_dirs=["base", "backend"] if not BackendConfig.is_kubernetes() else None,
        reload_excludes=(
            ["documents/*", "knowledge/*"]
            if not BackendConfig.is_kubernetes()
            else None
        ),
        reload=not BackendConfig.is_kubernetes(),
    )
