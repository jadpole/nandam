import logging
import uvicorn

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from base.core.exceptions import ApiError
from base.core.values import as_json

from documents.config import DocumentsConfig
from documents.routers import kubernetes, read
from documents.server.lifespan import lifespan
from documents.server.metrics import MetricsMiddleware

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Nandam Documents",
    version=DocumentsConfig.version,
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

app.include_router(kubernetes.router)
app.include_router(read.router)


@app.exception_handler(ApiError)
def api_error_handler(_request: Request, exc: ApiError) -> Response:
    logger.error(as_json({"api_error": exc.as_info(redacted=True)}))
    return exc.as_http_response()


if __name__ == "__main__":
    uvicorn.run(
        "documents.app:app",
        host="0.0.0.0",
        port=DocumentsConfig.web_server.port,
        reload=not DocumentsConfig.is_kubernetes(),
    )
