import logging
import uvicorn

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from base.core.exceptions import ApiError
from base.core.values import as_json

from knowledge.config import KnowledgeConfig
from knowledge.routers import kubernetes, query
from knowledge.server.lifespan import lifespan
from knowledge.server.metrics import MetricsMiddleware

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Nandam Knowledge",
    version=KnowledgeConfig.version,
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
app.include_router(query.router)


@app.exception_handler(ApiError)
def api_error_handler(_request: Request, exc: ApiError) -> Response:
    logger.error(as_json({"api_error": exc.as_info(redacted=True)}))
    return exc.as_http_response()


if __name__ == "__main__":
    uvicorn.run(
        "knowledge.app:app",
        host="0.0.0.0",
        port=KnowledgeConfig.web_server.port,
        reload=not KnowledgeConfig.is_kubernetes(),
    )
