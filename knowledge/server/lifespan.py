from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from fastapi import FastAPI

from base.server.lifespan import BaseLifespan
from knowledge.config import KnowledgeConfig


class KnowledgeLifespan(BaseLifespan):
    async def _handle_startup_background(self) -> None:
        pass

    async def _handle_shutdown_background(self) -> None:
        pass


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Spawn the background tasks and send the "ready" signal.

    Wait until SIGTERM is received, then send the "stopping" signal, which is
    polled by each background task.

    Once all background tasks are completed (gracefully shutdown), or they fail
    to do so in 3 seconds (timeout), send the "terminated" signal, which tells
    Kubernetes that the pod can be removed.

    NOTE: Also executes the Documents lifespan in local development, so that
    calls to the service are successful.
    """
    # fmt: off
    documents_lifespan = None
    if not KnowledgeConfig.api.documents_host:
        from documents.server.lifespan import DocumentsLifespan # noqa: PLC0415
        documents_lifespan = DocumentsLifespan(background_tasks=[])
        await documents_lifespan.on_startup(app_name="documents")
    # fmt: on

    lifespan = KnowledgeLifespan(background_tasks=[])
    await lifespan.on_startup(app_name="knowledge")
    yield
    await lifespan.on_shutdown()
    if documents_lifespan:
        await documents_lifespan.on_shutdown()
