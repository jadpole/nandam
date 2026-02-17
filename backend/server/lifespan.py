import asyncio

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from fastapi import FastAPI

from base.server.lifespan import BaseLifespan
from base.server.status import send_sigterm, send_terminated

from backend.config import BackendConfig
from backend.server.workspace_server import RUNNING_WORKSPACES


@dataclass(kw_only=True)
class BackendLifespan(BaseLifespan):
    app_name: str = "backend"

    async def _handle_startup_background(self) -> None:
        pass

    async def _handle_shutdown_background(self) -> None:
        """
        Wait for all workspace servers to shutdown gracefully.
        """
        tasks = [
            t
            for workspace in RUNNING_WORKSPACES.values()
            if (t := workspace._execution_task)  # noqa: SLF001
        ]
        await asyncio.gather(*tasks, return_exceptions=True)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """
    Spawn the background tasks and send the "ready" signal.

    Wait until SIGTERM is received, then send the "stopping" signal, which is
    polled by each background task.

    Once all background tasks are completed (gracefully shutdown), or they fail
    to do so in 3 seconds (timeout), send the "terminated" signal, which tells
    Kubernetes that the pod can be removed.
    """
    # fmt: off
    documents_lifespan = None
    if not BackendConfig.api.documents_host:
        from documents.server.lifespan import DocumentsLifespan # noqa: PLC0415
        documents_lifespan = DocumentsLifespan(background_tasks=[])
        await documents_lifespan.on_startup(app_name="backend")

    knowledge_lifespan = None
    if not BackendConfig.api.knowledge_host:
        from knowledge.server.lifespan import KnowledgeLifespan # noqa: PLC0415
        knowledge_lifespan = KnowledgeLifespan(background_tasks=[])
        await knowledge_lifespan.on_startup(app_name="backend")
    # fmt: on

    lifespan = BackendLifespan(background_tasks=[])
    await lifespan.on_startup(app_name="backend")

    yield

    send_sigterm()
    await lifespan.on_shutdown()
    if knowledge_lifespan:
        await knowledge_lifespan.on_shutdown()
    if documents_lifespan:
        await documents_lifespan.on_shutdown()
    send_terminated()
