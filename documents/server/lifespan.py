from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from fastapi import FastAPI

from base.server.lifespan import BaseLifespan
from base.server.status import send_sigterm, send_terminated


@dataclass(kw_only=True)
class DocumentsLifespan(BaseLifespan):
    app_name: str = "documents"

    async def _handle_startup_background(self) -> None:
        pass

    async def _handle_shutdown_background(self) -> None:
        pass


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
    lifespan = DocumentsLifespan(background_tasks=[])
    await lifespan.on_startup(app_name="documents")

    yield

    send_sigterm()
    await lifespan.on_shutdown()
    send_terminated()
