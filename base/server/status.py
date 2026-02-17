import asyncio

from pydantic import BaseModel
from typing import Literal

from base.config import BaseConfig
from base.core.exceptions import StoppedError


##
## Signals
##


_READY = asyncio.Event()
_STOPPING = asyncio.Event()
_TERMINATED = asyncio.Event()


def send_ready() -> None:
    _READY.set()


def send_sigterm() -> None:
    _STOPPING.set()


def send_terminated() -> None:
    _TERMINATED.set()


##
## Status
##


AppStatus = Literal["loading", "ok", "stopping", "terminated"]


def app_status() -> AppStatus:
    if _TERMINATED.is_set():
        return "terminated"
    elif _STOPPING.is_set():
        return "stopping"
    elif _READY.is_set():
        return "ok"
    else:
        return "loading"


def assert_is_alive() -> None:
    if app_status() not in ("loading", "ok"):
        raise StoppedError.timeout()


async def loop_if_alive(delay_secs: float) -> bool:
    try:
        async with asyncio.timeout(delay_secs):
            return await _STOPPING.wait()
    except TimeoutError:
        return False


async def wait_for_ready() -> None:
    await _READY.wait()


async def with_timeout_event(
    event: asyncio.Event,
    timeout: float | None = None,
) -> bool:
    wait_task = asyncio.create_task(event.wait())
    await with_timeout(wait_task, timeout)
    wait_task.cancel()
    return event.is_set()


async def with_timeout[R](
    task: asyncio.Task[R],
    timeout: float | None = None,
) -> R | StoppedError:
    if _STOPPING.is_set():
        return StoppedError.timeout()

    cancel_tasks: list[asyncio.Task] = []
    cancel_tasks.append(asyncio.create_task(_STOPPING.wait()))
    if timeout:
        cancel_tasks.append(asyncio.create_task(asyncio.sleep(timeout)))

    done, _ = await asyncio.wait(
        {task, *cancel_tasks},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for cancel_task in cancel_tasks:
        if cancel_task not in done:
            cancel_task.cancel()

    if task in done:
        return await task
    else:
        return StoppedError.timeout()


##
## Kubernetes
##


class HealthResponse(BaseModel):
    status: AppStatus
    version: str

    @staticmethod
    def new() -> HealthResponse:
        return HealthResponse(status=app_status(), version=BaseConfig.version)

    def status_code_live(self) -> int:
        match self.status:
            case "loading" | "ok" | "stopping":
                return 200  # Keep alive in Kubernetes.
            case "terminated":
                return 500  # Ready to be removed by Kubernetes.

    def status_code_ready(self) -> int:
        match self.status:
            case "ok":
                return 200
            case "loading" | "stopping":
                return 503  # Ingress should prefer other replicas.
            case "terminated":
                return 500  # Ready to be removed by Kubernetes.
