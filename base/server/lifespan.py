import asyncio
import contextlib
import logging
import sys

from dataclasses import dataclass
from pythonjsonlogger.json import JsonFormatter
from termcolor import colored

from base.config import BaseConfig
from base.server.alerting import spawn_alerts
from base.server.status import send_ready, send_terminated

logger = logging.getLogger(__name__)


@dataclass(kw_only=True)
class BaseLifespan:
    app_name: str
    background_tasks: list[asyncio.Task]

    ##
    ## Interface
    ##

    async def on_startup(self, app_name: str) -> None:
        if app_name == self.app_name:
            setup_logging(app_name=app_name, log_level=BaseConfig.log_level)
            self.background_tasks.append(asyncio.create_task(spawn_alerts(app_name)))

        startup_task = asyncio.create_task(self.on_startup_background())
        self.background_tasks.append(startup_task)
        if app_name == self.app_name:
            startup_task.add_done_callback(lambda _: send_ready())

    async def on_startup_background(self) -> None:
        # Send the "ready" signal, so the replica can start receiving requests,
        # once the background initialization tasks were completed.
        try:
            await self._handle_startup_background()
        except Exception:
            logger.exception("Background startup failed")
            send_terminated()  # Ask Kubernetes to restart the pod.

    async def on_shutdown(self) -> None:
        # Wait for all background tasks to complete (graceful shutdown), unless
        # they fail to do so in 3 seconds (timeout)
        self.background_tasks.append(
            asyncio.create_task(self._handle_shutdown_background()),
        )
        with contextlib.suppress(asyncio.TimeoutError):
            async with asyncio.timeout(3.0):
                await asyncio.gather(*self.background_tasks, return_exceptions=True)

    ##
    ## Implementation
    ##

    async def _handle_startup_background(self) -> None:
        pass

    async def _handle_shutdown_background(self) -> None:
        pass


##
## LOGGING
## - Logs of level WARNING and above are sent to stderr.
## - Logs below level WARNING are sent to stdout.
##


class NandamFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        result = super().format(record)

        color: str | None = None
        if "Traceback" in result:
            color = "light_red"
        elif record.levelno >= logging.ERROR:
            color = "red"
        elif record.levelno >= logging.WARNING:
            color = "yellow"
        elif record.levelno >= logging.INFO:
            color = "blue"

        return colored(result, color) if color else result


def setup_logging(
    app_name: str,
    log_level: int = logging.WARNING,
) -> None:
    """Configure the root logger and create the app logger."""
    # Setup the parent logger for base.
    _create_app_logger("base", BaseConfig.log_level, _get_log_formatter())
    # Setup the parent logger for app.
    _create_app_logger(app_name, log_level, _get_log_formatter())
    # Setup the root logger.
    _configure_root_logger(logging.WARNING, _get_log_formatter())


def _create_app_logger(
    app_name: str,
    log_level: int,
    log_format: logging.Formatter,
) -> logging.Logger:
    """Create the app's parent logger."""
    app_logger = logging.getLogger(app_name)
    app_logger.setLevel(log_level)
    app_logger.propagate = False

    consolehandler_err = logging.StreamHandler(sys.stderr)
    consolehandler_err.setLevel(logging.WARNING)
    consolehandler_err.setFormatter(log_format)
    app_logger.addHandler(consolehandler_err)

    consolehandler_std = logging.StreamHandler(sys.stdout)
    consolehandler_std.setLevel(logging.DEBUG)
    consolehandler_std.setFormatter(log_format)
    consolehandler_std.addFilter(lambda record: record.levelno < logging.WARNING)

    if log_level < logging.WARNING:
        app_logger.addHandler(consolehandler_std)

    return app_logger


def _get_log_formatter() -> logging.Formatter | JsonFormatter:
    if BaseConfig.is_kubernetes():
        fmt = [
            "%(asctime)",
            "%(levelname)",
            "%(name)",
            "%(thread)",
            "%(threadName)",
            "%(process)",
            "%(message)",
        ]
        return JsonFormatter("".join(fmt))
    else:
        return NandamFormatter(
            fmt="%(levelname)7s - %(name)-0s - [T%(thread)d][P%(process)d] %(message)s",
        )


def _configure_root_logger(log_level: int, log_format: logging.Formatter) -> None:
    """Configure the root logger.

    The root logger gets messages from all child loggers unless they have propagation explicitly disabled.
    It sends all WARNING (and above) events to stderr.
    It sends logs below level WARNING to stdout if log_level is 2 or above.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    consolehandler_err = logging.StreamHandler(sys.stderr)
    consolehandler_err.setLevel(logging.WARNING)
    consolehandler_err.setFormatter(log_format)
    root_logger.addHandler(consolehandler_err)

    consolehandler_std = logging.StreamHandler(sys.stdout)
    consolehandler_std.setLevel(logging.DEBUG)
    consolehandler_std.setFormatter(log_format)
    consolehandler_std.addFilter(lambda record: record.levelno < logging.WARNING)

    if log_level < logging.WARNING:
        root_logger.addHandler(consolehandler_std)
