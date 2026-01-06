import aiohttp
import logging

from base.config import BaseConfig

logger = logging.getLogger(__name__)


async def spawn_alerts(service: str) -> None:
    """
    If you have a service that runs end-to-end tests on the service, then you
    can configure `NANDAM_ALERTS_HOST` to notify it that a new version has been
    deployed and trigger a test run.
    """
    if not BaseConfig.api.alerts_host:
        return
    try:
        async with (
            aiohttp.ClientSession() as session,
            session.post(f"{BaseConfig.api.alerts_host}/schedule/{service}") as resp,
        ):
            resp.raise_for_status()
    except Exception:
        logger.exception("Failed to spawn %s alerts", service)
