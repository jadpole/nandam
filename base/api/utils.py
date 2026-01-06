import aiohttp
import json
import logging

from pydantic import BaseModel, TypeAdapter
from typing import Any

from base.config import BaseConfig
from base.core.exceptions import ApiError

logger = logging.getLogger(__name__)


async def post_request[Resp: BaseModel](
    endpoint: str,
    payload: BaseModel | dict[str, Any] | str,
    type_exc: type[ApiError],
    type_resp: type[Resp],
    authorization: str | None = None,
    headers: dict[str, str] | None = None,
    user_id: str | None = None,
    timeout_secs: float = 180.0,
) -> Resp:
    headers = headers.copy() if headers else {}
    if authorization:
        headers["authorization"] = authorization
    if user_id:
        headers["x-user-id"] = user_id

    if isinstance(payload, BaseModel):
        headers["content-type"] = "application/json"
    else:
        headers["content-type"] = "text/plain"

    data: str
    if isinstance(payload, BaseModel):
        data = payload.model_dump_json()
    elif isinstance(payload, dict):
        data = json.dumps(payload)
    else:
        data = payload

    try:
        async with aiohttp.ClientSession() as session:  # noqa: SIM117
            async with session.post(
                endpoint,
                headers=headers,
                data=data,
                ssl=BaseConfig.is_kubernetes(),
                timeout=aiohttp.ClientTimeout(total=timeout_secs),
            ) as response:
                if response.status == 200:  # noqa: PLR2004
                    data = await response.json()
                    return TypeAdapter(type_resp).validate_python(data)
                else:
                    text = await response.text()
                    raise type_exc.from_http(response.status, text)
    except type_exc:
        raise
    except Exception as exc:
        # Also catches errors when the service returns a malformed response, but
        # I don't think the distinction is useful.
        logger.exception("POST request to %s failed.", endpoint)
        raise type_exc.from_http(500, {"detail": {"error": "network error"}}) from exc
