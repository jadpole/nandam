import asyncio
import logging

from base.api.knowledge import KnowledgeRefreshId
from base.strings.resource import Realm, ResourceUri
from base.utils.sorted_list import bisect_extend, bisect_make

from knowledge.config import KnowledgeConfig
from knowledge.domain.resolve import (
    save_locators,
    try_infer_and_resolve_locators,
    try_resolve_locators,
)
from knowledge.server.context import Connector, KnowledgeContext
from knowledge.services.storage import SvcStorage

logger = logging.getLogger(__name__)


async def execute_refresh(
    context: KnowledgeContext,
    realms: list[Realm],
    previous: dict[Realm, KnowledgeRefreshId],
) -> list[ResourceUri]:
    connectors = (
        [connector for connector in context.connectors if connector.realm in realms]
        if realms
        else context.connectors
    )
    refresh_results = await asyncio.gather(
        *[
            _execute_refresh_connector(
                context, connector, previous.get(connector.realm)
            )
            for connector in connectors
        ]
    )
    merged: list[ResourceUri] = []
    for results in refresh_results:
        bisect_extend(merged, results, key=str)
    return merged


async def _execute_refresh_connector(
    context: KnowledgeContext,
    connector: Connector,
    previous: KnowledgeRefreshId | None,
) -> list[ResourceUri]:
    try:
        # Without validation, simply save the locators returned by `refresh` as
        # aliases and as "previous refresh" results.
        refresh_locators = await connector.refresh()
        if refresh_locators:
            await save_locators(context, refresh_locators)

        refresh_uris = bisect_make(
            (locator.resource_uri() for locator in refresh_locators),
            key=str,
        )
        if refresh_uris:
            await save_refresh_result(context, connector.realm, refresh_uris)

        # Then resolve the locators, to confirm access, and return only the ones
        # where access is confirmed.
        previous_uris = await read_refresh_since_previous(
            context, connector.realm, previous
        )
        bisect_extend(previous_uris, refresh_uris, key=str)

        resolved_new = await try_resolve_locators(context, refresh_locators)
        resolved_old = await try_infer_and_resolve_locators(
            context, list(previous_uris)
        )
        return bisect_make(
            (
                *(loc.resource_uri() for loc in resolved_new),
                *(loc.resource_uri() for loc in resolved_old.values()),
            ),
            key=str,
        )
    except Exception:
        if KnowledgeConfig.verbose:
            logger.exception("Failed to refresh connector: %s", connector.realm)
        return []


##
## Cache
##


async def save_refresh_result(
    context: KnowledgeContext,
    realm: Realm,
    results: list[ResourceUri],
) -> None:
    storage = context.service(SvcStorage)
    if not results:
        return

    storage_path = f"v1/refresh/{realm}/{context.refresh_id}"
    refresh_data = "\n".join(str(uri) for uri in results)
    await storage.object_set(storage_path, ".txt", refresh_data)


async def read_refresh_since_previous(
    context: KnowledgeContext,
    realm: Realm,
    previous: KnowledgeRefreshId | None,
) -> list[ResourceUri]:
    storage = context.service(SvcStorage)
    if not previous:
        return []

    try:
        refresh_list = await storage.object_list(f"v1/refresh/{realm}", ".txt")
        previous_ids: list[KnowledgeRefreshId] = [
            refresh_id
            for object_path in refresh_list.objects
            if (refresh_str := object_path.removeprefix(f"v1/refresh/{realm}/"))
            and refresh_str > previous
            and (refresh_id := KnowledgeRefreshId.try_decode(refresh_str))
        ]
        if not previous_ids:
            return []

        results: list[ResourceUri] = []
        for previous_id in previous_ids:
            storage_path = f"v1/refresh/{realm}/{previous_id}"
            refresh_data = await storage.object_get(storage_path, ".txt")
            if not refresh_data:
                continue
            bisect_extend(
                results,
                [
                    uri
                    for line in refresh_data.decode("utf-8").splitlines()
                    if (uri := ResourceUri.try_decode(line))
                ],
                key=str,
            )

        return results
    except Exception:
        if KnowledgeConfig.verbose:
            logger.exception("Failed to read previous refresh: %s/%s", realm, previous)
        return []
