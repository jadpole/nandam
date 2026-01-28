import asyncio

from dataclasses import dataclass

from base.core.unique_id import unique_id_from_str
from base.core.values import as_yaml, try_parse_yaml_as
from base.models.context import NdCache
from base.resources.relation import Relation, RelationId
from base.strings.resource import Affordance, AffordanceUri, ResourceUri, RootReference
from base.utils.sorted_list import bisect_insert, bisect_make

from knowledge.config import KnowledgeConfig
from knowledge.models.storage_metadata import Locator, ResourceHistory
from knowledge.models.storage_observed import AnyBundle, AnyBundle_
from knowledge.server.context import KnowledgeContext
from knowledge.services.storage import SvcStorage

STORAGE_READ_BATCH_SIZE: int = 10


@dataclass(kw_only=True)
class CacheStorage(NdCache):
    bundles: dict[AffordanceUri, AnyBundle_ | None]
    relation_defs: dict[RelationId, Relation | None]
    relation_refs: dict[ResourceUri, list[RelationId]]
    resource_bundles: dict[ResourceUri, list[Affordance]]
    resources: dict[ResourceUri, ResourceHistory | None]

    @classmethod
    def initialize(cls) -> "CacheStorage":
        return CacheStorage(
            bundles={},
            relation_defs={},
            relation_refs={},
            resource_bundles={},
            resources={},
        )


##
## Resource
##


async def save_resource_history(
    context: KnowledgeContext,
    history: ResourceHistory,
) -> None:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)

    uri = history.merged().locator.resource_uri()
    storage_path = _generate_resource_path(uri)
    await storage.object_set(storage_path, ".yml", as_yaml(history))
    cache.resources[uri] = history


async def read_resource_history(
    context: KnowledgeContext,
    uri: ResourceUri,
) -> ResourceHistory | None:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)
    if uri in cache.resources:
        return cache.resources[uri]

    storage_path = _generate_resource_path(uri)
    storage_data = await storage.object_get(storage_path, ".yml")
    resource = (
        try_parse_yaml_as(ResourceHistory, storage_data) if storage_data else None
    )

    # If the resource metadata is not cached, then ignore cached contents.
    cache.resources[uri] = resource
    return resource


def _generate_resource_path(uri: ResourceUri) -> str:
    return f"v1/resource/{str(uri).removeprefix('ndk://')}"


##
## Observation Bundle
##


async def list_cached_bundles(
    context: KnowledgeContext,
    uri: ResourceUri,
) -> list[Affordance]:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)
    if uri in cache.resource_bundles:
        return cache.resource_bundles[uri]

    bundle_prefix = _generate_bundle_prefix(uri)
    bundle_list = await storage.object_list(bundle_prefix, ".yml")
    cached_bundles = bisect_make(
        (
            aff
            for bundle in bundle_list.objects
            if (aff := Affordance.try_decode(bundle))
        ),
        key=str,
    )

    cache.resource_bundles[uri] = cached_bundles
    return cached_bundles


async def read_cached_bundle(
    context: KnowledgeContext,
    uri: ResourceUri,
    affordance: Affordance,
) -> AnyBundle | None:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)

    affordance_uri = uri.child_affordance(affordance)
    if affordance_uri in cache.bundles:
        return cache.bundles[affordance_uri]

    bundle: AnyBundle | None = None
    try:
        bundle_path = _generate_bundle_path(uri, affordance)
        bundle_data = await storage.object_get(bundle_path, ".yml")
        if bundle_data:
            bundle = try_parse_yaml_as(AnyBundle_, bundle_data)  # type: ignore
    except ValueError:
        pass  # Failed to deserialize the cache: ignore legacy.

    cache.bundles[affordance_uri] = bundle
    return bundle


async def remove_cached_bundle(
    context: KnowledgeContext,
    new_bundle: AnyBundle,
) -> bool:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)

    resource_uri = new_bundle.uri.resource_uri()
    bundle_path = _generate_bundle_path(resource_uri, new_bundle.uri.suffix)
    existed = await storage.object_delete(bundle_path, ".yml")

    cache.bundles[new_bundle.uri] = None
    return existed


async def save_cached_bundle(
    context: KnowledgeContext,
    new_bundle: AnyBundle,
) -> None:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)

    resource_uri = new_bundle.uri.resource_uri()
    bundle_path = _generate_bundle_path(resource_uri, new_bundle.uri.suffix)
    await storage.object_set(bundle_path, ".yml", as_yaml(new_bundle))
    cache.bundles[new_bundle.uri] = new_bundle


def _generate_bundle_prefix(resource_uri: ResourceUri) -> str:
    resource_path = str(resource_uri).removeprefix("ndk://").replace("/", "+")
    return f"v1/observed/{resource_path}"


def _generate_bundle_path(resource_uri: ResourceUri, affordance: Affordance) -> str:
    bundle_prefix = _generate_bundle_prefix(resource_uri)
    bundle_stem = affordance.as_suffix().removeprefix("$").replace("/", "+")
    return f"{bundle_prefix}/{bundle_stem}"


##
## Alias (Reference -> Locator)
##


async def read_alias(context: KnowledgeContext, uri: RootReference) -> Locator | None:
    storage = context.service(SvcStorage)
    try:
        alias_path = _generate_alias_path(uri)
        alias_data = await storage.object_get(alias_path, ".yml")
        return try_parse_yaml_as(Locator, alias_data) if alias_data else None
    except ValueError:
        return None  # Locator matches no available type: ignore legacy.


async def remove_alias(context: KnowledgeContext, uri: RootReference) -> bool:
    storage = context.service(SvcStorage)
    alias_path = _generate_alias_path(uri)
    return await storage.object_delete(alias_path, ".yml")


async def save_alias(
    context: KnowledgeContext,
    uri: RootReference,
    locator: Locator,
) -> None:
    storage = context.service(SvcStorage)
    alias_path = _generate_alias_path(uri)
    await storage.object_set(alias_path, ".yml", as_yaml(locator))


def _generate_alias_path(uri: RootReference) -> str:
    alias_id = unique_id_from_str(
        str(uri),
        num_chars=40,
        salt=f"knowledge-alias-{KnowledgeConfig.environment}",
    )
    return f"v1/alias/{alias_id}"


##
## Relation
##


async def list_relation_ids(
    context: KnowledgeContext,
    resource_uri: ResourceUri,
) -> list[RelationId]:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)

    if resource_uri in cache.relation_refs:
        relation_ids = cache.relation_refs[resource_uri].copy()
    else:
        resource_part = str(resource_uri).removeprefix("ndk://").replace("/", "+")
        refs_prefix = f"v1/relation/refs/{resource_part}"
        refs_list = await storage.object_list(refs_prefix, ".txt")
        relation_ids = sorted(
            [
                relation_id
                for object_path in refs_list.objects
                if (relation_str := object_path.removeprefix(f"{refs_prefix}/"))
                and (relation_id := RelationId.try_decode(relation_str))
            ],
            key=str,
        )
        cache.relation_refs[resource_uri] = relation_ids

    return relation_ids


async def list_relations(
    context: KnowledgeContext,
    resource_uri: ResourceUri,
) -> list[Relation]:
    relations: list[Relation] = []
    relation_ids = await list_relation_ids(context, resource_uri)
    for start_index in range(0, len(relation_ids), STORAGE_READ_BATCH_SIZE):
        tasks = [
            read_relation(context, relation_id)
            for relation_id in relation_ids[
                start_index : start_index + STORAGE_READ_BATCH_SIZE
            ]
        ]
        results = await asyncio.gather(*tasks)
        for result in results:
            if result:
                bisect_insert(relations, result, key=lambda r: str(r.unique_id()))

    return relations


async def read_relation(
    context: KnowledgeContext,
    relation_id: RelationId,
) -> Relation | None:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)
    if relation_id in cache.relation_defs:
        return cache.relation_defs[relation_id]

    def_path = f"v1/relation/defs/{relation_id}"
    def_data = await storage.object_get(def_path, ".yml")
    relation = try_parse_yaml_as(Relation, def_data) if def_data else None

    cache.relation_defs[relation_id] = relation
    return relation


async def remove_relation(
    context: KnowledgeContext,
    owner_uri: ResourceUri,  # noqa: ARG001
    relation: Relation,
) -> bool:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)

    relation_id = relation.unique_id()
    cache.relation_defs[relation_id] = None
    for node_uri in relation.get_nodes():
        node_part = str(node_uri).removeprefix("ndk://").replace("/", "+")
        ref_path = f"v1/relation/refs/{node_part}/{relation_id}"
        await storage.object_delete(ref_path, ".txt")
        if node_uri in cache.relation_refs:
            cache.relation_refs[node_uri] = [
                rid for rid in cache.relation_refs[node_uri] if rid != relation_id
            ]

    def_path = f"v1/relation/defs/{relation_id}"
    return await storage.object_delete(def_path, ".yml")


async def save_relation(
    context: KnowledgeContext,
    owner_uri: ResourceUri,  # noqa: ARG001
    relation: Relation,
) -> None:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheStorage)

    relation_id = relation.unique_id()
    cache.relation_defs[relation_id] = relation
    for node_uri in relation.get_nodes():
        node_part = str(node_uri).removeprefix("ndk://").replace("/", "+")
        ref_path = f"v1/relation/refs/{node_part}/{relation_id}"
        await storage.object_set(ref_path, ".txt", "")
        if node_uri in cache.relation_refs:
            bisect_insert(
                cache.relation_refs[node_uri],
                relation_id,
                key=str,
            )

    relation_path = f"v1/relation/defs/{relation_id}"
    await storage.object_set(relation_path, ".yml", as_yaml(relation))
