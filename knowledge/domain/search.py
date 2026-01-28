import logging
import numpy as np

from dataclasses import dataclass
from pydantic import BaseModel

from base.core.values import as_yaml, try_parse_yaml_as
from base.models.context import NdCache
from base.resources.aff_body import AffBody, AffBodyChunk, AffBodyMedia
from base.strings.resource import Observable, ObservableUri, ResourceUri

from knowledge.domain.storage import read_resource_history
from knowledge.server.context import KnowledgeContext
from knowledge.services.inference import SvcInference
from knowledge.services.storage import SvcStorage

logger = logging.getLogger(__name__)


class ResourceEmbedding(BaseModel, frozen=True):
    """
    An embedding generated for a particular observable.
    NOTE: The same observable may have multiple embeddings.
    """

    suffix: Observable
    values: list[float]


class ResourceEmbeddings(BaseModel, frozen=True):
    uri: ResourceUri
    embeddings: list[ResourceEmbedding]


@dataclass(kw_only=True)
class CacheSearch(NdCache):
    embeddings: dict[ResourceUri, ResourceEmbeddings | None]

    @classmethod
    def initialize(cls) -> "CacheSearch":
        return CacheSearch(embeddings={})


##
## Embeddings
##


async def search_by_embeddings(
    context: KnowledgeContext,
    queries: list[str],
    prefixes: list[ResourceUri],
    max_results: int,
    threshold_similarity: float,
) -> list[tuple[ObservableUri, float]]:
    inference = context.service(SvcInference)
    storage = context.service(SvcStorage)

    # Embed the queries.
    query_embeddings = [
        np.array(embed_query)
        for query in queries
        if (embed_query := await inference.embedding(query))
    ]

    # If no query was successfully embedded, then return an empty result.
    if not query_embeddings:
        return []

    # Calculate the embeddings for the resources matched by the prefixes,
    # discarding the worst results at each step.
    results: dict[ObservableUri, float] = {}
    for prefix in prefixes:
        prefix_path = f"meta/{str(prefix).removeprefix('ndk://')}"
        prefix_list = await storage.object_list(prefix_path, ".yml")
        for file_path in prefix_list.objects:
            resource_uri = ResourceUri.try_decode("ndk://" + file_path[len("meta/") :])
            if not resource_uri:
                continue

            embeddings = await load_body_embeddings(context, resource_uri)
            if not embeddings:
                continue

            # Pick the best cosine similarity among the queries and the
            # embeddings for that resource.
            # fmt: off
            for embedding in embeddings:
                observable_uri = resource_uri.child_observable(embedding.suffix)
                for query_embedding in query_embeddings:
                    similarity = np.dot(embedding.values, query_embedding)
                    if (
                        similarity > threshold_similarity
                        and similarity > results.get(observable_uri, 0)
                    ):
                        results[observable_uri] = similarity

        # After processing each prefix, discard the worst results.
        if len(results) > max_results:
            sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)
            results = dict(sorted_results[:max_results])

    sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)
    return [
        (resource_uri, similarity)
        for resource_uri, similarity in sorted_results[:max_results]
    ]


async def load_body_embeddings(
    context: KnowledgeContext,
    uri: ResourceUri,
) -> list[ResourceEmbedding]:
    inference = context.service(SvcInference)

    embeddings = await read_resource_embeddings(context, uri)
    if embeddings:
        return [
            emb
            for emb in embeddings.embeddings
            if isinstance(emb.suffix, AffBody | AffBodyChunk)
        ]

    history = await read_resource_history(context, uri)
    if not history:
        return []

    inputs_description: dict[Observable, str] = {
        field.target: field.value
        for field in history.all_fields()
        if isinstance(field.target, AffBody | AffBodyChunk | AffBodyMedia)
        and field.name == "description"
        and isinstance(field.value, str)
    }
    if not inputs_description:
        return []

    logger.info("Generating %s body embeddings...", len(inputs_description))
    embeds_description: dict[Observable, list[float]] = {}
    for observable, description in inputs_description.items():
        if embed_description := await inference.embedding(description):
            embeds_description[observable] = embed_description

    logger.info(
        "Generated %s/%s body embeddings",
        len(embeds_description),
        len(inputs_description),
    )

    embeddings = [
        ResourceEmbedding(suffix=suffix, values=embed_description)
        for suffix, embed_description in embeds_description.items()
    ]

    if len(embeds_description) > 1:
        chunk_embeds = [
            np.array(embed_description)
            for embed_description in embeds_description.values()
        ]
        root_embed = np.mean(chunk_embeds, axis=1)
        root_embed = root_embed / np.linalg.norm(root_embed)
        embeddings.append(
            ResourceEmbedding(suffix=AffBody.new(), values=root_embed.tolist())
        )

    await save_resource_embeddings(context, uri, embeddings)
    return embeddings


##
## Embeddings
##


async def read_resource_embeddings(
    context: KnowledgeContext,
    uri: ResourceUri,
) -> ResourceEmbeddings | None:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheSearch)

    if uri in cache.embeddings:
        return cache.embeddings[uri]

    storage_path = _generate_embedding_path(uri)
    storage_data = await storage.object_get(storage_path, ".yml")
    embeddings = (
        try_parse_yaml_as(ResourceEmbeddings, storage_data) if storage_data else None
    )

    cache.embeddings[uri] = embeddings
    return embeddings


async def save_resource_embeddings(
    context: KnowledgeContext,
    uri: ResourceUri,
    embeddings: list[ResourceEmbedding],
) -> ResourceEmbeddings:
    storage = context.service(SvcStorage)
    cache = context.cached(CacheSearch)

    previous_embeddings = await read_resource_embeddings(context, uri)
    if previous_embeddings:
        changed_roots = {e.suffix.root() for e in embeddings}
        kept_embeddings = [
            emb
            for emb in previous_embeddings.embeddings
            if emb.suffix.root() not in changed_roots
        ]
        embeddings = [*kept_embeddings, *embeddings]

    new_embeddings = ResourceEmbeddings(
        uri=uri,
        embeddings=sorted(
            embeddings,
            key=lambda emb: (
                str(emb.suffix) + "/" + "/".join(f"{v:.6f}" for v in emb.values[:16])
            ),
        ),
    )

    storage_path = _generate_embedding_path(uri)
    await storage.object_set(storage_path, ".yml", as_yaml(new_embeddings))

    cache.embeddings[uri] = new_embeddings
    return new_embeddings


def _generate_embedding_path(uri: ResourceUri) -> str:
    return f"v1/embeddings/{str(uri).removeprefix('ndk://')}"
