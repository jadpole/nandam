from base.api.documents import DocumentsReadResponse
from base.api.knowledge import KnowledgeSettings
from base.core.values import as_yaml, try_parse_yaml_as
from base.resources.action import LoadMode, ResourcesLoadAction, ResourcesObserveAction
from base.resources.bundle import Resource, Resources
from base.resources.relation import Relation
from base.server.auth import NdAuth
from base.strings.auth import RequestId
from base.strings.resource import Affordance, Observable, ResourceUri, WebUrl

from knowledge.connectors.public import PublicConnector
from knowledge.connectors.web import WebConnector
from knowledge.domain.query import execute_query_all
from knowledge.domain.resolve import CacheResolve
from knowledge.models.exceptions import DownloadError
from knowledge.models.storage import Locator
from knowledge.server.context import KnowledgeContext
from knowledge.server.request import read_connectors_config
from knowledge.services.downloader import SvcDownloader, SvcDownloaderStub
from knowledge.services.inference import SvcInference, SvcInferenceStub
from knowledge.services.storage import SvcStorage, SvcStorageStub


def given_context(
    *,
    stub_downloader: dict[str, DocumentsReadResponse | DownloadError] | None = None,
    stub_inference: bool,
    stub_storage: dict[str, bytes] | None,
) -> KnowledgeContext:
    # NOTE: Instantiate `AuthKeycloak` using the `DEBUG_AUTH_USER_*` environment
    # variables (when `stub_auth` is None) for `Connector.resolve` access check.
    context = KnowledgeContext.new(
        auth=NdAuth.from_headers(x_request_id=RequestId.stub()),
        request_timestamp=None,
        settings=KnowledgeSettings(),
    )

    context.add_service(
        SvcDownloaderStub.initialize(stub_download=stub_downloader)
        if stub_downloader is not None
        else SvcDownloader.initialize(context)
    )
    context.add_service(
        SvcInferenceStub() if stub_inference else SvcInference.initialize(context)
    )
    context.add_service(
        SvcStorageStub(items=stub_storage)
        if stub_storage is not None
        else SvcStorage.initialize()
    )

    for connector_config in read_connectors_config().connectors:
        context.add_connector(connector_config.instantiate(context))
    # TODO: TempConnector(context=context)
    context.connectors.append(PublicConnector(context=context))
    context.connectors.append(WebConnector(context=context))

    return context


def log_locator(step: str, locator: Locator | None) -> None:
    locator_str = as_yaml(locator) if locator else "None"
    print(f'<locator step="{step}">\n{locator_str}\n</locator>')


def log_resources(step: str, resources: Resources) -> None:
    # TODO:
    # collapsed_collection_prefixes = [
    #     "ndk://gitlab/file/",
    # ]

    log_resources = resources.model_dump()
    # TODO:
    # for content in log_resources["contents"]:
    #     if content.get("blob") and len(content["blob"]) > 100:
    #         content["blob"] = content["blob"][:80] + "..."
    #     if content.get("text") and len(content["text"]) > 100:
    #         content["text"] = content["text"][:80] + "..."
    #     if content.get("download_url") and len(content["download_url"]) > 100:
    #         content["download_url"] = content["download_url"][:80] + "..."
    #
    #     if content.get("results"):
    #         for prefix in collapsed_collection_prefixes:
    #             if any(result.startswith(prefix) for result in content["results"]):
    #                 content["results"] = [
    #                     uri for uri in content["results"] if not uri.startswith(prefix)
    #                 ]
    #                 content["results"].append(f"{prefix}...")
    #         content["results"] = sorted(content["results"])

    log_lines = as_yaml(log_resources).splitlines()
    if len(log_lines) > 60:
        log_lines = [*log_lines[:40], f"... {len(log_lines) - 40} lines omitted."]

    log_joined = "\n".join(log_lines)
    print(f'<resources step="{step}">\n{log_joined}\n</resources>')


def log_relations(
    step: str,
    context: KnowledgeContext,
    origins: list[ResourceUri],
) -> None:
    if not (storage := context.service(SvcStorageStub)):
        return

    relations = [
        parsed
        for key, value in storage.items.items()
        if key.startswith("relations/defs/")
        and (parsed := try_parse_yaml_as(Relation, value))
        and any(r in parsed.get_nodes() for r in origins)
    ]
    print(f'<relations step="{step}">\n{as_yaml(relations)}\n</relations>')


def log_storage(step: str, context: KnowledgeContext) -> None:
    collapsed_prefixes = [
        "meta/github/file/",
        "meta/gitlab/file/",
        "meta/jira/issue/",
        "relations/defs/",
        "relations/refs/",
    ]

    if not (storage := context.service(SvcStorageStub)):
        return

    storage_keys = list(storage.items.keys())
    for prefix in collapsed_prefixes:
        if len([key.startswith(prefix) for key in storage_keys]) > 10:
            storage_keys = [key for key in storage_keys if not key.startswith(prefix)]
            storage_keys.append(f"{prefix}...")

    print(f'<storage step="{step}">\n{as_yaml(sorted(storage_keys))}\n</storage>')


async def run_test_connector_full(
    *,
    context: KnowledgeContext,
    web_url: WebUrl | str,
    resource_uri: ResourceUri | str,
    expand_depth: int = 0,
    expand_mode: LoadMode = "none",
    load_mode: LoadMode = "auto",
    observe: list[Observable],
    expected_resolve_name: str | None = None,
    expected_resolve_description: str | None = None,
    expected_resolve_citation_url: str | None = None,
    expected_resolve_affordances: list[str] | None = None,
    expected_load_relations: list[Relation] | None = None,
    expected_load_locator: Locator,
    expected_load_name: str,
    expected_load_mime_type: str | None,
    expected_load_affordances: list[str] | None = None,
) -> tuple[Resources, Resources]:
    """
    NOTE: To simulate persistence, all contexts access the same `stub_storage`.
    """
    if (
        expected_resolve_name
        or expected_resolve_description
        or expected_resolve_citation_url
        or expected_resolve_affordances
    ):
        resolve_resources, _, _ = await run_test_connector_resolve(
            context=context,
            web_url=web_url,
            expected_resource_uri=resource_uri,
            expected_name=expected_resolve_name,
            expected_description=expected_resolve_description,
            expected_citation_url=expected_resolve_citation_url,
            expected_affordances=expected_resolve_affordances or [],
        )
    else:
        resolve_resources = Resources()

    load_resources = await run_connector_step_load(
        context=context,
        uri=resource_uri if expected_resolve_affordances else web_url,
        resource_uri=resource_uri,
        expand_depth=expand_depth,
        expand_mode=expand_mode,
        load_mode=load_mode,
        observe=observe,
        expected_relations=expected_load_relations,
        expected_locator=expected_load_locator,
        expected_name=expected_load_name,
        expected_mime_type=expected_load_mime_type,
        expected_affordances=expected_load_affordances,
    )

    # read_resources = await run_connector_step_observe(
    #     context=context,
    #     resource_uri=resource_uri,
    #     observe=observe,
    # )

    return resolve_resources, load_resources


async def run_test_connector_resolve(
    *,
    context: KnowledgeContext,
    web_url: WebUrl | str,
    expected_resource_uri: ResourceUri | str,
    expected_name: str | None,
    expected_locator: Locator | None = None,
    expected_description: str | None = None,
    expected_citation_url: str | None = None,
    expected_affordances: list[str],
) -> tuple[Resources, Locator, Resource]:
    """
    NOTE: To simulate persistence, mutates `stub_storage`.
    """
    web_url = WebUrl.decode(str(web_url))
    expected_resource_uri = ResourceUri.decode(str(expected_resource_uri))

    resources = await execute_query_all(
        context,
        [ResourcesLoadAction(uri=web_url, load_mode="none")],
    )
    locator = context.cached(CacheResolve).locators.get(expected_resource_uri)
    print()
    log_locator("resolve", locator)
    log_resources("resolve", resources)
    log_storage("resolve", context)
    print()

    # Check that the resolved locator is correct.
    assert locator is not None
    if expected_locator is not None:
        assert locator == expected_locator

    # Resolve requests do not perform observations.
    assert resources.observations == []

    # Resolve requests return a resource with the requested URL as an alias.
    resource = resources.get_resource(expected_resource_uri)
    assert isinstance(resource, Resource)
    assert resource.uri == expected_resource_uri
    assert web_url in resource.aliases
    assert resources.infer_knowledge_uri(web_url) == expected_resource_uri

    # Resolve requests return minimal metadata, which can be inferred without
    # downloading or parsing the underlying content.
    if expected_name:
        assert resource.attributes.name == expected_name
    if expected_description is not None:
        if expected_description:
            assert resource.attributes.description == expected_description
        else:
            assert resource.attributes.description is None
    if expected_citation_url:
        assert str(resource.attributes.citation_url) == expected_citation_url

    # Resolve requests return the affordances supported by the resource, without
    # inferring their description (since load_mode="none").
    actual_affordances = [str(info.suffix) for info in resource.affordances]
    assert actual_affordances == expected_affordances

    return resources, locator, resource


async def run_connector_step_load(  # noqa: C901
    *,
    context: KnowledgeContext,
    uri: ResourceUri | WebUrl | str,
    resource_uri: ResourceUri | str,
    expand_depth: int = 0,
    expand_mode: LoadMode = "none",
    load_mode: LoadMode = "auto",
    observe: list[Observable] | None = None,
    expected_locator: Locator | None = None,
    expected_relations: list[Relation] | None = None,
    expected_name: str,
    expected_mime_type: str | None,
    expected_affordances: list[str] | None = None,
) -> Resources:
    """
    NOTE: To simulate persistence, mutates `stub_storage`.
    """
    uri = (
        ResourceUri.decode(str(uri))
        if str(uri).startswith("ndk://")
        else WebUrl.decode(str(uri))
    )
    resource_uri = ResourceUri.decode(str(resource_uri))

    # Load the resource.
    query_action = ResourcesLoadAction(
        uri=uri,
        expand_depth=expand_depth,
        expand_mode=expand_mode,
        load_mode=load_mode,
        observe=observe or [],
    )
    resources = await execute_query_all(context, [query_action])
    locator = context.cached(CacheResolve).locators.get(resource_uri)
    print()
    log_locator("load", locator)
    log_resources("load", resources)
    log_relations("load", context, [resource_uri])
    log_storage("load", context)
    print()

    # Check that the resolved locator is correct.
    if expected_locator is not None:
        assert locator == expected_locator

    # Check that the resource is correctly loaded.
    resource = resources.get_resource(resource_uri)
    assert isinstance(resource, Resource)
    assert resource.uri == resource_uri
    assert resources.infer_knowledge_uri(uri) == resource_uri
    if isinstance(uri, WebUrl):
        assert uri in resource.aliases

    # ... but they DO ingest it to populate "affordances" in the resource metadata.
    assert resource.attributes.name == expected_name
    if expected_mime_type:
        assert resource.attributes.mime_type == expected_mime_type
    if expected_affordances:
        for aff in expected_affordances:
            info_exists = (
                resource.info().get_affordance(Affordance.decode(aff)) is not None
            )
            assert info_exists, f"info not found: {resource_uri} > {aff}"

    if expected_relations and (storage := context.service(SvcStorageStub)):
        for relation in expected_relations:
            assert resource.relations is not None
            assert relation in resource.relations
            relation_id = relation.unique_id()
            def_path = f"v1/relation/defs/{relation_id}.yml"
            assert def_path in storage.items, f"missing relation def: {relation_id}"
            for node_uri in relation.get_nodes():
                node_part = str(node_uri).removeprefix("ndk://").replace("/", "+")
                ref_path = f"v1/relation/refs/{node_part}/{relation_id}.txt"
                assert ref_path in storage.items, f"missing relation ref: {relation_id}"

    # Load actions without "observe" do not return contents, even though some
    # observations may be made behind-the-scenes to generate the infos.
    if observe:
        for aff in observe:
            observation = resources.get_observation(resource_uri.child_observable(aff))
            assert observation, f"observation not found: {resource_uri} > {aff}"
    else:
        assert resources.observations == []

    return resources


async def run_connector_step_observe(
    *,
    context: KnowledgeContext,
    resource_uri: ResourceUri | str,
    observe: list[Observable],
) -> Resources:
    """
    NOTE: To simulate persistence, mutates `stub_storage`.
    """
    resource_uri = ResourceUri.decode(str(resource_uri))

    # Read the resource.
    resources = await execute_query_all(
        context,
        [
            ResourcesObserveAction(uri=resource_uri.child_observable(aff))
            for aff in observe
        ],
    )
    locator = context.cached(CacheResolve).locators.get(resource_uri)
    print()
    log_locator("read", locator)
    log_resources("read", resources)
    log_storage("read", context)
    print()

    for aff in observe:
        observation = resources.get_observation(resource_uri.child_observable(aff))
        assert observation, f"observation not found: {resource_uri} > {aff}"

    return resources
