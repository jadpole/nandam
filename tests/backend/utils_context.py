from typing import Any, Literal

from base.core.exceptions import ApiError
from base.core.values import as_yaml, parse_yaml_as
from base.resources.bundle import ObservationError, Resource, ResourceError
from base.resources.observation import Observation
from base.server.auth import NdAuth
from base.strings.auth import ServiceId
from base.strings.process import ProcessName, ProcessUri
from base.strings.scope import Workspace

from backend.models.process_result import ProcessResult, ProcessSuccess
from backend.models.tool_info import ToolMode
from backend.server.context import NdProcess, WorkspaceContext, WorkspaceRequest
from backend.server.resources import CacheResources
from backend.services.kv_store import SvcKVStoreMemory


class StubProcess(NdProcess):
    kind: Literal["stub"] = "stub"
    owner: ServiceId = ServiceId.stub()
    mode: ToolMode = "internal"

    def _get_auth(self) -> NdAuth:
        if request := self._ctx.get_request(self.process_uri):
            return request.auth
        else:
            raise ApiError(f"{type(self).__name__} auth not found")

    def _get_context(self) -> WorkspaceContext:
        return self._ctx

    async def on_spawn(self) -> None:
        pass  # No-op: updates sent by tests via `_on_update`.


def given_context(
    kvstore_items: dict[str, Any] | None = None,
    observations: list[Observation | ObservationError] | None = None,
    resources: list[Resource | ResourceError] | None = None,
) -> WorkspaceContext:
    if kvstore_items is None:
        kvstore_items = {}

    workspace = Workspace.stub_internal()
    context = WorkspaceContext.new(workspace=workspace)
    context.add_service(SvcKVStoreMemory(items=kvstore_items))

    if resources or observations:
        cached_resources = context.cached(CacheResources).resources
        cached_resources.update(resources=resources, observations=observations)

    return context


def given_stub_process(
    name: str = "stub_process",
    arguments: dict[str, Any] | None = None,
    result: ProcessResult | None = None,
    kvstore_items: dict[str, Any] | None = None,
    observations: list[Observation | ObservationError] | None = None,
    resources: list[Resource | ResourceError] | None = None,
) -> NdProcess:
    context = given_context(
        kvstore_items=kvstore_items,
        observations=observations,
        resources=resources,
    )

    auth = NdAuth.stub()
    process_uri = ProcessUri.stub()
    context.requests[process_uri] = WorkspaceRequest(auth=auth)

    process = StubProcess(
        process_uri=process_uri,
        name=ProcessName.decode(name),
        arguments=arguments or {"example": "argument"},
        result=result,
    )
    process._ctx = context
    return process


def assert_ndprocess_serialization(process: NdProcess) -> None:
    process_yaml = as_yaml(process)
    print(f"<yaml>\n{process_yaml}\n</yaml>")

    parsed = parse_yaml_as(NdProcess, process_yaml)
    print(f"<parsed>\n{as_yaml(parsed)}\n</parsed>")

    assert parsed.kind == process.kind  # type: ignore
    assert parsed.process_uri == process.process_uri
    assert parsed.owner == process.owner
    assert parsed.name == process.name
    assert parsed.mode == process.mode
    assert parsed.created_at.isoformat() == process.created_at.isoformat()
    assert parsed.updated_at.isoformat() == process.updated_at.isoformat()
    assert parsed.arguments == process.arguments
    assert parsed.history == process.history
    assert parsed.result == process.result

    if isinstance(process.result, ProcessSuccess):
        assert isinstance(parsed.result, ProcessSuccess)
        assert type(parsed.result.value) is type(process.result.value)

    parsed_self = parse_yaml_as(type(process), process_yaml)
    print(f"<parsed_self>\n{as_yaml(parsed_self)}\n</parsed_self>")
    assert parsed_self == parsed
