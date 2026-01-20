import pytest

from pydantic import BaseModel, TypeAdapter

from base.strings.auth import UserId
from base.strings.microsoft import MsGroupId
from base.strings.scope import (
    REGEX_RELEASE,
    REGEX_SCOPE_PERSONAL,
    REGEX_SCOPE,
    Release,
    Scope,
    ScopeInternal,
    ScopeMsGroup,
    ScopePersonal,
    ScopePrivate,
    Workspace,
    WorkspaceDefault,
)


##
## Release
##


class DemoRelease(BaseModel):
    release: Release


def test_release_jsonschema() -> None:
    schema = TypeAdapter(Release).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "Release"
    assert schema["pattern"] == f"^{REGEX_RELEASE}$"


def test_release_field_jsonschema() -> None:
    schema = TypeAdapter(DemoRelease).json_schema()
    print(schema)
    assert "release" in schema["properties"]
    assert "release" in schema["required"]
    assert schema["properties"]["release"]["type"] == "string"
    assert schema["properties"]["release"]["title"] == "Release"
    assert schema["properties"]["release"]["pattern"] == f"^{REGEX_RELEASE}$"


@pytest.mark.parametrize("value", Release._schema_examples())
def test_release_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(Release).validate_python(value)
    print(actual)
    assert type(actual) is Release
    assert actual == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(Release).validate_json(value_json)
    assert type(actual_json) is Release
    assert actual_json == actual


@pytest.mark.parametrize("value", ["", "aiexporterprod", "ai_exporter_prod"])
def test_release_validate_invalid_format(value: str) -> None:
    with pytest.raises(ValueError, match="invalid Release: expected pattern"):
        TypeAdapter(Release).validate_python(value)


@pytest.mark.parametrize("value", ["ai-exporter"])
def test_release_validate_invalid_missing_environment(value: str) -> None:
    with pytest.raises(ValueError, match="invalid Release: expected pattern"):
        TypeAdapter(Release).validate_python(value)


##
## Scope - Schema
##


class DemoScope(BaseModel):
    scope: Scope


class DemoScopePersonal(BaseModel):
    scope: ScopePersonal


def test_scope_jsonschema() -> None:
    schema = TypeAdapter(Scope).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "Scope"
    assert schema["pattern"] == f"^{REGEX_SCOPE}$"

    schema = Scope.model_json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "Scope"
    assert schema["pattern"] == f"^{REGEX_SCOPE}$"


def test_scope_field_jsonschema() -> None:
    schema = DemoScope.model_json_schema()
    print(schema)
    assert "scope" in schema["properties"]
    assert "scope" in schema["required"]
    assert schema["properties"]["scope"] == {"$ref": "#/$defs/Scope"}
    assert schema["$defs"]["Scope"]["type"] == "string"
    assert schema["$defs"]["Scope"]["title"] == "Scope"
    assert schema["$defs"]["Scope"]["pattern"] == f"^{REGEX_SCOPE}$"


def test_scope_personal_jsonschema() -> None:
    schema = TypeAdapter(ScopePersonal).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "ScopePersonal"
    assert schema["pattern"] == f"^{REGEX_SCOPE_PERSONAL}$"

    schema = ScopePersonal.model_json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "ScopePersonal"
    assert schema["pattern"] == f"^{REGEX_SCOPE_PERSONAL}$"


def test_scope_personal_field_jsonschema() -> None:
    schema = DemoScopePersonal.model_json_schema()
    print(schema)
    assert "scope" in schema["properties"]
    assert "scope" in schema["required"]
    assert schema["properties"]["scope"] == {"$ref": "#/$defs/ScopePersonal"}
    assert schema["$defs"]["ScopePersonal"]["type"] == "string"
    assert schema["$defs"]["ScopePersonal"]["title"] == "ScopePersonal"
    assert schema["$defs"]["ScopePersonal"]["pattern"] == f"^{REGEX_SCOPE_PERSONAL}$"


##
## Scope - Serialization
##


@pytest.mark.parametrize("value", Scope._schema_examples())
def test_scope_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(Scope).validate_python(value)
    print(actual)
    assert isinstance(actual, Scope)
    assert type(actual) is not Scope
    assert str(actual) == value
    assert actual.model_dump() == value
    assert actual.model_dump_json() == f'"{value}"'

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(Scope).validate_json(value_json)
    assert isinstance(actual_json, Scope)
    assert type(actual_json) is not Scope
    assert actual_json == actual


def test_scope_variant_internal() -> None:
    value = "internal"
    scope = TypeAdapter(Scope).validate_python(value)
    print(scope)

    assert type(scope) is ScopeInternal
    assert str(scope) == value
    assert scope == ScopeInternal()


def test_scope_variant_msgroup() -> None:
    group_id = MsGroupId.decode("c9bb7ba2-c84a-4247-9d48-d5dc0e8b59e6")
    value = f"msgroup-{group_id}"
    scope = TypeAdapter(Scope).validate_python(value)
    print(scope)

    assert type(scope) is ScopeMsGroup
    assert str(scope) == value
    assert scope.group_id == group_id
    assert scope == ScopeMsGroup(group_id=group_id)


def test_scope_variant_personal() -> None:
    user_id = UserId.teams("54916b77-a320-4496-a8f6-f4ce7ab46fc8")
    value = "personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8"
    scope = TypeAdapter(Scope).validate_python(value)
    print(scope)

    assert type(scope) is ScopePersonal
    assert str(scope) == value
    assert scope.user_id == user_id
    assert scope == ScopePersonal(user_id=user_id)


def test_scope_variant_private() -> None:
    chat_id = "0123456789abcdefghijklmnopqrstuvwxyz"
    value = "private-0123456789abcdefghijklmnopqrstuvwxyz"
    scope = TypeAdapter(Scope).validate_python(value)
    print(scope)

    assert type(scope) is ScopePrivate
    assert str(scope) == value
    assert scope.chat_id == chat_id
    assert scope == ScopePrivate(chat_id=chat_id)


##
## Workspace - Schema
##


# TODO


##
## Workspace - Serialization
##


@pytest.mark.parametrize("value", Workspace._schema_examples())
def test_workspace_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(Workspace).validate_python(value)
    print(actual)
    assert isinstance(actual, Workspace)
    assert type(actual) is not Workspace
    assert str(actual) == value
    assert actual.model_dump() == value
    assert actual.model_dump_json() == f'"{value}"'

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(Workspace).validate_json(value_json)
    assert isinstance(actual_json, Workspace)
    assert type(actual_json) is not Workspace
    assert actual_json == actual

    if isinstance(actual, WorkspaceDefault) and actual.release:
        assert WorkspaceDefault.new(actual.scope, actual.release) == actual


@pytest.mark.parametrize(
    "value",
    [
        "ndw://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/default-",
        "ndw://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/channel-",
        "ndw://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/channel-####################################",
        "ndw://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/unknown",
    ],
)
def test_workspace_validate_err_invalid(value: str) -> None:
    with pytest.raises(ValueError, match="invalid Workspace: expected pattern"):
        TypeAdapter(Workspace).validate_python(value)


@pytest.mark.parametrize(
    ("expected", "release"),
    [
        ("ndw://internal/default-ai-exporter-prod", "ai-exporter-prod"),
        (
            "ndw://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/default-ai-exporter-prod",
            "ai-exporter-prod",
        ),
        (
            "ndw://personal-54916b77-a320-4496-a8f6-f4ce7ab46fc8/default",
            "nandam-teams-dev",
        ),
        (
            "ndw://private-0123456789abcdefghijklmnopqrstuvwxyz/default",
            "ai-exporter-prod",
        ),
        (
            "ndw://private-0123456789abcdefghijklmnopqrstuvwxyz/default",
            "nandam-teams-dev",
        ),
    ],
)
def test_workspace_variant_default_ok(expected: str, release: str) -> None:
    scope_str = expected.removeprefix("ndw://").split("/", 1)[0]
    parsed_scope = Scope.decode(scope_str)
    parsed_release = Release.decode(release)
    actual = WorkspaceDefault.new(parsed_scope, parsed_release)
    assert str(actual) == expected
