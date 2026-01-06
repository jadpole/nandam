import pytest

from pydantic import BaseModel, TypeAdapter

from base.strings.microsoft import (
    MsChannelId,
    MsChannelName,
    MsSiteId,
    MsSiteName,
    REGEX_MS_CHANNEL_ID,
    REGEX_MS_SITE_ID,
    REGEX_MS_SITE_NAME,
)


##
## MsChannelName
##


class DemoMsChannelName(BaseModel):
    channel_name: MsChannelName


def test_mschannelname_jsonschema() -> None:
    schema = TypeAdapter(MsChannelName).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "MsChannelName"
    assert schema.get("pattern") is None


def test_mschannelname_field_jsonschema() -> None:
    schema = TypeAdapter(DemoMsChannelName).json_schema()
    print(schema)
    assert "channel_name" in schema["properties"]
    assert "channel_name" in schema["required"]
    assert schema["properties"]["channel_name"]["type"] == "string"
    assert schema["properties"]["channel_name"]["title"] == "MsChannelName"
    assert schema["properties"]["channel_name"].get("pattern") is None


@pytest.mark.parametrize("value", MsChannelName._schema_examples())
def test_mschannelname_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(MsChannelName).validate_python(value)
    print(actual)
    assert type(actual) is MsChannelName
    assert actual == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(MsChannelName).validate_json(value_json)
    assert type(actual_json) is MsChannelName
    assert actual_json == actual


##
## MsChannelId
##


class DemoMsChannelId(BaseModel):
    channel_id: MsChannelId


def test_mschannelid_jsonschema() -> None:
    schema = TypeAdapter(MsChannelId).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "MsChannelId"
    assert schema.get("pattern") == f"^{REGEX_MS_CHANNEL_ID}$"


def test_mschannelid_field_jsonschema() -> None:
    schema = TypeAdapter(DemoMsChannelId).json_schema()
    print(schema)
    assert "channel_id" in schema["properties"]
    assert "channel_id" in schema["required"]
    assert schema["properties"]["channel_id"]["type"] == "string"
    assert schema["properties"]["channel_id"]["title"] == "MsChannelId"
    assert (
        schema["properties"]["channel_id"].get("pattern") == f"^{REGEX_MS_CHANNEL_ID}$"
    )


@pytest.mark.parametrize("value", MsChannelId._schema_examples())
def test_mschannelid_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(MsChannelId).validate_python(value)
    print(actual)
    assert type(actual) is MsChannelId
    assert actual == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(MsChannelId).validate_json(value_json)
    assert type(actual_json) is MsChannelId
    assert actual_json == actual


##
## MsSiteId
##


class DemoMsSiteId(BaseModel):
    site_id: MsSiteId


def test_mssiteid_jsonschema() -> None:
    schema = TypeAdapter(MsSiteId).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "MsSiteId"
    assert schema["pattern"] == f"^{REGEX_MS_SITE_ID}$"


def test_mssiteid_field_jsonschema() -> None:
    schema = TypeAdapter(DemoMsSiteId).json_schema()
    print(schema)
    assert "site_id" in schema["properties"]
    assert "site_id" in schema["required"]
    assert schema["properties"]["site_id"]["type"] == "string"
    assert schema["properties"]["site_id"]["title"] == "MsSiteId"
    assert schema["properties"]["site_id"]["pattern"] == f"^{REGEX_MS_SITE_ID}$"


@pytest.mark.parametrize("value", MsSiteId._schema_examples())
def test_mssiteid_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(MsSiteId).validate_python(value)
    print(actual)
    assert type(actual) is MsSiteId
    assert actual == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(MsSiteId).validate_json(value_json)
    assert type(actual_json) is MsSiteId
    assert actual_json == actual


@pytest.mark.parametrize(
    "value",
    [
        "",
        "-00000000-0000-0000-0000-000000000000",
        "-0000-0000-0000-000000000000",
        "0000-0000-0000-000000000000",
        "00000000-0000-0000-0000-000000000000-",
        "00000000-0000-0000-0000-",
        "00000000-0000-0000-0000",
    ],
)
def test_mssiteid_validate_invalid_format(value: str) -> None:
    with pytest.raises(ValueError, match="invalid MsSiteId: expected pattern"):
        TypeAdapter(MsSiteId).validate_python(value)


##
## MsSiteName
##


class DemoMsSiteName(BaseModel):
    site_name: MsSiteName


def test_mssitename_jsonschema() -> None:
    schema = TypeAdapter(MsSiteName).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "MsSiteName"
    assert schema["pattern"] == f"^{REGEX_MS_SITE_NAME}$"


def test_mssitename_field_jsonschema() -> None:
    schema = TypeAdapter(DemoMsSiteName).json_schema()
    print(schema)
    assert "site_name" in schema["properties"]
    assert "site_name" in schema["required"]
    assert schema["properties"]["site_name"]["type"] == "string"
    assert schema["properties"]["site_name"]["title"] == "MsSiteName"
    assert schema["properties"]["site_name"]["pattern"] == f"^{REGEX_MS_SITE_NAME}$"


@pytest.mark.parametrize("value", MsSiteName._schema_examples())
def test_mssitename_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(MsSiteName).validate_python(value)
    print(actual)
    assert type(actual) is MsSiteName
    assert actual == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(MsSiteName).validate_json(value_json)
    assert type(actual_json) is MsSiteName
    assert actual_json == actual


@pytest.mark.parametrize(
    "value",
    [
        "",
        "-starts-with-dash",
        "ends-with-dash-",
        "contains--double-dash",
        "special@characters",
        "123-starts-with-numbers",
    ],
)
def test_mssitename_validate_invalid_format(value: str) -> None:
    with pytest.raises(ValueError, match="invalid MsSiteName: expected pattern"):
        TypeAdapter(MsSiteName).validate_python(value)
