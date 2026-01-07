import pytest

from pydantic import BaseModel, TypeAdapter

from base.strings.data import DataUri, MimeType, REGEX_DATA_URI, REGEX_MIMETYPE


##
## DataUri
##


class DemoDataUri(BaseModel):
    datauri: DataUri


INVALID_EXAMPLES_DATAURI = {
    "missing_data": "data:image/png;base64,",
    "missing_mime_type": "image/png;base64,"
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5/hPwAIAgL/4d1j8wAAAABJRU5ErkJggg==",
    "missing_prefix_data": ":image/png;base64,"
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5/hPwAIAgL/4d1j8wAAAABJRU5ErkJggg==",
    "missing_suffix_base64": "data:image/png;,"
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5/hPwAIAgL/4d1j8wAAAABJRU5ErkJggg==",
}


def test_datauri_jsonschema() -> None:
    schema = TypeAdapter(DataUri).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "DataUri"
    assert schema["pattern"] == f"^{REGEX_DATA_URI}$"


def test_datauri_field_jsonschema() -> None:
    schema = TypeAdapter(DemoDataUri).json_schema()
    print(schema)
    assert "datauri" in schema["properties"]
    assert "datauri" in schema["required"]
    assert schema["properties"]["datauri"]["type"] == "string"
    assert schema["properties"]["datauri"]["title"] == "DataUri"
    assert schema["properties"]["datauri"]["pattern"] == f"^{REGEX_DATA_URI}$"


@pytest.mark.parametrize("value", DataUri._schema_examples())
def test_datauri_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(DataUri).validate_python(value)
    print(actual)
    assert type(actual) is DataUri
    assert actual == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(DataUri).validate_json(value_json)
    assert type(actual_json) is DataUri
    assert actual_json == actual

    mime_type, data_base64 = actual.parts()
    rewrapped = DataUri.new(mime_type, data_base64)
    assert rewrapped == value


@pytest.mark.parametrize("value", list(INVALID_EXAMPLES_DATAURI.keys()))
def test_filename_validate_invalid_format(value: str) -> None:
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(DataUri).validate_python(INVALID_EXAMPLES_DATAURI[value])


##
## MimeType
##


class DemoMimeType(BaseModel):
    mimetype: MimeType


def test_mimetype_jsonschema() -> None:
    schema = TypeAdapter(MimeType).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "MimeType"
    assert schema["pattern"] == f"^{REGEX_MIMETYPE}$"


def test_mimetype_field_jsonschema() -> None:
    schema = TypeAdapter(DemoMimeType).json_schema()
    print(schema)
    assert "mimetype" in schema["properties"]
    assert "mimetype" in schema["required"]
    assert schema["properties"]["mimetype"]["type"] == "string"
    assert schema["properties"]["mimetype"]["title"] == "MimeType"
    assert schema["properties"]["mimetype"]["pattern"] == f"^{REGEX_MIMETYPE}$"


@pytest.mark.parametrize("value", MimeType._schema_examples())
def test_mimetype_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(MimeType).validate_python(value)
    print(actual)
    assert type(actual) is MimeType
    assert actual == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(MimeType).validate_json(value_json)
    assert type(actual_json) is MimeType
    assert actual_json == actual


@pytest.mark.parametrize("value", ["", "text", "text/html#"])
def test_mimetype_validate_invalid_format(value: str) -> None:
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(MimeType).validate_python(value)


@pytest.mark.parametrize(
    ("mime_type", "mode"),
    [
        ("application/pdf", "document"),
        (
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "document",
        ),
        (
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "spreadsheet",
        ),
        (
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "document",
        ),
        ("audio/mpeg", "media"),
        ("image/jpeg", "image"),
        ("text/csv", "spreadsheet"),
        ("text/markdown", "markdown"),
        ("text/plain", "plain"),
        ("video/mp4", "media"),
    ],
)
def test_mimetype_mode(mime_type: str, mode: str) -> None:
    assert MimeType.decode(mime_type).mode() == mode
