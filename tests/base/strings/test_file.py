import pytest

from pydantic import BaseModel, TypeAdapter

from base.strings.file import FileName, FilePath, REGEX_FILENAME, REGEX_FILEPATH


def _run_normalize_str_filename(value: str, expected: str) -> None:
    normalized = FileName.normalize(value)
    assert normalized == expected


##
## FileName
##


class DemoFileName(BaseModel):
    filename: FileName


def test_filename_jsonschema() -> None:
    schema = TypeAdapter(FileName).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "FileName"
    assert schema["pattern"] == f"^{REGEX_FILENAME}$"


def test_filename_field_jsonschema() -> None:
    schema = DemoFileName.model_json_schema()
    print(schema)
    assert "filename" in schema["properties"]
    assert "filename" in schema["required"]
    assert schema["properties"]["filename"]["type"] == "string"
    assert schema["properties"]["filename"]["title"] == "FileName"
    assert schema["properties"]["filename"]["pattern"] == f"^{REGEX_FILENAME}$"


@pytest.mark.parametrize("value", FileName._schema_examples())
def test_filename_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(FileName).validate_python(value)
    print(actual)
    assert type(actual) is FileName
    assert actual == value

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(FileName).validate_json(value_json)
    assert type(actual_json) is FileName
    assert actual_json == actual

    normalized = FileName.normalize(value)
    assert normalized == actual


@pytest.mark.parametrize("value", ["-"])
def test_filename_validate_ok_special(value: str) -> None:
    actual = TypeAdapter(FileName).validate_python(value)
    print(actual)
    assert type(actual) is FileName
    assert actual == value

    normalized = FileName.normalize(value)
    assert normalized == actual


@pytest.mark.parametrize("value", ["", "file#yml"])
def test_filename_validate_invalid_format(value: str) -> None:
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(FileName).validate_python(value)


@pytest.mark.parametrize("value", ["."])
def test_filename_validate_invalid_special(value: str) -> None:
    with pytest.raises(ValueError, match=f"invalid FileName: got '{value}'"):
        TypeAdapter(FileName).validate_python(value)


def test_filename_normalize_valid():
    _run_normalize_str_filename(
        "Large_Document_Storage_and_Retrieval_with_Vector_Search.html",
        "Large_Document_Storage_and_Retrieval_with_Vector_Search.html",
    )


def test_filename_normalize_accents():
    _run_normalize_str_filename(
        "Réception.html",
        "Reception.html",
    )


def test_filename_normalize_special_chars():
    _run_normalize_str_filename(
        "Large+Document+Storage++and++Retrieval+with+Vector+Search.html",
        "Large_Document_Storage_and_Retrieval_with_Vector_Search.html",
    )
    _run_normalize_str_filename(
        "Large+Document+Storage/and/Retrieval+with+Vector+Search.html",
        "Large_Document_Storage_and_Retrieval_with_Vector_Search.html",
    )


def test_filename_normalize_unquote():
    _run_normalize_str_filename(
        "Large%20Document%20Storage%20%20and%20%20Retrieval%20with%20Vector%20Search.html",
        "Large_Document_Storage_and_Retrieval_with_Vector_Search.html",
    )


def test_filename_normalize_mixed():
    _run_normalize_str_filename(
        "Large-Document-Storage%20_and+%20Retrieval%20%20with__Vector++Search.html",
        "Large-Document-Storage_and_Retrieval_with_Vector_Search.html",
    )


##
## FilePath
##


class DemoFilePath(BaseModel):
    filepath: FilePath


def test_filepath_jsonschema() -> None:
    schema = TypeAdapter(FilePath).json_schema()
    print(schema)
    assert schema["type"] == "string"
    assert schema["title"] == "FilePath"
    assert schema["pattern"] == f"^{REGEX_FILEPATH}$"


def test_filepath_field_jsonschema() -> None:
    schema = DemoFilePath.model_json_schema()
    print(schema)
    assert "filepath" in schema["properties"]
    assert "filepath" in schema["required"]
    assert schema["properties"]["filepath"]["type"] == "string"
    assert schema["properties"]["filepath"]["title"] == "FilePath"
    assert schema["properties"]["filepath"]["pattern"] == f"^{REGEX_FILEPATH}$"


@pytest.mark.parametrize("value", FilePath._schema_examples())
def test_filepath_validate_ok_examples(value: str) -> None:
    actual = TypeAdapter(FilePath).validate_python(value)
    print(actual)
    assert type(actual) is FilePath
    assert actual == value
    assert all(type(part) is FileName for part in actual.parts())
    assert actual.parts() == value.split("/")

    value_json = f'"{value}"'.encode()
    actual_json = TypeAdapter(FilePath).validate_json(value_json)
    assert isinstance(actual_json, FilePath)
    assert actual_json == actual


@pytest.mark.parametrize("value", ["-"])
def test_filepath_validate_ok_special(value: str) -> None:
    actual = TypeAdapter(FilePath).validate_python(value)
    print(actual)
    assert type(actual) is FilePath
    assert actual == value


@pytest.mark.parametrize("value", ["", "file#yml"])
def test_filepath_validate_invalid_format(value: str) -> None:
    with pytest.raises(ValueError, match="expected pattern"):
        TypeAdapter(FilePath).validate_python(value)


@pytest.mark.parametrize("value", ["."])
def test_filepath_validate_invalid_special(value: str) -> None:
    with pytest.raises(ValueError, match=f"invalid FileName: {value}"):
        TypeAdapter(FilePath).validate_python(value)


def test_filepath_extend_filename() -> None:
    parent = FilePath.decode("a/b")
    actual = parent.extend(FileName("c")).extend(FileName("d"))
    print(actual)
    assert type(actual) is FilePath
    assert str(actual) == "a/b/c/d"


def test_filepath_extend_filepath() -> None:
    parent = FilePath.decode("a/b")
    actual = parent.extend(FilePath("c/d"))
    print(actual)
    assert type(actual) is FilePath
    assert str(actual) == "a/b/c/d"
