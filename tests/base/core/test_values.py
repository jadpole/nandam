import pytest
import yaml

from datetime import datetime, UTC
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel
from typing import Any

from base.core.strings import ValidatedStr
from base.core.values import as_yaml, parse_yaml_as, YamlResponse
from base.strings.file import FilePath


##
## Model
##


class DemoItem(BaseModel):
    path: FilePath
    value: int

    @classmethod
    def stub(cls, path: str, value: int) -> "DemoItem":
        return DemoItem(path=FilePath.decode(path), value=value)


class DemoModel(BaseModel):
    title: str
    items: list[DemoItem]
    metadata: dict[FilePath, Any]
    created_at: datetime
    optional_field: str | None = None

    @classmethod
    def stub(cls) -> "DemoModel":
        return cls(
            title="Example",
            items=[
                DemoItem.stub("some-path", 12),
                DemoItem.stub("another/path", 42),
            ],
            metadata={
                FilePath.decode("key"): "value",
                FilePath.decode("nested"): {"inside": True},
            },
            created_at=datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC),
        )


class DemoString(ValidatedStr):
    @classmethod
    def _parse(cls, v: str) -> "DemoString":
        if not v:
            raise ValueError("Cannot be empty")
        return cls(v)


class WrappedString(BaseModel):
    content: DemoString

    @classmethod
    def stub(cls, content: str) -> "WrappedString":
        return cls(content=DemoString.decode(content))


##
## YamlResponse
##


def test_yaml_response_render() -> None:
    """Test YamlResponse rendering of content"""
    app = FastAPI()

    @app.get("/test", response_class=YamlResponse)
    def get_test():
        return DemoItem.stub("test", 42)

    client = TestClient(app)
    response = client.get("/test")
    print(response)

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/x-yaml; charset=utf-8"

    # Parse the response content
    content = yaml.safe_load(response.content)
    assert type(content["path"]) is str  # Not the wrapper class!
    assert content["path"] == "test"
    assert content["value"] == 42


##
## as_yaml, parse_yaml_as - ValidatedStr
##


def test_as_yaml_short_string() -> None:
    content_str = "Short string"
    wrapped = WrappedString.stub(content_str)
    actual = as_yaml(wrapped)
    print(actual)
    assert actual == "content: Short string"

    decoded = parse_yaml_as(WrappedString, actual)
    assert type(decoded) is WrappedString
    assert type(decoded.content) is DemoString
    assert decoded == wrapped
    assert decoded.content == content_str


def test_as_yaml_long_string() -> None:
    content_str = "A " + "very " * 14 + "long string"  # Length of 83 > threshold.
    wrapped = WrappedString.stub(content_str)
    actual = as_yaml(wrapped)
    print(actual)
    assert actual == f"content: |-\n  {content_str}"

    decoded = parse_yaml_as(WrappedString, actual)
    assert type(decoded) is WrappedString
    assert type(decoded.content) is DemoString
    assert decoded == wrapped
    assert decoded.content == content_str


def test_as_yaml_multiline_string() -> None:
    content_str = "Line 1\nLine 2\nLine 3"
    actual = as_yaml(WrappedString.stub(content_str))
    wrapped = WrappedString.stub(content_str)
    actual = as_yaml(wrapped)
    print(actual)
    assert actual == "content: |-\n  Line 1\n  Line 2\n  Line 3"

    decoded = parse_yaml_as(WrappedString, actual)
    assert type(decoded) is WrappedString
    assert type(decoded.content) is DemoString
    assert decoded == wrapped
    assert decoded.content == content_str


##
## as_yaml, parse_yaml_as - nested models
##


def test_as_yaml_simple_model() -> None:
    """Test serializing a simple Pydantic model to YAML"""
    model = DemoItem.stub("test", 42)

    yaml_str = as_yaml(model)
    print(yaml_str)
    assert yaml_str == "path: test\nvalue: 42"

    parsed = parse_yaml_as(DemoItem, yaml_str)
    print(parsed)
    assert type(parsed) is DemoItem
    assert type(parsed.path) is FilePath
    assert parsed == model


def test_as_yaml_complex_model() -> None:
    """Test serializing a complex Pydantic model to YAML"""
    model = DemoModel.stub()

    yaml_str = as_yaml(model)
    print(yaml_str)
    assert (
        yaml_str
        == """\
title: Example
items:
- path: some-path
  value: 12
- path: another/path
  value: 42
metadata:
  key: value
  nested:
    inside: true
created_at: '2023-01-01T12:00:00+00:00'
optional_field: null\
"""
    )
    # Verify the YAML structure
    parsed = parse_yaml_as(DemoModel, yaml_str)
    print(parsed)
    assert type(parsed) is DemoModel
    assert parsed == model

    assert type(parsed.items[0]) is DemoItem
    assert type(parsed.items[0].path) is FilePath
    assert type(parsed.items[1]) is DemoItem
    assert type(parsed.items[1].path) is FilePath
    assert all(type(k) is FilePath for k in parsed.metadata)


def test_as_yaml_dict_with_datetime() -> None:
    """Test datetime handling in YAML serialization"""
    dt = datetime(2023, 5, 15, 10, 30, 0, tzinfo=UTC)
    yaml_str = as_yaml({"date": dt})
    assert yaml_str == "date: '2023-05-15T10:30:00+00:00'"


def test_as_yaml_custom_type_with_str_repr() -> None:
    """Test handling of custom types in YAML serialization"""

    class CustomType:
        def __str__(self) -> str:
            return "custom-value"

    yaml_str = as_yaml({"custom": CustomType()})
    assert yaml_str == "custom: custom-value"


##
## parse_yaml_as - error handling
##


def test_parse_yaml_as_error_missing() -> None:
    """Test validation errors during YAML parsing"""
    yaml_str = """
    # missing path field
    value: 42
    """
    with pytest.raises(ValueError, match="Field required"):
        parse_yaml_as(DemoItem, yaml_str)


def test_parse_yaml_as_error_empty() -> None:
    """Test validation errors during YAML parsing"""
    yaml_str = """
    path: ''
    value: 42
    """
    with pytest.raises(ValueError, match="invalid FilePath: expected pattern"):
        parse_yaml_as(DemoItem, yaml_str)


def test_parse_yaml_as_error_format() -> None:
    """Test validation errors during YAML parsing"""
    yaml_str = """
    path: 'broken/../path'
    value: 42
    """
    with pytest.raises(
        ValueError,
        match=r"invalid FilePath: got 'broken/../path': invalid FileName: got '..'",
    ):
        parse_yaml_as(DemoItem, yaml_str)


def test_parse_yaml_as_error_wrong_type() -> None:
    """Test validation errors during YAML parsing"""
    yaml_str = """
    path:
      some: 'random object'
    value: 42
    """
    with pytest.raises(ValueError, match="Input should be a valid string"):
        parse_yaml_as(DemoItem, yaml_str)
