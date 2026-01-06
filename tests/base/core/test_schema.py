from datetime import datetime
from pydantic import BaseModel, Field

from base.core.schema import as_jsonschema
from base.core.values import as_json
from base.strings.resource import Realm


class ExampleFlat(BaseModel):
    name: str = Field(..., description="The name of the person")
    age: int | None = None


class ExampleNested(BaseModel):
    users: list[ExampleFlat]
    last_refreshed: datetime


EXPECTED_SCHEMA_FLAT = {
    "type": "object",
    "properties": {
        "name": {"description": "The name of the person", "type": "string"},
        "age": {"anyOf": [{"type": "integer"}, {"type": "null"}], "default": None},
    },
    "required": ["age", "name"],
    "additionalProperties": False,
}


EXPECTED_SCHEMA_REALM = {
    "type": "string",
    "pattern": "^[a-z][a-z0-9]+(?:-[a-z0-9]+)*$",
    "examples": ["jira", "sharepoint", "www"],
}


def test_as_jsonschema_flat():
    actual = as_jsonschema(ExampleFlat)
    print(f"<actual>\n{as_json(actual,indent=2)}\n</actual>")
    assert actual == EXPECTED_SCHEMA_FLAT


def test_as_jsonschema_nested():
    actual = as_jsonschema(ExampleNested)
    print(f"<actual>\n{as_json(actual,indent=2)}\n</actual>")
    assert actual == {
        "type": "object",
        "properties": {
            "users": {
                "type": "array",
                "items": EXPECTED_SCHEMA_FLAT,
            },
            "last_refreshed": {
                "type": "string",
                "format": "date-time",
            },
        },
        "required": ["last_refreshed", "users"],
        "additionalProperties": False,
    }


def test_as_jsonschema_validated_str():
    class WrappedRealm(BaseModel):
        value: Realm

    actual = as_jsonschema(Realm)
    actual_wrapped = as_jsonschema(WrappedRealm)
    print(f"<actual>\n{as_json(actual,indent=2)}\n</actual>")
    print(f"<actual_wrapped>\n{as_json(actual_wrapped,indent=2)}\n</actual_wrapped>")

    assert actual == EXPECTED_SCHEMA_REALM
    assert actual_wrapped == {
        "type": "object",
        "properties": {"value": EXPECTED_SCHEMA_REALM},
        "required": ["value"],
        "additionalProperties": False,
    }
