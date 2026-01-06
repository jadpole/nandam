import jsonref

from pydantic import TypeAdapter
from pydantic.json_schema import JsonSchemaValue
from typing import Any


##
## Pydantic BaseModel -> JSON-Schema (compatible with OpenAI)
##


def as_jsonschema(model: type) -> JsonSchemaValue:
    schema = TypeAdapter(model).json_schema()
    schema = _normalize_jsonrefs(schema)
    schema.pop("$defs", None)
    _clean_jsonschema_mut(schema)
    return schema


def _clean_jsonschema_mut(node: Any) -> None:  # noqa: C901, PLR0912
    if not isinstance(node, dict):
        return

    # If this is an object schema, enforce strictness.
    properties = node.get("properties")
    node_type = node.get("type")
    if isinstance(properties, dict) or node_type == "object":
        # Some schemas may omit type but provide properties; normalize type.
        if node_type is None:
            node["type"] = "object"
        # Require all properties and disallow additional props
        node["additionalProperties"] = False
        if isinstance(properties, dict):
            node["required"] = sorted(properties.keys())

    # Remove unnecessary information injected by Pydantic.
    node.pop("title", None)

    # Recurse into known child locations
    # - properties
    if isinstance(properties, dict):
        for child in properties.values():
            _clean_jsonschema_mut(child)

    # - items (array schemas)
    if "items" in node:
        items = node["items"]
        if isinstance(items, list):
            for child in items:
                _clean_jsonschema_mut(child)
        else:
            _clean_jsonschema_mut(items)

    # - composite keywords
    for key in ("allOf", "anyOf", "oneOf", "not", "if", "then", "else"):
        if key in node:
            value = node[key]
            if isinstance(value, list):
                for child in value:
                    _clean_jsonschema_mut(child)
            else:
                _clean_jsonschema_mut(value)

    # - definitions for referenced sub-schemas
    for defs_key in ("$defs", "definitions"):
        if defs_key in node and isinstance(node[defs_key], dict):
            for child in node[defs_key].values():
                _clean_jsonschema_mut(child)

    # - patternProperties/unevaluatedProperties are not used, but traverse if present
    if "patternProperties" in node and isinstance(node["patternProperties"], dict):
        for child in node["patternProperties"].values():
            _clean_jsonschema_mut(child)


def _normalize_jsonrefs(schema: JsonSchemaValue) -> JsonSchemaValue:
    return jsonref.replace_refs(schema, loader=_jsonref_loader, proxies=False)  # type: ignore


def _jsonref_loader(uri: str, **kwargs) -> dict[str, Any]:
    """
    For security, disallow reading referenced schemas from the Web.
    """
    if uri:
        raise ValueError(f"External references are not allowed: {uri}")
    return jsonref.jsonloader(uri, **kwargs)
