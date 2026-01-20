import copy
import jcs
import json
import logging
import yaml

from collections.abc import Iterable
from datetime import datetime
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, TypeAdapter
from typing import Any

from base.config import BaseConfig

logger = logging.getLogger(__name__)


##
## YAML Dumper
##


YAML_MAX_LINE_LENGTH = 80
"""
The maximum string length before using the "|-" style.
"""

YAML_PREVENT_BLOCKS_WIDTH = 240
"""
The maximum line width before wrapping when `prevent_blocks` is `True`.
"""


class YamlResponse(PlainTextResponse):
    media_type = "text/x-yaml"

    def render(self, content: Any) -> bytes:
        return as_yaml(content).encode("utf-8")


def yaml_representer_str(dumper: yaml.SafeDumper, data: Any) -> yaml.ScalarNode:
    """
    Strings that contain newlines, or that are longer than 80 characters and
    contain whitespace are, wrapped in a YAML literal style block scalar, i.e.,
    prefixed with "|-".  Otherwise, left as-is.
    """
    dumper.allow_unicode = True
    if (len(data) > YAML_MAX_LINE_LENGTH and " " in data) or "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


class NandamYamlDumper(yaml.SafeDumper):
    pass


NandamYamlDumper.add_representer(str, yaml_representer_str)


##
## Pydantic
##


def as_value(value: Any) -> Any:  # noqa: PLR0911
    if isinstance(value, BaseModel):
        if value.model_extra:
            extra = copy.deepcopy(value.model_extra) if value.model_extra else {}
            fields = value.model_dump(fallback=as_value)
            return {**fields, **extra}
        else:
            return value.model_dump(fallback=as_value)
    elif isinstance(value, dict):
        return {as_value(key): as_value(value) for key, value in value.items()}
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, (int, float, bool)):
        return value
    elif isinstance(value, str):
        return str(value)
    elif isinstance(value, Iterable):
        return [as_value(item) for item in value]
    else:
        return str(value)


def as_json(
    value: Any,
    *,
    indent: int | None = None,
) -> str:
    return json.dumps(value, default=as_value, indent=indent)


def as_json_canonical(value: Any) -> str:
    return jcs.canonicalize(as_value(value), utf8=False)  # type: ignore


def as_yaml(
    value: Any,
    *,
    sort_keys: bool = False,
    prevent_blocks: bool = False,
) -> str:
    """
    Serialize `value` as YAML.

    NOTE: The current implementation is inefficient, since it uses JSON as an
    intermediate representation to convert `str` subclasses and `StructStr` into
    the primitive `str` type.
    """
    clean_value = json.loads(as_json(value))
    if prevent_blocks:
        return yaml.dump(
            clean_value,
            Dumper=yaml.SafeDumper,
            width=YAML_PREVENT_BLOCKS_WIDTH,
            sort_keys=sort_keys,
        ).strip()
    else:
        return yaml.dump(
            clean_value,
            Dumper=NandamYamlDumper,
            sort_keys=sort_keys,
        ).strip()


def parse_yaml_as[T](type_: type[T], value: bytes | str) -> T:
    """
    Deserialize `value` into a Pydantic model.
    """
    return TypeAdapter(type_).validate_python(yaml.safe_load(value))


def try_parse_yaml_as[T](type_: type[T], value: bytes | str) -> T | None:
    try:
        return parse_yaml_as(type_, value)
    except ValueError:
        if BaseConfig.verbose:
            logger.exception(
                "Failed to parse YAML as %s:\n```\n%s\n```\n",
                type_.__name__,
                value.strip(),
            )
        return None


def wrap_exclude_none(value: Any, handler, info) -> Any:
    """
    Usage: `Annotated[T, WrapSerializer(wrap_exclude_none)]`
    """
    partial_result = handler(value, info)
    return {k: v for k, v in partial_result.items() if v is not None}


def wrap_exclude_none_or_empty(value: Any, handler, info) -> Any:
    """
    Usage: `Annotated[T, WrapSerializer(wrap_exclude_none_or_empty)]`
    """
    partial_result = handler(value, info)
    return {k: v for k, v in partial_result.items() if v is not None and v != []}
