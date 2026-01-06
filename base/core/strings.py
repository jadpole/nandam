import re
import unicodedata

from collections.abc import Hashable
from pydantic import (
    BaseModel,
    ModelWrapValidatorHandler,
    model_validator,
    model_serializer,
)
from pydantic_core import core_schema
from pydantic.annotated_handlers import GetCoreSchemaHandler, GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from typing import Any, Literal, Self, TYPE_CHECKING
from urllib.parse import unquote_plus


class ValidatedStr(str):
    __slots__ = ()

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source: type[Any],
        _handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_after_validator_function(
            cls.decode,
            core_schema.str_schema(),
            serialization={"type": "to-string"},
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        core_schema: core_schema.CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        field_schema = handler(core_schema)
        field_schema.update(
            type="string",
            title=cls.__name__,
            pattern=(
                f"^{schema_regex}$" if (schema_regex := cls._schema_regex()) else None
            ),
            examples=cls._schema_examples(),
        )
        return field_schema

    @classmethod
    def decode(cls, v: Any, /) -> Self:
        """
        Parse a Python string into the `ValidatedStr` subclass, matching it
        against the regex and running the extra validation in `_parse` when it
        is defined for the subclass.

        Raises an exception (`TypeError` or `ValueError`) if the string is
        invalid according to either of these checks.
        """
        if not isinstance(v, str):
            raise TypeError(
                f"invalid {cls.__name__}: expected str, got {type(v).__name__}: {v}"
            )

        if (regex := cls._schema_regex()) and not re.fullmatch(regex, v):
            raise ValueError(
                f"invalid {cls.__name__}: expected pattern '{regex}', got '{v}'"
            )

        return cls._parse(v)

    @classmethod
    def try_decode(cls, v: Any) -> Self | None:
        """
        Similar to `validate`, but returns `None` on invalid inputs instead of
        raising an exception.  `None` inputs simply pass through as-is.
        """
        try:
            return cls.decode(v)
        except (TypeError, ValueError):
            return None

    @classmethod
    def decode_part(
        cls,
        container_type: type,
        container_value: str,
        part_value: str,
    ) -> Self:
        """
        Runs the validation from `validate`, but wraps the error (if any) into a
        container type of which `Self` is a part.
        """
        try:
            return cls.decode(part_value)
        except (TypeError, ValueError) as exc:
            error_message = (
                f"invalid {container_type.__name__}: got '{container_value}': {exc}"
            )
            raise ValueError(error_message) from exc

    @classmethod
    def _parse(cls, v: str) -> Self:
        """
        Extra validation for the subclass that runs when the regex matches,
        when it is not enough on its own.
        """
        return cls(v)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        """
        Examples of reasonable values for the subclass, used in JSON-Schema and
        in the unit tests for the validation.
        """
        return []

    @classmethod
    def _schema_regex(cls) -> str:
        """
        Regex pattern for the valid values for the subclass, included in the
        JSON-Schema and used for validation.
        """
        return ""


class StructStr(BaseModel, Hashable, frozen=True):
    def __hash__(self) -> int:
        return hash(self._serialize())

    def __lt__(self, other: Any) -> bool:
        return self._serialize() < str(other)

    def __str__(self) -> str:
        return self._serialize()

    @classmethod
    def __get_pydantic_json_schema__(
        cls,
        core_schema: core_schema.CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        field_schema = handler(core_schema)

        field_schema.update(
            type="string",
            title=cls.__name__,
            examples=cls._schema_examples(),
        )
        if schema_regex := cls._schema_regex():
            field_schema.update(pattern=f"^{schema_regex}$")

        field_schema.pop("description", None)
        field_schema.pop("properties", None)
        field_schema.pop("required", None)

        return field_schema

    @model_validator(mode="wrap")
    @classmethod
    def deserialize_model(
        cls,
        data: Any,
        handler: ModelWrapValidatorHandler[Self],
    ) -> Self:
        if isinstance(data, str):
            return cls.decode(data)
        return handler(data)

    @model_serializer
    def serialize_model(self) -> str:
        return self._serialize()

    if TYPE_CHECKING:
        # Ensure type checkers see the correct return type
        def model_dump(  # pyright: ignore[reportIncompatibleMethodOverride]
            self,
            *,
            mode: Literal["json", "python"] | str = "python",  # noqa: PYI051
            include: Any = None,
            exclude: Any = None,
            by_alias: bool | None = False,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            round_trip: bool = False,
            warnings: bool = True,
        ) -> str: ...

    @classmethod
    def decode(cls, v: Any, /) -> Self:
        """
        Parse a Python string into the `ValidatedStr` subclass, matching it
        against the regex and running the extra validation in `_parse` when it
        is defined for the subclass.

        Raises an exception (`TypeError` or `ValueError`) if the string is
        invalid according to either of these checks.
        """
        if not isinstance(v, str):
            raise TypeError(
                f"invalid {cls.__name__}: expected str, got {type(v).__name__}: {v}"
            )

        if (regex := cls._schema_regex()) and not re.fullmatch(regex, v):
            raise ValueError(
                f"invalid {cls.__name__}: expected pattern '{regex}', got '{v}'"
            )

        return cls._parse(v)

    @classmethod
    def try_decode(cls, v: Any) -> Self | None:
        """
        Similar to `validate`, but returns `None` on invalid inputs instead of
        raising an exception.  `None` inputs simply pass through as-is.
        """
        try:
            return cls.decode(v)
        except (TypeError, ValueError):
            return None

    @classmethod
    def decode_part(
        cls,
        container_type: type,
        container_value: str,
        part_value: str,
    ) -> Self:
        """
        Runs the validation from `validate`, but wraps the error (if any) into a
        container type of which `Self` is a part.
        """
        try:
            return cls.decode(part_value)
        except (TypeError, ValueError) as exc:
            error_message = (
                f"invalid {container_type.__name__}: got '{container_value}': {exc}"
            )
            raise ValueError(error_message) from exc

    @classmethod
    def _parse(cls, v: str) -> Self:
        """
        Extra validation for the subclass that runs when the regex matches,
        when it is not enough on its own.
        """
        raise NotImplementedError("Subclasses must implement StructStr._parse")

    @classmethod
    def _schema_examples(cls) -> list[str]:
        """
        Examples of reasonable values for the subclass, used in JSON-Schema and
        in the unit tests for the validation.
        """
        return []

    @classmethod
    def _schema_regex(cls) -> str:
        """
        Regex pattern for the valid values for the subclass, included in the
        JSON-Schema and used for validation.
        """
        return ""

    def _serialize(self) -> str:
        raise NotImplementedError("Subclasses must implement StructStr._serialize")


def normalize_str(
    value: str,
    *,
    allowed_special_chars: str = "",
    disallowed_replacement: str = "",
    other_replacements: dict[str, str] | None = None,
    remove_duplicate_chars: str = "",
    remove_prefix_chars: str = "",
    remove_suffix_chars: str = "",
    unquote_url: bool = False,
) -> str:
    """
    Turn any string into an ASCII alphanumeric equivalent, removing accents,
    and replace chars not in `allowed_special_characters` by `replacement`.
    """
    escaped = {"-", "."}
    regex_special_chars = "".join(
        f"\\{c}" if c in escaped else c for c in allowed_special_chars
    )

    # Unquote URL encodings (when enabled).
    if unquote_url:
        value = unquote_plus(value)

    # Normalize disallowed characters.
    value = value.strip()
    value = unicodedata.normalize("NFKD", value).encode("ASCII", "ignore").decode()
    if other_replacements:
        for before, after in other_replacements.items():
            value = value.replace(before, after)
    value = re.sub(
        rf"[^a-zA-Z0-9{regex_special_chars}]+", disallowed_replacement, value
    )

    # Remove non-alphanumeric prefix and suffix (when enabled).
    if remove_prefix_chars:
        value = value.lstrip(remove_prefix_chars)
    if remove_suffix_chars:
        value = value.rstrip(remove_suffix_chars)

    # Deduplicate consecutive special chars (when enabled).
    if remove_duplicate_chars:
        for c in remove_duplicate_chars:
            special_char = f"\\{c}" if c in escaped else c
            value = re.sub(special_char + "{2,}", c, value)

    return value
