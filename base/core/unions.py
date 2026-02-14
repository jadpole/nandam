import logging

from pydantic import BaseModel, Field, model_validator, ModelWrapValidatorHandler
from typing import Annotated, Any, Self, Union


logger = logging.getLogger(__name__)

_ALREADY_WARNED: list[str] = []


class ModelUnion(BaseModel, frozen=True):
    @classmethod
    def discriminated_union(cls) -> Annotated:
        subclasses = [
            subclass
            for _, subclass in sorted(cls._subclasses().items(), key=lambda x: x[0])
        ]
        return Annotated[Union[*subclasses], Field(discriminator=cls._kind_field())]

    @classmethod
    def _kind_field(cls) -> str:
        return "kind"

    @classmethod
    def _subclasses(cls) -> dict[str, type[Self]]:
        kind_field = cls._kind_field()
        subclasses: dict[str, type[Self]] = {}
        cursors: list[type[Self]] = [cls]

        while cursors:
            next_cursors: list[type[Self]] = []
            for cursor in cursors:
                cursor_kind = cursor.model_fields.get(kind_field)
                if cursor_kind and isinstance(cursor_kind.default, str):
                    subclasses[cursor_kind.default] = cursor
                elif cursor_subclasses := cursor.__subclasses__():
                    next_cursors.extend(cursor_subclasses)
                elif (cursor_str := str(cursor)) not in _ALREADY_WARNED:
                    _ALREADY_WARNED.append(cursor_str)
                    logger.warning(
                        "ModelUnion leaf subclass %s has no default '%s' field",
                        cursor_str,
                        kind_field,
                    )

            cursors = next_cursors

        return subclasses

    @classmethod
    def _find_subclass(cls, kind: str) -> type[Self] | None:
        return cls._subclasses().get(kind)

    @classmethod
    def _from_dict(cls, obj: dict[str, Any]) -> Self:
        if kind := cls._find_subclass(obj.get("kind", "")):
            return kind.model_validate(obj)
        else:
            raise ValueError(f"unknown {cls} '{kind}'")

    @model_validator(mode="wrap")
    @classmethod
    def _validate_after(
        cls,
        value: Any,
        handler: ModelWrapValidatorHandler[Self],
    ) -> Any:
        if isinstance(value, dict):
            parsed = cls._from_dict(value) if cls.__subclasses__() else handler(value)
            parsed._validate_extra()  # noqa: SLF001
            return parsed
        else:
            return handler(value)

    def _validate_extra(self) -> None:
        """
        Perform extra validation on the parsed variant and, since the value has
        not been frozen yet, allows to populate `PrivateAttr`.
        """


class ModelUnionMut(BaseModel):
    @classmethod
    def discriminated_union(cls) -> Annotated:
        subclasses = [
            subclass
            for _, subclass in sorted(cls._subclasses().items(), key=lambda x: x[0])
        ]
        return Annotated[Union[*subclasses], Field(discriminator=cls._kind_field())]

    @classmethod
    def _kind_field(cls) -> str:
        return "kind"

    @classmethod
    def _subclasses(cls) -> dict[str, type[Self]]:
        kind_field = cls._kind_field()
        subclasses: dict[str, type[Self]] = {}
        cursors: list[type[Self]] = [cls]

        while cursors:
            next_cursors: list[type[Self]] = []
            for cursor in cursors:
                cursor_kind = cursor.model_fields.get(kind_field)
                if cursor_kind and isinstance(cursor_kind.default, str):
                    subclasses[cursor_kind.default] = cursor
                elif cursor_subclasses := cursor.__subclasses__():
                    next_cursors.extend(cursor_subclasses)
                elif (cursor_str := str(cursor)) not in _ALREADY_WARNED:
                    _ALREADY_WARNED.append(cursor_str)
                    logger.warning(
                        "ModelUnion leaf subclass %s has no default '%s' field",
                        cursor_str,
                        kind_field,
                    )

            cursors = next_cursors

        return subclasses

    @classmethod
    def _find_subclass(cls, kind: str) -> type[Self] | None:
        return cls._subclasses().get(kind)

    @classmethod
    def _from_dict(cls, obj: dict[str, Any]) -> Self:
        if kind := cls._find_subclass(obj.get("kind", "")):
            return kind.model_validate(obj)
        else:
            raise ValueError(f"unknown {cls} '{kind}'")

    @model_validator(mode="wrap")
    @classmethod
    def _validate_after(
        cls,
        value: Any,
        handler: ModelWrapValidatorHandler[Self],
    ) -> Any:
        if isinstance(value, dict):
            parsed = cls._from_dict(value) if cls.__subclasses__() else handler(value)
            parsed._validate_extra()  # noqa: SLF001
            return parsed
        else:
            return handler(value)

    def _validate_extra(self) -> None:
        """
        Perform extra validation on the parsed variant and, since the value has
        not been frozen yet, allows to populate `PrivateAttr`.
        """
