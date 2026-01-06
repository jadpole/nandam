import logging

from pydantic import BaseModel, Field, model_validator, ModelWrapValidatorHandler
from typing import Annotated, Any, Self, Union

logger = logging.getLogger(__name__)

ALREADY_WARNED: list[str] = []


class ModelUnion(BaseModel, frozen=True):
    kind: str

    @classmethod
    def union_discriminated(cls) -> Annotated:
        return Annotated[Union[*cls.union_subclasses()], Field(discriminator="kind")]

    @classmethod
    def union_subclasses(cls) -> list[type[Self]]:
        subclasses: list[type[Self]] = []
        cursors: list[type[Self]] = [cls]

        while cursors:
            next_cursors: list[type[Self]] = []
            for cursor in cursors:
                cursor_subclasses = cursor.__subclasses__()
                if cursor_subclasses:
                    next_cursors.extend(cursor_subclasses)
                elif (
                    (kind_field := cursor.model_fields.get("kind"))
                    and kind_field.default
                    and kind_field.default not in subclasses
                ):
                    subclasses.append(cursor)
                elif (cursor_str := str(cursor)) not in ALREADY_WARNED:
                    ALREADY_WARNED.append(cursor_str)
                    logger.warning(
                        "ModelUnion subclass %s has no default 'kind' field", cursor_str
                    )

            cursors = next_cursors

        return subclasses

    @classmethod
    def union_find_subclass(cls, kind: str) -> "type[Self] | None":
        return next(
            (
                subclass
                for subclass in cls.union_subclasses()
                if subclass.model_fields["kind"].default == kind
            ),
            None,
        )

    @classmethod
    def union_from_dict(cls, obj: dict[str, Any]) -> Self:
        if kind := cls.union_find_subclass(obj.get("kind", "")):
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
        return (
            cls.union_from_dict(value)
            if isinstance(value, dict) and cls.__subclasses__()
            else handler(value)
        )
