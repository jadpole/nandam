from pydantic import (
    BaseModel,
    Field,
    model_validator,
    ModelWrapValidatorHandler,
    PrivateAttr,
)
from typing import Annotated, Any, Literal, Self

from base.core.strings import ValidatedStr, normalize_str
from base.core.unique_id import unique_id_from_str
from base.core.values import as_json_canonical
from base.strings.resource import KnowledgeUri, ResourceUri

NUM_CHARS_RELATION_ID = 32
REGEX_RELATION_ID = r"[a-z]+-[a-z0-9]{32}"


class RelationId(ValidatedStr):
    """
    The unique identifier of a relation, which is constructed deterministically
    from its definition and is used by Knowledge to retrieve it.
    """

    def relation_type(self) -> str:
        return self.split("-")[0]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_RELATION_ID


class Relation(BaseModel, frozen=True):
    """
    NOTE: Never instantiated directly, but instead, parsing returns a subclass.
    Therefore, all subclasses MUST define `type: Literal` with a default value,
    which is used to instantiate the correct subclass.
    """

    type: str
    _cache_relation_id: RelationId | None = PrivateAttr(default=None)

    @staticmethod
    def from_dict(obj: dict[str, Any]) -> "Relation":
        relation_type = obj.get("type")
        for subclass in Relation.__subclasses__():
            if subclass.model_fields["type"].default == relation_type:
                return subclass.model_validate(obj)

        raise ValueError(f"unknown relation '{relation_type}'")

    @model_validator(mode="wrap")
    @classmethod
    def _validate_after(
        cls,
        value: Any,
        handler: ModelWrapValidatorHandler[Self],
    ) -> Any:
        """
        Decode into the correct Relation subclass based on 'type' and cache the
        unique ID in a private attribute for fast lookup.
        """
        if cls is Relation and isinstance(value, dict):
            rel = Relation.from_dict(value)
            return rel.model_copy(update={"_cache_relation_id": rel.unique_id()})
        else:
            return handler(value)

    def unique_id(self) -> RelationId:
        if self._cache_relation_id:
            return self._cache_relation_id

        relation_hash = unique_id_from_str(
            as_json_canonical(self),
            num_chars=NUM_CHARS_RELATION_ID,
            salt="knowledge-relation",
        )
        return RelationId(f"{self.type}-{relation_hash}")

    def get_nodes(self) -> list[ResourceUri]:
        return [self.get_source(), *self.get_targets()]

    def get_source(self) -> ResourceUri:
        raise NotImplementedError("Subclasses must implement Relation.source")

    def get_targets(self) -> list[ResourceUri]:
        raise NotImplementedError("Subclasses must implement Relation.targets")


##
## Variants
##


class RelationEmbed(Relation, frozen=True):
    type: Literal["embed"] = (
        "embed"  # pyright: ignore[reportIncompatibleVariableOverride]
    )
    source: KnowledgeUri
    target: KnowledgeUri

    def get_source(self) -> ResourceUri:
        return self.source.resource_uri()

    def get_targets(self) -> list[ResourceUri]:
        return [self.target.resource_uri()]


class RelationLink(Relation, frozen=True):
    type: Literal["link"] = (
        "link"  # pyright: ignore[reportIncompatibleVariableOverride]
    )
    source: KnowledgeUri
    target: KnowledgeUri

    def get_source(self) -> ResourceUri:
        return self.source.resource_uri()

    def get_targets(self) -> list[ResourceUri]:
        return [self.target.resource_uri()]


class RelationMisc(Relation, frozen=True):
    type: Literal["misc"] = (
        "misc"  # pyright: ignore[reportIncompatibleVariableOverride]
    )
    kind: str
    source: ResourceUri
    target: ResourceUri

    @staticmethod
    def new(
        kind: str,
        source: ResourceUri,
        target: ResourceUri,
    ) -> "RelationMisc":
        return RelationMisc(
            kind=(
                normalize_str(kind.lower().replace(" ", "_"), allowed_special_chars="_")
            ),
            source=source,
            target=target,
        )

    def get_source(self) -> ResourceUri:
        return self.source

    def get_targets(self) -> list[ResourceUri]:
        return [self.target]


class RelationParent(Relation, frozen=True):
    type: Literal["parent"] = (
        "parent"  # pyright: ignore[reportIncompatibleVariableOverride]
    )
    parent: ResourceUri
    child: ResourceUri

    def get_source(self) -> ResourceUri:
        return self.parent

    def get_targets(self) -> list[ResourceUri]:
        return [self.child]


AnyRelation = RelationEmbed | RelationLink | RelationMisc | RelationParent
AnyRelation_ = Annotated[AnyRelation, Field(discriminator="type")]
