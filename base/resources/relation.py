from pydantic import Field, SerializeAsAny, PrivateAttr
from typing import Annotated, Literal

from base.core.strings import ValidatedStr, normalize_str
from base.core.unions import ModelUnion
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

    def relation_kind(self) -> str:
        return self.split("-")[0]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_RELATION_ID


class Relation(ModelUnion, frozen=True):
    # NOTE: Never instantiated directly, but instead, parsing returns a subclass.
    # Therefore, all subclasses MUST define `kind: Literal` with a default value,
    # which is used to instantiate the correct subclass.

    _cache_relation_id: RelationId | None = PrivateAttr(default=None)

    def _validate_extra(self) -> None:
        self._cache_relation_id = (  # pyright: ignore[reportAttributeAccessIssue]
            self.unique_id()
        )

    def unique_id(self) -> RelationId:
        if self._cache_relation_id:
            return self._cache_relation_id

        relation_kind: str = self.kind  # type: ignore
        relation_hash = unique_id_from_str(
            as_json_canonical(self),
            num_chars=NUM_CHARS_RELATION_ID,
            salt="knowledge-relation",
        )
        return RelationId(f"{relation_kind}-{relation_hash}")

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
    kind: Literal["embed"] = "embed"
    source: KnowledgeUri
    target: KnowledgeUri

    def get_source(self) -> ResourceUri:
        return self.source.resource_uri()

    def get_targets(self) -> list[ResourceUri]:
        return [self.target.resource_uri()]


class RelationLink(Relation, frozen=True):
    kind: Literal["link"] = "link"
    source: KnowledgeUri
    target: KnowledgeUri

    def get_source(self) -> ResourceUri:
        return self.source.resource_uri()

    def get_targets(self) -> list[ResourceUri]:
        return [self.target.resource_uri()]


class RelationMisc(Relation, frozen=True):
    kind: Literal["misc"] = "misc"
    subkind: str
    source: ResourceUri
    target: ResourceUri

    @staticmethod
    def new(
        subkind: str,
        source: ResourceUri,
        target: ResourceUri,
    ) -> RelationMisc:
        return RelationMisc(
            subkind=normalize_str(
                subkind.lower().replace(" ", "_"),
                allowed_special_chars="_",
                remove_duplicate_chars="_",
                remove_prefix_chars="_",
                remove_suffix_chars="_",
            ),
            source=source,
            target=target,
        )

    def get_source(self) -> ResourceUri:
        return self.source

    def get_targets(self) -> list[ResourceUri]:
        return [self.target]


class RelationParent(Relation, frozen=True):
    kind: Literal["parent"] = "parent"
    parent: ResourceUri
    child: ResourceUri

    def get_source(self) -> ResourceUri:
        return self.parent

    def get_targets(self) -> list[ResourceUri]:
        return [self.child]


Relation_ = SerializeAsAny[Relation]

AnyRelation = RelationEmbed | RelationLink | RelationMisc | RelationParent
AnyRelation_ = Annotated[AnyRelation, Field(discriminator="kind")]
