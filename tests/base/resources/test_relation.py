from pydantic import BaseModel, TypeAdapter

from base.core.values import as_json
from base.resources.relation import (
    Relation,
    Relation_,
    RelationEmbed,
    RelationLink,
    RelationMisc,
    RelationParent,
)
from base.strings.resource import KnowledgeUri, ResourceUri


class DemoRelation(BaseModel):
    relation: Relation_


EXPECTED_SCHEMA_RELATION = {
    "properties": {},
    "title": "Relation",
    "type": "object",
}


def test_relation_jsonschema() -> None:
    schema = TypeAdapter(Relation).json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema == EXPECTED_SCHEMA_RELATION


def test_relation_field_jsonschema() -> None:
    schema = DemoRelation.model_json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    assert schema == {
        "$defs": {"Relation": EXPECTED_SCHEMA_RELATION},
        "properties": {"relation": {"$ref": "#/$defs/Relation"}},
        "required": ["relation"],
        "title": "DemoRelation",
        "type": "object",
    }


def test_relation_embed_jsonschema() -> None:
    schema = TypeAdapter(RelationEmbed).json_schema()
    print(f"<schema>\n{as_json(schema, indent=2)}\n</schema>")
    del schema["$defs"]
    assert schema == {
        "type": "object",
        "properties": {
            "kind": {
                "const": "embed",
                "default": "embed",
                "title": "Kind",
                "type": "string",
            },
            "source": {"$ref": "#/$defs/KnowledgeUri"},
            "target": {"$ref": "#/$defs/KnowledgeUri"},
        },
        "required": ["source", "target"],
        "title": "RelationEmbed",
    }


def _run_relation_validate(rel_type: type[Relation], relation: Relation) -> None:
    # Relation constructors call `_validate_extra`.
    assert relation._cache_relation_id

    adapter = TypeAdapter(rel_type)
    relation_dict = relation.model_dump()
    relation_json = relation.model_dump_json()
    print(f"<relation_json>\n{relation_json}\n</relation_json>")
    assert rel_type.model_validate(relation_dict) == relation
    assert rel_type.model_validate_json(relation_json) == relation
    assert Relation.model_validate(relation_dict) == relation
    assert Relation.model_validate_json(relation_json) == relation
    assert adapter.validate_python(relation_dict) == relation
    assert adapter.validate_json(relation_json) == relation

    # Relation validation calls `_validate_extra`.
    assert (
        rel_type.model_validate(relation_dict)._cache_relation_id
        == relation._cache_relation_id
    )
    assert (
        Relation.model_validate(relation_dict)._cache_relation_id
        == relation._cache_relation_id
    )
    assert (
        adapter.validate_json(relation_json)._cache_relation_id
        == relation._cache_relation_id
    )

    wrapped = DemoRelation(relation=relation)
    rewrapped = DemoRelation.model_validate_json(wrapped.model_dump_json())
    assert DemoRelation.model_validate(wrapped.model_dump()) == wrapped
    assert rewrapped == wrapped
    assert wrapped.model_dump_json() == rewrapped.model_dump_json()

    # Relation validation in fields calls `_validate_extra`.
    assert rewrapped.relation._cache_relation_id


def test_relation_embed_validate_ok() -> None:
    relation = RelationEmbed(
        source=KnowledgeUri.decode("ndk://jira/issue/PROJ-123"),
        target=KnowledgeUri.decode("ndk://jira/issue/PROJ-456"),
    )
    _run_relation_validate(RelationEmbed, relation)


def test_relation_link_validate_ok() -> None:
    relation = RelationLink(
        source=KnowledgeUri.decode("ndk://jira/issue/PROJ-123"),
        target=KnowledgeUri.decode("ndk://jira/issue/PROJ-456"),
    )
    _run_relation_validate(RelationLink, relation)


def test_relation_misc_validate_ok() -> None:
    relation = RelationMisc(
        subkind="duplicate",
        source=ResourceUri.decode("ndk://jira/issue/PROJ-123"),
        target=ResourceUri.decode("ndk://jira/issue/PROJ-456"),
    )
    _run_relation_validate(RelationMisc, relation)


def test_relation_parent_validate_ok() -> None:
    relation = RelationParent(
        parent=ResourceUri.decode("ndk://jira/issue/PROJ-123"),
        child=ResourceUri.decode("ndk://jira/issue/PROJ-456"),
    )
    _run_relation_validate(RelationParent, relation)
