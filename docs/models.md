# Modeling Patterns

Nandam uses several patterns for type-safe, self-documenting models that serialize cleanly to JSON/YAML and generate accurate OpenAPI schemas.

## Validated Strings

Many domain concepts are represented as strings with specific formats. Rather than using raw `str`, Nandam uses validated string types that:

1. Enforce format constraints at parse time
2. Generate regex patterns for OpenAPI schemas
3. Provide examples for documentation

### ValidatedStr

A subclass of `str` that validates against a regex pattern:

```python
class Realm(ValidatedStr):
    @classmethod
    def _schema_regex(cls) -> str:
        return r"[a-z][a-z0-9]+(?:-[a-z0-9]+)*"

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return ["jira", "sharepoint", "www"]
```

Usage:
```python
realm = Realm.decode("sharepoint")  # OK
realm = Realm.decode("INVALID!")    # Raises ValueError
realm = Realm.try_decode("maybe")   # Returns None on failure
```

Key methods:
- `decode(v)`: Parse and validate, raising on error
- `try_decode(v)`: Parse and validate, returning `None` on error
- `_schema_regex()`: Regex for validation and OpenAPI
- `_schema_examples()`: Example values for documentation

### StructStr

A Pydantic `BaseModel` that serializes as a string. Useful for structured data that should appear as a single string in JSON:

```python
class WebUrl(StructStr, frozen=True):
    domain: str
    port: int
    path: str
    query: list[tuple[str, str]]

    @classmethod
    def _parse(cls, v: str) -> Self:
        # Parse URL string into components
        ...

    def _serialize(self) -> str:
        # Rebuild URL string from components
        ...
```

In JSON, a `WebUrl` appears as `"https://example.com/path"`, but internally it's a structured object with accessible fields.

## Discriminated Unions

### ModelUnion

A base class for discriminated unions where each variant has a `kind` field:

```python
class Relation(ModelUnion, frozen=True):
    kind: str  # Discriminator

class RelationLink(Relation, frozen=True):
    kind: Literal["link"] = "link"
    source: KnowledgeUri
    target: KnowledgeUri

class RelationParent(Relation, frozen=True):
    kind: Literal["parent"] = "parent"
    parent: ResourceUri
    child: ResourceUri
```

When parsing JSON with `{"kind": "link", ...}`, Pydantic automatically instantiates `RelationLink`.

Key methods:
- `union_subclasses()`: List all concrete variants
- `union_find_subclass(kind)`: Find variant by kind value
- `union_from_dict(obj)`: Parse dict into correct variant

### Field-based Discriminators

For simple unions, use Pydantic's `Field(discriminator="...")`:

```python
AnyRelation_ = Annotated[
    RelationLink | RelationParent | ...,
    Field(discriminator="kind")
]
```

## Content Models

### ContentText

Represents parsed textual content (typically Markdown) as a sequence of parts:

```python
class ContentText(BaseModel, frozen=True):
    parts: list[TextPart]  # PartText, PartCode, PartLink, PartHeading, ...
    plain: str | None      # Original unparsed text (when available)
```

Part types:
- `PartText`: Plain text with separator hints
- `PartCode`: Fenced code blocks
- `PartLink`: References (markdown links, embeds, citations)
- `PartHeading`: Section headings
- `PartPageNumber`: Page markers (from PDFs)

Parsing modes:
- `markdown`: Full Markdown parsing (default)
- `data`: Structured data like JSON/CSV where references may appear but not Markdown formatting
- `plain`: Raw text without any parsing

```python
# Parse Markdown with link extraction
content = ContentText.parse("See [doc](ndk://...)")

# Create plain text
content = ContentText.new_plain("Raw text here")

# Access extracted links
links = content.dep_links()    # Non-embed links
embeds = content.dep_embeds()  # Embedded resources
```

### ContentBlob

Binary content (images, audio, etc.) with an optional text placeholder:

```python
class ContentBlob(BaseModel, frozen=True):
    uri: Reference
    placeholder: str | None  # Alt text or description
    mime_type: MimeType
    blob: str               # Base64 or URL
```

When LLMs cannot process the binary directly, `placeholder` provides a text alternative.

## Serialization Helpers

### Excluding None/Empty Values

Use `WrapSerializer` to omit fields with `None` or empty values:

```python
ResourceAttrs_ = Annotated[ResourceAttrs, WrapSerializer(wrap_exclude_none)]
AffordanceInfo_ = Annotated[AffordanceInfo, WrapSerializer(wrap_exclude_none_or_empty)]
```

This keeps JSON output clean:
```json
{"name": "doc.pdf", "mime_type": "application/pdf"}
// instead of
{"name": "doc.pdf", "mime_type": "application/pdf", "description": null, "created_at": null, ...}
```

## Design Principles

1. **Parse, don't validate**: Convert strings into structured types at system boundaries
2. **Immutable by default**: Use `frozen=True` for all models
3. **Schema-first**: Types generate their own OpenAPI schemas via `_schema_regex()` and `_schema_examples()`
4. **String interop**: Complex types like URIs serialize as strings for clean JSON
5. **Discriminated unions**: Use `kind` field for polymorphic types
