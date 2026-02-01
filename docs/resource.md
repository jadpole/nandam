# Resources and Affordances

The resource model is the foundation of Nandam's knowledge system. It provides a unified way to reference, load, and observe content from diverse sources.

## URI Structure

Every resource has a Knowledge URI with this structure:

```
ndk://{realm}/{subrealm}/{path...}[/{suffix}]
```

| Component | Description | Example |
|-----------|-------------|---------|
| `realm` | Connector that handles this resource | `sharepoint`, `jira`, `www` |
| `subrealm` | Namespace within the realm | Site name, project key |
| `path` | Unique identifier within subrealm | File path, issue number |
| `suffix` | Optional affordance/observable | `$body`, `$chunk/01` |

### URI Types

- **ResourceUri**: `ndk://realm/subrealm/path` — identifies the resource
- **AffordanceUri**: `ndk://realm/subrealm/path/$body` — identifies an affordance
- **ObservableUri**: `ndk://realm/subrealm/path/$chunk/01` — identifies content within an affordance

## Affordances

An "affordance" is a perspective on a resource. The same document can be viewed in different ways depending on the task:

| Affordance | Purpose | Observable Children |
|------------|---------|---------------------|
| `$body` | LLM-readable content (Markdown) | `$chunk/*`, `$media/*` |
| `$collection` | List of child resources | — |
| `$file` | Raw file download | `$file/{path}` |
| `$plain` | Raw text content (JSON, CSV) | — |

### $body

The primary affordance for documents. Content is:
1. Converted to Markdown
2. Chunked for context window management
3. Images extracted as `$media` observables

The `$body` observation provides a "table of contents" showing available chunks and their descriptions.

### $collection

For containers (folders, spaces, projects). Returns a list of child `ResourceUri` values.

### $file

For binary files. Returns a download URL (or inline base64 for small files). Used by tools like Code Interpreter.

### $plain

For structured text (JSON, CSV, logs). Content is wrapped in a code block, preserving exact formatting.

## Observables

Observables are addressable content within an affordance:

### $chunk

Numbered sections of a `$body`:
- `$chunk/00` — First chunk
- `$chunk/01` — Second chunk
- `$chunk/01/00` — Nested chunk (subsection)

Each chunk fits within an LLM context window (~4K tokens by default).

### $media

Embedded images/media within a `$body`:
- `$media/figure.png`
- `$media/figures/diagram.svg`

Media can be rendered as:
- Inline images (when supported by LLM)
- Placeholder text (description or alt text)

## Observations

An "observation" is the actual content of an observable. Each observation type corresponds to an observable suffix:

| Observable | Observation Class | Content |
|------------|-------------------|---------|
| `$body` | `ObsBody` | Table of contents or single-chunk content |
| `$chunk/*` | `ObsChunk` | Markdown text |
| `$media/*` | `ObsMedia` | Image blob + placeholder |
| `$collection` | `ObsCollection` | List of `ResourceUri` |
| `$file/*` | `ObsFile` | Download URL |
| `$plain` | `ObsPlain` | Raw text + MIME type |

### Rendering

Observations can be rendered in two modes:

- **Info mode** (`render_info()`): Brief placeholder showing URI and description
- **Body mode** (`render_body()`): Full content for LLM consumption

```xml
<!-- Info mode -->
<document uri="ndk://..." description="Quarterly report" />

<!-- Body mode -->
<document uri="ndk://...">
Full markdown content here...
</document>
```

## Resource Metadata

Each resource has attributes:

```python
class ResourceAttrs(BaseModel, frozen=True):
    name: str                     # Human-readable title
    mime_type: MimeType | None    # Original format
    description: str | None       # What the resource contains
    citation_url: WebUrl | None   # Link for humans
    created_at: datetime | None
    updated_at: datetime | None
    revision_data: str | None     # Content revision tag
    revision_meta: str | None     # Metadata revision tag
```

The `revision_*` fields enable efficient cache invalidation:
- If `revision_data` changes → content must be re-fetched
- If `revision_meta` changes → metadata must be updated
- If neither is available → fall back to `updated_at`

## Relations

Resources can be connected through relations:

| Relation | Meaning |
|----------|---------|
| `RelationParent` | Resource is a container for another |
| `RelationLink` | Resource references another in its content |
| `RelationEmbed` | Resource embeds another's content |
| `RelationMisc` | Custom relationship with subkind |

Relations are bidirectional: querying either endpoint finds the relation.

```python
class RelationParent(Relation, frozen=True):
    kind: Literal["parent"] = "parent"
    parent: ResourceUri
    child: ResourceUri
```

## Labels

Labels are extracted metadata stored per-observable:

```python
class ResourceLabel(BaseModel):
    name: LabelName        # e.g., "description"
    target: Observable     # e.g., $body, $chunk/01
    value: Any
```

Common labels:
- `description`: LLM-generated summary of the content
- `placeholder`: LLM-generated drop-in replacement for unsupported media
- Custom labels defined by connectors

Labels allow efficient access to metadata without loading full content.

## Bundles

A "bundle" is the cached representation of an affordance's observations:

- **BundleBody**: All `$body`, `$chunk/*`, `$media/*` for a document
- **BundleCollection**: The `$collection` observation
- **BundleFile**: A single `$file/*` observation

Bundles are stored in object storage and loaded atomically.

## External URIs and Aliases

External URIs (web URLs) can be resolved to Knowledge URIs:

```
https://mycompany.sharepoint.com/sites/Team/doc.docx
    → ndk://sharepoint/Team/doc.docx
```

Each resource tracks its aliases, enabling:
- Deduplication (same file via different URLs)
- Citation URL generation
- External link resolution within documents
