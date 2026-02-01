# Knowledge Service

The Knowledge service manages resources: their metadata, content, and relationships. It provides a unified interface for loading resources from various sources (Confluence, SharePoint, Jira, etc.) and caching their processed representations.

## Core Concepts

### Resource
A resource is anything with a unique URI that agents can reference: documents, pages, issues, folders, etc. Each resource has:

- **URI**: Unique identifier (`ndk://realm/subrealm/path`)
- **Attributes**: Name, description, MIME type, timestamps, revision tags
- **Aliases**: External URIs that resolve to this resource
- **Affordances**: Different perspectives on the resource
- **Labels**: LLM-generated metadata, e.g., descriptions or tags
- **Relations**: Links to other resources

### Realm
The "realm" prefix of a URI identifies which connector handles the resource:

| Realm | Connector | Example |
|-------|-----------|---------|
| `confluence` | Atlassian Confluence | Pages, spaces |
| `github` | GitHub | Repositories, files |
| `jira` | Atlassian Jira | Issues, projects |
| `sharepoint` | Microsoft SharePoint/OneDrive | Documents, folders |
| `testrail` | TestRail | Test cases, runs |
| `tableau` | Tableau | Dashboards, views |
| `www` | Public web | Web pages |

## Query API

The primary endpoint accepts a batch of actions:

```
POST /v1/query
```

Request:
```json
{
  "settings": { ... },
  "actions": [
    { "method": "resources/load", "uri": "ndk://...", ... },
    { "method": "resources/observe", "uri": "ndk://.../$body" },
    ...
  ]
}
```

Response:
```json
{
  "resources": [ ... ],
  "observations": [ ... ]
}
```

### Actions

#### resources/load
Load resource metadata. Optionally expand related resources and observe affordances.

```json
{
  "method": "resources/load",
  "uri": "ndk://sharepoint/site/doc.docx",
  "load_mode": "auto",
  "expand_depth": 1,
  "observe": ["$body"]
}
```

- `load_mode`: `"auto"` (refresh if stale), `"force"` (always refresh), `"none"` (cache only)
- `expand_depth`: How many levels of related resources to include
- `observe`: Which affordances to observe

#### resources/observe
Load the content of a specific observable.

```json
{
  "method": "resources/observe",
  "uri": "ndk://sharepoint/site/doc.docx/$chunk/01"
}
```

#### resources/attachment
Upload content to be associated with a resource (used when connectors cannot read content directly).

```json
{
  "method": "resources/attachment",
  "uri": "ndk://local/uploads/file.pdf",
  "attachment": {
    "type": "blob",
    "mime_type": "application/pdf",
    "blob": "base64..."
  }
}
```

## Storage Model

Knowledge persists data in object storage with this structure:

```
v1/
├── resource/           # Resource metadata (ResourceHistory)
│   └── realm/subrealm/path.yml
├── observed/           # Observation bundles
│   └── realm+subrealm+path/
│       ├── body.yml
│       ├── collection.yml
│       └── file+figures+image.png.yml
├── alias/              # External URI → Resource mappings
│   └── {hash}.yml
└── relation/
    ├── defs/           # Relation definitions
    │   └── {relation_id}.yml
    └── refs/           # Resource → Relation index
        └── realm+subrealm+path/
            └── {relation_id}.txt
```

### ResourceHistory
Each resource's metadata includes its current state plus a history of deltas, enabling:

- Detecting whether content has changed (via `revision_data`/`revision_meta`)
- Tracking which affordances are available
- Knowing when fields were last updated

### Bundles
Bundles store the cached observations for an affordance:

- `BundleBody`: Markdown content, sections, chunks, media
- `BundleCollection`: List of child resource URIs
- `BundleFile`: Download URL and metadata

### Aliases
External URIs (web URLs) are hashed to create deterministic lookup paths. When a resource has multiple external URLs, each maps to the same Knowledge URI.

### Relations
Relations are stored bidirectionally:
1. The relation definition (`defs/{id}.yml`)
2. References from each node (`refs/{resource_path}/{id}.txt`)

This allows efficient lookup of all relations for a given resource.

## Connectors

Connectors implement the `observe` method to fetch content from external sources:

```python
async def observe(
    self,
    context: KnowledgeContext,
    uri: ResourceUri,
    affordance: Observable,
    cached: ResourceView | None,
) -> ObserveResult | None:
    ...
```

Each connector:
1. Fetches content from the external source
2. Returns either a `Fragment` (raw content) or a pre-processed `Bundle`
3. May include relations to other resources

The Knowledge service then:
1. Processes fragments through the ingestion pipeline
2. Chunks large documents
3. Resizes/optimizes images
4. Extracts links and creates relations
5. Generates LLM descriptions (via inference service)
6. Caches the result

## Caching Strategy

Resources are cached based on:

- **revision_data**: Content revision (e.g., SharePoint `cTag`)
- **revision_meta**: Metadata revision (e.g., SharePoint `eTag`)
- **updated_at**: Last modification timestamp (fallback)

When `load_mode="auto"`:
1. If cached and revisions match → return cached
2. If cached but stale → refresh from source
3. If not cached → fetch from source

When `load_mode="force"`:
- Always fetch from source, ignoring cache

When `load_mode="none"`:
- Only return cached data, never fetch
