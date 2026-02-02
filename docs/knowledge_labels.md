# Knowledge Labels

Labels are LLM-generated metadata attached to observations. They enable tools and
agents to filter and discover relevant resources without reading full documents.


## Overview

The label generation system:

1. Takes `BundleBody` resources containing observations (`$body`, `$chunk`, `$media`)
2. Applies label definitions from configuration (or defaults)
3. Generates labels via LLM inference with structured JSON output
4. Returns `ResourceLabel` objects with the generated metadata


## Observations vs Embed Targets

A key architectural distinction:

| Concept | Purpose | Example |
|---------|---------|---------|
| **Observations** | What we generate labels for | `$body`, `$chunk/00`, `$media/fig.png` |
| **Embed Targets** | What we embed in the prompt | `$body` OR `$chunk/*` (not both) |

For multi-chunk documents, the body is a container that expands to its chunks.
To avoid content duplication in the prompt:

- **Single-chunk documents**: Embed `$body` directly
- **Multi-chunk documents**: Embed `$chunk` URIs (body would expand to the same content)

Labels are still generated for **all** observation types (body, chunks, media),
regardless of what is embedded. The LLM sees the content once and generates
labels for all requested URIs.


## Supported Observations

| Type | Description |
|------|-------------|
| `$body` | The main document body (text or blob) |
| `$chunk` | A section of a large document (e.g., `$chunk/00`) |
| `$media` | Embedded media files (images, diagrams) |


## Prompt Rendering

When rendering embed targets in the prompt:

- **Body** (`$body`): Embedded directly
- **Chunks** (`$chunk/*`): Wrapped in `<document uri=".../$body">` to provide parent context
- **Media**: Embedded as inline images (for supported formats) or placeholders


## Token-Based Grouping

Embed targets are grouped into batches that fit within the LLM context window:

- **Threshold**: ~80,000 tokens per group
- **Batching**: Greedy algorithm filling groups sequentially
- **Scope**: All labels for resources in a group are requested together

This reduces API calls while ensuring related observations share context.


## Caching

Labels are cached to avoid redundant generation:

- Previously generated labels are passed as `cached`
- The system skips `(label_name, observation_uri)` pairs already cached
- Enables incremental generation when new observations are added


## Configuration

Labels are configured via `labels.yml` with the following schema:

```yaml
labels:
  - info:
      name: description
      forall: [body, chunk, media]
      prompt: |
        Generate a concise description...
    filters:
      default: allow
      allowlist: []
```

| Property | Description |
|----------|-------------|
| `name` | Unique label identifier |
| `forall` | Observation types to target: `body`, `chunk`, `media` |
| `prompt` | Instructions for the LLM |
| `filters` | Optional resource URI filters |


## Default Labels

When no configuration is present, two labels are generated:

### `description`
- **Targets**: `$body`, `$chunk`, `$media`
- **Purpose**: A 2-3 sentence summary helping users decide whether to consult the source

### `placeholder`
- **Targets**: `$media` only
- **Purpose**: Textual representation of media for agents that cannot view images


## Structured Output

Labels are generated via structured JSON output:

- Property names follow the pattern `{label_name}_{normalized_uri}`
- All properties are nullable (returns `null` when inference fails)
- URI normalization removes special characters for valid JSON keys

Example property name:
```
ndk://public/arxiv/paper/$chunk/00
  → description_publicarxivpaperchunk00
```


## Media Support

The system supports inline images for multimodal LLMs:

- **Formats**: PNG, JPEG, WebP, HEIC, HEIF
- **Limit**: 20 media items per request
- **Fallback**: Unsupported formats use placeholder text
