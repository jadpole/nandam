# Connectors

Connectors are the bridge between Knowledge and external data sources. Each connector implements a standard interface to resolve references, fetch metadata, and observe content from a specific platform (Confluence, Jira, SharePoint, etc.).

## Architecture

```
Reference → Connector.locator() → Locator
Locator   → Connector.resolve() → ResolveResult (metadata, expired affordances)
Locator   → Connector.observe() → ObserveResult (content bundle)
```

The Knowledge service maintains a list of connectors and iterates through them (in registration order) to find one that handles a given reference.

## The Connector Interface

```python
@dataclass(kw_only=True)
class Connector:
    context: KnowledgeContext
    realm: Realm

    async def locator(self, reference: RootReference) -> Locator | None: ...
    async def resolve(self, locator: Locator, cached: ResourceView | None) -> ResolveResult: ...
    async def observe(self, locator: Locator, observable: Observable, resolved: MetadataDelta) -> ObserveResult: ...
```

### locator()

Converts a reference (web URL or resource URI) into a `Locator`.

**Returns:**
- `Locator`: When this connector handles the reference
- `None`: When another connector should handle it
- Raises `UnavailableError`: When this connector should handle it, but the resource doesn't exist

**Responsibilities:**
- Parse web URLs to extract resource identifiers
- Validate that resource URIs match expected patterns
- NOT responsible for access validation (that's `resolve()`'s job)

### resolve()

Validates access and fetches metadata without loading content.

**Returns:** `ResolveResult` with:
- `metadata`: Updated resource attributes and available affordances
- `expired`: List of observables that need re-fetching
- `should_cache`: Whether to persist the metadata

**Responsibilities:**
- Verify the client can access the resource
- Return available affordances
- Detect content changes via revision tags
- Identify which cached observations are stale

### observe()

Fetches the actual content for an observable.

**Returns:** `ObserveResult` with:
- `bundle`: Content as `Fragment` or pre-processed `Bundle`
- `metadata`: Additional metadata discovered during observation
- `relations`: Links to other resources
- `should_cache`: Whether to persist the content
- `option_*`: Flags controlling ingestion behavior

## Locators

A `Locator` is a structured representation of a resource's location. Each connector defines its own locator types:

```python
class ConfluencePageLocator(Locator, frozen=True):
    kind: Literal["confluence_page"] = "confluence_page"
    realm: Realm
    domain: str
    space_key: FileName
    page_id: FileName

    def resource_uri(self) -> ResourceUri: ...
    def content_url(self) -> WebUrl: ...
    def citation_url(self) -> WebUrl: ...
```

**Required methods:**
- `resource_uri()`: Generate the canonical Knowledge URI
- `content_url()`: URL to fetch content from
- `citation_url()`: URL for human-readable citations

**Design principles:**
- Immutable (`frozen=True`)
- Discriminated by `kind` field
- Contains all information needed to fetch the resource

## Result Types

### ResolveResult

```python
class ResolveResult(BaseModel, frozen=True):
    metadata: MetadataDelta = Field(default_factory=MetadataDelta)
    expired: list[Observable] = Field(default_factory=list)
    should_cache: bool = False
```

- Return empty `metadata` when unchanged or when minimal resolution is sufficient
- Populate `expired` with observables whose cached content is stale
- Set `should_cache=True` for resources worth persisting

### ObserveResult

```python
class ObserveResult(BaseModel, frozen=True):
    bundle: AnyBundle | Fragment
    metadata: MetadataDelta = Field(default_factory=MetadataDelta)
    relations: list[Relation] = Field(default_factory=list)
    should_cache: bool = False
    option_fields: bool = False
    option_relations_link: bool = False
    option_relations_parent: bool = False
```

**Bundle types:**
- `Fragment`: Raw content (text + blobs) to be processed by ingestion
- `BundleBody`, `BundleCollection`, `BundleFile`: Pre-processed bundles

**Option flags:**
- `option_fields`: Generate LLM descriptions for chunks/media
- `option_relations_link`: Extract link relations from content
- `option_relations_parent`: Create parent relations from collections

## Authentication

Connectors obtain credentials through the context:

```python
# Basic auth (username:password)
authorization, is_public = self.context.basic_authorization(
    self.realm,
    public_username="CONFLUENCE_USERNAME",  # env var name
    public_password="CONFLUENCE_TOKEN",      # env var name
)

# Bearer token
authorization, is_public = self.context.bearer_authorization(
    self.realm,
    public_token="GITHUB_TOKEN",  # env var name
)
```

Credentials are resolved in order:
1. Per-request credentials from `KnowledgeSettings.creds`
2. Public credentials from environment variables

The `is_public` flag indicates whether public (shared) credentials were used.

## Implementation Patterns

### Connector Configuration

Connectors are often configured per-instance (e.g., one per Confluence domain):

```python
class ConfluenceConnectorConfig(BaseModel):
    kind: Literal["confluence"] = "confluence"
    realm: Realm
    domain: str
    public_token: str | None

    def instantiate(self, context: KnowledgeContext) -> ConfluenceConnector:
        return ConfluenceConnector(
            context=context,
            realm=self.realm,
            domain=self.domain,
            public_token=self.public_token,
        )
```

### Handle Pattern

For connectors that cache API responses within a request, use a "handle" object:

```python
@dataclass(kw_only=True)
class JiraConnector(Connector):
    domain: str
    _handle: JiraHandle | None = None

    async def _acquire_handle(self) -> JiraHandle:
        if self._handle is None:
            self._handle = JiraHandle(
                context=self.context,
                realm=self.realm,
                domain=self.domain,
                _authorization=self._get_authorization(),
                _cache_issues={},
            )
        return self._handle
```

### URL Pattern Matching

Use regex to identify URL patterns:

```python
REGEX_URL_ISSUE = r"browse/([A-Za-z]+-\d+)"

async def locator(self, reference: RootReference) -> Locator | None:
    if isinstance(reference, WebUrl):
        if reference.domain != self.domain:
            return None

        if match := re.fullmatch(REGEX_URL_ISSUE, reference.path):
            return JiraIssueLocator(...)

        raise UnavailableError.new()  # URL matches domain but not pattern
```

### Revision Detection

Use revision tags to detect content changes:

```python
async def resolve(self, locator: Locator, cached: ResourceView | None) -> ResolveResult:
    metadata = await self._fetch_metadata(locator)

    expired = (
        cached
        and metadata.revision_data
        and cached.metadata.revision_data != metadata.revision_data
    )

    return ResolveResult(
        metadata=metadata,
        expired=[AffBody.new()] if expired else [],
    )
```

## Connector Checklist

When implementing a new connector:

1. **Define locator types** for each resource variant
2. **Implement `locator()`** with URL pattern matching
3. **Implement `resolve()`** with access validation and metadata fetching
4. **Implement `observe()`** for each supported affordance
5. **Handle errors** by raising `UnavailableError` (not found/forbidden)
6. **Use `BadRequestError`** for unsupported observables
7. **Configure caching** via `should_cache` flags
8. **Extract relations** when useful for navigation
9. **Test** with `utils_connectors.py` test utilities

## Existing Connectors

| Connector | Realm | Resource Types |
|-----------|-------|----------------|
| `ConfluenceConnector` | `confluence` | Pages, blog posts |
| `GithubConnector` | `github` | Repos, files, issues |
| `JiraConnector` | `jira` | Issues, boards, filters, JQL searches |
| `MicrosoftConnector` | `sharepoint` | SharePoint/OneDrive files and folders |
| `PublicConnector` | Various | Public resources with known formats |
| `QATestrailConnector` | `testrail` | Test cases, runs |
| `TableauConnector` | `tableau` | Dashboards, views |
| `WebConnector` | `www` | Generic web pages (fallback) |

The `WebConnector` acts as a fallback for URLs not handled by other connectors.
