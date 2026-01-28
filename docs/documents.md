# Documents Service

The Documents service converts files from arbitrary formats into normalized representations that can be consumed by LLMs. It acts as a stateless transformer: given a URL or file, it returns structured text and extracted blobs.

## Architecture

The service follows a two-stage pipeline:

1. **Download**: Fetch content from a URL using a matching downloader
2. **Extract**: Convert the downloaded content into text using a matching extractor

```
URL → Downloader → Downloaded → Extractor → Extracted → DocumentsReadResponse
```

Both stages use a "first match" strategy: the service iterates through registered handlers and uses the first one that claims to support the input.

## Downloaders

Downloaders fetch content from URLs and return a `Downloaded` object containing the raw bytes, filename, MIME type, and response headers.

| Downloader | Purpose |
|------------|---------|
| `ConfluenceDownloader` | Handles Confluence pages and attachments via Atlassian API |
| `TableauDownloader` | Exports Tableau views and dashboards |
| `WebDownloader` | Generic HTTP(S) downloads (fallback) |

Downloaders are domain-specific: each `ConfluenceDownloader` or `TableauDownloader` instance is configured for a specific domain (e.g., `mycompany.atlassian.net`).

## Extractors

Extractors convert downloaded content into a normalized `Extracted` object containing text (typically Markdown), an optional name/title, and extracted blobs (images, etc.).

| Extractor | Formats |
|-----------|---------|
| `ArchiveExtractor` | ZIP, TAR, etc. |
| `ConversionExtractor` | Format conversions via external tools |
| `ExcelExtractor` | XLSX, XLS spreadsheets → CSV/Markdown |
| `HtmlPageExtractor` | HTML pages → Markdown |
| `ImageExtractor` | Images → placeholder + blob |
| `PandocExtractor` | DOCX, PPTX, LaTeX, etc. via Pandoc |
| `PdfExtractor` | PDF → Markdown + images |
| `PlainTextExtractor` | TXT, CSV, JSON, etc. |
| `TranscriptExtractor` | Audio/video transcripts (SRT, VTT) |
| `UnstructuredExtractor` | Fallback using Unstructured.io |

## Response Modes

The `DocumentsReadResponse.mode` field indicates how the text should be interpreted:

| Mode | Description |
|------|-------------|
| `plain` | Raw text without structure (logs, transcripts) |
| `data` | Structured data (CSV, JSON) where references may appear but not Markdown formatting |
| `markdown` | Full Markdown with headings, links, code blocks, images |

## Blobs

When a document contains embedded images or other binary content, extractors:

1. Generate a `FragmentUri` placeholder (e.g., `self://figures/image.png`)
2. Store the blob data as a base64-encoded `DataUri`
3. Reference the placeholder in the Markdown text using `![alt](self://figures/image.png)`

The Knowledge service later resolves these placeholders into absolute URIs.

## API

The service exposes three endpoints:

### POST /v1/download

Download from URL and extract content.

Request body (`DocumentsDownloadRequest`):
- `url`: The URL to download and extract
- `headers`: Additional HTTP headers for the download
- `original`: If `true`, skip post-processing (return raw content)
- `mime_type`: Override MIME type detection
- `doc`: PDF/document processing options
- `html`: HTML parsing options (selectors to include/exclude)
- `transcript`: Audio/video transcript options

### POST /v1/blob

Process a base64-encoded file.

Request body (`DocumentsBlobRequest`):
- `name`: Filename
- `mime_type`: MIME type of the content
- `blob`: Base64-encoded file content
- Same processing options as `/v1/download`

### POST /v1/upload

Process a multipart file upload. Options passed via headers (`X-Original`, `X-Mime-Type`, etc.).

### Response

All endpoints return `DocumentsReadResponse`:
- `name`: Inferred document title or filename
- `mime_type`: Original MIME type
- `headers`: Response headers from download (for metadata extraction)
- `mode`: How to interpret the text (`plain`, `data`, `markdown`)
- `text`: Extracted text content
- `blobs`: Dictionary of `FragmentUri → DataUri` for embedded content
