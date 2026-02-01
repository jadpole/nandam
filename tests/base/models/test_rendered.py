from base.models.content import ContentBlob, ContentText, PartLink, PartText
from base.models.rendered import Rendered, RenderedPartial
from base.resources.aff_body import AffBodyMedia, ObsMedia
from base.resources.observation import Observation
from base.strings.data import DataUri, MimeType
from base.strings.resource import ObservableUri


##
## Test Fixtures
##


def _given_content_text(text: str) -> ContentText:
    return ContentText.new_plain(text)


def _given_content_blob(
    uri: str = "ndk://test/-/doc/$media/image.png",
    mime_type: str = "image/png",
    placeholder: str = "An image",
) -> ContentBlob:
    return ContentBlob(
        uri=ObservableUri.decode(uri),
        placeholder=placeholder,
        mime_type=MimeType.decode(mime_type),
        blob="iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",  # 1x1 PNG
    )


def _given_obs_media(
    uri: str = "ndk://test/-/doc/$media/image.png",
    mime_type: str = "image/png",
    placeholder: str = "An image",
) -> ObsMedia:
    media_uri = ObservableUri[AffBodyMedia].decode(uri)
    return ObsMedia.new(
        resource_uri=media_uri.resource_uri(),
        path=media_uri.suffix.path,
        mime_type=MimeType.decode(mime_type),
        blob=DataUri.new(MimeType.decode(mime_type), b"\x89PNG\r\n\x1a\n"),
        description=None,
        placeholder=placeholder,
    )


##
## Rendered.render
##


def test_rendered_render_plain_text() -> None:
    content = _given_content_text("Hello world")
    observations: list[Observation] = []

    rendered = Rendered.render(content, observations)

    assert len(rendered.text) >= 1
    assert rendered.blobs == []


def test_rendered_render_with_embed() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/image.png")
    content = ContentText.new(
        [
            PartText.new("Before "),
            PartLink.new("embed", "figure", media_uri),
            PartText.new(" after"),
        ]
    )
    obs = _given_obs_media(uri=str(media_uri))
    observations: list[Observation] = [obs]
    rendered = Rendered.render(content, observations)

    # Should have the embed resolved
    assert len(rendered.blobs) == 1
    assert rendered.blobs[0].uri == media_uri


def test_rendered_render_missing_embed_keeps_link() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/missing.png")
    content = ContentText.new(
        [
            PartLink.new("embed", "figure", media_uri),
        ]
    )
    observations: list[Observation] = []  # No matching observation

    rendered = Rendered.render(content, observations)

    # Should keep the link as-is
    assert len(rendered.blobs) == 0
    assert any(isinstance(p, PartLink) and p.href == media_uri for p in rendered.text)


##
## Rendered.as_llm_inline
##


def test_rendered_as_llm_inline_with_supported_media() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    blob = _given_content_blob(uri=str(media_uri))
    rendered = Rendered(
        text=[
            PartText.new("Before "),
            PartLink.new("embed", None, media_uri),
            PartText.new(" after"),
        ],
        blobs=[blob],
    )
    supports_media = [MimeType.decode("image/png")]

    result = rendered.as_llm_inline(supports_media, limit_media=None)

    # Should include the blob inline
    assert any(isinstance(item, ContentBlob) for item in result)


def test_rendered_as_llm_inline_without_supported_media() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    blob = _given_content_blob(uri=str(media_uri))
    rendered = Rendered(
        text=[
            PartText.new("Before "),
            PartLink.new("embed", None, media_uri),
            PartText.new(" after"),
        ],
        blobs=[blob],
    )
    supports_media: list[MimeType] = []  # No support

    result = rendered.as_llm_inline(supports_media, limit_media=None)

    # Should use placeholder instead
    assert all(isinstance(item, str) for item in result)
    # The placeholder text should appear
    assert any("An image" in str(item) for item in result)


def test_rendered_as_llm_inline_respects_limit() -> None:
    media_uri1 = ObservableUri.decode("ndk://test/-/doc/$media/fig1.png")
    media_uri2 = ObservableUri.decode("ndk://test/-/doc/$media/fig2.png")
    blob1 = _given_content_blob(uri=str(media_uri1), placeholder="Image 1")
    blob2 = _given_content_blob(uri=str(media_uri2), placeholder="Image 2")
    rendered = Rendered(
        text=[
            PartLink.new("embed", None, media_uri1),
            PartLink.new("embed", None, media_uri2),
        ],
        blobs=[blob1, blob2],
    )
    supports_media = [MimeType.decode("image/png")]

    result = rendered.as_llm_inline(supports_media, limit_media=1)

    # Should include only 1 blob, the other becomes placeholder
    blob_count = sum(1 for item in result if isinstance(item, ContentBlob))
    assert blob_count == 1


##
## Rendered.as_llm_split
##


def test_rendered_as_llm_split_separates_text_and_blobs() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    blob = _given_content_blob(uri=str(media_uri))
    rendered = Rendered(
        text=[
            PartText.new("Some text "),
            PartLink.new("embed", None, media_uri),
        ],
        blobs=[blob],
    )
    supports_media = [MimeType.decode("image/png")]

    text, blobs = rendered.as_llm_split(supports_media, limit_media=None)

    assert isinstance(text, str)
    assert len(blobs) == 1
    assert blobs[0].uri == media_uri


def test_rendered_as_llm_split_deduplicates_blobs() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    blob = _given_content_blob(uri=str(media_uri))
    rendered = Rendered(
        text=[
            PartLink.new("embed", None, media_uri),  # Same blob twice
            PartText.new(" middle "),
            PartLink.new("embed", None, media_uri),
        ],
        blobs=[blob],
    )
    supports_media = [MimeType.decode("image/png")]

    _, blobs = rendered.as_llm_split(supports_media, limit_media=None)

    # Should only have one blob, not two
    assert len(blobs) == 1


##
## Rendered.get_blob
##


def test_rendered_get_blob_found() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    blob = _given_content_blob(uri=str(media_uri))
    rendered = Rendered(text=[], blobs=[blob])

    result = rendered.get_blob(media_uri)

    assert result is blob


def test_rendered_get_blob_not_found() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    other_uri = ObservableUri.decode("ndk://test/-/doc/$media/other.png")
    blob = _given_content_blob(uri=str(media_uri))
    rendered = Rendered(text=[], blobs=[blob])

    result = rendered.get_blob(other_uri)

    assert result is None


##
## RenderedPartial
##


def test_rendered_partial_new() -> None:
    observations: list[Observation] = []
    partial = RenderedPartial.new(observations)

    assert partial.text == []
    assert partial.blobs == []
    assert partial.available_obs == []


def test_rendered_partial_render_part_text() -> None:
    partial = RenderedPartial.new([])
    part = PartText.new("Hello")

    partial.render_part_mut(part)

    assert len(partial.text) == 1
    assert partial.text[0] == part


def test_rendered_partial_render_embed_blob() -> None:
    partial = RenderedPartial.new([])
    blob = _given_content_blob()

    partial.render_embed_mut(blob, label="test")

    assert len(partial.blobs) == 1
    assert len(partial.text) == 1
    assert isinstance(partial.text[0], PartLink)
    assert partial.text[0].mode == "embed"


def test_rendered_partial_render_embed_text_recurses() -> None:
    partial = RenderedPartial.new([])
    inner_text = ContentText.new(
        [
            PartText.new("Inner content"),
        ]
    )

    partial.render_embed_mut(inner_text)

    # Should have recursively added the inner parts
    assert len(partial.text) >= 1
    assert any(isinstance(p, PartText) and "Inner" in p.text for p in partial.text)
