from base.models.content import ContentBlob, ContentText, PartLink, PartText
from base.models.rendered import Rendered
from base.resources.aff_body import AffBodyMedia, ObsMedia
from base.strings.data import DataUri, MimeType
from base.strings.resource import ObservableUri


##
## Test Fixtures
##


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
    content = ContentText.parse("Hello world")
    rendered = Rendered.render(content, [])
    assert len(rendered.blocks) == 1
    assert rendered.blocks[0].type == "text"


def test_rendered_render_with_embed() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/image.png")
    obs_media = _given_obs_media(uri=str(media_uri))
    content = ContentText.new(
        [
            PartText.new("Before "),
            PartLink.new("embed", "figure", media_uri),
            PartText.new(" after"),
        ]
    )

    rendered = Rendered.render(content, [obs_media])
    assert len(rendered.blocks) == 3
    assert rendered.blocks[0].type == "text"
    assert rendered.blocks[1].type == "blob"
    assert rendered.blocks[1].uri == media_uri
    assert rendered.blocks[2].type == "text"


def test_rendered_render_missing_embed_keeps_link() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/missing.png")
    content = ContentText.parse(f"![figure]({media_uri})")

    rendered = Rendered.render(content, [])
    assert len(rendered.blocks) == 1
    assert rendered.blocks[0].type == "text"

    result = rendered.as_llm_inline([])
    assert result == [f"![figure]({media_uri})"]


##
## Rendered.as_llm_inline
##


def test_rendered_as_llm_inline_with_media_supported() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    obs_media = _given_obs_media(uri=str(media_uri))
    supports_media = [MimeType.decode("image/png")]

    rendered = Rendered.render_parts(
        parts=[
            PartText.new("Before "),
            PartLink.new("embed", None, media_uri),
            PartText.new(" after"),
        ],
        observations=[obs_media],
    )

    result = rendered.as_llm_inline(supports_media)
    assert len(result) == 3
    assert result[0] == "Before"
    assert isinstance(result[1], ContentBlob)
    assert result[2] == " after"


def test_rendered_as_llm_inline_with_media_unsupported() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    obs_media = _given_obs_media(uri=str(media_uri))
    rendered = Rendered.render_parts(
        parts=[
            PartText.new("Before "),
            PartLink.new("embed", None, media_uri),
            PartText.new(" after"),
        ],
        observations=[obs_media],
    )

    result = rendered.as_llm_inline(supports_media=[])
    result_str = "\n\n".join(
        f"@BLOB {p.uri}" if isinstance(p, ContentBlob) else p for p in result
    )
    print(f"<result>\n{result_str}\n</result>")
    assert len(result) == 1
    assert result[0] == (
        """\
Before

<blob uri="ndk://test/-/doc/$media/fig.png" mimetype="image/png">
An image
</blob>

 after\
"""
    )


def test_rendered_as_llm_inline_with_multiple_media() -> None:
    media_uri1 = ObservableUri.decode("ndk://test/-/doc/$media/fig1.png")
    media_uri2 = ObservableUri.decode("ndk://test/-/doc/$media/fig2.png")
    obs_media1 = _given_obs_media(uri=str(media_uri1), placeholder="Image 1")
    obs_media2 = _given_obs_media(uri=str(media_uri2), placeholder="Image 2")
    rendered = Rendered.render_parts(
        parts=[
            PartLink.new("embed", None, media_uri1),
            PartLink.new("embed", None, media_uri2),
        ],
        observations=[obs_media1, obs_media2],
    )

    # Should include only 1 blob, the other becomes placeholder
    result = rendered.as_llm_inline([MimeType.decode("image/png")])
    assert len(result) == 2
    assert isinstance(result[0], ContentBlob)
    assert isinstance(result[1], ContentBlob)


##
## Rendered.as_llm_split
##


def test_rendered_as_llm_split_separates_text_and_blobs() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    obs_media = _given_obs_media(uri=str(media_uri))
    rendered = Rendered.render_parts(
        parts=[
            PartText.new("Some text "),
            PartLink.new("embed", None, media_uri),
        ],
        observations=[obs_media],
    )

    text, blobs = rendered.as_llm_split([MimeType.decode("image/png")])
    assert text == f"Some text\n\n![]({media_uri})"
    assert len(blobs) == 1
    assert blobs[0].uri == media_uri


def test_rendered_as_llm_split_deduplicates_blobs() -> None:
    media_uri = ObservableUri.decode("ndk://test/-/doc/$media/fig.png")
    obs_media = _given_obs_media(uri=str(media_uri))
    rendered = Rendered.render_parts(
        parts=[
            PartLink.new("embed", None, media_uri),  # Same blob twice
            PartText.new(" middle "),
            PartLink.new("embed", None, media_uri),
        ],
        observations=[obs_media],
    )

    text, blobs = rendered.as_llm_split([MimeType.decode("image/png")])
    assert text == f"![]({media_uri})\n\n middle\n\n![]({media_uri})"
    assert len(blobs) == 1
    assert blobs[0].uri == media_uri
