from functools import cache
from importlib.resources import files
from io import BytesIO
from pathlib import Path
from PIL import Image
from pydantic import BaseModel, Field

from base.core.values import parse_yaml_as
from base.resources.aff_body import AffBodyMedia, ObsMedia
from base.strings.data import DataUri, MimeType
from base.strings.resource import ObservableUri, ResourceUri, WebUrl


##
## Documents
##


class DemoFileHeader(BaseModel):
    resource_uri: ResourceUri | None = None
    content_url: WebUrl | None = None
    blobs: list[str] = Field(default_factory=list)


def _read_test_markdown(path: str) -> tuple[str, DemoFileHeader]:
    file_content = files("tests.data").joinpath(path).read_text()

    if file_content.startswith("---\n"):
        header_yaml, text = file_content.split("\n---\n", 1)
        header = parse_yaml_as(DemoFileHeader, header_yaml)
    else:
        text = file_content
        header = DemoFileHeader()

    return text, header


@cache
def read_2303_11366v2() -> tuple[str, DemoFileHeader]:
    return _read_test_markdown("sample-fragment-2303.11366v2.md")


@cache
def read_lotto_6_49() -> tuple[str, DemoFileHeader]:
    return _read_test_markdown("sample-fragment-lotto-6-49.md")


##
## Media
##


@cache
def read_sample_image() -> DataUri:
    output = BytesIO()
    image = Image.open(Path(__file__).parent / "sample-image.png")
    image.convert("RGBA").save(output, format="webp", optimize=True)
    return DataUri.new(MimeType("image/webp"), output.getvalue())


def given_sample_media() -> ObsMedia:
    image_uri = ObservableUri[AffBodyMedia].decode("ndk://stub/-/output.png/$media")
    image_mime, image_blob = read_sample_image().parts()
    return ObsMedia(
        uri=image_uri,
        description="Stingray Music Web Player",
        placeholder="A web player that is playing 'Sucker' by Jonas Brothers.",
        mime_type=image_mime,
        blob=image_blob,
    )
