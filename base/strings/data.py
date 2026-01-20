import base64
import mimetypes

from typing import Literal

from base.core.strings import ValidatedStr
from base.strings.file import FileName

REGEX_UUID = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
REGEX_MIMETYPE = r"[a-z\-]+/[a-z0-9+\-.]+"
REGEX_BASE64 = r"[\w\d+/]+=*"
REGEX_DATA_URI = rf"data:{REGEX_MIMETYPE};base64,{REGEX_BASE64}"


##
## MIME
##


MimeMode = Literal[
    "document",
    "image",
    "markdown",
    "media",
    "plain",
    "spreadsheet",
]


class MimeType(ValidatedStr):
    """
    The MIME type of a resource.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "text/markdown",
            "image/jpeg",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_MIMETYPE

    ##
    ## Infer MIME type
    ##

    @staticmethod
    def guess(url: str, web: bool = False) -> "MimeType | None":
        overrides = EXTENSION_OVERRIDES_WEB if web else EXTENSION_OVERRIDES_FILE
        if ext := next((ext for ext in overrides if url.endswith(ext)), None):
            return overrides[ext]
        elif guess := mimetypes.guess_type(url, strict=False)[0]:
            return MimeType.decode(guess)
        else:
            return None

    @staticmethod
    def guess_from_info(
        filename: str | None,
        content_type: str | None,
    ) -> "MimeType | None":
        if (
            content_type
            and (decoded_mime_type := MimeType.try_decode(content_type))
            and decoded_mime_type not in MIME_TYPES_USELESS
        ):
            return decoded_mime_type
        elif filename:
            return MimeType.guess(filename, web=False)
        else:
            return None

    @staticmethod
    def guess_or_default(url: str, default_value: str, web: bool = False) -> "MimeType":
        return MimeType.guess(url, web) or MimeType.decode(default_value)

    @staticmethod
    def guess_or_plain(url: str, web: bool = False) -> "MimeType":
        return MimeType.guess(url, web) or MIME_TYPE_PLAIN

    @staticmethod
    def guess_from_bytes(data: bytes | str) -> "MimeType | None":
        # If we get raw bytes, only convert the first 30 bytes (=> 40 chars),
        # since this is enough to match against any magic number.
        if isinstance(data, bytes):
            data = base64.b64encode(data[:30]).decode()
        assert isinstance(data, str)

        if data.startswith("iVBORw0K"):
            return MimeType.decode("image/png")
        elif data.startswith("/9j/"):
            return MimeType.decode("image/jpeg")
        elif data.startswith(("R0lGODdh", "R0lGODlh")):
            return MimeType.decode("image/gif")
        elif data.startswith("UklGR") and data[11:15] == "XRUJQ":
            return MimeType.decode("image/webp")
        else:
            return None

    def guess_extension(self) -> str | None:
        guessed_ext = mimetypes.guess_extension(self, strict=False)
        if not guessed_ext and self in MIME_TO_EXTENSION_OVERRIDES:
            guessed_ext = MIME_TO_EXTENSION_OVERRIDES[self]
        return guessed_ext

    ##
    ## Utils
    ##

    def mode(
        self,
    ) -> MimeMode:
        if self.startswith("image/"):
            return "image"
        elif self in ("text/markdown", "text/x-markdown"):
            return "markdown"
        elif self.startswith(("audio/", "video/")):
            return "media"
        elif self in MIME_TYPES_SPREADSHEET:
            return "spreadsheet"
        elif str(self) not in MIME_TYPES_NOT_TEXT and (
            self.startswith("text/") or str(self) in MIME_TYPES_TEXT
        ):
            return "plain"
        else:
            return "document"


MIME_TYPE_PLAIN = MimeType("text/plain")
MIME_TYPE_MARKDOWN = MimeType("text/markdown")
MIME_TYPE_YAML = MimeType("text/x-yaml")
MIME_TYPE_ARXIV_TEX = MimeType("text/x-tex")
MIME_TYPE_ARXIV_SRC = MimeType("application/x-eprint-tar")

MIME_TYPES_SPREADSHEET = [
    MimeType("application/vnd.ms-excel"),
    MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    MimeType("text/csv"),
]

EXTENSION_OVERRIDES_FILE = {
    ".bat": MimeType("application/bat"),
    ".bib": MimeType("application/x-bibtex"),
    ".md": MIME_TYPE_MARKDOWN,
    ".mdx": MIME_TYPE_MARKDOWN,
    ".rs": MimeType("text/x-rust"),
    ".sh": MimeType("text/x-shellscript"),
    ".ts": MimeType("text/x-typescript"),
    ".yaml": MIME_TYPE_YAML,
    ".yml": MIME_TYPE_YAML,
}

EXTENSION_OVERRIDES_WEB = {
    **EXTENSION_OVERRIDES_FILE,
    ".php": MimeType("text/html"),
}

MIME_TYPES_TEXT = [
    MimeType("application/bat"),
    MimeType("application/javascript"),
    MimeType("application/json"),
    MimeType("application/ld+json"),
    MimeType("application/sql"),
    MimeType("application/wasm"),
    MimeType("application/x-bibtex"),
    MimeType("application/x-c++src"),
    MimeType("application/x-csrc"),
    MimeType("application/x-httpd-php-source"),
    MimeType("application/x-httpd-php"),
    MimeType("application/x-java-class"),
    MimeType("application/x-perl"),
    MimeType("application/x-ruby"),
    MimeType("application/x-shellscript"),
    MimeType("application/x-sql"),
    MimeType("application/x-tex"),
    MimeType("application/x-yaml"),
    MimeType("application/xhtml+xml"),
    MimeType("application/xml"),
    MimeType("application/xslt+xml"),
]

MIME_TYPES_NOT_TEXT = [
    MIME_TYPE_ARXIV_SRC,
    MIME_TYPE_ARXIV_TEX,
]

MIME_TYPES_USELESS = [
    MimeType("application/download"),
    MimeType("application/force-download"),
    MimeType("application/gzip"),
    MimeType("application/octet-stream"),
    MimeType("application/x-file-to-save"),
    MimeType("application/x-unknown"),
]

MIME_TO_EXTENSION_OVERRIDES: dict[str, str] = {
    "audio/wav": ".wav",
}


##
## Blob Data URI (base64)
##


class DataUri(ValidatedStr):
    """
    The content of a blob, represented as `"data:{mime_type};base64,{data}"`.
    """

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "data:image/png;base64,"
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAAXNSR0IA"
            "rs4c6QAAAA1JREFUGFdjSJn2zhYABXACJjXArEYAAAAASUVORK5CYII="
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_DATA_URI

    @staticmethod
    def new(mime_type: MimeType, data: bytes | str) -> "DataUri":
        if isinstance(data, bytes):
            data = base64.b64encode(data).decode()
        return DataUri(f"data:{mime_type};base64,{data}")

    @staticmethod
    def stub(blob: str | None = None) -> "DataUri":
        if not blob:
            return DataUri.decode(  # ffff00 ff
                "data:image/png;base64,"
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP//AAT/Af9mVsegAAAAAElFTkSuQmCC"
            )
        elif blob == "discard":
            return DataUri.decode(  # ff00ff ff
                "data:image/png;base64,"
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AP8A/wQAAf/2bp8NAAAAAElFTkSuQmCC"
            )
        elif blob == "file":
            return DataUri.decode(  # 4a0d33 ff
                "data:image/png;base64,"
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAAD0lEQVR4AQEEAPv/AEoNMwEvAIte5JyFAAAAAElFTkSuQmCC"
            )
        elif blob.startswith("data:"):
            return DataUri.decode(blob)
        else:
            return DataUri.decode(f"data:image/webp;base64,{blob}")

    def as_bytes(self) -> bytes:
        _, data_base64 = self.split(",", maxsplit=1)
        return base64.b64decode(data_base64)

    def guess_ext(self) -> str:
        mime_type, _ = self.parts()
        return mime_type.guess_extension() or ""

    def mime_type(self) -> MimeType:
        return self.parts()[0]

    def parts(self) -> tuple[MimeType, str]:
        """
        Parses the data URI into `(mime_type, data_base64)`.
        """
        pair = self.removeprefix("data:").split(";base64,", maxsplit=1)
        mime_type, data_base64 = pair
        return MimeType(mime_type), data_base64


##
## Base64
##


class Base64Std(ValidatedStr):
    @staticmethod
    def from_bytes(data: bytes) -> "Base64Std":
        return Base64Std(base64.b64encode(data).decode())

    @classmethod
    def _schema_regex(cls) -> str:
        return r"[A-Za-z0-9+/]+=*"

    @classmethod
    def from_filename(cls, filename: FileName) -> "Base64Std":
        """
        Convert `Base64Id.as_filename` back into the original value.
        """
        item_id = str(filename).replace("-", "+").replace("_", "/")
        padding = -len(item_id) % 4
        if padding > 0:
            item_id += "=" * padding
        return cls(item_id)

    def as_filename(self) -> FileName:
        """
        Transform base64 characters ("+" and "/", alongside "=" padding) into
        a format compatible with `ResourceUri`.
        """
        filename = self.replace("+", "-").replace("/", "_").rstrip("=")
        return FileName(filename)

    def as_bytes(self) -> bytes:
        return base64.b64decode(self)

    def as_url_safe(self) -> "Base64Safe":
        return Base64Safe(self.replace("+", "-").replace("/", "_"))


class Base64Safe(ValidatedStr):
    @staticmethod
    def from_bytes(data: bytes) -> "Base64Safe":
        return Base64Safe(base64.urlsafe_b64encode(data).decode())

    @classmethod
    def _schema_regex(cls) -> str:
        return r"[A-Za-z0-9\-_]+=*"

    @classmethod
    def from_filename(cls, filename: FileName) -> "Base64Safe":
        """
        Convert `Base64Id.as_filename` back into the original value.
        """
        item_id = str(filename)
        padding = -len(filename) % 4
        if padding > 0:
            item_id += "=" * padding
        return cls(item_id)

    def as_filename(self) -> FileName:
        """
        Transform base64 characters ("+" and "/", alongside "=" padding) into
        a format compatible with `ResourceUri`.
        """
        filename = self.rstrip("=")
        return FileName(filename)

    def as_bytes(self) -> bytes:
        return base64.urlsafe_b64decode(self)

    def as_standard(self) -> "Base64Std":
        return Base64Std(self.replace("-", "+").replace("_", "/"))
