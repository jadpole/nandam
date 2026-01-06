import contextlib
import re

from pathlib import Path
from typing import Self

from base.core.strings import normalize_str, ValidatedStr

REGEX_FILENAME = r"[a-zA-Z0-9\-._]+"
REGEX_FILEPATH = rf"{REGEX_FILENAME}(?:/{REGEX_FILENAME})*"


class FileName(ValidatedStr):
    """
    A well-formed component of a `FilePath` or `ResourceUri`, which supports a
    more limited character set than `WebUrl`.

    NOTE: Some Resource URIs require "-" to be a valid filename.
    However, any other filename with no alphanumeric character is invalid.
    Notably, "." and ".." are rejected.
    """

    @classmethod
    def _parse(cls, v: str) -> "FileName":
        if v != "-" and re.fullmatch(r"[\-._]+", v):
            raise ValueError(f"invalid {cls.__name__}: got '{v}'")
        return FileName(v)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "document-42.pdf",
            "image_file.png",
            ".gitignore",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_FILENAME

    @classmethod
    def normalize(cls, value: str) -> Self:
        """
        Try to generate a filename from an arbitrary string, usually a title.
        Replaces accented characters with their ASCII equivalent.
        """
        if value == "-":
            return cls("-")

        normalized = normalize_str(
            value,
            allowed_special_chars="-._",
            disallowed_replacement="_",
            remove_duplicate_chars="-._",
            remove_prefix_chars="-_",
            remove_suffix_chars="-._",
            unquote_url=True,
        )

        # Reject non-ASCII file names.  Notably, fails to generate a filename
        # from the Kanji title of a web page or YouTube video.
        if normalized in ("", "_", "."):
            raise ValueError(f"cannot normalize {cls.__name__}, got '{value}'")

        return cls.decode(normalized)

    @classmethod
    def try_normalize(cls, value: str) -> Self | None:
        try:
            return cls.normalize(value)
        except ValueError:
            return None

    @staticmethod
    def from_http_headers(headers: dict[str, str]) -> "FileName | None":
        prefixes = ["attachment;", "inline;"]
        filename_attributes = ["filename=", "filename*=UTF-8''"]

        if (disposition := headers.get("content-disposition")) and any(
            disposition.startswith(p) for p in prefixes
        ):
            for attribute in filename_attributes:
                with contextlib.suppress(ValueError):
                    filename_position = disposition.rindex(attribute)
                    if filename := FileName.try_normalize(
                        disposition[filename_position + len(attribute) :]
                        .split(";")[0]
                        .replace('"', "")
                    ):
                        return filename

        return None

    def ext(self) -> str | None:
        # Documents sometimes expects a multi-part extension.
        special_exts = [
            ".tar.gz",
        ]
        if ext := next((ext for ext in special_exts if self.endswith(ext)), None):
            return ext
        return Path(self).suffix

    def filepath(self) -> "FilePath":
        return FilePath(str(self))

    ##
    ## Manipulation
    ##

    def with_ext(self, new_extension: str | None) -> "FileName":
        """
        Replace the extension of the filename.
        - When passing `None` or an empty string, strip the extension.
        - The extension, when given, must start with ".".
        """
        basename = str(self)
        if (old_ext := self.ext()) and basename != old_ext:
            basename = basename.removesuffix(old_ext)

        if not new_extension:
            return FileName.decode(basename)
        else:
            assert new_extension.startswith(".")
            assert new_extension != "."
            return FileName.decode(f"{basename}{new_extension}")


class FilePath(ValidatedStr):
    """
    A sequence of `FileName` joined by "/", which can be used to represent the
    path of, e.g., a `ResourceUri`.
    """

    @staticmethod
    def new(parts: list[FileName]) -> "FilePath":
        if not parts:
            raise ValueError("cannot create FilePath from empty list")
        return FilePath("/".join([str(part) for part in parts]))

    @classmethod
    def _parse(cls, v: str) -> Self:
        for part in v.split("/"):
            FileName.decode_part(cls, v, part)
        return cls(v)

    @classmethod
    def _schema_examples(cls) -> list[str]:
        return [
            "directory/document-42.pdf",
            "image_file.png",
            "-/.gitignore",
        ]

    @classmethod
    def _schema_regex(cls) -> str:
        return REGEX_FILEPATH

    @classmethod
    def normalize(cls, value: str) -> Self:
        """
        A `FilePath` is normalizable when each `FileName` is normalizable.
        """
        parts = [FileName.try_normalize(part) for part in value.split("/")]
        if None in parts:
            raise ValueError(f"cannot normalize {cls.__name__}, got '{value}'")
        return cls("/".join(parts))  # type: ignore

    @classmethod
    def try_normalize(cls, value: str) -> Self | None:
        try:
            return cls.normalize(value)
        except ValueError:
            return None

    ##
    ## Manipulation
    ##

    def is_child_or(self, parent_or_self: "FilePath") -> bool:
        return self == parent_or_self or self.startswith(f"{parent_or_self}/")

    def parts(self) -> list[FileName]:
        """Return the file names that make up the file path."""
        return [FileName(part) for part in self.split("/")]

    def extend(self, child_path: "FileName | FilePath") -> "FilePath":
        return FilePath(f"{self}/{child_path}")

    def filename(self) -> FileName:
        """Return the last part of the file path."""
        return FileName(self.rsplit("/", maxsplit=1)[-1])

    def ext(self) -> str | None:
        return self.filename().ext()

    def with_ext(self, new_extension: str | None) -> "FilePath":
        """Modifies the filename to have the given extension."""
        parts = self.parts()
        new_filename = parts[-1].with_ext(new_extension)
        return FilePath("/".join([*parts[:-1], new_filename]))
