import aiofiles
import io

from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from base.api.documents import FragmentMode, FragmentUri
from base.strings.data import MIME_TYPE_PLAIN, DataUri, MimeType
from base.strings.file import FileName, FilePath
from base.strings.resource import WebUrl


@dataclass(kw_only=True)
class Downloaded:
    url: WebUrl | None
    response_headers: dict[str, str]
    name: str | None
    mime_type: MimeType | None
    filename: FileName | None
    charset: str | None

    @staticmethod
    def stub_text(
        *,
        url: str | None = None,
        response_headers: dict[str, str] | None = None,
        name: str | None = None,
        mime_type: str | None = None,
        filename: str | None = None,
        charset: str = "utf-8",
        text: str,
    ) -> "DownloadedData":
        return DownloadedData(
            url=WebUrl.decode(url) if url else None,
            response_headers=response_headers or {},
            name=name,
            mime_type=MimeType.decode(mime_type) if mime_type else None,
            filename=FileName.decode(filename) if filename else None,
            charset=charset,
            data=text.encode(charset),
        )

    def delete_tempfile(self) -> None:
        pass

    def mime_type_forced(self) -> MimeType:
        if self.mime_type:
            return self.mime_type
        elif self.filename:
            return MimeType.guess_or_plain(str(self.filename), False)
        else:
            return MIME_TYPE_PLAIN

    def open_bytes(self) -> BinaryIO:
        raise NotImplementedError("Subclasses must implement Downloaded.open_bytes")

    async def read_bytes_async(self) -> bytes:
        raise NotImplementedError("Subclasses must implement Downloaded.open_bytes")

    def read_text(self) -> str:
        with self.open_bytes() as f:
            return f.read().decode(self.charset or "utf-8", errors="ignore")

    async def read_text_async(self) -> str:
        file_bytes = await self.read_bytes_async()
        return file_bytes.decode(self.charset or "utf-8", errors="ignore")


@dataclass(kw_only=True)
class DownloadedFile(Downloaded):
    tempfile_path: Path

    def delete_tempfile(self) -> None:
        Path(self.tempfile_path).unlink(missing_ok=True)

    def open_bytes(self) -> BinaryIO:
        return open(self.tempfile_path, mode="rb")

    async def read_bytes_async(self) -> bytes:
        async with aiofiles.open(self.tempfile_path, mode="rb") as f:
            return await f.read()


@dataclass(kw_only=True)
class DownloadedData(Downloaded):
    data: bytes

    def open_bytes(self) -> BinaryIO:
        return io.BytesIO(self.data)

    async def read_bytes_async(self) -> bytes:
        return self.data


@dataclass(kw_only=True)
class Extracted:
    mode: FragmentMode
    name: str | None
    path: FilePath | None
    mime_type: MimeType
    blobs: dict[FragmentUri, DataUri]
    text: str
