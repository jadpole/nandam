from datetime import datetime
from typing import Literal

from base.resources.metadata import AffordanceInfo, ObservationInfo
from base.resources.observation import Observation, ObservationBundle
from base.strings.data import DataUri, MimeType
from base.strings.file import FileName, REGEX_FILENAME, FilePath
from base.strings.resource import Affordance, Observable, ObservableUri, WebUrl

REGEX_SUFFIX_FILE = rf"\$file(?:/{REGEX_FILENAME})*"


##
## Suffix
##


class AffFile(Affordance, Observable, frozen=True):
    @staticmethod
    def new(path: FileName | FilePath | list[FileName] | None = None) -> "AffFile":
        if not path:
            path = []
        elif isinstance(path, FileName):
            path = [path]
        elif isinstance(path, FilePath):
            path = path.parts()
        return AffFile(path=path)

    @classmethod
    def suffix_kind(cls) -> str:
        return "file"

    @classmethod
    def _suffix_regex(cls) -> str:
        return REGEX_SUFFIX_FILE

    @classmethod
    def _suffix_examples(cls) -> list[str]:
        return ["$file", "$file/main.tex", "$file/figures/filename.png"]

    def affordance(self) -> "AffFile":
        return self


##
## Observation
##


class ObsFile(Observation[AffFile], frozen=True):
    kind: Literal["file"] = "file"
    description: str | None
    mime_type: MimeType | None
    expiry: datetime | None
    download_url: DataUri | WebUrl

    def info(self) -> ObservationInfo:
        return ObservationInfo(
            suffix=self.uri.suffix,
            num_tokens=None,
            mime_type=self.mime_type,
            description=self.description,
        )

    def info_attributes(self) -> list[tuple[str, str]]:
        attributes = super().info_attributes()
        if self.expiry:
            attributes.append(("expiry", self.expiry.isoformat()))
        return attributes


class BundleFile(ObservationBundle[AffFile], frozen=True):
    kind: Literal["file"] = "file"
    description: str | None
    mime_type: MimeType | None
    expiry: datetime | None
    download_url: DataUri | WebUrl

    def info(self) -> AffordanceInfo:
        return AffordanceInfo(
            suffix=self.uri.suffix,
            mime_type=self.mime_type,
            description=self.description,
        )

    def observations(self) -> list[Observation]:
        return [
            ObsFile(
                uri=ObservableUri.decode(str(self.uri)),
                description=self.description,
                mime_type=self.mime_type,
                expiry=self.expiry,
                download_url=self.download_url,
            )
        ]
