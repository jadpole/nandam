import re

from dataclasses import dataclass
from typing import Literal

from base.resources.aff_body import AffBody
from base.core.exceptions import BadRequestError
from base.resources.metadata import AffordanceInfo
from base.strings.file import FileName
from base.strings.resource import (
    ExternalUri,
    Observable,
    Realm,
    ResourceUri,
    RootReference,
    WebUrl,
)

from knowledge.models.storage_metadata import Locator, MetadataDelta, ResourceView
from knowledge.server.context import (
    Connector,
    KnowledgeContext,
    ObserveResult,
    ResolveResult,
)
from knowledge.services.downloader import SvcDownloader

REALM_PUBLIC = Realm.decode("public")


class ArXivLocator(Locator, frozen=True):
    kind: Literal["arxiv"] = "arxiv"
    realm: Realm = REALM_PUBLIC
    paper_id: FileName

    @staticmethod
    def from_web(url: WebUrl) -> "ArXivLocator | None":
        """
        Extract the unique ID of the paper and discard all other parameters.
        """
        domain = url.domain.removeprefix("www.")
        if (
            domain == "arxiv.org"
            and (
                match := re.fullmatch(
                    r"(?:abs|pdf|src)/(\d{4}.\d{5}(?:v\d+)?)",
                    url.path,
                )
            )
            and (paper_id := FileName.try_decode(match.group(1)))
        ):
            return ArXivLocator(paper_id=paper_id)
        else:
            return None

    @staticmethod
    def from_uri(uri: ResourceUri) -> "ArXivLocator | None":
        if uri.realm == "public" and uri.subrealm == "arxiv" and len(uri.path) == 1:
            return ArXivLocator(paper_id=uri.path[0])
        else:
            return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("arxiv"),
            path=[self.paper_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(f"https://arxiv.org/abs/{self.paper_id}")

    def citation_url(self) -> WebUrl:
        return self.content_url()


class YouTubeLocator(Locator, frozen=True):
    kind: Literal["youtube"] = "youtube"
    realm: Realm = REALM_PUBLIC
    video_id: FileName

    @staticmethod
    def from_web(url: WebUrl) -> "YouTubeLocator | None":
        """
        Extract the YouTube video ID and discard all other parameters.
        """
        domain = url.domain.removeprefix("www.")
        if domain == "youtube.com" and (  # noqa: SIM114
            video_id := FileName.try_decode(url.get_query("v"))
        ):
            return YouTubeLocator(video_id=video_id)
        elif domain == "youtu.be" and (video_id := FileName.try_decode(url.path)):
            return YouTubeLocator(video_id=video_id)
        else:
            return None

    @staticmethod
    def from_uri(uri: ResourceUri) -> "YouTubeLocator | None":
        if uri.realm == "public" and uri.subrealm == "youtube" and len(uri.path) == 1:
            return YouTubeLocator(video_id=uri.path[0])
        else:
            return None

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("youtube"),
            path=[self.video_id],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(f"https://youtu.be/{self.video_id}")

    def citation_url(self) -> WebUrl:
        return self.content_url()


@dataclass(kw_only=True)
class PublicConnector(Connector):
    realm: Realm = REALM_PUBLIC

    async def locator(self, reference: RootReference) -> Locator | None:
        # fmt: off
        if isinstance(reference, WebUrl):
            return (
                ArXivLocator.from_web(reference)
                or YouTubeLocator.from_web(reference)
            )
        elif isinstance(reference, ExternalUri):
            return None
        else:
            return (
                ArXivLocator.from_uri(reference)
                or YouTubeLocator.from_uri(reference)
            )

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        assert isinstance(locator, ArXivLocator | YouTubeLocator)

        # Always use the cached metadata when available.
        if cached:
            return ResolveResult()

        # Otherwise, simply return the list of supported affordances.
        return ResolveResult(
            metadata=MetadataDelta(affordances=[AffordanceInfo(suffix=AffBody.new())]),
            should_cache=True,
        )

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        """
        NOTE: Always download the "document" when the resource is first loaded.
        NOTE: Document errors are hoisted onto the resource for simplicity.
        """
        assert isinstance(locator, ArXivLocator | YouTubeLocator)

        match locator, observable:
            case (ArXivLocator(), AffBody()):
                return await _public_read_arxiv_body(self.context, locator)
            case (YouTubeLocator(), AffBody()):
                return await _public_read_youtube_body(self.context, locator)
            case _:
                raise BadRequestError.observable(observable.as_suffix())


##
## Read
##


async def _public_read_arxiv_body(
    context: KnowledgeContext,
    locator: ArXivLocator,
) -> ObserveResult:
    """
    Download and parse the requested URL via the Documents service.
    NOTE: Cache documents that are expensive to parse and unlikely to change.
    """
    downloader = context.service(SvcDownloader)

    # When downloading an ArXiv paper, first try to fetch the source LaTeX,
    # then fallback to the PDF if it is not available.
    src_url = WebUrl.decode(f"https://arxiv.org/src/{locator.paper_id}")
    pdf_url = WebUrl.decode(f"https://arxiv.org/pdf/{locator.paper_id}")
    try:
        response = await downloader.documents_read_download(src_url, None)
    except Exception:
        response = await downloader.documents_read_download(pdf_url, None)

    return ObserveResult(
        bundle=response.as_fragment(),
        metadata=MetadataDelta(name=response.name, mime_type=response.mime_type),
        should_cache=True,
        option_fields=True,
        option_relations_link=True,
    )


async def _public_read_youtube_body(
    context: KnowledgeContext,
    locator: YouTubeLocator,
) -> ObserveResult:
    downloader = context.service(SvcDownloader)

    content_url = locator.content_url()
    response = await downloader.documents_read_download(content_url, None)

    return ObserveResult(
        bundle=response.as_fragment(),
        metadata=MetadataDelta(name=response.name, mime_type=response.mime_type),
        should_cache=True,
        option_fields=True,
        option_relations_link=False,
    )
