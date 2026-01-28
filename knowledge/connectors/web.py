from dataclasses import dataclass
from typing import Literal

from base.resources.aff_body import AffBody
from base.core.exceptions import BadRequestError, UnavailableError
from base.core.unique_id import unique_id_from_str
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

REALM_WWW = Realm.decode("www")

NUM_CHARS_WWW_ID = 40


class WebLocator(Locator, frozen=True):
    kind: Literal["web"] = "web"
    realm: Realm = REALM_WWW
    url: WebUrl

    @staticmethod
    def from_web(url: WebUrl) -> "WebLocator":
        """
        Clean the web URL to deduplicate among equivalent ones, hence they have
        """
        return WebLocator(url=url)

    def resource_uri(self) -> ResourceUri:
        domain = self.url.domain.removeprefix("www.")
        hashed_url = unique_id_from_str(
            str(self.url.model_copy(update={"domain": domain})),
            num_chars=NUM_CHARS_WWW_ID,
            salt="knowledge-www",
        )
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.normalize(domain),
            path=[FileName.decode(hashed_url)],
        )

    def content_url(self) -> WebUrl:
        return self.url

    def citation_url(self) -> WebUrl:
        return self.content_url()


@dataclass(kw_only=True)
class WebConnector(Connector):
    realm: Realm = REALM_WWW

    async def locator(self, reference: RootReference) -> Locator | None:
        """
        Web URLs always resolve, defaulting to `WebLocator` when no other
        connector matches.  However, we never infer a locator from resource
        URIs in the "www" realm, since the hash is not reversible.
        """
        if isinstance(reference, WebUrl):
            return WebLocator.from_web(reference)
        elif isinstance(reference, ExternalUri):
            return None
        else:
            raise UnavailableError.new()

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        """
        We never load the metadata for generic web resources, but instead, let
        the domain logic create a placeholder and ingest it when a client reads
        its content for the first time.
        """
        assert isinstance(locator, WebLocator)

        # For web resources, always use the cached metadata when available.
        if cached:
            return ResolveResult()

        # Otherwise, simply return the list of supported affordances.
        # NOTE: We never "expire" web resources, since there is no way to tell
        # whether they changed and they are almost never cached anyway.
        return ResolveResult(
            metadata=MetadataDelta(
                affordances=[AffordanceInfo(suffix=AffBody.new())],
            ),
        )

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, WebLocator)
        match locator, observable:
            case (WebLocator(), AffBody()):
                return await _web_read_body(self.context, locator)
            case _:
                raise BadRequestError.observable(observable.as_suffix())


##
## Read
##


async def _web_read_body(
    context: KnowledgeContext,
    locator: WebLocator,
) -> ObserveResult:
    """
    Download and parse the requested URL via the Documents service.

    NOTE: Cache when the file is expensive to parse and unlikely to change.
    NOTE: Never record relations from the Web, to avoid accumulating useless
    backlinks or leaking signed URLs.
    """
    downloader = context.service(SvcDownloader)
    response = await downloader.documents_read_download(locator.content_url(), None)

    return ObserveResult(
        bundle=response.as_fragment(),
        metadata=MetadataDelta(
            name=response.name,
            mime_type=response.mime_type,
        ),
        should_cache=response.mime_type.mode() in ("document", "media"),
        option_fields=True,
        option_relations_link=False,
    )
