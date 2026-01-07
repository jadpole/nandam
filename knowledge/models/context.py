from dataclasses import dataclass
from pydantic import BaseModel, Field

from base.api.documents import Fragment
from base.core.exceptions import UnavailableError
from base.models.context import NdContext
from base.resources.observation import ObservationBundle
from base.resources.relation import Relation_
from base.strings.auth import UserId, authorization_basic_credentials
from base.strings.resource import ExternalUri, Observable, Realm, ResourceUri

from knowledge.config import KnowledgeConfig
from knowledge.models.storage import (
    Locator,
    MetadataDelta,
    MetadataDelta_,
    ResourceView,
)


class ResolveResult(BaseModel, frozen=True):
    metadata: MetadataDelta_ = Field(default_factory=MetadataDelta)
    """
    The new metadata of the resource, when it changed since the last ingestion
    or when it is accessed for the first time and the connector can infer some
    metadata without relying on observations.
    """
    expired: list[Observable] = Field(default_factory=list)
    """
    The observations that expired since they were cached and that, therefore,
    should be read again using the connector.
    """
    should_cache: bool = False
    """
    Whether to cache the resource metadata (when no content is cached) and only
    define an 'alias' from the Resource URI to the Locator, re-running `resolve`
    each time the resource is accessed.
    """


class ObservedResult(BaseModel, frozen=True):
    observation: ObservationBundle | Fragment
    metadata: MetadataDelta_ = Field(default_factory=MetadataDelta)
    relations: list[Relation_] = Field(default_factory=list)
    should_cache: bool = False
    """
    Whether the ingested bundle should be cached, to avoid reading it again from
    the connector until it appears in `ResolveResult.expired`.
    """
    option_descriptions: bool = False
    """
    Whether to generate descriptions for `BundleBody` chunks and media.
    """
    option_relations_link: bool = False
    """
    Whether to generate "link" relations from `BundleBody` chunks.
    """
    option_relations_parent: bool = False
    """
    Whether to generate "parent" relations from `BundleCollection` results.
    """


@dataclass(kw_only=True)
class Connector:
    context: "KnowledgeContext"
    realm: Realm

    async def locator(self, reference: ResourceUri | ExternalUri) -> Locator | None:
        """
        Resolve a Reference into a Locator.

        When the connector is not responsible for the reference, return `None`,
        to push it to the next connector in the chain.

        Raise `UnavailableError` when the connector is in fact responsible for
        this resource, but it either does not exist, cannot be located, or we
        can already infer that the client is not allowed to view it.

        NOTE: Since this is not invoked when accessing a `ResourceUri` that is
        already cached in Storage, responsibility for access control ultimately
        rests with `Connector.resolve`.
        """
        raise NotImplementedError("Subclasses must implement Connector.locator")

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        """
        Must fulfill the following responsibilities:

        - Check that the client is allowed to access the resource, which is
          critical when reading observations from the cache.
        - Refresh the cached metadata that can be inferred without observations.
        - Infer which `affordances` are available.
        - Infer which observations `expired`.

        Return an empty `MetadataDelta` when the metadata is unchanged or cannot
        be inferred, e.g., for a public web page.  When the resource metadata is
        incomplete, an alias will be saved in Storage, allowing later ingestion
        using the resource URI.

        Raise `UnavailableError` when the resource does not exist or the client
        is not allowed to view it.

        NOTE: Also used to "resolve" the minimal information available for URIs,
        so the client can decide whether it is worth ingesting before investing
        the time required to do so.

        NOTE: Expensive metadata can be delegated to `Connector.observe`.
        """
        raise NotImplementedError("Subclasses must implement Connector.resolve")

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: ResourceView,
    ) -> ObservedResult:
        """
        Perform a (possibly expensive) observation of the resource.  Return the
        updated metadata (when useful), alongside the observation bundle and how
        it should be ingested (whether to cache, generate descriptions, etc.)
        The result processed by `domain.ingestion`.

        NOTE: The `resolved` contains the information from the cache, updated
        with the metadata from `Connector.resolve`.
        """
        raise NotImplementedError("Subclasses must implement Connector.observe")


class KnowledgeContext(NdContext):
    connectors: list[Connector]
    creds: dict[str, str]

    def user_id(self) -> UserId | None:
        return self.auth.user_id if self.auth else None

    def basic_authorization(
        self,
        realm: Realm,
        public_username: str | None,
        public_password: str | None,
    ) -> tuple[str, bool]:
        if authorization := self.get_basic_authorization(
            realm, public_username, public_password
        ):
            return authorization
        else:
            raise UnavailableError.new()

    def bearer_authorization(
        self,
        realm: Realm,
        public_token: str | None,
    ) -> tuple[str, bool]:
        if authorization := self.get_bearer_authorization(realm, public_token):
            return authorization
        else:
            raise UnavailableError.new()

    def get_basic_authorization(
        self,
        realm: Realm,
        public_username: str | None,
        public_password: str | None,
    ) -> tuple[str, bool] | None:
        if basic_header := self.creds.get(str(realm)):
            return basic_header, False
        elif (username := KnowledgeConfig.get(public_username)) and (
            password := KnowledgeConfig.get(public_password)
        ):
            return authorization_basic_credentials(username, password), True
        else:
            return None

    def get_bearer_authorization(
        self,
        realm: Realm,
        public_token: str | None,
    ) -> tuple[str, bool] | None:
        if private_token := self.creds.get(str(realm)):
            return f"Bearer {private_token}", False
        elif access_token := KnowledgeConfig.get(public_token):
            return f"Bearer {access_token}", True
        else:
            return None
