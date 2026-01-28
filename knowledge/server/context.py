from dataclasses import dataclass
from datetime import UTC, datetime
from pydantic import BaseModel, Field
from typing import Literal

from base.api.documents import Fragment
from base.api.knowledge import KnowledgeSettings
from base.core.exceptions import ServiceError, UnavailableError
from base.models.context import NdContext
from base.resources.relation import Relation_
from base.server.auth import NdAuth
from base.strings.auth import authorization_basic_credentials
from base.strings.resource import (
    KnowledgeUri,
    Observable,
    Realm,
    ResourceUri,
    RootReference,
)

from knowledge.config import KnowledgeConfig
from knowledge.models.storage_metadata import (
    Locator,
    MetadataDelta,
    MetadataDelta_,
    ResourceView,
)
from knowledge.models.storage_observed import AnyBundle_


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


class ObserveResult(BaseModel, frozen=True):
    bundle: AnyBundle_ | Fragment
    metadata: MetadataDelta_ = Field(default_factory=MetadataDelta)
    relations: list[Relation_] = Field(default_factory=list)
    should_cache: bool = False
    """
    Whether the ingested bundle should be cached, to avoid reading it again from
    the connector until it appears in `ResolveResult.expired`.
    """
    option_fields: bool = False
    """
    Whether to generate fields for `BundleBody` chunks and media.
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

    async def locator(self, reference: RootReference) -> Locator | None:
        """
        Resolve a Reference into a Locator.

        When the connector is not responsible for the reference, return `None`,
        to push it to the next connector in the chain.

        Raise `UnavailableError` when the connector should be responsible for
        this resource, but its Locator cannot be inferred (e.g., does not exist).

        NOTE: Not responsible for access validation: this responsibility rests
        with `Connector.resolve`.  This method is not invoked on URIs that are
        already cached in Storage.
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
        resolved: MetadataDelta,
    ) -> ObserveResult:
        """
        Perform a (possibly expensive) observation of the resource.  Return the
        updated metadata (when useful), alongside the observation bundle and how
        it should be ingested (whether to cache, generate descriptions, etc.)
        The result processed by `domain.ingestion`.

        NOTE: The `resolved` contains the information from the cache, updated
        with the metadata from `Connector.resolve`.
        """
        raise NotImplementedError("Subclasses must implement Connector.observe")


@dataclass(kw_only=True)
class KnowledgeContext(NdContext):
    auth: NdAuth
    connectors: list[Connector]
    creds: dict[str, str]
    prefix_rules: list[tuple[str, Literal["allow", "block"]]]
    timestamp: datetime

    @staticmethod
    def new(
        *,
        auth: NdAuth,
        request_timestamp: datetime | None,
        settings: KnowledgeSettings,
    ) -> "KnowledgeContext":
        request_timestamp = request_timestamp or datetime.now(UTC)
        return KnowledgeContext(
            auth=auth,
            caches=[],
            timestamp=request_timestamp,
            services=[],
            connectors=[],
            creds=settings.creds,
            prefix_rules=settings.prefix_rules,
        )

    ##
    ## Authorization
    ##

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

    ##
    ## Connectors
    ##

    def add_connector(self, connector: Connector) -> None:
        """
        NOTE: `Connector.locator` will be called in the order of registration to
        match it against a given URI.  Consider this when conflicts may occur.
        """
        existing = next((c.realm == connector.realm for c in self.connectors), None)
        if not existing:
            self.connectors.append(connector)
        else:
            raise ServiceError.bad_connector(
                connector.realm,
                f"already exists: {type(connector).__name__} -> {type(existing).__name__}",
            )

    def connector[T: Connector](self, type_: type[T]) -> T:
        for connector in self.connectors:
            if isinstance(connector, type_):
                return connector
        raise UnavailableError.new()

    def find_connector(self, locator: Locator | ResourceUri) -> Connector:
        for connector in self.connectors:
            if connector.realm == locator.realm:
                return connector
        raise UnavailableError.new()

    async def locator(self, uri: RootReference) -> tuple[Locator, Connector]:
        for connector in self.connectors:
            if locator := await connector.locator(uri):
                return locator, connector
        raise UnavailableError.new()

    ##
    ## Helper
    ##

    def should_backlink(self, uri: KnowledgeUri) -> bool:
        """
        TODO: Move logic into the connector, loaded from the connector config?
        """
        return uri.realm not in ("www,")
