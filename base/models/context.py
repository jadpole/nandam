from dataclasses import dataclass
from typing import Self

from base.core.exceptions import ServiceError
from base.strings.auth import ServiceId
from base.utils.sorted_list import bisect_find, bisect_insert


@dataclass(kw_only=True)
class NdCache:
    """
    Caches allow domain logic to store state for the duration of a request,
    e.g., the content of files read from storage.
    """

    @classmethod
    def initialize(cls) -> Self:
        return cls()


@dataclass(kw_only=True)
class NdService:
    """
    Services implement functionality that can be used in the domain logic,
    allowing stubs to be injected in unit tests.  For example:

    - Side-effects;
    - Remote APIs that are enabled by some workspaces;
    - Pre-loaded state;
    - Interactions with the client.
    """

    service_id: ServiceId


@dataclass(kw_only=True)
class NdContext:
    """
    The context shared by all processes spawned by a given request.
    Provides the necessary data, services, and caches used by the domain logic.
    """

    caches: list[NdCache]
    services: list[NdService]

    ##
    ## Caches
    ##

    def cached[C: NdCache](self, type_: type[C]) -> C:
        """
        Return a cache matching the given type.
        When it does not already exist, initialize it.
        """
        if cached := next((c for c in self.caches if isinstance(c, type_)), None):
            return cached
        else:
            cached = type_.initialize()
            bisect_insert(self.caches, cached, key=lambda c: type(c).__name__)
            return cached

    ##
    ## Services
    ##

    def add_service(self, service: NdService) -> None:
        if existing := bisect_find(
            self.services,
            service.service_id,
            key=lambda s: s.service_id,
        ):
            raise ServiceError.duplicate(
                name=str(service.service_id),
                type_before=type(existing),
                type_after=type(service),
            )
        else:
            bisect_insert(self.services, service, key=lambda s: s.service_id)

    def get_service[S: NdService](
        self,
        type_: type[S],
        service_id: ServiceId | None = None,
    ) -> S | None:
        try:
            return self.service(type_, service_id)
        except ServiceError:
            return None

    def service[S: NdService](
        self,
        type_: type[S],
        service_id: ServiceId | None = None,
    ) -> S:
        if service_id:
            svc = bisect_find(self.services, service_id, key=lambda s: s.service_id)
            if not svc:
                raise ServiceError.not_found(str(service_id), type_)
            if not isinstance(svc, type_):
                raise ServiceError.bad_type(str(service_id), type_, type(svc))
            return svc
        else:
            svc = next((svc for svc in self.services if isinstance(svc, type_)), None)
            if not svc:
                raise ServiceError.not_found(str(service_id), type_)
            return svc

    def services_implementing[I](self, interface: type[I]) -> list[I]:
        return [svc for svc in self.services if isinstance(svc, interface)]
