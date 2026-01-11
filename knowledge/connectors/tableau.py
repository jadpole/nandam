import re

from dataclasses import dataclass
from pydantic import BaseModel, Field
from typing import Literal

from base.resources.aff_body import AffBody
from base.core.exceptions import BadRequestError, UnavailableError
from base.resources.metadata import AffordanceInfo
from base.strings.data import MimeType
from base.strings.file import FileName
from base.strings.resource import (
    ExternalUri,
    Observable,
    Realm,
    ResourceUri,
    RootReference,
    WebUrl,
)

from knowledge.services.downloader import SvcDownloader
from knowledge.models.context import Connector, KnowledgeContext
from knowledge.models.context import Locator, ObserveResult, ResolveResult
from knowledge.models.storage import MetadataDelta, ResourceView


REGEX_TABLEAU_VIEW = r"/views/([A-Za-z0-9_\-]+)/([A-Za-z0-9_\-]+)(?:\?.+)?"


##
## Config
##


class TableauConnectorConfig(BaseModel):
    kind: Literal["tableau"] = "tableau"
    realm: Realm
    domain: str
    public_username: str | None = None
    public_password: str | None = None
    public_reports: dict[str, list[str]] = Field(default_factory=dict)
    """
    A mapping { group -> [workbook] } of reports that can be accessed using the
    default credentials.  Useful for service accounts with too many permissions.
    """

    def instantiate(self, context: KnowledgeContext) -> "TableauConnector":
        return TableauConnector(
            context=context,
            realm=self.realm,
            domain=self.domain,
            public_username=self.public_username,
            public_password=self.public_password,
            public_reports=self.public_reports,
        )


##
## Locators
##


class TableauViewLocator(Locator, frozen=True):
    kind: Literal["tableau_view"] = "tableau_view"
    realm: Realm
    domain: str
    workbook: FileName
    sheet: FileName

    @staticmethod
    def from_web(domain: str, url: WebUrl, realm: Realm) -> "TableauViewLocator | None":
        """
        Match URLs of the form:
        - https://{domain}/#/views/{workbook}/{sheet}
        - https://{domain}/views/{workbook}/{sheet}
        """
        if url.domain != domain:
            return None

        match = None
        if url.fragment:
            match = re.fullmatch(REGEX_TABLEAU_VIEW, url.fragment)
        if not match:
            match = re.fullmatch(REGEX_TABLEAU_VIEW, f"/{url.path}")
        if not match:
            return None

        workbook = FileName.decode(match.group(1))
        sheet = FileName.decode(match.group(2))
        return TableauViewLocator(
            realm=realm,
            domain=domain,
            workbook=workbook,
            sheet=sheet,
        )

    @staticmethod
    def from_uri(domain: str, uri: ResourceUri) -> "TableauViewLocator | None":
        if uri.subrealm != "view":
            return None
        if len(uri.path) != 2:  # noqa: PLR2004
            return None
        return TableauViewLocator(
            realm=uri.realm,
            domain=domain,
            workbook=uri.path[0],
            sheet=uri.path[1],
        )

    def resource_uri(self) -> ResourceUri:
        return ResourceUri(
            realm=self.realm,
            subrealm=FileName.decode("view"),
            path=[self.workbook, self.sheet],
        )

    def content_url(self) -> WebUrl:
        return WebUrl.decode(
            f"https://{self.domain}/views/{self.workbook}/{self.sheet}"
        )

    def citation_url(self) -> WebUrl:
        return self.content_url()


AnyTableauLocator = TableauViewLocator


##
## Connector
##


@dataclass(kw_only=True)
class TableauConnector(Connector):
    domain: str
    public_username: str | None
    public_password: str | None
    public_reports: dict[str, list[str]]

    async def locator(self, reference: RootReference) -> Locator | None:
        if isinstance(reference, WebUrl):
            if reference.domain != self.domain:
                return None

            locator = TableauViewLocator.from_web(self.domain, reference, self.realm)
            if not locator:
                raise UnavailableError.new()

            return locator

        elif isinstance(reference, ExternalUri):
            return None

        else:
            if reference.realm != self.realm:
                return None

            locator = TableauViewLocator.from_uri(self.domain, reference)
            if not locator:
                raise UnavailableError.new()

            return locator

    async def resolve(
        self,
        locator: Locator,
        cached: ResourceView | None,
    ) -> ResolveResult:
        """
        NOTE: Tableau views are never cached, so the client always receives an
        up-to-date rendered report.
        """
        assert isinstance(locator, AnyTableauLocator)

        _, is_public = self._get_authorization()
        if is_public:
            self._assert_public_report(locator)

        match locator:
            case TableauViewLocator():
                metadata = MetadataDelta(
                    name=f"{locator.workbook} / {locator.sheet}",
                    mime_type=MimeType.decode("image/png"),
                    affordances=[AffordanceInfo(suffix=AffBody.new())],
                )

        return ResolveResult(metadata=metadata, should_cache=True)

    async def observe(
        self,
        locator: Locator,
        observable: Observable,
        resolved: MetadataDelta,
    ) -> ObserveResult:
        assert isinstance(locator, AnyTableauLocator)

        authorization, is_public = self._get_authorization()
        if is_public:
            self._assert_public_report(locator)

        match locator, observable:
            case (TableauViewLocator(), AffBody()):
                return await _tableau_read_view_body(
                    self.context, authorization, locator
                )
            case _:
                raise BadRequestError.observable(observable.as_suffix())

    def _get_authorization(self) -> tuple[str, bool]:
        return self.context.basic_authorization(
            self.realm, self.public_username, self.public_password
        )

    def _assert_public_report(self, locator: TableauViewLocator) -> None:
        if self.public_reports and not any(
            locator.workbook in workbooks for workbooks in self.public_reports.values()
        ):
            raise UnavailableError.new()


async def _tableau_read_view_body(
    context: KnowledgeContext,
    authorization: str,
    locator: "TableauViewLocator",
) -> ObserveResult:
    downloader = context.service(SvcDownloader)
    response = await downloader.documents_read_download(
        url=locator.content_url(),
        authorization=authorization,
        original=False,
    )

    return ObserveResult(
        bundle=response.as_fragment(),
        metadata=MetadataDelta(
            name=f"{locator.workbook} / {locator.sheet}",
            mime_type=response.mime_type,
        ),
        should_cache=False,
        option_descriptions=True,
        option_relations_link=False,
    )
