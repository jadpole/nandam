import asyncio
import re

from dataclasses import dataclass
from pydantic import BaseModel
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_views_dataframe
from typing import TYPE_CHECKING

from base.core.exceptions import ApiError
from base.strings.auth import parse_basic_credentials
from base.strings.data import MimeType
from base.strings.resource import WebUrl

from documents.config import DocumentsConfig
from documents.models.exceptions import DownloadError
from documents.models.pending import Downloaded, DownloadedData
from documents.models.processing import Downloader, ExtractOptions

if TYPE_CHECKING:
    import requests
    import pandas as pd


REGEX_TABLEAU_VIEW = r"/views/([A-Za-z0-9_\-]+)/([A-Za-z0-9_\-]+)(?:\?.+)?"


@dataclass(kw_only=True)
class TableauDownloader(Downloader):
    domain: str

    def match(self, url: WebUrl) -> bool:
        return TableauViewLocator.try_parse(self.domain, url) is not None

    async def download_url(
        self,
        url: WebUrl,
        options: ExtractOptions,
        headers: dict[str, str],
        authorization: str | None,
    ) -> Downloaded:
        locator = TableauViewLocator.try_parse(self.domain, url)
        assert locator

        if options.original:
            raise DownloadError(
                "Bad Request: cannot read Tableau in original format",
                code=400,
            )

        if authorization and (basic_creds := parse_basic_credentials(authorization)):
            username, password = basic_creds
        else:
            raise DownloadError.unauthorized("Basic")

        try:
            name, image_data = await asyncio.to_thread(
                _download_tableau_sync,
                self.domain,
                username,
                password,
                locator,
            )
            return DownloadedData(
                url=url,
                response_headers={},
                name=name,
                mime_type=MimeType.decode("image/png"),
                filename=None,
                charset=None,
                data=image_data,
            )
        except ApiError:
            raise
        except Exception as exc:
            raise DownloadError.unexpected("Tableau view cannot be downloaded") from exc


def _download_tableau_sync(
    domain: str,
    username: str,
    password: str,
    locator: "TableauViewLocator",
) -> tuple[str, bytes]:
    conn = TableauServerConnection(
        config_json={
            "tableauserver": {
                "server": f"https://{domain}",
                "api_version": "3.21",
                "username": username,
                "password": password,
                "site_name": "",
                "site_url": "",
            },
        },
        env="tableauserver",
        ssl_verify=(
            DocumentsConfig.is_kubernetes()
            and domain not in DocumentsConfig.ssl.disabled
        ),
    )
    try:
        auth_response = conn.sign_in()
        auth_response.raise_for_status()
    except Exception as exc:
        raise DownloadError.forbidden(f"Tableau sign in failed: {exc}") from exc

    # Find the view in Tableau Server
    views_df: pd.DataFrame = get_views_dataframe(conn)
    view = views_df[
        (
            views_df["contentUrl"].str.contains(
                f"{locator.workbook}/sheets/{locator.sheet}"
            )
        )
    ]
    if view.empty:
        raise DownloadError(
            f"Not Found: Tableau view {locator.workbook}/{locator.sheet}",
            code=404,
        )

    # Download the image representation of that views
    view_id = view["id"].iloc[0]  # type: ignore[attr-defined]
    workbook_human_name = view["workbook"].iloc[0]["name"]  # type: ignore[attr-defined]
    view_human_name = view["name"].iloc[0]  # type: ignore[attr-defined]

    image_req: requests.Response = conn.query_view_image(view_id=view_id)  # type: ignore
    image_req.raise_for_status()

    return f"{workbook_human_name} / {view_human_name}", image_req.content


##
## Locator
##


class TableauViewLocator(BaseModel):
    domain: str
    workbook: str
    sheet: str

    @staticmethod
    def try_parse(domain: str, url: WebUrl) -> "TableauViewLocator | None":
        if url.domain != domain:
            return None

        if match := re.fullmatch(REGEX_TABLEAU_VIEW, url.fragment):
            workbook, sheet = match.groups()
            return TableauViewLocator(domain=domain, workbook=workbook, sheet=sheet)

        if match := re.fullmatch(REGEX_TABLEAU_VIEW, f"/{url.path}"):
            workbook, sheet = match.groups()
            return TableauViewLocator(domain=domain, workbook=workbook, sheet=sheet)

        return None
