import pytest

from base.api.documents import DocOptions, HtmlOptions, TranscriptOptions
from base.strings.resource import WebUrl

from documents.downloaders.tableau import TableauDownloader, TableauViewLocator
from documents.models.exceptions import DownloadError
from documents.models.processing import ExtractOptions


def _given_extract_options(original: bool = False) -> ExtractOptions:
    return ExtractOptions(
        original=original,
        mime_type=None,
        doc=DocOptions(),
        html=HtmlOptions(),
        transcript=TranscriptOptions(),
    )


##
## TableauViewLocator.try_parse
##


def test_tableau_view_locator_parse_path():
    url = WebUrl.decode("https://tableau.example.com/views/MyWorkbook/MySheet")
    locator = TableauViewLocator.try_parse("tableau.example.com", url)

    assert locator is not None
    assert locator.domain == "tableau.example.com"
    assert locator.workbook == "MyWorkbook"
    assert locator.sheet == "MySheet"


def test_tableau_view_locator_parse_path_with_query():
    url = WebUrl.decode(
        "https://tableau.example.com/views/MyWorkbook/MySheet?:embed=y&:showAppBanner=false"
    )
    locator = TableauViewLocator.try_parse("tableau.example.com", url)

    assert locator is not None
    assert locator.workbook == "MyWorkbook"
    assert locator.sheet == "MySheet"


def test_tableau_view_locator_parse_fragment():
    url = WebUrl.decode("https://tableau.example.com/#/views/MyWorkbook/MySheet")
    locator = TableauViewLocator.try_parse("tableau.example.com", url)

    assert locator is not None
    assert locator.workbook == "MyWorkbook"
    assert locator.sheet == "MySheet"


def test_tableau_view_locator_parse_with_hyphen_underscore():
    url = WebUrl.decode("https://tableau.example.com/views/My-Work_book/My-Sheet_1")
    locator = TableauViewLocator.try_parse("tableau.example.com", url)

    assert locator is not None
    assert locator.workbook == "My-Work_book"
    assert locator.sheet == "My-Sheet_1"


def test_tableau_view_locator_no_match_different_domain():
    url = WebUrl.decode("https://other.example.com/views/MyWorkbook/MySheet")
    locator = TableauViewLocator.try_parse("tableau.example.com", url)

    assert locator is None


def test_tableau_view_locator_no_match_invalid_path():
    url = WebUrl.decode("https://tableau.example.com/other/path")
    locator = TableauViewLocator.try_parse("tableau.example.com", url)

    assert locator is None


def test_tableau_view_locator_no_match_home_page():
    url = WebUrl.decode("https://tableau.example.com/")
    locator = TableauViewLocator.try_parse("tableau.example.com", url)

    assert locator is None


##
## TableauDownloader.match
##


def test_tableau_downloader_match_view_path():
    downloader = TableauDownloader(domain="tableau.example.com")
    url = WebUrl.decode("https://tableau.example.com/views/MyWorkbook/MySheet")
    assert downloader.match(url) is True


def test_tableau_downloader_match_view_fragment():
    downloader = TableauDownloader(domain="tableau.example.com")
    url = WebUrl.decode("https://tableau.example.com/#/views/MyWorkbook/MySheet")
    assert downloader.match(url) is True


def test_tableau_downloader_no_match_different_domain():
    downloader = TableauDownloader(domain="tableau.example.com")
    url = WebUrl.decode("https://other.example.com/views/MyWorkbook/MySheet")
    assert downloader.match(url) is False


def test_tableau_downloader_no_match_non_view():
    downloader = TableauDownloader(domain="tableau.example.com")
    url = WebUrl.decode("https://tableau.example.com/users/admin")
    assert downloader.match(url) is False


##
## TableauDownloader.download_url - error cases
##


@pytest.mark.asyncio
async def test_tableau_downloader_rejects_original_mode():
    downloader = TableauDownloader(domain="tableau.example.com")
    url = WebUrl.decode("https://tableau.example.com/views/MyWorkbook/MySheet")
    options = _given_extract_options(original=True)

    with pytest.raises(DownloadError, match="original format"):
        await downloader.download_url(url, options, {}, authorization=None)


@pytest.mark.asyncio
async def test_tableau_downloader_requires_basic_auth():
    downloader = TableauDownloader(domain="tableau.example.com")
    url = WebUrl.decode("https://tableau.example.com/views/MyWorkbook/MySheet")
    options = _given_extract_options()

    with pytest.raises(DownloadError, match="Basic"):
        await downloader.download_url(url, options, {}, authorization=None)


@pytest.mark.asyncio
async def test_tableau_downloader_requires_basic_prefix():
    downloader = TableauDownloader(domain="tableau.example.com")
    url = WebUrl.decode("https://tableau.example.com/views/MyWorkbook/MySheet")
    options = _given_extract_options()

    with pytest.raises(DownloadError, match="Basic"):
        await downloader.download_url(url, options, {}, authorization="Bearer xyz")
