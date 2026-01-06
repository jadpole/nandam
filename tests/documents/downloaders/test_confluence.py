import pytest

from base.api.documents import DocOptions, HtmlOptions, TranscriptOptions
from base.strings.resource import WebUrl

from documents.downloaders.confluence import ConfluenceDownloader
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
## ConfluenceDownloader.match
##


def test_confluence_downloader_match_confluence_domain():
    downloader = ConfluenceDownloader(domain="wiki.example.com")
    url = WebUrl.decode("https://wiki.example.com/pages/viewpage.action?pageId=123")
    assert downloader.match(url) is True


def test_confluence_downloader_match_confluence_display_page():
    downloader = ConfluenceDownloader(domain="wiki.example.com")
    url = WebUrl.decode("https://wiki.example.com/display/SPACE/Page+Title")
    assert downloader.match(url) is True


def test_confluence_downloader_no_match_different_domain():
    downloader = ConfluenceDownloader(domain="wiki.example.com")
    url = WebUrl.decode("https://other.example.com/pages/viewpage.action?pageId=123")
    assert downloader.match(url) is False


def test_confluence_downloader_no_match_rest_api():
    """REST API endpoints should not match."""
    downloader = ConfluenceDownloader(domain="wiki.example.com")
    url = WebUrl.decode("https://wiki.example.com/rest/api/content/123")
    assert downloader.match(url) is False


def test_confluence_downloader_no_match_rest_wiki_api():
    downloader = ConfluenceDownloader(domain="wiki.example.com")
    url = WebUrl.decode("https://wiki.example.com/rest/wiki/content/123")
    assert downloader.match(url) is False


##
## ConfluenceDownloader.download_url - error cases
##


@pytest.mark.asyncio
async def test_confluence_downloader_rejects_original_mode():
    downloader = ConfluenceDownloader(domain="wiki.example.com")
    url = WebUrl.decode("https://wiki.example.com/pages/viewpage.action?pageId=123")
    options = _given_extract_options(original=True)

    with pytest.raises(DownloadError, match="original format"):
        await downloader.download_url(url, options, {}, authorization=None)


@pytest.mark.asyncio
async def test_confluence_downloader_requires_bearer_auth():
    downloader = ConfluenceDownloader(domain="wiki.example.com")
    url = WebUrl.decode("https://wiki.example.com/pages/viewpage.action?pageId=123")
    options = _given_extract_options()

    with pytest.raises(DownloadError, match="Bearer"):
        await downloader.download_url(url, options, {}, authorization=None)


@pytest.mark.asyncio
async def test_confluence_downloader_requires_bearer_prefix():
    downloader = ConfluenceDownloader(domain="wiki.example.com")
    url = WebUrl.decode("https://wiki.example.com/pages/viewpage.action?pageId=123")
    options = _given_extract_options()

    with pytest.raises(DownloadError, match="Bearer"):
        await downloader.download_url(url, options, {}, authorization="Basic xyz")
