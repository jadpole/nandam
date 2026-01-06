import bs4

from markdownify import MarkdownConverter
from typing import Any

from base.api.documents import FragmentUri
from base.strings.data import MIME_TYPE_PLAIN, DataUri, MimeType
from base.strings.resource import WebUrl

from documents.config import DocumentsConfig
from documents.extractors.image import download_image_as_data_uri
from documents.models.exceptions import ExtractError
from documents.models.pending import Downloaded, Extracted
from documents.models.processing import ExtractOptions, Extractor

ROOT_SELECTORS = {
    "open.spotify.com": "main > .GlueDropTarget",
}


class HtmlPageExtractor(Extractor):
    def match(self, downloaded: Downloaded, options: ExtractOptions) -> bool:
        return bool(
            not options.original
            and (
                downloaded.mime_type == "text/html"
                or (downloaded.filename and downloaded.filename.ext() == ".html")
            )
        )

    async def extract(
        self,
        downloaded: Downloaded,
        options: ExtractOptions,
        user_id: str | None,
    ) -> Extracted:
        try:
            # If we have well-formed HTML, extract the page content.
            # Otherwise, return the text file as-is (probably code or Markdown).
            page_html = await downloaded.read_text_async()
            soup = bs4.BeautifulSoup(page_html, "lxml")
            if not soup.find():
                return Extracted(
                    mode="plain",
                    name=None,
                    path=None,
                    mime_type=downloaded.mime_type or MIME_TYPE_PLAIN,
                    blobs={},
                    text=page_html.strip(),
                )

            root_selector: str | None = None
            if options.html.root_selector:
                root_selector = options.html.root_selector
            elif downloaded.url and downloaded.url.domain in ROOT_SELECTORS:
                root_selector = ROOT_SELECTORS[downloaded.url.domain]
            elif (
                downloaded.url
                and downloaded.url.domain in DocumentsConfig.domains.confluence
            ):
                root_selector = "#main-content"

            title = parse_title(soup)
            content, blobs = await parse_soup(downloaded.url, soup, root_selector)

            return Extracted(
                mode="markdown",
                name=title,
                path=None,
                mime_type=MimeType("text/html"),
                blobs=blobs,
                text=content,
            )
        except ExtractError:
            raise
        except Exception as exc:
            raise ExtractError.fail("html", str(exc)) from exc


def parse_title(soup: bs4.BeautifulSoup) -> str | None:
    """
    Extract the first <title> from the page.
    """
    for element in soup.find_all("title"):
        title = " ".join(element.stripped_strings)
        element.decompose()
        return title
    return None


async def parse_soup(  # noqa: C901, PLR0912, PLR0915
    url: WebUrl | None,
    soup: bs4.BeautifulSoup,
    root_selector: str | None,
) -> tuple[str, dict[FragmentUri, DataUri]]:
    base_url: WebUrl | None = url
    if (
        (base_tag := soup.select_one("base"))
        and (base_href := base_tag.get("href"))
        and isinstance(base_href, str)
    ):
        inferred_url = WebUrl.try_decode(base_href) or (
            base_url and (inferred_url := base_url.try_join_href(base_href))
        )
        if inferred_url:
            base_url = inferred_url

    root: bs4.BeautifulSoup | bs4.Tag = soup
    if (
        root_selector
        and (root_tag := soup.select_one(root_selector))
        and isinstance(root_tag, bs4.Tag)
    ):
        root = root_tag

    # Remove the page scaffolding, to avoid confusing the LLM.
    if isinstance(root, bs4.BeautifulSoup):
        stripped_tags = ["footer", "header", "nav", "script", "style", "svg"]
        stripped_attrs = []
        for tag in stripped_tags:
            for element in root.find_all(tag):
                element.decompose()
        for attrs in stripped_attrs:
            for element in root.find_all(None, attrs):
                element.decompose()

    stripped_filters: list[dict[str, Any]] = [
        # Ignore non-primary content roles.
        {"role": "banner"},
        {"role": "navigation"},
        {"role": "search"},
        # Ignore analytics and tracking.
        {"data-analytics": True},
        {"data-tracking": True},
        # Ignore Bootstrap hidden elements.
        {"class_": ["d-none"]},
        {"class_": ["d-print-none"]},
        # Ignore Common UI hidden elements.
        {"class_": ["ui-helper-hidden"]},
        {"class_": ["ui-hidden"]},
        # Ignore ReadSpeaker text that should not be read aloud.
        {"class_": ["rs_do_not_process"]},
        {"class_": ["rs_skip_always"]},
        {"class_": ["rs_skip"]},
    ]
    for stripped_filter in stripped_filters:
        for element in root.find_all(**stripped_filter):
            element.decompose()

    # Clear empty tags.
    stripped_empty_tags = ["a", "h1", "h2", "h3", "h4", "h5", "h6"]
    for tag in stripped_empty_tags:
        for element in root.find_all(tag):
            text = " ".join(element.stripped_strings).strip()
            if not text:
                element.decompose()

    # Add spacing around headings.
    for element in root.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
        element.insert_before("\n")

    # Extract URLs.
    for element in root.find_all("a"):
        assert isinstance(element, bs4.Tag)

        # We drop the title attribute, since the text content should provide
        # enough context to the LLM.
        if element.get("title"):
            del element.attrs["title"]

        if (link_href := element.get("href")) and isinstance(link_href, str):
            if normalized_url := _normalize_href(base_url, link_href):
                element.attrs["href"] = str(normalized_url)
            else:
                del element.attrs["href"]

    # Extract images.
    images: dict[FragmentUri, DataUri] = {}
    for element in root.find_all("img"):
        assert isinstance(element, bs4.Tag)

        if (
            (image_src := element.get("src"))
            and isinstance(image_src, str)
            and (normalized_url := _normalize_href(base_url, image_src))
            and (image_data := await download_image_as_data_uri(normalized_url))
            and (image_name := normalized_url.guess_filename(image_data.mime_type()))
        ):
            # Rendered as `![maybe alt text](self://filename) in the Markdown.
            image_uri = FragmentUri.new(image_name.filepath())
            element.attrs["src"] = str(image_uri)
            images[image_uri] = image_data
        else:
            # Images that cannot be downloaded are just discarded
            element.decompose()

    # Spotify: render aria grid as table.
    if url and url.domain == "open.spotify.com":
        for element in root.find_all("div", {"role": "grid"}):
            assert isinstance(element, bs4.Tag)
            element.insert_before(soup.new_string("\n"))
        for element in root.find_all("div", {"role": "row"}):
            assert isinstance(element, bs4.Tag)
            element.insert_before(soup.new_string("| "))
            element.append(soup.new_string("\n"))
        for element in root.find_all("div", {"role": "columnheader"}):
            assert isinstance(element, bs4.Tag)
            element.append(soup.new_string(" | "))
        for element in root.find_all("div", {"role": "gridcell"}):
            assert isinstance(element, bs4.Tag)
            element.append(soup.new_string(" | "))

    # Render Mermaid diagrams (library processes ".mermaid" elements).
    for element in root.find_all("div", {"class": "mermaid"}):
        assert isinstance(element, bs4.Tag)
        element.insert_before(soup.new_string("```mermaid"))
        element.append(soup.new_string("\n```\n\n"))

    try:
        content = MarkdownConverter(heading_style="ATX").convert_soup(root)
        return content, images
    except Exception as exc:
        error = f"Cannot translate HTML to Markdown: {exc}"
        raise ExtractError.fail("html", error) from exc


##
## Normalize URLs
##


def _normalize_href(base_url: WebUrl | None, link_href: str) -> WebUrl | None:
    try:
        if link_href.startswith("https://"):
            return WebUrl.try_decode(link_href)  # Keep full URLs as-is.
        elif link_href.startswith("#"):
            return None  # Discard anchors in the same page.
        elif base_url:
            return base_url.try_join_href(link_href)
    except ValueError:
        pass  # ignore URLs that cannot be parsed

    # Discard links that cannot be turned into URLs, either because the result
    # is invalid, or because we do not have enough information (no `page_url`).
    return None
