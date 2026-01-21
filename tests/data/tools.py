from pydantic import BaseModel

from base.core.schema import as_jsonschema
from base.resources.aff_body import AffBody, AffBodyChunk, AffBodyMedia
from base.strings.resource import ObservableUri

from backend.llm.message import LlmTool


class GenerateImageArguments(BaseModel):
    prompt: str


class ReadDocsArguments(BaseModel):
    uri: list[
        ObservableUri[AffBody]
        | ObservableUri[AffBodyChunk]
        | ObservableUri[AffBodyMedia]
    ]


class WebSearchArguments(BaseModel):
    query: str


TOOL_GENERATE_IMAGE = LlmTool(
    name="generate_image",
    description="Generate an image matching a given prompt.",
    arguments_schema=as_jsonschema(GenerateImageArguments),
)

TOOL_READ_DOCS = LlmTool(
    name="read_docs",
    description="""\
Return the content of an observation from its URI. \
Also works for arbitrary media, returning a transcript or description when you \
are unable to view the original format.\
""",
    arguments_schema=as_jsonschema(ReadDocsArguments),
)

TOOL_WEB_SEARCH = LlmTool(
    name="web_search",
    description="Return the search results from the Web matching a given query.",
    arguments_schema=as_jsonschema(WebSearchArguments),
)
