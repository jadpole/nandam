from datetime import datetime
from pydantic import BaseModel, Field
from typing import Annotated, Literal

from base.strings.data import DataUri, MimeType
from base.strings.resource import AffordanceUri, Observable, ResourceUri, WebUrl


class AttachmentFile(BaseModel):
    type: Literal["file"] = "file"
    expiry: datetime | None = None
    download_url: DataUri | WebUrl


class AttachmentPlain(BaseModel):
    type: Literal["plain"] = "plain"
    text: str


AnyAttachmentData = AttachmentFile | AttachmentPlain
AnyAttachmentData_ = Annotated[AnyAttachmentData, Field(discriminator="type")]


class ResourcesAttachmentAction(BaseModel):
    """
    Upload a file (binary or text) to Knowledge.

    NOTE: Attachments serve as a "default", when the connector cannot read the
    content of the affordance.  This allows services, notably Nandam, to upload
    files from a conversation (either user-provided or tool-generated), without
    allowing them to override the "real" resource.

    NOTE: An attached "file" is automatically ingested as "body" and/or "plain"
    when supported by its format.
    """

    method: Literal["resources/attachment"] = "resources/attachment"
    uri: ResourceUri | WebUrl
    name: str
    mime_type: MimeType | None
    description: str | None = None
    data: AnyAttachmentData_


LoadMode = Literal["auto", "force", "none"]


class ResourcesLoadAction(BaseModel):
    """
    Return the resource metadata.  Refresh it when updated, and ingest it when
    it does not already exist.  The resulting `Resource` lists the capabilities
    supported by the resource, whose content may be read with "resources/read".

    When `expand_depth > 0`, related resources are also returned.
    Otherwise, other `expand_*` parameters are ignored.
    - Unless `expand_mode` is overridden, related items are only "resolved",
      i.e., their metadata is not refreshed.

    When `observe` is provided, the specified observations are also returned,
    along with embedded observations and referenced resources.
    """

    method: Literal["resources/load"] = "resources/load"
    uri: ResourceUri | WebUrl
    expand_depth: int = 0
    expand_mode: LoadMode = "none"
    load_mode: LoadMode = "auto"
    observe: list[Observable] = Field(default_factory=list)


class ResourcesReadAction(BaseModel):
    """
    Return the content of the affordance (and its embeds).
    Cached contents are automatically refreshed when updated.
    """

    method: Literal["resources/read"] = "resources/read"
    uri: AffordanceUri


# TODO: Remove and migrate documentation to "resources/load".
# class ResourcesResolveAction(BaseModel):
#     """
#     When given a Web URL, resolve the corresponding Resource URI.
#     When given a Resource URI, validate that it is supported by a connector.
#
#     When the resource already exists, return any cached metadata.  However, new
#     resources are not automatically ingested.  Instead, it returns a placeholder
#     listing typical capabilities, then the resource may be ingested via either
#     "resources/load" or "resources/read".
#
#     This behaviour is useful when parsing an Event or a Document, where we wish
#     to replace Web URLs by the deduplicated Resource URI, but do not necessarily
#     wish to ingest the resource.
#     """
#
#     method: Literal["resources/resolve"] = "resources/resolve"
#     uri: ResourceUri | WebUrl


##
## Unions
##


QueryMethod = Literal[
    "resources/attachment",
    "resources/load",
    "resources/read",
    "resources/resolve",
]
QueryAction = ResourcesAttachmentAction | ResourcesLoadAction | ResourcesReadAction
QueryAction_ = Annotated[QueryAction, Field(discriminator="method")]
