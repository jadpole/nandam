from datetime import datetime
from pydantic import BaseModel, Field
from typing import Annotated, Literal

from base.strings.data import MIME_TYPE_PLAIN, MimeType
from base.strings.resource import Observable, ObservableUri, RootReference_, WebUrl


##
## Attachment
##


class AttachmentBlob(BaseModel, frozen=True):
    type: Literal["blob"] = "blob"
    mime_type: MimeType
    blob: str


class AttachmentPlain(BaseModel, frozen=True):
    type: Literal["plain"] = "plain"
    mime_type: MimeType = MIME_TYPE_PLAIN
    text: str


class AttachmentUrl(BaseModel, frozen=True):
    type: Literal["url"] = "url"
    mime_type: MimeType | None = None
    expiry: datetime | None = None
    download_url: WebUrl


AnyAttachmentData = AttachmentBlob | AttachmentPlain | AttachmentUrl
AnyAttachmentData_ = Annotated[AnyAttachmentData, Field(discriminator="type")]


class ResourcesAttachmentAction(BaseModel, frozen=True):
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
    uri: RootReference_
    name: str | None = None
    description: str | None = None
    attachment: AnyAttachmentData_


##
## Resource
##


LoadMode = Literal["auto", "force", "none"]


class ResourcesLoadAction(BaseModel, frozen=True):
    """
    Return the resource metadata.  Refresh it when updated, and ingest it when
    it does not already exist.  The resulting `Resource` lists the capabilities
    supported by the resource, whose content may be read with "resources/read".

    When `expand_depth > 0`, related resources are also returned.
    Otherwise, other `expand_*` parameters are ignored.
    - Unless `expand_mode` is overridden, related items are only "resolved",
      i.e., their metadata is not refreshed.

    When `observe` is provided, the specified observations are also returned,
    when supported, along with embedded observations and referenced resources.
    """

    method: Literal["resources/load"] = "resources/load"
    uri: RootReference_
    expand_depth: int = 0
    expand_mode: LoadMode = "none"
    load_mode: LoadMode = "auto"
    observe: list[Observable] = Field(default_factory=list)


class ResourcesObserveAction(BaseModel, frozen=True):
    """
    Return the observation of corresponding to the URI (along with embeds).
    Cached contents are automatically refreshed when updated.
    """

    method: Literal["resources/observe"] = "resources/observe"
    uri: ObservableUri


def max_load_mode(a: LoadMode, b: LoadMode):
    if a == "force" or b == "force":
        return "force"
    elif a == "auto" or b == "auto":
        return "auto"
    else:
        return "none"


##
## Unions
##


QueryMethod = Literal[
    "resources/attachment",
    "resources/load",
    "resources/observe",
]
QueryAction = ResourcesAttachmentAction | ResourcesLoadAction | ResourcesObserveAction
QueryAction_ = Annotated[QueryAction, Field(discriminator="method")]

QueryReadAction = ResourcesLoadAction | ResourcesObserveAction
QueryReadAction_ = Annotated[QueryReadAction, Field(discriminator="method")]

QueryWriteAction = ResourcesAttachmentAction
