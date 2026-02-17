import logging

from typing import Literal

from base.core.exceptions import ErrorInfo
from base.models.content import ContentText
from base.resources.metadata import ResourceInfo
from base.strings.auth import BotId, UserId
from base.strings.thread import ThreadCursor, ThreadUri
from base.utils.markdown import markdown_from_msteams
from base.utils.sorted_list import bisect_insert

from backend.models.api_client import ClientAttachment
from backend.models.workspace_thread import (
    BotMessagePart,
    ThreadMessage_,
    ThreadMessageBot,
    ThreadMessageUser,
)
from backend.server.context import WorkspaceContext
from backend.services.threads import SvcThreads

logger = logging.getLogger(__name__)


async def list_messages(
    threads: SvcThreads,
    sources: list[ThreadUri | ThreadCursor],
    use_cache: bool = False,
) -> tuple[list[ThreadCursor], list[ThreadMessage_]]:
    new_cursors: list[ThreadCursor] = []
    new_messages: list[ThreadMessage_] = []

    for source in sources:
        messages = await _list_messages_once(threads, source, use_cache=use_cache)

        if messages:
            thread_uri = (
                source.thread_uri() if isinstance(source, ThreadCursor) else source
            )
            new_cursor = thread_uri.cursor(messages[-1].message_id)
            bisect_insert(new_cursors, new_cursor, key=str)

        for message in messages:
            bisect_insert(new_messages, message, key=lambda m: m.timestamp.timestamp())

    return new_cursors, new_messages


async def _list_messages_once(
    threads: SvcThreads,
    source: ThreadUri | ThreadCursor,
    use_cache: bool = False,
) -> list[ThreadMessage_]:
    uri = source.thread_uri() if isinstance(source, ThreadCursor) else source
    messages = await threads.load_messages(uri, use_cache=use_cache)
    if not isinstance(source, ThreadCursor) or not messages:
        return messages

    # Skip messages until the cursor is found, then return the remaining ones.
    # NOTE: The message matching the cursor is also discarded.
    filtered: list[ThreadMessage_] = []
    skip_until = source.last_message_id
    for message in messages:
        if skip_until:
            if message.message_id != skip_until:
                skip_until = None
            continue
        filtered.append(message)
    if not skip_until:
        return messages

    # When the cursor was not found, then return all messages with a greater ID
    # instead, since message IDs are time-ordered.  Should be unreachable unless
    # the thread history expired.
    logger.warning("Cursor not found in list_messages: %s", str(source))
    return [message for message in messages if message.message_id > skip_until]


async def save_bot_message(
    context: WorkspaceContext,
    thread_uri: ThreadUri,
    sender: BotId,
    content: list[BotMessagePart],
) -> None:
    threads = context.service(SvcThreads)
    message = ThreadMessageBot(sender=sender, content=content)
    await threads.push_message(thread_uri, message)


async def unsafe_save_user_message(
    threads: SvcThreads,
    sender: UserId,
    thread_uri: ThreadUri,
    message_format: Literal["markdown", "html"],
    message_text: str,
    attachments: list[ClientAttachment],
) -> None:
    """
    TODO: Use mentions.
    """
    _mentions: list[str] = []
    if message_format == "html":
        message_text, _mentions = markdown_from_msteams(message_text)

    attachment_infos, attachment_errors = await _upload_attachments(attachments)

    message = ThreadMessageUser(
        sender=sender,
        content=ContentText.parse(message_text),
        attachments=attachment_infos,
        attachment_errors=attachment_errors,
    )
    await threads.push_message(thread_uri, message)


async def _upload_attachments(
    attachments: list[ClientAttachment],
) -> tuple[list[ResourceInfo], dict[str, ErrorInfo]]:
    """
    TODO: Invoke Knowledge API and save to workspace.
    """
    attachment_infos: list[ResourceInfo] = []
    attachment_errors: dict[str, ErrorInfo] = {}

    for attachment in attachments:
        attachment_errors[attachment.name] = ErrorInfo.new(
            code=400,
            message="Attachments not supported",
        )

    return attachment_infos, attachment_errors
