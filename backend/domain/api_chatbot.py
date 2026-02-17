import logging

from typing import Any

from base.strings.auth import BotId
from base.strings.scope import Workspace
from base.strings.thread import ThreadCursor

from backend.models.bot_persona import AnyPersona, ChatPersona
from backend.models.bot_state import BotState
from backend.services.kv_store import EXP_WEEK, SvcKVStore

logger = logging.getLogger(__name__)

# fmt: off
KEY_BOT_STATE = "bot:state:{workspace}:{bot_id}"             # LlmProxyState
# fmt: on


##
## Settings
## ---
## TODO: Update default persona in workspace / conversation.
##


async def bot_acquire(
    kv_store: SvcKVStore,
    workspace: Workspace,
    bot_id: BotId,
) -> BotState:
    key_bot = KEY_BOT_STATE.format(workspace=workspace.as_kv_path(), bot_id=bot_id)
    if bot := await kv_store.get(key_bot, BotState):
        return bot
    else:
        bot = BotState.new(workspace, bot_id, None)
        await kv_store.set_one(key_bot, bot, ex=EXP_WEEK)
        return bot


async def bot_save_update(
    kv_store: SvcKVStore,
    workspace: Workspace,
    bot_id: BotId,
    *,
    persona: AnyPersona | None = None,
    llm_state: Any | None = None,
    cursors: list[ThreadCursor] | None = None,
) -> BotState:
    key_bot = KEY_BOT_STATE.format(workspace=workspace.as_kv_path(), bot_id=bot_id)
    bot = await kv_store.get(key_bot, BotState) or BotState.new(workspace, bot_id, None)
    bot.apply_update(persona=persona, llm_state=llm_state, cursors=cursors)
    await kv_store.set_one(key_bot, bot, ex=EXP_WEEK)
    return bot


async def bot_default_persona(kv_store: SvcKVStore, workspace: Workspace) -> AnyPersona:  # noqa: ARG001
    return ChatPersona(
        system_message="""\
""",
        model="claude-opus",
        temperature=1.0,
        resources=[],
        capabilities=[],
    )
