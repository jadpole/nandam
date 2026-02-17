from pydantic import BaseModel, Field, TypeAdapter
from typing import Any

from base.core.values import as_value
from base.strings.auth import BotId
from base.strings.scope import Workspace
from base.strings.thread import ThreadCursor, ThreadUri
from base.utils.sorted_list import bisect_insert

from backend.models.bot_persona import AnyPersona, AnyPersona_

NUM_CHARS_BOT_SUFFIX = 6


class BotState(BaseModel):
    workspace: Workspace
    bot_id: BotId
    persona: AnyPersona_ | None
    llm_state: dict[str, Any] | None = None
    thread_cursors: list[ThreadCursor] = Field(default_factory=list)

    @staticmethod
    def new(
        workspace: Workspace,
        bot_id: BotId,
        persona: AnyPersona | None,
    ) -> BotState:
        return BotState(
            workspace=workspace,
            bot_id=bot_id,
            persona=persona,
            llm_state=None,
            thread_cursors=[],
        )

    def parse_state[S: BaseModel](self, type_: type[S]) -> S | None:
        return TypeAdapter(type_).validate_python(self.llm_state)

    def get_cursor(self, thread_uri: ThreadUri) -> ThreadCursor | None:
        for cursor in self.thread_cursors:
            if cursor.thread_uri() == thread_uri:
                return cursor
        return None

    def apply_update(
        self,
        *,
        persona: AnyPersona | None = None,
        llm_state: BaseModel | None = None,
        cursors: list[ThreadCursor] | None = None,
    ) -> None:
        if llm_state:
            self.llm_state = as_value(llm_state)
        if persona:
            self.persona = persona
        for cursor in cursors or []:
            bisect_insert(self.thread_cursors, cursor, key=str)
