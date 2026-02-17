"""
SvcThreads — Thread Persistence Service
========================================

Thin persistence layer over `SvcKVStore` for thread data, namespaced by the
workspace context.  Business logic lives in `backend.domain.threads`.

KV layout
---------
thread:info:{thread_uri}                ThreadInfo (JSON, via set_one / get)
thread:messages:{thread_uri}            LIST of ThreadMessage (via rpush / lrange)
thread:index:{workspace}                SET of ThreadUri strings (via sadd / smembers)
"""

import logging

from dataclasses import dataclass

from backend.models.exceptions import ThreadNotFoundError
from base.models.context import NdService
from base.strings.auth import ServiceId
from base.strings.scope import ScopeInternal, Workspace
from base.strings.thread import ThreadUri

from backend.models.workspace_thread import ThreadInfo, ThreadMessage_
from backend.services.kv_store import EXP_MONTH, SvcKVStore

logger = logging.getLogger(__name__)

SVC_THREADS = ServiceId.decode("svc-threads")

# fmt: off
KEY_THREAD_INFO = "thread:info:{thread_uri}"                # ThreadInfo
KEY_THREAD_MESSAGES = "thread:messages:{thread_uri}"        # LIST of ThreadMessage
KEY_THREAD_INDEX = "thread:index:{workspace}"               # SET of str(ThreadUri)
# fmt: on


@dataclass(kw_only=True)
class SvcThreads(NdService):
    """
    Low-level persistence for threads.

    Every method maps closely to a KV store operation.  Higher-level
    orchestration (create, add message, list) belongs in the domain layer.
    """

    service_id: ServiceId = SVC_THREADS
    kv_store: SvcKVStore
    workspace: Workspace
    _cache_info: dict[ThreadUri, ThreadInfo]
    _cache_messages: dict[ThreadUri, list[ThreadMessage_]]

    @staticmethod
    def initialize(workspace: Workspace, kv_store: SvcKVStore) -> SvcThreads:
        return SvcThreads(
            workspace=workspace,
            kv_store=kv_store,
            _cache_info={},
            _cache_messages={},
        )

    def assert_allowed(self, uri: ThreadUri) -> None:
        if not self.is_allowed(uri):
            raise ThreadNotFoundError.from_uri(uri)

    def is_allowed(self, uri: ThreadUri) -> bool:
        return uri.workspace == self.workspace or isinstance(
            uri.workspace.scope, ScopeInternal
        )

    async def list_threads(self) -> list[ThreadInfo]:
        uris = await self._list_thread_uris()
        results = await self.kv_store.mget(
            [KEY_THREAD_INFO.format(thread_uri=uri.as_kv_path()) for uri in uris],
            ThreadInfo,
        )
        for info in results:
            self._cache_info[info.uri] = info
        return results

    async def load_info(
        self,
        uri: ThreadUri,
        missing_ok: bool = True,
        use_cache: bool = False,
    ) -> ThreadInfo:
        self.assert_allowed(uri)
        if use_cache and uri in self._cache_info:
            return self._cache_info[uri]
        if info := await self._get_info(uri):
            return info

        # When no thread exists for the URI, create it.
        if missing_ok:
            info = ThreadInfo(uri=uri)
            await self._save_info(info)
            return info
        else:
            raise ThreadNotFoundError.from_uri(uri)

    async def load_messages(
        self,
        uri: ThreadUri,
        use_cache: bool = False,
    ) -> list[ThreadMessage_]:
        self.assert_allowed(uri)
        if use_cache and uri in self._cache_messages:
            return self._cache_messages[uri]

        key = KEY_THREAD_MESSAGES.format(thread_uri=uri.as_kv_path())
        results = await self.kv_store.lrange(key, 0, -1, ThreadMessage_)  # type: ignore
        self._cache_messages[uri] = results
        return results

    async def push_message(
        self,
        uri: ThreadUri,
        message: ThreadMessage_,
        missing_ok: bool = True,
        use_cache: bool = False,
    ) -> None:
        self.assert_allowed(uri)

        info = await self.load_info(uri, missing_ok=missing_ok, use_cache=use_cache)
        info.touch()

        key = KEY_THREAD_MESSAGES.format(thread_uri=uri.as_kv_path())
        await self.kv_store.rpush(key, message, ex=EXP_MONTH)
        await self._save_info(info)

        if use_cache and uri in self._cache_messages:
            self._cache_messages[uri].append(message)

    async def _get_info(self, uri: ThreadUri) -> ThreadInfo | None:
        key = KEY_THREAD_INFO.format(thread_uri=uri.as_kv_path())
        if info := await self.kv_store.get(key, ThreadInfo):
            self._cache_info[uri] = info
        return info

    async def _save_info(self, info: ThreadInfo) -> None:
        assert info.uri.workspace == self.workspace
        self._cache_info[info.uri] = info
        key_index = KEY_THREAD_INDEX.format(workspace=self.workspace.as_kv_path())
        key_info = KEY_THREAD_INFO.format(thread_uri=info.uri.as_kv_path())
        await self.kv_store.set_one(key_info, info, ex=EXP_MONTH)
        await self.kv_store.sadd(key_index, str(info.uri))
        await self.kv_store.expire(key_index, EXP_MONTH)

    async def _list_thread_uris(self) -> set[ThreadUri]:
        key = KEY_THREAD_INDEX.format(workspace=self.workspace.as_kv_path())
        return {
            uri
            for value in await self.kv_store.smembers(key, str)
            if (uri := ThreadUri.try_decode(value)) and uri.workspace == self.workspace
        }
