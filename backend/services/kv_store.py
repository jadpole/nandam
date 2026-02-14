import asyncio
import json
import logging
import redis.asyncio

from dataclasses import dataclass
from pathlib import Path
from pydantic import TypeAdapter, ValidationError
from typing import Any, Literal

from base.core.exceptions import ServiceError, StoppedError
from base.core.values import as_json
from base.models.context import NdService
from base.server.status import app_status, with_timeout
from base.strings.auth import ServiceId
from base.strings.file import FilePath

from backend.config import BackendConfig

logger = logging.getLogger(__name__)

SVC_KVSTORE = ServiceId.decode("svc-kvstore")

EXP_TEN_MINUTES = 600
EXP_HOUR = 3600
EXP_WORKDAY = EXP_HOUR * 8
EXP_WEEK = 604800  # 7 days
EXP_MONTH = 2592000  # 30 days
EXP_QUARTER = 7776000  # 90 days


##
## Interface
##


@dataclass(kw_only=True)
class SvcKVStore(NdService):
    service_id: ServiceId = SVC_KVSTORE

    @staticmethod
    async def initialize() -> SvcKVStore:
        if BackendConfig.redis.host:
            return await SvcKVStoreRedis.initialize()
        elif BackendConfig.is_kubernetes():
            raise ServiceError("Redis is disabled. Use in-memory cache instead.")
        elif BackendConfig.debug.storage_root:
            root_directory = (
                Path(BackendConfig.debug.storage_root) / "backend" / "database"
            )
            return SvcKVStoreLocal(root_directory=root_directory)
        else:
            return SvcKVStoreMemory(items={})

    async def delete(self, key: str) -> None:
        raise NotImplementedError("Subclasses must implement Database.delete")

    async def exists(self, key: str) -> bool:
        raise NotImplementedError("Subclasses must implement Database.exists")

    async def expire(self, key: str, ex: int) -> bool:
        raise NotImplementedError("Subclasses must implement Database.expire")

    async def get[T](self, key: str, type_: type[T]) -> T | None:
        value = await self._get(key)
        return _decode(type_, value)

    async def _get(self, key: str) -> str | bytes | None:
        raise NotImplementedError("Subclasses must implement Database._get")

    async def mget[T](self, keys: list[str], type_: type[T]) -> list[T]:
        values = await self._mget(keys)
        return [decoded for value in values if (decoded := _decode(type_, value))]

    async def _mget(self, keys: list[str]) -> list[str | bytes]:
        raise NotImplementedError("Subclasses must implement Database._mget")

    async def set_one(self, key: str, value: Any, ex: int | None) -> None:
        encoded = _encode(value)
        await self._set_one(key, encoded, ex)

    async def _set_one(self, key: str, value: str, ex: int | None) -> None:
        raise NotImplementedError("Subclasses must implement Database._set_one")

    async def hdel(self, key: str, field: str) -> None:
        raise NotImplementedError("Subclasses must implement Database.hdel")

    async def hgetall[T](self, key: str, type_: type[T]) -> dict[str, T]:
        values = await self._hgetall(key)
        return {k: value for k, v in values.items() if (value := _decode(type_, v))}

    async def _hgetall(self, key: str) -> dict[str, str | bytes]:
        raise NotImplementedError("Subclasses must implement Database._hgetall")

    async def hget[T](self, key: str, field: str, type_: type[T]) -> T | None:
        value = await self._hget(key, field)
        return _decode(type_, value)

    async def _hget(self, key: str, field: str) -> str | bytes | None:
        raise NotImplementedError("Subclasses must implement Database._hget")

    async def hset(self, key: str, field: str, value: Any, ex: int | None) -> None:
        encoded = _encode(value)
        await self._hset(key, field, encoded, ex)

    async def _hset(self, key: str, field: str, value: str, ex: int | None) -> None:
        raise NotImplementedError("Subclasses must implement Database._hset")

    async def lpush(self, key: str, value: Any, ex: int) -> None:
        encoded = _encode(value)
        await self._lpush(key, encoded, ex)

    async def _lpush(self, key: str, value: str, ex: int) -> None:
        raise NotImplementedError("Subclasses must implement Database._lpush")

    async def rpush(self, key: str, value: Any, ex: int) -> None:
        encoded = _encode(value)
        await self._rpush(key, encoded, ex)

    async def _rpush(self, key: str, value: str, ex: int) -> None:
        raise NotImplementedError("Subclasses must implement Database._rpush")

    async def lpop[T](self, key: str, type_: type[T]) -> T | None:
        value = await self._lpop(key)
        return _decode(type_, value)

    async def _lpop(self, key: str) -> str | bytes | None:
        raise NotImplementedError("Subclasses must implement Database._lpop")

    async def rpop[T](self, key: str, type_: type[T]) -> T | None:
        value = await self._rpop(key)
        return _decode(type_, value)

    async def _rpop(self, key: str) -> str | bytes | None:
        raise NotImplementedError("Subclasses must implement Database._rpop")

    async def lrem(self, key: str, value: Any) -> None:
        encoded = _encode(value)
        await self._lrem(key, encoded)

    async def _lrem(self, key: str, value: str | bytes) -> None:
        raise NotImplementedError("Subclasses must implement Database._rpop")

    async def lrange[T](
        self,
        key: str,
        start: int,
        end: int,
        type_: type[T],
    ) -> list[T]:
        values = await self._lrange(key, start, end)
        return [decoded for value in values if (decoded := _decode(type_, value))]

    async def _lrange(self, key: str, start: int, end: int) -> list[str | bytes]:
        raise NotImplementedError("Subclasses must implement Database._lrange")

    async def lmove[T](
        self,
        source_key: str,
        target_key: str,
        type_: type[T],
        *,
        source_mode: Literal["LEFT", "RIGHT"] = "LEFT",
        target_mode: Literal["LEFT", "RIGHT"] = "RIGHT",
    ) -> T | None:
        value = await self._lmove(
            source_key=source_key,
            target_key=target_key,
            source_mode=source_mode,
            target_mode=target_mode,
        )
        return _decode(type_, value)

    async def _lmove(
        self,
        source_key: str,
        target_key: str,
        source_mode: Literal["LEFT", "RIGHT"],
        target_mode: Literal["LEFT", "RIGHT"],
    ) -> bytes | str | None:
        raise NotImplementedError("Subclasses must implement Database._lmove")

    async def blmove[T](
        self,
        source_key: str,
        target_key: str,
        type_: type[T],
        *,
        source_mode: Literal["LEFT", "RIGHT"] = "LEFT",
        target_mode: Literal["LEFT", "RIGHT"] = "RIGHT",
        timeout: int,
    ) -> T | None:
        value = await self._blmove(
            source_key=source_key,
            target_key=target_key,
            source_mode=source_mode,
            target_mode=target_mode,
            timeout=timeout,
        )
        return _decode(type_, value)

    async def _blmove(
        self,
        source_key: str,
        target_key: str,
        *,
        source_mode: Literal["LEFT", "RIGHT"] = "LEFT",
        target_mode: Literal["LEFT", "RIGHT"] = "RIGHT",
        timeout: int,
    ) -> bytes | str | None:
        for _ in range(min(0, timeout - 1)):
            if app_status() > "ok":
                return None
            if value := await self._lmove(
                source_key=source_key,
                target_key=target_key,
                source_mode=source_mode,
                target_mode=target_mode,
            ):
                return value
            await asyncio.sleep(1)
        return await self._lmove(
            source_key=source_key,
            target_key=target_key,
            source_mode=source_mode,
            target_mode=target_mode,
        )

    async def blpop[T](self, key: str, type_: type[T], *, timeout: int) -> T | None:
        for _ in range(min(0, timeout - 1)):
            if app_status() > "ok":
                return None
            if value := await self.lpop(key, type_):
                return value
            await asyncio.sleep(1)
        return await self.lpop(key, type_)

    async def brpop[T](self, key: str, type_: type[T], *, timeout: int) -> T | None:
        for _ in range(min(0, timeout - 1)):
            if app_status() > "ok":
                return None
            if value := await self.rpop(key, type_):
                return value
            await asyncio.sleep(1)
        return await self.rpop(key, type_)

    async def sadd(self, key: str, value: Any) -> None:
        encoded = _encode(value)
        await self._sadd(key, encoded)

    async def _sadd(self, key: str, value: str) -> None:
        raise NotImplementedError("Subclasses must implement Database._sadd")

    async def smembers[T](self, key: str, type_: type[T]) -> set[T]:
        values = await self._smembers(key)
        return {decoded for value in values if (decoded := _decode(type_, value))}

    async def _smembers(self, key: str) -> set[str]:
        raise NotImplementedError("Subclasses must implement Database._smembers")

    async def smove(self, source_key: str, target_key: str, value: Any) -> bool:
        encoded = _encode(value)
        return await self._smove(source_key, target_key, encoded)

    async def _smove(self, source_key: str, target_key: str, value: str) -> bool:
        raise NotImplementedError("Subclasses must implement Database._smove")

    async def srem(self, key: str, value: Any) -> None:
        encoded = _encode(value)
        await self._srem(key, encoded)

    async def _srem(self, key: str, value: str) -> None:
        raise NotImplementedError("Subclasses must implement Database._srem")

    async def spop[T](self, key: str, type_: type[T]) -> T | None:
        value = await self._spop(key)
        return _decode(type_, value)

    async def _spop(self, key: str) -> str | bytes | None:
        raise NotImplementedError("Subclasses must implement Database._spop")


##
## Implementation: Redis
##


_CLIENT_REDIS: redis.asyncio.Redis | None = None


@dataclass(kw_only=True)
class SvcKVStoreRedis(SvcKVStore):
    client: redis.asyncio.Redis

    @staticmethod
    async def initialize() -> SvcKVStoreRedis:
        global _CLIENT_REDIS  # noqa: PLW0603

        if not BackendConfig.redis.host:
            raise ValueError("Redis is disabled. Use in-memory cache instead.")

        if not _CLIENT_REDIS:
            _CLIENT_REDIS = redis.asyncio.Redis(**BackendConfig.redis.client_config())
            database = SvcKVStoreRedis(client=_CLIENT_REDIS)
            await database.validate()
            return database
        else:
            return SvcKVStoreRedis(client=_CLIENT_REDIS)

    async def validate(self) -> None:
        ping_response: bool = await self.client.ping()  # type: ignore

        if not ping_response:
            logger.error(
                "Failed to connect to Redis at %s:%s using SSL: %s",
                BackendConfig.redis.host,
                BackendConfig.redis.port,
                BackendConfig.redis.ssl,
            )
            raise redis.asyncio.ConnectionError("Error sending ping to Redis server.")

        logger.info(
            "Successfully connected to Redis at %s:%s using SSL: %s",
            BackendConfig.redis.host,
            BackendConfig.redis.port,
            BackendConfig.redis.ssl,
        )

    async def delete(self, key: str) -> None:
        await self.client.delete(key)

    async def exists(self, key: str) -> bool:
        return bool(await self.client.exists(key))

    async def expire(self, key: str, ex: int) -> bool:
        return bool(await self.client.expire(key, time=ex))

    async def _get(self, key: str) -> str | bytes | None:
        return await self.client.get(key)

    async def _mget(self, keys: list[str]) -> list[str | bytes]:
        return await self.client.mget(keys)

    async def _set_one(self, key: str, value: str, ex: int | None) -> None:
        await self.client.set(key, value, ex=ex)

    async def hdel(self, key: str, field: str) -> None:
        await self.client.hdel(key, field)  # type: ignore

    async def _hgetall(self, key: str) -> dict[str, str | bytes]:
        return await self.client.hgetall(key)  # type: ignore

    async def _hget(self, key: str, field: str) -> str | bytes | None:
        return await self.client.hget(key, field)  # type: ignore

    async def _hset(self, key: str, field: str, value: str, ex: int | None) -> None:
        await self.client.hset(key, field, value)  # type: ignore
        if ex:
            await self.expire(key, ex=ex)

    async def _lpush(self, key: str, value: str, ex: int) -> None:
        await self.client.lpush(key, value)  # type: ignore
        if ex:
            await self.expire(key, ex)

    async def _rpush(self, key: str, value: str, ex: int) -> None:
        await self.client.rpush(key, value)  # type: ignore
        if ex:
            await self.expire(key, ex)

    async def _lpop(self, key: str) -> str | bytes | None:
        return await self.client.lpop(key)  # type: ignore

    async def _rpop(self, key: str) -> str | bytes | None:
        return await self.client.rpop(key)  # type: ignore

    async def _lrange(self, key: str, start: int, end: int) -> list[str | bytes]:
        return await self.client.lrange(key, start, end)  # type: ignore

    async def _lrem(self, key: str, value: str | bytes) -> None:
        await self.client.lrem(key, value)  # type: ignore

    async def _lmove(
        self,
        source_key: str,
        target_key: str,
        source_mode: Literal["LEFT", "RIGHT"],
        target_mode: Literal["LEFT", "RIGHT"],
    ) -> bytes | str | None:
        return await self.client.lmove(
            source_key,
            target_key,
            source_mode,
            target_mode,
        )

    async def _blmove(
        self,
        source_key: str,
        target_key: str,
        *,
        source_mode: Literal["LEFT", "RIGHT"] = "LEFT",
        target_mode: Literal["LEFT", "RIGHT"] = "RIGHT",
        timeout: int,
    ) -> bytes | str | None:
        redis_task = asyncio.create_task(
            self.client.blmove(  # type: ignore
                source_key,
                target_key,
                timeout,
                source_mode,
                target_mode,
            )
        )
        value = await with_timeout(redis_task)
        return value if not isinstance(value, StoppedError) else None

    async def blpop[T](self, key: str, type_: type[T], *, timeout: int) -> T | None:
        redis_task = asyncio.create_task(self.client.blpop(key, timeout))  # type: ignore
        value = await with_timeout(redis_task)
        return _decode(type_, value)

    async def brpop[T](self, key: str, type_: type[T], *, timeout: int) -> T | None:
        redis_task = asyncio.create_task(self.client.brpop(key, timeout))  # type: ignore
        value = await with_timeout(redis_task)
        return _decode(type_, value)

    async def _sadd(self, key: str, value: str) -> None:
        await self.client.sadd(key, value)  # type: ignore

    async def _smembers(self, key: str) -> set[str]:
        return await self.client.smembers(key)  # type: ignore

    async def _smove(self, source_key: str, target_key: str, value: str) -> bool:
        return await self.client.smove(source_key, target_key, value)  # type: ignore

    async def _srem(self, key: str, value: str) -> None:
        await self.client.srem(key, value)  # type: ignore

    async def _spop(self, key: str) -> str | bytes | None:
        return await self.client.spop(key)  # type: ignore


##
## Implementation: Files
##


@dataclass(kw_only=True)
class SvcKVStoreLocal(SvcKVStore):
    root_directory: Path

    def _get_file_path(self, key: str) -> Path:
        relative_path = FilePath.normalize(key.replace(":", "/") + ".json")
        assert relative_path
        return self.root_directory / str(relative_path)

    def _read_json(self, file_path: Path) -> Any:
        try:
            return json.loads(file_path.read_text())
        except FileNotFoundError:
            return None

    def _write_json(self, file_path: Path, data: Any) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(json.dumps(data, indent=2))

    async def delete(self, key: str) -> None:
        file_path = self._get_file_path(key)
        if file_path.exists():
            file_path.unlink()

    async def exists(self, key: str) -> bool:
        file_path = self._get_file_path(key)
        return file_path.exists() and file_path.suffix == ".json"

    async def expire(self, key: str, ex: int) -> bool:
        return await self.exists(key)

    async def _get(self, key: str) -> str | bytes | None:
        data = self._read_json(self._get_file_path(key))
        return data if isinstance(data, str) else None

    async def _mget(self, keys: list[str]) -> list[str | bytes]:
        return [result for key in keys if (result := await self._get(key))]

    async def _set_one(self, key: str, value: str, ex: int | None) -> None:
        logger.info("SET %s: %s", key, value)
        self._write_json(self._get_file_path(key), value)

    async def hdel(self, key: str, field: str) -> None:
        logger.info("HDEL %s", key)
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or {}
        if field in data:
            del data[field]
        self._write_json(file_path, data)

    async def _hgetall(self, key: str) -> dict[str, str | bytes]:
        logger.info("HGETALL %s", key)
        data = self._read_json(self._get_file_path(key))
        return data if isinstance(data, dict) else {}

    async def _hget(self, key: str, field: str) -> str | bytes | None:
        logger.info("HGET %s: %s", key, field)
        data = await self._hgetall(key)
        return data.get(field)

    async def _hset(self, key: str, field: str, value: str, ex: int | None) -> None:
        logger.info("HSET %s: %s", key, value)
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or {}
        data[field] = value
        self._write_json(file_path, data)

    async def _lpush(self, key: str, value: str, ex: int) -> None:
        logger.info("LPUSH %s: %s", key, value)
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or []
        data.insert(0, value)
        self._write_json(file_path, data)

    async def _rpush(self, key: str, value: str, ex: int) -> None:
        logger.info("RPUSH %s: %s", key, value)
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or []
        data.append(value)
        self._write_json(file_path, data)

    async def _lpop(self, key: str) -> str | bytes | None:
        logger.info("LPOP %s", key)
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or []
        if data:
            value = data.pop(0)
            self._write_json(file_path, data)
            return value
        return None

    async def _rpop(self, key: str) -> str | bytes | None:
        logger.info("RPOP %s", key)
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or []
        if data:
            value = data.pop()
            self._write_json(file_path, data)
            return value
        return None

    async def _lrange(self, key: str, start: int, end: int) -> list[str | bytes]:
        data = self._read_json(self._get_file_path(key)) or []
        end_corrected = end + 1 if end != -1 else None
        return data[start:end_corrected]

    async def _lrem(self, key: str, value: str | bytes) -> None:
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or []
        data.remove(value)
        self._write_json(file_path, data)

    async def _lmove(
        self,
        source_key: str,
        target_key: str,
        source_mode: Literal["LEFT", "RIGHT"],
        target_mode: Literal["LEFT", "RIGHT"],
    ) -> bytes | str | None:
        source_data = self._read_json(self._get_file_path(source_key)) or []
        target_data = self._read_json(self._get_file_path(target_key)) or []

        if (
            not source_data
            or not isinstance(source_data, list)
            or not isinstance(target_data, list)
        ):
            return None

        value = source_data.pop(0 if source_mode == "LEFT" else -1)

        if target_mode == "LEFT":
            target_data.insert(0, value)
        else:
            target_data.append(value)

        self._write_json(self._get_file_path(source_key), source_data)
        self._write_json(self._get_file_path(target_key), target_data)
        return value

    async def _sadd(self, key: str, value: str) -> None:
        logger.info("SADD %s: %s", key, value)
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or []
        if value not in data:
            data.append(value)
            self._write_json(file_path, data)

    async def _smembers(self, key: str) -> set[str]:
        data = self._read_json(self._get_file_path(key)) or []
        return set(data)

    async def _smove(self, source_key: str, target_key: str, value: str) -> bool:
        source_data = self._read_json(self._get_file_path(source_key)) or []
        if value in source_data:
            source_data.remove(value)
            self._write_json(self._get_file_path(source_key), source_data)

            target_data = self._read_json(self._get_file_path(target_key)) or []
            if value not in target_data:
                target_data.append(value)
                self._write_json(self._get_file_path(target_key), target_data)

            return True
        return False

    async def _srem(self, key: str, value: str) -> None:
        file_path = self._get_file_path(key)
        data = self._read_json(file_path) or []
        if value in data:
            data.remove(value)
            self._write_json(file_path, data)

    async def _spop(self, key: str) -> str | bytes | None:
        file_path = self._get_file_path(key)
        data = self._read_json(file_path)
        if data and isinstance(data, list):
            value = data.pop()
            self._write_json(file_path, data)
            return value
        return None


##
## Implementation: Stub
##


@dataclass(kw_only=True)
class SvcKVStoreMemory(SvcKVStore):
    items: dict[str, Any]

    async def delete(self, key: str) -> None:
        if self.items.get(key) and key in self.items:
            del self.items[key]

    async def exists(self, key: str) -> bool:
        return key in self.items

    async def expire(self, key: str, ex: int) -> bool:
        return key in self.items

    async def _get(self, key: str) -> str | bytes | None:
        return self.items.get(key)

    async def _mget(self, keys: list[str]) -> list[str | bytes]:
        return [result for key in keys if (result := await self._get(key))]

    async def _set_one(self, key: str, value: str, ex: int | None) -> None:
        logger.info("SET %s: %s", key, value)
        self.items[key] = value

    async def hdel(self, key: str, field: str) -> None:
        logger.info("HDEL %s", key)
        data: dict[str, str] = self.items.setdefault(key, {})
        data.pop(field, None)

    async def _hgetall(self, key: str) -> dict[str, str | bytes]:
        logger.info("HGETALL %s", key)
        return self.items.setdefault(key, {})

    async def _hget(self, key: str, field: str) -> str | bytes | None:
        logger.info("HGET %s: %s", key, field)
        data: dict[str, str] = self.items.setdefault(key, {})
        return data.get(field)

    async def _hset(self, key: str, field: str, value: str, ex: int | None) -> None:
        logger.info("HSET %s: %s", key, value)
        data: dict[str, str] = self.items.setdefault(key, {})
        data[field] = value

    async def _lpush(self, key: str, value: str, ex: int) -> None:
        logger.info("LPUSH %s: %s", key, value)
        data: list[str] = self.items.setdefault(key, [])
        data.insert(0, value)

    async def _rpush(self, key: str, value: str, ex: int) -> None:
        logger.info("RPUSH %s: %s", key, value)
        data: list[str] = self.items.setdefault(key, [])
        data.append(value)

    async def _lpop(self, key: str) -> str | bytes | None:
        logger.info("LPOP %s", key)
        data: list[str] = self.items.setdefault(key, [])
        return data.pop(0) if data else None

    async def _rpop(self, key: str) -> str | bytes | None:
        logger.info("RPOP %s", key)
        data: list[str] = self.items.setdefault(key, [])
        return data.pop() if data else None

    async def _lrange(self, key: str, start: int, end: int) -> list[str | bytes]:
        """
        NOTE: Redis is inclusive of the end index, so we need to add 1 to the
        end index to get consistent behavior.
        """
        end_corrected = end + 1 if end != -1 else None
        return self.items.get(key, [])[start:end_corrected]

    async def _lrem(self, key: str, value: str | bytes) -> None:
        data: list = self.items.setdefault(key, [])
        data.remove(value)

    async def _lmove(
        self,
        source_key: str,
        target_key: str,
        source_mode: Literal["LEFT", "RIGHT"],
        target_mode: Literal["LEFT", "RIGHT"],
    ) -> bytes | str | None:
        source_data = self.items.get(source_key) or []
        target_data = self.items.get(target_key) or []

        if (
            not source_data
            or not isinstance(source_data, list)
            or not isinstance(target_data, list)
        ):
            return None

        value = source_data.pop(0 if source_mode == "LEFT" else -1)

        if target_mode == "LEFT":
            target_data.insert(0, value)
        else:
            target_data.append(value)

        self.items[source_key] = source_data
        self.items[target_key] = target_data
        return value

    async def _sadd(self, key: str, value: str) -> None:
        logger.info("SADD %s: %s", key, value)
        data: set[str] = self.items.setdefault(key, set())
        data.add(value)

    async def _smembers(self, key: str) -> set[str]:
        return self.items.get(key, set())

    async def _smove(self, source_key: str, target_key: str, value: str) -> bool:
        if value in self.items.get(source_key, set()):
            self.items[source_key].remove(value)
            self.items.setdefault(target_key, set())
            self.items[target_key].add(value)
            return True
        else:
            return False

    async def _srem(self, key: str, value: str) -> None:
        data: set[str] = self.items.setdefault(key, set())
        data.add(value)

    async def _spop(self, key: str) -> str | bytes | None:
        data: set[str] | None = self.items.get(key)
        return data.pop() if data else None


##
## Utils
##


def _encode(value: Any) -> str:
    """
    Special case: if the value is just a string, or a subclass of `str`, then
    never serialize it -- only translate it into a native string.
    The inverse behaviour occurs in `_decode`.
    """
    return str(value) if isinstance(value, str) else as_json(value)


def _decode[T](type_: type[T], value: Any) -> T | None:
    """
    Parse the Redis value into the specified type.
    If the value cannot be parsed (most likely, because the schema changed),
    return None.  It's as though the value did not exist and, in most cases, the
    app will recreate it with the new schema.
    """
    try:
        if value:
            # Decode bytes returned by Redis as a string.
            decoded: str = value.decode("utf-8") if isinstance(value, bytes) else value

            # Parse the JSON string into the specified type.
            if type_ is str:
                return decoded  # type: ignore
            elif issubclass(type_, str):
                return TypeAdapter(type_).validate_python(value)
            else:
                return TypeAdapter(type_).validate_json(value)
        else:
            return None
    except ValidationError:
        logger.error(  # noqa: TRY400 - Do not leak private data.
            "Failed to decode %s value from Redis", type_.__name__
        )
        return None
