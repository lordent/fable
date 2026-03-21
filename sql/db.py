from __future__ import annotations

import json
from contextvars import ContextVar, Token

import asyncpg
from asyncpg.transaction import Transaction

_sessions_ctx: ContextVar[dict[str, asyncpg.Connection] | None] = ContextVar(
    "sessions_ctx", default=None
)


def get_sessions() -> dict[str, asyncpg.Connection]:
    sessions = _sessions_ctx.get()
    return sessions if sessions is not None else {}


def get_session(app_name: str) -> asyncpg.Connection | None:
    return get_sessions().get(app_name)


class Config:
    __slots__ = ("dsn", "apps", "debug")

    def __init__(self, dsn: str, apps: list[str], debug: bool = False):
        self.dsn = dsn
        self.apps = apps
        self.debug = debug


class Engine:
    _active: Engine | None = None
    __slots__ = ("_configs",)

    def __init__(self, *configs: Config):
        self._configs = {app: cfg for cfg in configs for app in cfg.apps}
        Engine._active = self

    def is_debug(self, app_name: str) -> bool:
        cfg = self._configs.get(app_name)
        return cfg.debug if cfg else False

    @classmethod
    def get_active(cls) -> Engine:
        if cls._active is None:
            raise RuntimeError("Engine не инициализирован.")
        return cls._active

    async def get_connection(self, app_name: str) -> asyncpg.Connection:
        config = self._configs.get(app_name)
        if not config:
            raise RuntimeError(f"Конфигурация для приложения '{app_name}' не найдена.")
        conn: asyncpg.Connection = await asyncpg.connect(config.dsn)
        await conn.set_type_codec(
            "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )
        await conn.set_type_codec(
            "json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )
        return conn


class TransactionContext:
    __slots__ = ("app_name", "checkpoint", "conn", "transaction", "_token", "_is_owner")

    def __init__(self, app_name: str, checkpoint: bool = True):
        self.app_name = app_name
        self.checkpoint = checkpoint
        self.conn: asyncpg.Connection | None = None
        self.transaction: Transaction | None = None
        self._token: Token | None = None
        self._is_owner: bool = False

    async def __aenter__(self) -> asyncpg.Connection:
        current_sessions = get_sessions().copy()
        existing_conn = current_sessions.get(self.app_name)

        if existing_conn:
            self.conn = existing_conn
            if self.checkpoint:
                await self.conn.execute(f"SAVEPOINT sp_{id(self)}")
            return self.conn

        self.conn = await Engine.get_active().get_connection(self.app_name)

        self.transaction = self.conn.transaction()
        await self.transaction.start()

        current_sessions[self.app_name] = self.conn
        self._token = _sessions_ctx.set(current_sessions)
        self._is_owner = True
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                if self._is_owner and self.transaction:
                    await self.transaction.rollback()
                elif self.checkpoint and self.conn:
                    await self.conn.execute(f"ROLLBACK TO SAVEPOINT sp_{id(self)}")
            else:
                if self._is_owner and self.transaction:
                    await self.transaction.commit()
                elif self.checkpoint and self.conn:
                    await self.conn.execute(f"RELEASE SAVEPOINT sp_{id(self)}")
        finally:
            if self._is_owner:
                if self.conn:
                    await self.conn.close()
                if self._token:
                    _sessions_ctx.reset(self._token)


class ConnectionManager:
    def __init__(self, app_name: str):
        self.app_name = app_name
        self.conn = None
        self.is_internal = False

    async def __aenter__(self) -> asyncpg.Connection:
        self.conn = get_session(self.app_name)
        if self.conn:
            self.is_internal = False
            return self.conn

        self.is_internal = True
        self.conn = await Engine.get_active().get_connection(self.app_name)
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.is_internal and self.conn:
            await self.conn.close()
