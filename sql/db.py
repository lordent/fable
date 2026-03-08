from __future__ import annotations

from contextvars import ContextVar, Token

import asyncpg
from asyncpg.transaction import Transaction

_session_ctx: ContextVar[asyncpg.Connection | None] = ContextVar(
    "session_ctx", default=None
)


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
        return self._configs[app_name].debug

    @classmethod
    def get_active(cls) -> Engine:
        if cls._active is None:
            raise RuntimeError(
                "Engine не инициализирован. Создайте Engine([DBConfig(...)]) на старте."
            )
        return cls._active

    async def get_connection(self, app_name: str) -> asyncpg.Connection:
        config = self._configs.get(app_name)
        if not config:
            raise RuntimeError(f"Конфигурация для приложения '{app_name}' не найдена.")
        return await asyncpg.connect(config.dsn)


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
        existing_conn = _session_ctx.get()

        if existing_conn:
            self.conn = existing_conn
            if self.checkpoint:
                await self.conn.execute(f"SAVEPOINT sp_{id(self)}")
            return self.conn

        self.conn = await Engine.get_active().get_connection(self.app_name)
        self.transaction = self.conn.transaction()
        await self.transaction.start()

        self._token = _session_ctx.set(self.conn)
        self._is_owner = True
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                if self._is_owner and self.transaction:
                    await self.transaction.rollback()
                elif self.checkpoint:
                    await self.conn.execute(f"ROLLBACK TO SAVEPOINT sp_{id(self)}")
            else:
                if self._is_owner and self.transaction:
                    await self.transaction.commit()
                elif self.checkpoint:
                    await self.conn.execute(f"RELEASE SAVEPOINT sp_{id(self)}")
        finally:
            if self._is_owner:
                await self.conn.close()
                if self._token:
                    _session_ctx.reset(self._token)
