from .field import TextField, TimestampField
from .model import Model


class MigrationModel(Model):
    _table = "_migrations"
    app = TextField(pk=True)
    name = TextField(pk=True)
    applied_at = TimestampField(auto_now=True)


class Operation:
    def up(self, conn):
        raise NotImplementedError

    def down(self, conn):
        raise NotImplementedError


class RunSQL(Operation):
    def __init__(self, sql: str):
        self.sql = sql

    async def up(self, conn):
        await conn.execute(self.sql)


class Migration:
    app: str
    name: str
    dependencies: list[tuple[str, str]] = []
    operations: list[Operation] = []
