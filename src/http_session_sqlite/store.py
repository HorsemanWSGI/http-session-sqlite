import time
import typing as t
from pathlib import Path
from http_session.meta import Store, SessionData
from cromlech.marshallers import Marshaller, PickleMarshaller
from sqlite3 import Connection, Row


table_statement = """CREATE TABLE IF NOT EXISTS {0}(
    sid VARCHAR(256),
    mtime INT,
    data BLOB,
    primary key (sid)
);"""


class SQLiteStore(Store):
    """File-based HTTP sessions store.
    """
    tablename: str = 'sessions'

    def __init__(self,
                 connection: Connection,
                 delta: int,
                 marshaller: t.Type[Marshaller] = PickleMarshaller):
        with connection:
            connection.execute(table_statement.format(self.tablename))
        connection.row_factory = Row
        self.connection = connection
        self.delta = delta  # timedelta in seconds.
        self.marshaller = marshaller

    def __iter__(self) -> t.Iterable[Path]:
        """Override to add a prefix or a namespace, if needed.
        """
        cursor = self.connection.cursor()
        for child in cursor.execute(f'select * from {self.tablename}'):
            yield child

    def delete_one(self, sid: str):
        with self.connection:
            self.connection.execute(
                f"DELETE FROM {self.tablename} WHERE sid=?;", (sid,))

    def get(self, sid: str) -> SessionData:
        cursor = self.connection.cursor()
        found = cursor.execute(
            f'select * from {self.tablename} WHERE sid=?',
            (sid,)
        ).fetchone()
        if found:
            epoch = time.time()
            if found['mtime'] + self.delta >= epoch:
                return self.marshaller.loads(found["data"])
            self.delete_one(sid)
        return self.new()

    def set(self, sid: str, data: t.Mapping) -> t.NoReturn:
        epoch = time.time()
        with self.connection:
            sdata = self.marshaller.dumps(data)
            self.connection.execute(
                f"INSERT INTO {self.tablename} (sid, mtime, data) "
                "VALUES(?, ?, ?) ON CONFLICT(sid) DO UPDATE SET "
                "mtime = ?, data=?;", (sid, epoch, sdata, epoch, sdata)
            )

    def touch(self, sid: str) -> t.NoReturn:
        epoch = time.time()
        concerned = epoch - self.delta
        with self.connection:
            self.connection.execute(
                f"UPDATE {self.tablename} SET mtime = ? "
                "WHERE mtime > ? AND sid = ?", (epoch, concerned, sid)
            )

    def clear(self, sid: str) -> t.NoReturn:
        self.delete_one(sid)

    delete = clear

    def flush_expired_sessions(self) -> t.NoReturn:
        concerned = time.time() - self.delta
        with self.connection:
            self.connection.execute(
                f"DELETE FROM {self.tablename} WHERE mtime < ? ;",
                (concerned,)
            )
