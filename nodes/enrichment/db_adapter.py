"""Database adapters for enrichment lookups.

Provides a uniform interface for executing parameterized queries against
SQLite (stdlib), PostgreSQL (psycopg2), and MySQL (pymysql). Each adapter
returns rows as dicts so callers never depend on driver-specific row types.

External drivers are imported lazily — the module is always importable even
when only the stdlib sqlite3 backend is needed.
"""

from __future__ import annotations

import re
import sqlite3
from abc import ABC, abstractmethod
from typing import Any

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def is_valid_identifier(name: str) -> bool:
    """Return True when *name* is a safe SQL identifier."""
    return bool(_IDENT_RE.match(name))


# ---------------------------------------------------------------------------
# Abstract adapter
# ---------------------------------------------------------------------------


class DatabaseAdapter(ABC):
    """Minimal query interface used by BatchLookup."""

    @abstractmethod
    def execute(self, query: str, params: tuple) -> list[dict[str, Any]]:
        """Run a parameterized SELECT and return rows as dicts."""
        ...

    @abstractmethod
    def validate(self) -> None:
        """Check that the connection is usable.  Raises on failure.

        Called at build time so misconfigured enrichments fail before
        the first message hits the pipeline.
        """
        ...

    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """Return True when *table* exists in the target database."""
        ...

    @abstractmethod
    def column_names(self, table: str) -> list[str]:
        """Return column names for *table* (empty list on error)."""
        ...


# ---------------------------------------------------------------------------
# SQLite (stdlib — always available)
# ---------------------------------------------------------------------------


class SqliteAdapter(DatabaseAdapter):
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def execute(self, query: str, params: tuple) -> list[dict[str, Any]]:
        with self._get_conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def validate(self) -> None:
        with self._get_conn() as conn:
            conn.execute("SELECT 1")

    def table_exists(self, table: str) -> bool:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
        return row is not None

    def column_names(self, table: str) -> list[str]:
        with self._get_conn() as conn:
            rows = conn.execute(f"PRAGMA table_info({_quote_ident(table)})").fetchall()
        return [r["name"] for r in rows]


def _quote_ident(name: str) -> str:
    """Double-quote an SQLite identifier."""
    return '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# PostgreSQL  (lazy psycopg2 import)
# ---------------------------------------------------------------------------

class PostgresAdapter(DatabaseAdapter):
    def __init__(self, connection_string: str):
        self.conn_string = connection_string
        self._conn: Any = None

    def _get_conn(self):
        import psycopg2
        import psycopg2.extras

        if self._conn is None or getattr(self._conn, "closed", False):
            self._conn = psycopg2.connect(self.conn_string)
        return self._conn

    def execute(self, query: str, params: tuple) -> list[dict[str, Any]]:
        import psycopg2.extras

        conn = self._get_conn()
        pg_query = query.replace("?", "%s")
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(pg_query, params)
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    def validate(self) -> None:
        import psycopg2

        conn = psycopg2.connect(self.conn_string)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        finally:
            conn.close()

    def table_exists(self, table: str) -> bool:
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name=%s",
                (table,),
            )
            return cur.fetchone() is not None

    def column_names(self, table: str) -> list[str]:
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name=%s ORDER BY ordinal_position",
                (table,),
            )
            return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# MySQL  (lazy pymysql import)
# ---------------------------------------------------------------------------

class MySQLAdapter(DatabaseAdapter):
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._conn: Any = None

    def _get_conn(self):
        import pymysql

        if self._conn is None or not getattr(self._conn, "open", False):
            self._conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
            )
        return self._conn

    def execute(self, query: str, params: tuple) -> list[dict[str, Any]]:
        import pymysql.cursors

        conn = self._get_conn()
        my_query = query.replace("?", "%s")
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(my_query, params)
            rows = cur.fetchall()
        return [dict(r) for r in rows]

    def validate(self) -> None:
        import pymysql

        conn = pymysql.connect(
            host=self.host, port=self.port, user=self.user,
            password=self.password, database=self.database,
            charset="utf8mb4",
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        finally:
            conn.close()

    def table_exists(self, table: str) -> bool:
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name=%s AND table_schema=%s",
                (table, self.database),
            )
            return cur.fetchone() is not None

    def column_names(self, table: str) -> list[str]:
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name=%s AND table_schema=%s "
                "ORDER BY ordinal_position",
                (table, self.database),
            )
            return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def build_adapter(
    db_type: str,
    *,
    db_path: str | None = None,
    connection_string: str | None = None,
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
    database: str | None = None,
) -> DatabaseAdapter:
    """Factory: return the right DatabaseAdapter for *db_type*."""
    t = (db_type or "sqlite").lower()
    if t == "sqlite":
        if not db_path:
            raise ValueError("sqlite adapter requires db_path")
        return SqliteAdapter(db_path)
    if t in ("postgresql", "postgres"):
        if connection_string:
            return PostgresAdapter(connection_string)
        raise ValueError("postgresql adapter requires connection_string")
    if t == "mysql":
        if host and port and user and password and database:
            return MySQLAdapter(host, port, user, password, database)
        raise ValueError(
            "mysql adapter requires (host, port, user, password, database)"
        )
    raise ValueError(f"unsupported db_type: {db_type!r}")


# map db_type -> driver package name for helpful error messages
DRIVER_NAMES = {
    "postgresql": "psycopg2",
    "postgres": "psycopg2",
    "mysql": "pymysql",
}
