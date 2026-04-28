"""sizes_store.py — SQLite CRUD for size catalogs.

One DB with two tables, `cardboard` and `others`, both with the same shape:
    id, name, width_mm, length_mm, width_in, length_in

Operators edit sizes via NiceGUI; callers pick the table by name. mm values
are integers (manufacturing tolerance), inch values are floats.

Usage:
    sizes = SizesStore.from_config(cfg.sizes)
    sizes.start()
    sid = sizes.add("cardboard", Size(name="A4", width_mm=210, length_mm=297,
                                      width_in=8.27, length_in=11.69))
    s = sizes.get("cardboard", sid)
    sizes.list("others")
    sizes.stop()

Concurrency: single connection, internal lock. Writes are operator-driven
and rare — no writer thread needed.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass, asdict
from pathlib import Path

log = logging.getLogger(__name__)

TABLES = ("cardboard", "others")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cardboard (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    width_mm   INTEGER NOT NULL,
    length_mm  INTEGER NOT NULL,
    width_in   REAL    NOT NULL,
    length_in  REAL    NOT NULL,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS others (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    width_mm   INTEGER NOT NULL,
    length_mm  INTEGER NOT NULL,
    width_in   REAL    NOT NULL,
    length_in  REAL    NOT NULL,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT
);
"""

_COLS = ("name", "width_mm", "length_mm", "width_in", "length_in")


@dataclass(frozen=True)
class SizesConfig:
    db_path: str


@dataclass
class Size:
    name:      str
    width_mm:  int
    length_mm: int
    width_in:  float
    length_in: float
    id:        int | None = None       # None until persisted


def _row_to_size(row: sqlite3.Row) -> Size:
    return Size(
        id        = row["id"],
        name      = row["name"],
        width_mm  = row["width_mm"],
        length_mm = row["length_mm"],
        width_in  = row["width_in"],
        length_in = row["length_in"],
    )


class SizesStore:
    """SQLite-backed size catalog (cardboard + others). Thread-safe via lock."""

    def __init__(self, cfg: SizesConfig):
        self.cfg = cfg
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, cfg: SizesConfig) -> "SizesStore":
        return cls(cfg)

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        Path(self.cfg.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.cfg.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def stop(self) -> None:
        if self._conn is not None:
            try: self._conn.close()
            except Exception: pass
            self._conn = None

    # ---- public API ---------------------------------------------------------

    def list(self, table: str) -> list[Size]:
        t = self._table(table)
        with self._lock:
            rows = self._require_conn().execute(
                f"SELECT * FROM {t} ORDER BY id ASC"
            ).fetchall()
        return [_row_to_size(r) for r in rows]

    def get(self, table: str, sid: int) -> Size | None:
        t = self._table(table)
        with self._lock:
            row = self._require_conn().execute(
                f"SELECT * FROM {t} WHERE id = ?", (int(sid),)
            ).fetchone()
        return _row_to_size(row) if row else None

    def add(self, table: str, s: Size) -> int:
        """Insert a new row. Returns the assigned id (s.id is ignored)."""
        t = self._table(table)
        d = self._payload(s)
        cols         = ", ".join(_COLS)
        placeholders = ", ".join(f":{c}" for c in _COLS)
        sql = f"INSERT INTO {t} ({cols}) VALUES ({placeholders})"
        with self._lock:
            conn = self._require_conn()
            cur  = conn.execute(sql, d)
            conn.commit()
            return int(cur.lastrowid)

    def update(self, table: str, s: Size) -> None:
        """Update by id. Bumps updated_at. Raises if s.id is None or missing."""
        if s.id is None:
            raise ValueError("update() requires Size.id")
        t = self._table(table)
        d = self._payload(s)
        d["id"] = int(s.id)
        set_clause = ", ".join(f"{c} = :{c}" for c in _COLS)
        sql = (
            f"UPDATE {t} SET {set_clause}, updated_at = datetime('now') "
            f"WHERE id = :id"
        )
        with self._lock:
            conn = self._require_conn()
            cur  = conn.execute(sql, d)
            conn.commit()
            if cur.rowcount == 0:
                raise KeyError(f"no row with id={s.id} in {t}")

    def delete(self, table: str, sid: int) -> None:
        """Hard delete. (No soft-delete here — sizes are reference data.)"""
        t = self._table(table)
        with self._lock:
            conn = self._require_conn()
            conn.execute(f"DELETE FROM {t} WHERE id = ?", (int(sid),))
            conn.commit()

    # ---- internals ----------------------------------------------------------

    @staticmethod
    def _table(table: str) -> str:
        if table not in TABLES:
            raise ValueError(f"unknown table {table!r}; expected one of {TABLES}")
        return table

    @staticmethod
    def _payload(s: Size) -> dict:
        d = asdict(s)
        d.pop("id", None)
        return d

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SizesStore not started — call .start() first")
        return self._conn
