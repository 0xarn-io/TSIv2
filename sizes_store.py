"""sizes_store.py — SQLite CRUD for size catalogs.

One DB with two tables, `cardboard` and `others`, both with the same shape:
    id, name, width_mm, length_mm

mm is the canonical unit. The UI offers an inches → mm helper (US shops
quote in inches) but only mm is persisted.

Usage:
    sizes = SizesStore.from_config(cfg.sizes)
    sizes.start()
    sid = sizes.add("cardboard", Size(name="35x70", width_mm=889, length_mm=1778))
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
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS others (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    width_mm   INTEGER NOT NULL,
    length_mm  INTEGER NOT NULL,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT
);
"""

_COLS = ("name", "width_mm", "length_mm")


@dataclass(frozen=True)
class SizesConfig:
    db_path: str


@dataclass
class Size:
    name:      str
    width_mm:  int
    length_mm: int
    id:        int | None = None       # None until persisted


def _row_to_size(row: sqlite3.Row) -> Size:
    return Size(
        id        = row["id"],
        name      = row["name"],
        width_mm  = int(row["width_mm"]),
        length_mm = int(row["length_mm"]),
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
            self._migrate(self._conn)
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
    def _migrate(conn: sqlite3.Connection) -> None:
        """Drop obsolete columns from earlier schemas. Idempotent."""
        # Old schemas had width_in / length_in (NOT NULL); the new model is
        # mm-only. Drop them if present so legacy DBs accept new inserts.
        OBSOLETE = ("width_in", "length_in")
        for table in TABLES:
            present = {r["name"] for r in conn.execute(
                f"PRAGMA table_info({table})"
            )}
            for col in OBSOLETE:
                if col in present:
                    try:
                        conn.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
                        log.info("sizes: dropped obsolete column %s.%s", table, col)
                    except sqlite3.OperationalError as e:
                        # SQLite < 3.35 doesn't support DROP COLUMN. Surface a
                        # clear message rather than masking the eventual NOT NULL
                        # failure.
                        log.warning(
                            "sizes: could not drop %s.%s (%s) — delete the "
                            "DB file or upgrade SQLite to 3.35+",
                            table, col, e,
                        )

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
