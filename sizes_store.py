"""sizes_store.py — SQLite CRUD for size catalogs.

One DB with two tables, `cardboard` and `others`, both with the same shape:
    id, name, width_mm, length_mm, slot

mm is the canonical unit. The optional `slot` column (0–19, unique across
both tables) pins a row to a numbered slot in the robot's Master /
Master_Dimmensions arrays — used by `robot_master.RobotMasterMonitor`
for the live two-way mirror. NULL slot = local-only, never pushed.

Wood routing: the *table* a row lives in tells you whether it's wood.
`others` rows are mirrored to the robot with wood=1, `cardboard` with
wood=0. Use `upsert_slot()` to insert/update by slot index — it picks
the right table automatically and moves the row across when the wood
flag flips.

Usage:
    sizes = SizesStore.from_config(cfg.sizes)
    sizes.start()
    sid = sizes.add("cardboard", Size(name="35x70", width_mm=889, length_mm=1778))
    s = sizes.get("cardboard", sid)
    sizes.list("others")
    sizes.upsert_slot(3, "Wood", 1000, 1000, wood=True)
    sizes.clear_slot(3)
    sizes.stop()

Concurrency: single connection, internal lock. Writes are operator-driven
and rare — no writer thread needed.

Subscriptions: `on_change(cb)` fires after every add/update/delete with
a `SizesChange` event. Used by the robot-side mirror to push DB edits
back to the controller.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Callable, Literal

log = logging.getLogger(__name__)

TABLES = ("cardboard", "others")
SLOT_COUNT = 20

# Wood routing: which table a wood=true / wood=false row lives in.
WOOD_TABLE = "others"
NON_WOOD_TABLE = "cardboard"

# Tables only — indexes are created post-migration so legacy DBs without the
# `slot` column don't trip the index DDL during executescript().
_SCHEMA_TABLES = """
CREATE TABLE IF NOT EXISTS cardboard (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    width_mm   INTEGER NOT NULL,
    length_mm  INTEGER NOT NULL,
    slot       INTEGER,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS others (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    width_mm   INTEGER NOT NULL,
    length_mm  INTEGER NOT NULL,
    slot       INTEGER,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT
);
"""

_SCHEMA_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_cardboard_slot ON cardboard(slot)
    WHERE slot IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_others_slot    ON others(slot)
    WHERE slot IS NOT NULL;
"""

# Columns the dataclass round-trips. Excludes id (autoincrement) and
# timestamps (auto-managed).
_COLS = ("name", "width_mm", "length_mm", "slot")


@dataclass(frozen=True)
class SizesConfig:
    db_path: str


@dataclass
class Size:
    name:      str
    width_mm:  int
    length_mm: int
    slot:      int | None = None
    id:        int | None = None       # None until persisted


@dataclass(frozen=True)
class SizesChange:
    table: str                                  # "cardboard" | "others"
    op:    Literal["add", "update", "delete"]
    size:  Size | None                          # None on delete
    sid:   int                                  # row id (post-insert / pre-delete)


def _row_to_size(row: sqlite3.Row) -> Size:
    return Size(
        id        = row["id"],
        name      = row["name"],
        width_mm  = int(row["width_mm"]),
        length_mm = int(row["length_mm"]),
        slot      = (None if row["slot"] is None else int(row["slot"])),
    )


def table_for_wood(wood: bool) -> str:
    """Routing rule: wood → others, non-wood → cardboard."""
    return WOOD_TABLE if wood else NON_WOOD_TABLE


def is_wood_table(table: str) -> bool:
    return table == WOOD_TABLE


class SizesStore:
    """SQLite-backed size catalog (cardboard + others). Thread-safe via lock."""

    def __init__(self, cfg: SizesConfig):
        self.cfg = cfg
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()
        self._cbs: list[Callable[[SizesChange], None]] = []
        self._silent = False     # set during sync writes to suppress callbacks

    @classmethod
    def from_config(cls, cfg: SizesConfig) -> "SizesStore":
        return cls(cfg)

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        Path(self.cfg.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.cfg.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA_TABLES)
            self._migrate(self._conn)
            self._conn.executescript(_SCHEMA_INDEXES)
            self._conn.commit()

    def stop(self) -> None:
        if self._conn is not None:
            try: self._conn.close()
            except Exception: pass
            self._conn = None

    # ---- subscriptions -----------------------------------------------------

    def on_change(
        self, cb: Callable[[SizesChange], None],
    ) -> Callable[[], None]:
        """Register a change callback. Returns an unsubscribe fn."""
        self._cbs.append(cb)
        return lambda: self._cbs.remove(cb)

    def _emit(self, ev: SizesChange) -> None:
        if self._silent:
            return
        for cb in list(self._cbs):
            try: cb(ev)
            except Exception as e:
                log.warning("sizes on_change cb failed: %s", e)

    def silent(self):
        """Context manager: suppress on_change emits inside the block.

        Used by the robot mirror to apply an inbound robot update without
        triggering an outbound push back to the robot.
        """
        store = self
        class _S:
            def __enter__(self):  store._silent = True
            def __exit__(self, *a): store._silent = False
        return _S()

    # ---- public CRUD --------------------------------------------------------

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
        if s.slot is not None:
            self._guard_slot(s.slot, exclude=None)
        d = self._payload(s)
        cols         = ", ".join(_COLS)
        placeholders = ", ".join(f":{c}" for c in _COLS)
        sql = f"INSERT INTO {t} ({cols}) VALUES ({placeholders})"
        with self._lock:
            conn = self._require_conn()
            cur  = conn.execute(sql, d)
            conn.commit()
            new_id = int(cur.lastrowid)
        s2 = Size(**{**asdict(s), "id": new_id})
        self._emit(SizesChange(table=t, op="add", size=s2, sid=new_id))
        return new_id

    def update(self, table: str, s: Size) -> None:
        """Update by id. Bumps updated_at. Raises if s.id is None or missing."""
        if s.id is None:
            raise ValueError("update() requires Size.id")
        t = self._table(table)
        if s.slot is not None:
            self._guard_slot(s.slot, exclude=(t, int(s.id)))
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
        self._emit(SizesChange(table=t, op="update", size=s, sid=int(s.id)))

    def delete(self, table: str, sid: int) -> None:
        """Hard delete. (No soft-delete here — sizes are reference data.)"""
        t = self._table(table)
        with self._lock:
            conn = self._require_conn()
            conn.execute(f"DELETE FROM {t} WHERE id = ?", (int(sid),))
            conn.commit()
        self._emit(SizesChange(table=t, op="delete", size=None, sid=int(sid)))

    # ---- slot-aware helpers ------------------------------------------------

    def get_slot(self, slot: int) -> tuple[str, Size] | None:
        """Locate the row pinned to `slot` in either table."""
        with self._lock:
            conn = self._require_conn()
            for t in TABLES:
                row = conn.execute(
                    f"SELECT * FROM {t} WHERE slot = ?", (int(slot),),
                ).fetchone()
                if row is not None:
                    return t, _row_to_size(row)
        return None

    def upsert_slot(
        self, slot: int, name: str,
        width_mm: int, length_mm: int, *, wood: bool,
    ) -> tuple[str, int]:
        """Idempotent: ensure the slot holds (name, w, l) in the wood-routed table.

        Cross-table moves (wood flag flipped) are handled by deleting from
        the wrong table and inserting in the right one — both events fire
        via on_change.

        Returns (table, sid) of the resulting row.
        """
        target  = table_for_wood(wood)
        current = self.get_slot(slot)
        size = Size(name=name, width_mm=int(width_mm),
                    length_mm=int(length_mm), slot=int(slot))
        if current is None:
            sid = self.add(target, size)
            return target, sid
        cur_table, cur_size = current
        if cur_table != target:
            # Wood flag changed — move the row.
            self.delete(cur_table, cur_size.id)        # type: ignore[arg-type]
            sid = self.add(target, size)
            return target, sid
        # Same table — update in place if anything changed.
        if (cur_size.name == size.name
                and cur_size.width_mm == size.width_mm
                and cur_size.length_mm == size.length_mm):
            return cur_table, cur_size.id              # type: ignore[return-value]
        size.id = cur_size.id
        self.update(cur_table, size)
        return cur_table, cur_size.id                  # type: ignore[return-value]

    def clear_slot(self, slot: int) -> bool:
        """Remove whichever row (if any) is pinned to `slot`. Returns True if deleted."""
        current = self.get_slot(slot)
        if current is None:
            return False
        table, size = current
        self.delete(table, int(size.id))               # type: ignore[arg-type]
        return True

    # ---- internals ----------------------------------------------------------

    def _guard_slot(
        self, slot: int, *, exclude: tuple[str, int] | None,
    ) -> None:
        """Raise if `slot` is already used by another row in either table."""
        if not (0 <= int(slot) < SLOT_COUNT):
            raise ValueError(
                f"slot {slot} out of range 0..{SLOT_COUNT - 1}"
            )
        with self._lock:
            conn = self._require_conn()
            for t in TABLES:
                row = conn.execute(
                    f"SELECT id FROM {t} WHERE slot = ?", (int(slot),),
                ).fetchone()
                if row is None:
                    continue
                if exclude is not None and exclude == (t, int(row["id"])):
                    continue
                raise ValueError(
                    f"slot {slot} already used by {t}#{row['id']}"
                )

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Drop obsolete columns + add new ones. Idempotent."""
        # Old: width_in / length_in NOT NULL — drop them.
        OBSOLETE = ("width_in", "length_in")
        # New: slot column for the robot mirror.
        TO_ADD = (
            ("slot", "INTEGER"),
        )
        for table in TABLES:
            cols = {r["name"] for r in conn.execute(
                f"PRAGMA table_info({table})"
            )}
            for col in OBSOLETE:
                if col in cols:
                    try:
                        conn.execute(f"ALTER TABLE {table} DROP COLUMN {col}")
                        log.info("sizes: dropped obsolete column %s.%s", table, col)
                    except sqlite3.OperationalError as e:
                        log.warning(
                            "sizes: could not drop %s.%s (%s) — delete the "
                            "DB file or upgrade SQLite to 3.35+",
                            table, col, e,
                        )
            for col, decl in TO_ADD:
                if col not in cols:
                    try:
                        conn.execute(
                            f"ALTER TABLE {table} ADD COLUMN {col} {decl}"
                        )
                        log.info("sizes: added column %s.%s", table, col)
                    except sqlite3.OperationalError as e:
                        log.warning(
                            "sizes: could not add %s.%s: %s", table, col, e,
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
