"""sizes_store.py — SQLite CRUD for the size catalog.

Single table `sizes`:
    id, name, width_mm, length_mm, slot, station3, created_at, updated_at

mm is the canonical unit. The optional `slot` column (0–19, unique) pins
a row to a numbered position in the robot's Master / Master_Dimmensions
arrays — used by `robot_master.RobotMasterMonitor` for the live two-way
mirror. NULL slot = local-only, never pushed.

`station3` is the boolean encoded in the third column of
Master_Dimmensions on the controller — 1 means the size is selectable at
station 3, 0 means it isn't. It rides on each row alongside the
dimensions; no table routing.

Usage:
    sizes = SizesStore.from_config(cfg.sizes)
    sizes.start()
    sid = sizes.add(Size(name="35x70", width_mm=889, length_mm=1778))
    s = sizes.get(sid)
    sizes.list()
    sizes.upsert_slot(3, "Wood", 1000, 1000, station3=True)
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

TABLE      = "sizes"
SLOT_COUNT = 20

_SCHEMA_TABLE = """
CREATE TABLE IF NOT EXISTS sizes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    width_mm   INTEGER NOT NULL,
    length_mm  INTEGER NOT NULL,
    slot       INTEGER,
    station3   INTEGER NOT NULL DEFAULT 0,
    created_at TEXT    NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT
);
"""

_SCHEMA_INDEXES = """
CREATE UNIQUE INDEX IF NOT EXISTS idx_sizes_slot ON sizes(slot)
    WHERE slot IS NOT NULL;
"""

# Columns the dataclass round-trips. Excludes id (autoincrement) and
# timestamps (auto-managed).
_COLS = ("name", "width_mm", "length_mm", "slot", "station3")


@dataclass(frozen=True)
class SizesConfig:
    db_path: str


@dataclass
class Size:
    name:      str
    width_mm:  int
    length_mm: int
    slot:      int  | None = None
    station3:  bool = False
    id:        int  | None = None       # None until persisted


@dataclass(frozen=True)
class SizesChange:
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
        station3  = bool(row["station3"]),
    )


class SizesStore:
    """SQLite-backed size catalog. Thread-safe via lock."""

    def __init__(self, cfg: SizesConfig, *, bus=None):
        self.cfg = cfg
        self._bus = bus
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()
        self._cbs: list[Callable[[SizesChange], None]] = []
        self._silent = False

    @classmethod
    def from_config(cls, cfg: SizesConfig, *, bus=None) -> "SizesStore":
        return cls(cfg, bus=bus)

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        Path(self.cfg.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.cfg.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(_SCHEMA_TABLE)
            self._migrate(self._conn)
            self._conn.executescript(_SCHEMA_INDEXES)
            self._conn.commit()

    def stop(self) -> None:
        if self._conn is not None:
            try: self._conn.close()
            except Exception: pass
            self._conn = None

    # ---- subscriptions ------------------------------------------------------

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
        if self._bus is not None:
            from events import SizesChanged, signals
            slot = (ev.size.slot if (ev.size is not None
                                     and ev.size.slot is not None) else -1)
            payload = (None if ev.size is None
                       else {"id": ev.size.id, "slot": ev.size.slot,
                             "name": ev.size.name,
                             "width_mm": ev.size.width_mm,
                             "length_mm": ev.size.length_mm,
                             "station3": ev.size.station3})
            self._bus.publish(signals.sizes_changed,
                              SizesChanged(slot=int(slot or -1),
                                           op=ev.op, payload=payload))

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

    def list(self) -> list[Size]:
        with self._lock:
            rows = self._require_conn().execute(
                f"SELECT * FROM {TABLE} ORDER BY id ASC"
            ).fetchall()
        return [_row_to_size(r) for r in rows]

    def get(self, sid: int) -> Size | None:
        with self._lock:
            row = self._require_conn().execute(
                f"SELECT * FROM {TABLE} WHERE id = ?", (int(sid),),
            ).fetchone()
        return _row_to_size(row) if row else None

    def add(self, s: Size) -> int:
        """Insert a new row. Returns the assigned id (s.id is ignored)."""
        if s.slot is not None:
            self._guard_slot(s.slot, exclude=None)
        d = self._payload(s)
        cols         = ", ".join(_COLS)
        placeholders = ", ".join(f":{c}" for c in _COLS)
        sql = f"INSERT INTO {TABLE} ({cols}) VALUES ({placeholders})"
        with self._lock:
            conn = self._require_conn()
            cur  = conn.execute(sql, d)
            conn.commit()
            new_id = int(cur.lastrowid)
        s2 = Size(**{**asdict(s), "id": new_id})
        self._emit(SizesChange(op="add", size=s2, sid=new_id))
        return new_id

    def update(self, s: Size) -> None:
        """Update by id. Bumps updated_at. Raises if s.id is None or missing."""
        if s.id is None:
            raise ValueError("update() requires Size.id")
        if s.slot is not None:
            self._guard_slot(s.slot, exclude=int(s.id))
        d = self._payload(s)
        d["id"] = int(s.id)
        set_clause = ", ".join(f"{c} = :{c}" for c in _COLS)
        sql = (
            f"UPDATE {TABLE} SET {set_clause}, updated_at = datetime('now') "
            f"WHERE id = :id"
        )
        with self._lock:
            conn = self._require_conn()
            cur  = conn.execute(sql, d)
            conn.commit()
            if cur.rowcount == 0:
                raise KeyError(f"no row with id={s.id}")
        self._emit(SizesChange(op="update", size=s, sid=int(s.id)))

    def delete(self, sid: int) -> None:
        """Hard delete. (No soft-delete here — sizes are reference data.)"""
        with self._lock:
            conn = self._require_conn()
            conn.execute(f"DELETE FROM {TABLE} WHERE id = ?", (int(sid),))
            conn.commit()
        self._emit(SizesChange(op="delete", size=None, sid=int(sid)))

    # ---- slot-aware helpers -------------------------------------------------

    def get_slot(self, slot: int) -> Size | None:
        """Return the row pinned to `slot`, if any."""
        with self._lock:
            row = self._require_conn().execute(
                f"SELECT * FROM {TABLE} WHERE slot = ?", (int(slot),),
            ).fetchone()
        return _row_to_size(row) if row else None

    def upsert_slot(
        self, slot: int, name: str,
        width_mm: int, length_mm: int, *, station3: bool,
    ) -> int:
        """Idempotent: ensure the slot holds the given (name, w, l, station3).

        Returns the row's sid. No on_change emit on a no-op.
        """
        size = Size(
            name=name, width_mm=int(width_mm), length_mm=int(length_mm),
            slot=int(slot), station3=bool(station3),
        )
        current = self.get_slot(slot)
        if current is None:
            return self.add(size)
        if (current.name      == size.name
                and current.width_mm  == size.width_mm
                and current.length_mm == size.length_mm
                and current.station3  == size.station3):
            return int(current.id)                          # type: ignore[arg-type]
        size.id = current.id
        self.update(size)
        return int(current.id)                              # type: ignore[arg-type]

    def clear_slot(self, slot: int) -> bool:
        """Remove the row pinned to `slot`. Returns True if deleted."""
        current = self.get_slot(slot)
        if current is None:
            return False
        self.delete(int(current.id))                        # type: ignore[arg-type]
        return True

    # ---- internals ----------------------------------------------------------

    def _guard_slot(self, slot: int, *, exclude: int | None) -> None:
        """Raise if `slot` is already used by another row."""
        if not (0 <= int(slot) < SLOT_COUNT):
            raise ValueError(
                f"slot {slot} out of range 0..{SLOT_COUNT - 1}"
            )
        with self._lock:
            row = self._require_conn().execute(
                f"SELECT id FROM {TABLE} WHERE slot = ?", (int(slot),),
            ).fetchone()
        if row is None:
            return
        if exclude is not None and int(row["id"]) == exclude:
            return
        raise ValueError(f"slot {slot} already used by sizes#{row['id']}")

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """One-shot folds: cardboard + others → sizes (with station3 set
        from which legacy table the row came from), then drops the legacy
        tables. Idempotent: re-runs are no-ops once the legacy tables are
        gone.
        """
        existing = {r["name"] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}

        # Legacy tables to fold into `sizes`. station3 default is what
        # the user's earlier wood-routing implied: rows from `others`
        # had wood=1, so station3=1.
        for legacy_table, station3 in (("cardboard", 0), ("others", 1)):
            if legacy_table not in existing:
                continue
            cols = {r["name"] for r in conn.execute(
                f"PRAGMA table_info({legacy_table})"
            )}
            common = [c for c in ("name", "width_mm", "length_mm", "slot")
                      if c in cols]
            if not common:
                conn.execute(f"DROP TABLE {legacy_table}")
                log.info("sizes: dropped legacy table %s (no usable columns)",
                         legacy_table)
                continue
            placeholders = ", ".join(common)
            conn.execute(
                f"INSERT INTO sizes ({placeholders}, station3) "
                f"SELECT {placeholders}, {station3} FROM {legacy_table}"
            )
            conn.execute(f"DROP TABLE {legacy_table}")
            log.info(
                "sizes: folded %s rows from %s (station3=%d) into sizes",
                conn.total_changes, legacy_table, station3,
            )

        # Make sure the `station3` column exists on a pre-existing `sizes`
        # table (e.g. user already migrated then we added the column).
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(sizes)")}
        if "station3" not in cols:
            try:
                conn.execute(
                    "ALTER TABLE sizes ADD COLUMN station3 INTEGER "
                    "NOT NULL DEFAULT 0"
                )
                log.info("sizes: added column sizes.station3")
            except sqlite3.OperationalError as e:
                log.warning("sizes: could not add station3: %s", e)

    @staticmethod
    def _payload(s: Size) -> dict:
        d = asdict(s)
        d.pop("id", None)
        # SQLite stores bools as 0/1 INTs.
        d["station3"] = 1 if d.get("station3") else 0
        return d

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("SizesStore not started — call .start() first")
        return self._conn
