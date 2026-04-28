"""recipes_store.py — SQLite CRUD for process recipes.

Recipes are keyed by an integer code that the PLC writes to
`pyADS.nRecipeCode`. The DB is the source of truth; operators edit recipes
via NiceGUI; `RecipePublisher` pushes the active setpoints to the PLC when
the code changes.

Usage:
    recipes = RecipesStore.from_config(cfg.recipes)
    recipes.start()
    recipes.save(Recipe(code=1, x_topsheet_length=711, y_topsheet_width=400, ...))
    r = recipes.get(1)
    recipes.stop()

Concurrency: single connection, internal lock. Writes are operator-driven
and rare — no writer thread needed.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass, field, fields, asdict
from pathlib import Path

log = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS recipe (
    code              INTEGER PRIMARY KEY,
    x_topsheet_length INTEGER NOT NULL DEFAULT 0,
    x_topsheet_width  INTEGER NOT NULL DEFAULT 0,
    x_units           INTEGER NOT NULL DEFAULT 0,
    x1_pos            INTEGER NOT NULL DEFAULT 0,
    x2_pos            INTEGER NOT NULL DEFAULT 0,
    x3_pos            INTEGER NOT NULL DEFAULT 0,
    x_folding         INTEGER NOT NULL DEFAULT 0,
    y_topsheet_length INTEGER NOT NULL DEFAULT 0,
    y_topsheet_width  INTEGER NOT NULL DEFAULT 0,
    y_units           INTEGER NOT NULL DEFAULT 0,
    y1_pos            INTEGER NOT NULL DEFAULT 0,
    y2_pos            INTEGER NOT NULL DEFAULT 0,
    y3_pos            INTEGER NOT NULL DEFAULT 0,
    y_folding         INTEGER NOT NULL DEFAULT 0,
    wood              INTEGER NOT NULL DEFAULT 0,
    wood_x_pos        INTEGER NOT NULL DEFAULT 0,
    wood_y_pos        INTEGER NOT NULL DEFAULT 0,
    active            INTEGER NOT NULL DEFAULT 1,
    created_at        TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at        TEXT
);
"""


@dataclass(frozen=True)
class RecipesConfig:
    db_path: str


@dataclass
class Recipe:
    code:              int
    x_topsheet_length: int  = 0
    x_topsheet_width:  int  = 0
    x_units:           int  = 0
    x1_pos:            int  = 0
    x2_pos:            int  = 0
    x3_pos:            int  = 0
    x_folding:         bool = False
    y_topsheet_length: int  = 0
    y_topsheet_width:  int  = 0
    y_units:           int  = 0
    y1_pos:            int  = 0
    y2_pos:            int  = 0
    y3_pos:            int  = 0
    y_folding:         bool = False
    wood:              bool = False
    wood_x_pos:        int  = 0
    wood_y_pos:        int  = 0
    active:            bool = True


# Columns we round-trip via the dataclass (excludes auto-managed timestamps).
_RECIPE_COLS = [f.name for f in fields(Recipe)]


_BOOL_COLS = ("x_folding", "y_folding", "wood", "active")


def _row_to_recipe(row: sqlite3.Row) -> Recipe:
    d = {k: row[k] for k in _RECIPE_COLS}
    for c in _BOOL_COLS:
        d[c] = bool(d[c])
    return Recipe(**d)


class RecipesStore:
    """SQLite-backed recipe CRUD. Thread-safe via an internal lock."""

    def __init__(self, cfg: RecipesConfig):
        self.cfg = cfg
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, cfg: RecipesConfig) -> "RecipesStore":
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

    def list(self, *, active_only: bool = True) -> list[Recipe]:
        sql = "SELECT * FROM recipe"
        if active_only:
            sql += " WHERE active = 1"
        sql += " ORDER BY code ASC"
        with self._lock:
            rows = self._require_conn().execute(sql).fetchall()
        return [_row_to_recipe(r) for r in rows]

    def get(self, code: int) -> Recipe | None:
        with self._lock:
            row = self._require_conn().execute(
                "SELECT * FROM recipe WHERE code = ?", (int(code),)
            ).fetchone()
        return _row_to_recipe(row) if row else None

    def save(self, r: Recipe) -> None:
        """Upsert by code. Bumps updated_at."""
        d = asdict(r)
        for c in _BOOL_COLS:
            d[c] = 1 if d[c] else 0
        cols = list(d.keys())
        placeholders = ", ".join(f":{c}" for c in cols)
        col_list = ", ".join(cols)
        update_set = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "code")
        sql = f"""
            INSERT INTO recipe ({col_list}) VALUES ({placeholders})
            ON CONFLICT(code) DO UPDATE SET
                {update_set},
                updated_at = datetime('now')
        """
        with self._lock:
            conn = self._require_conn()
            conn.execute(sql, d)
            conn.commit()

    def deactivate(self, code: int) -> None:
        """Soft-delete: mark inactive. Keeps history intact."""
        with self._lock:
            conn = self._require_conn()
            conn.execute(
                "UPDATE recipe SET active = 0, updated_at = datetime('now') "
                "WHERE code = ?",
                (int(code),),
            )
            conn.commit()

    def delete(self, code: int) -> None:
        """Hard delete by code. No-op if the row doesn't exist."""
        with self._lock:
            conn = self._require_conn()
            conn.execute("DELETE FROM recipe WHERE code = ?", (int(code),))
            conn.commit()

    def rename(self, old_code: int, new_code: int) -> None:
        """Atomically move the row at old_code to new_code.

        Preserves created_at; bumps updated_at. Raises:
          KeyError       — old_code does not exist.
          ValueError     — new_code already exists.
        """
        old_code, new_code = int(old_code), int(new_code)
        if old_code == new_code:
            return
        with self._lock:
            conn = self._require_conn()
            try:
                conn.execute("BEGIN")
                if conn.execute(
                    "SELECT 1 FROM recipe WHERE code = ?", (new_code,),
                ).fetchone() is not None:
                    raise ValueError(f"recipe code {new_code} already exists")
                cur = conn.execute(
                    "UPDATE recipe SET code = ?, updated_at = datetime('now') "
                    "WHERE code = ?",
                    (new_code, old_code),
                )
                if cur.rowcount == 0:
                    raise KeyError(f"recipe code {old_code} not found")
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ---- internals ----------------------------------------------------------

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("RecipesStore not started — call .start() first")
        return self._conn
