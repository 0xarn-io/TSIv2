"""recipes_store.py — SQLite CRUD for process recipes.

Recipes are keyed by an integer code that the PLC writes to
`pyADS.nRecipeCode`. The DB is the source of truth; operators edit recipes
via NiceGUI; `RecipePublisher` pushes the active setpoints to the PLC when
the code changes.

Usage:
    recipes = RecipesStore.from_config(cfg.recipes)
    recipes.start()
    recipes.save(Recipe(code=1, name="Tall Box", width_mm=711, ...))
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
    code          INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    width_mm      INTEGER NOT NULL,
    height_mm     INTEGER NOT NULL,
    depth_mm      INTEGER NOT NULL,
    width_tol     INTEGER NOT NULL DEFAULT 5,
    height_tol    INTEGER NOT NULL DEFAULT 5,
    depth_tol     INTEGER NOT NULL DEFAULT 5,
    x1_pos        INTEGER NOT NULL DEFAULT 0,
    x2_pos        INTEGER NOT NULL DEFAULT 0,
    x3_pos        INTEGER NOT NULL DEFAULT 0,
    y1_pos        INTEGER NOT NULL DEFAULT 0,
    y2_pos        INTEGER NOT NULL DEFAULT 0,
    y3_pos        INTEGER NOT NULL DEFAULT 0,
    rapid_program TEXT NOT NULL DEFAULT '',
    active        INTEGER NOT NULL DEFAULT 1,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT
);
"""


@dataclass(frozen=True)
class RecipesConfig:
    db_path: str


@dataclass
class Recipe:
    code:          int
    name:          str
    width_mm:      int
    height_mm:     int
    depth_mm:      int
    description:   str = ""
    width_tol:     int = 5
    height_tol:    int = 5
    depth_tol:     int = 5
    x1_pos:        int = 0
    x2_pos:        int = 0
    x3_pos:        int = 0
    y1_pos:        int = 0
    y2_pos:        int = 0
    y3_pos:        int = 0
    rapid_program: str = ""
    active:        bool = True


# Columns we round-trip via the dataclass (excludes auto-managed timestamps).
_RECIPE_COLS = [f.name for f in fields(Recipe)]


def _row_to_recipe(row: sqlite3.Row) -> Recipe:
    d = {k: row[k] for k in _RECIPE_COLS}
    d["active"] = bool(d["active"])
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
        d["active"] = 1 if d["active"] else 0
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

    # ---- internals ----------------------------------------------------------

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("RecipesStore not started — call .start() first")
        return self._conn
