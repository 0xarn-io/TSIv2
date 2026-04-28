"""recipe_publisher.py — bridge an active-recipe code from the PLC to setpoints.

When the PLC writes a new value to `cfg.code_alias` (DINT), this module
looks the recipe up in `RecipesStore` and writes the matching setpoints
struct to `cfg.setpoints_alias` (ST_RecipeSetpoints).

Threading: notifications run on pyads's AmsRouter thread; making sync ADS
writes from there deadlocks (response handler == caller). So the
notification callback only enqueues; a dedicated worker thread does the
actual plc.write off the AmsRouter.
"""
from __future__ import annotations

import logging
import queue
import threading
from dataclasses import dataclass

from recipes_store import Recipe, RecipesStore
from twincat_comm  import TwinCATComm

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecipePublisherConfig:
    code_alias:      str   # PLC writes a DINT here
    setpoints_alias: str   # Python writes ST_RecipeSetpoints here
    cycle_ms:        int = 100


def _recipe_to_struct(r: Recipe) -> dict:
    """Recipe → ST_RecipeSetpoints dict.

    Adding a setpoint: append a field here AND in [structs.ST_RecipeSetpoints]
    in plc_signals.toml. No other code changes.
    """
    return {
        "nXTopsheetLength": int(r.x_topsheet_length),
        "nXTopsheetWidth":  int(r.x_topsheet_width),
        "nXUnits":          int(r.x_units),
        "nX1Pos":            int(r.x1_pos),
        "nX2Pos":            int(r.x2_pos),
        "nX3Pos":            int(r.x3_pos),
        "bXFolding":         bool(r.x_folding),
        "nYTopsheetLength": int(r.y_topsheet_length),
        "nYTopsheetWidth":  int(r.y_topsheet_width),
        "nYUnits":          int(r.y_units),
        "nY1Pos":            int(r.y1_pos),
        "nY2Pos":            int(r.y2_pos),
        "nY3Pos":            int(r.y3_pos),
        "bYFolding":         bool(r.y_folding),
        "bWood":             bool(r.wood),
        "nWoodXPos":        int(r.wood_x_pos),
        "nWoodYPos":        int(r.wood_y_pos),
    }


_STOP = object()


class RecipePublisher:
    """PLC subscribe(code) → recipes.get(code) → plc.write(setpoints).

    Writes run on a worker thread, never on pyads's notification thread.
    """

    def __init__(
        self,
        recipes: RecipesStore,
        plc: TwinCATComm,
        cfg: RecipePublisherConfig,
    ):
        self.recipes = recipes
        self.plc = plc
        self.cfg = cfg
        self._handles: tuple[int, int] | None = None
        self._queue: queue.Queue = queue.Queue()
        self._worker: threading.Thread | None = None
        self._stop_evt = threading.Event()

    def start(self) -> None:
        self.plc.validate([self.cfg.code_alias, self.cfg.setpoints_alias])

        self._stop_evt.clear()
        self._worker = threading.Thread(
            target=self._run, daemon=True, name="recipe-pub-writer",
        )
        self._worker.start()

        # Push current recipe once so the PLC isn't stale at boot.
        try:
            current = self.plc.read(self.cfg.code_alias)
            self._queue.put(int(current))
        except Exception as e:
            log.warning("initial recipe read failed: %s", e)

        try:
            self._handles = self.plc.subscribe(
                self.cfg.code_alias,
                lambda _alias, val: self._queue.put(int(val)),
                cycle_time_ms=self.cfg.cycle_ms,
                on_change=True,
            )
        except Exception as e:
            log.warning(
                "recipe code subscription failed (%s); recipe publisher "
                "disabled until '%s' is available on the PLC",
                e, self.cfg.code_alias,
            )
            self._handles = None

    def stop(self) -> None:
        if self._handles is not None:
            try: self.plc.unsubscribe(self._handles)
            except Exception as e: log.warning("unsubscribe code failed: %s", e)
            self._handles = None

        self._stop_evt.set()
        self._queue.put(_STOP)
        if self._worker is not None:
            self._worker.join(timeout=2.0)
            self._worker = None

    # ---- worker -------------------------------------------------------------

    def _run(self) -> None:
        while not self._stop_evt.is_set():
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is _STOP:
                break
            try:
                self._apply(int(item))
            except Exception:
                log.exception("recipe writer crashed on code=%s", item)

    def _apply(self, code: int) -> None:
        if code <= 0:
            # 0 means "no selection" — don't warn, don't query the DB.
            return
        recipe = self.recipes.get(code)
        if recipe is None:
            log.warning("recipe code %s not found in DB — no setpoints written", code)
            return
        try:
            self.plc.write(self.cfg.setpoints_alias, _recipe_to_struct(recipe))
            log.info("recipe %s pushed", code)
        except Exception as e:
            log.warning("recipe setpoints write failed: %s", e)
