"""recipe_publisher.py — bridge an active-recipe code from the PLC to setpoints.

When the PLC writes a new value to `cfg.code_alias` (DINT), this module
looks the recipe up in `RecipesStore` and writes the matching setpoints
struct to `cfg.setpoints_alias` (ST_RecipeSetpoints).

Threading: notifications run on pyads's AmsRouter thread; making sync ADS
writes from there deadlocks (response handler == caller). So the
notification callback only enqueues; a dedicated worker thread does the
actual plc.write off the AmsRouter.

Bus mode: when an EventBus is supplied, the publisher subscribes to
PlcSignalChanged with an alias filter (no per-alias callback ownership),
and calls plc.ensure_published(code_alias) so the underlying ADS
notification gets registered. Legacy mode (bus=None) keeps the original
plc.subscribe(code_alias, cb) call.
"""
from __future__ import annotations

import logging
import queue
import threading
from dataclasses import dataclass
from typing import Callable

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
        *,
        bus=None,
    ):
        self.recipes = recipes
        self.plc = plc
        self.cfg = cfg
        self._bus = bus
        self._last_code: int | None = None
        self._handles: tuple[int, int] | None = None
        self._bus_unsub: Callable[[], None] | None = None
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

        if self._bus is not None:
            self._start_bus_mode()
        else:
            self._start_legacy_mode()

    def _start_bus_mode(self) -> None:
        """Bus mode: ensure_published + alias-filtered bus subscription."""
        from events import signals
        self.plc.ensure_published(
            self.cfg.code_alias, cycle_time_ms=self.cfg.cycle_ms,
        )
        self._bus_unsub = self._bus.subscribe_filtered(
            signals.plc_signal_changed,
            lambda p: self._queue.put(int(p.value)),
            mode="thread",
            alias=self.cfg.code_alias,
        )

    def _start_legacy_mode(self) -> None:
        """Legacy mode: per-alias callback enqueues to the worker."""
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
        if self._bus_unsub is not None:
            try: self._bus_unsub()
            except Exception as e: log.warning("bus unsubscribe failed: %s", e)
            self._bus_unsub = None
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
        prev = self._last_code
        if code != prev and self._bus is not None:
            from events import RecipeCodeChanged, signals
            self._bus.publish(signals.recipe_code_changed,
                              RecipeCodeChanged(code=int(code), prev=prev))
        self._last_code = int(code)

        if code <= 0:
            # 0 means "no selection" — don't warn, don't query the DB.
            return
        recipe = self.recipes.get(code)
        if recipe is None:
            log.warning("recipe code %s not found in DB — no setpoints written", code)
            return
        try:
            struct = _recipe_to_struct(recipe)
            self.plc.write(self.cfg.setpoints_alias, struct)
            log.info("recipe %s pushed", code)
            if self._bus is not None:
                from events import RecipeSetpointsPushed, signals
                self._bus.publish(signals.recipe_setpoints_pushed,
                                  RecipeSetpointsPushed(code=int(code),
                                                        values=dict(struct)))
        except Exception as e:
            log.warning("recipe setpoints write failed: %s", e)
