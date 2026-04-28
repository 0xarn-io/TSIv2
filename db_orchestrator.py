"""db_orchestrator.py — builds and lifecycles the data layer.

Owns the SQLite stores (recipes / unit log / errors) plus the recipe
publisher that bridges the recipes DB to the PLC. External dependencies
(`plc`, `bridge`, optional `archive`) are injected — the orchestrator
never reaches back into Main.py.

Adding a new DB module = 1 build line + 1 self._register(...) line below.
Removing one = delete the same 2 lines (or just drop its config section
from app_config.toml — `None` services are skipped automatically).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from errors_store     import ErrorsStore
from recipe_publisher import RecipePublisher
from recipes_store    import RecipesStore
from sizes_store      import SizesStore
from unit_logger      import UnitLogger

log = logging.getLogger(__name__)


@dataclass
class _Service:
    name:  str
    start: Callable[[], None]
    stop:  Callable[[], None]


class DBOrchestrator:
    """Builds + lifecycles the data layer (recipes / unit log / errors).

    Each store is independently usable; this class is purely a convenience
    so Main.py only needs one start()/stop() pair regardless of how many
    DB modules exist now or in the future.
    """

    def __init__(self, cfg, *, plc, bridge, archive=None):
        self.cfg     = cfg
        self._plc    = plc
        self._bridge = bridge
        self._archive = archive
        self._services: list[_Service] = []
        self._started:  list[_Service] = []
        self._build()

    @classmethod
    def from_config(cls, cfg, *, plc, bridge, archive=None) -> "DBOrchestrator":
        return cls(cfg, plc=plc, bridge=bridge, archive=archive)

    # ---- build (1 line per store) ------------------------------------------

    def _build(self) -> None:
        c = self.cfg

        self.errors     = (ErrorsStore.from_config(c.errors_log)
                           if getattr(c, "errors_log", None) else None)
        self.recipes    = (RecipesStore.from_config(c.recipes)
                           if getattr(c, "recipes", None) else None)
        self.sizes      = (SizesStore.from_config(c.sizes)
                           if getattr(c, "sizes", None) else None)
        self.recipe_pub = (RecipePublisher(self.recipes, self._plc, c.plc.recipe)
                           if self.recipes and getattr(c.plc, "recipe", None)
                           else None)
        self.unit_log   = (UnitLogger(
                              c.unit_log, self._bridge, self._plc,
                              recipe_alias=(c.plc.recipe.code_alias
                                            if getattr(c.plc, "recipe", None)
                                            else None),
                              archive=self._archive,
                          )
                           if getattr(c, "unit_log", None) else None)

        # Order: errors first so anything can log during start, recipes
        # before recipe_pub (recipe_pub reads from recipes), unit_log last
        # so the DB is open before any UnitEvent fires.
        self._register(self.errors)
        self._register(self.recipes)
        self._register(self.recipe_pub)
        self._register(self.sizes)
        self._register(self.unit_log)

    def _register(self, obj) -> None:
        if obj is None:
            return
        self._services.append(_Service(
            name=type(obj).__name__,
            start=obj.start,
            stop=obj.stop,
        ))

    # ---- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        for s in self._services:
            try:
                s.start()
                self._started.append(s)
            except Exception:
                log.exception(
                    "data-layer start failed at %s — rolling back", s.name,
                )
                self.stop()
                raise

    def stop(self) -> None:
        for s in reversed(self._started):
            try: s.stop()
            except Exception: log.exception("stop failed for %s", s.name)
        self._started.clear()

    # ---- introspection -----------------------------------------------------

    def services(self) -> list[str]:
        """Names of registered services in startup order. Useful for tests/logs."""
        return [s.name for s in self._services]
