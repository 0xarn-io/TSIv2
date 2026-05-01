"""dashboard.py — top-level UI composition for Warak TSI.

Owns the @ui.page routes and the tabs. Each panel is built from its
matching store; missing stores are silently skipped so the dashboard
degrades gracefully when a feature is disabled.

Main.py only constructs `Dashboard` and calls `register_routes()` —
it never touches NiceGUI primitives directly.

Adding a new tab:
    1. import its panel class
    2. accept its store/manager in `Dashboard.build`
    3. add a tuple to `_TAB_SPECS` and a constructor to `_PANEL_BUILDERS`
       below — the rest of the rendering picks it up automatically.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

from nicegui import ui

from camera_panel     import CameraManager
from errors_panel     import ErrorsPanel
from errors_store     import ErrorsStore
from recipes_panel    import RecipesPanel
from recipes_store    import RecipesStore
from robot_panel      import RobotPanel
from robot_status     import RobotMonitor
from robot_variables  import RobotVariablesMonitor
from robot_vars_panel import RobotVarsPanel
from sizes_panel      import SizesPanel
from sizes_store      import SizesStore
from theme            import apply_theme, card, warak_header

log = logging.getLogger(__name__)


# Tab name → display label, route path, panel attribute on Dashboard.
@dataclass(frozen=True)
class _TabSpec:
    name:    str             # internal id, also URL slug
    label:   str             # shown on the tab
    icon:    str             # Material icon name
    attr:    str             # attribute on Dashboard holding the mountable
    mount:   Callable[[object], None]  # (panel) -> None — mount inside a tab_panel


def _mount_cameras(cm: CameraManager) -> None:
    with ui.column().classes("w-full p-6 gap-4"):
        cm.build()


def _mount_panel(panel) -> None:
    panel.mount()


_TAB_SPECS: tuple[_TabSpec, ...] = (
    _TabSpec("cameras",   "Cameras", "videocam",                "cameras",   _mount_cameras),
    _TabSpec("robot",     "Robot",   "precision_manufacturing", "robot",     _mount_panel),
    _TabSpec("vars",      "Vars",    "tune",                    "robot_vars",_mount_panel),
    _TabSpec("recipes",   "Recipes", "menu_book",               "recipes",   _mount_panel),
    _TabSpec("sizes",     "Sizes",   "straighten",              "sizes",     _mount_panel),
    _TabSpec("errors",    "Errors",  "report_problem",          "errors",    _mount_panel),
)


@dataclass
class Dashboard:
    """Composition root for the UI. Build via `Dashboard.build(...)`."""

    cameras:    CameraManager   | None = None
    robot:      RobotPanel      | None = None
    robot_vars: RobotVarsPanel  | None = None
    recipes:    RecipesPanel    | None = None
    sizes:      SizesPanel      | None = None
    errors:     ErrorsPanel     | None = None
    title:      str             = "TSI Gen 1.5"

    # ---- builder ------------------------------------------------------------

    @classmethod
    def build(
        cls,
        *,
        cameras:            CameraManager          | None = None,
        robot_monitor:      RobotMonitor           | None = None,
        robot_vars_monitor: RobotVariablesMonitor  | None = None,
        recipes_store:      RecipesStore           | None = None,
        sizes_store:        SizesStore             | None = None,
        errors_store:       ErrorsStore            | None = None,
        title:              str = "TSI Gen 1.5",
        bus                                        = None,
    ) -> "Dashboard":
        # `bus` is accepted now so panels can subscribe to bus events as
        # they're migrated. Currently each panel still pulls from its
        # store/monitor on a UI timer.
        return cls(
            cameras    = cameras,
            robot      = RobotPanel(robot_monitor)         if robot_monitor      else None,
            robot_vars = RobotVarsPanel(robot_vars_monitor) if robot_vars_monitor else None,
            recipes    = RecipesPanel(recipes_store)       if recipes_store      else None,
            sizes      = SizesPanel(sizes_store)           if sizes_store        else None,
            errors     = ErrorsPanel(errors_store)         if errors_store       else None,
            title      = title,
        )

    # ---- routes -------------------------------------------------------------

    def register_routes(self) -> None:
        """Wire @ui.page("/") and the per-tab deep links."""
        # Use closures so each route knows its initial tab.
        def _factory(tab_name: str):
            def _page() -> None:
                self.render(tab_name)
            return _page

        # Default route: first available tab.
        first = self._available_tabs()
        default_tab = first[0].name if first else "cameras"
        ui.page("/")(_factory(default_tab))

        # Per-tab deep links.
        for spec in _TAB_SPECS:
            if self._panel_for(spec) is None:
                continue
            ui.page(f"/{spec.name}")(_factory(spec.name))

    # ---- render -------------------------------------------------------------

    def render(self, initial_tab: str) -> None:
        """Single-page tabbed dashboard. Called inside @ui.page."""
        apply_theme()
        warak_header(self.title)

        available = self._available_tabs()
        if not available:
            with ui.column().classes("p-8"):
                with card():
                    ui.label("No panels enabled. Add a [recipes], [sizes], "
                             "[errors_log] or cameras section to "
                             "app_config.toml.").classes("text-gray-500")
            return

        # Fall back to the first available tab if the requested one isn't built.
        names = [t.name for t in available]
        chosen = initial_tab if initial_tab in names else names[0]

        with ui.column().classes("w-full max-w-7xl mx-auto px-4 pb-8"):
            with ui.tabs().props("inline-label dense").classes(
                "text-[#0053A1]"
            ) as tabs:
                tab_objs = {
                    spec.name: ui.tab(spec.label, icon=spec.icon).props(
                        f"name={spec.name}"
                    )
                    for spec in available
                }
            with ui.tab_panels(
                tabs, value=tab_objs[chosen],
            ).classes("w-full bg-transparent"):
                for spec in available:
                    with ui.tab_panel(tab_objs[spec.name]).classes("p-0"):
                        spec.mount(self._panel_for(spec))

    # ---- helpers ------------------------------------------------------------

    def _panel_for(self, spec: _TabSpec):
        return getattr(self, spec.attr, None)

    def _available_tabs(self) -> list[_TabSpec]:
        return [s for s in _TAB_SPECS if self._panel_for(s) is not None]
