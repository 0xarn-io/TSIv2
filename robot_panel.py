"""robot_panel.py — live ABB OmniCore status panel.

Reads a fresh `RobotStatus` snapshot from `RobotMonitor.status()` every
second and renders the same booleans the publisher mirrors to the PLC,
plus the raw RWS strings (ctrl_state / opmode / exec_state) and the
speed ratio. Read-only; no commands.
"""
from __future__ import annotations

import logging
import time

from nicegui import ui

from robot_status import RobotMonitor, RobotStatus
from theme        import card

log = logging.getLogger(__name__)

_REFRESH_INTERVAL_S = 1.0


class RobotPanel:
    """Read-only ABB robot status display."""

    def __init__(self, monitor: RobotMonitor):
        self.monitor = monitor
        self._labels: dict[str, ui.label] = {}
        self._flag_chips: dict[str, ui.element] = {}
        self._timer: ui.timer | None = None

    # ---- top-level mount ----------------------------------------------------

    def mount(self) -> None:
        with ui.column().classes("w-full gap-4 p-6"):
            self._render_summary_card()
            self._render_flags_card()
            self._render_raw_card()
        self._refresh()
        self._timer = ui.timer(_REFRESH_INTERVAL_S, self._refresh)

    # ---- summary (big readiness state) --------------------------------------

    def _render_summary_card(self) -> None:
        with card():
            with ui.row().classes("items-center gap-4 w-full"):
                ui.icon("smart_toy").classes("text-5xl text-[#0053A1]")
                with ui.column().classes("gap-0 flex-grow"):
                    self._labels["headline"] = ui.label("…").classes(
                        "text-xl font-semibold"
                    )
                    self._labels["sub"] = ui.label("").classes(
                        "text-sm text-gray-500"
                    )
                with ui.column().classes("gap-0 items-end"):
                    self._labels["speed_value"] = ui.label("0%").classes(
                        "text-2xl font-semibold text-[#0053A1] font-mono"
                    )
                    ui.label("Speed").classes(
                        "text-xs uppercase tracking-wider text-gray-500"
                    )

    # ---- boolean chips ------------------------------------------------------

    def _render_flags_card(self) -> None:
        with card("Status flags"):
            with ui.row().classes("flex-wrap gap-2 w-full"):
                for key, label in (
                    ("is_ready",   "Ready"),
                    ("motors_on",  "Motors on"),
                    ("auto_mode",  "Auto mode"),
                    ("running",    "Running"),
                    ("guard_stop", "Guard stop"),
                    ("estop",      "E-Stop"),
                ):
                    self._flag_chips[key] = self._chip(label)

    def _chip(self, text: str) -> ui.element:
        chip = ui.element("div").classes(
            "inline-flex items-center gap-2 px-3 py-1 rounded-full text-xs "
            "font-semibold uppercase tracking-wider border bg-gray-100 "
            "text-gray-500 border-gray-200"
        )
        with chip:
            ui.element("span").classes(
                "w-2 h-2 rounded-full bg-gray-400"
            )
            ui.label(text)
        return chip

    # ---- raw RWS fields -----------------------------------------------------

    def _render_raw_card(self) -> None:
        with card("RWS state"):
            with ui.grid(columns=3).classes("w-full gap-3"):
                for key, label in (
                    ("ctrl_state", "Controller state"),
                    ("opmode",     "Operating mode"),
                    ("exec_state", "Execution"),
                ):
                    with ui.column().classes("gap-0"):
                        ui.label(label).classes(
                            "text-xs uppercase tracking-wider text-gray-500"
                        )
                        self._labels[key] = ui.label("…").classes(
                            "text-base font-mono"
                        )
            self._labels["last_polled"] = ui.label("").classes(
                "text-xs text-gray-400 mt-3"
            )

    # ---- refresh ------------------------------------------------------------

    def _refresh(self) -> None:
        try:
            s = self.monitor.status()
        except Exception as e:
            log.warning("robot status read failed: %s", e)
            return

        # Summary
        if s.is_ready:
            self._labels["headline"].text = "Ready"
            self._labels["headline"].classes(
                replace="text-xl font-semibold text-[#118938]",
            )
            self._labels["sub"].text = "Motors on · Auto · RAPID running"
        else:
            self._labels["headline"].text = self._summary_text(s)
            self._labels["headline"].classes(
                replace="text-xl font-semibold text-[#C0392B]",
            )
            self._labels["sub"].text = (
                f"{s.ctrl_state} / {s.opmode} / {s.exec_state}"
            )
        self._labels["speed_value"].text = f"{int(s.speed_ratio)}%"

        # Chips
        self._update_chip("is_ready",   s.is_ready,   "positive")
        self._update_chip("motors_on",  s.motors_on,  "positive")
        self._update_chip("auto_mode",  s.auto_mode,  "positive")
        self._update_chip("running",    s.running,    "positive")
        self._update_chip("guard_stop", s.guard_stop, "warning")
        self._update_chip("estop",      s.estop,      "negative")

        # Raw
        self._labels["ctrl_state"].text = s.ctrl_state or "—"
        self._labels["opmode"].text     = s.opmode     or "—"
        self._labels["exec_state"].text = s.exec_state or "—"
        if s.last_polled:
            ago = max(0.0, time.monotonic() - s.last_polled)
            self._labels["last_polled"].text = (
                f"Last successful poll {ago:.1f}s ago"
            )
        else:
            self._labels["last_polled"].text = "Waiting for first poll…"

    def _update_chip(self, key: str, on: bool, tone: str) -> None:
        """Re-style a chip according to its on/off state and tone class."""
        base = (
            "inline-flex items-center gap-2 px-3 py-1 rounded-full text-xs "
            "font-semibold uppercase tracking-wider border"
        )
        if not on:
            cls = (f"{base} bg-gray-100 text-gray-500 border-gray-200")
            dot = "bg-gray-400"
        elif tone == "positive":
            cls = f"{base} bg-[#4DA32F]/10 text-[#118938] border-[#4DA32F]/30"
            dot = "bg-[#4DA32F]"
        elif tone == "warning":
            cls = f"{base} bg-yellow-100 text-yellow-800 border-yellow-200"
            dot = "bg-yellow-500"
        else:  # negative
            cls = f"{base} bg-red-100 text-red-800 border-red-200"
            dot = "bg-red-500"
        chip = self._flag_chips[key]
        chip.classes(replace=cls)
        # Update the dot (first child span).
        try:
            dot_el = chip.default_slot.children[0]
            dot_el.classes(replace=f"w-2 h-2 rounded-full {dot}")
        except Exception:
            pass

    @staticmethod
    def _summary_text(s: RobotStatus) -> str:
        if s.estop:
            return "Emergency Stop"
        if s.guard_stop:
            return "Guard Stop"
        if not s.motors_on:
            return "Motors off"
        if not s.auto_mode:
            return "Manual mode"
        if not s.running:
            return "RAPID stopped"
        return "Not ready"
