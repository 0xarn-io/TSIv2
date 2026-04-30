"""robot_vars_panel.py — UI for the RobotVariablesMonitor.

Lists every configured RAPID variable; refreshes the displayed value on
a 1 s timer (the monitor's own thread is what actually polls RWS, this
just reads its cache). Read-only entries are labels; RW entries get an
inline editor (number / checkbox / input) plus a Set button.
"""
from __future__ import annotations

import logging

from nicegui import ui

from robot_variables import RobotVariableConfig, RobotVariablesMonitor
from theme           import card

log = logging.getLogger(__name__)

_REFRESH_INTERVAL_S = 1.0


class RobotVarsPanel:
    """Read + write panel for RAPID variables exposed via RWS."""

    def __init__(self, monitor: RobotVariablesMonitor):
        self.monitor = monitor
        self._value_labels: dict[str, ui.label] = {}
        self._editors:      dict[str, ui.element] = {}
        self._timer: ui.timer | None = None

    # ---- mount --------------------------------------------------------------

    def mount(self) -> None:
        with ui.column().classes("w-full gap-4 p-6"):
            with card("RAPID variables"):
                if not self.monitor.vars:
                    ui.label("No variables configured. Add [[robot.vars]] "
                             "entries in app_config.toml.").classes(
                        "text-gray-500 italic"
                    )
                    return
                self._render_header()
                for cfg in self.monitor.vars.values():
                    self._render_row(cfg)
            # Parent the timer to this column so it dies with the page.
            self._timer = ui.timer(_REFRESH_INTERVAL_S, self._refresh)
        self._refresh()

    # ---- rendering ----------------------------------------------------------

    def _render_header(self) -> None:
        with ui.row().classes(
            "w-full items-center gap-3 px-3 py-2 text-xs font-semibold "
            "uppercase tracking-wider text-gray-500 border-b border-[#E5E9EE]"
        ):
            ui.label("Alias").classes("w-40")
            ui.label("RAPID path").classes("w-72 text-gray-400")
            ui.label("Type").classes("w-16")
            ui.label("Mode").classes("w-12")
            ui.label("Value").classes("flex-grow")
            ui.label("").classes("w-24 text-right")

    def _render_row(self, cfg: RobotVariableConfig) -> None:
        with ui.row().classes(
            "w-full items-center gap-3 px-3 py-2 text-sm "
            "border-b border-[#E5E9EE]"
        ):
            ui.label(cfg.alias).classes("w-40 font-medium")
            ui.label(f"{cfg.task}/{cfg.module}/{cfg.symbol}").classes(
                "w-72 font-mono text-xs text-gray-500 truncate"
            )
            ui.label(cfg.type).classes("w-16 font-mono text-xs uppercase")
            self._mode_chip(cfg)
            self._render_value_cell(cfg)
            self._render_action_cell(cfg)

    def _mode_chip(self, cfg: RobotVariableConfig) -> None:
        if cfg.mode == "rw":
            tone = "bg-[#0698D6]/10 text-[#0698D6] border-[#0698D6]/20"
        else:
            tone = "bg-gray-100 text-gray-500 border-gray-200"
        with ui.element("div").classes(
            f"w-12 inline-flex items-center justify-center rounded-md "
            f"text-xs font-semibold uppercase border {tone} py-0.5"
        ):
            ui.label(cfg.mode)

    def _render_value_cell(self, cfg: RobotVariableConfig) -> None:
        if cfg.mode == "rw":
            with ui.element("div").classes("flex-grow"):
                self._editors[cfg.alias] = self._make_editor(cfg)
        else:
            self._value_labels[cfg.alias] = ui.label("…").classes(
                "flex-grow font-mono"
            )

    def _make_editor(self, cfg: RobotVariableConfig) -> ui.element:
        if cfg.type == "num":
            return ui.number(value=0, format="%g").classes("w-40")
        if cfg.type == "bool":
            return ui.checkbox()
        return ui.input(value="").classes("w-64")

    def _render_action_cell(self, cfg: RobotVariableConfig) -> None:
        with ui.row().classes("w-24 justify-end gap-1"):
            if cfg.mode == "rw":
                ui.button(
                    "Set", icon="upload",
                    on_click=lambda c=cfg: self._submit(c),
                ).props("dense unelevated color=primary")

    # ---- runtime ------------------------------------------------------------

    def _refresh(self) -> None:
        for alias, label in self._value_labels.items():
            v = self.monitor.get(alias)
            label.text = "…" if v is None else _format_value(v)

    def _submit(self, cfg: RobotVariableConfig) -> None:
        editor = self._editors.get(cfg.alias)
        if editor is None:
            return
        new_value: object = editor.value
        if cfg.type == "num":
            try:
                new_value = float(new_value or 0)
            except (TypeError, ValueError):
                ui.notify(f"{cfg.alias}: invalid number", type="warning")
                return
        elif cfg.type == "bool":
            new_value = bool(new_value)
        else:
            new_value = "" if new_value is None else str(new_value)

        try:
            self.monitor.set(cfg.alias, new_value)
        except Exception as e:
            log.exception("robot var write failed")
            ui.notify(f"Write failed: {e}", type="negative")
            return
        ui.notify(f"{cfg.alias} = {_format_value(new_value)}", type="positive")


# ---- helpers ----------------------------------------------------------------

def _format_value(v) -> str:
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, float):
        return f"{v:g}"
    return str(v)
