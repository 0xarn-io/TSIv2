"""errors_panel.py — read-only NiceGUI viewer for the ErrorsStore.

Top filter bar (severity dropdown + device substring + manual refresh),
auto-refresh every 5 s, color-coded rows by severity. All NiceGUI
imports stay inside this module.
"""
from __future__ import annotations

import logging

from nicegui import ui

from errors_store import ErrorsStore
from theme        import card, severity_classes

log = logging.getLogger(__name__)

_SEVERITIES_FILTER = ("all", "info", "warning", "error", "critical")
_REFRESH_INTERVAL_S = 5.0
_LIMIT = 200


class ErrorsPanel:
    """Live error log viewer (read-only)."""

    def __init__(self, store: ErrorsStore):
        self.store = store
        self._severity = "all"
        self._device   = ""
        self._rows_container: ui.column | None = None
        self._timer: ui.timer | None = None

    # ---- top-level mount ----------------------------------------------------

    def mount(self) -> None:
        with ui.column().classes("w-full gap-4 p-6"):
            self._render_filters()
            self._rows_container = ui.column().classes("w-full gap-1")
            # Parent the timer to the page container so NiceGUI destroys
            # it when the user navigates away. Stops orphaned ticks
            # raising "parent slot deleted".
            self._timer = ui.timer(_REFRESH_INTERVAL_S, self._refresh)
        self._refresh()

    # ---- filter bar ---------------------------------------------------------

    def _render_filters(self) -> None:
        with card():
            with ui.row().classes("items-center gap-3 w-full"):
                ui.select(
                    list(_SEVERITIES_FILTER),
                    value=self._severity,
                    label="Severity",
                    on_change=lambda e: self._set_severity(e.value),
                ).classes("w-40")
                ui.input(
                    label="Device contains",
                    placeholder="python, robot, plc…",
                    on_change=lambda e: self._set_device(e.value or ""),
                ).classes("w-64").props("clearable")
                ui.space()
                ui.button(
                    "Refresh", icon="refresh",
                    on_click=self._refresh,
                ).props("flat color=primary")

    def _set_severity(self, v: str) -> None:
        self._severity = v or "all"
        self._refresh()

    def _set_device(self, v: str) -> None:
        self._device = v.strip()
        self._refresh()

    # ---- rows ---------------------------------------------------------------

    def _refresh(self) -> None:
        if self._rows_container is None:
            return
        # The container/timer pair lives across page visits because the
        # panel itself is a Dashboard singleton. If the user navigated
        # away, NiceGUI deletes the previous client; the next timer tick
        # then accesses an Element whose .client is gone and raises
        # RuntimeError. Detect that, drop our stale refs, and stop the
        # timer — the next page mount will create fresh ones.
        try:
            self._rows_container.clear()
        except RuntimeError:
            self._discard_stale_state()
            return
        try:
            rows = self._fetch()
        except Exception as e:
            with self._rows_container:
                ui.label(f"Failed to load errors: {e}").classes("text-red-500")
            return

        with self._rows_container:
            self._render_header()
            if not rows:
                ui.label("No errors logged.").classes(
                    "text-gray-500 italic px-3 py-4"
                )
                return
            for r in rows:
                self._render_row(r)

    def _discard_stale_state(self) -> None:
        self._rows_container = None
        if self._timer is not None:
            try: self._timer.delete()
            except Exception: pass
            self._timer = None

    def _fetch(self) -> list[dict]:
        # Substring filter on device → ad-hoc query. Otherwise use recent().
        if self._device:
            sql = "SELECT * FROM error_log WHERE device LIKE ?"
            params: list = [f"%{self._device}%"]
            if self._severity != "all":
                sql += " AND severity = ?"
                params.append(self._severity)
            sql += " ORDER BY id DESC LIMIT ?"
            params.append(_LIMIT)
            return self.store.query(sql, params)

        sev = None if self._severity == "all" else self._severity
        return self.store.recent(limit=_LIMIT, severity=sev)

    def _render_header(self) -> None:
        with ui.row().classes(
            "w-full items-center gap-3 px-3 py-2 text-xs font-semibold "
            "uppercase tracking-wider text-gray-500 border-b border-[#E5E9EE]"
        ):
            ui.label("Time").classes("w-44")
            ui.label("Severity").classes("w-20")
            ui.label("Device").classes("w-28")
            ui.label("Code").classes("w-14 text-right")
            ui.label("Title").classes("flex-grow")

    def _render_row(self, r: dict) -> None:
        sev = r.get("severity") or ""
        tint = severity_classes(sev)
        with ui.row().classes(
            f"w-full items-center gap-3 px-3 py-2 text-sm "
            f"border-b border-[#E5E9EE] {tint}"
        ):
            ui.label(r.get("ts") or "").classes("w-44 text-gray-700 font-mono text-xs")
            ui.label(sev.upper()).classes(
                f"w-20 text-xs font-semibold {_severity_text(sev)}"
            )
            with ui.column().classes("w-28 gap-0"):
                ui.label(r.get("device") or "").classes("text-sm")
                if r.get("subsystem"):
                    ui.label(r["subsystem"]).classes("text-xs text-gray-500")
            ui.label(str(r.get("code") or "")).classes("w-14 text-right font-mono")
            with ui.column().classes("flex-grow gap-0"):
                ui.label(r.get("title") or "").classes("text-sm font-medium")
                msg = r.get("message")
                if msg:
                    ui.label(msg).classes("text-xs text-gray-600 truncate")


def _severity_text(severity: str) -> str:
    return {
        "info":     "text-blue-700",
        "warning":  "text-yellow-700",
        "error":    "text-red-700",
        "critical": "text-red-900",
    }.get(severity, "text-gray-600")
