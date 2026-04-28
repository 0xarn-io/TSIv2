"""sizes_panel.py — NiceGUI admin page for the SizesStore.

Two tabs (cardboard / others); per-row edit + delete; "+ Add" opens a
dialog. All writes go straight through SizesStore (it's thread-safe).

Usage in Main.py:
    sizes_panel = SizesPanel(db.sizes) if db.sizes else None

    @ui.page("/sizes")
    def sizes_page():
        if sizes_panel: sizes_panel.mount()
"""
from __future__ import annotations

import logging

from nicegui import ui

from sizes_store import Size, SizesStore, TABLES

log = logging.getLogger(__name__)


class SizesPanel:
    """Admin UI for cardboard + others size catalogs."""

    def __init__(self, store: SizesStore):
        self.store = store
        self._containers: dict[str, ui.column] = {}

    def mount(self) -> None:
        """Build the page. Call inside a @ui.page function."""
        ui.label("Sizes").classes("text-2xl font-bold mb-2")
        with ui.tabs() as tabs:
            tab_objs = {t: ui.tab(t.capitalize()) for t in TABLES}
        with ui.tab_panels(tabs, value=tab_objs[TABLES[0]]).classes("w-full"):
            for t in TABLES:
                with ui.tab_panel(tab_objs[t]):
                    self._build_tab(t)

    # ---- per-tab UI ---------------------------------------------------------

    def _build_tab(self, table: str) -> None:
        with ui.row().classes("items-center mb-2"):
            ui.button(
                "Add size", icon="add",
                on_click=lambda t=table: self._open_dialog(t),
            ).props("color=primary")
            ui.button(
                "Refresh", icon="refresh",
                on_click=lambda t=table: self._refresh(t),
            ).props("flat")

        self._containers[table] = ui.column().classes("w-full")
        self._refresh(table)

    def _refresh(self, table: str) -> None:
        col = self._containers[table]
        col.clear()
        with col:
            self._render_header()
            try:
                rows = self.store.list(table)
            except Exception as e:
                ui.label(f"Failed to load: {e}").classes("text-red-500")
                return
            if not rows:
                ui.label("No entries yet.").classes("text-gray-500 italic py-2")
                return
            for s in rows:
                self._render_row(table, s)

    @staticmethod
    def _render_header() -> None:
        with ui.row().classes(
            "w-full items-center font-bold border-b py-1 text-sm"
        ):
            ui.label("ID").classes("w-12")
            ui.label("Name").classes("w-40")
            ui.label("W (mm)").classes("w-24 text-right")
            ui.label("L (mm)").classes("w-24 text-right")
            ui.label("W (in)").classes("w-24 text-right")
            ui.label("L (in)").classes("w-24 text-right")
            ui.label("").classes("flex-grow")

    def _render_row(self, table: str, s: Size) -> None:
        with ui.row().classes("w-full items-center border-b py-1 text-sm"):
            ui.label(str(s.id)).classes("w-12")
            ui.label(s.name).classes("w-40")
            ui.label(str(s.width_mm)).classes("w-24 text-right")
            ui.label(str(s.length_mm)).classes("w-24 text-right")
            ui.label(f"{s.width_in:.2f}").classes("w-24 text-right")
            ui.label(f"{s.length_in:.2f}").classes("w-24 text-right")
            with ui.row().classes("flex-grow justify-end gap-1"):
                ui.button(
                    icon="edit",
                    on_click=lambda s=s, t=table: self._open_dialog(t, s),
                ).props("dense flat")
                ui.button(
                    icon="delete",
                    on_click=lambda sid=s.id, t=table: self._delete(t, sid),
                ).props("dense flat color=negative")

    # ---- dialog (add / edit) ------------------------------------------------

    def _open_dialog(self, table: str, existing: Size | None = None) -> None:
        title = "Edit size" if existing else f"Add to {table}"

        with ui.dialog() as dialog, ui.card().classes("min-w-[360px]"):
            ui.label(title).classes("text-lg font-bold")

            name = ui.input("Name", value=existing.name if existing else "")
            wmm  = ui.number("Width (mm)",  value=existing.width_mm  if existing else 0,   format="%d")
            lmm  = ui.number("Length (mm)", value=existing.length_mm if existing else 0,   format="%d")
            win  = ui.number("Width (in)",  value=existing.width_in  if existing else 0.0, format="%.2f")
            lin  = ui.number("Length (in)", value=existing.length_in if existing else 0.0, format="%.2f")

            def fill_inches_from_mm() -> None:
                # 1 in = 25.4 mm
                win.value = round(float(wmm.value or 0) / 25.4, 2)
                lin.value = round(float(lmm.value or 0) / 25.4, 2)

            ui.button(
                "Fill inches from mm", icon="swap_horiz",
                on_click=fill_inches_from_mm,
            ).props("flat dense")

            def submit() -> None:
                try:
                    s = Size(
                        id=existing.id if existing else None,
                        name=(name.value or "").strip(),
                        width_mm=int(wmm.value or 0),
                        length_mm=int(lmm.value or 0),
                        width_in=float(win.value or 0.0),
                        length_in=float(lin.value or 0.0),
                    )
                    if not s.name:
                        ui.notify("Name is required", type="warning")
                        return
                    if existing:
                        self.store.update(table, s)
                    else:
                        self.store.add(table, s)
                except Exception as e:
                    log.exception("sizes save failed")
                    ui.notify(f"Save failed: {e}", type="negative")
                    return
                dialog.close()
                self._refresh(table)

            with ui.row().classes("justify-end w-full"):
                ui.button("Cancel", on_click=dialog.close).props("flat")
                ui.button("Save", on_click=submit).props("color=primary")

        dialog.open()

    # ---- delete -------------------------------------------------------------

    def _delete(self, table: str, sid: int) -> None:
        with ui.dialog() as dialog, ui.card():
            ui.label(f"Delete row id={sid} from {table}?")
            with ui.row().classes("justify-end w-full"):
                ui.button("Cancel", on_click=dialog.close).props("flat")
                def go() -> None:
                    try:
                        self.store.delete(table, sid)
                    except Exception as e:
                        ui.notify(f"Delete failed: {e}", type="negative")
                        return
                    dialog.close()
                    self._refresh(table)
                ui.button("Delete", on_click=go).props("color=negative")
        dialog.open()
