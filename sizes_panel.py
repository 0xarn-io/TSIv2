"""sizes_panel.py — NiceGUI admin page for the SizesStore.

Two tabs (cardboard / others); per-row edit + delete; "+ Add" opens a
dialog with mm fields and a non-stored inches calculator.

Usage in dashboard.py:
    panel = SizesPanel(db.sizes)
    panel.mount()                     # call inside @ui.page
"""
from __future__ import annotations

import logging

from nicegui import ui

from sizes_store import Size, SizesStore, TABLES
from theme       import card

log = logging.getLogger(__name__)


def _mm_to_in(mm: int) -> int:
    """Display-only conversion. Round to nearest inch (matches operator quotes)."""
    return int(round(mm / 25.4))


def _in_to_mm(inches: int) -> int:
    return int(round(float(inches) * 25.4))


class SizesPanel:
    """Admin UI for cardboard + others size catalogs."""

    def __init__(self, store: SizesStore):
        self.store = store
        self._containers: dict[str, ui.column] = {}

    # ---- top-level mount ----------------------------------------------------

    def mount(self) -> None:
        with ui.column().classes("w-full gap-4 p-6"):
            with card():
                with ui.tabs().props("inline-label dense").classes(
                    "text-[#0053A1]"
                ) as tabs:
                    tab_objs = {t: ui.tab(t.capitalize()) for t in TABLES}
                with ui.tab_panels(
                    tabs, value=tab_objs[TABLES[0]],
                ).classes("w-full bg-transparent"):
                    for t in TABLES:
                        with ui.tab_panel(tab_objs[t]).classes("p-0 pt-3"):
                            self._build_tab(t)

    # ---- per-tab UI ---------------------------------------------------------

    def _build_tab(self, table: str) -> None:
        with ui.row().classes("items-center mb-3 gap-2"):
            ui.button(
                f"New {table[:-1] if table.endswith('s') else table}",
                icon="add",
                on_click=lambda t=table: self._open_dialog(t),
            ).props("color=primary unelevated")
            ui.button(
                "Refresh", icon="refresh",
                on_click=lambda t=table: self._refresh(t),
            ).props("flat color=primary")
        self._containers[table] = ui.column().classes("w-full gap-1")
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
                ui.label("No entries yet.").classes(
                    "text-gray-500 italic py-3"
                )
                return
            for s in rows:
                self._render_row(table, s)

    @staticmethod
    def _render_header() -> None:
        with ui.row().classes(
            "w-full items-center gap-3 px-3 py-2 text-xs font-semibold "
            "uppercase tracking-wider text-gray-500 border-b border-[#E5E9EE]"
        ):
            ui.label("ID").classes("w-12")
            ui.label("Slot").classes("w-14 text-right")
            ui.label("Name").classes("w-40")
            ui.label("W (mm)").classes("w-24 text-right")
            ui.label("L (mm)").classes("w-24 text-right")
            ui.label("≈ W (in)").classes("w-20 text-right text-gray-400")
            ui.label("≈ L (in)").classes("w-20 text-right text-gray-400")
            ui.label("").classes("flex-grow")

    def _render_row(self, table: str, s: Size) -> None:
        with ui.row().classes(
            "w-full items-center gap-3 px-3 py-2 text-sm "
            "border-b border-[#E5E9EE] hover:bg-[#F4F6F8]"
        ):
            ui.label(str(s.id)).classes(
                "w-12 font-mono text-xs text-gray-500"
            )
            slot_text = "—" if s.slot is None else str(s.slot)
            ui.label(slot_text).classes(
                "w-14 text-right font-mono text-xs text-[#0053A1]"
            )
            ui.label(s.name).classes("w-40 font-medium")
            ui.label(str(s.width_mm)).classes("w-24 text-right font-mono")
            ui.label(str(s.length_mm)).classes("w-24 text-right font-mono")
            ui.label(str(_mm_to_in(s.width_mm))).classes(
                "w-20 text-right font-mono text-gray-400"
            )
            ui.label(str(_mm_to_in(s.length_mm))).classes(
                "w-20 text-right font-mono text-gray-400"
            )
            with ui.row().classes("flex-grow justify-end gap-1"):
                ui.button(
                    icon="edit",
                    on_click=lambda s=s, t=table: self._open_dialog(t, s),
                ).props("dense flat color=primary").tooltip("Edit")
                ui.button(
                    icon="delete",
                    on_click=lambda sid=s.id, t=table: self._delete(t, sid),
                ).props("dense flat color=negative").tooltip("Delete")

    # ---- dialog (add / edit) ------------------------------------------------

    def _open_dialog(self, table: str, existing: Size | None = None) -> None:
        title = (f"Edit {table[:-1] if table.endswith('s') else table} "
                 f"#{existing.id}") if existing else f"New {table[:-1] if table.endswith('s') else table}"

        with ui.dialog() as dialog, ui.card().classes(
            "min-w-[460px] p-0 overflow-hidden"
        ):
            with ui.element("div").classes(
                "bg-[#0053A1] text-white px-5 py-3"
            ):
                ui.label(title).classes("text-base font-semibold")

            with ui.column().classes("gap-4 p-5 w-full"):
                with ui.row().classes("gap-3 w-full items-center"):
                    name = ui.input(
                        "Name",
                        value=existing.name if existing else "",
                    ).classes("flex-grow")
                    slot = ui.number(
                        "Slot (0–19, blank = none)",
                        value=(existing.slot if existing and existing.slot is not None
                               else None),
                        format="%d", min=0, max=19,
                    ).classes("w-48")

                with card("Dimensions (mm)"):
                    with ui.grid(columns=2).classes("w-full gap-3"):
                        wmm = ui.number(
                            "Width (mm)",
                            value=existing.width_mm if existing else 0,
                            format="%d",
                        )
                        lmm = ui.number(
                            "Length (mm)",
                            value=existing.length_mm if existing else 0,
                            format="%d",
                        )

                with card("Inches calculator (not stored)"):
                    with ui.grid(columns=2).classes("w-full gap-3"):
                        win = ui.number(
                            "Width (in)",
                            value=_mm_to_in(existing.width_mm) if existing else 0,
                            format="%d",
                        )
                        lin = ui.number(
                            "Length (in)",
                            value=_mm_to_in(existing.length_mm) if existing else 0,
                            format="%d",
                        )

                    def fill_mm_from_inches() -> None:
                        wmm.value = _in_to_mm(win.value or 0)
                        lmm.value = _in_to_mm(lin.value or 0)

                    ui.button(
                        "Fill mm from inches", icon="swap_horiz",
                        on_click=fill_mm_from_inches,
                    ).props("flat dense color=primary").classes("mt-2")

                def submit() -> None:
                    raw_slot = slot.value
                    slot_val: int | None = None
                    if raw_slot not in (None, ""):
                        try:
                            slot_val = int(raw_slot)
                        except (TypeError, ValueError):
                            ui.notify("Slot must be an integer 0–19", type="warning")
                            return
                    try:
                        s = Size(
                            id=existing.id if existing else None,
                            name=(name.value or "").strip(),
                            width_mm=int(wmm.value or 0),
                            length_mm=int(lmm.value or 0),
                            slot=slot_val,
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

                with ui.row().classes("justify-end gap-2 w-full pt-1"):
                    ui.button("Cancel", on_click=dialog.close).props("flat")
                    ui.button(
                        "Save", icon="save", on_click=submit,
                    ).props("color=primary unelevated")

        dialog.open()

    # ---- delete -------------------------------------------------------------

    def _delete(self, table: str, sid: int) -> None:
        with ui.dialog() as dialog, ui.card().classes("min-w-[360px]"):
            ui.label(f"Delete {table} #{sid}?").classes(
                "text-base font-semibold"
            )
            ui.label("This permanently removes the row.").classes(
                "text-sm text-gray-500"
            )
            with ui.row().classes("justify-end gap-2 w-full pt-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat")

                def go() -> None:
                    try:
                        self.store.delete(table, sid)
                    except Exception as e:
                        ui.notify(f"Delete failed: {e}", type="negative")
                        return
                    dialog.close()
                    self._refresh(table)

                ui.button(
                    "Delete", icon="delete", on_click=go,
                ).props("color=negative unelevated")
        dialog.open()
