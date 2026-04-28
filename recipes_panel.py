"""recipes_panel.py — NiceGUI admin page for the RecipesStore.

List + create / edit / delete with a single dialog laid out in three
sections (X side / Y side / Wood). Uses theme.card for visual grouping
and theme.PRIMARY for section accents — no NiceGUI primitives leak into
non-UI modules.

Usage in dashboard.py:
    panel = RecipesPanel(db.recipes)
    panel.mount()                     # call inside @ui.page
"""
from __future__ import annotations

import logging
from dataclasses import asdict

from nicegui import ui

from recipes_store import Recipe, RecipesStore
from theme         import card

log = logging.getLogger(__name__)


class RecipesPanel:
    """Recipes admin: list + create + edit (with rename) + hard delete."""

    def __init__(self, store: RecipesStore):
        self.store = store
        self._list_container: ui.column | None = None

    # ---- top-level mount ----------------------------------------------------

    def mount(self) -> None:
        with ui.column().classes("w-full gap-4 p-6"):
            self._render_toolbar()
            self._list_container = ui.column().classes("w-full gap-3")
        self._refresh()

    # ---- toolbar ------------------------------------------------------------

    def _render_toolbar(self) -> None:
        with card():
            with ui.row().classes("items-center gap-2"):
                ui.button(
                    "New recipe", icon="add",
                    on_click=lambda: self._open_dialog(),
                ).props("color=primary unelevated")
                ui.button(
                    "Refresh", icon="refresh",
                    on_click=self._refresh,
                ).props("flat color=primary")
                ui.space()
                ui.label().bind_text_from(
                    self, "_count_label",
                ).classes("text-sm text-gray-500")

    @property
    def _count_label(self) -> str:
        try:
            return f"{len(self.store.list(active_only=False))} recipes"
        except Exception:
            return ""

    # ---- list ---------------------------------------------------------------

    def _refresh(self) -> None:
        if self._list_container is None:
            return
        self._list_container.clear()
        with self._list_container:
            try:
                rows = self.store.list(active_only=False)
            except Exception as e:
                ui.label(f"Failed to load recipes: {e}").classes("text-red-500")
                return
            if not rows:
                with card():
                    ui.label("No recipes yet — click New recipe to add one."
                             ).classes("text-gray-500 italic")
                return
            for r in rows:
                self._render_row(r)

    def _render_row(self, r: Recipe) -> None:
        with card(padding="p-4"):
            with ui.row().classes("w-full items-center gap-4"):
                # Big code badge
                with ui.element("div").classes(
                    "rounded-lg bg-[#0053A1] text-white px-3 py-1 "
                    "font-semibold text-sm tracking-wide"
                ):
                    ui.label(f"#{r.code}")

                with ui.column().classes("gap-0 flex-grow"):
                    ui.label(_summary(r)).classes("text-sm font-semibold")
                    ui.label(_subline(r)).classes("text-xs text-gray-500")

                # Wood pill (only when set)
                if r.wood:
                    ui.label("Wood").classes(
                        "text-xs font-semibold uppercase tracking-wider "
                        "bg-[#118938]/10 text-[#118938] rounded-full px-3 py-1"
                    )

                with ui.row().classes("gap-1"):
                    ui.button(
                        icon="edit",
                        on_click=lambda r=r: self._open_dialog(r),
                    ).props("dense flat color=primary").tooltip("Edit")
                    ui.button(
                        icon="delete",
                        on_click=lambda r=r: self._confirm_delete(r),
                    ).props("dense flat color=negative").tooltip("Delete")

    # ---- add / edit dialog --------------------------------------------------

    def _open_dialog(self, existing: Recipe | None = None) -> None:
        is_edit = existing is not None
        original_code = existing.code if existing else None

        with ui.dialog() as dialog, ui.card().classes(
            "min-w-[640px] max-h-[90vh] p-0 overflow-hidden"
        ):
            with ui.element("div").classes(
                "bg-[#0053A1] text-white px-5 py-3"
            ):
                ui.label(f"Edit recipe #{original_code}" if is_edit
                         else "New recipe").classes(
                    "text-base font-semibold"
                )

            # Disclaimer banner — every numeric field below is in mm.
            with ui.element("div").classes(
                "bg-[#0053A1]/5 border-b border-[#0053A1]/10 px-5 py-2 "
                "text-xs text-[#0053A1] flex items-center gap-2"
            ):
                ui.icon("straighten").classes("text-base")
                ui.label("All measurements are in millimeters (mm).")

            # Scrollable body — keeps Save/Cancel pinned and visible.
            with ui.column().classes(
                "gap-4 p-5 w-full overflow-y-auto"
            ).style("max-height: calc(90vh - 180px)"):
                code = ui.number(
                    "Code", value=existing.code if existing else 1,
                    format="%d", min=0,
                ).classes("w-32")

                # ---- X side ----
                inputs_x = self._dim_section(
                    "X side", existing,
                    fields={
                        "x_topsheet_length": "Topsheet length",
                        "x_topsheet_width":  "Topsheet width",
                        "x_units":           "Units",
                        "x1_pos":            "X1 pos",
                        "x2_pos":            "X2 pos",
                        "x3_pos":            "X3 pos",
                    },
                    fold_field="x_folding",
                    fold_label="X folding",
                )

                # ---- Y side ----
                inputs_y = self._dim_section(
                    "Y side", existing,
                    fields={
                        "y_topsheet_length": "Topsheet length",
                        "y_topsheet_width":  "Topsheet width",
                        "y_units":           "Units",
                        "y1_pos":            "Y1 pos",
                        "y2_pos":            "Y2 pos",
                        "y3_pos":            "Y3 pos",
                    },
                    fold_field="y_folding",
                    fold_label="Y folding",
                )

                # ---- Wood ----
                inputs_w = self._wood_section(existing)

            # Pinned action bar — stays visible while the body scrolls.
            with ui.element("div").classes(
                "border-t border-[#E5E9EE] px-5 py-3 bg-[#F8FAFC]"
            ):
                with ui.row().classes("justify-end gap-2 w-full"):
                    ui.button("Cancel", on_click=dialog.close).props("flat")
                    ui.button(
                        "Save", icon="save",
                        on_click=lambda: self._submit(
                            dialog, original_code,
                            int(code.value or 0),
                            inputs_x, inputs_y, inputs_w,
                            existing,
                        ),
                    ).props("color=primary unelevated")

        dialog.open()

    def _dim_section(
        self,
        title: str, existing: Recipe | None,
        *, fields: dict[str, str],
        fold_field: str, fold_label: str,
    ) -> dict[str, ui.element]:
        """Render one of the X/Y dimension cards. Returns the input map."""
        inputs: dict[str, ui.element] = {}
        with card(title):
            with ui.grid(columns=3).classes("w-full gap-3"):
                for fname, label in fields.items():
                    inputs[fname] = ui.number(
                        label,
                        value=getattr(existing, fname) if existing else 0,
                        format="%d",
                    )
            inputs[fold_field] = ui.checkbox(
                fold_label,
                value=getattr(existing, fold_field) if existing else False,
            ).classes("mt-2")
        return inputs

    def _wood_section(self, existing: Recipe | None) -> dict[str, ui.element]:
        inputs: dict[str, ui.element] = {}
        with card("Wood"):
            inputs["wood"] = ui.checkbox(
                "Enable wood",
                value=existing.wood if existing else False,
            )
            with ui.grid(columns=2).classes("w-full gap-3 mt-2"):
                inputs["wood_x_pos"] = ui.number(
                    "Wood X pos",
                    value=existing.wood_x_pos if existing else 0,
                    format="%d",
                )
                inputs["wood_y_pos"] = ui.number(
                    "Wood Y pos",
                    value=existing.wood_y_pos if existing else 0,
                    format="%d",
                )
        return inputs

    def _submit(
        self,
        dialog: ui.dialog,
        original_code: int | None,
        new_code: int,
        inputs_x: dict, inputs_y: dict, inputs_w: dict,
        existing: Recipe | None,
    ) -> None:
        if new_code <= 0:
            ui.notify("Code must be > 0", type="warning")
            return

        # Assemble the Recipe.
        kwargs = {"code": new_code, "active": True}
        for src in (inputs_x, inputs_y, inputs_w):
            for k, w in src.items():
                v = w.value
                kwargs[k] = bool(v) if isinstance(v, bool) else int(v or 0)
        recipe = Recipe(**kwargs)

        try:
            # Rename path: existing row at original_code, code changed.
            if original_code is not None and new_code != original_code:
                self.store.rename(original_code, new_code)
                # `rename` keeps the row's data; now overwrite with edits.
            self.store.save(recipe)
        except (ValueError, KeyError) as e:
            ui.notify(f"Save failed: {e}", type="negative")
            return
        except Exception as e:
            log.exception("recipes save failed")
            ui.notify(f"Save failed: {e}", type="negative")
            return

        dialog.close()
        ui.notify(
            f"Recipe #{new_code} {'updated' if existing else 'created'}",
            type="positive",
        )
        self._refresh()

    # ---- delete -------------------------------------------------------------

    def _confirm_delete(self, r: Recipe) -> None:
        with ui.dialog() as dialog, ui.card().classes("min-w-[360px]"):
            ui.label(f"Delete recipe #{r.code}?").classes("text-base font-semibold")
            ui.label("This permanently removes the row.").classes(
                "text-sm text-gray-500"
            )
            with ui.row().classes("justify-end gap-2 w-full pt-2"):
                ui.button("Cancel", on_click=dialog.close).props("flat")

                def go() -> None:
                    try:
                        self.store.delete(r.code)
                    except Exception as e:
                        ui.notify(f"Delete failed: {e}", type="negative")
                        return
                    dialog.close()
                    ui.notify(f"Recipe #{r.code} deleted", type="positive")
                    self._refresh()

                ui.button(
                    "Delete", icon="delete", on_click=go,
                ).props("color=negative unelevated")
        dialog.open()


# ---- pure helpers (no UI) ---------------------------------------------------

def _summary(r: Recipe) -> str:
    return (
        f"X {r.x_topsheet_length}×{r.x_topsheet_width} ×{r.x_units}    "
        f"Y {r.y_topsheet_length}×{r.y_topsheet_width} ×{r.y_units}"
    )


def _subline(r: Recipe) -> str:
    fold_x = "fold X" if r.x_folding else ""
    fold_y = "fold Y" if r.y_folding else ""
    bits = [
        f"X pos {r.x1_pos}/{r.x2_pos}/{r.x3_pos}",
        f"Y pos {r.y1_pos}/{r.y2_pos}/{r.y3_pos}",
    ]
    if fold_x: bits.append(fold_x)
    if fold_y: bits.append(fold_y)
    if r.wood: bits.append(f"wood @ ({r.wood_x_pos}, {r.wood_y_pos})")
    return "   ·   ".join(bits)
