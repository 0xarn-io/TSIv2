"""box_scene.py — 3D NiceGUI scene with N boxes sized from config (mm).

Usage in main.py:
    scene = BoxScene.from_config(cfg.boxes)
    scene.on_click(lambda i: print(f"Clicked box {i}"))

    @ui.page("/")
    def index():
        scene.mount()
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from nicegui import ui

MM_TO_M = 0.001


@dataclass
class BoxConfig:
    width_mm:  int
    height_mm: int
    depth_mm:  int
    x_pos:     float = 0.0
    color:     str = "#a1845a"


class BoxScene:
    """N boxes laid out in a NiceGUI 3D scene. Click events go to subscribers."""

    def __init__(self, boxes: list[BoxConfig], width: int = 800, height: int = 500):
        self._cfgs = list(boxes)
        self._canvas_w = width
        self._canvas_h = height
        self._boxes: list = []
        self._click_cbs: list[Callable[[int], None]] = []

    @classmethod
    def from_config(cls, boxes: list[BoxConfig], **kwargs) -> "BoxScene":
        return cls(boxes, **kwargs)

    def on_click(self, cb: Callable[[int], None]) -> Callable[[], None]:
        """Register a click callback. Returns an unsubscribe function."""
        self._click_cbs.append(cb)
        return lambda: self._click_cbs.remove(cb)

    def mount(self) -> None:
        """Mount the canvas. Call inside a @ui.page function."""
        self._boxes.clear()
        with ui.scene(
            width=self._canvas_w, height=self._canvas_h, on_click=self._handle_click,
        ).classes("border rounded") as scene:
            for cfg in self._cfgs:
                box = scene.box(1, 1, 1).material(cfg.color)
                self._apply_size(box, cfg)
                self._boxes.append(box)

    def set_size(
        self, index: int, *,
        width_mm: int | None = None,
        height_mm: int | None = None,
        depth_mm: int | None = None,
    ) -> None:
        """Update one box's dimensions live. Pass any subset."""
        cfg = self._cfgs[index]
        if width_mm  is not None: cfg.width_mm  = width_mm
        if height_mm is not None: cfg.height_mm = height_mm
        if depth_mm  is not None: cfg.depth_mm  = depth_mm
        if index < len(self._boxes):
            self._apply_size(self._boxes[index], cfg)

    # ---- internals ----

    def _handle_click(self, e) -> None:
        if not e.hits:
            return
        hit = e.hits[0]
        for i, box in enumerate(self._boxes):
            if hit.object_id == box.id:
                for cb in list(self._click_cbs):
                    try:
                        cb(i)
                    except Exception as ex:
                        print(f"[WARN] box click callback failed: {ex}")
                return

    @staticmethod
    def _apply_size(box, cfg: BoxConfig) -> None:
        w = cfg.width_mm  * MM_TO_M
        h = cfg.height_mm * MM_TO_M
        d = cfg.depth_mm  * MM_TO_M
        box.scale(w, h, d)
        box.move(x=cfg.x_pos, y=h / 2)
