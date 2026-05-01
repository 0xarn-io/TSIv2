"""snapshot_archive.py — saves camera JPEGs to disk, indexed by date + name.

    archive = SnapshotArchive.from_config(cfg.snapshots)
    archive.start()                                  # one-shot prune
    path = archive.save("entry", jpeg_bytes)         # → "<root>/2026-04-28/entry_073412_123.jpg"

Pairs with `CameraManager` so each captured frame is persisted; the unit
log then references these paths by camera name.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class SnapshotArchiveConfig:
    root_dir:  str
    keep_days: int = 90          # 0 = keep forever


class SnapshotArchive:
    """Date-bucketed JPEG archive. Append-only, no DB."""

    def __init__(self, cfg: SnapshotArchiveConfig):
        self.cfg = cfg
        self.root = Path(cfg.root_dir)
        self._latest: dict[str, str] = {}
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, cfg: SnapshotArchiveConfig) -> "SnapshotArchive":
        return cls(cfg)

    # ---- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        if self.cfg.keep_days > 0:
            try:
                n = self.prune()
                if n:
                    log.info("snapshot_archive: pruned %d old file(s)", n)
            except Exception as e:
                log.warning("snapshot_archive: prune failed: %s", e)

    def stop(self) -> None:
        # No background work; nothing to release.
        pass

    # ---- public API ---------------------------------------------------------

    def save(self, name: str, jpeg_bytes: bytes) -> str:
        """Save bytes to <root>/<YYYY-MM-DD>/<name>_<HHMMSS>_<us>.jpg, return path."""
        now = datetime.now()
        day_dir = self.root / now.strftime("%Y-%m-%d")
        day_dir.mkdir(parents=True, exist_ok=True)

        # Microsecond suffix avoids collisions on bursty calls.
        fname = f"{name}_{now.strftime('%H%M%S')}_{now.microsecond:06d}.jpg"
        path = day_dir / fname
        path.write_bytes(jpeg_bytes)

        path_str = str(path)
        with self._lock:
            self._latest[name] = path_str
        return path_str

    def latest_path(self, name: str) -> str | None:
        with self._lock:
            return self._latest.get(name)

    def prune(self) -> int:
        """Delete .jpg files with mtime older than keep_days. Returns count."""
        if self.cfg.keep_days <= 0 or not self.root.is_dir():
            return 0
        cutoff = time.time() - self.cfg.keep_days * 86400
        removed = 0
        for f in self.root.rglob("*.jpg"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
            except OSError:
                pass
        # Clean up now-empty day directories.
        for d in sorted([p for p in self.root.iterdir() if p.is_dir()],
                        key=lambda p: p.name, reverse=True):
            try:
                if not any(d.iterdir()):
                    d.rmdir()
            except OSError:
                pass
        return removed
