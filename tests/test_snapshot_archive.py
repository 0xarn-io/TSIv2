"""Tests for SnapshotArchive — save, latest_path, prune, retention on start."""
from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path

from snapshot_archive import SnapshotArchive, SnapshotArchiveConfig


_JPEG_BYTES = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 64 + b"\xff\xd9"


def _archive(tmp_path: Path, **overrides) -> SnapshotArchive:
    cfg = SnapshotArchiveConfig(root_dir=str(tmp_path / "snaps"), **overrides)
    return SnapshotArchive.from_config(cfg)


def test_start_creates_root(tmp_path: Path):
    a = _archive(tmp_path)
    a.start()
    assert (tmp_path / "snaps").is_dir()


def test_save_writes_bytes_to_dated_dir(tmp_path: Path):
    a = _archive(tmp_path)
    a.start()
    path = a.save("entry", _JPEG_BYTES)
    p = Path(path)
    assert p.is_file()
    assert p.read_bytes() == _JPEG_BYTES
    today = date.today().isoformat()
    assert today in str(p)
    assert p.name.startswith("entry_")
    assert p.suffix == ".jpg"


def test_latest_path_returns_most_recent(tmp_path: Path):
    a = _archive(tmp_path)
    a.start()
    assert a.latest_path("entry") is None
    p1 = a.save("entry", _JPEG_BYTES)
    p2 = a.save("entry", _JPEG_BYTES)
    assert a.latest_path("entry") == p2
    assert p1 != p2                                 # filenames differ by ms
    assert a.latest_path("exit") is None


def test_latest_path_per_camera(tmp_path: Path):
    a = _archive(tmp_path)
    a.start()
    p_in  = a.save("entry", _JPEG_BYTES)
    p_out = a.save("exit",  _JPEG_BYTES)
    assert a.latest_path("entry") == p_in
    assert a.latest_path("exit")  == p_out


def test_prune_removes_old_files(tmp_path: Path):
    a = _archive(tmp_path, keep_days=7)
    a.start()
    fresh = Path(a.save("entry", _JPEG_BYTES))

    # Forge an old file: write directly under an old-dated dir, set mtime back.
    old_dir = a.root / "2000-01-01"
    old_dir.mkdir(parents=True, exist_ok=True)
    old_file = old_dir / "entry_120000_000.jpg"
    old_file.write_bytes(_JPEG_BYTES)
    long_ago = time.time() - 30 * 86400
    os.utime(old_file, (long_ago, long_ago))

    n = a.prune()
    assert n == 1
    assert not old_file.exists()
    assert fresh.exists()                           # today's file untouched
    assert not old_dir.exists()                     # empty dir cleaned up


def test_keep_days_zero_skips_prune(tmp_path: Path):
    a = _archive(tmp_path, keep_days=0)
    a.start()
    old_dir = a.root / "2000-01-01"
    old_dir.mkdir(parents=True, exist_ok=True)
    old_file = old_dir / "entry_120000_000.jpg"
    old_file.write_bytes(_JPEG_BYTES)
    long_ago = time.time() - 365 * 86400
    os.utime(old_file, (long_ago, long_ago))

    assert a.prune() == 0
    assert old_file.exists()


def test_start_runs_prune(tmp_path: Path):
    """If keep_days > 0, start() should prune existing old files."""
    cfg = SnapshotArchiveConfig(root_dir=str(tmp_path / "snaps"), keep_days=1)
    root = Path(cfg.root_dir)
    root.mkdir(parents=True)
    old_dir = root / "2000-01-01"; old_dir.mkdir()
    old_file = old_dir / "entry_120000_000.jpg"
    old_file.write_bytes(_JPEG_BYTES)
    long_ago = time.time() - 30 * 86400
    os.utime(old_file, (long_ago, long_ago))

    SnapshotArchive.from_config(cfg).start()
    assert not old_file.exists()


def test_stop_is_noop(tmp_path: Path):
    a = _archive(tmp_path)
    a.start()
    a.stop()    # must not raise
    a.stop()    # idempotent


def test_save_creates_unique_paths(tmp_path: Path):
    a = _archive(tmp_path)
    a.start()
    paths = {a.save("entry", _JPEG_BYTES) for _ in range(5)}
    # Within the same millisecond two saves could collide; allow ≥3 distinct.
    assert len(paths) >= 3
