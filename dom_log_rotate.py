#!/usr/bin/env python3
"""
dom_log_rotate.py — daily rotation for the Sierra DOM dump files.

WHY THIS EXISTS
---------------
2026-08-20: C: filled to 9.4 MB free while the market was open. Sierra could no
longer append to ESU26-CME.scid, so 14.7 hours of overnight ES ticks
(18:48 Aug 19 -> 09:32 Aug 20) were lost permanently.

The four files below are the growth driver. They are written by the Sierra ACS
studies ESDomSnapshot / VXDomSnapshot, which have NO market-hours guard — they
emit roughly one snapshot per second around the clock, including weekends.

SAFE TO ROTATE because:
  1. Nothing in the trading system reads these files. No detector, no filter,
     no trade path. The bridge only TAILS them and POSTs to Railway.
  2. Their contents already live in Postgres (vps_es_dom_snapshots), so the
     local files are a raw duplicate of data we already hold.
  3. vps_data_bridge._check_vx_dom/_check_es_dom already handle rotation:
        if size < self._vx_dom_last_pos:
            self._vx_dom_last_pos = 0   # file rotated/truncated
     so a shrinking file makes the tailer restart cleanly at offset 0.
  4. The studies open with fopen(path,"ab") and CLOSE per snapshot, so the file
     is unlocked between writes and a rename lands in that gap.

Run daily after the close (16:05 ET) via Task Scheduler.

    python dom_log_rotate.py            # rotate + prune
    python dom_log_rotate.py --dry-run  # show what would happen
    python dom_log_rotate.py --status   # just print current sizes
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(r"C:\SierraChart\Data")

# The four files the DOM studies append to.
TARGETS = [
    "vx_dom.jsonl",
    "es_dom.jsonl",
    "vx_dom_features.csv",
    "es_dom_features.csv",
]

KEEP_DAYS = 4              # keep this many rotated generations per file
MIN_ROTATE_BYTES = 1024    # don't bother rotating a file this small
RENAME_RETRIES = 5         # Sierra closes between writes; retry across that gap
RENAME_BACKOFF_S = 0.4

LOG_PATH = Path(__file__).with_name("dom_rotate.log")

# vx_dom.jsonl -> vx_dom.jsonl.2026-08-20
_ROTATED_RE = re.compile(r"\.(\d{4}-\d{2}-\d{2})$")


def log(msg: str) -> None:
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {msg}"
    print(line)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass  # never let logging break rotation


def _mb(n: float) -> str:
    return f"{n / 1024 / 1024:,.1f} MB"


def free_mb() -> float:
    try:
        import shutil
        return shutil.disk_usage(str(DATA_DIR)).free / 1024 / 1024
    except Exception:
        return -1.0


def rotate_one(path: Path, dry: bool) -> int:
    """Rename path -> path.YYYY-MM-DD. Returns bytes rotated (0 if skipped)."""
    if not path.exists():
        log(f"  skip   {path.name}: does not exist")
        return 0

    size = path.stat().st_size
    if size < MIN_ROTATE_BYTES:
        log(f"  skip   {path.name}: only {size} bytes")
        return 0

    stamp = datetime.now().strftime("%Y-%m-%d")
    dest = path.with_name(f"{path.name}.{stamp}")

    # Already rotated today — fold today's new data into the same generation
    # rather than clobbering it or making a second stamp.
    if dest.exists():
        log(f"  skip   {path.name}: already rotated today ({dest.name} exists)")
        return 0

    if dry:
        log(f"  DRY    {path.name} -> {dest.name} ({_mb(size)})")
        return size

    # The study holds the file only for the instant of one fopen/fwrite/fclose.
    # Retry briefly so the rename lands between two snapshots.
    for attempt in range(1, RENAME_RETRIES + 1):
        try:
            os.replace(path, dest)
            log(f"  rotate {path.name} -> {dest.name} ({_mb(size)})")
            return size
        except (PermissionError, OSError) as e:
            if attempt == RENAME_RETRIES:
                log(f"  FAIL   {path.name}: {e} (left in place, will retry tomorrow)")
                return 0
            time.sleep(RENAME_BACKOFF_S)
    return 0


def prune(stem: str, dry: bool) -> int:
    """Delete rotated generations of `stem` older than KEEP_DAYS. Returns bytes freed."""
    cutoff = (datetime.now() - timedelta(days=KEEP_DAYS)).date()
    freed = 0
    for p in sorted(DATA_DIR.glob(f"{stem}.*")):
        m = _ROTATED_RE.search(p.name)
        if not m:
            continue  # not one of ours — never touch it
        try:
            when = datetime.strptime(m.group(1), "%Y-%m-%d").date()
        except ValueError:
            continue
        if when >= cutoff:
            continue
        size = p.stat().st_size
        if dry:
            log(f"  DRY    prune {p.name} ({_mb(size)})")
            freed += size
            continue
        try:
            p.unlink()
            log(f"  prune  {p.name} ({_mb(size)})")
            freed += size
        except Exception as e:
            log(f"  FAIL   prune {p.name}: {e}")
    return freed


def status() -> None:
    log(f"DOM file status — free disk {free_mb():,.0f} MB")
    total = 0
    for name in TARGETS:
        p = DATA_DIR / name
        live = p.stat().st_size if p.exists() else 0
        rot = sorted(DATA_DIR.glob(f"{name}.*"))
        rot = [r for r in rot if _ROTATED_RE.search(r.name)]
        rot_bytes = sum(r.stat().st_size for r in rot)
        total += live + rot_bytes
        log(f"  {name:24s} live {_mb(live):>12s}   "
            f"rotated {len(rot)} gen {_mb(rot_bytes):>12s}")
    log(f"  {'TOTAL':24s}      {_mb(total):>12s}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Rotate Sierra DOM dump files")
    ap.add_argument("--dry-run", action="store_true", help="show actions, change nothing")
    ap.add_argument("--status", action="store_true", help="print sizes and exit")
    args = ap.parse_args()

    if not DATA_DIR.exists():
        log(f"ERROR: {DATA_DIR} not found")
        return 2

    if args.status:
        status()
        return 0

    before = free_mb()
    log(f"=== DOM rotation start (keep {KEEP_DAYS} days) — free {before:,.0f} MB ===")

    rotated = freed = 0
    for name in TARGETS:
        rotated += rotate_one(DATA_DIR / name, args.dry_run)
        freed += prune(name, args.dry_run)

    after = free_mb()
    log(f"=== done — rotated {_mb(rotated)}, pruned {_mb(freed)}, "
        f"free {after:,.0f} MB ({after - before:+,.0f} MB) ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
