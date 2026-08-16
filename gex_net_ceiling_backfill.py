# -*- coding: utf-8 -*-
"""One-time backfill of setup_log.gex_net_ceiling (V18 input).

The nightly job `gex_state.stamp_setups(3)` only reaches back 3 days, so history
needs one pass. Uses the SAME `gex_state.compute()` the live path uses — the whole
point of that module is that there is exactly one implementation.

DB discipline (2026-06-03 outage): AUTOCOMMIT + a commit per day. A single long
read transaction against prod holds AccessShareLock and blocks db_init()'s startup
ALTERs, which crash-loops every deploy.

Run:  python gex_net_ceiling_backfill.py [--since 2026-02-19] [--dry-run]
"""
import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from app.gex_state import compute  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-02-19")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    eng = create_engine(os.environ["DATABASE_URL"])
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        c.execute(text("ALTER TABLE setup_log ADD COLUMN IF NOT EXISTS "
                       "gex_net_ceiling double precision"))
        days = [r[0] for r in c.execute(text(
            "SELECT DISTINCT date(ts AT TIME ZONE 'America/New_York') d FROM setup_log "
            "WHERE ts >= :s AND gex_net_ceiling IS NULL ORDER BY d"),
            {"s": args.since}).fetchall()]
        print(f"{len(days)} sessions need stamping (from {args.since})")

        total = miss = 0
        for d in days:
            snaps = c.execute(text(
                "SELECT ts, spot, rows FROM chain_snapshots "
                "WHERE spot IS NOT NULL AND spot > 100 "
                "  AND ts >= CAST(:d AS date) - 1 AND ts < CAST(:d AS date) + 1 "
                "ORDER BY ts"),
                {"d": d}).fetchall()
            trades = c.execute(text(
                "SELECT id, ts FROM setup_log "
                "WHERE date(ts AT TIME ZONE 'America/New_York') = :d "
                "  AND gex_net_ceiling IS NULL ORDER BY ts"), {"d": d}).fetchall()
            if not snaps or not trades:
                continue
            eps = [s[0].timestamp() for s in snaps]
            cache, out = {}, []
            for lid, ts in trades:
                e = ts.timestamp()
                lo, hi, j = 0, len(eps) - 1, -1
                while lo <= hi:                        # last snapshot AT OR BEFORE entry
                    mid = (lo + hi) // 2
                    if eps[mid] <= e:
                        j = mid; lo = mid + 1
                    else:
                        hi = mid - 1
                if j < 0 or (e - eps[j]) > 300:        # nothing within 5 min before
                    miss += 1
                    continue
                if j not in cache:
                    _, sp, rw = snaps[j]
                    rw = rw if isinstance(rw, list) else json.loads(rw)
                    cache[j] = compute(float(sp), rw)
                f = cache[j]
                if not f:
                    miss += 1
                    continue
                out.append({"lid": lid, "nc": f["net_ceiling"]})
            if out and not args.dry_run:
                # one bulk UPDATE per day (row-by-row over ~5.8k signals takes >10 min
                # and holds the connection far longer than necessary)
                vals = ",".join(
                    f"({o['lid']},{'NULL' if o['nc'] is None else float(o['nc'])})"
                    for o in out)
                # CAST is required: on a day where every signal has no wall overhead
                # the VALUES column is all-NULL and Postgres cannot infer its type.
                c.execute(text(
                    f"UPDATE setup_log s SET gex_net_ceiling = CAST(v.nc AS double precision) "
                    f"FROM (VALUES {vals}) AS v(id, nc) WHERE s.id = v.id"))
            total += len(out)
            print(f"  {d}  {len(out):>4} stamped", end="\r")

        print(f"\nstamped {total} rows, {miss} skipped (no snapshot within 5 min before entry)")
        if args.dry_run:
            print("DRY RUN — nothing written")
            return
        n = c.execute(text("SELECT count(*) FROM setup_log WHERE gex_net_ceiling IS NOT NULL")).scalar()
        nn = c.execute(text("SELECT count(*) FROM setup_log WHERE ts >= :s"),
                       {"s": args.since}).scalar()
        print(f"coverage: {n} of {nn} signals since {args.since} now carry gex_net_ceiling")


if __name__ == "__main__":
    main()
