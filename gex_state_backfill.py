# -*- coding: utf-8 -*-
"""Backfill the gex_state TABLE from chain_snapshots.

Why: gex_state.capture() only started writing on 2026-08-11, so the table held 4
days. Everything else (v7's SUPPORT gate, V18's net_ceiling) is analysed from
setup_log stamps instead, which is a SECOND path to the same numbers. Filling the
table means the live gate and the analysis read the SAME source, which is the
whole point of having one compute().

Uses gex_state.compute() unchanged — one implementation, live and historical.

DB discipline (2026-06-03 outage): AUTOCOMMIT and one bulk write per day. A long
read transaction against prod holds AccessShareLock and blocks db_init()'s startup
ALTERs, which crash-loops every deploy.

Run:  python gex_state_backfill.py [--since 2026-02-19] [--dry-run]
"""
import argparse
import json
import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from app.gex_state import compute, ET  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-02-19")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    eng = create_engine(os.environ["DATABASE_URL"])
    with eng.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        c.execute(text("""CREATE TABLE IF NOT EXISTS gex_state (
            et timestamp PRIMARY KEY, spot double precision, state text,
            net_gex double precision, net_dex double precision,
            zero_gamma double precision, call_wall double precision,
            put_wall double precision, max_gamma double precision, payload jsonb)"""))
        have = c.execute(text("SELECT count(*) FROM gex_state")).scalar()
        days = [r[0] for r in c.execute(text(
            "SELECT DISTINCT date(ts AT TIME ZONE 'America/New_York') d FROM chain_snapshots "
            "WHERE spot IS NOT NULL AND spot > 100 AND ts >= :s ORDER BY d"),
            {"s": args.since}).fetchall()]
        print(f"gex_state currently holds {have} rows; {len(days)} sessions to process "
              f"from {args.since}")

        total = skipped = 0
        for d in days:
            snaps = c.execute(text(
                "SELECT ts, spot, rows FROM chain_snapshots "
                "WHERE spot IS NOT NULL AND spot > 100 "
                "  AND ts >= CAST(:d AS date) - 1 AND ts < CAST(:d AS date) + 1 "
                "ORDER BY ts"), {"d": d}).fetchall()
            out = []
            for ts, spot, rows in snaps:
                et = ts.astimezone(ET).replace(tzinfo=None)
                if et.date() != d:
                    continue
                rows = rows if isinstance(rows, list) else json.loads(rows)
                f = compute(float(spot), rows)
                if not f:
                    skipped += 1
                    continue
                out.append(dict(et=et, spot=f["spot"], state=f["state"],
                                ng=f["net_gex"], nd=f["net_dex"], zg=f["zero_gamma"],
                                cw=f["call_wall"], pw=f["put_wall"], mg=f["max_gamma"],
                                pl=json.dumps(f)))
            if out and not args.dry_run:
                # ONE bulk upsert per day. Row-by-row over ~24k rows against a remote
                # DB runs for hours; this finishes in minutes. ON CONFLICT DO UPDATE so
                # re-runs refresh rows written by an older compute() — that is how
                # net_ceiling reaches the days capture() already wrote.
                import psycopg2.extras
                raw = c.connection.driver_connection
                with raw.cursor() as cur:
                    psycopg2.extras.execute_values(cur, """
                        INSERT INTO gex_state
                          (et,spot,state,net_gex,net_dex,zero_gamma,call_wall,put_wall,
                           max_gamma,payload)
                        VALUES %s
                        ON CONFLICT (et) DO UPDATE SET
                          spot=EXCLUDED.spot, state=EXCLUDED.state,
                          net_gex=EXCLUDED.net_gex, net_dex=EXCLUDED.net_dex,
                          zero_gamma=EXCLUDED.zero_gamma, call_wall=EXCLUDED.call_wall,
                          put_wall=EXCLUDED.put_wall, max_gamma=EXCLUDED.max_gamma,
                          payload=EXCLUDED.payload""",
                        [(o["et"], o["spot"], o["state"], o["ng"], o["nd"], o["zg"],
                          o["cw"], o["pw"], o["mg"], o["pl"]) for o in out],
                        page_size=500)
            total += len(out)
            print(f"  {d}  {len(out):>4} rows   (running {total})", end="\r")

        print(f"\nwrote {total} rows, {skipped} snapshots unusable")
        if args.dry_run:
            print("DRY RUN — nothing written")
            return
        n, d0, d1, nc = c.execute(text(
            "SELECT count(*), min(et)::date, max(et)::date, "
            "count(*) FILTER (WHERE payload ? 'net_ceiling') FROM gex_state")).fetchone()
        print(f"gex_state now: {n} rows, {d0} -> {d1}, {nc} carry net_ceiling")
        for st, cnt in c.execute(text(
                "SELECT state, count(*) FROM gex_state GROUP BY 1 ORDER BY 2 DESC")).fetchall():
            print(f"    {str(st):<20} {cnt}")


if __name__ == "__main__":
    main()
