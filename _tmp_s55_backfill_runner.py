"""S55 backfill runner — populates mes_sim_* columns for historical V14 trades.

⚠️ RUN AFTER _tmp_s55_db_migration.sql HAS BEEN APPLIED.
⚠️ DO NOT RUN DURING MARKET HOURS (9:30-16:00 ET).

Usage:
    python _tmp_s55_backfill_runner.py                     # full default range
    python _tmp_s55_backfill_runner.py --dry-run           # compute, no write
    python _tmp_s55_backfill_runner.py --start 2026-04-15 --end 2026-05-12
    python _tmp_s55_backfill_runner.py --date 2026-05-12   # single day

Default range: 2026-04-15 → today (matches the validated S55 prototype window).

Scope: V14 real-trader whitelist setups only (Skew Charm, AG Short,
       Vanna Pivot Bounce, VIX Divergence, ES Absorption).
       Other setups are skipped — they don't go through the MES path live
       and mes-sim outcome is undefined for them.

After completion:
    - Spot-check a few rows in the portal V14 dropdown — they should now
      show a ✦ MES-sim badge alongside the chain-sim Result/PnL.
    - Compare totals against `_tmp_s55_mes_trail_prototype.py` HTML report —
      backfill must match within rounding (same simulator).
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, date as _date_t

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:
    ET = None

# Ensure repo root on path so `app/...` imports resolve when run from repo root.
REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from sqlalchemy import create_engine, text  # noqa: E402

from app import mes_sim_backfill as msb  # noqa: E402


DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway",
)

DEFAULT_START = _date_t(2026, 4, 15)


def _today_et() -> _date_t:
    if ET is None:
        return datetime.utcnow().date()
    return datetime.now(ET).date()


def _safety_market_hours_guard():
    if ET is None:
        return
    now = datetime.now(ET)
    if now.weekday() >= 5:
        return  # weekend OK
    t = now.time()
    if t.hour == 9 and t.minute >= 30:
        market_open = True
    elif 9 < t.hour < 16:
        market_open = True
    elif t.hour == 16 and t.minute <= 10:
        market_open = True
    else:
        market_open = False
    if market_open:
        print(
            f"[s55-backfill] ⚠️ MARKET HOURS DETECTED ({now.strftime('%H:%M ET')}). "
            "Backfill is read-mostly but does UPDATE setup_log rows — abort "
            "to avoid contending with live writes."
        )
        print("[s55-backfill] If you really need to run during market hours, "
              "pass --i-know-what-im-doing.")
        sys.exit(1)


def _columns_exist(engine) -> bool:
    try:
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'setup_log'
                  AND column_name IN
                      ('mes_sim_outcome_pnl', 'mes_sim_outcome_result', 'mes_sim_max_fav')
            """)).fetchone()
        return row and row[0] == 3
    except Exception as e:
        print(f"[s55-backfill] column-check error: {e}")
        return False


def main():
    p = argparse.ArgumentParser(description="S55 MES-sim backfill runner")
    p.add_argument("--start", help="Start date YYYY-MM-DD (default 2026-04-15)")
    p.add_argument("--end", help="End date YYYY-MM-DD inclusive (default today ET)")
    p.add_argument("--date", help="Single date YYYY-MM-DD (shorthand for --start=--end=)")
    p.add_argument("--dry-run", action="store_true", help="Compute but do not write")
    p.add_argument("--i-know-what-im-doing", action="store_true",
                   help="Bypass market-hours safety check (NOT recommended)")
    args = p.parse_args()

    if not args.i_know_what_im_doing:
        _safety_market_hours_guard()

    if args.date:
        start = end = _date_t.fromisoformat(args.date)
    else:
        start = _date_t.fromisoformat(args.start) if args.start else DEFAULT_START
        end = _date_t.fromisoformat(args.end) if args.end else _today_et()

    if end < start:
        print(f"[s55-backfill] end={end} is before start={start} — abort")
        sys.exit(2)

    print(f"[s55-backfill] connecting to DB...")
    engine = create_engine(DB_URL, pool_pre_ping=True)

    if not _columns_exist(engine):
        print("[s55-backfill] ❌ ABORT: mes_sim_* columns not found on setup_log.")
        print("[s55-backfill] Apply _tmp_s55_db_migration.sql first.")
        sys.exit(3)
    print("[s55-backfill] ✓ migration verified (3 columns present)")

    print(f"[s55-backfill] range {start} → {end} (dry_run={args.dry_run})")
    print(f"[s55-backfill] whitelist: {sorted(msb.V14_WHITELIST)}")

    summary = msb.backfill_range(engine, start, end, dry_run=args.dry_run)

    print()
    print("=" * 70)
    print(f"{'date':<12} {'rows':>5} {'cmp':>5} {'wrt':>5} {'skp':>5} errors")
    print("-" * 70)
    for d in summary["dates"]:
        err = (d["errors"][0][:30] + "..") if d.get("errors") else ""
        print(f"{d['date']:<12} {d['rows']:>5} {d['computed']:>5} "
              f"{d.get('written', 0):>5} {d['skipped']:>5}  {err}")
    print("-" * 70)
    print(f"TOTAL         {summary['total_rows']:>5} "
          f"{summary['total_computed']:>5} {summary['total_written']:>5} "
          f"{summary['total_skipped']:>5}")
    print()

    if args.dry_run:
        print("[s55-backfill] dry-run complete — no DB writes performed")
    else:
        print(f"[s55-backfill] ✓ backfill complete — {summary['total_written']} rows updated")
        print("[s55-backfill] Next: spot-check portal V14 dropdown for ✦ MES-sim badges")


if __name__ == "__main__":
    main()
