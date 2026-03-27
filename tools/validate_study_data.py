"""
Data Validation Gate — run BEFORE any backtest/study analysis.
Usage: railway run python tools/validate_study_data.py --setup "Skew Charm" --filter v12
Returns PASS/FAIL with specific issues found.
"""
import os, sys, argparse
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.stdout.reconfigure(encoding='utf-8')

ET = ZoneInfo("America/New_York")

# Known contaminated dates (add here when discovered)
KNOWN_OUTAGES = {
    date(2026, 3, 26): "TS API outage 10:20-15:55 ET — frozen spot prices",
}

# Parameter change history (add here when params change)
PARAM_CHANGES = {
    "Skew Charm": [
        {"date": date(2026, 3, 18), "change": "SL 20->14", "field": "stop_loss", "breaks_outcomes": True},
        {"date": date(2026, 3, 22), "change": "Grading v2 deployed", "field": "grading", "breaks_outcomes": False},
        {"date": date(2026, 3, 27), "change": "Trail gap 8->5", "field": "trail_gap", "breaks_outcomes": True},
    ],
    "AG Short": [
        {"date": date(2026, 3, 25), "change": "15-min cooldown + AG-TARGET blocked", "field": "cooldown", "breaks_outcomes": False},
        {"date": date(2026, 3, 27), "change": "Trail activation 15->12", "field": "trail_activation", "breaks_outcomes": True},
    ],
    "DD Exhaustion": [],
    "ES Absorption": [
        {"date": date(2026, 3, 11), "change": "Original restored (CVD Divergence reverted)", "field": "algorithm"},
    ],
}

# V12 filter dates
FILTER_HISTORY = [
    {"date": date(2026, 3, 25), "version": "V12", "change": "Gap-up longs block added"},
    {"date": date(2026, 3, 24), "version": "V11", "change": "SC grade gate + time gates"},
    {"date": date(2026, 3, 14), "version": "V10", "change": "Smart VIX gate"},
]


def validate(setup_name, filter_version="v12", min_date=None):
    db_url = os.getenv('DATABASE_URL', '').replace('postgres://', 'postgresql://')
    if not db_url:
        print("FAIL: DATABASE_URL not set")
        return False

    engine = create_engine(db_url)
    issues = []
    warnings = []
    info = []

    with engine.connect() as conn:
        # 1. Get all trades for this setup with outcomes
        rows = conn.execute(text("""
            SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
                   direction, grade, spot, paradigm, greek_alignment,
                   outcome_result, outcome_pnl,
                   outcome_max_profit, outcome_max_loss,
                   outcome_stop_level
            FROM setup_log
            WHERE setup_name = :setup
              AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
            ORDER BY id
        """), {"setup": setup_name}).fetchall()

        info.append(f"Total trades with outcomes: {len(rows)}")
        if not rows:
            issues.append("NO DATA: Zero trades found")
            _print_report(setup_name, issues, warnings, info)
            return False

        # Date range
        dates = sorted(set(r[1].date() for r in rows))
        info.append(f"Date range: {dates[0]} to {dates[-1]} ({len(dates)} trading days)")

        # 2. Check for contaminated dates (trades WITH outcomes on outage days)
        for d in dates:
            if d in KNOWN_OUTAGES:
                outage_trades = [r for r in rows if r[1].date() == d and r[7] is not None]
                if outage_trades:
                    ids = [r[0] for r in outage_trades]
                    issues.append(f"CONTAMINATED DATE: {d} -- {KNOWN_OUTAGES[d]} -- {len(outage_trades)} trades still have outcomes! IDs: {ids}")

        # 3. Check parameter changes during period
        if setup_name in PARAM_CHANGES:
            for pc in PARAM_CHANGES[setup_name]:
                if dates[0] <= pc["date"] <= dates[-1]:
                    pre = sum(1 for r in rows if r[1].date() < pc["date"])
                    post = sum(1 for r in rows if r[1].date() >= pc["date"])
                    warnings.append(f"PARAM CHANGE: {pc['change']} on {pc['date']} — {pre} trades before, {post} after. Split analysis at boundary.")

        # 4. Check for frozen spot prices (staleness)
        spot_by_date = {}
        for r in rows:
            d = r[1].date()
            if d not in spot_by_date:
                spot_by_date[d] = []
            spot_by_date[d].append(r[4])  # spot

        for d, spots in spot_by_date.items():
            if len(spots) > 3:
                unique = len(set(round(s, 1) for s in spots))
                if unique <= 2:
                    issues.append(f"STALE DATA: {d} has {len(spots)} trades but only {unique} unique spot prices — likely frozen API")

        # 5. Check MFE outliers
        for r in rows:
            mfe = r[9] if r[9] else 0
            mae = r[10] if r[10] else 0
            if mfe > 50:
                warnings.append(f"HIGH MFE: id={r[0]} {r[1].strftime('%m-%d %H:%M')} MFE={mfe:.1f} — verify against market conditions")
            if mae < -30 and r[7] != 'LOSS':
                warnings.append(f"DEEP MAE NON-LOSS: id={r[0]} {r[1].strftime('%m-%d %H:%M')} MAE={mae:.1f} outcome={r[7]} — suspicious")

        # 6. Check SL consistency (outcome_stop_level)
        sl_values = {}
        for r in rows:
            if r[11] is not None:  # outcome_stop_level
                spot = r[4]
                is_long = r[2] == 'long'
                sl = round(abs(spot - r[11]), 0) if is_long else round(abs(r[11] - spot), 0)
                if sl not in sl_values:
                    sl_values[sl] = 0
                sl_values[sl] += 1

        if len(sl_values) > 3:
            # Multiple SL values suggest trail-overwritten stop levels or param changes
            common_sls = sorted(sl_values.items(), key=lambda x: -x[1])[:3]
            info.append(f"SL values found: {dict(common_sls)} (top 3)")
            if any(sl not in (14, 20) for sl, _ in common_sls if _ > 5):
                warnings.append(f"MIXED SL: Multiple stop-loss values detected — likely trail-overwritten pre-Mar 10 data or param changes")

        # 7. Compute clean dataset stats
        # Find latest BREAKING param change (one that invalidates prior outcome data)
        latest_change = date(2026, 1, 1)
        if setup_name in PARAM_CHANGES:
            for pc in PARAM_CHANGES[setup_name]:
                if pc.get("breaks_outcomes", False) and pc["date"] > latest_change:
                    latest_change = pc["date"]

        clean_rows = [r for r in rows if r[1].date() >= latest_change and r[1].date() not in KNOWN_OUTAGES]
        if min_date:
            clean_rows = [r for r in clean_rows if r[1].date() >= min_date]

        clean_wins = sum(1 for r in clean_rows if r[7] == 'WIN')
        clean_losses = sum(1 for r in clean_rows if r[7] == 'LOSS')
        clean_pnl = sum(r[8] for r in clean_rows if r[8])

        info.append(f"Clean dataset (post {latest_change}, excl outages): {len(clean_rows)} trades")
        info.append(f"  {clean_wins}W/{clean_losses}L, PnL={clean_pnl:+.1f}")

        if len(clean_rows) < 30:
            warnings.append(f"SMALL SAMPLE: Only {len(clean_rows)} clean trades — directional signal only, not precise magnitude")
        elif len(clean_rows) < 80:
            warnings.append(f"MODERATE SAMPLE: {len(clean_rows)} clean trades — moderate confidence")

    # Print report
    _print_report(setup_name, issues, warnings, info)
    return len(issues) == 0


def _print_report(setup_name, issues, warnings, info):
    status = "FAIL" if issues else ("WARN" if warnings else "PASS")
    print(f"\n{'='*70}")
    print(f"  DATA VALIDATION: {setup_name} — {status}")
    print(f"{'='*70}")

    if info:
        print(f"\n  INFO:")
        for i in info:
            print(f"    {i}")

    if issues:
        print(f"\n  BLOCKING ISSUES ({len(issues)}):")
        for i in issues:
            print(f"    [X] {i}")

    if warnings:
        print(f"\n  WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"    [!] {w}")

    if not issues and not warnings:
        print(f"\n  All checks passed.")

    print(f"\n{'='*70}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate study data before analysis")
    parser.add_argument("--setup", required=True, help="Setup name (e.g., 'Skew Charm')")
    parser.add_argument("--filter", default="v12", help="Filter version")
    parser.add_argument("--min-date", help="Minimum date (YYYY-MM-DD)")
    args = parser.parse_args()

    min_date = date.fromisoformat(args.min_date) if args.min_date else None
    ok = validate(args.setup, args.filter, min_date)
    sys.exit(0 if ok else 1)
