"""
Skew Charm v2 Grade Re-analysis
================================
Recomputes v2 grades for ALL historical SC trades, then analyzes
which grade combinations to filter for real-money trading.

Read-only — does NOT modify the database.
"""

import os, sys
from datetime import datetime, time as dtime, timedelta, timezone
import psycopg2
import psycopg2.extras
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    print("ERROR: DATABASE_URL not set"); sys.exit(1)

# ── V2 scoring constants (exact copy from setup_detector.py) ──

_GOOD_PARADIGMS = {"GEX-PURE", "SIDIAL-EXTREME", "SIDIAL-MESSY", "AG-TARGET",
                   "BOFA-LIS", "BofA-LIS", "AG-PURE", "GEX-MESSY"}
_BAD_PARADIGMS = {"GEX-LIS", "AG-LIS"}


def compute_v2_score(paradigm, fired_time, vix, abs_charm, skew_change_pct):
    """Compute v2 score and grade. Returns (total_score, grade, components_dict)."""

    # 1. Paradigm subtype (0-30)
    p_val = str(paradigm) if paradigm else ""
    if p_val in _GOOD_PARADIGMS:
        para_score = 30
    elif p_val in _BAD_PARADIGMS:
        para_score = 0
    else:
        para_score = 15

    # 2. Time of day INVERTED (0-25) — morning = best
    t = fired_time
    if t < dtime(12, 0):
        time_score = 25
    elif t < dtime(14, 0):
        time_score = 15
    elif t < dtime(14, 30):
        time_score = 10
    elif t < dtime(15, 0):
        time_score = 3   # 14:30 dead zone
    elif t < dtime(15, 30):
        time_score = 8
    else:
        time_score = 0   # 15:30+ death zone

    # 3. VIX regime (0-20)
    _vix = float(vix) if vix is not None else 22.0
    if _vix < 22:
        vix_score = 20
    elif _vix < 25:
        vix_score = 12
    else:
        vix_score = 5

    # 4. Charm INVERTED (0-15) — low abs(charm) = best
    ac = abs(abs_charm) if abs_charm is not None else 50_000_000  # default to mid
    if ac < 50_000_000:
        charm_score = 15
    elif ac < 100_000_000:
        charm_score = 10
    elif ac < 250_000_000:
        charm_score = 5
    else:
        charm_score = 0

    # 5. Skew magnitude (0-10) — we use 3 as conservative default
    #    since we don't have skew_change_pct stored in DB for most trades
    abs_change = abs(skew_change_pct) if skew_change_pct is not None else 3.5
    if abs_change >= 7:
        skew_score = 10
    elif abs_change >= 5:
        skew_score = 7
    else:
        skew_score = 3

    total = para_score + time_score + vix_score + charm_score + skew_score

    # Grade thresholds
    if total >= 80:
        grade = "A+"
    elif total >= 65:
        grade = "A"
    elif total >= 50:
        grade = "B"
    elif total >= 35:
        grade = "C"
    else:
        grade = "LOG"

    return total, grade, {
        "para": para_score, "time": time_score, "vix": vix_score,
        "charm": charm_score, "skew": skew_score
    }


def passes_v11_filter(setup_name, direction, alignment, vix, overvix, paradigm, fired_time):
    """Replicates _passes_live_filter() V11 logic exactly."""

    # V11 time-of-day gates
    t = fired_time
    if setup_name in ("Skew Charm", "DD Exhaustion"):
        if dtime(14, 30) <= t < dtime(15, 0):
            return False
        if t >= dtime(15, 30):
            return False
    if setup_name == "BofA Scalp" and t >= dtime(14, 30):
        return False

    is_long = direction in ("long", "bullish")
    align = alignment or 0

    if is_long:
        if align < 2:
            return False
        if setup_name == "Skew Charm":
            return True  # SC longs exempt from VIX gate
        if vix is not None and vix > 22:
            ov = overvix if overvix is not None else -99
            if ov < 2:
                return False
        return True
    else:
        # Shorts
        if setup_name in ("Skew Charm", "DD Exhaustion"):
            if paradigm == "GEX-LIS":
                return False
        if setup_name in ("Skew Charm", "AG Short"):
            return True
        if setup_name == "DD Exhaustion" and align != 0:
            return True
        return False


def main():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # ── Pull all SC trades with outcomes ──
    cur.execute("""
        SELECT id, direction, grade as old_grade, score as old_score,
               ts, outcome_result as outcome, outcome_pnl as pnl_pts,
               spot, greek_alignment as alignment, paradigm,
               vix, overvix,
               support_score, upside_score, floor_cluster_score, target_cluster_score, rr_score
        FROM setup_log
        WHERE setup_name = 'Skew Charm'
        AND outcome_result IN ('WIN', 'LOSS', 'EXPIRED')
        ORDER BY ts;
    """)
    trades = cur.fetchall()
    print(f"Total SC trades with outcomes: {len(trades)}")

    # ── For each trade, get charm from volland_snapshots ──
    results = []
    charm_found = 0
    charm_missing = 0

    for t in trades:
        trade_ts = t["ts"]

        # Get charm from volland snapshot closest to trade time
        cur.execute("""
            SELECT payload->'statistics'->>'aggregatedCharm' as agg_charm, ts
            FROM volland_snapshots
            WHERE ts <= %s
            AND ts >= %s - interval '5 minutes'
            AND payload->'statistics' IS NOT NULL
            ORDER BY ts DESC LIMIT 1;
        """, (trade_ts, trade_ts))
        vol_row = cur.fetchone()

        charm_value = None
        if vol_row and vol_row["agg_charm"]:
            # Parse charm string like "$7,298,110,681" or "-$1,234,567"
            charm_str = vol_row["agg_charm"]
            try:
                cleaned = charm_str.replace("$", "").replace(",", "").strip()
                charm_value = float(cleaned)
                charm_found += 1
            except (ValueError, TypeError):
                charm_missing += 1
        else:
            charm_missing += 1

        # Convert UTC ts to ET for time-of-day scoring
        if hasattr(trade_ts, 'astimezone'):
            ts_et = trade_ts.astimezone(ET)
            fired_time = ts_et.time()
        elif hasattr(trade_ts, 'replace'):
            # naive datetime — assume UTC, convert
            ts_et = trade_ts.replace(tzinfo=timezone.utc).astimezone(ET)
            fired_time = ts_et.time()
        else:
            fired_time = dtime(12, 0)

        # Compute v2 score
        new_score, new_grade, components = compute_v2_score(
            paradigm=t["paradigm"],
            fired_time=fired_time,
            vix=t["vix"],
            abs_charm=charm_value,
            skew_change_pct=None  # not stored in DB, use default 3
        )

        # Apply V11 filter
        v11_pass = passes_v11_filter(
            setup_name="Skew Charm",
            direction=t["direction"],
            alignment=t["alignment"],
            vix=t["vix"],
            overvix=t["overvix"],
            paradigm=t["paradigm"],
            fired_time=fired_time
        )

        results.append({
            **dict(t),
            "new_score": new_score,
            "new_grade": new_grade,
            "components": components,
            "charm_value": charm_value,
            "fired_time": fired_time,
            "v11_pass": v11_pass,
        })

    print(f"Charm data found: {charm_found}, missing: {charm_missing}")
    print()

    # ═══════════════════════════════════════════════════════════════
    # ANALYSIS
    # ═══════════════════════════════════════════════════════════════

    # ── A) V2 Grade Distribution (all trades) ──
    print("=" * 80)
    print("A) V2 GRADE DISTRIBUTION (ALL TRADES)")
    print("=" * 80)
    grade_order = ["A+", "A", "B", "C", "LOG"]
    for g in grade_order:
        count = sum(1 for r in results if r["new_grade"] == g)
        wins = sum(1 for r in results if r["new_grade"] == g and r["outcome"] == "WIN")
        losses = sum(1 for r in results if r["new_grade"] == g and r["outcome"] == "LOSS")
        expired = sum(1 for r in results if r["new_grade"] == g and r["outcome"] == "EXPIRED")
        pnl = sum(r["pnl_pts"] or 0 for r in results if r["new_grade"] == g)
        wr = wins / count * 100 if count > 0 else 0
        print(f"  {g:4s}: {count:3d} trades | {wins}W/{losses}L/{expired}E | WR {wr:.1f}% | PnL {pnl:+.1f} pts")
    print()

    # ── B) V2 Grade Performance (V11-filtered only) ──
    print("=" * 80)
    print("B) V2 GRADE PERFORMANCE (V11-FILTERED ONLY)")
    print("=" * 80)
    v11_trades = [r for r in results if r["v11_pass"]]
    print(f"V11 passes: {len(v11_trades)} / {len(results)} total")
    print()
    for g in grade_order:
        subset = [r for r in v11_trades if r["new_grade"] == g]
        count = len(subset)
        if count == 0:
            print(f"  {g:4s}: 0 trades")
            continue
        wins = sum(1 for r in subset if r["outcome"] == "WIN")
        losses = sum(1 for r in subset if r["outcome"] == "LOSS")
        expired = sum(1 for r in subset if r["outcome"] == "EXPIRED")
        pnl = sum(r["pnl_pts"] or 0 for r in subset)
        avg_pnl = pnl / count
        wr = wins / count * 100
        print(f"  {g:4s}: {count:3d} trades | {wins}W/{losses}L/{expired}E | WR {wr:.1f}% | PnL {pnl:+.1f} pts | avg {avg_pnl:+.1f}")
    print()

    # ── C) Compare Configurations (V11-filtered) ──
    print("=" * 80)
    print("C) CONFIGURATION COMPARISON (V11-FILTERED)")
    print("=" * 80)

    configs = {
        "All v2 grades":     {"A+", "A", "B", "C", "LOG"},
        "A+ and A only":     {"A+", "A"},
        "A+, A, B":          {"A+", "A", "B"},
        "A and B only":      {"A", "B"},
        "B only":            {"B"},
        "C only":            {"C"},
        "LOG only":          {"LOG"},
        "Block C+LOG (A+,A,B)": {"A+", "A", "B"},
        "Block LOG only":    {"A+", "A", "B", "C"},
    }

    def compute_max_dd(trade_list):
        """Compute max drawdown from running P&L."""
        cum = 0
        peak = 0
        max_dd = 0
        for r in sorted(trade_list, key=lambda x: x["ts"]):
            cum += r["pnl_pts"] or 0
            if cum > peak:
                peak = cum
            dd = peak - cum
            if dd > max_dd:
                max_dd = dd
        return max_dd

    print(f"{'Config':<25s} {'Trades':>6s} {'Wins':>5s} {'Loss':>5s} {'Exp':>4s} {'WR%':>6s} {'PnL':>8s} {'AvgPnL':>7s} {'MaxDD':>7s} {'PF':>6s}")
    print("-" * 90)

    for name, allowed_grades in configs.items():
        subset = [r for r in v11_trades if r["new_grade"] in allowed_grades]
        count = len(subset)
        if count == 0:
            print(f"{name:<25s} {'0':>6s}")
            continue
        wins = sum(1 for r in subset if r["outcome"] == "WIN")
        losses = sum(1 for r in subset if r["outcome"] == "LOSS")
        expired = sum(1 for r in subset if r["outcome"] == "EXPIRED")
        gross_win = sum(r["pnl_pts"] or 0 for r in subset if (r["pnl_pts"] or 0) > 0)
        gross_loss = abs(sum(r["pnl_pts"] or 0 for r in subset if (r["pnl_pts"] or 0) < 0))
        pnl = sum(r["pnl_pts"] or 0 for r in subset)
        avg_pnl = pnl / count
        wr = wins / count * 100
        max_dd = compute_max_dd(subset)
        pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
        print(f"{name:<25s} {count:6d} {wins:5d} {losses:5d} {expired:4d} {wr:5.1f}% {pnl:+7.1f} {avg_pnl:+6.1f} {max_dd:6.1f} {pf:5.2f}")
    print()

    # ── D) Trades where grade changed AND outcome matters ──
    print("=" * 80)
    print("D) GRADE CHANGES (old_grade != new_grade)")
    print("=" * 80)
    grade_changes = [r for r in results if r["old_grade"] != r["new_grade"]]
    print(f"Total grade changes: {len(grade_changes)} / {len(results)}")
    print()

    if grade_changes:
        print(f"{'ID':>6s} {'Date':>10s} {'Time':>5s} {'Dir':>5s} {'Old':>4s} {'New':>4s} {'Outcome':>7s} {'PnL':>6s} {'V11':>4s} {'Paradigm':>15s} {'VIX':>5s} {'Charm':>12s}")
        print("-" * 100)
        for r in sorted(grade_changes, key=lambda x: x["ts"]):
            charm_str = f"{r['charm_value']/1e6:+.0f}M" if r['charm_value'] is not None else "n/a"
            vix_str = f"{r['vix']:.1f}" if r['vix'] is not None else "n/a"
            date_str = r["ts"].strftime("%Y-%m-%d") if hasattr(r["ts"], "strftime") else str(r["ts"])[:10]
            time_str = r["fired_time"].strftime("%H:%M") if hasattr(r["fired_time"], "strftime") else str(r["fired_time"])[:5]
            print(f"{r['id']:6d} {date_str:>10s} {time_str:>5s} {r['direction']:>5s} {r['old_grade']:>4s} {r['new_grade']:>4s} {r['outcome']:>7s} {(r['pnl_pts'] or 0):+5.1f} {'Y' if r['v11_pass'] else 'N':>4s} {(r['paradigm'] or 'None'):>15s} {vix_str:>5s} {charm_str:>12s}")
    print()

    # ── D2) Impact: trades that were previously traded but would now be blocked (or vice versa) ──
    print("=" * 80)
    print("D2) ACTIONABLE CHANGES: Trades that switch from traded <-> blocked")
    print("=" * 80)

    # Grades that would be "traded" under current system (old: all grades pass, new: depends on config)
    # Current system trades all grades that pass V11 (no grade filter on SC)
    # New proposal: block certain grades

    for block_name, blocked_grades in [("Block C+LOG", {"C", "LOG"}), ("Block LOG only", {"LOG"})]:
        print(f"\n--- If we {block_name}: ---")
        newly_blocked = [r for r in v11_trades if r["new_grade"] in blocked_grades]
        if newly_blocked:
            wins_blocked = sum(1 for r in newly_blocked if r["outcome"] == "WIN")
            losses_blocked = sum(1 for r in newly_blocked if r["outcome"] == "LOSS")
            pnl_blocked = sum(r["pnl_pts"] or 0 for r in newly_blocked)
            print(f"  Would block {len(newly_blocked)} trades: {wins_blocked}W/{losses_blocked}L, PnL {pnl_blocked:+.1f}")
            for r in sorted(newly_blocked, key=lambda x: x["ts"]):
                charm_str = f"{r['charm_value']/1e6:+.0f}M" if r['charm_value'] is not None else "n/a"
                date_str = r["ts"].strftime("%Y-%m-%d") if hasattr(r["ts"], "strftime") else str(r["ts"])[:10]
                time_str = r["fired_time"].strftime("%H:%M") if hasattr(r["fired_time"], "strftime") else str(r["fired_time"])[:5]
                print(f"    id={r['id']} {date_str} {time_str} {r['direction']:>5s} grade={r['new_grade']} {r['outcome']:>7s} {(r['pnl_pts'] or 0):+5.1f} para={r['paradigm']} VIX={r['vix']}")
        else:
            print(f"  No trades would be blocked.")
    print()

    # ── E) Detailed component breakdown for each V11-filtered trade ──
    print("=" * 80)
    print("E) FULL TRADE LIST (V11-filtered, sorted by date)")
    print("=" * 80)
    print(f"{'ID':>6s} {'Date':>10s} {'Time':>5s} {'Dir':>5s} {'OldG':>4s} {'NewG':>4s} {'Score':>5s} {'P':>3s} {'T':>3s} {'V':>3s} {'Ch':>3s} {'Sk':>3s} {'Out':>7s} {'PnL':>6s} {'Align':>5s} {'Paradigm':>15s}")
    print("-" * 110)
    for r in sorted(v11_trades, key=lambda x: x["ts"]):
        c = r["components"]
        date_str = r["ts"].strftime("%Y-%m-%d") if hasattr(r["ts"], "strftime") else str(r["ts"])[:10]
        time_str = r["fired_time"].strftime("%H:%M") if hasattr(r["fired_time"], "strftime") else str(r["fired_time"])[:5]
        align_str = f"{r['alignment']:+d}" if r['alignment'] is not None else "n/a"
        print(f"{r['id']:6d} {date_str:>10s} {time_str:>5s} {r['direction']:>5s} {r['old_grade']:>4s} {r['new_grade']:>4s} {r['new_score']:5d} {c['para']:3d} {c['time']:3d} {c['vix']:3d} {c['charm']:3d} {c['skew']:3d} {r['outcome']:>7s} {(r['pnl_pts'] or 0):+5.1f} {align_str:>5s} {(r['paradigm'] or 'None'):>15s}")
    print()

    # ── F) Direction breakdown (V11-filtered) ──
    print("=" * 80)
    print("F) DIRECTION BREAKDOWN (V11-filtered)")
    print("=" * 80)
    for direction in ["long", "short"]:
        subset = [r for r in v11_trades if r["direction"] == direction]
        count = len(subset)
        if count == 0:
            print(f"  {direction.upper()}: 0 trades")
            continue
        wins = sum(1 for r in subset if r["outcome"] == "WIN")
        pnl = sum(r["pnl_pts"] or 0 for r in subset)
        wr = wins / count * 100
        print(f"  {direction.upper()}: {count} trades | WR {wr:.1f}% | PnL {pnl:+.1f}")

        for g in grade_order:
            gs = [r for r in subset if r["new_grade"] == g]
            if not gs:
                continue
            gw = sum(1 for r in gs if r["outcome"] == "WIN")
            gpnl = sum(r["pnl_pts"] or 0 for r in gs)
            gwr = gw / len(gs) * 100
            print(f"    {g:4s}: {len(gs):3d} trades | WR {gwr:.1f}% | PnL {gpnl:+.1f}")
    print()

    # ── G) Paradigm breakdown (V11-filtered) ──
    print("=" * 80)
    print("G) PARADIGM BREAKDOWN (V11-filtered)")
    print("=" * 80)
    paradigms = sorted(set(r["paradigm"] or "None" for r in v11_trades))
    print(f"{'Paradigm':>20s} {'Trades':>6s} {'WR%':>6s} {'PnL':>8s} {'AvgPnL':>7s}")
    print("-" * 55)
    for p in paradigms:
        subset = [r for r in v11_trades if (r["paradigm"] or "None") == p]
        count = len(subset)
        wins = sum(1 for r in subset if r["outcome"] == "WIN")
        pnl = sum(r["pnl_pts"] or 0 for r in subset)
        wr = wins / count * 100
        avg = pnl / count
        print(f"{p:>20s} {count:6d} {wr:5.1f}% {pnl:+7.1f} {avg:+6.1f}")
    print()

    # ── H) Final recommendation ──
    print("=" * 80)
    print("H) FINAL RECOMMENDATION")
    print("=" * 80)

    # Compare key configs
    all_v11 = v11_trades
    a_plus_a = [r for r in v11_trades if r["new_grade"] in {"A+", "A"}]
    a_plus_a_b = [r for r in v11_trades if r["new_grade"] in {"A+", "A", "B"}]
    block_log = [r for r in v11_trades if r["new_grade"] != "LOG"]

    for label, subset in [("All V11", all_v11), ("A+ & A only", a_plus_a),
                          ("A+, A, B", a_plus_a_b), ("Block LOG only", block_log)]:
        count = len(subset)
        if count == 0:
            continue
        wins = sum(1 for r in subset if r["outcome"] == "WIN")
        pnl = sum(r["pnl_pts"] or 0 for r in subset)
        wr = wins / count * 100
        max_dd = compute_max_dd(subset)
        gross_win = sum(r["pnl_pts"] or 0 for r in subset if (r["pnl_pts"] or 0) > 0)
        gross_loss = abs(sum(r["pnl_pts"] or 0 for r in subset if (r["pnl_pts"] or 0) < 0))
        pf = gross_win / gross_loss if gross_loss > 0 else float('inf')
        print(f"  {label:<20s}: {count:3d} trades | WR {wr:.1f}% | PnL {pnl:+.1f} | MaxDD {max_dd:.1f} | PF {pf:.2f}")

    print()
    print("KEY QUESTION: Does blocking low grades improve risk-adjusted returns?")
    print("Look at MaxDD reduction vs PnL sacrifice in section C.")
    print()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
