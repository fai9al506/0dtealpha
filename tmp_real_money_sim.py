"""
Real Money PnL Simulation for March 2026
Applies V11 filter + eval_trader_config_real.json stops to setup_log trades.

ACTUAL default stops from DB (not CLAUDE.md which is outdated):
  SC: SL=20 (all March)
  DD: SL=12
  AG: Dynamic LIS-based (varies per trade, stored in outcome_stop_level)
  PR: SL=15
  ESA: SL=8 (ES price space)
  GEX Long: SL=8 (disabled)
  BofA: SL varies (disabled)

Real money config stops:
  SC: SL=12, AG: SL=12, DD: SL=12, PR: SL=12, ESA: SL=8
  BE trigger: 5 pts (stop moves to breakeven after +5 pts MFE)
"""
import psycopg2
import pytz
from datetime import time as dtime
from collections import defaultdict

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
ET = pytz.timezone("US/Eastern")

# Real money config (from eval_trader_config_real.json)
REAL_CONFIG = {
    "GEX Long":          {"enabled": False, "stop": 8,  "target": None},
    "ES Absorption":     {"enabled": True,  "stop": 8,  "target": 10},
    "Paradigm Reversal": {"enabled": True,  "stop": 12, "target": 10},
    "Skew Charm":        {"enabled": True,  "stop": 12, "target": None},
    "BofA Scalp":        {"enabled": False, "stop": 12, "target": None},
    "AG Short":          {"enabled": True,  "stop": 12, "target": None},
    "DD Exhaustion":     {"enabled": True,  "stop": 12, "target": None},
    "GEX Velocity":      {"enabled": True,  "stop": 8,  "target": None},
}

QTY = 8
DOLLAR_PER_PT = 5
DOLLARS_PER_POINT = QTY * DOLLAR_PER_PT  # $40/pt
COMMISSION_RT = 2.16 * QTY * 2  # $34.56 per round-trip
BE_TRIGGER = 5.0
NO_TRADE_AFTER = dtime(16, 20)


def passes_v11(r):
    """Replicate _passes_live_filter() V11 logic."""
    sn = r["setup_name"]
    direction = r["direction"]
    grade = r.get("grade")
    paradigm = r.get("paradigm")
    align = r.get("greek_alignment") or 0
    vix = r.get("vix")
    overvix = r.get("overvix")

    if sn in ("VIX Compression", "IV Momentum", "Vanna Butterfly",
              "SB Absorption", "SB10 Absorption", "SB2 Absorption",
              "Vanna Pivot Bounce"):
        return False

    if sn == "Skew Charm" and grade and grade in ("C", "LOG"):
        return False

    ts_et = r["ts"].astimezone(ET) if r["ts"].tzinfo else ET.localize(r["ts"])
    t = ts_et.time()
    if sn in ("Skew Charm", "DD Exhaustion"):
        if dtime(14, 30) <= t < dtime(15, 0):
            return False
        if t >= dtime(15, 30):
            return False
    if sn == "BofA Scalp" and t >= dtime(14, 30):
        return False

    is_long = direction in ("long", "bullish")
    if is_long:
        if align < 2:
            return False
        if sn == "Skew Charm":
            return True
        if vix is not None and vix > 22:
            ov = overvix if overvix is not None else -99
            if ov < 2:
                return False
        return True
    else:
        if sn in ("Skew Charm", "DD Exhaustion"):
            if paradigm == "GEX-LIS":
                return False
        if sn in ("Skew Charm", "AG Short"):
            return True
        if sn == "DD Exhaustion" and align != 0:
            return True
        return False


def recalculate_pnl(r):
    """
    Recalculate trade PnL using real money stops.

    Uses the ACTUAL default stop from the DB (outcome_stop_level distance)
    compared to the real money config stop.
    """
    sn = r["setup_name"]
    orig_result = r["outcome_result"]
    orig_pnl = r["outcome_pnl"] or 0
    mfe = r["outcome_max_profit"]  # max favorable excursion (positive number)
    mae = r["outcome_max_loss"]    # max adverse excursion (negative number typically)

    real_stop = REAL_CONFIG[sn]["stop"]
    real_target = REAL_CONFIG[sn]["target"]

    # Compute the actual default stop distance from the DB
    spot = r["spot"]
    stop_level = r["outcome_stop_level"]
    if stop_level and spot:
        default_stop = round(abs(spot - stop_level), 1)
    else:
        # Fallback for missing stop_level
        default_stop = real_stop  # assume same

    has_mfe_mae = mfe is not None and mae is not None
    mae_abs = abs(mae) if mae else 0
    mfe_abs = abs(mfe) if mfe else 0

    new_result = orig_result
    new_pnl = orig_pnl
    note = "same"

    if not has_mfe_mae:
        if orig_result == "LOSS":
            new_pnl = -min(real_stop, abs(orig_pnl))
            note = f"no MFE/MAE: loss capped at {real_stop}"
        return new_result, new_pnl, note, default_stop

    # Skip recalc if stops are effectively the same
    if abs(real_stop - default_stop) < 0.5:
        return new_result, new_pnl, "same stop", default_stop

    # CASE 1: Real stop is TIGHTER than default
    if real_stop < default_stop:
        if orig_result == "WIN":
            # Did the adverse excursion exceed the tighter stop?
            if mae_abs >= real_stop:
                # Check if MFE happened first (BE would protect)
                first_event = r.get("outcome_first_event", "")
                if mfe_abs >= BE_TRIGGER and first_event in ("10pt", "target"):
                    # Favorable move happened first -> BE set -> protected
                    new_pnl = orig_pnl
                    note = f"WIN survived: BE before tighter stop (mae={mae_abs:.1f} mfe={mfe_abs:.1f})"
                elif mfe_abs >= BE_TRIGGER and mae_abs < mfe_abs:
                    # MFE larger than MAE suggests favorable move came first
                    new_pnl = orig_pnl
                    note = f"WIN survived: MFE({mfe_abs:.1f}) > MAE({mae_abs:.1f}), BE likely"
                else:
                    new_result = "LOSS"
                    new_pnl = -real_stop
                    note = f"tighter stop killed WIN: mae={mae_abs:.1f}>={real_stop} (was {default_stop:.0f})"
            else:
                new_pnl = orig_pnl
                note = f"WIN safe: mae={mae_abs:.1f} < {real_stop}"

        elif orig_result == "LOSS":
            # Loss happens sooner at the tighter stop
            new_pnl = -real_stop
            note = f"tighter stop: {default_stop:.0f}->{real_stop} (save {default_stop - real_stop:.0f} pts)"

        elif orig_result == "EXPIRED":
            if mae_abs >= real_stop:
                if mfe_abs >= BE_TRIGGER:
                    new_pnl = 0
                    note = f"EXPIRED: BE protects from tighter stop (mae={mae_abs:.1f})"
                else:
                    new_result = "LOSS"
                    new_pnl = -real_stop
                    note = f"tighter stop on EXPIRED: mae={mae_abs:.1f}>={real_stop}"
            else:
                new_pnl = orig_pnl
                note = f"EXPIRED safe: mae={mae_abs:.1f} < {real_stop}"

    # CASE 2: Real stop is WIDER than default
    elif real_stop > default_stop:
        if orig_result == "LOSS":
            if mae_abs < real_stop:
                # Trade survived with wider stop
                if real_target and mfe_abs >= real_target:
                    new_result = "WIN"
                    new_pnl = real_target
                    note = f"wider stop saved->WIN: mae={mae_abs:.1f}<{real_stop}, hit target"
                elif mfe_abs >= BE_TRIGGER:
                    new_result = "EXPIRED"
                    new_pnl = 0
                    note = f"wider stop+BE saved: mae={mae_abs:.1f}<{real_stop}"
                else:
                    new_result = "EXPIRED"
                    new_pnl = 0
                    note = f"wider stop survived: mae={mae_abs:.1f}<{real_stop}"
            else:
                new_pnl = -real_stop
                note = f"wider stop but still hit: mae={mae_abs:.1f}>={real_stop}"
        elif orig_result == "WIN":
            new_pnl = orig_pnl
            note = "WIN (wider stop, no change)"
        elif orig_result == "EXPIRED":
            new_pnl = orig_pnl
            note = "EXPIRED (wider stop, no change)"

    return new_result, new_pnl, note, default_stop


def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            id, ts, setup_name, direction, grade, score,
            paradigm, spot, lis, target,
            greek_alignment, vix, overvix,
            outcome_result, outcome_pnl,
            outcome_max_profit, outcome_max_loss,
            outcome_target_level, outcome_stop_level,
            outcome_first_event, outcome_elapsed_min,
            abs_es_price, charm_limit_entry
        FROM setup_log
        WHERE ts >= '2026-03-01' AND ts < '2026-04-01'
          AND outcome_result IS NOT NULL
        ORDER BY ts
    """)
    columns = [desc[0] for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    conn.close()

    print(f"Total March 2026 trades with outcomes: {len(rows)}")

    # Apply V11 + enabled setups + time cutoff
    filtered = []
    v11_blocked = 0
    disabled_blocked = 0
    time_blocked = 0

    for r in rows:
        if not passes_v11(r):
            v11_blocked += 1
            continue
        sn = r["setup_name"]
        cfg = REAL_CONFIG.get(sn, {})
        if not cfg.get("enabled", False):
            disabled_blocked += 1
            continue
        ts_et = r["ts"].astimezone(ET) if r["ts"].tzinfo else ET.localize(r["ts"])
        if ts_et.time() > NO_TRADE_AFTER:
            time_blocked += 1
            continue
        filtered.append(r)

    print(f"\nFiltering:")
    print(f"  V11 blocked: {v11_blocked}")
    print(f"  Disabled setup blocked: {disabled_blocked}")
    print(f"  After-16:20 ET blocked: {time_blocked}")
    print(f"  Final trades: {len(filtered)}")

    has_both = sum(1 for r in filtered
                   if r["outcome_max_profit"] is not None and r["outcome_max_loss"] is not None)
    missing = len(filtered) - has_both
    print(f"\n  MFE+MAE available: {has_both}/{len(filtered)}")

    # Recalculate
    results = []
    for r in filtered:
        new_result, new_pnl, note, def_stop = recalculate_pnl(r)
        ts_et = r["ts"].astimezone(ET) if r["ts"].tzinfo else ET.localize(r["ts"])
        results.append({
            "id": r["id"],
            "ts": ts_et,
            "setup_name": r["setup_name"],
            "direction": r["direction"],
            "grade": r["grade"],
            "orig_result": r["outcome_result"],
            "orig_pnl": r["outcome_pnl"] or 0,
            "new_result": new_result,
            "new_pnl": new_pnl,
            "mfe": r["outcome_max_profit"],
            "mae": r["outcome_max_loss"],
            "real_stop": REAL_CONFIG[r["setup_name"]]["stop"],
            "default_stop": def_stop,
            "note": note,
        })

    # ================================================================
    # RESULTS
    # ================================================================
    print("\n" + "=" * 90)
    print("  REAL MONEY SIMULATION -- March 2026")
    print(f"  8 MES x $5/pt = $40/pt | Commission: ${COMMISSION_RT:.2f}/trade")
    print("=" * 90)

    total_pts = sum(r["new_pnl"] for r in results)
    total_trades = len(results)
    wins = sum(1 for r in results if r["new_result"] == "WIN")
    losses = sum(1 for r in results if r["new_result"] == "LOSS")
    expired = sum(1 for r in results if r["new_result"] == "EXPIRED")
    wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
    gross_dollar = total_pts * DOLLARS_PER_POINT
    total_comm = total_trades * COMMISSION_RT
    net_dollar = gross_dollar - total_comm

    trading_days = len(set(r["ts"].date() for r in results))
    print(f"\n  OVERALL ({total_trades} trades, {trading_days} trading days)")
    print(f"  " + "-" * 60)
    print(f"  Wins: {wins} | Losses: {losses} | Expired: {expired}")
    print(f"  Win Rate: {wr:.1f}%")
    print(f"  Total Points: {total_pts:+.1f}")
    print(f"  Gross P&L:    ${gross_dollar:+,.2f}")
    print(f"  Commissions:  -${total_comm:,.2f}")
    print(f"  NET P&L:      ${net_dollar:+,.2f}")
    print(f"  Per trade avg: ${net_dollar / total_trades:+,.2f}")
    print(f"  Per day avg:   ${net_dollar / trading_days:+,.2f}")

    # By setup
    print(f"\n  " + "-" * 80)
    print(f"  BREAKDOWN BY SETUP")
    print(f"  " + "-" * 80)

    ss = defaultdict(lambda: {"t": 0, "w": 0, "l": 0, "e": 0, "pts": 0, "orig_pts": 0})
    for r in results:
        sn = r["setup_name"]
        ss[sn]["t"] += 1
        ss[sn]["pts"] += r["new_pnl"]
        ss[sn]["orig_pts"] += r["orig_pnl"]
        if r["new_result"] == "WIN":
            ss[sn]["w"] += 1
        elif r["new_result"] == "LOSS":
            ss[sn]["l"] += 1
        else:
            ss[sn]["e"] += 1

    for sn in sorted(ss, key=lambda x: -ss[x]["pts"]):
        s = ss[sn]
        wr_s = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
        gross = s["pts"] * DOLLARS_PER_POINT
        comm = s["t"] * COMMISSION_RT
        net = gross - comm
        real_sl = REAL_CONFIG[sn]["stop"]
        delta_pts = s["pts"] - s["orig_pts"]
        print(f"\n  {sn} (real SL={real_sl}):")
        print(f"    Trades: {s['t']}  W/L/E: {s['w']}/{s['l']}/{s['e']}  WR: {wr_s:.1f}%")
        print(f"    Points: {s['pts']:+.1f} (was {s['orig_pts']:+.1f}, delta {delta_pts:+.1f})")
        print(f"    Gross: ${gross:+,.2f}  Comm: -${comm:,.2f}  Net: ${net:+,.2f}")

    # Outcome changes
    changes = [r for r in results if r["new_result"] != r["orig_result"] or abs(r["new_pnl"] - r["orig_pnl"]) > 0.5]
    print(f"\n  " + "-" * 80)
    print(f"  OUTCOME CHANGES ({len(changes)} trades affected by stop differences)")
    print(f"  " + "-" * 80)

    for r in sorted(changes, key=lambda x: x["ts"]):
        changed_str = ""
        if r["new_result"] != r["orig_result"]:
            changed_str = f"{r['orig_result']:>8s} -> {r['new_result']:<8s}"
        else:
            changed_str = f"{r['new_result']:>8s} (same)    "
        mae_str = f"MAE={abs(r['mae']):.1f}" if r['mae'] is not None else "MAE=n/a"
        mfe_str = f"MFE={abs(r['mfe']):.1f}" if r['mfe'] is not None else "MFE=n/a"
        print(f"    {r['ts'].strftime('%m/%d %H:%M')} {r['setup_name']:20s} "
              f"SL {r['default_stop']:.0f}->{r['real_stop']}  "
              f"{changed_str}  "
              f"PnL: {r['orig_pnl']:+6.1f} -> {r['new_pnl']:+6.1f}  "
              f"{mae_str} {mfe_str}  "
              f"({r['note']})")

    # Comparison
    print(f"\n  " + "-" * 80)
    print(f"  COMPARISON: DEFAULT STOPS vs REAL MONEY STOPS")
    print(f"  " + "-" * 80)
    orig_pts = sum(r["orig_pnl"] for r in results)
    orig_dollar = orig_pts * DOLLARS_PER_POINT
    print(f"  Default stops:  {orig_pts:+.1f} pts = ${orig_dollar:+,.2f} gross")
    print(f"  Real stops:     {total_pts:+.1f} pts = ${gross_dollar:+,.2f} gross")
    diff_pts = total_pts - orig_pts
    print(f"  Difference:     {diff_pts:+.1f} pts = ${diff_pts * DOLLARS_PER_POINT:+,.2f}")
    print(f"  After comm:     ${net_dollar:+,.2f}")

    # Daily P&L
    print(f"\n  " + "-" * 80)
    print(f"  DAILY P&L")
    print(f"  " + "-" * 80)

    daily = defaultdict(lambda: {"trades": 0, "pts": 0, "w": 0, "l": 0, "e": 0})
    for r in results:
        d = r["ts"].date()
        daily[d]["trades"] += 1
        daily[d]["pts"] += r["new_pnl"]
        if r["new_result"] == "WIN":
            daily[d]["w"] += 1
        elif r["new_result"] == "LOSS":
            daily[d]["l"] += 1
        else:
            daily[d]["e"] += 1

    running_net = 0
    peak = 0
    max_dd = 0
    for d in sorted(daily):
        s = daily[d]
        day_gross = s["pts"] * DOLLARS_PER_POINT
        day_comm = s["trades"] * COMMISSION_RT
        day_net = day_gross - day_comm
        running_net += day_net
        peak = max(peak, running_net)
        dd = peak - running_net
        max_dd = max(max_dd, dd)
        day_name = d.strftime("%a")
        marker = " <<" if day_net < 0 else ""
        print(f"    {d} ({day_name}) | {s['trades']:2d}t | "
              f"{s['w']}W {s['l']}L {s['e']}E | "
              f"{s['pts']:+7.1f} pts | "
              f"${day_net:+9,.2f} | "
              f"cumul: ${running_net:+10,.2f}{marker}")

    green = sum(1 for d in daily
                if (daily[d]["pts"] * DOLLARS_PER_POINT - daily[d]["trades"] * COMMISSION_RT) > 0)
    red = sum(1 for d in daily
              if (daily[d]["pts"] * DOLLARS_PER_POINT - daily[d]["trades"] * COMMISSION_RT) < 0)
    flat = len(daily) - green - red

    print(f"\n  Peak equity:      ${peak:+,.2f}")
    print(f"  Max drawdown:     ${max_dd:,.2f}")
    print(f"  Green/Red/Flat:   {green}/{red}/{flat}")
    print(f"  Profit factor:    {abs(sum(r['new_pnl'] for r in results if r['new_pnl'] > 0)) / max(abs(sum(r['new_pnl'] for r in results if r['new_pnl'] < 0)), 0.01):.2f}")

    # Assumptions
    print(f"\n  " + "-" * 80)
    print(f"  ASSUMPTIONS & NOTES")
    print(f"  " + "-" * 80)
    print(f"  1. Default stops taken from ACTUAL DB outcome_stop_level (not CLAUDE.md)")
    print(f"     SC=20, DD=12, AG=dynamic(avg 17), PR=15, ESA=8")
    print(f"  2. Real config stops: SC=12, DD=12, AG=12, PR=12, ESA=8")
    print(f"  3. Key changes: SC 20->12 (big impact), PR 15->12, AG dynamic->12")
    print(f"  4. BE trigger 5 pts: if MFE>=5 before MAE>=stop, stop moves to BE")
    print(f"  5. MFE/MAE available for ALL {has_both} trades (100%)")
    print(f"  6. Temporal ordering approximated: first_event + MFE vs MAE magnitude")
    print(f"  7. Trailing stop behavior NOT re-simulated (original trail PnL kept)")
    print(f"  8. No slippage modeled")
    print(f"  9. Flat 16:44 CT not modeled (few trades run that late)")


if __name__ == "__main__":
    main()
