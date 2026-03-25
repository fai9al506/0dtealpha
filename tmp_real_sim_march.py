#!/usr/bin/env python3
"""Simulate Real Trader PnL for March 2026.
SC only, 1 MES ($5/pt), SL=14, max 2/dir concurrent, $300 daily loss limit, V11 filter.

KEY FINDING: setup_log.outcome_pnl uses the DEPLOYED SL at the time:
  - Mar 1-19: SL=20 (old code)
  - Mar 20+: SL=14 (deployed ~Mar 19 night)

real_trader.py has SL=14 (receives stop_dist from setup_detector which uses SL=14).
If real_trader had been running all March with SL=14, pre-Mar-20 losses of -20 should be -14.

We present BOTH scenarios:
  Scenario A: "As-deployed" (setup_log PnL as-is: SL=20 pre-Mar-20, SL=14 post)
  Scenario B: "SL=14 throughout" (pre-Mar-20 losses corrected -20 -> -14)
"""

import psycopg2, os
from datetime import datetime, timedelta, time as dtime, date
from collections import defaultdict
import zoneinfo

NY = zoneinfo.ZoneInfo("US/Eastern")
SL14_CUTOFF = date(2026, 3, 20)

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    SELECT id, ts, setup_name, direction, grade, score, paradigm, spot,
           outcome_result, outcome_pnl, outcome_target_level, outcome_stop_level,
           outcome_max_profit, outcome_max_loss, outcome_first_event, outcome_elapsed_min,
           greek_alignment, charm_limit_entry, vix, overvix
    FROM setup_log
    WHERE setup_name = 'Skew Charm'
      AND ts >= '2026-03-01'::date
      AND ts < '2026-04-01'::date
    ORDER BY ts ASC
""")
rows = cur.fetchall()
cols = [d[0] for d in cur.description]
all_trades = [dict(zip(cols, r)) for r in rows]
conn.close()

for t in all_trades:
    t["ts_et"] = t["ts"].astimezone(NY)
    t["trade_date"] = t["ts_et"].date()
    t["time_et"] = t["ts_et"].time()
    t["pnl_A"] = t["outcome_pnl"] or 0.0  # as-deployed
    # Scenario B: correct pre-Mar-20 losses from -20 to -14
    if (t["trade_date"] < SL14_CUTOFF
            and t["outcome_result"] == "LOSS"
            and t["outcome_pnl"] == -20.0):
        t["pnl_B"] = -14.0
    else:
        t["pnl_B"] = t["outcome_pnl"] or 0.0


# ====== V11 FILTER ======
def passes_v11(t):
    grade = t["grade"]
    direction = t["direction"]
    paradigm = t["paradigm"]
    align = t["greek_alignment"] or 0
    time_et = t["time_et"]

    if grade in ("C", "LOG"):
        return False, "grade_blocked"
    if dtime(14, 30) <= time_et < dtime(15, 0):
        return False, "time_1430_1500"
    if time_et >= dtime(15, 30):
        return False, "time_1530+"

    is_long = direction.lower() in ("long", "bullish")
    if is_long:
        if align < 2:
            return False, "align_low"
        return True, "pass"
    else:
        if paradigm == "GEX-LIS":
            return False, "gex_lis_blocked"
        return True, "pass"


# ====== SIMULATION ======
MES_POINT_VALUE = 5.0
QTY = 1
MAX_CONCURRENT_PER_DIR = 2
DAILY_LOSS_LIMIT = 300.0


def run_simulation(trades, pnl_key, commission_rt, label):
    """Run the full simulation with given PnL key and commission."""
    open_positions = []
    daily_loss = defaultdict(float)
    taken = []
    skip_v11 = []
    skip_conc = []
    skip_limit = []

    def est_close(t):
        fired = t["ts_et"]
        elapsed = t["outcome_elapsed_min"]
        if elapsed and elapsed > 0:
            return fired + timedelta(minutes=elapsed)
        out = t["outcome_result"]
        if out == "WIN":
            return fired + timedelta(minutes=30)
        elif out == "LOSS":
            return fired + timedelta(minutes=15)
        return fired.replace(hour=16, minute=0, second=0, microsecond=0)

    def count_open(positions, is_long, at_time):
        c = 0
        for p in positions:
            p_long = p["direction"].lower() in ("long", "bullish")
            if p_long == is_long and p["fired_at"] <= at_time < p["close_at"]:
                c += 1
        return c

    for t in trades:
        td = t["trade_date"]
        fired = t["ts_et"]

        ok, reason = passes_v11(t)
        if not ok:
            skip_v11.append((t, reason))
            continue

        if daily_loss[td] >= DAILY_LOSS_LIMIT:
            skip_limit.append(t)
            continue

        is_long = t["direction"].lower() in ("long", "bullish")
        if count_open(open_positions, is_long, fired) >= MAX_CONCURRENT_PER_DIR:
            skip_conc.append(t)
            continue

        close_at = est_close(t)
        open_positions.append({
            "id": t["id"], "direction": t["direction"],
            "fired_at": fired, "close_at": close_at,
        })

        pnl_dollars = t[pnl_key] * MES_POINT_VALUE * QTY
        if pnl_dollars < 0:
            daily_loss[td] += abs(pnl_dollars)

        taken.append(t)

    # ---- Print results ----
    total = len(taken)
    if total == 0:
        print(f"No trades taken for {label}")
        return

    wins = sum(1 for t in taken if t["outcome_result"] == "WIN")
    losses = sum(1 for t in taken if t["outcome_result"] == "LOSS")
    expired = sum(1 for t in taken if t["outcome_result"] == "EXPIRED")
    wr = wins / total * 100

    total_pts = sum(t[pnl_key] for t in taken)
    gross_dollars = total_pts * MES_POINT_VALUE * QTY
    total_comm = total * commission_rt
    net_dollars = gross_dollars - total_comm

    print()
    print("=" * 95)
    print(f"  {label}")
    print("=" * 95)
    print(f"Config: SC only, 1 MES ($5/pt), max 2 concurrent/direction, $300 daily loss limit")
    print(f"Commission: ${commission_rt:.2f} round-trip per trade")
    print()

    # Filter breakdown
    print("--- FILTER BREAKDOWN ---")
    print(f"Total SC trades in DB:          {len(trades)}")
    print(f"Blocked by V11 filter:          {len(skip_v11)}")
    v11r = defaultdict(int)
    for _, reason in skip_v11:
        v11r[reason] += 1
    for reason, count in sorted(v11r.items(), key=lambda x: -x[1]):
        print(f"  - {reason}: {count}")
    print(f"Blocked by concurrency cap:     {len(skip_conc)}")
    print(f"Blocked by daily loss limit:    {len(skip_limit)}")
    print(f"TRADES TAKEN:                   {total}")
    print()

    # Outcomes
    print("--- OUTCOME BREAKDOWN ---")
    print(f"WIN:     {wins:3d}  ({wins/total*100:.1f}%)")
    print(f"LOSS:    {losses:3d}  ({losses/total*100:.1f}%)")
    print(f"EXPIRED: {expired:3d}  ({expired/total*100:.1f}%)")
    print(f"Win Rate: {wr:.1f}%")
    print()

    # PnL
    print("--- P&L SUMMARY ---")
    print(f"Total PnL (points):   {total_pts:+.1f} pts")
    print(f"Gross PnL (dollars):  ${gross_dollars:+,.2f}")
    print(f"Commissions ({total}t x ${commission_rt:.2f}): -${total_comm:,.2f}")
    print(f"Net PnL (dollars):    ${net_dollars:+,.2f}")
    print(f"Avg PnL/trade (pts):  {total_pts/total:+.2f}")
    print(f"Avg PnL/trade ($):    ${net_dollars/total:+,.2f}")
    print()

    # Per-day
    print("--- PER-DAY BREAKDOWN ---")
    hdr = f"{'Date':12s} {'Day':4s} {'#':>3s} {'W':>3s} {'L':>3s} {'E':>3s} {'PnL pts':>9s} {'Gross $':>10s} {'Comm':>7s} {'Net $':>10s} {'CumNet $':>10s}"
    print(hdr)
    print("-" * len(hdr))

    daily_data = defaultdict(list)
    for t in taken:
        daily_data[t["trade_date"]].append(t)

    equity_curve = []
    running_eq = 0.0
    green = red = flat = 0

    for d in sorted(daily_data.keys()):
        dt = daily_data[d]
        dp = sum(t[pnl_key] for t in dt)
        dg = dp * MES_POINT_VALUE * QTY
        dc = len(dt) * commission_rt
        dn = dg - dc
        dw = sum(1 for t in dt if t["outcome_result"] == "WIN")
        dl = sum(1 for t in dt if t["outcome_result"] == "LOSS")
        de = sum(1 for t in dt if t["outcome_result"] == "EXPIRED")

        running_eq += dn
        equity_curve.append((d, running_eq))

        if dn > 0.01:
            green += 1
        elif dn < -0.01:
            red += 1
        else:
            flat += 1

        day_str = d.strftime("%a")[:3]
        print(f"{d}  {day_str:4s} {len(dt):3d} {dw:3d} {dl:3d} {de:3d}  {dp:+8.1f}  ${dg:+9,.2f}  ${dc:6,.2f}  ${dn:+9,.2f}  ${running_eq:+9,.2f}")

    print("-" * len(hdr))
    print(f"{'TOTAL':12s}       {total:3d} {wins:3d} {losses:3d} {expired:3d}  {total_pts:+8.1f}  ${gross_dollars:+9,.2f}  ${total_comm:6,.2f}  ${net_dollars:+9,.2f}")
    print()
    print(f"Green days: {green}  |  Red days: {red}  |  Flat days: {flat}")

    # Drawdown
    peak = 0.0
    max_dd = 0.0
    dd_from = dd_to = dd_start = None
    for d, eq in equity_curve:
        if eq > peak:
            peak = eq
            dd_start = d
        dd = peak - eq
        if dd > max_dd:
            max_dd = dd
            dd_from = dd_start
            dd_to = d

    peak_eq = 0.0
    max_tbt = 0.0
    run_eq = 0.0
    for t in taken:
        p = t[pnl_key] * MES_POINT_VALUE * QTY - commission_rt
        run_eq += p
        if run_eq > peak_eq:
            peak_eq = run_eq
        dd = peak_eq - run_eq
        if dd > max_tbt:
            max_tbt = dd

    print()
    print("--- DRAWDOWN ---")
    print(f"Max Daily Drawdown:         ${max_dd:,.2f}")
    if dd_from and dd_to:
        print(f"  Period: {dd_from} to {dd_to}")
    print(f"Max Trade-by-Trade DD:      ${max_tbt:,.2f}")

    # Longs vs shorts
    long_t = [t for t in taken if t["direction"].lower() in ("long", "bullish")]
    short_t = [t for t in taken if t["direction"].lower() in ("short", "bearish")]
    print()
    print("--- DIRECTION BREAKDOWN ---")
    if long_t:
        lp = sum(t[pnl_key] for t in long_t)
        lw = sum(1 for t in long_t if t["outcome_result"] == "WIN")
        print(f"Longs:  {len(long_t)} trades, {lw}W ({lw/len(long_t)*100:.0f}% WR), "
              f"{lp:+.1f} pts (${lp*5:+,.2f})")
    if short_t:
        sp = sum(t[pnl_key] for t in short_t)
        sw = sum(1 for t in short_t if t["outcome_result"] == "WIN")
        print(f"Shorts: {len(short_t)} trades, {sw}W ({sw/len(short_t)*100:.0f}% WR), "
              f"{sp:+.1f} pts (${sp*5:+,.2f})")

    # Grade breakdown
    print()
    print("--- GRADE BREAKDOWN ---")
    for grade in ["A+", "A", "B"]:
        gt = [t for t in taken if t["grade"] == grade]
        if gt:
            gp = sum(t[pnl_key] for t in gt)
            gw = sum(1 for t in gt if t["outcome_result"] == "WIN")
            print(f"  {grade:3s}: {len(gt)} trades, {gw}W ({gw/len(gt)*100:.0f}% WR), "
                  f"{gp:+.1f} pts (${gp*5:+,.2f})")

    # Win/loss averages
    w_pnls = [t[pnl_key] for t in taken if t["outcome_result"] == "WIN"]
    l_pnls = [t[pnl_key] for t in taken if t["outcome_result"] == "LOSS"]
    e_pnls = [t[pnl_key] for t in taken if t["outcome_result"] == "EXPIRED"]
    print()
    print("--- AVERAGES ---")
    if w_pnls:
        print(f"Avg Win:     {sum(w_pnls)/len(w_pnls):+.1f} pts (${sum(w_pnls)/len(w_pnls)*5:+,.2f})")
    if l_pnls:
        print(f"Avg Loss:    {sum(l_pnls)/len(l_pnls):+.1f} pts (${sum(l_pnls)/len(l_pnls)*5:+,.2f})")
    if e_pnls:
        print(f"Avg Expired: {sum(e_pnls)/len(e_pnls):+.1f} pts (${sum(e_pnls)/len(e_pnls)*5:+,.2f})")
    if w_pnls and l_pnls:
        tot_w = sum(w_pnls) + sum(p for p in e_pnls if p > 0)
        tot_l = abs(sum(l_pnls)) + abs(sum(p for p in e_pnls if p < 0))
        pf = tot_w / tot_l if tot_l > 0 else float("inf")
        print(f"Profit Factor: {pf:.2f}")

    # Charm limit
    cl = [t for t in taken if t["charm_limit_entry"] is not None]
    print(f"\nCharm S/R limit entries: {len(cl)} / {len(short_t)} shorts")
    if cl:
        clw = sum(1 for t in cl if t["outcome_result"] == "WIN")
        clp = sum(t[pnl_key] for t in cl)
        print(f"  WR: {clw}/{len(cl)} ({clw/len(cl)*100:.0f}%), PnL: {clp:+.1f} pts")

    # Concurrency-skipped
    print()
    print("--- CONCURRENCY-SKIPPED TRADES ---")
    if skip_conc:
        cp = sum(t[pnl_key] for t in skip_conc)
        cw = sum(1 for t in skip_conc if t["outcome_result"] == "WIN")
        cl2 = sum(1 for t in skip_conc if t["outcome_result"] == "LOSS")
        ce = sum(1 for t in skip_conc if t["outcome_result"] == "EXPIRED")
        print(f"  {len(skip_conc)} trades (would-be: {cw}W/{cl2}L/{ce}E, {cp:+.1f} pts)")
        for t in skip_conc:
            p = t[pnl_key]
            print(f"    ID={t['id']} {t['ts_et'].strftime('%m/%d %H:%M')} "
                  f"{t['direction']:5s} {t['grade']:3s} {t['outcome_result']} {p:+.1f}")
    else:
        print("  None")

    # Daily-limit-skipped
    print()
    print("--- DAILY-LIMIT-SKIPPED TRADES ---")
    if skip_limit:
        dlp = sum(t[pnl_key] for t in skip_limit)
        dlw = sum(1 for t in skip_limit if t["outcome_result"] == "WIN")
        dll = sum(1 for t in skip_limit if t["outcome_result"] == "LOSS")
        dle = sum(1 for t in skip_limit if t["outcome_result"] == "EXPIRED")
        print(f"  {len(skip_limit)} trades (would-be: {dlw}W/{dll}L/{dle}E, {dlp:+.1f} pts)")
        for t in skip_limit:
            p = t[pnl_key]
            print(f"    ID={t['id']} {t['ts_et'].strftime('%m/%d %H:%M')} "
                  f"{t['direction']:5s} {t['grade']:3s} {t['outcome_result']} {p:+.1f}")
    else:
        print("  None")

    return {
        "total": total, "wins": wins, "losses": losses, "expired": expired,
        "total_pts": total_pts, "gross": gross_dollars, "comm": total_comm,
        "net": net_dollars, "green": green, "red": red, "max_dd": max_dd,
        "max_tbt_dd": max_tbt, "wr": wr,
    }


# ====== RUN BOTH SCENARIOS ======
print(f"Total SC trades in March 2026: {len(all_trades)}")

# Check SL distribution
pre_losses = [t for t in all_trades if t["trade_date"] < SL14_CUTOFF and t["outcome_result"] == "LOSS"]
post_losses = [t for t in all_trades if t["trade_date"] >= SL14_CUTOFF and t["outcome_result"] == "LOSS"]
print(f"Pre-Mar-20 LOSS trades (SL=20 in DB): {len(pre_losses)}")
print(f"Post-Mar-20 LOSS trades (SL=14 in DB): {len(post_losses)}")

# Scenario A: As-deployed PnL
result_a = run_simulation(all_trades, "pnl_A", 2.44,
    "SCENARIO A: As-Deployed (SL=20 pre-Mar-20, SL=14 post)")

# Scenario B: SL=14 throughout
result_b = run_simulation(all_trades, "pnl_B", 2.44,
    "SCENARIO B: SL=14 Throughout (real_trader.py config)")

# ====== COMPARISON ======
print()
print()
print("=" * 95)
print("  SCENARIO COMPARISON")
print("=" * 95)
print(f"{'Metric':30s} {'Scenario A (as-deployed)':>25s} {'Scenario B (SL=14)':>25s}")
print("-" * 80)
for key, label in [
    ("total", "Trades Taken"),
    ("wins", "Wins"),
    ("losses", "Losses"),
    ("expired", "Expired"),
    ("wr", "Win Rate"),
    ("total_pts", "Total PnL (pts)"),
    ("gross", "Gross PnL ($)"),
    ("comm", "Commissions ($)"),
    ("net", "Net PnL ($)"),
    ("green", "Green Days"),
    ("red", "Red Days"),
    ("max_dd", "Max Daily DD ($)"),
    ("max_tbt_dd", "Max Trade-by-Trade DD ($)"),
]:
    va = result_a[key]
    vb = result_b[key]
    if key == "wr":
        print(f"{label:30s} {va:>24.1f}% {vb:>24.1f}%")
    elif key in ("gross", "comm", "net", "max_dd", "max_tbt_dd"):
        print(f"{label:30s} ${va:>23,.2f} ${vb:>23,.2f}")
    elif key == "total_pts":
        print(f"{label:30s} {va:>24.1f} {vb:>24.1f}")
    else:
        print(f"{label:30s} {va:>25d} {vb:>25d}")

# SL=14 improvement
diff_pts = result_b["total_pts"] - result_a["total_pts"]
diff_net = result_b["net"] - result_a["net"]
print()
print(f"SL=14 improvement: {diff_pts:+.1f} pts / ${diff_net:+,.2f}")

print()
print("=" * 95)
print("NOTES & ASSUMPTIONS:")
print("  1. PnL from setup_log.outcome_pnl (same trail: BE@10, activation=10, gap=8)")
print("  2. Commission: $2.44 RT ($0.50 TS broker + $0.72 exchange/NFA per side)")
print("  3. Position close timing: outcome_elapsed_min (actual) or estimates (30m win, 15m loss)")
print("  4. Charm S/R limit entries: assumed ALL filled (setup_log has no fill/timeout status)")
print("  5. Daily loss limit: $300 cumulative realized losses. Checked pre-trade, not mid-trade.")
print("  6. V11 filter: grade gate (A+/A/B), time gates (14:30-15:00, 15:30+), GEX-LIS shorts")
print("  7. Data: March 1-23, 2026 (16 trading days, through most recent available)")
print("  8. Scenario B corrects pre-Mar-20 LOSS trades from -20 to -14 pts (all had MAE > 14)")
print("  9. real_trader.py was actually deployed mid-March; Scenario B = 'what if from day 1'")
