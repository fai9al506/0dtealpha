"""Calculate today's hypothetical MES auto-trade P&L from setup logs."""
import os, sys
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT id, setup_name, direction, grade, score,
               spot, target, outcome_target_level, outcome_stop_level,
               outcome_result, outcome_pnl, outcome_first_event,
               outcome_max_profit, outcome_max_loss, outcome_elapsed_min,
               ts, bofa_stop_level, bofa_target_level
        FROM setup_log
        WHERE DATE(ts AT TIME ZONE 'America/New_York') = '2026-02-20'
        ORDER BY ts
    """)).mappings().all()

print(f"=== TODAY'S SETUPS ({len(rows)} trades) ===\n")

TOTAL_QTY = 10
T1_QTY = 5
T2_QTY = 5
FIRST_TARGET_PTS = 10.0
MES_TICK = 5.0  # $5 per point per MES contract

SINGLE_TARGET = {"BofA Scalp", "ES Absorption", "Paradigm Reversal"}
SPLIT_TARGET = {"GEX Long", "AG Short", "DD Exhaustion"}

total_mes_pnl = 0.0
wins = 0
losses = 0
opens = 0

for r in rows:
    setup = r['setup_name']
    direction = r['direction']
    result = r['outcome_result']
    spx_pnl = r['outcome_pnl'] or 0
    first_event = r['outcome_first_event']
    max_profit = r['outcome_max_profit'] or 0
    entry = r['spot']
    target_level = r['outcome_target_level']
    stop_level = r['outcome_stop_level']
    fired = r['ts']
    grade = r['grade']

    # Calculate distances
    target_dist = abs(target_level - entry) if (entry and target_level) else 0
    stop_dist = abs(entry - stop_level) if (entry and stop_level) else 0

    if not result:
        opens += 1
        mes_pnl = 0
        detail = "  (still open)"
    elif setup in SINGLE_TARGET:
        # Flow A: 10 MES, single target @ +10pts
        if result == "WIN":
            wins += 1
            mes_pnl = TOTAL_QTY * FIRST_TARGET_PTS * MES_TICK
        elif result == "LOSS":
            losses += 1
            mes_pnl = -TOTAL_QTY * stop_dist * MES_TICK
        else:  # EXPIRED
            mes_pnl = spx_pnl * TOTAL_QTY * MES_TICK
        detail = f"  Flow A: {TOTAL_QTY}@+{FIRST_TARGET_PTS}pts | stop={stop_dist:.1f}pts"

    elif setup in SPLIT_TARGET:
        # Flow B: T1=5@+10, T2=5@full_target or trail
        if result == "WIN":
            wins += 1
            # Did T1 fill? If outcome_pnl >= 10 or max_profit >= 10,
            # price definitely crossed +10 at some point -> T1 filled
            t1_would_fill = (spx_pnl >= FIRST_TARGET_PTS) or (max_profit >= FIRST_TARGET_PTS)
            if t1_would_fill:
                t1_pnl = T1_QTY * FIRST_TARGET_PTS * MES_TICK  # T1: 5@+10
                # T2: exits at outcome_pnl (trail exit or target hit)
                t2_pts = spx_pnl  # The actual exit level
                t2_pnl = T2_QTY * t2_pts * MES_TICK
                mes_pnl = t1_pnl + t2_pnl
                detail = f"  Flow B: T1=5@+10=${t1_pnl:.0f} + T2=5@{t2_pts:.1f}=${t2_pnl:.0f}"
            else:
                # pnl < 10 — T1 didn't fill, both exit at same level
                mes_pnl = spx_pnl * TOTAL_QTY * MES_TICK
                detail = f"  Flow B: No T1 fill (pnl={spx_pnl:.1f}), 10@{spx_pnl:.1f}"
        elif result == "LOSS":
            losses += 1
            if max_profit >= FIRST_TARGET_PTS:
                # T1 filled, then stop hit remaining 5
                t1_pnl = T1_QTY * FIRST_TARGET_PTS * MES_TICK
                t2_loss = T2_QTY * stop_dist * MES_TICK
                mes_pnl = t1_pnl - t2_loss
                detail = f"  Flow B: T1=5@+10=${t1_pnl:.0f} - Stop 5@{stop_dist:.1f}=-${t2_loss:.0f}"
            else:
                # Full stop, no T1 fill
                mes_pnl = -TOTAL_QTY * stop_dist * MES_TICK
                detail = f"  Flow B: Full stop {TOTAL_QTY}@{stop_dist:.1f}"
        else:  # EXPIRED
            if max_profit >= FIRST_TARGET_PTS:
                t1_pnl = T1_QTY * FIRST_TARGET_PTS * MES_TICK
                t2_pnl = T2_QTY * spx_pnl * MES_TICK
                mes_pnl = t1_pnl + t2_pnl
                detail = f"  Flow B: T1=5@+10=${t1_pnl:.0f} + expire 5@{spx_pnl:.1f}=${t2_pnl:.0f}"
            else:
                mes_pnl = spx_pnl * TOTAL_QTY * MES_TICK
                detail = f"  Flow B: Expired {spx_pnl:.1f}pts x{TOTAL_QTY}"
    else:
        mes_pnl = 0
        detail = f"  Unknown setup type"

    total_mes_pnl += mes_pnl
    time_str = fired.strftime('%H:%M') if fired else '?'

    result_str = result or "OPEN"
    pnl_str = f"${mes_pnl:+,.0f}" if result else "$?"
    print(f"{time_str} | {setup:20s} | {direction:6s} | {grade:7s} | {result_str:7s} | "
          f"SPX {spx_pnl:+6.1f}pts | maxP {max_profit:+6.1f} | MES {pnl_str:>8s}")
    print(detail)
    print()

resolved = wins + losses
wr = (wins / resolved * 100) if resolved else 0
print(f"{'='*80}")
print(f"RESOLVED: {resolved} trades ({wins}W / {losses}L / {opens} open) | Win Rate: {wr:.0f}%")
print(f"TOTAL MES P&L: ${total_mes_pnl:+,.0f}")
print(f"({TOTAL_QTY} MES x $5/pt, split-target: T1={T1_QTY}@+{FIRST_TARGET_PTS:.0f}pts, T2={T2_QTY}@full target)")
