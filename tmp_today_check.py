import os
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Mar 9 full day results
rows = c.execute(text("""
    SELECT to_char(ts, 'HH24:MI') as t, setup_name, direction, grade,
           outcome_result, outcome_pnl, greek_alignment
    FROM setup_log
    WHERE ts::date = '2026-03-09'
    ORDER BY ts
""")).fetchall()

total_pnl = 0
wins = 0
losses = 0
expired = 0
print(f"=== Mar 9 FULL DAY: {len(rows)} trades ===\n")
for r in rows:
    pnl = float(r[5] or 0)
    total_pnl += pnl
    if r[4] and 'WIN' in r[4]: wins += 1
    elif r[4] and 'LOSS' in r[4]: losses += 1
    elif r[4] and 'EXPIR' in r[4]: expired += 1
    print(f"  {r[0]} {r[1]:20s} {r[2]:7s} {r[3]:7s} {str(r[4] or 'OPEN'):15s} {pnl:+7.1f} align={r[6]}")

print(f"\nTOTAL: {wins}W / {losses}L / {expired}X = {total_pnl:+.1f} pts")

# Which setups are enabled on eval real?
eval_setups = ['AG Short', 'DD Exhaustion', 'Paradigm Reversal', 'Skew Charm']
print(f"\n=== EVAL REAL (enabled: {', '.join(eval_setups)}) ===")
print("Greek filter ON, max_stop_loss_pts=12\n")

eval_pnl = 0
eval_trades = 0
for r in rows:
    if r[1] not in eval_setups:
        continue
    pnl = float(r[5] or 0)
    # Cap stop at 12
    if r[4] and 'LOSS' in r[4] and pnl < -12:
        pnl = -12.0
    eval_pnl += pnl
    eval_trades += 1
    tag = " ** CAPPED" if r[4] and 'LOSS' in r[4] and float(r[5] or 0) < -12 else ""
    print(f"  {r[0]} {r[1]:20s} {r[2]:7s} {str(r[4] or 'OPEN'):15s} {pnl:+7.1f}{tag}")

print(f"\nEVAL REAL trades: {eval_trades}, PnL (capped): {eval_pnl:+.1f}")

# SIM auto-trader - all setups
print(f"\n=== SIM AUTO-TRADER (all setups, stacking) ===")
sim_setups = ['AG Short', 'DD Exhaustion', 'Paradigm Reversal', 'Skew Charm',
              'BofA Scalp', 'GEX Long', 'ES Absorption']
sim_pnl = sum(float(r[5] or 0) for r in rows if r[1] in sim_setups)
sim_n = sum(1 for r in rows if r[1] in sim_setups)
print(f"All setups: {sim_n} trades, PnL={sim_pnl:+.1f}")

# Daily summary for last 7 trading days
print(f"\n=== DAILY SUMMARY (last 7 days) ===")
days = c.execute(text("""
    SELECT ts::date as d, count(*),
           sum(case when outcome_result like '%WIN%' then 1 else 0 end),
           sum(case when outcome_result like '%LOSS%' then 1 else 0 end),
           round(sum(coalesce(outcome_pnl,0))::numeric, 1)
    FROM setup_log
    WHERE ts::date >= '2026-02-28'
    GROUP BY ts::date ORDER BY ts::date
""")).fetchall()
running = 0
for d in days:
    running += float(d[4])
    print(f"  {d[0]}  {d[1]:2d} trades  {d[2]}W/{d[3]}L  PnL={d[4]:+7.1f}  running={running:+.1f}")

c.close()
