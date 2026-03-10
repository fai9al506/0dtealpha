"""Check recent TS SIM trade results."""
import os, sys
sys.stdout.reconfigure(encoding='utf-8')
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

with engine.begin() as conn:
    rows = conn.execute(text(
        "SELECT id, ts, setup_name, direction, grade, score, spot, "
        "outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss, "
        "greek_alignment, paradigm, spot_vol_beta "
        "FROM setup_log "
        "WHERE outcome_result IN ('WIN', 'LOSS') "
        "AND grade != 'LOG' "
        "AND ts >= now() - interval '7 days' "
        "ORDER BY ts ASC"
    )).mappings().all()

print(f"Trades last 7 days: {len(rows)}")
total_pnl = 0
wins = 0
losses = 0
for r in rows:
    pnl = r['outcome_pnl'] or 0
    total_pnl += pnl
    if r['outcome_result'] == 'WIN': wins += 1
    else: losses += 1

if wins + losses > 0:
    print(f"W={wins} L={losses} WR={wins/(wins+losses)*100:.1f}% PnL={total_pnl:+.1f}")
print()

# Per-day summary
from collections import defaultdict
daily = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "trades": []})
for r in rows:
    d = r['ts'].strftime('%Y-%m-%d')
    pnl = r['outcome_pnl'] or 0
    daily[d]["pnl"] += pnl
    if r['outcome_result'] == 'WIN':
        daily[d]["w"] += 1
    else:
        daily[d]["l"] += 1
    daily[d]["trades"].append(r)

print("DAILY SUMMARY:")
cum = 0
for d in sorted(daily.keys()):
    dd = daily[d]
    cum += dd["pnl"]
    n = dd["w"] + dd["l"]
    wr = dd["w"]/n*100 if n else 0
    print(f"  {d}  W={dd['w']:>2d} L={dd['l']:>2d} WR={wr:5.1f}%  Day={dd['pnl']:>+7.1f}  Cum={cum:>+8.1f}")

print()
print("ALL TRADES:")
for r in rows:
    ts = r['ts'].strftime('%m/%d %H:%M')
    pnl = r['outcome_pnl'] or 0
    align = r['greek_alignment']
    align_s = f"{align:+d}" if align is not None else " ?"
    mp = r['outcome_max_profit'] or 0
    ml = r['outcome_max_loss'] or 0
    paradigm = (r['paradigm'] or '')[:15]
    print(f"  {ts}  {r['setup_name']:20s} {r['direction']:8s} [{r['grade']}] "
          f"align={align_s:>3s}  {r['outcome_result']:4s} {pnl:>+6.1f}  "
          f"maxP={mp:>+5.1f} maxL={ml:>+5.1f}  {paradigm}")

# Breakdown: how many trades would have been blocked by FIXED filter
print()
print("FIXED FILTER IMPACT (would-have-been-blocked):")
blocked = []
for r in rows:
    a = r['greek_alignment']
    if a is not None and a < 0:
        blocked.append(r)
    # F2
    elif r['setup_name'] == 'AG Short' and a is not None and a == -3:
        blocked.append(r)
    # F3
    elif r['setup_name'] == 'DD Exhaustion' and r['spot_vol_beta'] is not None:
        if -0.5 <= r['spot_vol_beta'] <= 0:
            blocked.append(r)

b_pnl = sum((r['outcome_pnl'] or 0) for r in blocked)
b_wins = sum(1 for r in blocked if r['outcome_result'] == 'WIN')
b_losses = len(blocked) - b_wins
print(f"  Would block: {len(blocked)} trades (W={b_wins} L={b_losses}) PnL={b_pnl:+.1f}")
for r in blocked:
    ts = r['ts'].strftime('%m/%d %H:%M')
    pnl = r['outcome_pnl'] or 0
    align = r['greek_alignment']
    align_s = f"{align:+d}" if align is not None else " ?"
    print(f"    {ts}  {r['setup_name']:20s} {r['direction']:8s} align={align_s:>3s}  "
          f"{r['outcome_result']:4s} {pnl:>+6.1f}")
