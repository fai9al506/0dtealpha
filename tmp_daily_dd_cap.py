import os
from sqlalchemy import create_engine, text
from collections import defaultdict

engine = create_engine(os.environ["DATABASE_URL"])

with engine.begin() as conn:
    all_rows = conn.execute(text("""
        SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, setup_name, direction,
               outcome_result, outcome_pnl, outcome_max_profit, grade
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND outcome_pnl IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

    all_paradigm = conn.execute(text("""
        SELECT ts AT TIME ZONE 'America/New_York' as ts_et, payload->>'paradigm' as paradigm
        FROM volland_snapshots
        WHERE payload->>'paradigm' IS NOT NULL
        ORDER BY ts ASC
    """)).mappings().all()

all_par = [(p['ts_et'], p['paradigm']) for p in all_paradigm]

def get_par(trade_ts):
    best = None
    for pts, par in all_par:
        if pts <= trade_ts:
            best = par
        elif pts > trade_ts:
            break
    return best or 'UNKNOWN'

eval_setups = {'AG Short', 'DD Exhaustion', 'ES Absorption', 'Paradigm Reversal'}
trailing = {'DD Exhaustion', 'AG Short', 'ES Absorption', 'GEX Long', 'Skew Charm'}

# Collect all filtered trades in order
trades = []
blocked = 0

for r in all_rows:
    setup = r['setup_name']
    if setup not in eval_setups:
        continue
    ts = r['ts_et']
    pnl = float(r['outcome_pnl'] or 0)
    max_p = float(r['outcome_max_profit'] or 0)
    result = r['outcome_result']
    paradigm = get_par(ts)

    if setup == 'DD Exhaustion':
        if ts.hour >= 14:
            blocked += 1
            continue
        if 'BOFA' in paradigm.upper() and 'PURE' in paradigm.upper():
            blocked += 1
            continue

    t1_hit = max_p >= 10
    if setup in trailing and t1_hit:
        split_pnl = round((10.0 + pnl) / 2, 1)
    else:
        split_pnl = pnl

    trades.append({
        "id": r["id"], "ts": ts, "setup": setup, "dir": r["direction"],
        "result": result, "pnl": split_pnl, "max_p": max_p,
        "day": ts.strftime("%Y-%m-%d")
    })

# 2 ES = 20 MES = $100/pt
DPP = 100  # dollars per point

for dd_cap_label, dd_cap in [("NO daily cap", None), ("$2000 daily loss cap", 2000), ("$1500 daily loss cap", 1500), ("$1000 daily loss cap", 1000)]:
    # Simulate trade by trade with daily loss cap
    daily_pnl = defaultdict(float)
    daily_trades_taken = defaultdict(int)
    daily_trades_skipped = defaultdict(int)
    daily_w = defaultdict(int)
    daily_l = defaultdict(int)

    for t in trades:
        day = t["day"]
        dollar_pnl = t["pnl"] * DPP

        # Check if daily loss cap hit
        if dd_cap is not None and daily_pnl[day] <= -dd_cap:
            daily_trades_skipped[day] += 1
            continue

        daily_pnl[day] += dollar_pnl
        daily_trades_taken[day] += 1
        if t["pnl"] > 0:
            daily_w[day] += 1
        elif t["pnl"] < 0:
            daily_l[day] += 1

    print(f"\n{'='*75}")
    print(f"  2 ES (20 MES, $100/pt) — {dd_cap_label}")
    print(f"{'='*75}")
    print(f"{'Day':>4} {'Date':>12} {'Taken':>5} {'Skip':>5} {'W':>3} {'L':>3} {'Day $':>9} {'Cum $':>9} {'DD $':>9}")
    print("-" * 70)

    cum = 0
    peak = 0
    max_dd = 0
    total_taken = 0
    total_skipped = 0
    pos_days = 0
    neg_days = 0
    best_day = -999999
    worst_day = 999999

    for i, d in enumerate(sorted(set(t["day"] for t in trades))):
        dp = daily_pnl[d]
        taken = daily_trades_taken[d]
        skipped = daily_trades_skipped[d]
        w = daily_w[d]
        l = daily_l[d]

        cum += dp
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
        total_taken += taken
        total_skipped += skipped
        best_day = max(best_day, dp)
        worst_day = min(worst_day, dp)
        if dp > 0: pos_days += 1
        elif dp < 0: neg_days += 1

        skip_mark = f" (skipped {skipped})" if skipped > 0 else ""
        print(f'{i+1:4d} {d:>12} {taken:5d} {skipped:5d} {w:3d} {l:3d} ${dp:>+8.0f} ${cum:>+8.0f} ${dd:>+8.0f}{skip_mark}')

    n_days = len(set(t["day"] for t in trades))
    print("-" * 70)
    print(f"  Total trades taken: {total_taken}, skipped: {total_skipped}")
    print(f"  Days: {n_days} ({pos_days} up, {neg_days} down)")
    print(f"  Total PnL: ${cum:+,.0f}")
    print(f"  Avg/day: ${cum/n_days:+,.0f}")
    print(f"  Best day: ${best_day:+,.0f}")
    print(f"  Worst day: ${worst_day:+,.0f}")
    print(f"  Max drawdown: ${max_dd:,.0f}")
    print(f"  Monthly proj (20d): ${cum/n_days*20:+,.0f}")

# Comparison summary
print(f"\n{'='*75}")
print("COMPARISON SUMMARY (2 ES)")
print(f"{'='*75}")
print(f"{'Scenario':>25} {'Total':>10} {'Avg/Day':>10} {'Monthly':>10} {'MaxDD':>10} {'Worst Day':>10}")
print("-" * 75)

for dd_cap_label, dd_cap in [("No cap", None), ("$2K daily loss cap", 2000), ("$1.5K daily loss cap", 1500), ("$1K daily loss cap", 1000)]:
    daily_pnl_s = defaultdict(float)
    for t in trades:
        day = t["day"]
        if dd_cap is not None and daily_pnl_s[day] <= -dd_cap:
            continue
        daily_pnl_s[day] += t["pnl"] * DPP

    cum_s = 0
    peak_s = 0
    max_dd_s = 0
    worst_s = 999999
    for d in sorted(daily_pnl_s.keys()):
        cum_s += daily_pnl_s[d]
        peak_s = max(peak_s, cum_s)
        max_dd_s = max(max_dd_s, peak_s - cum_s)
        worst_s = min(worst_s, daily_pnl_s[d])

    n = len(daily_pnl_s)
    monthly = cum_s / n * 20
    print(f'{dd_cap_label:>25} ${cum_s:>+9,.0f} ${cum_s/n:>+9,.0f} ${monthly:>+9,.0f} ${max_dd_s:>9,.0f} ${worst_s:>+9,.0f}')
