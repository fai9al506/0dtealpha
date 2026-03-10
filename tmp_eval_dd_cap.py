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

trades = []
for r in all_rows:
    setup = r['setup_name']
    if setup not in eval_setups:
        continue
    ts = r['ts_et']
    pnl = float(r['outcome_pnl'] or 0)
    max_p = float(r['outcome_max_profit'] or 0)
    paradigm = get_par(ts)

    if setup == 'DD Exhaustion':
        if ts.hour >= 14:
            continue
        if 'BOFA' in paradigm.upper() and 'PURE' in paradigm.upper():
            continue

    t1_hit = max_p >= 10
    if setup in trailing and t1_hit:
        split_pnl = round((10.0 + pnl) / 2, 1)
    else:
        split_pnl = pnl

    trades.append({
        "ts": ts, "setup": setup, "pnl": split_pnl,
        "day": ts.strftime("%Y-%m-%d")
    })

# 3 MES ($200 risk, 12pt stop) = $15/pt
DPP = 15

print("=" * 75)
print("50K EVAL ACCOUNT (3 MES = $15/pt, $200 risk)")
print("=" * 75)

for cap_label, cap in [("No cap", None), ("$600 cap", 600), ("$500 cap", 500), ("$400 cap", 400), ("$300 cap", 300)]:
    daily_pnl = defaultdict(float)
    daily_taken = defaultdict(int)
    daily_skipped = defaultdict(int)

    for t in trades:
        day = t["day"]
        if cap is not None and daily_pnl[day] <= -cap:
            daily_skipped[day] += 1
            continue
        daily_pnl[day] += t["pnl"] * DPP
        daily_taken[day] += 1

    print(f"\n--- {cap_label} ---")
    print(f"{'Day':>4} {'Date':>12} {'Taken':>5} {'Skip':>5} {'Day $':>8} {'Cum $':>8} {'DD $':>8}")
    print("-" * 58)

    cum = 0
    peak = 0
    max_dd = 0
    worst = 999999
    best = -999999

    for i, d in enumerate(sorted(daily_pnl.keys())):
        dp = daily_pnl[d]
        cum += dp
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
        worst = min(worst, dp)
        best = max(best, dp)
        skip = daily_skipped[d]
        skip_mark = f" (skip {skip})" if skip > 0 else ""
        print(f'{i+1:4d} {d:>12} {daily_taken[d]:5d} {skip:5d} ${dp:>+7.0f} ${cum:>+7.0f} ${dd:>+7.0f}{skip_mark}')

    n = len(daily_pnl)
    taken = sum(daily_taken.values())
    skipped = sum(daily_skipped.values())
    print(f"  Total: ${cum:+,.0f} | Avg/day: ${cum/n:+,.0f} | Monthly: ${cum/n*20:+,.0f}")
    print(f"  MaxDD: ${max_dd:,.0f} | Worst day: ${worst:+,.0f} | Best day: ${best:+,.0f}")
    print(f"  Trades: {taken} taken, {skipped} skipped")

# Summary
print(f"\n{'='*75}")
print("COMPARISON (3 MES, 50K account)")
print(f"{'='*75}")
print(f"{'Cap':>12} {'Total':>9} {'Avg/Day':>9} {'Monthly':>9} {'MaxDD':>8} {'Worst':>8} {'Skip':>5}")
print("-" * 62)

for cap_label, cap in [("No cap", None), ("$600", 600), ("$500", 500), ("$400", 400), ("$300", 300)]:
    daily_pnl_s = defaultdict(float)
    skipped_s = 0
    for t in trades:
        day = t["day"]
        if cap is not None and daily_pnl_s[day] <= -cap:
            skipped_s += 1
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
    print(f'{cap_label:>12} ${cum_s:>+8,.0f} ${cum_s/n:>+8,.0f} ${cum_s/n*20:>+8,.0f} ${max_dd_s:>7,.0f} ${worst_s:>+7,.0f} {skipped_s:5d}')

print(f"\nE2T rules: $1,100 daily loss limit, $2,000 EOD trailing DD")
print(f"At 3 MES: max loss per trade = $180 (12pt × $15)")
print(f"3 losses/day max = $540 worst case")
print(f"Current max_losses_per_day=3 already acts as ~$540 daily cap")
