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

# Collect all filtered trades
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

    # DD filters
    if setup == 'DD Exhaustion':
        if ts.hour >= 14:
            blocked += 1
            continue
        if 'BOFA' in paradigm.upper() and 'PURE' in paradigm.upper():
            blocked += 1
            continue

    # Split-target PnL
    t1_hit = max_p >= 10
    if setup in trailing and t1_hit:
        split_pnl = round((10.0 + pnl) / 2, 1)
    else:
        split_pnl = pnl

    trades.append({
        "id": r["id"], "ts": ts, "setup": setup, "dir": r["direction"],
        "result": result, "pnl": split_pnl, "max_p": max_p, "grade": r["grade"]
    })

# Daily aggregation
daily = defaultdict(lambda: {"trades": [], "pts": 0.0, "w": 0, "l": 0})
for t in trades:
    d = t["ts"].strftime("%Y-%m-%d")
    daily[d]["trades"].append(t)
    daily[d]["pts"] += t["pnl"]
    if t["pnl"] > 0:
        daily[d]["w"] += 1
    elif t["pnl"] < 0:
        daily[d]["l"] += 1

first_date = min(daily.keys())
last_date = max(daily.keys())

print("=" * 70)
print("FUNDED ACCOUNT SIMULATION (Eval Trader Rules, NO profit cap)")
print(f"Period: {first_date} to {last_date} ({len(daily)} trading days)")
print(f"Rules: 4 setups + DD filters, split-target PnL, no cap")
print(f"Blocked DD trades: {blocked}")
print("=" * 70)

# Simulate at different risk levels
for risk_label, mes_qty, dollar_per_pt in [
    ("$200 risk (3 MES)", 3, 15),
    ("$300 risk (5 MES)", 5, 25),
    ("$500 risk (10 MES)", 10, 50),
    ("4 ES ($200/pt)", 40, 200),
]:
    print(f"\n{'='*70}")
    print(f"  SCENARIO: {risk_label}  ({mes_qty} MES = ${dollar_per_pt}/pt)")
    print(f"{'='*70}")
    print(f"{'Day':>4} {'Date':>12} {'N':>3} {'W':>3}{'L':>3} {'PnL pts':>8} {'$':>9} {'Cum $':>9} {'DD $':>9}")
    print("-" * 65)

    cum = 0
    peak = 0
    max_dd = 0
    pos_days = 0
    neg_days = 0
    total_w = 0
    total_l = 0
    best_day = -999999
    worst_day = 999999

    for i, d in enumerate(sorted(daily.keys())):
        v = daily[d]
        pts = v["pts"]
        dollar = pts * dollar_per_pt
        cum += dollar
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)

        if pts > 0:
            pos_days += 1
        elif pts < 0:
            neg_days += 1
        total_w += v["w"]
        total_l += v["l"]
        best_day = max(best_day, dollar)
        worst_day = min(worst_day, dollar)

        dd_warn = " !" if dd > 1500 else ""
        print(f'{i+1:4d} {d:>12} {len(v["trades"]):3d} {v["w"]:3d}{v["l"]:3d} {pts:>+8.1f} ${dollar:>+8.0f} ${cum:>+8.0f} ${dd:>+8.0f}{dd_warn}')

    n = len(daily)
    total_pts = sum(daily[d]["pts"] for d in daily)
    print("-" * 65)
    print(f"{'':4} {'TOTAL':>12} {len(trades):3d} {total_w:3d}{total_l:3d} {total_pts:>+8.1f} ${total_pts*dollar_per_pt:>+8.0f}")
    print()
    print(f"  Days: {n} ({pos_days} up, {neg_days} down, {n-pos_days-neg_days} flat)")
    print(f"  Win rate (days): {pos_days/n*100:.0f}%")
    print(f"  Win rate (trades): {total_w/(total_w+total_l)*100:.0f}%")
    print(f"  Total PnL: ${total_pts*dollar_per_pt:+,.0f}")
    print(f"  Avg/day: ${total_pts*dollar_per_pt/n:+,.0f}")
    print(f"  Best day: ${best_day:+,.0f}")
    print(f"  Worst day: ${worst_day:+,.0f}")
    print(f"  Max drawdown: ${max_dd:,.0f}")
    print(f"  Monthly proj (20d): ${total_pts*dollar_per_pt/n*20:+,.0f}")

# Setup breakdown
print(f"\n{'='*70}")
print("SETUP BREAKDOWN (all scenarios same WR)")
print(f"{'='*70}")

setup_stats = defaultdict(lambda: {"w": 0, "l": 0, "e": 0, "pnl": 0.0})
for t in trades:
    s = t["setup"]
    if t["pnl"] > 0:
        setup_stats[s]["w"] += 1
    elif t["pnl"] < 0:
        setup_stats[s]["l"] += 1
    else:
        setup_stats[s]["e"] += 1
    setup_stats[s]["pnl"] += t["pnl"]

print(f"{'Setup':>20} {'N':>4} {'W':>4} {'L':>4} {'E':>3} {'WR':>5} {'PnL':>8} {'Avg':>6}")
print("-" * 58)
for s in ['AG Short', 'DD Exhaustion', 'ES Absorption', 'Paradigm Reversal']:
    d = setup_stats[s]
    n = d["w"] + d["l"] + d["e"]
    wr = d["w"] / max(d["w"] + d["l"], 1) * 100
    avg = d["pnl"] / max(n, 1)
    print(f'{s:>20} {n:4d} {d["w"]:4d} {d["l"]:4d} {d["e"]:3d} {wr:4.0f}% {d["pnl"]:+8.1f} {avg:+6.1f}')

total_n = sum(d["w"]+d["l"]+d["e"] for d in setup_stats.values())
total_w = sum(d["w"] for d in setup_stats.values())
total_l = sum(d["l"] for d in setup_stats.values())
total_pnl = sum(d["pnl"] for d in setup_stats.values())
print("-" * 58)
print(f'{"TOTAL":>20} {total_n:4d} {total_w:4d} {total_l:4d} {"":3s} {total_w/(total_w+total_l)*100:4.0f}% {total_pnl:+8.1f} {total_pnl/max(total_n,1):+6.1f}')
