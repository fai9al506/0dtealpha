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

# Per-trade stats
wins = 0
losses = 0
expired = 0
total_pnl = 0
blocked_trades = 0

# Per-setup stats
setup_stats = defaultdict(lambda: {"w": 0, "l": 0, "e": 0, "pnl": 0.0})

# Daily stats
daily = defaultdict(lambda: {"trades": 0, "pts": 0.0, "wins": 0, "losses": 0})

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
            blocked_trades += 1
            continue
        if 'BOFA' in paradigm.upper() and 'PURE' in paradigm.upper():
            blocked_trades += 1
            continue

    # Split-target PnL
    t1_hit = max_p >= 10
    if setup in trailing and t1_hit:
        split_pnl = round((10.0 + pnl) / 2, 1)
    else:
        split_pnl = pnl

    day = ts.strftime('%Y-%m-%d')
    daily[day]["trades"] += 1
    daily[day]["pts"] += split_pnl

    if split_pnl > 0:
        wins += 1
        daily[day]["wins"] += 1
        setup_stats[setup]["w"] += 1
    elif split_pnl < 0:
        losses += 1
        daily[day]["losses"] += 1
        setup_stats[setup]["l"] += 1
    else:
        expired += 1
        setup_stats[setup]["e"] += 1

    total_pnl += split_pnl
    setup_stats[setup]["pnl"] += split_pnl

total_trades = wins + losses + expired
wr = wins / max(wins + losses, 1) * 100

print("=" * 60)
print("EVAL TRADER RULES — FULL HISTORICAL STATS")
print("(4 setups + DD filters, split-target PnL)")
print("=" * 60)

print(f"\nTotal trades: {total_trades} (blocked {blocked_trades} DD trades)")
print(f"Wins: {wins}  Losses: {losses}  Expired: {expired}")
print(f"Win Rate: {wr:.0f}%")
print(f"Total PnL: {total_pnl:+.1f} pts")
print(f"Avg per trade: {total_pnl/max(total_trades,1):+.1f} pts")
pf_w = sum(sp for sp in [setup_stats[s]["pnl"] for s in setup_stats] if sp > 0) or sum(split_pnl for split_pnl in [] )

# Profit factor
gross_wins = 0
gross_losses = 0
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
    if split_pnl > 0:
        gross_wins += split_pnl
    elif split_pnl < 0:
        gross_losses += abs(split_pnl)

pf = gross_wins / max(gross_losses, 0.01)
print(f"Profit Factor: {pf:.2f}")
print(f"Gross Wins: {gross_wins:+.1f} pts  Gross Losses: {gross_losses:+.1f} pts")

print(f"\n{'Setup':>20} {'W':>4} {'L':>4} {'E':>3} {'WR':>5} {'PnL':>8} {'Avg':>6}")
print("-" * 55)
for s in ['AG Short', 'DD Exhaustion', 'ES Absorption', 'Paradigm Reversal']:
    d = setup_stats[s]
    n = d["w"] + d["l"] + d["e"]
    wr_s = d["w"] / max(d["w"] + d["l"], 1) * 100
    avg = d["pnl"] / max(n, 1)
    print(f'{s:>20} {d["w"]:4d} {d["l"]:4d} {d["e"]:3d} {wr_s:4.0f}% {d["pnl"]:+8.1f} {avg:+6.1f}')

# Daily breakdown
print(f"\n{'='*60}")
print("DAILY P&L")
print(f"{'='*60}")
print(f"{'Date':>12} {'N':>4} {'W':>3} {'L':>3} {'PnL':>8} {'$(10MES)':>10}")
print("-" * 45)

pos_days = 0
neg_days = 0
flat_days = 0
max_dd_dollar = 0
peak_dollar = 0
cum_dollar = 0
streak_pos = 0
streak_neg = 0
max_streak_pos = 0
max_streak_neg = 0

for d in sorted(daily.keys()):
    v = daily[d]
    dollar = v["pts"] * 50
    cum_dollar += dollar
    peak_dollar = max(peak_dollar, cum_dollar)
    dd = peak_dollar - cum_dollar
    max_dd_dollar = max(max_dd_dollar, dd)

    if v["pts"] > 0:
        pos_days += 1
        streak_pos += 1
        streak_neg = 0
        max_streak_pos = max(max_streak_pos, streak_pos)
    elif v["pts"] < 0:
        neg_days += 1
        streak_neg += 1
        streak_pos = 0
        max_streak_neg = max(max_streak_neg, streak_neg)
    else:
        flat_days += 1

    print(f'{d:>12} {v["trades"]:4d} {v["wins"]:3d} {v["losses"]:3d} {v["pts"]:>+8.1f} ${dollar:>+9.0f}')

n_days = len(daily)
print("-" * 45)
print(f'{"TOTAL":>12} {total_trades:4d} {wins:3d} {losses:3d} {total_pnl:>+8.1f} ${total_pnl*50:>+9.0f}')

print(f"\n{'='*60}")
print("E2T PASS SIMULATION")
print(f"{'='*60}")
print(f"Trading days: {n_days}")
print(f"Positive days: {pos_days} ({pos_days/n_days*100:.0f}%)")
print(f"Negative days: {neg_days} ({neg_days/n_days*100:.0f}%)")
print(f"Flat days: {flat_days}")
print(f"Avg pts/day: {total_pnl/n_days:+.1f}")
print(f"Avg $/day (10 MES): ${total_pnl/n_days*50:+.0f}")
print(f"Max drawdown: ${max_dd_dollar:,.0f}")
print(f"Max winning streak: {max_streak_pos} days")
print(f"Max losing streak: {max_streak_neg} days")

# E2T pass simulation
print(f"\n--- E2T 50K TCP Pass Simulation ---")
print(f"Target: $2,600 profit")
print(f"Max EOD trailing drawdown: $2,000")
print(f"Daily loss limit: $1,100")

# Simulate day by day with $900 cap and $1100 loss limit
cum = 0
peak = 0
passed = False
blown = False

print(f"\n{'Day':>4} {'Date':>12} {'Raw$':>8} {'Capped$':>8} {'Cum$':>8} {'DD$':>8} {'Status':>10}")
print("-" * 65)

for i, d in enumerate(sorted(daily.keys())):
    v = daily[d]
    raw_dollar = v["pts"] * 50

    # Apply caps
    if raw_dollar > 900:
        capped = 900
    elif raw_dollar < -1000:  # $1100 limit with $100 buffer
        capped = -1000
    else:
        capped = raw_dollar

    cum += capped
    peak = max(peak, cum)
    dd = peak - cum

    status = ""
    if cum >= 2600 and not passed:
        passed = True
        status = "PASSED!"
    if dd >= 2000:
        blown = True
        status = "BLOWN!"

    print(f'{i+1:4d} {d:>12} ${raw_dollar:>+7.0f} ${capped:>+7.0f} ${cum:>+7.0f} ${dd:>+7.0f} {status:>10}')

print(f"\nFinal balance delta: ${cum:+,.0f}")
print(f"Max drawdown hit: ${max_dd_dollar:,.0f}")
if passed:
    print("RESULT: WOULD HAVE PASSED")
elif blown:
    print("RESULT: WOULD HAVE BLOWN (drawdown)")
else:
    print("RESULT: STILL IN PROGRESS")

# How many 5-day windows would pass?
print(f"\n--- 5-Day Rolling Window Analysis ---")
sorted_days = sorted(daily.keys())
pass_count = 0
blow_count = 0
neutral_count = 0

for start in range(len(sorted_days) - 4):
    window = sorted_days[start:start+5]
    cum_w = 0
    peak_w = 0
    blown_w = False
    passed_w = False

    for d in window:
        v = daily[d]
        raw = v["pts"] * 50
        capped = min(max(raw, -1000), 900)
        cum_w += capped
        peak_w = max(peak_w, cum_w)
        if peak_w - cum_w >= 2000:
            blown_w = True
        if cum_w >= 2600:
            passed_w = True

    status_w = "PASS" if passed_w and not blown_w else ("BLOWN" if blown_w else "NEUTRAL")
    days_str = f"{window[0]} to {window[-1]}"
    print(f"  {days_str}: ${cum_w:>+7.0f}  {status_w}")

    if passed_w and not blown_w:
        pass_count += 1
    elif blown_w:
        blow_count += 1
    else:
        neutral_count += 1

total_windows = pass_count + blow_count + neutral_count
print(f"\n5-day windows: {total_windows}")
print(f"  PASS: {pass_count} ({pass_count/max(total_windows,1)*100:.0f}%)")
print(f"  BLOWN: {blow_count} ({blow_count/max(total_windows,1)*100:.0f}%)")
print(f"  NEUTRAL: {neutral_count} ({neutral_count/max(total_windows,1)*100:.0f}%)")
