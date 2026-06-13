"""Backtest a REFINED EVAL strategy: limit eval_trader to top 3 safe buckets.

Whitelist:
  - DD Exhaustion long  WHERE paradigm = BOFA-PURE          (126t / +$2,423 / DD -$500)
  - AG Short short      ALL paradigms                        (84t / +$1,573 / DD -$300)
  - ES Absorption bullish ALL paradigms                      (334t / +$2,358 / DD -$594)

Plus combined variants:
  - All 3 setups + at 1 MES
  - All 3 setups + at 2 MES (for sizing analysis)

Reports per-day cumulative equity, drawdown curve, days to pass +$1,600 eval
target, E2T compliance breaches.
"""
import os, psycopg2
from datetime import date
from collections import defaultdict

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

# Pull data
cur.execute("""
    SELECT id, ts::date AS d, ts, setup_name, direction, grade, paradigm,
           greek_alignment, outcome_pnl
    FROM setup_log
    WHERE ts::date >= '2026-03-01'
      AND ts::date <= '2026-05-20'
      AND setup_name IN ('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion')
      AND notified = true
      AND outcome_pnl IS NOT NULL
    ORDER BY ts
""")
rows = cur.fetchall()


def is_refined_eval(name, dir_, para):
    """The proposed refined eval whitelist."""
    if name == "DD Exhaustion" and dir_ in ("long", "bullish") and para == "BOFA-PURE":
        return True
    if name == "AG Short" and dir_ == "short":
        return True
    if name == "ES Absorption" and dir_ == "bullish":
        return True
    return False


def is_baseline_eval(name, dir_, para):
    """Baseline = current V14 whitelist (all setups)."""
    return name in ("Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion")


def simulate(rows, filter_fn, label, mes_mult=1.0):
    """Run a strategy through history, return daily/cumulative stats."""
    daily = defaultdict(float)
    by_setup = defaultdict(lambda: {"n": 0, "wins": 0, "losses": 0, "pnl": 0.0})
    for sid, d, ts, name, dir_, grade, para, align, pnl in rows:
        if not filter_fn(name, dir_, para):
            continue
        pnl_d = float(pnl) * 5.0 * mes_mult
        daily[d] += pnl_d
        bs = by_setup[(name, dir_)]
        bs["n"] += 1
        bs["pnl"] += pnl_d
        if float(pnl) > 0: bs["wins"] += 1
        elif float(pnl) < 0: bs["losses"] += 1

    # Equity curve
    all_days = sorted(daily.keys())
    cum = 0.0
    peak = 0.0
    max_dd = 0.0
    max_dd_date = None
    days_to_target = None  # +$1,600
    daily_breaches = 0
    trailing_breaches = []
    monthly = defaultdict(float)
    daily_pnls = []

    for d in all_days:
        p = daily[d]
        daily_pnls.append((d, p))
        cum += p
        monthly[d.strftime("%Y-%m")] += p
        if cum > peak:
            peak = cum
        dd = cum - peak
        if dd < max_dd:
            max_dd = dd
            max_dd_date = d
        if days_to_target is None and cum >= 1600:
            days_to_target = (d - all_days[0]).days + 1
        if p < -550:
            daily_breaches += 1
        if dd < -1500:
            trailing_breaches.append(d)

    wins = sum(1 for _, p in daily_pnls if p > 0)
    losses = sum(1 for _, p in daily_pnls if p < 0)
    flats = sum(1 for _, p in daily_pnls if p == 0)

    print(f"\n{'=' * 75}")
    print(f"STRATEGY: {label}   (mes_mult={mes_mult})")
    print(f"{'=' * 75}")
    print(f"  Final equity:        ${cum:+.2f}")
    print(f"  Max drawdown:        ${max_dd:+.2f} on {max_dd_date}")
    print(f"  Trading days:        {len(all_days)} ({wins}W / {losses}L / {flats}flat)")
    if days_to_target:
        print(f"  Days to +$1,600:     {days_to_target} calendar days (~{days_to_target * 5 // 7} trading days)")
    else:
        print(f"  Days to +$1,600:     NEVER (peak: ${peak:.2f})")
    print(f"  E2T -$550 daily breaches: {daily_breaches}")
    print(f"  E2T -$1500 trail breaches: {len(trailing_breaches)}")
    if trailing_breaches:
        print(f"    First breach: {trailing_breaches[0]}, last: {trailing_breaches[-1]}")
    print(f"  Monthly P&L:")
    for m in sorted(monthly):
        print(f"    {m}: ${monthly[m]:+.2f}")
    print(f"  Per-setup contribution:")
    for (name, dir_), bs in sorted(by_setup.items(), key=lambda x: -x[1]["pnl"]):
        wr = bs["wins"] / max(1, bs["wins"] + bs["losses"]) * 100
        print(f"    {name:<18} {dir_:<8} n={bs['n']:>3} WR={wr:>5.1f}% PnL=${bs['pnl']:>+8.2f}")

    # Last 5 days summary (recency)
    print(f"  Last 5 trading days:")
    for d, p in daily_pnls[-5:]:
        print(f"    {d}: ${p:+.2f}")

    return {"final": cum, "max_dd": max_dd, "days_to_target": days_to_target,
            "daily_breaches": daily_breaches, "trail_breaches": len(trailing_breaches),
            "daily_pnls": daily_pnls}


# Run scenarios
baseline_1x = simulate(rows, is_baseline_eval, "BASELINE eval (current V14 whitelist, 1 MES)", 1.0)
refined_1x = simulate(rows, is_refined_eval, "REFINED eval (DD long BOFA-PURE + AG Short + ES Abs bullish, 1 MES)", 1.0)
refined_2x = simulate(rows, is_refined_eval, "REFINED eval at 2 MES", 2.0)


# Side-by-side comparison
print(f"\n{'=' * 75}")
print("COMPARISON TABLE")
print('=' * 75)
print(f"{'Strategy':<55} {'Final':>10} {'MaxDD':>10} {'Days→$1600':>12} {'TrailBreaches':>14}")
for label, s in [
    ("Baseline (full V14 whitelist) @ 1 MES", baseline_1x),
    ("Refined (top 3 safe buckets) @ 1 MES", refined_1x),
    ("Refined (top 3 safe buckets) @ 2 MES", refined_2x),
]:
    dt = str(s["days_to_target"]) if s["days_to_target"] else "NEVER"
    print(f"  {label:<53} ${s['final']:>+8.2f}  ${s['max_dd']:>+8.2f}  {dt:>10}  {s['trail_breaches']:>10}")

# Equity curve sparkline (text) for refined 1x
print(f"\n{'=' * 75}")
print("REFINED 1 MES — equity curve by week (cumulative $)")
print('=' * 75)
weekly = defaultdict(float)
all_days = sorted(set(d for d, _ in refined_1x["daily_pnls"]))
cum = 0.0
for d, p in refined_1x["daily_pnls"]:
    cum += p
    # ISO week
    iso = d.isocalendar()
    weekly[(iso[0], iso[1])] = cum
for (y, w), eq in sorted(weekly.items()):
    bar = "#" * int(max(0, eq / 100))
    print(f"  {y}-W{w:02d}: ${eq:+8.2f} {bar}")

cur.close(); c.close()
