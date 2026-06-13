"""Dig into Option 2 (dual-account drawdown) + Option 6 (best single setup).

Data window: Mar 1 - May 20 2026 (~3 months, ~81 trading days)
Filter: V14-whitelist setups + notified=true (live-filter passed)
Outcome source: setup_log.outcome_pnl (in points), × $5/MES, × 1 MES base

OPTION 2 — Dual eval-account drawdown:
  - Split signals by direction (long → LONG acct, short → SHORT acct)
  - Compute daily P&L per direction
  - Find worst single-day per direction
  - Find longest losing streak per direction
  - Find max consecutive drawdown per direction
  - Compare to E2T 25K TCP rules ($550 daily loss cap, $1,500 trailing DD)

OPTION 6 — Best single setup for eval pass / 2x sizing:
  - Group by (setup_name, direction) and also (setup_name, direction, paradigm)
  - For each bucket: trades, WR, total $, MaxDD, monthly breakdown
  - Find best (high WR + consistency + frequency)
"""
import os, psycopg2
from datetime import date
from collections import defaultdict

E2T_DAILY_LOSS_CAP = -550.0
E2T_TRAILING_DD = -1500.0  # from peak
MES_DOLLARS = 5.0
QTY = 1

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

# Pull all V14-whitelist signals that were notified (passed live filter) Mar 1 - May 20
cur.execute("""
    SELECT id, ts::date AS d, ts, setup_name, direction, grade, paradigm,
           greek_alignment, outcome_pnl, outcome_result
    FROM setup_log
    WHERE ts::date >= '2026-03-01'
      AND ts::date <= '2026-05-20'
      AND setup_name IN ('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion')
      AND notified = true
      AND outcome_pnl IS NOT NULL
    ORDER BY ts
""")
rows = cur.fetchall()
print(f"Loaded {len(rows)} V14-whitelist trades Mar 1 - May 20 ({len(set(r[1] for r in rows))} trading days)\n")

# ============================================================
# OPTION 2: Dual-account split analysis
# ============================================================
print("=" * 75)
print("OPTION 2: Dual eval-account split — drawdown analysis")
print("=" * 75)

# Build per-direction daily P&L (in dollars at 1 MES)
daily_long = defaultdict(float)
daily_short = defaultdict(float)
trades_long = defaultdict(int)
trades_short = defaultdict(int)

for sid, d, ts, name, dir_, grade, para, align, pnl, res in rows:
    is_long = dir_ in ("long", "bullish")
    pnl_d = float(pnl) * MES_DOLLARS * QTY
    if is_long:
        daily_long[d] += pnl_d
        trades_long[d] += 1
    else:
        daily_short[d] += pnl_d
        trades_short[d] += 1

all_days = sorted(set(list(daily_long.keys()) + list(daily_short.keys())))

# Per-direction worst days, longest losing streak, max drawdown from peak
def analyze_account(daily, trades, label):
    """Compute account stats with E2T-style trailing DD."""
    cumulative = 0.0
    peak = 0.0
    worst_dd = 0.0
    worst_dd_day = None
    worst_single_day = 0.0
    worst_single_day_date = None
    days_blown_daily_cap = 0
    days_blown_trailing_dd = []
    consec_losing = 0
    max_consec_losing = 0
    consec_losing_d = 0.0
    max_consec_d = 0.0
    monthly = defaultdict(float)
    n_winning_days = 0
    n_losing_days = 0
    n_flat_days = 0

    for d in all_days:
        pnl = daily[d]
        if abs(pnl) < 0.01 and trades[d] == 0:
            continue  # no trade that day
        cumulative += pnl
        if pnl > 0:
            n_winning_days += 1
            consec_losing = 0
            consec_losing_d = 0.0
        elif pnl < 0:
            n_losing_days += 1
            consec_losing += 1
            consec_losing_d += pnl
            if consec_losing > max_consec_losing:
                max_consec_losing = consec_losing
            if consec_losing_d < max_consec_d:
                max_consec_d = consec_losing_d
        else:
            n_flat_days += 1
        if cumulative > peak:
            peak = cumulative
        drawdown = cumulative - peak
        if drawdown < worst_dd:
            worst_dd = drawdown
            worst_dd_day = d
        if pnl < worst_single_day:
            worst_single_day = pnl
            worst_single_day_date = d
        if pnl < E2T_DAILY_LOSS_CAP:
            days_blown_daily_cap += 1
        if drawdown < E2T_TRAILING_DD:
            days_blown_trailing_dd.append(d)
        monthly[d.strftime("%Y-%m")] += pnl

    print(f"\n[{label}]")
    print(f"  Total: ${cumulative:+.2f}  Days: {n_winning_days}W / {n_losing_days}L / {n_flat_days}flat")
    print(f"  Worst single day:   ${worst_single_day:+.2f} on {worst_single_day_date}")
    print(f"  Worst drawdown:     ${worst_dd:+.2f} on {worst_dd_day}")
    print(f"  Max consec losing:  {max_consec_losing} days = ${max_consec_d:+.2f}")
    print(f"  Days violating E2T -$550 daily cap: {days_blown_daily_cap}")
    print(f"  Days violating E2T -$1500 trailing DD: {len(days_blown_trailing_dd)}")
    if days_blown_trailing_dd:
        print(f"    Dates: {days_blown_trailing_dd[:5]}...")
    print(f"  Monthly P&L:")
    for m in sorted(monthly):
        print(f"    {m}: ${monthly[m]:+.2f}")
    return {"total": cumulative, "worst_dd": worst_dd, "worst_day": worst_single_day,
            "blown_daily": days_blown_daily_cap, "blown_trailing": len(days_blown_trailing_dd)}

long_stats = analyze_account(daily_long, trades_long, "LONG account")
short_stats = analyze_account(daily_short, trades_short, "SHORT account")

print("\n=== OPTION 2 VERDICT ===")
print(f"  LONG  account: total ${long_stats['total']:+.2f}, worst day ${long_stats['worst_day']:+.2f}, max DD ${long_stats['worst_dd']:+.2f}")
print(f"  SHORT account: total ${short_stats['total']:+.2f}, worst day ${short_stats['worst_day']:+.2f}, max DD ${short_stats['worst_dd']:+.2f}")
print(f"  Total daily-cap breaches: long={long_stats['blown_daily']}, short={short_stats['blown_daily']}")
print(f"  Total trailing-DD breaches: long={long_stats['blown_trailing']}, short={short_stats['blown_trailing']}")
if long_stats['blown_trailing'] > 0 or short_stats['blown_trailing'] > 0:
    print(f"  !! AT LEAST ONE ACCOUNT WOULD HAVE BLOWN E2T DRAWDOWN HISTORICALLY")
else:
    print(f"  OK NEITHER ACCOUNT WOULD HAVE BLOWN E2T DRAWDOWN ON 3-MO SAMPLE")

# ============================================================
# OPTION 6: Best single setup for eval-pass / 2x sizing
# ============================================================
print()
print("=" * 75)
print("OPTION 6: Find single-setup-with-criteria worth 2x OR sole eval setup")
print("=" * 75)

# Bucket by (setup_name, direction). Also drill paradigm where bucket has 30+ trades.
buckets = defaultdict(list)
for sid, d, ts, name, dir_, grade, para, align, pnl, res in rows:
    key = (name, dir_)
    buckets[key].append((d, float(pnl), grade, para, align))

def analyze_bucket(label, trades):
    if len(trades) < 5:
        return None
    pnls = [t[1] for t in trades]
    total = sum(pnls)
    wins = sum(1 for p in pnls if p > 0)
    losses = sum(1 for p in pnls if p < 0)
    flats = sum(1 for p in pnls if p == 0)
    n = len(trades)
    wr = wins / max(1, wins + losses) * 100

    # MaxDD
    cum = 0.0
    peak = 0.0
    maxdd = 0.0
    for p in pnls:
        cum += p
        if cum > peak: peak = cum
        if cum - peak < maxdd: maxdd = cum - peak

    # Monthly
    by_month = defaultdict(float)
    for d, p, _, _, _ in trades:
        by_month[d.strftime("%Y-%m")] += p

    return {
        "label": label, "n": n, "wr": wr, "total_pts": total, "total_$": total*5,
        "maxdd_pts": maxdd, "maxdd_$": maxdd*5,
        "wins": wins, "losses": losses, "flats": flats,
        "monthly": dict(by_month),
    }

print(f"\n--- BY SETUP × DIRECTION ---")
candidates = []
for (name, dir_), trades in sorted(buckets.items(), key=lambda x: -len(x[1])):
    res = analyze_bucket(f"{name} {dir_}", trades)
    if not res: continue
    months_positive = sum(1 for v in res["monthly"].values() if v > 0)
    months_total = len(res["monthly"])
    print(f"\n  {res['label']:<32} n={res['n']:>3} WR={res['wr']:>5.1f}%  PnL=${res['total_$']:>8.2f}  "
          f"MaxDD=${res['maxdd_$']:>+8.2f}  mo+={months_positive}/{months_total}")
    print(f"    Monthly: " + "  ".join(f"{m}={res['monthly'][m]*5:+.0f}" for m in sorted(res['monthly'])))
    # Candidate if WR >= 70%, all months positive, MaxDD modest
    if res["wr"] >= 70 and months_positive == months_total and res["n"] >= 30 and abs(res["maxdd_$"]) < abs(res["total_$"]) * 0.4:
        candidates.append(res)

print()
print("--- TOP CANDIDATES (WR>=70%, all months green, MaxDD < 40% of PnL, n>=30) ---")
if not candidates:
    print("  No buckets pass all 4 criteria — try paradigm drill-down")
else:
    for c_ in sorted(candidates, key=lambda x: -x["total_$"]):
        print(f"  OK {c_['label']:<32} n={c_['n']} WR={c_['wr']:.1f}% PnL=${c_['total_$']:.2f} MaxDD=${c_['maxdd_$']:.2f}")

# Drill paradigm for top setups
print()
print("--- PARADIGM DRILL on top setups (looking for 2x-sizing buckets) ---")
top_setups = [k for k, v in buckets.items() if len(v) >= 50]
for setup_dir in top_setups[:4]:
    name, dir_ = setup_dir
    para_buckets = defaultdict(list)
    for t in buckets[setup_dir]:
        d, pnl, grade, para, align = t
        para_buckets[para].append(t)
    print(f"\n  [{name} {dir_}]")
    for para, trades in sorted(para_buckets.items(), key=lambda x: -len(x[1])):
        if len(trades) < 15: continue
        res = analyze_bucket(f"{name} {dir_} {para}", trades)
        if not res: continue
        months_positive = sum(1 for v in res["monthly"].values() if v > 0)
        months_total = len(res["monthly"])
        flag = "  !! 2X CANDIDATE" if res["wr"] >= 75 and months_positive == months_total and res["n"] >= 30 else ""
        print(f"    {para:<18} n={res['n']:>3} WR={res['wr']:>5.1f}%  PnL=${res['total_$']:>+8.2f}  "
              f"MaxDD=${res['maxdd_$']:>+7.2f}  mo+={months_positive}/{months_total}{flag}")

# === EVAL PASS PROJECTION ===
print()
print("=" * 75)
print("EVAL PASS PROJECTION — what's the fastest single-setup-only path?")
print("=" * 75)
print(f"E2T 25K TCP: need +$1,600 from current ~$25,150 balance")
print(f"Daily cap: -$550, Trailing DD: -$1,500")
print()
TRADING_DAYS = 81  # Mar 1 - May 20
for c_ in candidates:
    monthly_avg = c_["total_$"] / 3  # approx 3 months
    daily_avg = c_["total_$"] / TRADING_DAYS
    days_to_pass = 1600 / max(1, daily_avg)
    print(f"  {c_['label']:<32} avg=${daily_avg:+.2f}/day → pass eval in ~{days_to_pass:.0f} trading days")

cur.close(); c.close()
