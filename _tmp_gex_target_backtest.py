"""S180 — GEX-TARGET paradigm long-side backtest.

User claim: paradigm GEX-TARGET means "GEX magnet target reached, price
stabilizing/reversing." Therefore long entries in GEX-TARGET should fail.

Mechanism per Volland framework: GEX paradigm has 3 subtypes —
  GEX-LIS:    spot near LIS, dealers absorbing (bullish coiling)
  GEX-PURE:   GEX field clean, dealer-supported, prime long regime
  GEX-TARGET: spot is AT a +GEX magnet (price has REACHED its destination)
              → upward pressure fades, mean-reversion expected
  GEX-MESSY:  GEX field fragmented, no clean magnet (avoid)

Validates against today's incident: 3 longs in GEX-TARGET (lids 3184/3185/3189)
all lost -$152.50 real broker.

This script:
  1. Quantifies long-side performance in GEX-TARGET vs other paradigms
  2. Filters to V16-eligible whitelist (SC + DD long V16.1 align>=0 + ES Abs)
  3. Multi-regime split (Feb/Mar/Apr/May 2026)
  4. Bootstrap 95% CI on the per-trade mean
  5. Block-rule simulation: if we add "block longs at GEX-TARGET", what changes?
"""
import psycopg2
import statistics
from collections import defaultdict

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB); cur = conn.cursor()

# Pull all long trades for the 3 setups, all paradigms, since Feb 1 2026.
# Apply V16-like filtering (close to current live filter) for fair comparison.
cur.execute("""
    SELECT id, setup_name, direction, grade, paradigm,
           greek_alignment, vix, vix3m,
           (ts AT TIME ZONE 'America/New_York') as et_ts,
           outcome_result, outcome_pnl,
           vanna_cliff_side, vanna_peak_side
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND direction IN ('long', 'bullish')
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'ES Absorption')
      AND outcome_pnl IS NOT NULL
      AND grade IS NOT NULL
    ORDER BY ts
""")
all_rows = cur.fetchall()
print(f"Pulled {len(all_rows)} long trades (SC/DD/ES Abs) since Feb 1 2026\n")

def v16_eligible(setup, grade, paradigm, align, vix, vanna_cliff, vanna_peak, et_ts):
    """Approximation of current V16 filter for longs.
    Returns True if the trade would have been TSRT-eligible."""
    # Universal: block SIDIAL-EXTREME longs
    if paradigm == "SIDIAL-EXTREME":
        return False
    # Setup-specific:
    if setup == "Skew Charm":
        if grade in ("C", "LOG"):
            return False
        # V14: align=3 + bad paradigm
        if align == 3 and paradigm in ("GEX-LIS", "AG-LIS", "AG-PURE", "BOFA-MESSY"):
            return False
        # V16 R5: GEX-LIS all-align
        if paradigm == "GEX-LIS":
            return False
        # V16 R2: OpEx Friday
        if et_ts and et_ts.weekday() == 4 and 15 <= et_ts.day <= 21:
            return False
        return True
    elif setup == "DD Exhaustion":
        # V16.1: align>=0 for longs (V14 still blocks align==3 + bad paradigm)
        if align is None or align < 0:
            return False
        if align == 3:  # V14 contrarian-fails-when-fully-aligned
            return False
        if vix is not None and vix >= 22:
            return False
        if paradigm in ("GEX-LIS", "AG-LIS", "AG-PURE", "BofA-LIS", "BOFA-MESSY"):
            return False
        if grade == "C":
            return False
        return True
    elif setup == "ES Absorption":
        if grade not in ("A", "A+"):
            return False
        if paradigm in ("AG-TARGET", "AG-LIS"):
            return False
        if align is None or align < 0:  # direction-matched alignment for bullish
            return False
        return True
    return False


# Bucket by V16-eligibility AND paradigm
buckets = defaultdict(list)  # (paradigm, eligible) -> list of (lid, setup, pnl)
ineligible_total = 0
eligible_total = 0
for r in all_rows:
    lid, setup, direction, grade, paradigm, align, vix, vix3m, et_ts, out, pnl, vc, vp = r
    eligible = v16_eligible(setup, grade, paradigm, align, vix, vc, vp, et_ts)
    if not eligible:
        ineligible_total += 1
        continue
    eligible_total += 1
    if not paradigm:
        paradigm = "NULL"
    buckets[paradigm].append((lid, setup, float(pnl), et_ts))

print(f"V16-eligible: {eligible_total}, filtered out: {ineligible_total}\n")

# Per-paradigm aggregate (long-side, V16-eligible)
def fmt(trades):
    n = len(trades)
    if n == 0: return "n=0"
    pnls = [t[2] for t in trades]
    wr = sum(1 for p in pnls if p > 0) / n * 100
    total = sum(pnls)
    mean = total / n
    return f"n={n:3d} WR={wr:5.1f}% total={total:+7.1f}pt ${total*5:+8.0f} mean={mean:+5.2f}pt"

# Sort paradigms by total PnL ascending (worst first)
sorted_paras = sorted(buckets.items(), key=lambda x: sum(t[2] for t in x[1]))
print("V16-ELIGIBLE LONGS by paradigm (sorted worst -> best):")
print("-" * 95)
for para, trades in sorted_paras:
    by_setup = defaultdict(list)
    for t in trades:
        by_setup[t[1]].append(t)
    print(f"\n  {para:18s}  TOTAL: {fmt(trades)}")
    for setup, st in by_setup.items():
        print(f"    {setup:20s}  {fmt(st)}")

# Bootstrap 95% CI on GEX-TARGET specifically
import random
random.seed(42)
def bootstrap_ci(samples, n_iter=2000):
    if not samples: return (None, None, None)
    means = []
    for _ in range(n_iter):
        sample = [random.choice(samples) for _ in samples]
        means.append(sum(sample) / len(sample))
    means.sort()
    return (means[int(n_iter*0.025)], statistics.mean(means), means[int(n_iter*0.975)])

print("\n\n" + "=" * 95)
print("FOCUS: GEX-TARGET LONGS (V16-eligible)")
print("=" * 95)
gt = buckets.get("GEX-TARGET", [])
if not gt:
    print("No V16-eligible GEX-TARGET longs found.")
else:
    pnls = [t[2] for t in gt]
    print(f"Total: {fmt(gt)}")
    print(f"Mean: {sum(pnls)/len(pnls):+.2f} pt, StdDev: {statistics.stdev(pnls) if len(pnls)>1 else 0:.2f}")
    lo, mid, hi = bootstrap_ci(pnls)
    if lo is not None:
        print(f"Bootstrap 95% CI on per-trade mean: [{lo:+.2f}, {hi:+.2f}]")
        sig = "STRICTLY NEGATIVE (block-justified)" if hi < 0 else ("STRICTLY POSITIVE" if lo > 0 else "CROSSES ZERO (regime noise)")
        print(f"CI interpretation: {sig}")
    # Monthly split
    by_month = defaultdict(list)
    for t in gt:
        by_month[t[3].strftime("%Y-%m")].append(t)
    print("\nMonthly:")
    for m in sorted(by_month):
        print(f"  {m}: {fmt(by_month[m])}")
    # By setup
    by_setup = defaultdict(list)
    for t in gt:
        by_setup[t[1]].append(t)
    print("\nBy setup:")
    for setup, st in sorted(by_setup.items()):
        print(f"  {setup:20s}  {fmt(st)}")

# Block-rule simulation: what happens to overall V16 long PnL if we BLOCK GEX-TARGET?
print("\n\n" + "=" * 95)
print("BLOCK-RULE SIMULATION: Add 'block longs in GEX-TARGET' to V16")
print("=" * 95)
all_eligible = [t for trades in buckets.values() for t in trades]
v16_baseline_total = sum(t[2] for t in all_eligible)
v16_baseline_wr = sum(1 for t in all_eligible if t[2] > 0) / len(all_eligible) * 100
v16_baseline_n = len(all_eligible)

# Without GEX-TARGET longs
post_block = [t for t in all_eligible if any(t in trades for trades in buckets.values()) and t not in gt]
# Actually simpler: drop the gt list from all_eligible
gt_set = set(t[0] for t in gt)
post = [t for t in all_eligible if t[0] not in gt_set]
post_total = sum(t[2] for t in post)
post_wr = sum(1 for t in post if t[2] > 0) / len(post) * 100 if post else 0

print(f"V16 baseline (current):       n={v16_baseline_n:4d}  WR={v16_baseline_wr:5.1f}%  total={v16_baseline_total:+7.1f}pt  ${v16_baseline_total*5:+8.0f}")
print(f"V16 + block GEX-TARGET longs: n={len(post):4d}  WR={post_wr:5.1f}%  total={post_total:+7.1f}pt  ${post_total*5:+8.0f}")
delta = post_total - v16_baseline_total
print(f"\n>>> DELTA: {delta:+.1f}pt  ${delta*5:+.0f} at 1 MES across {len(gt)} blocked trades")
print(f">>> WR improvement: {post_wr - v16_baseline_wr:+.2f} percentage points")

# Monthly stability of the block (does it help every month?)
print("\nPer-month delta of the proposed block (only including months where GEX-TARGET fired):")
by_month_eligible = defaultdict(list)
by_month_gt = defaultdict(list)
for t in all_eligible:
    by_month_eligible[t[3].strftime("%Y-%m")].append(t)
for t in gt:
    by_month_gt[t[3].strftime("%Y-%m")].append(t)
for m in sorted(by_month_gt):
    base = sum(t[2] for t in by_month_eligible[m])
    blocked = sum(t[2] for t in by_month_gt[m])
    after = base - blocked
    n_blocked = len(by_month_gt[m])
    print(f"  {m}: base={base:+7.1f}pt  blocked-trades-pnl={blocked:+7.1f}pt ({n_blocked}t)  "
          f"after-block={after:+7.1f}pt  delta={-blocked:+.1f}pt")

conn.close()
