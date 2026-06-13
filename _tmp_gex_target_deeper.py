"""Deeper GEX-TARGET investigation:
  1. SHORTS in GEX-TARGET (mechanism predicts: should be GOOD)
  2. Full universe (portal sim, no V16 filter) — wider sample
  3. Real broker vs portal sim gap for GEX-TARGET specifically
  4. Per-setup breakdown with statistical confidence
  5. Time-of-day pattern in GEX-TARGET
  6. Total sample bias: how many trades total in GEX-TARGET universe?
"""
import psycopg2, statistics, random
from collections import defaultdict

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
conn = psycopg2.connect(DB); cur = conn.cursor()

random.seed(42)
def bs_ci(samples, n_iter=2000):
    if len(samples) < 2: return (None, None, None)
    means = []
    for _ in range(n_iter):
        s = [random.choice(samples) for _ in samples]
        means.append(sum(s)/len(s))
    means.sort()
    return (means[int(n_iter*0.025)], statistics.mean(means), means[int(n_iter*0.975)])

def fmt(trades):
    n = len(trades)
    if n == 0: return "n=0"
    pnls = [t[1] for t in trades]
    wr = sum(1 for p in pnls if p > 0)/n*100
    total = sum(pnls)
    return f"n={n:3d} WR={wr:5.1f}% total={total:+7.1f}pt ${total*5:+8.0f} mean={total/n:+5.2f}pt"

# -----------------------------------------------------------------------------
# 1. FULL UNIVERSE — all GEX-TARGET trades, both directions, all setups, no V16 filter
print("="*100)
print("1. FULL GEX-TARGET UNIVERSE (no V16 filter applied, portal P&L)")
print("="*100)
cur.execute("""
    SELECT id, setup_name, direction, grade, paradigm,
           outcome_pnl, (ts AT TIME ZONE 'America/New_York')
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND paradigm = 'GEX-TARGET'
      AND outcome_pnl IS NOT NULL
""")
rows = cur.fetchall()
print(f"Total GEX-TARGET trades: {len(rows)}\n")

longs = [(r[0], float(r[5]), r[1], r[3], r[6]) for r in rows if r[2] in ('long','bullish')]
shorts = [(r[0], float(r[5]), r[1], r[3], r[6]) for r in rows if r[2] in ('short','bearish')]

print(f"LONGS:  {fmt(longs)}")
if longs:
    pnls = [t[1] for t in longs]
    lo, m, hi = bs_ci(pnls)
    if lo is not None: print(f"  Bootstrap 95% CI mean: [{lo:+.2f}, {hi:+.2f}]")

print(f"SHORTS: {fmt(shorts)}")
if shorts:
    pnls = [t[1] for t in shorts]
    lo, m, hi = bs_ci(pnls)
    if lo is not None: print(f"  Bootstrap 95% CI mean: [{lo:+.2f}, {hi:+.2f}]")

print("\nLONGS by setup:")
by_setup = defaultdict(list)
for t in longs:
    by_setup[t[2]].append((t[0], t[1]))
for setup, st in sorted(by_setup.items(), key=lambda x: sum(t[1] for t in x[1])):
    print(f"  {setup:25s} {fmt(st)}")
    if len(st) >= 3:
        pnls = [t[1] for t in st]
        lo, m, hi = bs_ci(pnls)
        if lo is not None:
            sig = ("NEG-SIG" if hi < 0 else "POS-SIG" if lo > 0 else "noise")
            print(f"    CI: [{lo:+.2f}, {hi:+.2f}]  ({sig})")

print("\nSHORTS by setup:")
by_setup = defaultdict(list)
for t in shorts:
    by_setup[t[2]].append((t[0], t[1]))
for setup, st in sorted(by_setup.items(), key=lambda x: -sum(t[1] for t in x[1])):
    print(f"  {setup:25s} {fmt(st)}")
    if len(st) >= 3:
        pnls = [t[1] for t in st]
        lo, m, hi = bs_ci(pnls)
        if lo is not None:
            sig = ("NEG-SIG" if hi < 0 else "POS-SIG" if lo > 0 else "noise")
            print(f"    CI: [{lo:+.2f}, {hi:+.2f}]  ({sig})")

# -----------------------------------------------------------------------------
# 2. WIDER sample — GEX-TARGET vs all other paradigms, all 3 long setups
print("\n\n" + "="*100)
print("2. GEX-TARGET vs OTHER paradigms (LONGS only, SC/DD/ES Abs, ALL grades, no V16)")
print("="*100)
cur.execute("""
    SELECT setup_name, paradigm, COUNT(*) as n,
           AVG(outcome_pnl) as mean_pnl,
           SUM(outcome_pnl) as total_pnl,
           SUM(CASE WHEN outcome_pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as wr
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND direction IN ('long', 'bullish')
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'ES Absorption')
      AND outcome_pnl IS NOT NULL
      AND paradigm IS NOT NULL
    GROUP BY setup_name, paradigm
    HAVING COUNT(*) >= 5
    ORDER BY setup_name, AVG(outcome_pnl)
""")
prev_setup = None
for r in cur.fetchall():
    setup, para, n, mean, total, wr = r
    if setup != prev_setup:
        print(f"\n  {setup}")
        print(f"  {'paradigm':18s} {'n':>4s} {'wr':>6s} {'mean':>7s} {'total':>9s}")
        prev_setup = setup
    flag = " <- GEX-TARGET" if para == "GEX-TARGET" else ""
    print(f"  {para:18s} {n:>4d} {wr:>5.1f}% {mean:>+6.2f}pt {total:>+7.1f}pt{flag}")

# -----------------------------------------------------------------------------
# 3. REAL BROKER vs PORTAL gap for GEX-TARGET specifically
print("\n\n" + "="*100)
print("3. REAL BROKER P&L (from real_trade_orders) vs PORTAL outcome_pnl — GEX-TARGET longs only")
print("="*100)
cur.execute("""
    SELECT sl.id, sl.setup_name, sl.outcome_pnl,
           rto.state->>'fill_price', rto.state->>'close_fill_price',
           sl.direction
    FROM setup_log sl
    JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
    WHERE sl.ts >= '2026-02-01'
      AND sl.direction IN ('long', 'bullish')
      AND sl.paradigm = 'GEX-TARGET'
      AND sl.outcome_pnl IS NOT NULL
""")
gt_real = cur.fetchall()
print(f"Real broker fires in GEX-TARGET longs: {len(gt_real)}\n")
print(f"{'lid':>5s} {'setup':22s} {'portal':>8s} {'fill':>8s} {'close':>8s} {'real':>8s} {'gap':>7s}")
total_portal = 0.0
total_real = 0.0
for lid, setup, portal, fill, close, direction in gt_real:
    portal_pnl = float(portal)
    if fill and close:
        fill_f, close_f = float(fill), float(close)
        real_pnl = (close_f - fill_f) if direction in ('long','bullish') else (fill_f - close_f)
        gap = real_pnl - portal_pnl
        print(f"{lid:>5d} {setup:22s} {portal_pnl:>+7.1f} {fill_f:>8.2f} {close_f:>8.2f} {real_pnl:>+7.2f} {gap:>+7.2f}")
        total_portal += portal_pnl
        total_real += real_pnl
    else:
        print(f"{lid:>5d} {setup:22s} {portal_pnl:>+7.1f} {'-':>8s} {'-':>8s} {'-':>8s} {'-':>7s}")
print(f"\n  TOTAL: portal {total_portal:+.1f}pt (${total_portal*5:+.0f})   "
      f"real {total_real:+.1f}pt (${total_real*5:+.0f})   "
      f"gap {(total_real-total_portal):+.1f}pt (${(total_real-total_portal)*5:+.0f})")

# -----------------------------------------------------------------------------
# 4. Monthly stability — GEX-TARGET longs vs other paradigms by month
print("\n\n" + "="*100)
print("4. MONTHLY: GEX-TARGET longs (all setups combined, V16-eligible & not)")
print("="*100)
cur.execute("""
    SELECT to_char((ts AT TIME ZONE 'America/New_York')::date, 'YYYY-MM') as ym,
           COUNT(*),
           SUM(outcome_pnl),
           AVG(outcome_pnl),
           SUM(CASE WHEN outcome_pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as wr
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND direction IN ('long', 'bullish')
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'ES Absorption')
      AND outcome_pnl IS NOT NULL
      AND paradigm = 'GEX-TARGET'
    GROUP BY ym
    ORDER BY ym
""")
print(f"{'month':>8s} {'n':>4s} {'wr':>6s} {'total':>9s} {'mean':>7s}")
for r in cur.fetchall():
    print(f"{r[0]:>8s} {r[1]:>4d} {float(r[4]):>5.1f}% {float(r[2]):+8.1f}pt {float(r[3]):+6.2f}pt")

# -----------------------------------------------------------------------------
# 5. Time-of-day — does GEX-TARGET become toxic later in session?
print("\n\n" + "="*100)
print("5. TIME-OF-DAY: GEX-TARGET longs (does later in session = worse?)")
print("="*100)
cur.execute("""
    SELECT EXTRACT(HOUR FROM (ts AT TIME ZONE 'America/New_York')) as hr,
           COUNT(*),
           SUM(outcome_pnl),
           AVG(outcome_pnl),
           SUM(CASE WHEN outcome_pnl > 0 THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as wr
    FROM setup_log
    WHERE ts >= '2026-02-01'
      AND direction IN ('long', 'bullish')
      AND setup_name IN ('Skew Charm', 'DD Exhaustion', 'ES Absorption')
      AND outcome_pnl IS NOT NULL
      AND paradigm = 'GEX-TARGET'
    GROUP BY hr
    ORDER BY hr
""")
print(f"{'hour':>6s} {'n':>4s} {'wr':>6s} {'total':>9s} {'mean':>7s}")
for r in cur.fetchall():
    hr, n, total, mean, wr = r
    print(f"{int(hr):>4d}:00 {n:>4d} {wr:>5.1f}% {float(total):+8.1f}pt {float(mean):+6.2f}pt")

# -----------------------------------------------------------------------------
# 6. Today's 3 losses — are they representative of GEX-TARGET, or outliers?
print("\n\n" + "="*100)
print("6. TODAY (2026-05-22) GEX-TARGET LONGS — context")
print("="*100)
cur.execute("""
    SELECT id, setup_name, grade, outcome_pnl,
           (ts AT TIME ZONE 'America/New_York') as et_ts,
           greek_alignment, vix
    FROM setup_log
    WHERE ts::date = '2026-05-22'
      AND direction IN ('long', 'bullish')
      AND paradigm = 'GEX-TARGET'
      AND outcome_pnl IS NOT NULL
    ORDER BY ts
""")
for r in cur.fetchall():
    print(f"  lid={r[0]} {r[1]:22s} g={r[2] or '-':4s} "
          f"align={r[5] or '-'} vix={r[6] or 0:.1f}  "
          f"et={str(r[4])[:19]}  portal_pnl={float(r[3]):+.1f}pt")

conn.close()
