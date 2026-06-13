"""MCHK — Monthly Projection Checkpoint for May 2026 (first run).

Pulls REAL placed-trade results from setup_log + real_trade_orders.
Per-lid broker P&L computed identically to app/trade_reconcile.py:
  real_pts = (exit - fill) if long else (fill - exit)
  exit = stop_fill_price OR close_fill_price
Dollars @ 1 MES = real_pts * 5.0 * qty  (commission noted separately).

All numbers from DB. PLACED = real_trade_orders row exists (actually fired).
Splits pre-V16 (May 1-17, broken regime) vs post-V16 (May 18-31, anchor).
"""
import os, sys, psycopg2, json
from collections import defaultdict
from datetime import date
sys.stdout.reconfigure(encoding='utf-8')

MES_PT = 5.0
COMM_PER_RT = 1.0  # $1/RT MES per memory

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Every placed real trade in May 2026 (join on setup_log_id; ET trade date).
cur.execute("""
    SELECT sl.id,
           (sl.ts AT TIME ZONE 'America/New_York')::date AS et_date,
           sl.setup_name, sl.direction, sl.grade, sl.paradigm,
           sl.outcome_result, sl.outcome_pnl, sl.real_trade_skip_reason,
           rto.state
    FROM setup_log sl
    JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
    WHERE (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-05-01'
      AND (sl.ts AT TIME ZONE 'America/New_York')::date <= '2026-05-31'
    ORDER BY sl.ts
""")
rows = cur.fetchall()

def real_pts_of(state, direction):
    if isinstance(state, str):
        state = json.loads(state)
    fill = state.get("fill_price")
    exit_p = state.get("stop_fill_price") or state.get("close_fill_price")
    qty = state.get("quantity") or 1
    if fill is None or exit_p is None:
        return None, qty, state.get("close_reason", "?")
    is_long = (direction or "").lower() in ("long", "bullish")
    pts = (exit_p - fill) if is_long else (fill - exit_p)
    return pts, qty, state.get("close_reason", "?")

def bucket(d):
    return "pre_v16" if d <= date(2026, 5, 17) else "post_v16"

stats = {"pre_v16": [], "post_v16": []}
daily = defaultdict(lambda: {"real_d": 0.0, "portal_p": 0.0, "n": 0})
no_exit = []
skip_nonnull = 0

for (lid, et_date, setup, direction, grade, paradigm, oc_res, oc_pnl,
     skip_reason, state) in rows:
    if skip_reason is not None:
        skip_nonnull += 1  # placed but filter-flagged? track anomaly
    pts, qty, creason = real_pts_of(state, direction)
    portal_p = float(oc_pnl) if oc_pnl is not None else 0.0
    b = bucket(et_date)
    if pts is None:
        no_exit.append((lid, et_date, setup, creason))
        rec = {"lid": lid, "date": et_date, "setup": setup, "dir": direction,
               "grade": grade, "real_pts": None, "qty": qty,
               "portal_p": portal_p, "oc_res": oc_res, "creason": creason}
    else:
        real_d = pts * MES_PT * qty - COMM_PER_RT * qty
        rec = {"lid": lid, "date": et_date, "setup": setup, "dir": direction,
               "grade": grade, "real_pts": pts, "qty": qty, "real_d": real_d,
               "portal_p": portal_p, "oc_res": oc_res, "creason": creason}
        daily[et_date]["real_d"] += real_d
        daily[et_date]["portal_p"] += portal_p
        daily[et_date]["n"] += 1
    stats[b].append(rec)

def summarize(label, recs):
    closed = [r for r in recs if r["real_pts"] is not None]
    n = len(recs)
    nc = len(closed)
    real_pts = sum(r["real_pts"] * r["qty"] for r in closed)
    real_d = sum(r["real_d"] for r in closed)
    portal_p = sum(r["portal_p"] for r in recs)
    real_wins = sum(1 for r in closed if r["real_d"] > 0)
    portal_wins = sum(1 for r in recs if (r["oc_res"] or "").upper() == "WIN")
    capture = (real_pts / portal_p * 100) if portal_p else float("nan")
    print(f"\n===== {label} =====")
    print(f"  Placed (fired):        {n}  (closed w/ fills: {nc}, no-exit: {n-nc})")
    print(f"  Portal pts (outcome):  {portal_p:+.1f}p   (= ${portal_p*MES_PT:+.0f} @1MES)")
    print(f"  Real broker pts:       {real_pts:+.1f}p   (= ${real_d:+.0f} @1MES, after ${COMM_PER_RT}/RT comm)")
    print(f"  Capture (real/portal): {capture:.0f}%")
    if nc:
        print(f"  Real WR (P&L>0):       {real_wins}/{nc} = {real_wins/nc*100:.0f}%")
    if n:
        print(f"  Portal WR (oc=WIN):    {portal_wins}/{n} = {portal_wins/n*100:.0f}%")
    # per-setup
    bys = defaultdict(lambda: {"n": 0, "real_d": 0.0, "portal_p": 0.0, "w": 0, "nc": 0})
    for r in recs:
        s = bys[r["setup"]]
        s["n"] += 1
        s["portal_p"] += r["portal_p"]
        if r["real_pts"] is not None:
            s["real_d"] += r["real_d"]; s["nc"] += 1
            if r["real_d"] > 0: s["w"] += 1
    print(f"  Per setup:")
    for name, s in sorted(bys.items(), key=lambda kv: kv[1]["real_d"]):
        wr = f"{s['w']}/{s['nc']}" if s['nc'] else "0/0"
        print(f"    {name:18s} {s['n']:2d}t  real ${s['real_d']:+7.0f}  portal {s['portal_p']:+6.1f}p  WR {wr}")
    return {"n": n, "nc": nc, "real_pts": real_pts, "real_d": real_d,
            "portal_p": portal_p, "capture": capture,
            "real_wr": (real_wins/nc if nc else 0)}

summ = {}
for b, lbl in [("pre_v16", "PRE-V16 (May 1-17, broken regime — context only)"),
               ("post_v16", "POST-V16 (May 18-31, the ANCHOR month)")]:
    summ[b] = summarize(lbl, stats[b])

# Daily green/red for post-V16
print("\n===== POST-V16 DAILY (real $ @1MES) =====")
post_days = sorted(d for d in daily if d >= date(2026, 5, 18))
green = red = flat = 0
for d in post_days:
    v = daily[d]["real_d"]; n = daily[d]["n"]
    tag = "🟢" if v > 0 else ("🔴" if v < 0 else "⚪")
    if v > 0: green += 1
    elif v < 0: red += 1
    else: flat += 1
    print(f"  {d}  {tag} ${v:+7.0f}  ({n} trades, portal {daily[d]['portal_p']:+.1f}p)")
print(f"  Green/Red/Flat: {green}/{red}/{flat}  ({len(post_days)} trading days)")

# Projection math
print("\n===== PROJECTION MATH =====")
pv = summ["post_v16"]
if post_days:
    days = len(post_days)
    real_per_day = pv["real_d"] / days
    print(f"  Post-V16 real: ${pv['real_d']:+.0f} over {days} trading days = ${real_per_day:+.1f}/day @1MES")
    print(f"  Implied monthly (×21 trading days): ${real_per_day*21:+.0f}/mo @1MES")
    print(f"  Implied monthly @1ES (×10):          ${real_per_day*21*10:+.0f}/mo")
    print(f"  Portal post-V16: {pv['portal_p']:+.1f}p (${pv['portal_p']*MES_PT:+.0f}) → capture {pv['capture']:.0f}%")

print("\n===== TIER-ADVANCE GATE =====")
month_green = pv["real_d"] > 0
cap_ok = pv["capture"] >= 85
print(f"  Green month (real$>0):  {'PASS' if month_green else 'FAIL'} (${pv['real_d']:+.0f})")
print(f"  Capture >85%:           {'PASS' if cap_ok else 'FAIL'} ({pv['capture']:.0f}%)")
print(f"  → Cleared to 3 MES:     {'YES' if (month_green and cap_ok) else 'NO'}")

if no_exit:
    print(f"\n  NO-EXIT lids (ghost/open, excluded from real$): {len(no_exit)}")
    for lid, d, s, cr in no_exit:
        print(f"    lid={lid} {d} {s} ({cr})")
if skip_nonnull:
    print(f"\n  ANOMALY: {skip_nonnull} placed trades have non-null skip_reason")

cur.close(); conn.close()
