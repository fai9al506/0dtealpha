"""Backtest: block LONGS in AG-* paradigm while spot is far ABOVE the Volland target.

For each placed LONG (real_trade_orders, Apr 1+):
  - nearest volland snapshot within 6 min -> paradigm + target (parsed $)
  - dist = spot - target  (positive = price above target = drag-down room)
Buckets (AG-* paradigm only): >30, 20-30, 10-20, <=10 above, target>=spot
Also non-AG longs as control.
Broker $ per lid (MCHK method), portal-pnl fallback flagged.
"""
import os, json, re
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
import bisect

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

cur.execute("""
    SELECT ts, payload->'statistics'->>'paradigm', payload->'statistics'->>'target'
    FROM volland_snapshots WHERE ts >= '2026-04-01'
    ORDER BY ts
""")
vt, vp, vtgt = [], [], []
last_tgt = (None, None)  # (ts, val) carry-forward
last_para = (None, None)
for ts, para, tgt in cur.fetchall():
    val = None
    if tgt:
        m = re.search(r"[\d,]+", tgt)
        if m:
            val = float(m.group().replace(",", ""))
    if val is not None:
        last_tgt = (ts, val)
    elif last_tgt[0] is not None and (ts - last_tgt[0]).total_seconds() <= 1200:
        val = last_tgt[1]
    if para is not None:
        last_para = (ts, para)
    elif last_para[0] is not None and (ts - last_para[0]).total_seconds() <= 1200:
        para = last_para[1]
    vt.append(ts); vp.append(para); vtgt.append(val)

def volland_at(ts):
    i = bisect.bisect_left(vt, ts)
    best = None
    for j in (i - 1, i):
        if 0 <= j < len(vt):
            d = abs((vt[j] - ts).total_seconds())
            if d <= 360 and (best is None or d < best[0]):
                best = (d, j)
    if best is None:
        return None, None
    j = best[1]
    return vp[j], vtgt[j]

cur.execute("""
    SELECT r.setup_log_id, r.state, l.setup_name, l.direction, l.ts, l.spot, l.outcome_pnl, l.paradigm
    FROM real_trade_orders r JOIN setup_log l ON l.id = r.setup_log_id
    WHERE l.ts >= '2026-04-01' ORDER BY l.ts
""")
buckets = defaultdict(list)
detail_jun3 = []
fallbacks = 0
for lid, state, name, d, ts, spot, pnl, para_log in cur.fetchall():
    if (d or "").lower() not in ("long", "bullish", "buy"):
        continue
    st = state if isinstance(state, dict) else json.loads(state or "{}")
    fill = st.get("fill_price"); close_p = st.get("close_fill_price")
    qty = float(st.get("qty") or 1)
    if fill is not None and close_p is not None:
        usd = (float(close_p) - float(fill)) * 5.0 * qty
    elif pnl is not None:
        usd = float(pnl) * 5.0; fallbacks += 1
    else:
        continue
    para, tgt = volland_at(ts)
    if spot is None or tgt is None or para is None:
        buckets[("?", "no-data")].append(usd); continue
    spot = float(spot)
    dist = spot - tgt
    is_ag = para.startswith("AG")
    if dist <= 0:
        b = "tgt>=spot (bullish)"
    elif dist <= 10:
        b = "0-10 above tgt"
    elif dist <= 20:
        b = "10-20 above tgt"
    elif dist <= 30:
        b = "20-30 above tgt"
    else:
        b = ">30 above tgt"
    buckets[("AG" if is_ag else "nonAG", b)].append(usd)
    t_et = ts.astimezone(ET)
    if t_et.date().isoformat() == "2026-06-03":
        detail_jun3.append((lid, t_et.strftime("%H:%M"), name, para, tgt, spot, dist, usd))

print(f"(fallback-$ used for {fallbacks} longs)")
for grp in ("AG", "nonAG", "?"):
    keys = [k for k in buckets if k[0] == grp]
    if not keys:
        continue
    print(f"\n=== {grp} paradigm LONGS ===")
    order = ["tgt>=spot (bullish)", "0-10 above tgt", "10-20 above tgt", "20-30 above tgt", ">30 above tgt", "no-data"]
    for b in order:
        xs = buckets.get((grp, b))
        if not xs:
            continue
        w = sum(1 for u in xs if u > 0.5); l = sum(1 for u in xs if u < -0.5)
        print(f"  {b:20s} n={len(xs):3d}  W{w}/L{l}  total ${sum(xs):+8.2f}  avg ${sum(xs)/len(xs):+7.2f}")

print("\n=== Jun 3 longs detail ===")
for lid, t, name, para, tgt, spot, dist, usd in detail_jun3:
    print(f"  lid {lid} {t} {name:14s} {para:9s} tgt={tgt:.0f} spot={spot:.1f} dist={dist:+.1f} -> ${usd:+.2f}")
c.close()
