"""User's refinement: block new entry only when open same-direction trades are UNDERWATER.

unrealized pts of open trade j at new signal i = (sig_spot_i - sig_spot_j) * sign_j
(both SPX chain spots at signal times -- no path lookup needed)

Variants:
  U1: >=2 open same-DIR  and sum unreal < 0    -> block
  U2: >=2 open same-DIR  and sum unreal < -5   -> block
  U3: >=1 open same-DIR  and sum unreal < -5   -> block
  U4: >=2 open same SETUP+dir and sum unreal < 0 -> block
  C2: blunt cap=2 same setup+dir (reference)
Realistic close times (price-touch reconstruction). Blocked trades free slots.
"""
import os, json
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
from datetime import date

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()
cur.execute("SELECT ts, spot FROM chain_snapshots WHERE spot IS NOT NULL AND ts >= '2026-04-01' ORDER BY ts")
path_by_day = defaultdict(list)
for ts, spot in cur.fetchall():
    t = ts.astimezone(ET)
    path_by_day[t.date()].append((t, float(spot)))

cur.execute("""
    SELECT r.setup_log_id, r.state, l.setup_name, l.direction, l.ts, l.spot, l.outcome_pnl
    FROM real_trade_orders r JOIN setup_log l ON l.id = r.setup_log_id
    WHERE l.ts >= '2026-04-01' ORDER BY l.ts
""")
trades = []
for lid, state, name, d, ts, sig_spot, pnl in cur.fetchall():
    st = state if isinstance(state, dict) else json.loads(state or "{}")
    t = ts.astimezone(ET)
    sign = 1 if (d or "").lower() in ("long", "bullish", "buy") else -1
    fill = st.get("fill_price"); cp = st.get("close_fill_price")
    qty = float(st.get("quantity") or st.get("qty") or 1)
    if fill is not None and cp is not None:
        usd = (float(cp) - float(fill)) * sign * 5.0 * qty
    elif pnl is not None:
        usd = float(pnl) * 5.0
    else:
        continue
    close_t = None
    if fill is not None and cp is not None and sig_spot is not None:
        basis = float(fill) - float(sig_spot)
        level = float(cp) - basis
        es = float(sig_spot)
        for tt, s in path_by_day.get(t.date(), []):
            if tt <= t:
                continue
            if (level <= es and s <= level + 0.3) or (level > es and s >= level - 0.3):
                close_t = tt; break
    if close_t is None:
        close_t = t.replace(hour=16, minute=0, second=0)
    trades.append(dict(lid=lid, name=name, dir="L" if sign > 0 else "S", sign=sign,
                       t_open=t, t_close=close_t, usd=usd, d=t.date(),
                       sspot=float(sig_spot) if sig_spot is not None else None))

def replay(variant, era=None):
    by_day = defaultdict(list)
    for x in trades:
        if era and x["d"] < era:
            continue
        by_day[x["d"]].append(x)
    base = sum(x["usd"] for xs in by_day.values() for x in xs)
    kept, removed = 0.0, []
    for d, xs in sorted(by_day.items()):
        open_list = []  # accepted: dicts
        for x in sorted(xs, key=lambda z: z["t_open"]):
            open_list = [o for o in open_list if o["t_close"] > x["t_open"]]
            blocked = False
            if x["sspot"] is not None:
                if variant in ("U1", "U2", "U3"):
                    sel = [o for o in open_list if o["dir"] == x["dir"] and o["sspot"] is not None]
                elif variant == "U4":
                    sel = [o for o in open_list if o["dir"] == x["dir"] and o["name"] == x["name"] and o["sspot"] is not None]
                else:
                    sel = [o for o in open_list if o["dir"] == x["dir"] and o["name"] == x["name"]]
                if variant == "C2":
                    blocked = len(sel) >= 2
                else:
                    unreal = sum((x["sspot"] - o["sspot"]) * o["sign"] for o in sel)
                    min_n = 1 if variant == "U3" else 2
                    thr = 0.0 if variant in ("U1", "U4") else -5.0
                    blocked = len(sel) >= min_n and unreal < thr
            if blocked:
                removed.append(x); continue
            kept += x["usd"]
            open_list.append(x)
    return base, kept, removed

for era_name, era in (("FULL Apr1+", None), ("POST-V16 May18+", date(2026, 5, 18))):
    print(f"\n=== {era_name} ===")
    for v in ("U1", "U2", "U3", "U4", "C2"):
        base, kept, rem = replay(v, era)
        rw = sum(1 for x in rem if x["usd"] > 0.5)
        print(f"  {v}: delta {kept-base:+8.2f}  removed {len(rem):2d} ({rw} winners, ${sum(x['usd'] for x in rem):+.2f})")
    base, kept, rem = replay("U4", era)
    print("  U4 removed:")
    for x in rem:
        print(f"    {x['d']} {x['t_open'].strftime('%H:%M')} {x['name']:14s} {x['dir']} ${x['usd']:+.2f}")
c.close()
