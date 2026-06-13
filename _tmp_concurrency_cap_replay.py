"""Realistic replay: block entry when >= CAP same setup+direction trades currently OPEN.
Uses reconstructed close times (price-touch method). Blocked trades free their slot.
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
        entry_spot = float(sig_spot)
        for tt, s in path_by_day.get(t.date(), []):
            if tt <= t:
                continue
            if (level <= entry_spot and s <= level + 0.3) or (level > entry_spot and s >= level - 0.3):
                close_t = tt; break
    if close_t is None:
        close_t = t.replace(hour=16, minute=0, second=0)
    trades.append(dict(lid=lid, name=name, dir="L" if sign > 0 else "S",
                       t_open=t, t_close=close_t, usd=usd, d=t.date()))

def replay(cap, key_fn, era=None):
    by_day = defaultdict(list)
    for x in trades:
        if era and x["d"] < era:
            continue
        by_day[x["d"]].append(x)
    base = sum(x["usd"] for xs in by_day.values() for x in xs)
    kept, removed = 0.0, []
    for d, xs in sorted(by_day.items()):
        open_list = []  # (close_t, key) of ACCEPTED trades
        for x in sorted(xs, key=lambda z: z["t_open"]):
            open_list = [(ct, k) for ct, k in open_list if ct > x["t_open"]]
            k = key_fn(x)
            n_open = sum(1 for _, kk in open_list if kk == k)
            if n_open >= cap:
                removed.append(x); continue
            kept += x["usd"]
            open_list.append((x["t_close"], k))
    return base, kept, removed

for era_name, era in (("FULL Apr1+", None), ("POST-V16 May18+", date(2026, 5, 18))):
    print(f"\n=== {era_name} ===")
    for label, cap, kf in (
        ("same setup+dir cap=2", 2, lambda x: (x["name"], x["dir"])),
        ("same setup+dir cap=1", 1, lambda x: (x["name"], x["dir"])),
        ("same DIRECTION cap=2", 2, lambda x: x["dir"]),
    ):
        base, kept, rem = replay(cap, kf, era)
        rw = sum(1 for x in rem if x["usd"] > 0.5)
        rem_usd = sum(x["usd"] for x in rem)
        print(f"  {label:22s}: delta {kept-base:+8.2f}  removed {len(rem)} trades (${rem_usd:+.2f}, {rw} winners)")
    # detail for the leader
    base, kept, rem = replay(2, lambda x: (x["name"], x["dir"]), era)
    print("  removed by setup+dir cap=2:")
    for x in rem:
        print(f"    {x['d']} {x['t_open'].strftime('%H:%M')} {x['name']:14s} {x['dir']} ${x['usd']:+.2f}")
c.close()
