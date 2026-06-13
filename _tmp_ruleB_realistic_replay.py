"""REALISTIC consecutive-loss breaker replay (user's challenge: losses realize at CLOSE, not entry).

Close-time reconstruction: level_spx = close_fill_price - (fill_price - signal_spot);
first chain_snapshots ts after signal where spot touches level (direction-aware).
Fallback (no close fill / never touches): 16:00 ET.

Event replay: per direction, count consecutive REALIZED losses (usd <= -$20) ordered by
close time; reset on any realized close > -$20. On 3rd consecutive -> block that
direction for N minutes from THAT CLOSE TIME. Blocked trades never enter (their
outcomes drop out of the event stream).
"""
import os, json
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
from datetime import timedelta, date, time as dtime
import bisect

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
    # reconstruct close time
    close_t = None
    day_path = path_by_day.get(t.date(), [])
    if fill is not None and cp is not None and sig_spot is not None:
        basis = float(fill) - float(sig_spot)
        level = float(cp) - basis
        after = [(tt, s) for tt, s in day_path if tt > t]
        entry_spot = float(sig_spot)
        for tt, s in after:
            if (level <= entry_spot and s <= level + 0.3) or (level > entry_spot and s >= level - 0.3):
                close_t = tt
                break
    if close_t is None:
        close_t = t.replace(hour=16, minute=0, second=0)
    trades.append(dict(lid=lid, name=name, dir="L" if sign > 0 else "S",
                       t_open=t, t_close=close_t, usd=usd, d=t.date()))

print("=== Jun 3 reconstructed timeline ===")
for x in trades:
    if x["d"] == date(2026, 6, 3):
        print(f"  lid {x['lid']} {x['name']:14s} {x['dir']} open {x['t_open'].strftime('%H:%M')} -> close {x['t_close'].strftime('%H:%M')}  ${x['usd']:+.2f}")

def replay(cool_min, era=None, thresh=3):
    by_day = defaultdict(list)
    for x in trades:
        if era and x["d"] < era:
            continue
        by_day[x["d"]].append(x)
    base = sum(x["usd"] for xs in by_day.values() for x in xs)
    kept_total = 0.0
    removed = []
    for d, xs in sorted(by_day.items()):
        xs = sorted(xs, key=lambda z: z["t_open"])
        active = []   # accepted trades
        block = {}    # dir -> until
        # event-driven: process entries in order; before each entry, process closes that realized before it
        closed_stream = []  # (close_t, dir, usd) of accepted trades, processed lazily
        consec = defaultdict(int)
        processed = 0
        for x in xs:
            # realize all accepted closes before this entry
            closed_stream.sort()
            while processed < len(closed_stream) and closed_stream[processed][0] <= x["t_open"]:
                ct, dr, usd = closed_stream[processed]; processed += 1
                if usd <= -20:
                    consec[dr] += 1
                    if consec[dr] >= thresh:
                        block[dr] = ct + timedelta(minutes=cool_min)
                        consec[dr] = 0
                else:
                    consec[dr] = 0
            if x["dir"] in block and x["t_open"] < block[x["dir"]]:
                removed.append(x)
                continue
            kept_total += x["usd"]
            closed_stream.append((x["t_close"], x["dir"], x["usd"]))
    return base, kept_total, removed

for era_name, era in (("FULL Apr1+", None), ("POST-V16 May18+", date(2026, 5, 18))):
    print(f"\n=== {era_name} ===")
    for cm in (90, 120, 180, 99999):
        base, kept, rem = replay(cm, era)
        rw = sum(1 for x in rem if x["usd"] > 0.5)
        label = f"{cm}min" if cm < 99999 else "rest-of-day"
        print(f"  cooldown {label:11s}: delta {kept-base:+8.2f}  removed {len(rem)} ({rw} winners)")
    base, kept, rem = replay(90, era)
    if rem:
        print("  removed @90min:")
        for x in rem:
            print(f"    {x['d']} {x['t_open'].strftime('%H:%M')} {x['name']:14s} {x['dir']} ${x['usd']:+.2f}")
c.close()
