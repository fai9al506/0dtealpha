"""Backtest: TSRT global entry gate on realized + UNREALIZED <= threshold.

Replays actually-placed trades (bot-own broker fills). At each entry time t:
  realized   = sum of real $ of trades already closed today (incl $1 comm)
  unrealized = sum over open positions of (ES mark @ t - entry) * dir * $5
If realized + unrealized <= threshold -> entry BLOCKED (gate re-evaluated per
entry, so it lifts if open positions recover). Gated P&L = actual minus
blocked trades' real P&L. ES mark from vps_es_range_bars 5pt closes.
"""
import os
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT sl.id, (sl.ts AT TIME ZONE 'America/New_York') AS et,
               sl.setup_name, sl.direction, sl.outcome_elapsed_min, rto.state
        FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        ORDER BY sl.ts
    """)).fetchall()

    trades = []
    for lid, et, name, direction, elapsed, st in rows:
        st = st or {}
        fill = st.get("fill_price")
        exit_p = (st.get("stop_fill_price_pre_fifo_reconcile") or st.get("stop_fill_price")
                  or st.get("close_fill_price_pre_fifo_reconcile") or st.get("close_fill_price"))
        if fill is None or exit_p is None or elapsed is None:
            continue
        is_long = (direction or "").lower() in ("long", "bullish")
        pts = (float(exit_p) - float(fill)) * (1 if is_long else -1)
        trades.append({
            "lid": lid, "et": et, "day": et.date().isoformat(), "name": name,
            "is_long": is_long, "entry": float(fill),
            "close": et + timedelta(minutes=float(elapsed)),
            "usd": pts * 5.0 - 1.0,
        })

    days = sorted({t["day"] for t in trades})
    # ES bar closes per day (ts_start, close) for marking
    bars_by_day = {}
    for d in days:
        b = c.execute(text("""
            SELECT ts_start, bar_close FROM vps_es_range_bars
            WHERE trade_date = :d AND range_pts = 5.0 ORDER BY ts_start
        """), {"d": d}).fetchall()
        if not b:  # legacy fallback
            b = c.execute(text("""
                SELECT ts_start, bar_close FROM es_range_bars
                WHERE trade_date = :d AND source = 'rithmic' ORDER BY ts_start
            """), {"d": d}).fetchall()
        bars_by_day[d] = [(r[0], float(r[1])) for r in b]

print(f"{len(trades)} placed trades, {len(days)} days, {days[0]} -> {days[-1]}")
no_bars = [d for d in days if not bars_by_day[d]]
if no_bars:
    print(f"days without ES bars (unrealized=0 assumed): {no_bars}")


from zoneinfo import ZoneInfo
NY = ZoneInfo("America/New_York")


def mark(day, t):
    """ES close of last bar starting at/before t. Normalize both to naive ET."""
    t_naive = t.astimezone(NY).replace(tzinfo=None) if t.tzinfo else t
    best = None
    for ts, cl in bars_by_day[day]:
        ts_naive = ts.astimezone(NY).replace(tzinfo=None) if ts.tzinfo else ts - timedelta(hours=4)
        if ts_naive <= t_naive:
            best = cl
        else:
            break
    return best


def run_gate(threshold, day_filter=None):
    tot_actual = tot_gated = 0.0
    blocked = []
    by_day = defaultdict(list)
    for t in trades:
        if day_filter and not day_filter(t["day"]):
            continue
        by_day[t["day"]].append(t)
    day_gated = {}
    for day, items in by_day.items():
        admitted = []
        d_act = d_gat = 0.0
        for t in sorted(items, key=lambda x: x["et"]):
            d_act += t["usd"]
            realized = sum(a["usd"] for a in admitted if a["close"] <= t["et"])
            unrl = 0.0
            for a in admitted:
                if a["close"] > t["et"]:
                    m = mark(day, t["et"])
                    if m is not None:
                        unrl += (m - a["entry"]) * (1 if a["is_long"] else -1) * 5.0
            if realized + unrl <= threshold:
                blocked.append(t)
                continue
            admitted.append(t)
            d_gat += t["usd"]
        tot_actual += d_act
        tot_gated += d_gat
        day_gated[day] = (d_act, d_gat)
    return tot_actual, tot_gated, blocked, day_gated


V16 = lambda d: d >= "2026-05-18"

for label, flt in [("FULL era", None), ("post-V16", V16)]:
    print(f"\n===== {label} =====")
    print(f"{'thresh':>8}{'actual $':>10}{'gated $':>10}{'saved $':>9}{'blocked':>9}{'blk W/L':>9}")
    for th in (-200.0, -250.0, -300.0):
        act, gat, blk, _ = run_gate(th, flt)
        w = sum(1 for b in blk if b["usd"] > 0)
        l = len(blk) - w
        print(f"{th:>8.0f}{act:>+10.0f}{gat:>+10.0f}{gat-act:>+9.0f}{len(blk):>9}{f'{w}/{l}':>9}")

# detail at -300 post-V16: which days changed + blocked trade list
print("\n=== -300 gate, post-V16: days affected ===")
act, gat, blk, day_gated = run_gate(-300.0, V16)
for d in sorted(day_gated):
    a, g = day_gated[d]
    if abs(a - g) > 1:
        print(f"  {d}: actual {a:+.0f} -> gated {g:+.0f} (saved {g-a:+.0f})")
print("blocked trades:")
for b in sorted(blk, key=lambda x: x["et"]):
    print(f"  {b['day']} {b['et'].strftime('%H:%M')} {b['name']:<15} "
          f"{'L' if b['is_long'] else 'S'}  outcome ${b['usd']:+.0f}")
