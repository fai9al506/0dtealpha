"""How did per-strike charm evolve through the morning of 2026-06-03?
Focus: where were the dominant bars, and what was at 7545-7555?"""
import os
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

cur.execute("""
    SELECT ts_utc, strike, value, current_price
    FROM volland_exposure_points
    WHERE greek='charm' AND expiration_option IS NULL AND ticker='SPX'
      AND ts_utc >= '2026-06-03 13:00+00'
    ORDER BY ts_utc
""")
snaps = defaultdict(list); spot_at = {}
for ts, strike, val, cp in cur.fetchall():
    t = ts.astimezone(ET)
    snaps[t].append((float(strike), float(val)))
    if cp is not None:
        spot_at[t] = float(cp)

times = sorted(snaps.keys())
targets = ["09:40", "10:00", "10:30", "11:00", "11:30", "12:00", "12:30"]
for tgt in targets:
    hh, mm = map(int, tgt.split(":"))
    best = min(times, key=lambda t: abs((t.hour*60+t.minute) - (hh*60+mm)))
    pts = snaps[best]; spot = spot_at.get(best)
    top = sorted(pts, key=lambda x: -abs(x[1]))[:6]
    zone = [(s, v) for s, v in pts if 7540 <= s <= 7560]
    zone_str = ", ".join(f"{s:.0f}:{v/1e6:+.1f}M" for s, v in sorted(zone)) or "none"
    print(f"\n=== {best.strftime('%H:%M')} ET  spot={spot} ===")
    for s, v in sorted(top):
        print(f"  top: {s:7.0f}  {v/1e6:+8.1f}M  dist {s-spot:+7.1f}" if spot else f"  top: {s:7.0f} {v/1e6:+8.1f}M")
    print(f"  7540-7560 zone: {zone_str}")
c.close()
