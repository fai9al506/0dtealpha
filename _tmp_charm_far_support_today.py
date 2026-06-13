"""Verify user's observation for 2026-06-03 morning charm structure + spot path."""
import os
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict

ET = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

cur.execute("""
    SELECT expiration_option, count(*) FROM volland_exposure_points
    WHERE greek='charm' AND ts_utc >= '2026-06-03 13:00+00'
    GROUP BY 1
""")
print("charm rows today by exp:", cur.fetchall())

cur.execute("""
    SELECT ts_utc, strike, value, current_price
    FROM volland_exposure_points
    WHERE greek='charm' AND expiration_option IS NULL AND ticker='SPX'
      AND ts_utc >= '2026-06-03 13:00+00'
    ORDER BY ts_utc
""")
rows = cur.fetchall()
snaps = defaultdict(list)
spot_at = {}
for ts, strike, val, cp in rows:
    t_et = ts.astimezone(ET)
    snaps[t_et].append((float(strike), float(val)))
    if cp is not None:
        spot_at[t_et] = float(cp)

times = sorted(snaps.keys())
print(f"TODAY-charm snapshots: {len(times)}")
shown = 0
for t in times:
    if (t.hour, t.minute) >= (9, 30) and shown < 4:
        pts = snaps[t]
        spot = spot_at.get(t)
        top = sorted(pts, key=lambda x: -abs(x[1]))[:8]
        print(f"\n=== {t.strftime('%H:%M')} ET  spot={spot} ===")
        for s, v in sorted(top):
            d = f"{s - spot:+7.1f}" if spot else "  n/a"
            print(f"  strike {s:7.0f}  charm {v/1e6:+10.1f}M  dist {d}")
        shown += 1

# spot path today
cur.execute("SELECT ts, spot FROM chain_snapshots WHERE ts >= '2026-06-03 13:00+00' ORDER BY ts")
sp = [(t.astimezone(ET), float(s)) for t, s in cur.fetchall() if s]
if sp:
    lo = min(sp, key=lambda x: x[1]); hi = max(sp, key=lambda x: x[1])
    print(f"\nspot: first={sp[0][1]:.1f}@{sp[0][0].strftime('%H:%M')}  LOW={lo[1]:.1f}@{lo[0].strftime('%H:%M')}  HIGH={hi[1]:.1f}@{hi[0].strftime('%H:%M')}  last={sp[-1][1]:.1f}@{sp[-1][0].strftime('%H:%M')}")
c.close()
