"""Per-day cap replay matrix, post-V16 (May 18+): cap 3 vs 4,5,6,7,8."""
import os
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text

url = os.environ['DATABASE_URL'].replace('postgresql://', 'postgresql+psycopg://', 1)
eng = create_engine(url, pool_pre_ping=True, isolation_level='AUTOCOMMIT')

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT sl.id, (sl.ts AT TIME ZONE 'America/New_York') AS et,
               sl.direction, sl.outcome_pnl, sl.outcome_elapsed_min,
               (rto.setup_log_id IS NOT NULL) AS placed
        FROM setup_log sl
        LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (rto.setup_log_id IS NOT NULL
               OR sl.real_trade_skip_reason IN ('cap_long_full','cap_short_full'))
          AND (sl.ts AT TIME ZONE 'America/New_York')::date >= '2026-05-18'
        ORDER BY sl.ts
    """)).fetchall()

cands = []
for lid, et, direction, pnl, elapsed, placed in rows:
    if pnl is None or elapsed is None:
        continue
    cands.append({
        "et": et, "day": et.date().isoformat(),
        "is_long": (direction or "").lower() in ("long", "bullish"),
        "pnl": float(pnl), "close": et + timedelta(minutes=float(elapsed)),
        "placed": bool(placed),
    })

by_day = defaultdict(list)
for cd in cands:
    by_day[cd["day"]].append(cd)


def replay_day(items, cap, breaker=-300.0):
    open_pos, closedq, total = [], [], 0.0
    n_admit = 0
    for t in sorted(items, key=lambda x: x["et"]):
        realized = sum(u for ct, u in closedq if ct <= t["et"])
        open_pos = [p for p in open_pos if p["close"] > t["et"]]
        if realized <= breaker:
            continue
        if sum(1 for p in open_pos if p["is_long"] == t["is_long"]) >= cap:
            continue
        n_admit += 1
        open_pos.append(t)
        usd = t["pnl"] * 5.0 - 1.0
        closedq.append((t["close"], usd))
        total += usd
    return total, n_admit


CAPS = (3, 4, 5, 6, 7, 8)
hdr = f"{'day':<12}{'sig':>5}" + "".join(f"{('cap'+str(c)):>9}" for c in CAPS) + f"{'   c8-c3':>9}"
print(hdr)
print("-" * len(hdr))
tot = {c: 0.0 for c in CAPS}
for day in sorted(by_day):
    items = by_day[day]
    vals = {}
    for cap in CAPS:
        v, _ = replay_day(items, cap)
        vals[cap] = v
        tot[cap] += v
    diff = vals[8] - vals[3]
    mark = "  <--" if abs(diff) > 25 else ""
    print(f"{day:<12}{len(items):>5}" + "".join(f"{vals[c]:>+9.0f}" for c in CAPS)
          + f"{diff:>+9.0f}{mark}")
print("-" * len(hdr))
print(f"{'TOTAL':<12}{len(cands):>5}" + "".join(f"{tot[c]:>+9.0f}" for c in CAPS)
      + f"{tot[8]-tot[3]:>+9.0f}")
n_days = len(by_day)
print(f"\n{n_days} trading days; per-day avg: " +
      ", ".join(f"cap{c} ${tot[c]/n_days:+.0f}" for c in CAPS))
wins = {c: min(tot.items(), key=lambda x: 0)[0] for c in CAPS}
for c in CAPS:
    per = [replay_day(by_day[d], c)[0] for d in by_day]
    neg = [p for p in per if p < 0]
    print(f"cap{c}: worst day {min(per):+.0f}, red days {len(neg)}/{n_days}, "
          f"sum of red days {sum(neg):+.0f}")
