"""Concurrency-cap replay study (S211 candidate).

Candidates = TSRT-placed trades + cap_{long,short}_full skips. Chronological
replay per cap N: admit if same-direction open count < N AND daily realized
(at CLOSE times, incl $1/RT comm) > -$300. Sim outcomes (outcome_pnl /
outcome_elapsed_min) used for ALL candidates so placed vs skipped are
apples-to-apples. Validation: N=3 must ≈ the actually-placed set.
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
               sl.setup_name, sl.direction, sl.outcome_pnl, sl.outcome_elapsed_min,
               sl.real_trade_skip_reason,
               (rto.setup_log_id IS NOT NULL) AS placed
        FROM setup_log sl
        LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE (rto.setup_log_id IS NOT NULL
               OR sl.real_trade_skip_reason IN ('cap_long_full','cap_short_full'))
        ORDER BY sl.ts
    """)).fetchall()

cands = []
skipped_unresolved = 0
for lid, et, name, direction, pnl, elapsed, skip, placed in rows:
    if pnl is None or elapsed is None:
        skipped_unresolved += 1
        continue
    is_long = (direction or "").lower() in ("long", "bullish")
    cands.append({
        "lid": lid, "et": et, "day": et.date().isoformat(),
        "is_long": is_long, "pnl": float(pnl),
        "close": et + timedelta(minutes=float(elapsed)),
        "placed": bool(placed),
    })

days = sorted({c["day"] for c in cands})
print(f"candidates: {len(cands)} ({sum(1 for c in cands if c['placed'])} placed, "
      f"{sum(1 for c in cands if not c['placed'])} cap-skipped), "
      f"{skipped_unresolved} dropped (no outcome), window {days[0]} -> {days[-1]}")

# earliest cap skip = study floor (before that, cap-skip logging didn't exist)
first_skip_day = min(c["day"] for c in cands if not c["placed"])
print(f"first cap-skip logged: {first_skip_day} (study floor)")
cands = [c for c in cands if c["day"] >= first_skip_day]


def replay(cap, day_filter=None, breaker=-300.0):
    """Chronological replay. Returns (total_pts, per_day dict, admitted, blocked)."""
    per_day = defaultdict(float)
    admitted = blocked_cap = blocked_brk = 0
    by_day = defaultdict(list)
    for cd in cands:
        if day_filter and not day_filter(cd["day"]):
            continue
        by_day[cd["day"]].append(cd)
    for day, items in by_day.items():
        open_pos = []   # admitted trades still open
        closedq = []    # (close_time, usd)
        for t in sorted(items, key=lambda x: x["et"]):
            # realized so far today (close-time realization, incl comm)
            realized = sum(u for ct, u in closedq if ct <= t["et"])
            open_pos = [p for p in open_pos if p["close"] > t["et"]]
            if realized <= breaker:
                blocked_brk += 1
                continue
            n_same = sum(1 for p in open_pos if p["is_long"] == t["is_long"])
            if n_same >= cap:
                blocked_cap += 1
                continue
            admitted += 1
            open_pos.append(t)
            usd = t["pnl"] * 5.0 - 1.0  # $1/RT comm
            closedq.append((t["close"], usd))
            per_day[day] += usd
    total = sum(per_day.values())
    return total, per_day, admitted, blocked_cap, blocked_brk


V16 = lambda d: d >= "2026-05-18"
WEEK = lambda d: "2026-06-01" <= d <= "2026-06-05"

print(f"\n{'cap':>4}{'FULL $':>10}{'postV16 $':>11}{'week $':>9}{'admit':>7}{'capX':>6}{'brkX':>6}"
      f"{'worst day':>16}{'best day':>16}")
for cap in (1, 2, 3, 4, 5, 99):
    tot, pd, adm, bc, bb = replay(cap)
    t16, pd16, *_ = replay(cap, V16)
    tw, pdw, *_ = replay(cap, WEEK)
    if pd:
        wd = min(pd.items(), key=lambda x: x[1])
        bd = max(pd.items(), key=lambda x: x[1])
        wd_s = f"{wd[0][5:]} {wd[1]:+.0f}"
        bd_s = f"{bd[0][5:]} {bd[1]:+.0f}"
    else:
        wd_s = bd_s = "-"
    print(f"{cap:>4}{tot:>+10.0f}{t16:>+11.0f}{tw:>+9.0f}{adm:>7}{bc:>6}{bb:>6}{wd_s:>16}{bd_s:>16}")

# validation: cap=3 replay vs actually-placed sim set (same window)
tot3, pd3, adm3, *_ = replay(3)
placed_sim = defaultdict(float)
n_placed = 0
for cd in cands:
    if cd["placed"]:
        placed_sim[cd["day"]] += cd["pnl"] * 5.0 - 1.0
        n_placed += 1
actual_total = sum(placed_sim.values())
print(f"\nVALIDATION (Gate 2): cap=3 replay admitted {adm3} vs actually placed {n_placed}; "
      f"replay ${tot3:+.0f} vs actual-placed-sim ${actual_total:+.0f} "
      f"({(tot3/actual_total*100 if actual_total else 0):.0f}%)")

# per-day diffs > $50 for cap=3 vs actual (model error inspection)
print("days where cap=3 replay differs from actual placed-sim by > $50:")
for d in sorted(set(list(pd3.keys()) + list(placed_sim.keys()))):
    diff = pd3.get(d, 0) - placed_sim.get(d, 0)
    if abs(diff) > 50:
        print(f"  {d}: replay {pd3.get(d,0):+.0f} vs actual {placed_sim.get(d,0):+.0f} (diff {diff:+.0f})")
