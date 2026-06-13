"""Trade sim: LONG on first touch of dominant below-spot charm bar.

Rule (evaluated live, charm structure evolves intraday):
  - At each spot tick (10:00-15:30 ET), use latest charm snapshot (NULL-exp, SPX)
  - S_sup = below-spot strike (strike <= spot-5 at signal-check) with max |charm|
  - require |charm(S_sup)| >= 0.6 * max |charm| overall (it IS the dominant structure)
    and |charm(S_sup)| >= 10M absolute
  - entry: spot <= S_sup + 3  -> LONG at spot
  - exit: T +10 / SL -8, EXPIRED at 16:00 close
  - one trade per day (first touch)

Compare: same walk with placebo rule = LONG when spot is 25+ pts below the 10:00
spot (naive dip-buy, no charm), same T/S, first per day, only on days with NO
charm trade (separation) -- plus an all-days naive variant for context.
"""
import os
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
from statistics import median

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

scur = c.cursor(name="charm_stream")  # server-side cursor, streams instead of one giant fetch
scur.itersize = 50000
scur.execute("""
    SELECT ts_utc, strike, value
    FROM volland_exposure_points
    WHERE greek='charm' AND ticker='SPX' AND expiration_option IS NULL
      AND abs(value) >= 5e6
    ORDER BY ts_utc
""")
charm = defaultdict(lambda: defaultdict(list))
for ts, strike, val in scur:
    t = ts.astimezone(ET)
    charm[t.date()][t].append((float(strike), float(val)))
scur.close()

cur.execute("SELECT ts, spot FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY ts")
path = defaultdict(list)
for ts, spot in cur.fetchall():
    t = ts.astimezone(ET)
    path[t.date()].append((t, float(spot)))


def walk(p, i_entry, entry, tgt=10.0, sl=8.0):
    for t, s in p[i_entry + 1:]:
        if s <= entry - sl:
            return "LOSS", -sl
        if s >= entry + tgt:
            return "WIN", tgt
    return "EXPIRED", p[-1][1] - entry


trades, placebo, naive_all = [], [], []
for d, snaps in sorted(charm.items()):
    p = [(t, s) for t, s in path[d] if 10 * 60 <= t.hour * 60 + t.minute <= 16 * 60]
    if len(p) < 50 or len(set(s for _, s in p)) < 10:
        continue
    ctimes = sorted(snaps.keys())
    spot0 = p[0][1]

    # --- charm-support trade ---
    done = False
    ci = 0
    for i, (t, s) in enumerate(p):
        if done or t.hour * 60 + t.minute > 15 * 60 + 30:
            break
        while ci + 1 < len(ctimes) and ctimes[ci + 1] <= t:
            ci += 1
        if ctimes[ci] > t:
            continue
        pts = snaps[ctimes[ci]]
        if not pts:
            continue
        gmax = max(abs(v) for _, v in pts)
        below = [(st, v) for st, v in pts if st <= s]
        if not below:
            continue
        s_sup, v_sup = max(below, key=lambda x: abs(x[1]))
        if abs(v_sup) < max(0.6 * gmax, 10e6):
            continue
        if s_sup - 2 <= s <= s_sup + 3:
            res, pnl = walk(p, i, s)
            trades.append(dict(d=d, t=t, entry=s, s_sup=s_sup, v=v_sup, res=res, pnl=pnl))
            done = True

    # --- naive dip-buy placebo: long at -25 pts from 10:00 spot ---
    for i, (t, s) in enumerate(p):
        if t.hour * 60 + t.minute > 15 * 60 + 30:
            break
        if s <= spot0 - 25:
            res, pnl = walk(p, i, s)
            naive_all.append(dict(d=d, res=res, pnl=pnl))
            if not done:  # day without charm trade
                placebo.append(dict(d=d, res=res, pnl=pnl))
            break


def report(name, ts_):
    if not ts_:
        print(f"{name}: no trades")
        return
    w = sum(1 for x in ts_ if x["res"] == "WIN")
    l = sum(1 for x in ts_ if x["res"] == "LOSS")
    e = sum(1 for x in ts_ if x["res"] == "EXPIRED")
    tot = sum(x["pnl"] for x in ts_)
    print(f"{name}: n={len(ts_)}  W{w}/L{l}/E{e}  WR(W vs L)={100*w/max(w+l,1):.0f}%  total {tot:+.1f} pts  avg {tot/len(ts_):+.2f}")


print("=== CHARM-SUPPORT LONG (T10/S8) ===")
report("charm-support", trades)
for x in trades:
    print(f"  {x['d']} {x['t'].strftime('%H:%M')}  entry={x['entry']:.1f} sup={x['s_sup']:.0f} ({x['v']/1e6:+.0f}M)  {x['res']} {x['pnl']:+.1f}")

print("\n=== NAIVE -25pt DIP-BUY (all days, context) ===")
report("naive", naive_all)
c.close()
