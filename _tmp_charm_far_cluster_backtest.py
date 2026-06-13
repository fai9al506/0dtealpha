"""Backtest user hypothesis (2026-06-03):
When the dominant charm bars sit FAR BELOW spot (no big bars near spot),
(1) DRAG: price gets pulled down toward the cluster intraday
(2) SUPPORT: the cluster holds — price touches but does not breach, bounces

Signal at ~10:00 ET (charm needs the morning to build):
  K_dom  = strike with max |charm| (NULL-exp TODAY charm, SPX)
  near_max = max |charm| within +/-15 pts of spot
  FAR_BELOW = (spot - K_dom >= 25) AND (near_max < 0.4 * |V_dom|)
  FAR_ABOVE = symmetric
  NEUTRAL   = everything else

Outcomes from chain_snapshots spot path 10:00 -> 16:00 ET:
  drop      = spot@10:00 - min(spot after 10:00)
  reached   = min spot <= K_dom + 5  (for below clusters)
  breached  = min spot <  K_dom - 5
  bounce    = max spot AFTER touch-time minus touch low (pts recovered)

Validation Protocol: DB only, dates stated, no params changed mid-era for charm capture.
"""
import os
import psycopg2
from zoneinfo import ZoneInfo
from collections import defaultdict
from statistics import median

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

# --- charm snapshots near 10:00 ET per day ---
cur.execute("""
    SELECT ts_utc, strike, value, current_price
    FROM volland_exposure_points
    WHERE greek='charm' AND ticker='SPX' AND expiration_option IS NULL
    ORDER BY ts_utc
""")
by_day = defaultdict(lambda: defaultdict(list))
spot_at = {}
for ts, strike, val, cp in cur.fetchall():
    t = ts.astimezone(ET)
    by_day[t.date()][t].append((float(strike), float(val)))
    if cp is not None:
        spot_at[t] = float(cp)

# --- spot path per day from chain_snapshots ---
cur.execute("SELECT ts, spot FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY ts")
path = defaultdict(list)
for ts, spot in cur.fetchall():
    t = ts.astimezone(ET)
    path[t.date()].append((t, float(spot)))

results = []
for d, snaps in sorted(by_day.items()):
    times = sorted(snaps.keys())
    # nearest snapshot to 10:00, must be 9:50-10:15
    tgt = 10 * 60
    cands = [t for t in times if 9 * 60 + 50 <= t.hour * 60 + t.minute <= 10 * 60 + 15]
    if not cands:
        continue
    t0 = min(cands, key=lambda t: abs(t.hour * 60 + t.minute - tgt))
    pts = snaps[t0]
    spot0 = spot_at.get(t0)
    if spot0 is None or not pts:
        continue
    # staleness guard: need a real spot path with enough samples after 10:00
    p = [(t, s) for t, s in path[d] if t >= t0 and t.hour * 60 + t.minute <= 16 * 60]
    if len(p) < 50:
        continue
    # frozen-spot outage guard
    uniq = len(set(s for _, s in p))
    if uniq < 10:
        continue

    k_dom, v_dom = max(pts, key=lambda x: abs(x[1]))
    near_max = max((abs(v) for s, v in pts if abs(s - spot0) <= 15), default=0.0)
    dist = spot0 - k_dom  # + means cluster below spot

    if dist >= 25 and near_max < 0.4 * abs(v_dom):
        regime = "FAR_BELOW"
    elif dist <= -25 and near_max < 0.4 * abs(v_dom):
        regime = "FAR_ABOVE"
    else:
        regime = "NEUTRAL"

    lows = min(p, key=lambda x: x[1])
    highs = max(p, key=lambda x: x[1])
    close = p[-1][1]
    drop = spot0 - lows[1]
    rise = highs[1] - spot0

    rec = dict(d=d, regime=regime, spot0=spot0, k_dom=k_dom, v_dom=v_dom,
               near_max=near_max, dist=dist, drop=drop, rise=rise,
               close_chg=close - spot0)

    # support test (only meaningful when cluster below spot and price reached it)
    if dist > 0:
        touch = [(t, s) for t, s in p if s <= k_dom + 5]
        rec["reached"] = bool(touch)
        if touch:
            t_touch = touch[0][0]
            after = [(t, s) for t, s in p if t >= t_touch]
            low_after = min(after, key=lambda x: x[1])
            rec["breach_depth"] = k_dom - low_after[1]  # + = went below K_dom
            rec["breached"] = low_after[1] < k_dom - 5
            # bounce: from the lowest point, how far did it recover by EOD
            post_low = [(t, s) for t, s in after if t >= low_after[0]]
            rec["bounce"] = max(s for _, s in post_low) - low_after[1]
    results.append(rec)

print(f"days analyzed: {len(results)}  ({results[0]['d']} .. {results[-1]['d']})")
for reg in ("FAR_BELOW", "FAR_ABOVE", "NEUTRAL"):
    rs = [r for r in results if r["regime"] == reg]
    if not rs:
        continue
    print(f"\n--- {reg}: n={len(rs)} ---")
    print(f"  drop from 10:00 (pts): median {median(r['drop'] for r in rs):.1f}  mean {sum(r['drop'] for r in rs)/len(rs):.1f}")
    print(f"  rise from 10:00 (pts): median {median(r['rise'] for r in rs):.1f}  mean {sum(r['rise'] for r in rs)/len(rs):.1f}")
    print(f"  close chg: median {median(r['close_chg'] for r in rs):+.1f}  mean {sum(r['close_chg'] for r in rs)/len(rs):+.1f}")
    if reg == "FAR_BELOW":
        reach = [r for r in rs if r.get("reached")]
        print(f"  reached cluster (low <= K_dom+5): {len(reach)}/{len(rs)}")
        if reach:
            br = [r for r in reach if r["breached"]]
            print(f"  breached >5 pts below K_dom: {len(br)}/{len(reach)}")
            print(f"  breach depth median: {median(r['breach_depth'] for r in reach):+.1f} pts")
            print(f"  bounce from low: median {median(r['bounce'] for r in reach):.1f}  >=8pt: {sum(1 for r in reach if r['bounce']>=8)}/{len(reach)}  >=10pt: {sum(1 for r in reach if r['bounce']>=10)}/{len(reach)}")
        for r in rs:
            b = r.get("bounce")
            b_str = f"{b:.1f}" if b is not None else "-"
            print(f"    {r['d']}  spot0={r['spot0']:.0f} K={r['k_dom']:.0f} V={r['v_dom']/1e6:+.0f}M dist={r['dist']:+.0f} nearmax={r['near_max']/1e6:.0f}M | drop={r['drop']:.1f} reach={r.get('reached')} breach={r.get('breached','-')} bounce={b_str} close={r['close_chg']:+.1f}")

# SUPPORT test independent of "far": ALL days with a below-spot dominant strike that got touched
print("\n--- SUPPORT TEST (all days, dominant strike below spot, price reached it) ---")
sup = [r for r in results if r["dist"] > 5 and r.get("reached")]
print(f"n={len(sup)}")
if sup:
    br = [r for r in sup if r["breached"]]
    print(f"breached: {len(br)}/{len(sup)} ({100*len(br)/len(sup):.0f}%)")
    print(f"bounce >=8pt: {sum(1 for r in sup if r['bounce']>=8)}/{len(sup)}  >=10pt: {sum(1 for r in sup if r['bounce']>=10)}/{len(sup)}")
    print(f"median bounce: {median(r['bounce'] for r in sup):.1f} pts, median breach depth {median(r['breach_depth'] for r in sup):+.1f}")
c.close()
