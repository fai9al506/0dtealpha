"""
BACKTEST: GEX Long v3 with the Volland-paradigm GATE REMOVED.

Question (user, 2026-06-02): even when Volland labels the regime NON-GEX (e.g.
BOFA-PURE, like today), the per-strike GEX structure can be a clean GEX Long.
Should the paradigm label be a hard blocker, or only a grading input?

Method:
 - Reuse the VALIDATED v3.1 harness functions (_features / _classify /
   _simulate_exit) from app/gex_long_v3.py — same machinery that produced the
   77%/+170p baseline. (Volland gamma TODAY + charm structure.)
 - Generate signals FROM SCRATCH across the volland-snapshot timeline (15-min
   cooldown, mirrors S191), NOT just from logged GEX Long rows. This is the only
   way to surface signals on non-GEX days, which never get logged today.
 - Apply the live v3 entry rules EXCEPT the paradigm gate:
       verdict in {A++,A,B}  AND  hour_et < 15
   plus the v3.2 confluence rule for the alignment leg:
       align >= 0  OR  paradigm in BULL_PARADIGMS
 - Recompute greek_alignment(long) exactly as main.py does:
       +1 charm>0, +1 vanna_all>0, +1 spot<=max_plus_gex
 - Simulate exit with the SAME params as v3.1 (SL14 / target=max(magnet,e+20) /
   trail act15 gap5) walking chain_snapshots spot to 16:00 ET.

Then split outcomes by paradigm bucket:
   GEX-*      = what the system ALREADY fires (sanity cross-check vs known v3 base)
   non-GEX    = the NEW trades removing the gate would admit (BOFA-PURE, AG-*, ...)
"""
import psycopg2
from collections import defaultdict
from app.gex_long_v3 import (_features, _classify, _simulate_exit,
                             BULL_PARADIGMS, SL_PTS, TARGET_FLOOR)

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
START = "2026-02-23"
END = "2026-06-02"
COOLDOWN_MIN = 15

conn = psycopg2.connect(DB)
cur = conn.cursor()

# Pull volland snapshots in window with paradigm + aggregatedCharm + spot.
cur.execute(f"""
    SELECT ts,
           (ts AT TIME ZONE 'America/New_York') AS t_et,
           (payload->'statistics'->>'paradigm') AS paradigm,
           (payload->'statistics'->>'aggregatedCharm') AS agg_charm,
           (payload->>'current_price') AS price
    FROM volland_snapshots
    WHERE ts::date BETWEEN '{START}' AND '{END}'
      AND payload->'statistics'->>'paradigm' IS NOT NULL
      AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '14:59'
    ORDER BY ts
""")
snaps = cur.fetchall()
print(f"volland snapshots in window (09:35-14:59 ET): {len(snaps)}")

def vanna_all_at(ts):
    """Total vanna (ALL expiration) near ts — sign drives the vanna alignment leg."""
    cur.execute("""SELECT ts_utc FROM volland_exposure_points
                   WHERE ts_utc BETWEEN %s - interval '5 min' AND %s
                     AND greek='vanna' AND expiration_option='ALL'
                   ORDER BY ts_utc DESC LIMIT 1""", (ts, ts))
    r = cur.fetchone()
    if not r:
        return None
    cur.execute("""SELECT COALESCE(SUM(value),0) FROM volland_exposure_points
                   WHERE ts_utc=%s AND greek='vanna' AND expiration_option='ALL'""", (r[0],))
    v = cur.fetchone()
    return float(v[0]) if v else None

def max_plus_gex_strike(f, spot):
    # strongest +GEX strike above spot (the magnet) — used for spot<=max_plus_gex leg
    return f.get('gex_magnet_strike')

signals = []
last_fire_by_day = {}

for ts, t_et, paradigm, agg_charm, price in snaps:
    if not price:
        continue
    spot = float(price)
    day = t_et.date()
    # 15-min cooldown per day
    last = last_fire_by_day.get(day)
    if last is not None and (t_et - last).total_seconds() < COOLDOWN_MIN * 60:
        continue

    try:
        f = _features(cur, ts, spot)
    except Exception:
        f = None
    verdict = _classify(f)
    if verdict not in ('A++', 'A', 'B'):
        continue
    if t_et.hour >= 15:
        continue

    # alignment (long): charm>0, vanna_all>0, spot<=max_plus_gex
    charm_v = None
    if agg_charm not in (None, ''):
        try:
            charm_v = float(str(agg_charm).replace('$', '').replace(',', ''))
        except Exception:
            charm_v = None
    vanna_v = vanna_all_at(ts)
    mpg = max_plus_gex_strike(f, spot)
    align = 0
    if charm_v is not None:
        align += 1 if charm_v > 0 else -1
    if vanna_v is not None:
        align += 1 if vanna_v > 0 else -1
    if mpg:
        align += 1 if (spot <= mpg) else -1

    is_bull_para = paradigm in BULL_PARADIGMS
    passes = (align >= 0) or is_bull_para
    if not passes:
        continue

    # simulate
    entry = spot
    magnet = f['gex_magnet_strike']
    target = max(magnet or 0, entry + TARGET_FLOOR)
    try:
        result, pnl, max_fav, reason = _simulate_exit(cur, ts, entry, target)
    except Exception:
        continue
    if result in ('NO_PATH',):
        continue

    last_fire_by_day[day] = t_et
    signals.append({
        'day': day, 't_et': t_et, 'paradigm': paradigm, 'verdict': verdict,
        'align': align, 'is_gex_para': 'GEX' in (paradigm or '').upper(),
        'is_bull_para': is_bull_para, 'result': result, 'pnl': pnl,
        'max_fav': max_fav, 'reason': reason, 'spot': spot,
    })

print(f"total generated signals (gate removed): {len(signals)}\n")

def stats(sigs, label):
    if not sigs:
        print(f"{label:32s}  n=0")
        return
    n = len(sigs)
    wins = sum(1 for s in sigs if s['result'] == 'WIN')
    pnl = sum(s['pnl'] for s in sigs)
    wr = wins / n * 100
    # max drawdown over the equity curve (chronological)
    ss = sorted(sigs, key=lambda s: s['t_et'])
    eq = 0; peak = 0; mdd = 0
    for s in ss:
        eq += s['pnl']; peak = max(peak, eq); mdd = min(mdd, eq - peak)
    gross_w = sum(s['pnl'] for s in sigs if s['pnl'] > 0)
    gross_l = sum(s['pnl'] for s in sigs if s['pnl'] < 0)
    pf = (gross_w / abs(gross_l)) if gross_l < 0 else 99
    print(f"{label:32s}  n={n:3d}  WR={wr:4.0f}%  PnL={pnl:+7.1f}p  "
          f"avg={pnl/n:+5.1f}  PF={pf:4.2f}  MaxDD={mdd:6.1f}p  (~${pnl*5:+,.0f}@1MES)")

print("=" * 100)
print("SPLIT BY PARADIGM BUCKET")
print("=" * 100)
gex = [s for s in signals if s['is_gex_para']]
nongex = [s for s in signals if not s['is_gex_para']]
stats(gex,    "GEX-* (system already fires)")
stats(nongex, "non-GEX (gate-removal ADDS)")
print("-" * 100)
# non-GEX detail by paradigm
byp = defaultdict(list)
for s in nongex:
    byp[s['paradigm']].append(s)
for p in sorted(byp, key=lambda x: -len(byp[x])):
    stats(byp[p], f"   {p}")
print("-" * 100)
stats([s for s in nongex if s['is_bull_para']], "non-GEX via BULL_PARADIGMS route")
stats([s for s in nongex if not s['is_bull_para']], "non-GEX via align>=0 route only")
print("=" * 100)
print("COMBINED (current GEX-* + added non-GEX) vs CURRENT (GEX-* only)")
stats(gex, "CURRENT  (gate ON, GEX-* only)")
stats(signals, "PROPOSED (gate OFF, all paradigms)")

# Today's signals specifically
print("=" * 100)
print("TODAY 2026-06-02 generated signals:")
today = [s for s in signals if str(s['day']) == '2026-06-02']
for s in today:
    print(f"   {str(s['t_et'])[11:19]} {s['paradigm']:12s} {s['verdict']:3s} "
          f"align={s['align']:+d} bull_para={s['is_bull_para']} -> {s['result']:7s} "
          f"{s['pnl']:+.1f}p (mfe {s['max_fav']:+.1f}) [{s['reason']}]")
if not today:
    print("   (none generated — check cooldown/verdict)")

conn.close()
