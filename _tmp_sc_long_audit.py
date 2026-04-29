"""SC LONG audit — why aren't they firing on real account in bullish regime?"""
import psycopg2
from datetime import time as dtime, date

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

# All SC LONGS Apr 17+ with full V13 context
cur.execute("""
  SELECT id, ts AT TIME ZONE 'America/New_York' as t,
         DATE(ts AT TIME ZONE 'America/New_York') as d,
         grade, paradigm, spot, vix, overvix, greek_alignment,
         v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side,
         outcome_result, outcome_pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND ts >= '2026-04-17' AND outcome_result IS NOT NULL ORDER BY ts
""")
longs = cur.fetchall()

print(f"=== SC LONGS V13 era (Apr 17-28): {len(longs)} total ===\n")

# Replicate _passes_live_filter for SC long
def v13_long_pass(t):
    lid, ts, d, grade, par, spot, vix, ov, align, gex, dd, cliff, peak, res, pnl = t
    t_only = ts.time()
    # Time blocks
    if dtime(14, 30) <= t_only < dtime(15, 0): return (False, "14:30-15:00 dead zone")
    if t_only >= dtime(15, 30): return (False, ">=15:30")
    # Gap filter
    # (skip — depends on _daily_gap_pts at runtime)
    # Long-side gates
    if par == "SIDIAL-EXTREME": return (False, "SIDIAL-EXTREME")
    if align is not None and align < 2: return (False, f"align={align}<2")
    # SC long vanna: cliff=A + peak=B = block
    if cliff == 'A' and peak == 'B': return (False, "cliff=A peak=B")
    # SC longs exempt from VIX gate (per code)
    return (True, "PASSED")

# Per-day breakdown
print(f"{'ID':<6}{'Time':<7}{'Grade':<6}{'Para':<13}{'Align':>6}{'V13 verdict':<25}{'Result':<9}{'PnL':>6}")
print("-"*80)

passed_count = 0
passed_pnl = 0
blocked_align = 0
blocked_other = 0
day_summary = {}

for t in longs:
    lid, ts, d, grade, par, spot, vix, ov, align, gex, dd, cliff, peak, res, pnl = t
    ok, reason = v13_long_pass(t)
    pnl_v = float(pnl) if pnl else 0
    flag = "PASSED" if ok else f"BLK: {reason}"
    if ok:
        passed_count += 1
        passed_pnl += pnl_v
    elif "align" in reason:
        blocked_align += 1
    else:
        blocked_other += 1

    if d not in day_summary:
        day_summary[d] = {"total": 0, "passed": 0, "pnl_passed": 0, "pnl_total": 0}
    day_summary[d]["total"] += 1
    day_summary[d]["pnl_total"] += pnl_v
    if ok:
        day_summary[d]["passed"] += 1
        day_summary[d]["pnl_passed"] += pnl_v

    print(f"#{lid:<5}{ts.strftime('%H:%M'):<7}{grade or '?':<6}{par[:12] if par else '?':<13}{align if align is not None else '?':>+6}  {flag:<25}{res:<9}{pnl_v:+5.1f}")

print(f"\nTotal V13 era SC longs: {len(longs)}")
print(f"  Passed V13: {passed_count} (PnL ${passed_pnl*5:+.0f})")
print(f"  Blocked by align<2: {blocked_align}")
print(f"  Blocked by other: {blocked_other}")

# Per-day
print(f"\n=== Per-day SC LONG summary ===")
print(f"{'Date':<12}{'Total':>7}{'Passed':>8}{'Total PnL':>12}{'Passed PnL':>12}")
for d in sorted(day_summary.keys()):
    s = day_summary[d]
    total_d = f"${s['pnl_total']*5:+.0f}"
    passed_d = f"${s['pnl_passed']*5:+.0f}"
    print(f"  {d}  {s['total']:>5}  {s['passed']:>6}  {total_d:>10}  {passed_d:>10}")

# Did SC longs reach real account?
print(f"\n=== SC LONGS on real account V13 era ===")
cur.execute("""
  SELECT setup_log_id, state->>'direction', state->>'fill_price',
         state->>'stop_fill_price', state->>'close_reason',
         created_at AT TIME ZONE 'America/New_York'
  FROM real_trade_orders
  WHERE state->>'setup_name' = 'Skew Charm' AND state->>'direction' = 'long'
    AND created_at >= '2026-04-17'
""")
real_longs = cur.fetchall()
print(f"SC LONG real placements: {len(real_longs)}")
for r in real_longs:
    print(f"  #{r[0]} {r[5].strftime('%m-%d %H:%M')} fill={r[2]} exit={r[3]} reason={r[4]}")

# Check alignment distribution for V13-era SC longs
print(f"\n=== Greek alignment distribution for V13 era SC longs ===")
align_counts = {}
for t in longs:
    a = t[8]
    if a not in align_counts: align_counts[a] = []
    align_counts[a].append(t)
for a in sorted(align_counts.keys(), key=lambda x: x or -99):
    sub = align_counts[a]
    n = len(sub)
    w = sum(1 for t in sub if t[13]=="WIN")
    l = sum(1 for t in sub if t[13]=="LOSS")
    pnl = sum(float(t[14]) if t[14] else 0 for t in sub)
    print(f"  align={a}: {n}t  W={w} L={l}  PnL={pnl:+.1f}pt  (V13 {'PASSES' if a is not None and a >= 2 else 'BLOCKS'})")

# What about SC longs with align<2 historical performance? Are we losing $$ by blocking them?
print(f"\n=== SC longs align<2 (V13 BLOCKS) — full history Mar 1+ ===")
cur.execute("""
  SELECT greek_alignment, COUNT(*),
         SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) w,
         SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) l,
         ROUND(SUM(outcome_pnl)::numeric, 1) pnl
  FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
    AND ts >= '2026-03-01' AND outcome_result IS NOT NULL
  GROUP BY greek_alignment ORDER BY greek_alignment
""")
print(f"{'Align':>6}{'Trades':>8}{'Wins':>6}{'Losses':>8}{'WR':>7}{'PnL':>10}{'V13':>8}")
for r in cur.fetchall():
    a, n, w, l, pnl = r
    wr = w/(w+l)*100 if w+l else 0
    v13 = "PASS" if a is not None and a >= 2 else "BLOCK"
    print(f"  {str(a):>6}{n:>8}{w:>6}{l:>8}{wr:>6.1f}%{pnl:>8.1f}pt   {v13}")

# Dig: what regime were SC longs firing in V13 era?
print(f"\n=== SC LONG fire context per bullish day ===")
for tgt_d in (date(2026,4,22), date(2026,4,24), date(2026,4,28)):
    cur.execute("""
      SELECT id, ts AT TIME ZONE 'America/New_York' as t, grade, paradigm, spot,
             greek_alignment, vanna_cliff_side, vanna_peak_side,
             outcome_result, outcome_pnl
      FROM setup_log WHERE setup_name='Skew Charm' AND direction='long'
        AND DATE(ts AT TIME ZONE 'America/New_York') = %s AND outcome_result IS NOT NULL
      ORDER BY ts
    """, (tgt_d,))
    rows = cur.fetchall()
    if not rows: continue
    print(f"\n  {tgt_d} ({len(rows)} SC longs fired):")
    for r in rows:
        v13_ok = r[5] is not None and r[5] >= 2 and not (r[6] == 'A' and r[7] == 'B')
        print(f"    #{r[0]} {r[1].strftime('%H:%M')} {r[2] or '?'} {r[3] or '?':12} spot={float(r[4]):.1f} align={r[5]:+d} cliff={r[6]}/{r[7]}  V13={'PASS' if v13_ok else 'BLOCK'}  {r[8]} {r[9]}")
