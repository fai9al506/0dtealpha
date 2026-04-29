"""Trade-by-trade audit on bullish days for V13 SC shorts."""
import psycopg2
from datetime import time as dtime

conn = psycopg2.connect('postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway')
cur = conn.cursor()

cur.execute("""
  WITH daily AS (
    SELECT DATE(ts AT TIME ZONE 'America/New_York') d,
           (array_agg(spot ORDER BY ts ASC) FILTER (WHERE spot IS NOT NULL))[1] open_,
           (array_agg(spot ORDER BY ts DESC) FILTER (WHERE spot IS NOT NULL))[1] close_
    FROM chain_snapshots WHERE DATE(ts AT TIME ZONE 'America/New_York') >= '2026-03-01' GROUP BY 1
  )
  SELECT d, open_, close_, close_ - open_ net
  FROM daily WHERE close_ - open_ > 15 ORDER BY d
""")
bullish = cur.fetchall()

def v13_pass(t):
    lid, ts, spot, grade, par, gex, dd, cliff, peak, res, pnl = t
    if grade in ("C", "LOG"): return False
    if dtime(14, 30) <= ts.time() < dtime(15, 0): return False
    if ts.time() >= dtime(15, 30): return False
    if gex is not None and float(gex) >= 75: return False
    if dd is not None and float(dd) >= 3_000_000_000: return False
    if par == "GEX-LIS": return False
    if cliff == 'A' and peak == 'B': return False
    return True

print("=== TRADE-BY-TRADE on BULLISH DAYS ===\n")
total_act = total_cb = total_tr = total_b = 0.0

for d_obj, day_open, day_close, day_net in bullish:
    d_str = str(d_obj)
    cur.execute("""
      SELECT id, ts AT TIME ZONE 'America/New_York', spot, grade, paradigm,
             v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, outcome_result, outcome_pnl
      FROM setup_log WHERE setup_name='Skew Charm' AND direction='short'
        AND DATE(ts AT TIME ZONE 'America/New_York') = %s AND outcome_result IS NOT NULL ORDER BY ts
    """, (d_str,))
    trades = cur.fetchall()
    if not trades: continue

    print(f"--- {d_str} (BULLISH +{day_net:.0f}pt: {day_open:.0f} -> {day_close:.0f}) ---")
    a = c = tr = b = 0.0
    consec = 0; cb_p = False
    for t in trades:
        lid, ts, spot, grade, par, gex, dd, cliff, peak, res, pnl = t
        v13_ok = v13_pass(t)
        from_open = float(spot) - float(day_open)
        trend_blk = ts.time() >= dtime(11, 0) and from_open > 20
        cb_blk = cb_p
        pv = float(pnl) if pnl else 0
        if v13_ok:
            a += pv
            if not cb_blk: c += pv
            if not trend_blk: tr += pv
            if not cb_blk and not trend_blk: b += pv
            if not cb_blk:
                if res == "LOSS":
                    consec += 1
                    if consec >= 2: cb_p = True
                elif res == "WIN":
                    consec = 0
        marker = "V13-BLK"
        if v13_ok:
            if cb_blk and trend_blk: marker = "BOTH-BLK"
            elif cb_blk: marker = "CB-BLK"
            elif trend_blk: marker = "TREND-BLK"
            else: marker = "TAKEN"
        gex_s = f"{int(gex)}" if gex else "?"
        print(f"  #{lid} {ts.strftime('%H:%M')} spot={float(spot):.1f} ({from_open:+5.1f}) {grade or '?':3} {par or '?':12} GEX={gex_s:4} cliff={cliff or '-'}/{peak or '-'} | {marker:9} | {res:7} ${pv*5:+.0f}")
    print(f"  >> ACTUAL ${a*5:+.0f} | CB ${c*5:+.0f} | TREND ${tr*5:+.0f} | BOTH ${b*5:+.0f}\n")
    total_act += a; total_cb += c; total_tr += tr; total_b += b

print("=== AGGREGATE on BULLISH DAYS ===")
print(f"V13 actual:        ${total_act*5:+.0f}")
print(f"V13 + CB:          ${total_cb*5:+.0f} (saves {(total_cb-total_act)*5:+.0f})")
print(f"V13 + TREND >20:   ${total_tr*5:+.0f} (saves {(total_tr-total_act)*5:+.0f})")
print(f"V13 + BOTH:        ${total_b*5:+.0f} (saves {(total_b-total_act)*5:+.0f})")

print("\n=== TREND threshold sweep on bullish days only ===")
for thresh in [10, 15, 20, 25, 30]:
    s = 0.0
    for d_obj, day_open, day_close, day_net in bullish:
        cur.execute("""
          SELECT id, ts AT TIME ZONE 'America/New_York', spot, grade, paradigm,
                 v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, outcome_result, outcome_pnl
          FROM setup_log WHERE setup_name='Skew Charm' AND direction='short'
            AND DATE(ts AT TIME ZONE 'America/New_York') = %s AND outcome_result IS NOT NULL ORDER BY ts
        """, (str(d_obj),))
        for t in cur.fetchall():
            if not v13_pass(t): continue
            lid, ts, spot, grade, par, gex, dd, cliff, peak, res, pnl = t
            from_open = float(spot) - float(day_open)
            if not (ts.time() >= dtime(11, 0) and from_open > thresh):
                s += float(pnl) if pnl else 0
    print(f"  thresh > {thresh}pt: filtered total ${s*5:+.0f} (saves {(s-total_act)*5:+.0f})")

print("\n=== Verify TREND filter on CHOP days (does it hurt?) ===")
cur.execute("""
  WITH daily AS (
    SELECT DATE(ts AT TIME ZONE 'America/New_York') d,
           (array_agg(spot ORDER BY ts ASC) FILTER (WHERE spot IS NOT NULL))[1] open_,
           (array_agg(spot ORDER BY ts DESC) FILTER (WHERE spot IS NOT NULL))[1] close_
    FROM chain_snapshots WHERE DATE(ts AT TIME ZONE 'America/New_York') >= '2026-03-01' GROUP BY 1
  )
  SELECT d, open_ FROM daily WHERE ABS(close_ - open_) <= 15 ORDER BY d
""")
chop = cur.fetchall()

for thresh in [15, 20, 25]:
    s_act = s_filt = 0.0
    for d_obj, day_open in chop:
        cur.execute("""
          SELECT id, ts AT TIME ZONE 'America/New_York', spot, grade, paradigm,
                 v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, outcome_result, outcome_pnl
          FROM setup_log WHERE setup_name='Skew Charm' AND direction='short'
            AND DATE(ts AT TIME ZONE 'America/New_York') = %s AND outcome_result IS NOT NULL ORDER BY ts
        """, (str(d_obj),))
        for t in cur.fetchall():
            if not v13_pass(t): continue
            lid, ts, spot, grade, par, gex, dd, cliff, peak, res, pnl = t
            pv = float(pnl) if pnl else 0
            s_act += pv
            from_open = float(spot) - float(day_open)
            if not (ts.time() >= dtime(11, 0) and from_open > thresh):
                s_filt += pv
    print(f"  CHOP thresh>{thresh}: V13=${s_act*5:.0f}  Filtered=${s_filt*5:+.0f} (delta {(s_filt-s_act)*5:+.0f})")
