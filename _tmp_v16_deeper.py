"""Deeper V16 vs TSRT analysis — clarify the findings."""
import os, psycopg2

c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()

# === A. VIX Div historicals — were they placed when env was TRUE? ===
# VIX Div was shipped to real on 2026-05-03, disabled 2026-05-18 (commit 980b136).
# Anything placed May 3-17 = legit. May 18+ would be leak.
print("=" * 70)
print("A. VIX DIV PLACEMENTS — historical (env true) or current leak?")
print("=" * 70)
cur.execute("""
    SELECT rto.setup_log_id, rto.created_at, sl.direction, sl.outcome_pnl,
           sl.outcome_result, rto.state->>'close_reason' as cr
    FROM real_trade_orders rto
    JOIN setup_log sl ON sl.id = rto.setup_log_id
    WHERE sl.setup_name = 'VIX Divergence'
    ORDER BY rto.created_at
""")
for sid, created, dir_, pnl, result, cr in cur.fetchall():
    et = created.strftime("%Y-%m-%d %H:%M ET")
    era = "leak?" if created.date() > __import__("datetime").date(2026, 5, 18) else "legit (env true at the time)"
    print(f"  lid={sid} {et} {dir_} pnl={pnl} result={result} reason={cr} -- {era}")

# === B. The 1 whitelist_reject signal — which one? ===
print()
print("=" * 70)
print("B. whitelist_reject signal (the 1)")
print("=" * 70)
cur.execute("""
    SELECT sl.id, sl.ts, sl.setup_name, sl.direction, sl.real_trade_skip_reason
    FROM setup_log sl
    WHERE sl.real_trade_skip_reason = 'whitelist_reject'
    ORDER BY sl.ts DESC LIMIT 5
""")
for sid, ts, name, dir_, reason in cur.fetchall():
    print(f"  lid={sid} {ts.strftime('%Y-%m-%d %H:%M')} {name} {dir_} -- {reason}")

# === C. DD SHORTS that PASSED notified=true → these may show as V16-passed in portal ===
print()
print("=" * 70)
print("C. DD SHORTS — did any pass live filter (notified=true) but get blocked by")
print("   _dd_short_block in dispatch? If yes, V16 portal may show as 'passed'.")
print("=" * 70)
cur.execute("""
    SELECT sl.id, sl.ts, sl.direction, sl.grade, sl.paradigm, sl.greek_alignment,
           sl.notified, sl.real_trade_skip_reason
    FROM setup_log sl
    LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
    WHERE sl.setup_name = 'DD Exhaustion'
      AND sl.direction NOT IN ('long','bullish')
      AND sl.ts::date >= '2026-05-06'
      AND rto.setup_log_id IS NULL
    ORDER BY sl.ts DESC
""")
dd_short_notified = []
for sid, ts, dir_, grade, para, align, notified, skip in cur.fetchall():
    if notified:
        dd_short_notified.append((sid, ts, dir_, grade, para, align, skip))
print(f"  Total DD short signals (last 14d, not placed): 62")
print(f"  Of those, notified=true (live filter passed): {len(dd_short_notified)}")
print(f"  These passed _passes_live_filter() but real_trader blocked at dispatch via _dd_short_block.")
print(f"  V16 portal must ALSO block them to match TSRT.")
print()
print("  Sample (first 10):")
for sid, ts, dir_, grade, para, align, skip in dd_short_notified[:10]:
    print(f"    lid={sid} {ts.strftime('%Y-%m-%d %H:%M')} {dir_} g={grade} p={para} align={align} skip={skip}")

# === D. Date split of "no skip_reason but not placed" — is it the S114 telemetry gap? ===
print()
print("=" * 70)
print("D. NO skip_reason but not placed — gated by setup_log.ts era?")
print("=" * 70)
cur.execute("""
    SELECT sl.ts::date as d, COUNT(*) as cnt
    FROM setup_log sl
    LEFT JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
    WHERE sl.ts::date >= '2026-05-06'
      AND sl.real_trade_skip_reason IS NULL
      AND rto.setup_log_id IS NULL
      AND sl.setup_name IN ('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion','VIX Divergence','GEX Long')
    GROUP BY sl.ts::date ORDER BY d
""")
for d, cnt in cur.fetchall():
    print(f"    {d}: {cnt} whitelist signals not placed, no skip_reason")

# === E. Check today's "daily_loss_limit" blocked signals — what would they have been ===
print()
print("=" * 70)
print("E. Today's daily_loss_limit blocks (the 7 from the breaker bug):")
print("=" * 70)
cur.execute("""
    SELECT sl.id, sl.ts, sl.setup_name, sl.direction, sl.grade, sl.paradigm, sl.outcome_pnl
    FROM setup_log sl
    WHERE sl.real_trade_skip_reason = 'daily_loss_limit'
      AND sl.ts::date = '2026-05-20'
    ORDER BY sl.ts
""")
for sid, ts, name, dir_, grade, para, pnl in cur.fetchall():
    print(f"    lid={sid} {ts.strftime('%H:%M')} {name} {dir_} g={grade} p={para} sim_pnl={pnl}")

cur.close(); c.close()
