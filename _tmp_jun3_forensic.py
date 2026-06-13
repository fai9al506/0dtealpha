"""June 3 2026 forensic: what did TSRT actually trade, in order, and where did -$300 come from?"""
import os, json
import psycopg2
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); cur = c.cursor()

# real trades today
cur.execute("""
    SELECT r.setup_log_id, r.state, l.setup_name, l.direction, l.ts, l.spot, l.grade,
           l.outcome_result, l.outcome_pnl, l.paradigm
    FROM real_trade_orders r JOIN setup_log l ON l.id = r.setup_log_id
    WHERE l.ts >= '2026-06-03 13:00+00' AND l.ts < '2026-06-04 01:00+00'
    ORDER BY l.ts
""")
rows = cur.fetchall()
print(f"=== REAL TRADES Jun 3: {len(rows)} ===")
tot = 0.0
for lid, state, name, d, ts, spot, grade, res, pnl, para in rows:
    st = state if isinstance(state, dict) else json.loads(state or "{}")
    t = ts.astimezone(ET).strftime("%H:%M")
    fill = st.get("fill_price"); close_p = st.get("close_fill_price")
    qty = st.get("qty") or st.get("total_qty")
    closer = st.get("close_reason")
    rpnl = None
    if fill and close_p:
        mult = 5.0  # MES $/pt
        sign = 1 if (d or "").upper() in ("LONG","BULLISH","BUY") else -1
        rpnl = (float(close_p) - float(fill)) * sign * mult * float(qty or 1)
        tot += rpnl
    print(f"  lid {lid}  {t} ET  {name:15s} {d:5s} g={grade} para={para} spot={spot} | fill={fill} close={close_p} reason={closer} | broker ${rpnl if rpnl is None else round(rpnl,2)} | portal {res} {pnl}")
print(f"\nsum of per-lid broker P&L (pre-commission): ${tot:.2f}")

# any skipped due to loss cap? check skip reasons today
cur.execute("""
    SELECT setup_name, direction, ts, real_trade_skip_reason, outcome_result, outcome_pnl, grade, paradigm
    FROM setup_log
    WHERE ts >= '2026-06-03 13:00+00' AND ts < '2026-06-04 01:00+00'
      AND real_trade_skip_reason IS NOT NULL
    ORDER BY ts
""")
skips = cur.fetchall()
print(f"\n=== SKIPPED signals: {len(skips)} ===")
from collections import Counter
cnt = Counter(s[3] for s in skips)
print(dict(cnt))
# show the post-cap skips with their portal outcomes (what we missed)
for name, d, ts, reason, res, pnl, grade, para in skips:
    if "loss" in (reason or "").lower() or "cap" in (reason or "").lower() or "breaker" in (reason or "").lower():
        t = ts.astimezone(ET).strftime("%H:%M")
        print(f"  {t} {name:15s} {d:5s} g={grade} para={para} -> {reason} | portal would-be: {res} {pnl}")
c.close()
