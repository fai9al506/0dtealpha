import os, psycopg, json
from datetime import datetime
from zoneinfo import ZoneInfo
ET = ZoneInfo("America/New_York")
today = datetime.now(ET).date()
conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=True)
cur = conn.cursor()
cur.execute("""
  SELECT s.id, s.ts, s.setup_name, s.direction, s.grade, s.paradigm, r.state
  FROM setup_log s JOIN real_trade_orders r ON r.setup_log_id = s.id
  WHERE s.ts::date = %s ORDER BY s.ts
""", (today,))
rows = cur.fetchall()
print(f"=== REAL TSRT trades today: {len(rows)} (bot's-own-fills P&L, MES $5/pt, -$1/RT comm) ===")
gross=0.0; net=0.0; wins=0; losses=0
for r in rows:
    lid, ts, name, dirn, grade, para, state = r
    t = ts.astimezone(ET).strftime("%H:%M")
    st = state if isinstance(state, dict) else json.loads(state)
    entry = st.get("fill_price")
    cr = st.get("close_reason","")
    exit_ = st.get("close_fill_price") if st.get("close_fill_price") is not None else st.get("stop_fill_price")
    q = st.get("quantity",1) or 1
    short = str(dirn).lower() in ("short","bearish")
    if entry is None or exit_ is None:
        print(f"  lid {lid} {t} {name:<16} {str(dirn):<7} entry={entry} exit={exit_} -- INCOMPLETE  reason={cr}")
        continue
    pts = (entry - exit_) if short else (exit_ - entry)
    usd = pts * 5 * q
    gross += usd; net += usd - 1*q
    if usd>0: wins+=1
    else: losses+=1
    print(f"  lid {lid} {t} {name:<16} {str(dirn):<7} {str(grade):<3} entry={entry} exit={exit_} {pts:+.2f}pt ${usd:+.2f}  {cr}")
print(f"\n  GROSS ${gross:+.2f} | NET (after $1/RT) ${net:+.2f} | {wins}W {losses}L of {len(rows)}")

# breaker block timing
print("\n=== daily_loss_limit blocks today (when breaker was active) ===")
cur.execute("""SELECT ts, setup_name, direction FROM setup_log
  WHERE ts::date=%s AND real_trade_skip_reason='daily_loss_limit' ORDER BY ts""",(today,))
for r in cur.fetchall():
    print(f"  {r[0].astimezone(ET).strftime('%H:%M')} {r[1]} {r[2]}")
