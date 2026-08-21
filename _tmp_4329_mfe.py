# -*- coding: utf-8 -*-
import os, json, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
# trade facts
cur.execute("""SELECT to_char(ts AT TIME ZONE 'America/New_York','HH24:MI:SS'), spot, outcome_max_profit, outcome_max_loss,
   outcome_elapsed_min, round(outcome_pnl::numeric,1), round(mes_sim_outcome_pnl::numeric,1), round(mes_sim_max_fav::numeric,1)
   FROM setup_log WHERE id=4329""")
r=cur.fetchone()
print(f"#4329 entry {r[0]}  SPX_entry={r[1]}  SPX_MFE={r[2]}  SPX_MAE={r[3]}  dur={r[4]}min")
print(f"  portal(SPX) pnl={r[5]}  | mes_sim pnl={r[6]}  mes_sim_MFE={r[7]}")
cur.execute("SELECT state FROM real_trade_orders WHERE setup_log_id=4329")
s=cur.fetchone()[0]; s=s if isinstance(s,dict) else json.loads(s)
print(f"  real: ES_entry_fill={s.get('fill_price')}  max_favorable(tracked)={s.get('max_favorable')}  close_fill={s.get('close_fill_price')}  reason={s.get('close_reason')}")
# ES range bars path 11:48 -> 12:40 (entry ES ~7476)
ent=s.get('fill_price')
cur.execute("""SELECT to_char(ts_end AT TIME ZONE 'America/New_York','HH24:MI'), high, low, close
   FROM vps_es_range_bars WHERE date(ts_end AT TIME ZONE 'America/New_York')='2026-06-24'
   AND (ts_end AT TIME ZONE 'America/New_York')::time BETWEEN '11:48' AND '12:45' ORDER BY ts_end""")
bars=cur.fetchall()
if bars:
    hi=max(b[1] for b in bars); lo=min(b[2] for b in bars)
    print(f"\nES range bars 11:48-12:45: {len(bars)} bars  ES high={hi} low={lo}")
    print(f"  ES MFE vs entry {ent}: +{hi-ent:.1f}   ES MAE: {lo-ent:.1f}")
    print("  bars(time/high/low/close):")
    for b in bars[:14]: print(f"    {b[0]}  H{b[1]} L{b[2]} C{b[3]}")
else:
    print("\nNO vps_es_range_bars in window — check source")
# basis at entry vs at SPX-high time
cur.execute("""SELECT to_char(ts AT TIME ZONE 'America/New_York','HH24:MI'), spot FROM chain_snapshots
   WHERE date(ts AT TIME ZONE 'America/New_York')='2026-06-24' AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '11:48' AND '12:00' ORDER BY ts""")
print("\nSPX 11:48-12:00:", [(t,float(sp)) for t,sp in cur.fetchall()])
c.close()
