# -*- coding: utf-8 -*-
"""Trade-by-trade on the worst broker days: broker round-trips aligned to placed signals."""
import os, json, psycopg2
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

def signals_for(day):
    cur.execute("""
      SELECT to_char(sl.ts AT TIME ZONE 'America/New_York','HH24:MI') et,
             sl.setup_name, sl.direction, sl.grade, sl.paradigm, sl.live_pass,
             ROUND(sl.outcome_pnl::numeric,1), sl.outcome_result
      FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
      WHERE date(sl.ts AT TIME ZONE 'America/New_York')=%s
      ORDER BY sl.ts""",(day,))
    return cur.fetchall()

def broker_for(day):
    cur.execute("SELECT trades FROM tsrt_daily_stmt WHERE day=%s",(day,))
    r=cur.fetchone()
    if not r: return []
    return r[0] if isinstance(r[0],list) else json.loads(r[0] or '[]')

for day in ('2026-06-11','2026-06-10'):
    sigs=signals_for(day); brk=broker_for(day)
    print("\n"+"="*100)
    print(f"  {day}   placed signals={len(sigs)}   broker round-trips={len(brk)}")
    print("="*100)
    print("  -- PLACED SIGNALS (portal) --")
    print(f"  {'et':<6}{'setup':<15}{'dir':<7}{'gr':<4}{'paradigm':<15}{'V16':<6}{'simPnL':>7}{'res':>5}")
    offsum=0; v16sum=0
    for et,sn,d,g,p,lp,pnl,res in sigs:
        tag='' if lp else ' *OFF'
        print(f"  {et:<6}{(sn or '')[:14]:<15}{(d or '')[:6]:<7}{(g or '')[:3]:<4}{(p or '')[:14]:<15}{str(bool(lp)):<6}{float(pnl or 0):>7.1f}{(res or '')[:4]:>5}{tag}")
        if lp: v16sum+=float(pnl or 0)
        else: offsum+=float(pnl or 0)
    print(f"  -> V16-pass sim sum={v16sum:+.1f}   OFF-filter sim sum={offsum:+.1f}")
    print("  -- BROKER ROUND-TRIPS (actual fills) --")
    print(f"  {'in':<6}{'out':<6}{'dir':<6}{'entry':>9}{'exit':>9}{'pts':>7}{'usd':>8}{'acct':>10}")
    busd=0
    for t in brk:
        busd+=t.get('usd',0)
        print(f"  {t.get('entry_et',''):<6}{t.get('exit_et',''):<6}{t.get('dir','')[:5]:<6}{t.get('entry',0):>9.2f}{t.get('exit',0):>9.2f}{t.get('pts',0):>7.1f}{t.get('usd',0):>8.1f}{str(t.get('account',''))[:9]:>10}")
    print(f"  -> broker gross ${busd:+.1f}")
conn.close()
