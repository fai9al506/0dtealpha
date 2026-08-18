# -*- coding: utf-8 -*-
import os, json, psycopg2
c=psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit=True; cur=c.cursor()
cur.execute("""SELECT rto.setup_log_id, rto.state, to_char(sl.ts AT TIME ZONE 'America/New_York','HH24:MI') t, sl.setup_name, sl.direction
   FROM real_trade_orders rto JOIN setup_log sl ON sl.id=rto.setup_log_id
   WHERE date(sl.ts AT TIME ZONE 'America/New_York')='2026-06-24' ORDER BY sl.ts""")
rows=cur.fetchall(); c.close()
def pnl(entry,ex,islong,q): 
    if entry is None or ex is None: return None
    return ((ex-entry) if islong else (entry-ex))*q*5
pre_tot=post_tot=0; acc={}
print(f"{'lid':>5} {'t':>6} {'setup':<14}{'dir':<7}{'acct':>9}{'entry':>9}{'pre_exit':>9}{'post_exit':>10}{'pre$':>8}{'post$':>8}")
for lid,st,t,nm,dr in rows:
    s=st if isinstance(st,dict) else json.loads(st)
    il=dr in ('long','bullish'); q=s.get('quantity') or 1; en=s.get('fill_price')
    pre=s.get('close_fill_price_pre_fifo_reconcile'); post=s.get('close_fill_price')
    if pre is None: pre=post  # not reconciled = unchanged
    pp=pnl(en,pre,il,q); qp=pnl(en,post,il,q)
    a=s.get('account_id','?')[-4:]
    if pp is not None: pre_tot+=pp
    if qp is not None: post_tot+=qp; acc[a]=acc.get(a,0)+qp
    mark=' <== #4329' if lid==4329 else ''
    print(f"{lid:>5} {t:>6} {str(nm):<14}{str(dr):<7}{a:>9}{en or 0:>9.2f}{(pre or 0):>9.2f}{(post or 0):>10.2f}{(pp or 0):>+8.0f}{(qp or 0):>+8.0f}{mark}")
print(f"\nTOTAL  pre-FIFO (what each signal's TRAIL did): ${pre_tot:+.1f}")
print(f"TOTAL  post-FIFO (broker-matched truth):        ${post_tot:+.1f}")
print(f"per-account post-FIFO: {acc}")
print(f"\n>> CONSERVATION: pre and post totals {'MATCH' if abs(pre_tot-post_tot)<2 else 'DIFFER by $%.1f'%(pre_tot-post_tot)} (day-$ is the same; only per-lid attribution shuffles)")
