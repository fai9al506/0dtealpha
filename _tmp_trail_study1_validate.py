# -*- coding: utf-8 -*-
"""Phase 1: MES-bar coverage + build trade universe + validate mes_walk vs broker.
Reuses the production mes_walk() so the optimizer uses the SAME validated simulator."""
import os, sys, json, psycopg2
from collections import defaultdict
from datetime import timedelta
sys.path.insert(0, os.path.abspath("."))
from app.mes_sim_backfill import mes_walk, _DEFAULT_PARAMS

conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

# 1) MES bar coverage
cur.execute("""SELECT MIN(date(ts_start AT TIME ZONE 'America/New_York')),
                      MAX(date(ts_start AT TIME ZONE 'America/New_York')),
                      COUNT(*) FROM vps_es_range_bars WHERE range_pts=5""")
mn,mx,nb=cur.fetchone()
print(f"vps_es_range_bars(5pt): {nb} bars, {mn} -> {mx}")

# 2) Load ALL 5pt bars in window into memory, keyed by ET date
cur.execute("""SELECT ts_start, ts_end, bar_open, bar_high, bar_low, bar_close
               FROM vps_es_range_bars WHERE range_pts=5 ORDER BY ts_start""")
allbars=[(r[0],r[1],float(r[2]),float(r[3]),float(r[4]),float(r[5])) for r in cur.fetchall() if r[2] is not None]
print(f"loaded {len(allbars)} bars into memory")

def bars_after(ts, max_min):
    end=ts+timedelta(minutes=max_min)
    return [b for b in allbars if b[0]>=ts and b[0]<=end]

# 3) Universe: trailing setups, with entry ES price + live params + chain outcome.
#    entry ES = signal_es_price (real_trade_orders) else fill_price else first bar open after signal.
TRAIL_SETUPS=('Skew Charm','AG Short','DD Exhaustion','GEX Long','VIX Divergence','ES Absorption')
cur.execute(f"""
  SELECT sl.id, sl.ts, sl.setup_name, sl.direction, sl.grade, sl.spot,
         sl.outcome_pnl, sl.outcome_result, sl.outcome_max_profit, sl.outcome_elapsed_min,
         sl.trail_sl, sl.trail_activation, sl.trail_gap, sl.live_pass, sl.vix,
         (rto.state->>'signal_es_price')::float, (rto.state->>'fill_price')::float
  FROM setup_log sl
  LEFT JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE sl.setup_name = ANY(%s)
    AND sl.outcome_result IS NOT NULL
    AND date(sl.ts AT TIME ZONE 'America/New_York') >= %s
  ORDER BY sl.ts
""",(list(TRAIL_SETUPS), str(mn)))
rows=cur.fetchall()
print(f"universe rows (trailing setups, MES-era, resolved): {len(rows)}")

univ=[]
for (sid,ts,sn,d,gr,spot,opnl,ores,omfe,oem,tsl,tact,tgap,lp,vix,sig_es,fill) in rows:
    is_long=(d or '').lower() in ('long','bullish')
    entry=None
    if sig_es and sig_es>0: entry=float(sig_es)
    elif fill and fill>0: entry=float(fill)
    else:
        b=bars_after(ts,10)
        if b: entry=b[0][2]
    if entry is None: continue
    univ.append(dict(id=sid,ts=ts,setup=sn,is_long=is_long,grade=gr,entry=entry,
                     chain_pnl=float(opnl or 0),chain_res=ores,chain_mfe=float(omfe or 0),
                     elapsed=int(oem) if oem else 90,
                     tsl=tsl,tact=tact,tgap=tgap,live_pass=bool(lp),vix=float(vix or 0)))
print(f"universe with entry price: {len(univ)}")

# 4) Baseline mes_walk with LIVE params -> compare to broker (day-level) for validation
def live_params(u):
    df=_DEFAULT_PARAMS.get(u['setup'],{"sl":14,"be_trigger":None,"be_lock":0,"trail_act":10,"trail_gap":5})
    return dict(sl=float(u['tsl']) if u['tsl'] is not None else df['sl'],
                be_trigger=df['be_trigger'], be_lock=df['be_lock'],
                trail_act=float(u['tact']) if u['tact'] is not None else df['trail_act'],
                trail_gap=float(u['tgap']) if u['tgap'] is not None else df['trail_gap'])

for u in univ:
    p=live_params(u)
    mm=max(u['elapsed']+30,60)
    r=mes_walk(bars_after(u['ts'],mm),u['entry'],u['is_long'],p['sl'],p['be_trigger'],p['be_lock'],p['trail_act'],p['trail_gap'],mm)
    u['mes_pnl']=round(r['pnl'],2); u['mes_mfe']=round(r['mfe'],2); u['mes_reason']=r['reason']

# day-level validation vs broker gross (placed trades only)
cur.execute("SELECT day, trades FROM tsrt_daily_stmt ORDER BY day")
brk={}
for day,tr in cur.fetchall():
    js=tr if isinstance(tr,list) else json.loads(tr or '[]')
    brk[str(day)]=sum(t.get('pts',0) for t in js)
cur.execute("""SELECT setup_log_id FROM real_trade_orders""")
placed=set(r[0] for r in cur.fetchall())

from datetime import timezone
ET_bars=defaultdict(lambda:[0.0,0.0,0.0])  # date -> [chain, mes, n]
for u in univ:
    if u['id'] not in placed: continue
    d=(u['ts'].astimezone()).date()  # not tz-correct, recompute below
# recompute ET date properly via query
cur.execute("""SELECT id, date(ts AT TIME ZONE 'America/New_York') FROM setup_log WHERE id=ANY(%s)""",
            (list(placed),))
etdate={r[0]:str(r[1]) for r in cur.fetchall()}
agg=defaultdict(lambda:[0.0,0.0,0])
for u in univ:
    if u['id'] not in placed: continue
    d=etdate.get(u['id'])
    if not d: continue
    agg[d][0]+=u['chain_pnl']; agg[d][1]+=u['mes_pnl']; agg[d][2]+=1

print("\n=== VALIDATION: placed trailing-setup trades, day-level chain vs mes vs broker ===")
print(f"{'date':<12}{'n':>4}{'chain':>9}{'mes_walk':>10}{'brokerPTS':>11}{'mes-brk':>9}")
tc=tm=tb=0
for d in sorted(agg):
    if d not in brk: continue
    ch,me,n=agg[d]; bp=brk[d]
    print(f"{d:<12}{n:>4}{ch:>9.1f}{me:>10.1f}{bp:>11.1f}{me-bp:>9.1f}")
    tc+=ch; tm+=me; tb+=bp
print("-"*55)
print(f"{'TOTAL':<12}{'':>4}{tc:>9.1f}{tm:>10.1f}{tb:>11.1f}{tm-tb:>9.1f}")
print(f"\nmes_walk vs broker: total gap {tm-tb:+.1f} pts. If small, simulator is validated for optimization.")

# stash universe for phase 2/3
import pickle
with open("_tmp_trail_universe.pkl","wb") as f: pickle.dump(univ,f)
print(f"\nsaved {len(univ)} universe trades to _tmp_trail_universe.pkl")
conn.close()
