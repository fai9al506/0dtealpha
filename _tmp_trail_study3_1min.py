# -*- coding: utf-8 -*-
"""Phase 3: backtest the trail on clean 1-min SPX OHLC (point-equivalent to MES).
(A) validate live-params 1-min trail vs broker (day-level).
(B) confirm it reproduces the +25.3 trade as ~+7.
Saves universe+bars for the grid search (phase 4)."""
import os, sys, json, pickle, bisect, psycopg2
from datetime import timedelta
from collections import defaultdict
sys.path.insert(0,'.')
from app.mes_sim_backfill import mes_walk, _DEFAULT_PARAMS
conn=psycopg2.connect(os.environ["DATABASE_URL"]); cur=conn.cursor()

# 1-min SPX bars -> list (ts_start, ts_end, o,h,l,c), epoch index
cur.execute("""SELECT ts, bar_open,bar_high,bar_low,bar_close FROM spx_ohlc_1m ORDER BY ts""")
bars=[]
for ts,o,h,l,c in cur.fetchall():
    if o is None: continue
    bars.append((ts, ts+timedelta(minutes=1), float(o),float(h),float(l),float(c)))
bt=[b[0].timestamp() for b in bars]
print(f"1-min SPX bars: {len(bars)}")
def bars_after(ts,mm):
    e0=ts.timestamp(); e1=e0+mm*60
    i=bisect.bisect_left(bt,e0); out=[]
    j=i
    while j<len(bars) and bt[j]<=e1: out.append(bars[j]); j+=1
    return out

# universe: trailing setups, entry = SPX spot (we sim in SPX space)
TRAIL=('Skew Charm','AG Short','DD Exhaustion','GEX Long','VIX Divergence','ES Absorption')
cur.execute(f"""SELECT sl.id, sl.ts, sl.setup_name, sl.direction, sl.grade, sl.spot,
  sl.outcome_pnl, sl.outcome_result, sl.outcome_max_profit, sl.outcome_elapsed_min,
  sl.trail_sl, sl.trail_activation, sl.trail_gap, sl.live_pass, sl.vix
  FROM setup_log sl WHERE sl.setup_name=ANY(%s) AND sl.outcome_result IS NOT NULL
  AND sl.spot IS NOT NULL AND date(sl.ts AT TIME ZONE 'America/New_York')>='2026-02-20'
  ORDER BY sl.ts""",(list(TRAIL),))
univ=[]
for (sid,ts,sn,d,gr,spot,opnl,ores,omfe,oem,tsl,tact,tgap,lp,vix) in cur.fetchall():
    univ.append(dict(id=sid,ts=ts,setup=sn,is_long=(d or '').lower() in ('long','bullish'),
        grade=gr,entry=float(spot),chain_pnl=float(opnl or 0),chain_res=ores,
        elapsed=int(oem) if oem else 90,tsl=tsl,tact=tact,tgap=tgap,
        live_pass=bool(lp),vix=float(vix or 0)))
print(f"universe (trailing, with 1-min coverage era): {len(univ)}")

def live_params(u):
    df=_DEFAULT_PARAMS.get(u['setup'],{"sl":14,"be_trigger":None,"be_lock":0,"trail_act":10,"trail_gap":5})
    return (float(u['tsl']) if u['tsl'] is not None else df['sl'],df['be_trigger'],df['be_lock'],
            float(u['tact']) if u['tact'] is not None else df['trail_act'],
            float(u['tgap']) if u['tgap'] is not None else df['trail_gap'])

# walk all with live params (SPX 1-min)
for u in univ:
    sl,bt_,bl,ta,tg=live_params(u); mm=max(u['elapsed']+30,60)
    r=mes_walk(bars_after(u['ts'],mm),u['entry'],u['is_long'],sl,bt_,bl,ta,tg,mm)
    u['s1_pnl']=round(r['pnl'],2); u['s1_mfe']=round(r['mfe'],2); u['s1_reason']=r['reason']

# (B) +25.3 trade sanity
t=[u for u in univ if u['id']==3905]
if t: print(f"\n[sanity] +25.3 trade (id3905): chain={t[0]['chain_pnl']}  1min-trail={t[0]['s1_pnl']}  (broker +6.8)")

# (A) validate vs broker day-level (placed trades)
cur.execute("SELECT setup_log_id FROM real_trade_orders"); placed=set(r[0] for r in cur.fetchall())
cur.execute("SELECT id, date(ts AT TIME ZONE 'America/New_York') FROM setup_log WHERE id=ANY(%s)",(list(placed),))
etd={r[0]:str(r[1]) for r in cur.fetchall()}
cur.execute("SELECT day, trades FROM tsrt_daily_stmt ORDER BY day")
brk={}
for day,tr in cur.fetchall():
    js=tr if isinstance(tr,list) else json.loads(tr or '[]')
    brk[str(day)]=sum(t.get('pts',0) for t in js)
agg=defaultdict(lambda:[0.0,0.0,0])
for u in univ:
    if u['id'] not in placed: continue
    d=etd.get(u['id']);
    if not d: continue
    agg[d][0]+=u['chain_pnl']; agg[d][1]+=u['s1_pnl']; agg[d][2]+=1
print("\n=== VALIDATION: 1-min SPX trail (live params) vs broker, day-level ===")
print(f"{'date':<12}{'n':>4}{'chain':>9}{'1min':>9}{'broker':>9}{'1m-brk':>8}{'chn-brk':>8}")
tc=t1=tb=0
for d in sorted(agg):
    if d not in brk: continue
    ch,s1,n=agg[d]; bp=brk[d]; tc+=ch; t1+=s1; tb+=bp
    print(f"{d:<12}{n:>4}{ch:>9.1f}{s1:>9.1f}{bp:>9.1f}{s1-bp:>8.1f}{ch-bp:>8.1f}")
print("-"*60)
print(f"{'TOTAL':<12}{'':>4}{tc:>9.1f}{t1:>9.1f}{tb:>9.1f}{t1-tb:>8.1f}{tc-tb:>8.1f}")
print(f"\n1-min trail vs broker total gap: {t1-tb:+.1f} pts  |  chain vs broker: {tc-tb:+.1f} pts")
print("(smaller |1m-brk| than |chn-brk| => 1-min sim is the honest optimizer basis)")

pickle.dump((univ,bars,bt),open("_tmp_1min_univ.pkl","wb"))
print("saved universe+bars")
conn.close()
