"""REAL-TIME-implementable protections only (no look-ahead). Counterfactual P&L
on broker trades, May19-Jun12. Size-down = halve that trade's broker pts.

P1 VIX-at-open halve: if day's first VIX >= thr, halve ALL that day's trades.
P2 consecutive-stop size-down: within a day, after 2 consecutive prior STOP losses,
   halve every subsequent trade (decision uses only PAST outcomes -> real-time legal).
P3 consecutive-stop PAUSE: after 3 consecutive prior stops same day, skip rest of day.
P4 rolling realized-range halve: once range-so-far(at entry) >= thr, halve subsequent.
All compared to broker baseline. Also show $ and which days affected.
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','DD Exhaustion')

# per-trade chronological, with vix + close_reason + broker pts + entry time for range
cur.execute("""SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') et, sl.direction, sl.vix, rto.state
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE sl.setup_name IN %s AND (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19'
   ORDER BY sl.ts""",(WL,))
rows=[]
for sid,et,direction,vix,state in cur.fetchall():
    st=state if isinstance(state,dict) else json.loads(state)
    f=st.get('fill_price'); e=st.get('stop_fill_price') or st.get('close_fill_price')
    if f is None or e is None: continue
    f,e=float(f),float(e)
    pts=(e-f) if direction in ('long','bullish') else (f-e)
    is_stop = str(st.get('close_reason'))=='stop_filled'
    rows.append({'day':str(et)[:10],'et':str(et)[11:16],'pts':pts,'stop':is_stop,
                 'vix':float(vix) if vix else None})

# first vix per day
firstvix={}
for r in rows:
    firstvix.setdefault(r['day'], r['vix'])

# realized range so far per day at each entry (from chain)
cur.execute("""SELECT (ts AT TIME ZONE 'America/New_York')::date d,
                      to_char(ts AT TIME ZONE 'America/New_York','HH24:MI') t, spot
   FROM chain_snapshots WHERE spot IS NOT NULL
   AND (ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' ORDER BY ts""")
spot_series=defaultdict(list)
for d,t,s in cur.fetchall(): spot_series[str(d)].append((t,float(s)))
def range_so_far(day,et):
    pts=[s for t,s in spot_series.get(day,[]) if t<=et]
    return (max(pts)-min(pts)) if pts else 0

base=sum(r['pts'] for r in rows)*5
def show(name,total): print(f"  {name:48} Results: {total-base:+.0f} $  (era {base:+.0f} -> {total:+.0f})")
print(f"BASELINE broker era = {base:+.0f} $\n")

# P1 VIX-at-open
print("P1 VIX-at-open halve whole day:")
for thr in [18,19,20]:
    tot=sum((r['pts']*0.5 if (firstvix.get(r['day']) or 0)>=thr else r['pts']) for r in rows)*5
    days=sorted({r['day'] for r in rows if (firstvix.get(r['day']) or 0)>=thr})
    show(f"  vix1>={thr} (days affected={len(days)})", tot)

# P2 consecutive-stop size-down
print("\nP2 after 2 consecutive prior stops -> halve rest of day:")
tot=0.0; affected=0
byday=defaultdict(list)
for r in rows: byday[r['day']].append(r)
for day,trs in byday.items():
    consec=0
    for r in trs:
        mult = 0.5 if consec>=2 else 1.0
        if mult<1: affected+=1
        tot+=r['pts']*mult
        consec = consec+1 if r['stop'] else 0
tot*=5
show(f"  (trades halved={affected})", tot)

# P3 consecutive-stop pause (skip after 3 consecutive prior stops)
print("\nP3 after 3 consecutive prior stops -> skip rest of day:")
for N in [2,3]:
    tot=0.0; skipped=0
    for day,trs in byday.items():
        consec=0; paused=False
        for r in trs:
            if paused: skipped+=1; continue
            tot+=r['pts']
            consec = consec+1 if r['stop'] else 0
            if consec>=N: paused=True
        # note: pausing uses only past info -> legal
    tot*=5
    show(f"  pause after {N} consec stops (skipped={skipped})", tot)

# P4 rolling realized range halve
print("\nP4 once realized-range-so-far >= thr -> halve subsequent trades:")
for thr in [60,80,100]:
    tot=0.0; aff=0
    for r in rows:
        rsf=range_so_far(r['day'],r['et'])
        mult=0.5 if rsf>=thr else 1.0
        if mult<1: aff+=1
        tot+=r['pts']*mult
    tot*=5
    show(f"  range_so_far>={thr} (trades halved={aff})", tot)

# COMBO P1(vix>=19) + P2
print("\nCOMBO vix1>=19 halve-day + consec-stop(2) halve:")
tot=0.0
for day,trs in byday.items():
    vmult=0.5 if (firstvix.get(day) or 0)>=19 else 1.0
    consec=0
    for r in trs:
        cmult=0.5 if consec>=2 else 1.0
        tot+=r['pts']*vmult*cmult
        consec=consec+1 if r['stop'] else 0
tot*=5
show("  combined", tot)
conn.close()
