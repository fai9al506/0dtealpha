"""GATE QUESTION: why profitable for months, then 6-day bleed?

Hypothesis: edge is REGIME-CONDITIONAL. Wins in low-vol/range/up tape,
loses in sustained high-vol downtrend. June = first sustained one since go-live.

Method (consistent across whole history): portal setup_log outcomes for the
V16-whitelist setups, joined to market regime per day (SPX trend, range, VIX).
Uses outcome_pnl (chain sim) so methodology is identical pre/post broker era.
Also overlays REAL broker $ where real_trade_orders exists.
"""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

WL = ('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','DD Exhaustion')

# data span
cur.execute("SELECT min(ts AT TIME ZONE 'America/New_York')::date, max(ts AT TIME ZONE 'America/New_York')::date FROM setup_log")
print("setup_log span:", cur.fetchone())

# market regime per day (range, trend, vix) from chain_snapshots
cur.execute("""
  WITH d AS (SELECT (ts AT TIME ZONE 'America/New_York')::date dd, spot, vix,
                    ts AT TIME ZONE 'America/New_York' et FROM chain_snapshots
             WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::date>='2026-02-01')
  SELECT dd, max(spot)-min(spot) rng,
         (array_agg(spot ORDER BY et DESC))[1]-(array_agg(spot ORDER BY et))[1] AS close_minus_open,
         avg(vix) FROM d GROUP BY dd ORDER BY dd""")
reg={}
for dd,rng,cmo,vix in cur.fetchall():
    reg[str(dd)]={'rng':float(rng),'trend':float(cmo or 0),'vix':float(vix) if vix else None}

# system outcomes per day (portal sim, WL setups only, longs vs shorts)
cur.execute(f"""
  SELECT (sl.ts AT TIME ZONE 'America/New_York')::date dd, sl.direction, sl.outcome_pnl,
         CASE WHEN rto.setup_log_id IS NULL THEN 0 ELSE 1 END AS real
  FROM setup_log sl LEFT JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
  WHERE sl.setup_name IN %s AND sl.outcome_pnl IS NOT NULL
    AND (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-02-01'
""", (WL,))
day=defaultdict(lambda:{'n':0,'w':0,'pts':0.0,'long_n':0,'long_pts':0.0,'short_pts':0.0,'real_n':0})
for dd,direction,pnl,real in cur.fetchall():
    ds=str(dd); pnl=float(pnl); a=day[ds]
    a['n']+=1; a['pts']+=pnl
    if pnl>0: a['w']+=1
    if direction in ('long','bullish'): a['long_n']+=1; a['long_pts']+=pnl
    else: a['short_pts']+=pnl
    if real: a['real_n']+=1

# ---- monthly rollup ----
print("\n=== MONTHLY: system (portal-sim, WL setups) vs regime ===")
mon=defaultdict(lambda:{'days':0,'n':0,'w':0,'pts':0.0,'long_pts':0.0,'short_pts':0.0,
                        'hv_days':0,'down_days':0,'rng_sum':0.0,'vix_sum':0.0,'vix_n':0,'real_n':0})
for ds in sorted(day):
    m=ds[:7]; a=day[ds]; r=reg.get(ds,{})
    M=mon[m]; M['days']+=1; M['n']+=a['n']; M['w']+=a['w']; M['pts']+=a['pts']
    M['long_pts']+=a['long_pts']; M['short_pts']+=a['short_pts']; M['real_n']+=a['real_n']
    rng=r.get('rng',0); M['rng_sum']+=rng
    if rng>=120: M['hv_days']+=1
    if r.get('trend',0)<=-25: M['down_days']+=1
    if r.get('vix'): M['vix_sum']+=r['vix']; M['vix_n']+=1
print(f"{'month':8} {'days':>4} {'trades':>6} {'WR':>4} {'pts':>7} {'long':>7} {'short':>7} {'avgRng':>6} {'avgVIX':>6} {'HVd':>4} {'DOWNd':>5}")
for m in sorted(mon):
    M=mon[m]; wr=M['w']/M['n']*100 if M['n'] else 0
    avgvix=M['vix_sum']/M['vix_n'] if M['vix_n'] else 0
    print(f"{m:8} {M['days']:>4} {M['n']:>6} {wr:>3.0f}% {M['pts']:>7.0f} {M['long_pts']:>7.0f} {M['short_pts']:>7.0f} "
          f"{M['rng_sum']/M['days']:>6.0f} {avgvix:>6.1f} {M['hv_days']:>4} {M['down_days']:>5}")

# ---- the crux: system performance bucketed by regime (all history) ----
print("\n=== SYSTEM P&L BY REGIME BUCKET (portal-sim, all history Feb-Jun) ===")
def bucket(ds):
    r=reg.get(ds,{}); rng=r.get('rng',0); tr=r.get('trend',0)
    vol = 'HIVOL(rng>=120)' if rng>=120 else 'normal(rng<120)'
    dirn = 'DOWN(<=-25)' if tr<=-25 else ('UP(>=25)' if tr>=25 else 'FLAT')
    return vol, dirn
bk=defaultdict(lambda:{'days':0,'pts':0.0,'long_pts':0.0,'short_pts':0.0,'n':0,'w':0})
for ds in sorted(day):
    a=day[ds]; vol,dirn=bucket(ds); k=(vol,dirn)
    b=bk[k]; b['days']+=1; b['pts']+=a['pts']; b['long_pts']+=a['long_pts']
    b['short_pts']+=a['short_pts']; b['n']+=a['n']; b['w']+=a['w']
print(f"{'vol':16} {'dir':12} {'days':>4} {'trades':>6} {'WR':>4} {'pts':>8} {'long':>8} {'short':>8}")
for k in sorted(bk):
    b=bk[k]; wr=b['w']/b['n']*100 if b['n'] else 0
    print(f"{k[0]:16} {k[1]:12} {b['days']:>4} {b['n']:>6} {wr:>3.0f}% {b['pts']:>8.0f} {b['long_pts']:>8.0f} {b['short_pts']:>8.0f}")

# how many HIVOL-DOWN days per month (the kill bucket)?
print("\n=== HIVOL+DOWN days (the kill regime) by month ===")
kill=defaultdict(list)
for ds in sorted(day):
    vol,dirn=bucket(ds)
    if vol.startswith('HIVOL') and dirn.startswith('DOWN'):
        kill[ds[:7]].append((ds, day[ds]['pts'], reg.get(ds,{}).get('rng'), reg.get(ds,{}).get('trend')))
for m in sorted(kill):
    print(f"  {m}: {len(kill[m])} days -> "+", ".join(f"{d}({p:+.0f}p,rng{rng:.0f})" for d,p,rng,tr in kill[m]))
conn.close()
