"""Day-by-day per-trade forensics. User Q: Jun 11 closed BULLISH (+101) yet we
lost -$308; Jun 12 was chop and still lost. Why? Overlay each real trade on the
intraday SPX path. Compare to winning days (Jun 4 +460, May 20 +392)."""
import os, sys, psycopg2, json
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()
WL=('Skew Charm','AG Short','Vanna Pivot Bounce','VIX Divergence','ES Absorption','DD Exhaustion')

def spx_at(daystr):
    cur.execute("""SELECT (ts AT TIME ZONE 'America/New_York') et, spot FROM chain_snapshots
       WHERE (ts AT TIME ZONE 'America/New_York')::date=%s AND spot IS NOT NULL ORDER BY et""",(daystr,))
    return [(str(et)[11:16], float(s)) for et,s in cur.fetchall()]

def rp(state,direction):
    if isinstance(state,str): state=json.loads(state)
    f=state.get('entry_fill_price') or state.get('fill_price')
    e=state.get('stop_fill_price') or state.get('close_fill_price')
    if f is None or e is None: return None,None,None
    f,e=float(f),float(e)
    return ((e-f) if direction in ('long','bullish') else (f-e)), f, e

def day_report(daystr):
    path=spx_at(daystr)
    if path:
        op=path[0][1]; cl=path[-1][1]; lo=min(p[1] for p in path); hi=max(p[1] for p in path)
        # morning low/high timing
        print(f"\n{'='*70}\n{daystr}  SPX open {op:.0f} -> close {cl:.0f} ({cl-op:+.0f})  lo {lo:.0f} hi {hi:.0f} range {hi-lo:.0f}")
        # print hourly path
        seen=set(); hr=[]
        for t,s in path:
            h=t[:2]
            if h not in seen: seen.add(h); hr.append(f"{t}={s:.0f}")
        print("  path: "+"  ".join(hr))
    cur.execute("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction,
                          sl.grade, sl.outcome_pnl, rto.state
       FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
       WHERE sl.setup_name IN %s AND (sl.ts AT TIME ZONE 'America/New_York')::date=%s
       ORDER BY sl.ts""",(WL,daystr))
    rows=cur.fetchall()
    print(f"  {len(rows)} real trades:")
    print(f"   {'entry_et':9} {'setup':14} {'dir':6} {'gr':3} {'entry':>8} {'exit':>8} {'pts':>6} {'$':>6} {'chain':>6}")
    tot=0.0
    for et,setup,direction,grade,opnl,state in rows:
        p,f,e=rp(state,direction)
        if p is None: continue
        tot+=p
        print(f"   {str(et)[11:16]:9} {setup:14} {direction:6} {str(grade):3} {str(f):>8} {str(e):>8} {p:>+6.1f} {p*5:>+6.0f} {(opnl or 0):>+6.1f}")
    print(f"   --- broker total: {tot:+.1f} pts (${tot*5:+.0f})")

for d in ['2026-06-11','2026-06-12','2026-06-04','2026-05-20']:
    day_report(d)
conn.close()
