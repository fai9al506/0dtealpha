"""Get production ES Absorption results + SL/TP optimization."""
import psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import timedelta, time as dtime
import os, math

DB_URL = os.environ.get('DATABASE_URL')

def find_swings(bars, pivot_n=2):
    swings = []
    for i in range(pivot_n, len(bars) - pivot_n):
        is_low = all(bars[i]['bar_low'] <= bars[i-j]['bar_low'] and bars[i]['bar_low'] <= bars[i+j]['bar_low'] for j in range(1, pivot_n+1))
        if is_low:
            swings.append({'type':'low','price':bars[i]['bar_low'],'cvd':bars[i]['cvd'],'ts':bars[i]['ts_start'],'bar_idx':i,'volume':bars[i]['volume']})
        is_high = all(bars[i]['bar_high'] >= bars[i-j]['bar_high'] and bars[i]['bar_high'] >= bars[i+j]['bar_high'] for j in range(1, pivot_n+1))
        if is_high:
            swings.append({'type':'high','price':bars[i]['bar_high'],'cvd':bars[i]['cvd'],'ts':bars[i]['ts_start'],'bar_idx':i,'volume':bars[i]['volume']})
    swings.sort(key=lambda s: s['ts'])
    return swings

def detect_simple(bars, swings):
    divs = []
    lows = [s for s in swings if s['type']=='low']
    highs = [s for s in swings if s['type']=='high']
    for i in range(1, len(lows)):
        p,c = lows[i-1], lows[i]
        if c['price']<p['price'] and c['cvd']>p['cvd']:
            divs.append({'direction':'long','price':c['price'],'ts':c['ts'],'bar_idx':c['bar_idx']})
        if c['price']>p['price'] and c['cvd']<p['cvd']:
            divs.append({'direction':'long','price':c['price'],'ts':c['ts'],'bar_idx':c['bar_idx']})
    for i in range(1, len(highs)):
        p,c = highs[i-1], highs[i]
        if c['price']>p['price'] and c['cvd']<p['cvd']:
            divs.append({'direction':'short','price':c['price'],'ts':c['ts'],'bar_idx':c['bar_idx']})
        if c['price']<p['price'] and c['cvd']>p['cvd']:
            divs.append({'direction':'short','price':c['price'],'ts':c['ts'],'bar_idx':c['bar_idx']})
    divs.sort(key=lambda d: d['ts'])
    return divs

def filt(divs):
    cd = {'long':None,'short':None}
    out = []
    for d in divs:
        ts=d['ts']
        if hasattr(ts,'utcoffset') and ts.utcoffset() is not None:
            ts_utc=ts.replace(tzinfo=None)-ts.utcoffset()
        else:
            ts_utc=ts.replace(tzinfo=None)
        et=ts_utc-timedelta(hours=5)
        if dtime(et.hour,et.minute)<dtime(10,0) or dtime(et.hour,et.minute)>dtime(15,30): continue
        c=d['direction']
        if cd[c] and ts<cd[c]: continue
        out.append(d)
        cd[c]=ts+timedelta(minutes=15)
    return out

def sim(divs, bars, sl, tp):
    trades=[]
    for d in divs:
        entry=d['price']; is_long=d['direction']=='long'
        tgt=entry+tp if is_long else entry-tp
        stp=entry-sl if is_long else entry+sl
        result='EXPIRED'; pnl=0
        for j in range(d['bar_idx']+1, len(bars)):
            hi=float(bars[j]['bar_high']); lo=float(bars[j]['bar_low'])
            if is_long:
                if lo<=stp: result='LOSS';pnl=-sl;break
                if hi>=tgt: result='WIN';pnl=tp;break
            else:
                if hi>=stp: result='LOSS';pnl=-sl;break
                if lo<=tgt: result='WIN';pnl=tp;break
        if result=='EXPIRED':
            ep=float(bars[-1]['bar_close']); pnl=(ep-entry) if is_long else (entry-ep)
        trades.append({'result':result,'pnl':pnl})
    return trades

def main():
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT DISTINCT trade_date FROM es_range_bars WHERE source='rithmic' ORDER BY trade_date")
    dates = [r['trade_date'] for r in cur.fetchall()]

    # Production ES Absorption
    print("="*65, flush=True)
    print("  PRODUCTION ES ABSORPTION (setup_log)", flush=True)
    print("="*65, flush=True)

    cur.execute("""
        SELECT id, direction, grade, score, outcome_result, outcome_pnl,
               ts AT TIME ZONE 'America/New_York' as ts_et
        FROM setup_log
        WHERE setup_name = 'ES Absorption'
        ORDER BY id DESC LIMIT 100
    """)
    sigs = cur.fetchall()
    resolved = [s for s in sigs if s['outcome_result'] in ('WIN','LOSS')]
    wins = sum(1 for s in resolved if s['outcome_result']=='WIN')
    losses = len(resolved)-wins
    pts = sum(float(s['outcome_pnl'] or 0) for s in resolved)
    wr = wins/len(resolved)*100 if resolved else 0
    print(f"  {len(resolved)} resolved: {wins}W/{losses}L WR={wr:.1f}% {pts:+.1f}pts", flush=True)

    by_grade = defaultdict(lambda:{'n':0,'w':0,'pts':0})
    for s in resolved:
        g=by_grade[s['grade']]; g['n']+=1
        if s['outcome_result']=='WIN': g['w']+=1
        g['pts']+=float(s['outcome_pnl'] or 0)
    for grade in sorted(by_grade):
        g=by_grade[grade]; gwr=g['w']/g['n']*100 if g['n'] else 0
        print(f"    {grade}: {g['n']}t WR={gwr:.0f}% {g['pts']:+.1f}pts", flush=True)

    # SL/TP optimization
    print(f"\n{'='*65}", flush=True)
    print("  SL/TP OPTIMIZATION — VP-simple on rithmic", flush=True)
    print("="*65, flush=True)

    results = []
    for sl, tp in [(5,5),(5,8),(5,10),(8,8),(8,10),(8,12),(10,10),(10,12),(10,15),(12,10),(12,15)]:
        all_t = []
        for trade_date in dates:
            cur.execute("""
                SELECT bar_idx,bar_open,bar_high,bar_low,bar_close,
                       bar_volume AS volume,bar_delta AS delta,cumulative_delta AS cvd,
                       ts_start,ts_end
                FROM es_range_bars WHERE source='rithmic' AND trade_date=%s AND status='closed'
                ORDER BY bar_idx ASC
            """, (str(trade_date),))
            bars=cur.fetchall()
            if len(bars)<20: continue
            swings=find_swings(bars)
            all_t.extend(sim(filt(detect_simple(bars,swings)),bars,sl,tp))
        w=sum(1 for t in all_t if t['result']=='WIN')
        n=len(all_t); p=sum(t['pnl'] for t in all_t)
        wr=w/n*100 if n else 0
        avg=p/n if n else 0
        results.append((sl,tp,n,w,wr,p,avg))
        print(f"  SL={sl:>2}/T={tp:>2}: {n:>3}t {w:>2}W WR={wr:>5.1f}% {p:>+7.1f}pts avg={avg:>+5.1f}", flush=True)

    # Best by total pts
    best = max(results, key=lambda x: x[5])
    print(f"\n  Best by total: SL={best[0]}/T={best[1]} ({best[5]:+.1f}pts)", flush=True)
    # Best by avg (min 20 trades)
    qualified = [r for r in results if r[2] >= 20]
    if qualified:
        best_avg = max(qualified, key=lambda x: x[6])
        print(f"  Best by avg:   SL={best_avg[0]}/T={best_avg[1]} ({best_avg[6]:+.1f}pts/trade)", flush=True)

    conn.close()
    print("\nDone.", flush=True)

if __name__ == '__main__':
    main()
