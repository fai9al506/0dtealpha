"""A) Regime separators: did any REAL-TIME metric warn that Jun5-12 was a bad
   regime to size down? Compare May19-Jun4 vs Jun5-12 + bucket outcomes.
B) Dark Mate Semi sizing re-test on the authoritative Excel V16 log.
"""
import os, sys, json, psycopg2, statistics
import pandas as pd
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8')
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
df['Date']=pd.to_datetime(df['Date']); df['m']=df['Date'].dt.strftime('%Y-%m')
conn=psycopg2.connect(os.environ['DATABASE_URL']); cur=conn.cursor()

# pull metrics for all Excel trades (join setup_log by id)
ids=tuple(int(x) for x in df['ID'])
cur.execute("""SELECT id,(ts AT TIME ZONE 'America/New_York') et, direction, vix, vix3m,
   spot_vol_beta, paradigm FROM setup_log WHERE id IN %s""",(ids,))
M={}
for sid,et,d,vix,vix3m,svb,par in cur.fetchall():
    M[sid]={'et':et,'dir':d,'vix':float(vix) if vix else None,
            'vix3m':float(vix3m) if vix3m else None,
            'svb':float(svb) if svb else None,'par':par}

# semi_basket: latest row <= trade et
cur.execute("SELECT et,basket_pct FROM semi_basket ORDER BY et")
sb=[(r[0],float(r[1])) for r in cur.fetchall()]
import bisect
sb_t=[x[0] for x in sb]
def basket_at(et_naive):
    i=bisect.bisect_right(sb_t, et_naive)-1
    return sb[i][1] if i>=0 else None

df['pnl']=df['P&L']
def period(d):
    ds=d.strftime('%Y-%m-%d')
    if ds<'2026-05-19': return 'pre'
    return 'WIN(May19-Jun4)' if ds<='2026-06-04' else 'LOSS(Jun5-12)'
df['period']=df['Date'].apply(period)

# ---- A) regime metric comparison (post-V16 only) ----
print("=== A) Real-time metric averages: WIN vs LOSS period ===")
post=df[df['period'].isin(['WIN(May19-Jun4)','LOSS(Jun5-12)'])].copy()
for col,fn in [('VIX',lambda r:M[r['ID']]['vix']),
               ('VIX-VIX3M (overvix)',lambda r:(M[r['ID']]['vix']-M[r['ID']]['vix3m']) if M[r['ID']]['vix'] and M[r['ID']]['vix3m'] else None),
               ('spot_vol_beta',lambda r:M[r['ID']]['svb']),
               ('|basket%|',lambda r:abs(basket_at(M[r['ID']]['et'].replace(tzinfo=None))) if basket_at(M[r['ID']]['et'].replace(tzinfo=None)) is not None else None)]:
    for per in ['WIN(May19-Jun4)','LOSS(Jun5-12)']:
        vals=[fn(r) for _,r in post[post['period']==per].iterrows()]
        vals=[v for v in vals if v is not None]
        if vals: print(f"  {col:22} {per:18} avg={statistics.mean(vals):+.2f}  median={statistics.median(vals):+.2f}")
    print()

# ---- A2) does basket CONTRADICTION predict losses? (the warning signal) ----
print("=== A2) outcome by semi-basket alignment (post-V16) ===")
def align(r):
    b=basket_at(M[r['ID']]['et'].replace(tzinfo=None))
    if b is None: return 'no_data'
    dsign=1 if M[r['ID']]['dir'] in ('long','bullish') else -1
    if abs(b)<0.15: return 'neutral'
    return 'confirm' if (b>0)==(dsign>0) else 'CONTRADICT'
post['align']=post.apply(align,axis=1)
g=post.groupby('align')['pnl'].agg(n='count',total='sum',mean='mean',
     wr=lambda x:(x>0).mean()*100).round(2)
print(g.to_string())
print("\n  same split, LOSS window only:")
g2=post[post['period']=='LOSS(Jun5-12)'].groupby('align')['pnl'].agg(n='count',total='sum',wr=lambda x:(x>0).mean()*100).round(1)
print(g2.to_string())

# ---- B) Semi sizing re-test (Scheme A: confirm 2x / neutral 1x / contradict 0.5x) ----
def mult(r):
    a=align(r)
    return {'confirm':2.0,'neutral':1.0,'CONTRADICT':0.5,'no_data':1.0}[a]
df['mult']=df.apply(lambda r: mult(r) if r['period']!='pre' else 1.0, axis=1)
df['pnl_semi']=df['pnl']*df['mult']
print("\n=== B) Dark Mate SEMI sizing re-test (Excel V16 log) ===")
print("  monthly: baseline vs semi-sized")
mm=df[df['period']!='pre'].groupby('m').agg(base=('pnl','sum'),semi=('pnl_semi','sum')).round(1)
print(mm.to_string())
print("\n  by period:")
pp=df[df['period']!='pre'].groupby('period').agg(base=('pnl','sum'),semi=('pnl_semi','sum')).round(1)
print(pp.to_string())
conn.close()
