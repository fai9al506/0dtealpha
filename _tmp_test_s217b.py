"""S217 regression (corrected): shipped classify() must equal the backtest's inline
logic for EVERY post-V16 trade (proves the module == validated Scheme B logic)."""
import os, sys, bisect
sys.stdout.reconfigure(encoding='utf-8'); sys.path.insert(0,'.')
from app import basket_gate
import psycopg2, pandas as pd
from collections import Counter
df=pd.read_excel(r'C:\Users\Faisa\Downloads\trade_log_2026-06-13.xlsx', sheet_name='trade_log_2026-06-13')
ids=tuple(int(x) for x in df['ID'])
conn=psycopg2.connect(os.environ['DATABASE_URL']);cur=conn.cursor()
cur.execute("""SELECT sl.id,(sl.ts AT TIME ZONE 'America/New_York') et, sl.direction
   FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
   WHERE (sl.ts AT TIME ZONE 'America/New_York')::date>='2026-05-19' AND sl.id IN %s ORDER BY sl.ts""",(ids,))
trades=cur.fetchall()
cur.execute("SELECT et,basket_pct FROM semi_basket ORDER BY et")
sb=[(r[0],float(r[1])) for r in cur.fetchall()]; sbt=[x[0] for x in sb]
def basket_at(t):
    i=bisect.bisect_right(sbt,t)-1; return sb[i][1] if i>=0 else None
def inline(b, d):  # exact logic from _tmp_scheme_matrix backtest
    il=d in ('long','bullish')
    if abs(b)<0.15: return 'neutral'
    return 'confirm' if (b>0)==il else 'contradict'
mismatch=0; c=Counter()
for sid,et,d in trades:
    b=basket_at(et.replace(tzinfo=None))
    if b is None: c['no_data']+=1; continue
    a_ship=basket_gate.classify(b,d); a_inline=inline(b,d)
    c[a_ship]+=1
    if a_ship!=a_inline:
        mismatch+=1; print(f"  MISMATCH #{sid}: ship={a_ship} inline={a_inline} b={b}")
print(f"trades={len(trades)}  counts={dict(c)}  mismatches={mismatch}")
# block decision check: 0/0/1 blocks neutral+contradict
blocks=sum(1 for sid,et,d in trades if (lambda b: b is not None and basket_gate.classify(b,d) in ('neutral','contradict'))(basket_at(et.replace(tzinfo=None))))
takes=len(trades)-blocks
print(f"0/0/1 would: TAKE {takes} (confirm+no_data), SKIP {blocks} (neutral+contradict)")
print("RESULT:", "PASS — classifier == backtest logic, 0 mismatches" if mismatch==0 else f"FAIL — {mismatch} mismatches")
conn.close()
sys.exit(0 if mismatch==0 else 1)
