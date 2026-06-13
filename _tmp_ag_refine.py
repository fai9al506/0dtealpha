import os, psycopg2
from collections import defaultdict
from zoneinfo import ZoneInfo
ET=ZoneInfo('America/New_York')
cur=psycopg2.connect(os.environ['DATABASE_URL']).cursor()
cur.execute("""SELECT id,ts,grade,greek_alignment,paradigm,vix,outcome_pnl,outcome_result,
  gap_to_lis,upside,rr_ratio
  FROM setup_log WHERE setup_name='AG Short' AND direction='short'
  AND outcome_result IS NOT NULL AND outcome_result<>'OPEN' ORDER BY id""")
rows=cur.fetchall()
def stat(s):
    n=len(s)
    if n==0: return "n=0"
    w=sum(1 for r in s if r[7]=='WIN'); net=sum(float(r[6]) for r in s if r[6] is not None)
    return f"n={n:<4} WR={w/n*100:3.0f}%  net={net:+7.1f}p (${net*5:+5.0f})  avg={net/n:+5.2f}"
print("BASELINE          :", stat(rows))
def seg(name, keyfn, order=None):
    print(f"\n=== by {name} ===")
    d=defaultdict(list)
    for r in rows: d[keyfn(r)].append(r)
    keys=order if order else sorted(d, key=lambda x:(x is None,x))
    for k in keys:
        if k in d: print(f"  {str(k):<14}: {stat(d[k])}")
seg("paradigm", lambda r:r[4])
seg("VIX band", lambda r:'<20' if (r[5] or 22)<20 else ('20-25' if (r[5] or 22)<25 else '>=25'), ['<20','20-25','>=25'])
seg("hour ET", lambda r:r[1].astimezone(ET).hour)
seg("alignment", lambda r:r[3])
seg("grade", lambda r:r[2], ['A+','A','B','C','LOG'])
seg("gap_to_lis band", lambda r:'<=5' if (r[8] or 0)<=5 else ('5-10' if (r[8] or 0)<=10 else '>10'), ['<=5','5-10','>10'])
cur.close()
