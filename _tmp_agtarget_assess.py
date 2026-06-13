import os, psycopg2
from collections import defaultdict
cur=psycopg2.connect(os.environ['DATABASE_URL']).cursor()

# All AG Short signals with an outcome, full history
cur.execute("""
 SELECT id, ts::date, ts, grade, greek_alignment, paradigm,
        outcome_pnl, outcome_result, mes_sim_outcome_pnl, real_trade_skip_reason
 FROM setup_log
 WHERE setup_name='AG Short' AND direction='short'
   AND outcome_result IS NOT NULL AND outcome_result <> 'OPEN'
 ORDER BY id
""")
rows=cur.fetchall()
def stat(sub):
    n=len(sub); 
    if n==0: return "n=0"
    wins=sum(1 for r in sub if r[7]=='WIN')
    net=sum(float(r[6]) for r in sub if r[6] is not None)
    wr=wins/n*100
    return f"n={n:<4} WR={wr:4.0f}%  net={net:+7.1f}pt (~${net*5:+.0f}@1MES)  avg={net/n:+.2f}"

agt=[r for r in rows if r[5]=='AG-TARGET']
non=[r for r in rows if r[5]!='AG-TARGET']
print("=== AG Short full history (resolved) ===")
print("ALL AG Short      :", stat(rows))
print("AG-TARGET (blocked):", stat(agt))
print("non-AG-TARGET     :", stat(non))
print(f"\ndate range AG-TARGET: {agt[0][1] if agt else '-'} .. {agt[-1][1] if agt else '-'}")

# By month
print("\n=== AG-TARGET by month ===")
bym=defaultdict(list)
for r in agt: bym[str(r[1])[:7]].append(r)
for m in sorted(bym): print(f"  {m}: {stat(bym[m])}")

# By grade
print("\n=== AG-TARGET by grade ===")
byg=defaultdict(list)
for r in agt: byg[str(r[3])].append(r)
for g in sorted(byg): print(f"  {g:<4}: {stat(byg[g])}")

# By alignment
print("\n=== AG-TARGET by greek_alignment ===")
bya=defaultdict(list)
for r in agt: bya[r[4]].append(r)
for a in sorted(bya, key=lambda x:(x is None, x)): print(f"  al={a}: {stat(bya[a])}")
cur.close()
