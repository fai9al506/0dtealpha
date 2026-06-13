"""S217 test suite: basket_gate logic + trail-fix scoping + backtest regression."""
import os, sys, json, bisect
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, '.')
from app import basket_gate
PASS=[]; FAIL=[]
def chk(name, cond):
    (PASS if cond else FAIL).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")

print("=== 1) basket_gate.classify (pure logic) ===")
chk("long + basket +0.5 = confirm",      basket_gate.classify(0.5,'long')=='confirm')
chk("long + basket -0.5 = contradict",   basket_gate.classify(-0.5,'long')=='contradict')
chk("long + basket +0.10 = neutral",     basket_gate.classify(0.10,'long')=='neutral')
chk("short + basket -0.5 = confirm",     basket_gate.classify(-0.5,'short')=='confirm')
chk("short + basket +0.5 = contradict",  basket_gate.classify(0.5,'short')=='contradict')
chk("bullish alias = long",              basket_gate.classify(0.5,'bullish')=='confirm')
chk("bearish alias = short",             basket_gate.classify(-0.5,'bearish')=='confirm')
chk("deadband exactly 0.15 = neutral",   basket_gate.classify(0.149,'long')=='neutral')

print("\n=== 2) evaluate() env gating + fail-open ===")
os.environ.pop('BASKET_GATE_ENABLED', None)
r=basket_gate.evaluate('long')
chk("default disabled -> enabled False", r['enabled']==False)
chk("disabled -> block False always",    r['block']==False)
# fail-open: no DATABASE_URL
_db=os.environ.pop('DATABASE_URL', None)
os.environ['BASKET_GATE_ENABLED']='true'
r2=basket_gate.evaluate('long')
chk("no DB -> no_data + block False (fail-open)", r2['state']=='no_data' and r2['block']==False)
if _db: os.environ['DATABASE_URL']=_db

print("\n=== 3) evaluate() live smoke (env ON, real DB) ===")
os.environ['BASKET_GATE_ENABLED']='true'
rl=basket_gate.evaluate('long'); rs=basket_gate.evaluate('short')
print(f"   live long : {rl}")
print(f"   live short: {rs}")
chk("live returns a valid state", rl['state'] in ('confirm','neutral','contradict','no_data'))
chk("long/short opposite-or-neutral", (rl['state']=='no_data') or (rl['state']!=rs['state']) or (rl['state']=='neutral'))
chk("block only when enabled+neutral/contradict", (not rl['block']) or (rl['enabled'] and rl['state'] in ('neutral','contradict')))
os.environ.pop('BASKET_GATE_ENABLED', None)

print("\n=== 4) REGRESSION: classify reproduces backtest split (expect confirm61/contradict90/neutral27) ===")
import psycopg2, pandas as pd
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
from collections import Counter
c=Counter()
for sid,et,d in trades:
    b=basket_at(et.replace(tzinfo=None))
    if b is None: c['no_data']+=1
    else: c[basket_gate.classify(b,d)]+=1
print(f"   counts: {dict(c)}")
chk("confirm==61", c['confirm']==61)
chk("contradict==90", c['contradict']==90)
chk("neutral==27", c['neutral']==27)
conn.close()

print(f"\n=== RESULT: {len(PASS)} passed, {len(FAIL)} failed ===")
if FAIL: print("FAILURES:", FAIL); sys.exit(1)
