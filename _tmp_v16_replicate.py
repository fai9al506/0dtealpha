import os, psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo
ET=ZoneInfo('America/New_York')
c=psycopg2.connect(os.environ['DATABASE_URL']); cur=c.cursor()
today=datetime.now(ET).date()
cur.execute("""
 SELECT sl.id, sl.setup_name, sl.direction, sl.grade, sl.ts, sl.outcome_pnl,
        sl.greek_alignment, sl.paradigm, sl.vix, sl.overvix,
        sl.v13_gex_above, sl.v13_dd_near, sl.vanna_cliff_side, sl.vanna_peak_side,
        sl.real_trade_skip_reason, (rto.setup_log_id IS NOT NULL) AS placed
 FROM setup_log sl LEFT JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
 WHERE sl.ts::date=%s ORDER BY sl.ts
""",(today,))
rows=cur.fetchall()

def passes_v16(l):
    sn=l['setup_name']; align=l['greek_alignment'] or 0
    isLong = l['direction'] in ('long','bullish')
    et=l['ts'].astimezone(ET); mins=et.hour*60+et.minute
    para=l['paradigm']; vix=l['vix'] or 0; grade=l['grade']
    allowed={'Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion','GEX Long'}
    if sn not in allowed: return False
    if sn=='DD Exhaustion' and not isLong: return False
    if sn=='GEX Long': return False
    if isLong and para=='GEX-TARGET' and et.hour>=13: return False
    def isOpex():
        return et.weekday()==4 and 15<=et.day<=21
    if sn=='DD Exhaustion' and isLong:
        if para=='SIDIAL-EXTREME' and 840<=mins<900: return False
        if align<0: return False
        if align>=3: return False
        if vix>=22: return False
        if para in ('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY'): return False
        if grade=='C': return False
        return True
    # gap filter (longs only) — skip, need _tlDailyGaps; assume pass (no >30 gap today)
    if sn=='Skew Charm' and grade in ('C','LOG'): return False
    if sn in ('IV Momentum','Vanna Butterfly'): return False
    # V11 time gates
    if sn in ('Skew Charm','DD Exhaustion') and (870<=mins<900): return False
    if sn in ('Skew Charm','DD Exhaustion') and mins>=930: return False
    # V13 bullish block
    def v13bull():
        if isLong: return False
        if sn not in ('Skew Charm','DD Exhaustion'): return False
        if (l['v13_gex_above'] or 0)>=75: return True
        if (l['v13_dd_near'] or 0)>=3000000000: return True
        return False
    if v13bull(): return False
    # V13 vanna block
    def v13vanna():
        cc=l['vanna_cliff_side']; p=l['vanna_peak_side']
        if cc is None: return False
        if not isLong:
            if sn=='DD Exhaustion' and cc=='A' and p=='B': return True
            if sn=='Skew Charm' and cc=='A' and p=='B': return True
            if sn=='AG Short' and cc=='B' and p=='A': return True
        return False
    if v13vanna(): return False
    # V13 DD quality
    def v13ddq():
        if sn!='DD Exhaustion': return False
        if isLong:
            if align>=3: return True
            if vix>=22: return True
            if para in ('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY'): return True
            if grade=='C': return True
        else:
            if para=='BOFA-PURE': return True
            if grade=='A+': return True
            if grade=='C': return True
        return False
    if v13ddq(): return False
    if sn=='ES Absorption':
        if grade not in ('A','A+'): return False
        if para in ('AG-TARGET','AG-LIS'): return False
        if mins>=945: return False
        if isLong and align<0: return False
        if not isLong and align>0: return False
        if not isLong and mins>=840: return False
        return True
    if sn=='Skew Charm' and isLong and para=='GEX-LIS': return False
    if sn=='Skew Charm' and isLong and isOpex(): return False
    if sn=='AG Short' and isOpex(): return False
    # V10BaseV14
    if isLong:
        if sn=='Skew Charm':
            if para=='SIDIAL-EXTREME' and 840<=mins<900: return False
            if align==3 and para in ('GEX-LIS','AG-LIS','AG-PURE','BOFA-MESSY'): return False
            return True
        if para=='SIDIAL-EXTREME' and 840<=mins<900: return False
        if align<2: return False
        if vix>22 and (l['overvix'] or -99)<2: return False
        return True
    if sn in ('Skew Charm','DD Exhaustion') and para=='GEX-LIS': return False
    if sn=='Skew Charm': return True
    if sn=='AG Short': return True
    if sn=='DD Exhaustion' and align!=0: return True
    return False

cols=['id','setup_name','direction','grade','ts','outcome_pnl','greek_alignment','paradigm','vix','overvix','v13_gex_above','v13_dd_near','vanna_cliff_side','vanna_peak_side','real_trade_skip_reason','placed']
v16_sum=0; v16_n=0; placed_in=0; notplaced_in=[]
print(f"{'id':<5}{'setup':<12}{'dir':<6}{'gr':<4}{'time':<7}{'pnl':>7}  {'placed':<7}{'skip'}")
for r in rows:
    l=dict(zip(cols,r))
    if passes_v16(l):
        pf=float(l['outcome_pnl']) if l['outcome_pnl'] is not None else 0
        v16_sum+=pf; v16_n+=1
        t=l['ts'].astimezone(ET).strftime('%H:%M')
        mk='YES' if l['placed'] else 'no'
        if l['placed']: placed_in+=1
        else: notplaced_in.append((l['id'],l['setup_name'],pf,l['real_trade_skip_reason']))
        print(f"{l['id']:<5}{l['setup_name'][:11]:<12}{l['direction'][:5]:<6}{str(l['grade']):<4}{t:<7}{pf:>7.1f}  {mk:<7}{l['real_trade_skip_reason'] or ''}")
print(f"\nV16-passing: {v16_n} signals, outcome_pnl sum = {v16_sum:.1f} pts  (= ${v16_sum*5:.0f} @1MES)")
print(f"  of which PLACED on TSRT: {placed_in}")
print(f"  NOT placed (breaker/other) but shown in V16: {len(notplaced_in)}")
for i in notplaced_in: print("   ",i)
cur.close(); c.close()
