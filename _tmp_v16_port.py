# -*- coding: utf-8 -*-
"""EXACT port of portal _tlPassesStrategy(l,'v16') (main.py:18833). Validate vs portal:
920 trades / 619W-300L / 67% / +3408.1 pts (all-time, Feb 4+). GEX Long->false, VIX Div not allowed."""
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
ET=ZoneInfo("America/New_York")
# daily gaps (exact endpoint query)
gaps={}
for r in C.execute(text("""WITH closes AS (SELECT DISTINCT ON (date(ts AT TIME ZONE 'America/New_York')) date(ts AT TIME ZONE 'America/New_York') d, spot p FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY date(ts AT TIME ZONE 'America/New_York'), ts DESC),
 opens AS (SELECT DISTINCT ON (date(ts AT TIME ZONE 'America/New_York')) date(ts AT TIME ZONE 'America/New_York') d, spot p FROM chain_snapshots WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::time>='09:30' ORDER BY date(ts AT TIME ZONE 'America/New_York'), ts ASC)
 SELECT o.d, o.p-c.p gap FROM opens o JOIN closes c ON c.d=(SELECT MAX(c2.d) FROM closes c2 WHERE c2.d<o.d) ORDER BY o.d""")).fetchall():
    if r[1] is not None: gaps[str(r[0])]=round(float(r[1]),1)

def passes_v16(l):
    sn=l['setup_name'] or ''; align=l['greek_alignment'] if l['greek_alignment'] is not None else 0
    isLong=l['direction'] in ('long','bullish'); para=l['paradigm']; grade=l['grade']
    ts=l['ts']  # tz-aware UTC
    et=ts.astimezone(ET) if ts else None
    mins=(et.hour*60+et.minute) if et else None
    def gapFilter():
        if not ts or not isLong: return True
        g=gaps.get(et.date().isoformat())
        if g is not None and abs(g)>30 and mins is not None and mins<600: return False
        return True
    def v11():
        if mins is None: return True
        if sn in('Skew Charm','DD Exhaustion') and 870<=mins<900: return False
        if sn in('Skew Charm','DD Exhaustion') and mins>=930: return False
        if sn=='BofA Scalp' and mins>=870: return False
        return True
    def v13Bull():
        if isLong: return False
        if sn not in('Skew Charm','DD Exhaustion'): return False
        ga=l['v13_gex_above'] or 0; dn=l['v13_dd_near'] or 0
        return ga>=75 or dn>=3000000000
    def v13Vanna():
        c=l['vanna_cliff_side']; p=l['vanna_peak_side']
        if c is None: return False
        if not isLong:
            if sn=='DD Exhaustion' and c=='A' and p=='B': return True
            if sn=='Skew Charm' and c=='A' and p=='B': return True
            if sn=='AG Short' and c=='B' and p=='A': return True
        return False
    def v13DDQ():
        if sn!='DD Exhaustion': return False
        if isLong:
            if align>=3: return True
            if (l['vix'] or 0)>=22: return True
            if para in('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY'): return True
            if grade=='C': return True
        else:
            if para=='BOFA-PURE': return True
            if grade=='A+': return True
            if grade=='C': return True
        return False
    def scLongAlignBlock():
        if sn!='Skew Charm' or not isLong: return False
        return align==3 and para in('GEX-LIS','AG-LIS','AG-PURE','BOFA-MESSY')
    def v10BaseV14():
        if isLong:
            if sn=='Skew Charm':
                if para=='SIDIAL-EXTREME' and mins is not None and 840<=mins<900: return False
                if scLongAlignBlock(): return False
                return True
            if para=='SIDIAL-EXTREME' and mins is not None and 840<=mins<900: return False
            if align<2: return False
            if (l['vix'] or 0)>22 and (l['overvix'] if l['overvix'] is not None else -99)<2: return False
            return True
        if sn in('Skew Charm','DD Exhaustion') and para=='GEX-LIS': return False
        if sn=='Skew Charm': return True
        if sn=='AG Short': return True
        if sn=='DD Exhaustion' and align!=0: return True
        return False
    def isOpex():
        if not et: return False
        return et.weekday()==4 and 15<=et.day<=21   # Friday=4 in python
    # ---- v16 ----
    if sn not in('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion'): return False
    if sn=='DD Exhaustion' and not isLong: return False
    if sn=='AG Short' and para=='AG-TARGET': return False
    if isLong and para=='GEX-TARGET' and et and et.hour>=13: return False
    if sn=='DD Exhaustion' and isLong:
        if para=='SIDIAL-EXTREME' and mins is not None and 840<=mins<900: return False
        if align<0: return False
        if align>=3: return False
        if (l['vix'] or 0)>=22: return False
        if para in('GEX-LIS','AG-LIS','AG-PURE','BofA-LIS','BOFA-MESSY'): return False
        if grade=='C': return False
        return True
    if not gapFilter(): return False
    if sn=='Skew Charm' and grade in('C','LOG'): return False
    if sn in('IV Momentum','Vanna Butterfly'): return False
    if not v11(): return False
    if v13Bull(): return False
    if v13Vanna(): return False
    if v13DDQ(): return False
    if sn=='ES Absorption':
        if grade not in('A','A+'): return False
        if para in('AG-TARGET','AG-LIS'): return False
        if mins is not None and mins>=945: return False
        if isLong and align<0: return False
        if not isLong and align>0: return False
        if not isLong and mins is not None and mins>=840: return False
        return True
    if sn=='Skew Charm' and isLong and para=='GEX-LIS': return False
    if sn=='Skew Charm' and isLong and isOpex(): return False
    if sn=='AG Short' and isOpex(): return False
    return v10BaseV14()

rows=C.execute(text("""SELECT id, setup_name, direction, greek_alignment, grade, paradigm, vix, overvix, ts,
   v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, outcome_pnl, outcome_result
 FROM setup_log WHERE ts >= '2026-02-01' ORDER BY ts""")).mappings().all()
passed=[dict(r) for r in rows if passes_v16(r)]
res=[p for p in passed if p['outcome_result'] in ('WIN','LOSS') and p['outcome_pnl'] is not None]
w=sum(1 for p in res if p['outcome_result']=='WIN'); l_=sum(1 for p in res if p['outcome_result']=='LOSS')
pts=sum(float(p['outcome_pnl']) for p in res)
allres=[p for p in passed if p['outcome_pnl'] is not None]
print(f"V16-passing total: {len(passed)} | with outcome_pnl: {len(allres)} | WIN/LOSS resolved: {len(res)}")
print(f"  WIN {w} / LOSS {l_} / WR {100*w/(w+l_):.0f}% / pts {pts:+.1f}")
print(f"  (all-outcome pts incl EXPIRED: {sum(float(p['outcome_pnl']) for p in allres):+.1f}, n={len(allres)})")
print(f"  TARGET: 920 / 619W-300L / 67% / +3408.1")
# by setup
from collections import Counter
cnt=Counter(p['setup_name'] for p in res); print("  by setup (resolved):",dict(cnt))
