import os, sys, psycopg2
from zoneinfo import ZoneInfo
sys.path.insert(0, '.')
from app import live_filter as LF
ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"]); c.autocommit = True; cur = c.cursor()

cur.execute("""
 WITH closes AS (SELECT DISTINCT ON (date(ts AT TIME ZONE 'America/New_York')) date(ts AT TIME ZONE 'America/New_York') d, spot p FROM chain_snapshots WHERE spot IS NOT NULL ORDER BY date(ts AT TIME ZONE 'America/New_York'), ts DESC),
      opens AS (SELECT DISTINCT ON (date(ts AT TIME ZONE 'America/New_York')) date(ts AT TIME ZONE 'America/New_York') d, spot p FROM chain_snapshots WHERE spot IS NOT NULL AND (ts AT TIME ZONE 'America/New_York')::time>='09:30' ORDER BY date(ts AT TIME ZONE 'America/New_York'), ts ASC)
 SELECT o.d, o.p-c.p gap FROM opens o JOIN closes c ON c.d=(SELECT MAX(c2.d) FROM closes c2 WHERE c2.d<o.d)""")
GAPS = {str(d): round(float(g), 1) for d, g in cur.fetchall() if g is not None}

def etmins(ts):
    e = ts.astimezone(ET); return e.hour * 60 + e.minute
def isLongf(d): return d in ('long', 'bullish')
def A(l): return l['greek_alignment'] if l['greek_alignment'] is not None else 0

def gapFilter(l):
    ts = l['ts']
    if not ts or not isLongf(l['direction']): return True
    g = GAPS.get(ts.astimezone(ET).date().isoformat())
    if g is not None and abs(g) > 30 and etmins(ts) < 600: return False
    return True
def v11gates(l):
    m = etmins(l['ts']); sn = l['setup_name'] or ''
    if (sn in ('Skew Charm', 'DD Exhaustion')) and (870 <= m < 900): return False
    if (sn in ('Skew Charm', 'DD Exhaustion')) and m >= 930: return False
    if sn == 'BofA Scalp' and m >= 870: return False
    return True
def v13Bull(l):
    sn = l['setup_name'] or ''
    if isLongf(l['direction']): return False
    if sn not in ('Skew Charm', 'DD Exhaustion'): return False
    if (l['v13_gex_above'] or 0) >= 75: return True
    if (l['v13_dd_near'] or 0) >= 3000000000: return True
    return False
def v13Vanna(l):
    sn = l['setup_name'] or ''; cc = l['vanna_cliff_side']; p = l['vanna_peak_side']
    if cc is None: return False
    if not isLongf(l['direction']):
        if sn == 'DD Exhaustion' and cc == 'A' and p == 'B': return True
        if sn == 'Skew Charm' and cc == 'A' and p == 'B': return True
        if sn == 'AG Short' and cc == 'B' and p == 'A': return True
    return False
def v13DDQ(l):
    sn = l['setup_name'] or ''; align = A(l)
    if sn != 'DD Exhaustion': return False
    if isLongf(l['direction']):
        if align >= 3: return True
        if (l['vix'] or 0) >= 22: return True
        if l['paradigm'] in ('GEX-LIS', 'AG-LIS', 'AG-PURE', 'BofA-LIS', 'BOFA-MESSY'): return True
        if l['grade'] == 'C': return True
    else:
        if l['paradigm'] == 'BOFA-PURE': return True
        if l['grade'] == 'A+': return True
        if l['grade'] == 'C': return True
    return False
def scLongAlignBlockV14(l):
    sn = l['setup_name'] or ''; align = A(l)
    if sn != 'Skew Charm' or not isLongf(l['direction']): return False
    return align == 3 and l['paradigm'] in ('GEX-LIS', 'AG-LIS', 'AG-PURE', 'BOFA-MESSY')
def v10BaseV14(l):
    sn = l['setup_name'] or ''; align = A(l)
    if isLongf(l['direction']):
        if sn == 'Skew Charm':
            if l['paradigm'] == 'SIDIAL-EXTREME' and 840 <= etmins(l['ts']) < 900: return False
            if scLongAlignBlockV14(l): return False
            return True
        if l['paradigm'] == 'SIDIAL-EXTREME' and 840 <= etmins(l['ts']) < 900: return False
        if align < 2: return False
        if (l['vix'] or 0) > 22 and (l['overvix'] if l['overvix'] is not None else -99) < 2: return False
        return True
    if sn in ('Skew Charm', 'DD Exhaustion') and l['paradigm'] == 'GEX-LIS': return False
    if sn == 'Skew Charm': return True
    if sn == 'AG Short': return True
    if sn == 'DD Exhaustion' and align != 0: return True
    return False

def v7(l):
    align = A(l); sn = l['setup_name'] or ''
    if isLongf(l['direction']): return align >= 2
    if sn == 'Skew Charm': return True
    if sn == 'DD Exhaustion' and align != 0: return True
    return False
def v8(l):
    align = A(l); sn = l['setup_name'] or ''
    if isLongf(l['direction']):
        if align < 2: return False
        if (l['vix'] or 0) > 26 and (l['overvix'] if l['overvix'] is not None else -99) < 2: return False
        return True
    if sn == 'Skew Charm': return True
    if sn == 'AG Short': return True
    if sn == 'DD Exhaustion' and align != 0: return True
    return False
def v9(l):
    align = A(l); sn = l['setup_name'] or ''
    if isLongf(l['direction']):
        if align < 2: return False
        if sn == 'Skew Charm': return True
        if (l['vix'] or 0) > 22 and (l['overvix'] if l['overvix'] is not None else -99) < 2: return False
        return True
    if sn == 'Skew Charm': return True
    if sn == 'AG Short': return True
    if sn == 'DD Exhaustion' and align != 0: return True
    return False
def v12(l):
    sn = l['setup_name'] or ''; align = A(l)
    if l['ts'] and isLongf(l['direction']):
        g = GAPS.get(l['ts'].astimezone(ET).date().isoformat())
        if g is not None and abs(g) > 30 and etmins(l['ts']) < 600: return False
    if sn == 'Skew Charm' and l['grade'] in ('C', 'LOG'): return False
    if sn in ('VIX Divergence', 'IV Momentum', 'Vanna Butterfly'): return False
    m = etmins(l['ts'])
    if sn in ('Skew Charm', 'DD Exhaustion') and 870 <= m < 900: return False
    if sn in ('Skew Charm', 'DD Exhaustion') and m >= 930: return False
    if sn == 'BofA Scalp' and m >= 870: return False
    if isLongf(l['direction']):
        if l['paradigm'] == 'SIDIAL-EXTREME': return False
        if align < 2: return False
    else:
        if l['paradigm'] == 'GEX-LIS': return False
    return True
def v14(l):
    sn = l['setup_name'] or ''; align = A(l)
    if not gapFilter(l): return False
    if sn == 'Skew Charm' and l['grade'] in ('C', 'LOG'): return False
    if sn in ('IV Momentum', 'Vanna Butterfly'): return False
    if sn == 'VIX Divergence':
        if not isLongf(l['direction']): return False
        if l['grade'] == 'C': return False
        return True
    if not v11gates(l): return False
    if v13Bull(l): return False
    if v13Vanna(l): return False
    if v13DDQ(l): return False
    if sn == 'ES Absorption':
        if l['grade'] not in ('A', 'A+'): return False
        if l['paradigm'] in ('AG-TARGET', 'AG-LIS'): return False
        m = etmins(l['ts'])
        if m >= 945: return False
        if isLongf(l['direction']) and align < 0: return False
        if not isLongf(l['direction']) and align > 0: return False
        return True
    return v10BaseV14(l)

def v16(l): return LF.passes_v16(l, GAPS)
def v16sb(l): return LF.passes_v16_sb(l, GAPS)

FILTERS = {'v7': v7, 'v8': v8, 'v9': v9, 'v12': v12, 'v14': v14, 'v16': v16, 'v16sb': v16sb}
COLS = "id, setup_name, direction, greek_alignment, grade, paradigm, vix, overvix, ts, v13_gex_above, v13_dd_near, vanna_cliff_side, vanna_peak_side, basket_pct, outcome_pnl, mes_sim_outcome_pnl, outcome_result"
KEYS = [k.strip() for k in COLS.split(',')]

def load(month):
    cur.execute(f"SELECT {COLS} FROM setup_log WHERE to_char(ts AT TIME ZONE 'America/New_York','YYYY-MM')=%s ORDER BY ts", (month,))
    return [dict(zip(KEYS, r)) for r in cur.fetchall()]

def report(month):
    rows = load(month)
    print(f"\n===== {month}  ({len(rows)} signals) =====")
    print(f"{'filt':6} {'n':>4} {'chainSum':>9} | {'nMes':>4} {'chain(m)':>9} {'mesSum':>9} {'capt%':>6} | {'WR':>4}")
    for fn, fx in FILTERS.items():
        sel = [l for l in rows if fx(l)]
        chain = [float(l['outcome_pnl']) for l in sel if l['outcome_pnl'] is not None]
        m = [l for l in sel if l['mes_sim_outcome_pnl'] is not None and l['outcome_pnl'] is not None]
        chainM = sum(float(l['outcome_pnl']) for l in m)
        mesM = sum(float(l['mes_sim_outcome_pnl']) for l in m)
        dec = [l for l in sel if l['outcome_result'] in ('WIN', 'LOSS', 'EXPIRED', 'TIMEOUT')]
        wr = 100 * sum(1 for l in dec if l['outcome_result'] == 'WIN') / max(1, len(dec))
        capt = (mesM / chainM * 100) if chainM else float('nan')
        print(f"{fn:6} {len(chain):>4} {sum(chain):>9.1f} | {len(m):>4} {chainM:>9.1f} {mesM:>9.1f} {capt:>6.0f} | {wr:>4.0f}")

for mo in ['2026-07', '2026-06', '2026-05', '2026-04']:
    report(mo)
