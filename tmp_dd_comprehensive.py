"""
Comprehensive DD Exhaustion analysis — ALL Volland metrics.
Uses batch queries for performance (not per-trade).
"""
import os, json, re, sys
from datetime import datetime, timedelta
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL)

def q(sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def parse_money(s):
    if not s: return None
    s = str(s).strip().replace(",", "").replace("$", "")
    neg = s.startswith("-")
    s = s.lstrip("-+")
    try: return float(s) * (-1 if neg else 1)
    except: return None

# ─── 1. Get ALL DD Exhaustion trades ────────────────────────────────────────
trades = q("""
    SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, ts as created_at,
           direction, grade, score, spot, outcome_result, outcome_pnl,
           outcome_first_event, outcome_max_profit, outcome_max_loss,
           paradigm, vix, comments
    FROM setup_log WHERE setup_name = 'DD Exhaustion' AND outcome_result IS NOT NULL ORDER BY ts
""")
print(f"\n{'='*80}")
print(f"DD EXHAUSTION COMPREHENSIVE ANALYSIS -- {len(trades)} trades")
print(f"{'='*80}")

# ─── 2. Batch fetch Volland snapshots (all at once) ────────────────────────
print("Fetching Volland snapshots...")
vol_all = q("""
    SELECT ts,
           payload->'statistics'->>'paradigm' as paradigm,
           payload->'statistics'->>'lis' as lis,
           payload->'statistics'->>'aggregatedCharm' as agg_charm,
           payload->'statistics'->>'aggregatedDeltaDecay' as agg_dd,
           payload->'statistics'->>'spotVolBeta' as svb
    FROM volland_snapshots
    WHERE payload->'statistics' IS NOT NULL
      AND payload->'statistics'->>'paradigm' IS NOT NULL
    ORDER BY ts
""")
print(f"  {len(vol_all)} volland snapshots loaded")

# ─── 3. Batch fetch vanna ALL data ─────────────────────────────────────────
print("Fetching vanna data...")
vanna_all = q("""
    SELECT ts_utc,
           SUM(value) as vanna_net,
           SUM(CASE WHEN value > 0 THEN value ELSE 0 END) as vanna_pos,
           SUM(CASE WHEN value < 0 THEN value ELSE 0 END) as vanna_neg
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'ALL'
    GROUP BY ts_utc ORDER BY ts_utc
""")
print(f"  {len(vanna_all)} vanna snapshots loaded")

# ─── 4. Batch fetch vanna by type ──────────────────────────────────────────
print("Fetching vanna by type...")
vanna_type_all = q("""
    SELECT ts_utc, expiration_option, SUM(value) as total
    FROM volland_exposure_points
    WHERE greek = 'vanna'
    GROUP BY ts_utc, expiration_option ORDER BY ts_utc
""")
# Build lookup: ts -> {type: total}
vanna_type_map = {}
for r in vanna_type_all:
    ts_key = r['ts_utc']
    if ts_key not in vanna_type_map:
        vanna_type_map[ts_key] = {}
    vanna_type_map[ts_key][r['expiration_option'] or 'NULL'] = float(r['total'])
print(f"  {len(vanna_type_map)} vanna-type snapshots loaded")

# ─── 5. Batch fetch DD concentration ───────────────────────────────────────
print("Fetching DD concentration data...")
dd_conc_all = q("""
    SELECT ts_utc, strike, value
    FROM volland_exposure_points
    WHERE greek = 'deltaDecay'
    ORDER BY ts_utc, ABS(value) DESC
""")
# Build lookup: ts -> [values sorted by |value| desc]
dd_conc_map = {}
for r in dd_conc_all:
    ts_key = r['ts_utc']
    if ts_key not in dd_conc_map:
        dd_conc_map[ts_key] = []
    if r['value']:
        dd_conc_map[ts_key].append(float(r['value']))
print(f"  {len(dd_conc_map)} DD exposure snapshots loaded")

# ─── 6. Batch fetch charm exposure ─────────────────────────────────────────
print("Fetching charm exposure data...")
charm_exp_all = q("""
    SELECT ts_utc, SUM(value) as total,
           SUM(CASE WHEN value > 0 THEN value ELSE 0 END) as pos_total,
           SUM(CASE WHEN value < 0 THEN value ELSE 0 END) as neg_total
    FROM volland_exposure_points WHERE greek = 'charm'
    GROUP BY ts_utc ORDER BY ts_utc
""")
print(f"  {len(charm_exp_all)} charm snapshots loaded")

# ─── 7. Batch fetch gamma by type ──────────────────────────────────────────
print("Fetching gamma data...")
gamma_type_all = q("""
    SELECT ts_utc, expiration_option, SUM(value) as total
    FROM volland_exposure_points WHERE greek = 'gamma'
    GROUP BY ts_utc, expiration_option ORDER BY ts_utc
""")
gamma_type_map = {}
for r in gamma_type_all:
    ts_key = r['ts_utc']
    if ts_key not in gamma_type_map:
        gamma_type_map[ts_key] = {}
    gamma_type_map[ts_key][r['expiration_option'] or 'NULL'] = float(r['total'])
print(f"  {len(gamma_type_map)} gamma snapshots loaded")

# ─── Helper: find nearest snapshot ──────────────────────────────────────────
from bisect import bisect_left

def find_nearest(sorted_list, ts_key, ts_field, max_delta_s=300):
    """Find nearest item in sorted list within max_delta_s seconds."""
    # Simple linear scan (data is sorted)
    best = None
    best_delta = max_delta_s + 1
    for item in sorted_list:
        delta = abs((item[ts_field] - ts_key).total_seconds())
        if delta < best_delta:
            best = item
            best_delta = delta
        elif delta > best_delta + 600:
            break  # sorted, past the point
    return best if best_delta <= max_delta_s else None

def find_nearest_pair(sorted_list, ts_key, ts_field, max_delta_s=600):
    """Find TWO nearest items before ts_key."""
    pair = []
    for item in reversed(sorted_list):
        delta = (ts_key - item[ts_field]).total_seconds()
        if 0 <= delta <= max_delta_s:
            pair.append(item)
            if len(pair) == 2:
                break
        elif delta > max_delta_s:
            break
    return pair

def find_nearest_ts(sorted_list, ts_key, ts_field, max_delta_s=300):
    """Find nearest ts in a sorted list of dicts."""
    best = None
    best_delta = max_delta_s + 1
    for item in sorted_list:
        delta = abs((item[ts_field] - ts_key).total_seconds())
        if delta < best_delta:
            best = item
            best_delta = delta
    return best if best_delta <= max_delta_s else None

def find_nearest_map(ts_map, ts_key, max_delta_s=300):
    """Find nearest key in a dict keyed by timestamps."""
    best_key = None
    best_delta = max_delta_s + 1
    for k in ts_map:
        delta = abs((k - ts_key).total_seconds())
        if delta < best_delta:
            best_key = k
            best_delta = delta
    return best_key if best_delta <= max_delta_s else None

# ─── 8. Enrich trades ──────────────────────────────────────────────────────
print("\nEnriching trades...")
enriched = []
for t in trades:
    row = dict(t)
    ts = t['created_at']  # UTC timestamp

    # Volland snapshot (nearest)
    vol = find_nearest(vol_all, ts, 'ts', 300)
    if vol:
        row['vol_paradigm'] = vol['paradigm']
        row['vol_lis'] = vol['lis']
        row['vol_svb'] = vol['svb']
        row['charm_value'] = parse_money(vol['agg_charm'])
        row['dd_value_raw'] = parse_money(vol['agg_dd'])
    else:
        row['vol_paradigm'] = row['vol_lis'] = row['vol_svb'] = None
        row['charm_value'] = row['dd_value_raw'] = None

    # DD shift from two nearest snapshots
    pair = find_nearest_pair(vol_all, ts, 'ts', 600)
    if len(pair) >= 2:
        dd_curr = parse_money(pair[0]['agg_dd'])
        dd_prev = parse_money(pair[1]['agg_dd'])
        if dd_curr is not None and dd_prev is not None:
            row['dd_value'] = dd_curr
            row['dd_shift'] = dd_curr - dd_prev
        else:
            row['dd_value'] = dd_curr
            row['dd_shift'] = None
        if row['charm_value'] is None:
            row['charm_value'] = parse_money(pair[0]['agg_charm'])
    elif len(pair) == 1:
        row['dd_value'] = parse_money(pair[0]['agg_dd'])
        row['dd_shift'] = None
    else:
        row['dd_value'] = row['dd_shift'] = None

    # Vanna
    vn = find_nearest_ts(vanna_all, ts, 'ts_utc', 300)
    if vn:
        row['vanna_net'] = float(vn['vanna_net']) if vn['vanna_net'] else None
        row['vanna_pos'] = float(vn['vanna_pos']) if vn['vanna_pos'] else None
        row['vanna_neg'] = float(vn['vanna_neg']) if vn['vanna_neg'] else None
    else:
        row['vanna_net'] = row['vanna_pos'] = row['vanna_neg'] = None

    # Vanna by type
    vt_key = find_nearest_map(vanna_type_map, ts, 300)
    row['vanna_by_type'] = vanna_type_map.get(vt_key, {}) if vt_key else {}

    # DD concentration
    dc_key = find_nearest_map(dd_conc_map, ts, 300)
    if dc_key and dd_conc_map[dc_key]:
        dd_vals = dd_conc_map[dc_key]  # already sorted by |value| desc
        total_abs = sum(abs(v) for v in dd_vals)
        if total_abs > 0 and len(dd_vals) >= 3:
            top3_abs = sum(abs(dd_vals[i]) for i in range(min(3, len(dd_vals))))
            row['dd_concentration'] = round(top3_abs / total_abs * 100, 1)
        else:
            row['dd_concentration'] = None
    else:
        row['dd_concentration'] = None

    # Charm exposure
    ce = find_nearest_ts(charm_exp_all, ts, 'ts_utc', 300)
    if ce and ce['total'] is not None:
        row['charm_total_exposure'] = float(ce['total'])
    else:
        row['charm_total_exposure'] = None

    # Gamma by type
    gt_key = find_nearest_map(gamma_type_map, ts, 300)
    row['gamma_by_type'] = gamma_type_map.get(gt_key, {}) if gt_key else {}

    # Time features
    if t['ts_et']:
        row['hour'] = t['ts_et'].hour
        row['minute'] = t['ts_et'].minute
        row['date_str'] = t['ts_et'].strftime('%Y-%m-%d')
    else:
        row['hour'] = row['minute'] = None
        row['date_str'] = '?'

    # LIS distance
    if row.get('vol_lis') and row.get('spot'):
        try:
            lis_val = float(str(row['vol_lis']).replace(',', ''))
            row['lis_distance'] = round(float(row['spot']) - lis_val, 2)
            row['above_lis'] = row['lis_distance'] > 0
        except:
            row['lis_distance'] = row['above_lis'] = None
    else:
        row['lis_distance'] = row['above_lis'] = None

    enriched.append(row)

dd_shift_n = sum(1 for t in enriched if t.get('dd_shift') is not None)
charm_n = sum(1 for t in enriched if t.get('charm_value') is not None)
vanna_n = sum(1 for t in enriched if t.get('vanna_net') is not None)
print(f"Enriched {len(enriched)} trades | DD shift: {dd_shift_n} | Charm: {charm_n} | Vanna: {vanna_n}")

# ─── Stats helpers ──────────────────────────────────────────────────────────
def stats(tl, label=""):
    if not tl: return {"n":0,"wins":0,"losses":0,"expired":0,"wr":0,"pnl":0,"avg":0,"label":label}
    n=len(tl)
    w=sum(1 for t in tl if t.get('outcome_result') in ('WIN','WIN_TRAIL'))
    l=sum(1 for t in tl if t.get('outcome_result')=='LOSS')
    e=sum(1 for t in tl if t.get('outcome_result')=='EXPIRED')
    p=sum(float(t.get('outcome_pnl') or 0) for t in tl)
    wr=w/max(w+l,1)*100
    return {"n":n,"wins":w,"losses":l,"expired":e,"wr":round(wr,1),"pnl":round(p,1),"avg":round(p/n,1),"label":label}

def ps(s):
    print(f"  {s['label']:48s} | N={s['n']:3d} | {s['wins']:2d}W/{s['losses']:2d}L/{s['expired']:2d}E | WR={s['wr']:5.1f}% | PnL={s['pnl']:+8.1f} | Avg={s['avg']:+6.1f}")

# ═══════════════════════════════════════════════════════════════════════════
# ANALYSIS SECTIONS
# ═══════════════════════════════════════════════════════════════════════════

print(f"\n{'='*90}")
print("BASELINE")
ps(stats(enriched, "ALL DD Exhaustion"))
ps(stats([t for t in enriched if t['direction']=='long'], "LONG"))
ps(stats([t for t in enriched if t['direction']=='short'], "SHORT"))

# ─── TIME OF DAY ────────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 1: TIME OF DAY")
for h in range(10, 16):
    b=[t for t in enriched if t.get('hour')==h]
    if b: ps(stats(b, f"  Hour {h}:00-{h}:59"))
print()
for c in [13,14,15]:
    keep=[t for t in enriched if t.get('hour',99)<c]
    block=[t for t in enriched if t.get('hour',99)>=c]
    ps(stats(keep, f"  KEEP before {c}:00"))
    ps(stats(block, f"  BLOCK after {c}:00"))
    bs=stats(block)
    print(f"    >> Blocking saves: {bs['pnl']:+.1f} pts ({bs['wins']}W killed)")

# ─── PARADIGM ───────────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 2: PARADIGM")
paradigms={}
for t in enriched:
    p=str(t.get('vol_paradigm') or t.get('paradigm') or 'UNKNOWN').upper().strip()
    paradigms.setdefault(p,[]).append(t)
for p in sorted(paradigms.keys(), key=lambda x:len(paradigms[x]), reverse=True):
    ps(stats(paradigms[p], f"  {p}"))

print()
bofa=[t for t in enriched if 'BOFA' in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper()]
messy=[t for t in enriched if 'MESSY' in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper()]
ps(stats(bofa, "  BOFA group"))
ps(stats(messy, "  MESSY group"))

# ─── CHARM ──────────────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 3: CHARM (absolute value)")
ct=[t for t in enriched if t.get('charm_value') is not None]
print(f"  Trades with charm data: {len(ct)}")
for lo,hi,lab in [(0,20e6,"<$20M"),(20e6,50e6,"$20-50M"),(50e6,100e6,"$50-100M"),
                   (100e6,200e6,"$100-200M"),(200e6,500e6,"$200-500M"),(500e6,float('inf'),"$500M+")]:
    b=[t for t in ct if lo<=abs(t['charm_value'])<hi]
    if b: ps(stats(b, f"  Charm {lab}"))
print()
for ceil in [50e6,100e6,200e6,250e6,500e6]:
    keep=[t for t in ct if abs(t['charm_value'])<=ceil]
    block=[t for t in ct if abs(t['charm_value'])>ceil]
    ps(stats(keep, f"  Charm <= ${ceil/1e6:.0f}M KEEP"))
    if block:
        bs=stats(block)
        print(f"    BLOCK: N={bs['n']}, {bs['wins']}W/{bs['losses']}L, PnL={bs['pnl']:+.1f}")

# ─── VANNA ──────────────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 4: VANNA ALL (net)")
vt=[t for t in enriched if t.get('vanna_net') is not None]
print(f"  Trades with vanna data: {len(vt)}")
ps(stats([t for t in vt if t['vanna_net']>0], "  Vanna net POSITIVE"))
ps(stats([t for t in vt if t['vanna_net']<=0], "  Vanna net NEGATIVE"))
print()
# Direction alignment
al=[t for t in vt if (t['direction']=='long' and t['vanna_net']>0) or (t['direction']=='short' and t['vanna_net']<0)]
op=[t for t in vt if (t['direction']=='long' and t['vanna_net']<0) or (t['direction']=='short' and t['vanna_net']>0)]
ps(stats(al, "  Vanna WITH direction"))
ps(stats(op, "  Vanna AGAINST direction (contrarian)"))

# Vanna by sub-type
print()
for vtype in ['ALL','CALL','PUT','C0DTE','P0DTE']:
    has=[t for t in enriched if vtype in t.get('vanna_by_type',{})]
    if has:
        pos=[t for t in has if t['vanna_by_type'][vtype]>0]
        neg=[t for t in has if t['vanna_by_type'][vtype]<=0]
        if pos: ps(stats(pos, f"  Vanna {vtype} positive"))
        if neg: ps(stats(neg, f"  Vanna {vtype} negative"))

# ─── DD SHIFT MAGNITUDE ────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 5: DD SHIFT MAGNITUDE")
st=[t for t in enriched if t.get('dd_shift') is not None]
print(f"  Trades with DD shift data: {len(st)}")
for lo,hi,lab in [(0,200e6,"<$200M"),(200e6,500e6,"$200-500M"),(500e6,1e9,"$500M-1B"),
                   (1e9,2e9,"$1-2B"),(2e9,3e9,"$2-3B"),(3e9,5e9,"$3-5B"),(5e9,float('inf'),"$5B+")]:
    b=[t for t in st if lo<=abs(t['dd_shift'])<hi]
    if b: ps(stats(b, f"  DD shift {lab}"))
print()
for thresh in [200e6,500e6,1e9,2e9]:
    keep=[t for t in st if abs(t['dd_shift'])>=thresh]
    block=[t for t in st if abs(t['dd_shift'])<thresh]
    ps(stats(keep, f"  DD shift >= ${thresh/1e6:.0f}M KEEP"))
    if block:
        bs=stats(block)
        print(f"    BLOCK: N={bs['n']}, {bs['wins']}W/{bs['losses']}L, PnL={bs['pnl']:+.1f}")

# ─── DD CONCENTRATION ──────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 6: DD CONCENTRATION")
ct2=[t for t in enriched if t.get('dd_concentration') is not None]
print(f"  Trades with DD conc data: {len(ct2)}")
for lo,hi,lab in [(0,40,"<40%"),(40,60,"40-60%"),(60,75,"60-75%"),(75,90,"75-90%"),(90,100.1,"90%+")]:
    b=[t for t in ct2 if lo<=t['dd_concentration']<hi]
    if b: ps(stats(b, f"  DD conc {lab}"))

# ─── LIS DISTANCE ──────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 7: LIS DISTANCE + SIDE")
lt=[t for t in enriched if t.get('lis_distance') is not None]
print(f"  Trades with LIS data: {len(lt)}")
ps(stats([t for t in lt if t['direction']=='long' and t['above_lis']], "  LONG above LIS"))
ps(stats([t for t in lt if t['direction']=='long' and not t['above_lis']], "  LONG below LIS"))
ps(stats([t for t in lt if t['direction']=='short' and t['above_lis']], "  SHORT above LIS"))
ps(stats([t for t in lt if t['direction']=='short' and not t['above_lis']], "  SHORT below LIS"))
print()
rs=[t for t in lt if (t['direction']=='long' and not t['above_lis']) or (t['direction']=='short' and t['above_lis'])]
ws=[t for t in lt if (t['direction']=='long' and t['above_lis']) or (t['direction']=='short' and not t['above_lis'])]
ps(stats(rs, "  RIGHT SIDE of LIS (long below / short above)"))
ps(stats(ws, "  WRONG SIDE of LIS"))
for lo,hi,lab in [(0,5,"0-5pt"),(5,15,"5-15pt"),(15,30,"15-30pt"),(30,float('inf'),"30+pt")]:
    b=[t for t in lt if lo<=abs(t['lis_distance'])<hi]
    if b: ps(stats(b, f"  LIS dist {lab}"))

# ─── SPOT-VOL BETA ─────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 8: SPOT-VOL BETA")
svb_t=[t for t in enriched if t.get('vol_svb') is not None]
print(f"  Trades with SVB data: {len(svb_t)}")
for t in svb_t:
    try: t['svb_val']=float(t['vol_svb'])
    except: t['svb_val']=None
sv=[t for t in svb_t if t.get('svb_val') is not None]
if sv:
    ps(stats([t for t in sv if t['svb_val']<-0.5], "  SVB < -0.5 (strong neg)"))
    ps(stats([t for t in sv if -0.5<=t['svb_val']<0], "  SVB -0.5 to 0"))
    ps(stats([t for t in sv if t['svb_val']>=0], "  SVB >= 0"))

# ─── VIX ────────────────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 9: VIX")
vx=[t for t in enriched if t.get('vix') is not None]
print(f"  Trades with VIX data: {len(vx)}")
for lo,hi,lab in [(0,15,"<15"),(15,18,"15-18"),(18,22,"18-22"),(22,float('inf'),"22+")]:
    b=[t for t in vx if lo<=float(t['vix'])<hi]
    if b: ps(stats(b, f"  VIX {lab}"))

# ─── GAMMA ──────────────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 10: GAMMA EXPOSURE")
for gtype in ['ALL','CALL','PUT','C0DTE','P0DTE']:
    has=[t for t in enriched if gtype in t.get('gamma_by_type',{})]
    if has:
        pos=[t for t in has if t['gamma_by_type'][gtype]>0]
        neg=[t for t in has if t['gamma_by_type'][gtype]<=0]
        if pos: ps(stats(pos, f"  Gamma {gtype} positive"))
        if neg: ps(stats(neg, f"  Gamma {gtype} negative"))

# ─── DIRECTION x TIME ──────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 11: DIRECTION x TIME")
for d in ['long','short']:
    for h in range(10,16):
        b=[t for t in enriched if t['direction']==d and t.get('hour')==h]
        if b: ps(stats(b, f"  {d.upper()} @ {h}:00"))

# ─── DD ABSOLUTE VALUE ─────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("FILTER 12: DD ABSOLUTE VALUE")
dv=[t for t in enriched if t.get('dd_value') is not None]
print(f"  Trades with DD value: {len(dv)}")
for lo,hi,lab in [(float('-inf'),-5e9,"< -$5B"),(-5e9,-2e9,"-$5B to -$2B"),(-2e9,0,"-$2B to 0"),
                   (0,2e9,"0 to +$2B"),(2e9,5e9,"+$2B to +$5B"),(5e9,float('inf'),"> +$5B")]:
    b=[t for t in dv if lo<=t['dd_value']<hi]
    if b: ps(stats(b, f"  DD value {lab}"))

# ─── MAX PROFIT ─────────────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("MAX PROFIT / MAX LOSS DISTRIBUTION")
mp=[t for t in enriched if t.get('outcome_max_profit') is not None]
winners=[t for t in mp if t['outcome_result'] in ('WIN','WIN_TRAIL')]
losers=[t for t in mp if t['outcome_result']=='LOSS']
if winners:
    avg_mp=sum(float(t['outcome_max_profit']) for t in winners)/len(winners)
    print(f"  Winners avg max_profit: {avg_mp:.1f} pts ({len(winners)} trades)")
if losers:
    avg_mp_l=sum(float(t['outcome_max_profit']) for t in losers)/len(losers)
    green=[t for t in losers if float(t['outcome_max_profit'])>0]
    never=[t for t in losers if float(t['outcome_max_profit'])<=0]
    print(f"  Losers avg max_profit: {avg_mp_l:.1f} pts ({len(losers)} trades)")
    print(f"  Losers went green first: {len(green)}/{len(losers)} ({len(green)/len(losers)*100:.0f}%)")
    print(f"  Losers NEVER went green: {len(never)}/{len(losers)} ({len(never)/len(losers)*100:.0f}%)")

# ═══════════════════════════════════════════════════════════════════════════
# COMBINED FILTER SIMULATIONS
# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*90}")
print("COMBINED FILTER SIMULATIONS")
print(f"{'='*90}")

baseline=stats(enriched, "BASELINE")

def test_combo(trades, filters, label):
    remaining=list(trades); blocked=[]
    for fn,ff in filters.items():
        nr=[]
        for t in remaining:
            if ff(t): nr.append(t)
            else: blocked.append(t)
        remaining=nr
    s=stats(remaining,label)
    bw=sum(1 for t in blocked if t.get('outcome_result') in ('WIN','WIN_TRAIL'))
    bs=stats(blocked)
    ps(s)
    print(f"    Blocked: {bs['n']} trades ({bw}W killed), PnL saved={bs['pnl']:+.1f}, net delta={s['pnl']-baseline['pnl']:+.1f}")
    return remaining

print(f"\n--- Individual filters ---")
test_combo(enriched, {"F1": lambda t: t.get('hour',99)<14}, "F1: time < 14:00")
test_combo(enriched, {"F2": lambda t: 'BOFA-PURE' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper()}, "F2: no BOFA-PURE")
test_combo(enriched, {"F3": lambda t: t.get('dd_shift') is None or abs(t['dd_shift'])>=500e6}, "F3: shift >= $500M")
test_combo(enriched, {"F4": lambda t: t.get('charm_value') is None or abs(t['charm_value'])>=50e6}, "F4: charm >= $50M")
test_combo(enriched, {"F5": lambda t: t.get('charm_value') is None or abs(t['charm_value'])<=250e6}, "F5: charm <= $250M")
test_combo(enriched, {"F6": lambda t: t.get('dd_concentration') is None or t['dd_concentration']<75}, "F6: DD conc < 75%")
test_combo(enriched, {"F7": lambda t: t.get('vanna_net') is None or t['vanna_net']<0}, "F7: vanna ALL negative")

print(f"\n--- Combo filters ---")
test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "F2": lambda t: 'BOFA-PURE' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper(),
}, "A: time<14 + no BOFA-PURE")

test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "F5": lambda t: t.get('charm_value') is None or abs(t['charm_value'])<=250e6,
}, "B: time<14 + charm<=$250M")

test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "F2": lambda t: 'BOFA-PURE' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper(),
    "F5": lambda t: t.get('charm_value') is None or abs(t['charm_value'])<=250e6,
}, "C: time<14 + no BOFA-PURE + charm<=$250M")

test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "F2": lambda t: 'BOFA-PURE' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper(),
    "F3": lambda t: t.get('dd_shift') is None or abs(t['dd_shift'])>=500e6,
}, "D: time<14 + no BOFA-PURE + shift>=$500M")

test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "F2": lambda t: 'BOFA-PURE' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper(),
    "F5": lambda t: t.get('charm_value') is None or abs(t['charm_value'])<=250e6,
    "F6": lambda t: t.get('dd_concentration') is None or t['dd_concentration']<75,
}, "E: time<14 + no BOFA + charm<=$250M + conc<75%")

test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "F2": lambda t: 'BOFA-PURE' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper(),
    "F5": lambda t: t.get('charm_value') is None or abs(t['charm_value'])<=250e6,
    "F7": lambda t: t.get('vanna_net') is None or t['vanna_net']<0,
}, "F: time<14 + no BOFA + charm<=$250M + vanna neg")

test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "F2": lambda t: 'BOFA-PURE' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper(),
    "F3": lambda t: t.get('dd_shift') is None or abs(t['dd_shift'])>=500e6,
    "F5": lambda t: t.get('charm_value') is None or abs(t['charm_value'])<=250e6,
    "F6": lambda t: t.get('dd_concentration') is None or t['dd_concentration']<75,
}, "G: ALL (time+para+shift+charm+conc)")

test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "LIS": lambda t: t.get('lis_distance') is None or (
        (t['direction']=='long' and t['lis_distance']<=0) or
        (t['direction']=='short' and t['lis_distance']>=0)),
}, "H: time<14 + right side of LIS")

test_combo(enriched, {
    "F1": lambda t: t.get('hour',99)<14,
    "F2": lambda t: 'BOFA-PURE' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper(),
    "F5": lambda t: t.get('charm_value') is None or abs(t['charm_value'])<=250e6,
    "LIS": lambda t: t.get('lis_distance') is None or (
        (t['direction']=='long' and t['lis_distance']<=0) or
        (t['direction']=='short' and t['lis_distance']>=0)),
}, "I: time<14 + no BOFA + charm<=$250M + right LIS")

# ─── TRADE DETAIL TABLE ────────────────────────────────────────────────────
print(f"\n{'='*90}")
print("TRADE DETAIL TABLE")
print(f"{'='*90}")
hdr=f"{'ID':>4} | {'Date':10} | {'Time':5} | {'Dir':5} | {'Res':8} | {'PnL':>7} | {'MaxP':>6} | {'DDshift':>10} | {'DDval':>10} | {'Charm':>10} | {'Paradigm':18} | {'Vanna':>8} | {'DDc%':>5} | {'LIS':>7}"
print(hdr)
print('-'*len(hdr))

for t in enriched:
    dd_s=f"{t['dd_shift']/1e6:+.0f}M" if t.get('dd_shift') is not None else "?"
    dd_v=f"{t['dd_value']/1e9:+.1f}B" if t.get('dd_value') is not None else "?"
    ch=f"{t['charm_value']/1e6:+.0f}M" if t.get('charm_value') is not None else "?"
    para=str(t.get('vol_paradigm') or t.get('paradigm') or '?')[:18]
    vn=f"{t['vanna_net']:+.0f}" if t.get('vanna_net') is not None else "?"
    dc=f"{t['dd_concentration']:.0f}%" if t.get('dd_concentration') is not None else "?"
    ld=f"{t['lis_distance']:+.1f}" if t.get('lis_distance') is not None else "?"
    ts_str=t['ts_et'].strftime('%H:%M') if t.get('ts_et') else '?'
    mp=f"{float(t['outcome_max_profit']):.0f}" if t.get('outcome_max_profit') is not None else "?"
    pnl=float(t.get('outcome_pnl') or 0)
    print(f"{t['id']:>4} | {t['date_str']:10} | {ts_str:5} | {t['direction']:5} | {t['outcome_result']:8} | {pnl:+7.1f} | {mp:>6} | {dd_s:>10} | {dd_v:>10} | {ch:>10} | {para:18} | {vn:>8} | {dc:>5} | {ld:>7}")

# ─── TODAY'S TRADES + ALL-TIME SUMMARY ──────────────────────────────────────
print(f"\n{'='*90}")
print("TODAY'S TRADES (all setups)")
today = q("""
    SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et,
           setup_name, direction, grade, score, spot,
           outcome_result, outcome_pnl, outcome_max_profit
    FROM setup_log WHERE ts >= CURRENT_DATE ORDER BY ts
""")
if today:
    tp=0
    for t in today:
        p=float(t.get('outcome_pnl') or 0); tp+=p
        res=t.get('outcome_result') or 'OPEN'
        ts_s=t['ts_et'].strftime('%H:%M') if t.get('ts_et') else '?'
        print(f"  #{t['id']:>4} {ts_s} {t['setup_name']:20} {t['direction']:5} {t.get('grade',''):6} spot={float(t.get('spot',0)):.1f} => {res:8} {p:+7.1f}")
    print(f"\n  Today total: {tp:+.1f} pts across {len(today)} trades")
else:
    print("  No trades today")

print(f"\n{'='*90}")
print("ALL-TIME P&L SUMMARY (all setups)")
at = q("""
    SELECT setup_name, COUNT(*) as n,
           SUM(CASE WHEN outcome_result IN ('WIN','WIN_TRAIL') THEN 1 ELSE 0 END) as wins,
           SUM(CASE WHEN outcome_result = 'LOSS' THEN 1 ELSE 0 END) as losses,
           SUM(CASE WHEN outcome_result = 'EXPIRED' THEN 1 ELSE 0 END) as expired,
           SUM(COALESCE(outcome_pnl, 0)) as total_pnl
    FROM setup_log WHERE outcome_result IS NOT NULL GROUP BY setup_name ORDER BY total_pnl DESC
""")
gt=0; gn=0
for r in at:
    n=r['n']; w=r['wins']; l=r['losses']; e=r['expired']
    p=float(r['total_pnl']); wr=w/max(w+l,1)*100
    gt+=p; gn+=n
    print(f"  {r['setup_name']:20} | N={n:3d} | {w:2d}W/{l:2d}L/{e:2d}E | WR={wr:5.1f}% | PnL={p:+8.1f}")
print(f"  {'GRAND TOTAL':20} | N={gn:3d} |                       | PnL={gt:+8.1f}")

conn.close()
print(f"\n{'='*90}")
print("ANALYSIS COMPLETE")
