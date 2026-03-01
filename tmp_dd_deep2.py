"""
DD Exhaustion deep dive — focused on WHEN it works vs fails.
Studies immediate losses vs winners, cross-tabulates key metrics.
"""
import os, json
from datetime import datetime, timedelta
from collections import defaultdict
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

# ─── Load trades ────────────────────────────────────────────────────────────
trades = q("""
    SELECT id, ts AT TIME ZONE 'America/New_York' as ts_et, ts as created_at,
           direction, grade, score, spot, outcome_result, outcome_pnl,
           outcome_first_event, outcome_max_profit, outcome_max_loss,
           paradigm, vix, comments
    FROM setup_log WHERE setup_name = 'DD Exhaustion' AND outcome_result IS NOT NULL ORDER BY ts
""")

# ─── Load Volland data (batch) ──────────────────────────────────────────────
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

vanna_all = q("""
    SELECT ts_utc, SUM(value) as vanna_net
    FROM volland_exposure_points
    WHERE greek = 'vanna' AND expiration_option = 'ALL'
    GROUP BY ts_utc ORDER BY ts_utc
""")

dd_conc_all = q("""
    SELECT ts_utc, strike, value
    FROM volland_exposure_points WHERE greek = 'deltaDecay'
    ORDER BY ts_utc, ABS(value) DESC
""")
dd_conc_map = {}
for r in dd_conc_all:
    dd_conc_map.setdefault(r['ts_utc'], [])
    if r['value']: dd_conc_map[r['ts_utc']].append(float(r['value']))

# ─── Nearest helpers ────────────────────────────────────────────────────────
def find_nearest(lst, ts, ts_field, max_s=300):
    best, bd = None, max_s+1
    for item in lst:
        d = abs((item[ts_field]-ts).total_seconds())
        if d < bd: best, bd = item, d
        elif d > bd+600: break
    return best if bd <= max_s else None

def find_nearest_map(m, ts, max_s=300):
    bk, bd = None, max_s+1
    for k in m:
        d = abs((k-ts).total_seconds())
        if d < bd: bk, bd = k, d
    return bk if bd <= max_s else None

# ─── Enrich ─────────────────────────────────────────────────────────────────
enriched = []
for t in trades:
    row = dict(t)
    ts = t['created_at']

    vol = find_nearest(vol_all, ts, 'ts', 300)
    row['charm_value'] = parse_money(vol['agg_charm']) if vol else None
    row['vol_paradigm'] = vol['paradigm'] if vol else None
    row['vol_lis'] = vol['lis'] if vol else None
    row['vol_svb'] = vol['svb'] if vol else None
    row['dd_value'] = parse_money(vol['agg_dd']) if vol else None

    vn = find_nearest(vanna_all, ts, 'ts_utc', 300)
    row['vanna_net'] = float(vn['vanna_net']) if vn and vn['vanna_net'] else None

    dc_key = find_nearest_map(dd_conc_map, ts, 300)
    if dc_key and dd_conc_map[dc_key]:
        vals = dd_conc_map[dc_key]
        total_abs = sum(abs(v) for v in vals)
        if total_abs > 0 and len(vals) >= 3:
            row['dd_concentration'] = round(sum(abs(vals[i]) for i in range(min(3,len(vals)))) / total_abs * 100, 1)
        else: row['dd_concentration'] = None
    else: row['dd_concentration'] = None

    if t['ts_et']:
        row['hour'] = t['ts_et'].hour
        row['minute'] = t['ts_et'].minute
        row['date_str'] = t['ts_et'].strftime('%Y-%m-%d')
        row['time_str'] = t['ts_et'].strftime('%H:%M')
    else:
        row['hour'] = row['minute'] = None

    # Classify trade quality
    mp = float(t.get('outcome_max_profit') or 0)
    ml = float(t.get('outcome_max_loss') or 0)
    res = t.get('outcome_result', '')
    pnl = float(t.get('outcome_pnl') or 0)
    row['max_profit'] = mp
    row['max_loss'] = ml
    row['pnl'] = pnl

    if res in ('WIN', 'WIN_TRAIL'):
        row['category'] = 'WINNER'
    elif res == 'LOSS' and mp <= 1:
        row['category'] = 'IMMEDIATE_LOSS'  # never went meaningfully green
    elif res == 'LOSS' and mp > 1:
        row['category'] = 'REVERSAL_LOSS'   # went green then reversed
    elif res == 'EXPIRED' and pnl > 0:
        row['category'] = 'EXPIRED_GREEN'
    elif res == 'EXPIRED':
        row['category'] = 'EXPIRED_RED'
    else:
        row['category'] = 'OTHER'

    # Paradigm group
    p = str(row.get('vol_paradigm') or row.get('paradigm') or '').upper()
    if 'BOFA' in p:
        row['para_group'] = 'BOFA'
    elif 'GEX' in p and 'AG' not in p:
        row['para_group'] = 'GEX'
    elif 'AG' in p:
        row['para_group'] = 'AG'
    elif 'SIDIAL' in p:
        row['para_group'] = 'SIDIAL'
    elif 'MESSY' in p:
        row['para_group'] = 'MESSY'
    else:
        row['para_group'] = 'OTHER'

    # Charm bucket
    cv = row.get('charm_value')
    if cv is not None:
        ac = abs(cv)
        if ac < 20e6: row['charm_bucket'] = '<20M'
        elif ac < 50e6: row['charm_bucket'] = '20-50M'
        elif ac < 100e6: row['charm_bucket'] = '50-100M'
        elif ac < 200e6: row['charm_bucket'] = '100-200M'
        else: row['charm_bucket'] = '200M+'
    else: row['charm_bucket'] = '?'

    # Vanna regime
    if row.get('vanna_net') is not None:
        row['vanna_regime'] = 'NEG' if row['vanna_net'] < 0 else 'POS'
        # Direction alignment
        if (row['direction']=='long' and row['vanna_net']>0) or (row['direction']=='short' and row['vanna_net']<0):
            row['vanna_alignment'] = 'WITH'
        else:
            row['vanna_alignment'] = 'AGAINST'
    else:
        row['vanna_regime'] = '?'
        row['vanna_alignment'] = '?'

    # DD concentration bucket
    dc = row.get('dd_concentration')
    if dc is not None:
        if dc < 35: row['conc_bucket'] = '<35%'
        elif dc < 45: row['conc_bucket'] = '35-45%'
        elif dc < 55: row['conc_bucket'] = '45-55%'
        else: row['conc_bucket'] = '55%+'
    else: row['conc_bucket'] = '?'

    enriched.append(row)

# ─── Stats ──────────────────────────────────────────────────────────────────
def stats(tl, label=""):
    if not tl: return {"n":0,"wins":0,"losses":0,"expired":0,"wr":0,"pnl":0,"avg":0,"label":label}
    n=len(tl)
    w=sum(1 for t in tl if t['category']=='WINNER')
    l=sum(1 for t in tl if 'LOSS' in t['category'])
    e=sum(1 for t in tl if 'EXPIRED' in t['category'])
    p=sum(t['pnl'] for t in tl)
    wr=w/max(w+l,1)*100
    return {"n":n,"wins":w,"losses":l,"expired":e,"wr":round(wr,1),"pnl":round(p,1),"avg":round(p/n,1),"label":label}

def ps(s):
    if s['n']==0: return
    print(f"  {s['label']:50s} | N={s['n']:3d} | {s['wins']:2d}W/{s['losses']:2d}L/{s['expired']:2d}E | WR={s['wr']:5.1f}% | PnL={s['pnl']:+8.1f} | Avg={s['avg']:+6.1f}")

# ═══════════════════════════════════════════════════════════════════════════
print(f"\n{'='*100}")
print(f"DD EXHAUSTION DEEP DIVE — {len(enriched)} trades")
print(f"{'='*100}")

# ─── 1. TRADE CATEGORY BREAKDOWN ───────────────────────────────────────────
print(f"\n{'='*100}")
print("1. TRADE CATEGORIES — How trades end")
print(f"{'='*100}")
cats = defaultdict(list)
for t in enriched: cats[t['category']].append(t)
for c in ['WINNER','REVERSAL_LOSS','IMMEDIATE_LOSS','EXPIRED_GREEN','EXPIRED_RED']:
    if c in cats:
        avg_mp = sum(t['max_profit'] for t in cats[c])/len(cats[c])
        avg_ml = sum(t['max_loss'] for t in cats[c])/len(cats[c])
        avg_pnl = sum(t['pnl'] for t in cats[c])/len(cats[c])
        print(f"  {c:20s}: N={len(cats[c]):3d} | AvgPnL={avg_pnl:+6.1f} | AvgMaxProfit={avg_mp:+5.1f} | AvgMaxLoss={avg_ml:+5.1f}")

# ─── 2. IMMEDIATE LOSSES — What went wrong? ────────────────────────────────
print(f"\n{'='*100}")
print("2. IMMEDIATE LOSSES — trades that NEVER went green (MaxProfit <= 1)")
print("   These are pure signal failures. What do they have in common?")
print(f"{'='*100}")

imm = cats.get('IMMEDIATE_LOSS', [])
win = cats.get('WINNER', [])

# Compare immediate losses vs winners across every metric
def compare(metric_fn, label, buckets=None):
    print(f"\n  --- {label} ---")
    if buckets:
        for bname, bfn in buckets:
            w_b = [t for t in win if bfn(t)]
            l_b = [t for t in imm if bfn(t)]
            all_b = [t for t in enriched if bfn(t)]
            if w_b or l_b:
                wn = len(w_b)
                ln = len(l_b)
                an = len(all_b)
                wr = wn/max(wn+ln,1)*100
                print(f"    {bname:30s} | Winners={wn:2d} ImmLoss={ln:2d} | All={an:3d} | WR(W vs ImmL)={wr:5.1f}%")
    else:
        w_vals = [metric_fn(t) for t in win if metric_fn(t) is not None]
        l_vals = [metric_fn(t) for t in imm if metric_fn(t) is not None]
        if w_vals and l_vals:
            print(f"    Winners  avg={sum(w_vals)/len(w_vals):+12.1f} | N={len(w_vals)}")
            print(f"    ImmLoss  avg={sum(l_vals)/len(l_vals):+12.1f} | N={len(l_vals)}")

compare(None, "TIME OF DAY", [
    ("10:00-10:59", lambda t: t.get('hour')==10),
    ("11:00-11:59", lambda t: t.get('hour')==11),
    ("12:00-12:59", lambda t: t.get('hour')==12),
    ("13:00-13:59", lambda t: t.get('hour')==13),
    ("14:00+", lambda t: t.get('hour',0)>=14),
])

compare(None, "DIRECTION", [
    ("LONG", lambda t: t['direction']=='long'),
    ("SHORT", lambda t: t['direction']=='short'),
])

compare(None, "DIRECTION x MORNING", [
    ("LONG 10:00-10:59", lambda t: t['direction']=='long' and t.get('hour')==10),
    ("SHORT 10:00-10:59", lambda t: t['direction']=='short' and t.get('hour')==10),
    ("LONG 11:00-11:59", lambda t: t['direction']=='long' and t.get('hour')==11),
    ("SHORT 11:00-11:59", lambda t: t['direction']=='short' and t.get('hour')==11),
])

compare(None, "VANNA REGIME", [
    ("Vanna ALL NEGATIVE", lambda t: t.get('vanna_regime')=='NEG'),
    ("Vanna ALL POSITIVE", lambda t: t.get('vanna_regime')=='POS'),
])

compare(None, "VANNA x DIRECTION", [
    ("Vanna WITH direction", lambda t: t.get('vanna_alignment')=='WITH'),
    ("Vanna AGAINST direction", lambda t: t.get('vanna_alignment')=='AGAINST'),
])

compare(None, "PARADIGM GROUP", [
    ("BOFA", lambda t: t.get('para_group')=='BOFA'),
    ("AG", lambda t: t.get('para_group')=='AG'),
    ("GEX", lambda t: t.get('para_group')=='GEX'),
    ("SIDIAL", lambda t: t.get('para_group')=='SIDIAL'),
])

compare(None, "CHARM BUCKET", [
    ("<20M", lambda t: t.get('charm_bucket')=='<20M'),
    ("20-50M", lambda t: t.get('charm_bucket')=='20-50M'),
    ("50-100M", lambda t: t.get('charm_bucket')=='50-100M'),
    ("100-200M", lambda t: t.get('charm_bucket')=='100-200M'),
    ("200M+", lambda t: t.get('charm_bucket')=='200M+'),
])

compare(None, "DD CONCENTRATION", [
    ("<35%", lambda t: t.get('conc_bucket')=='<35%'),
    ("35-45%", lambda t: t.get('conc_bucket')=='35-45%'),
    ("45-55%", lambda t: t.get('conc_bucket')=='45-55%'),
    ("55%+", lambda t: t.get('conc_bucket')=='55%+'),
])

# Vanna values
compare(lambda t: t.get('vanna_net'), "VANNA NET VALUE (raw)")
compare(lambda t: abs(t.get('charm_value') or 0) if t.get('charm_value') is not None else None, "CHARM ABS VALUE")

# ─── 3. IMMEDIATE LOSS TRADE LIST ──────────────────────────────────────────
print(f"\n{'='*100}")
print("3. IMMEDIATE LOSS TRADE LIST")
print(f"{'='*100}")
print(f"  {'ID':>4} | {'Date':10} | {'Time':5} | {'Dir':5} | {'PnL':>7} | {'MP':>4} | {'Charm':>10} | {'Paradigm':18} | {'Vanna':>12} | {'DDc':>5} | {'VannaAlign':>10}")
for t in imm:
    ch = f"{t['charm_value']/1e6:+.0f}M" if t.get('charm_value') is not None else "?"
    para = str(t.get('vol_paradigm') or t.get('paradigm') or '?')[:18]
    vn = f"{t['vanna_net']:+.0f}" if t.get('vanna_net') is not None else "?"
    dc = f"{t['dd_concentration']:.0f}%" if t.get('dd_concentration') is not None else "?"
    print(f"  {t['id']:>4} | {t['date_str']:10} | {t['time_str']:5} | {t['direction']:5} | {t['pnl']:+7.1f} | {t['max_profit']:+4.0f} | {ch:>10} | {para:18} | {vn:>12} | {dc:>5} | {t.get('vanna_alignment','?'):>10}")

# ─── 4. WINNER TRADE LIST ──────────────────────────────────────────────────
print(f"\n{'='*100}")
print("4. WINNER TRADE LIST")
print(f"{'='*100}")
print(f"  {'ID':>4} | {'Date':10} | {'Time':5} | {'Dir':5} | {'PnL':>7} | {'MP':>4} | {'Charm':>10} | {'Paradigm':18} | {'Vanna':>12} | {'DDc':>5} | {'VannaAlign':>10}")
for t in win:
    ch = f"{t['charm_value']/1e6:+.0f}M" if t.get('charm_value') is not None else "?"
    para = str(t.get('vol_paradigm') or t.get('paradigm') or '?')[:18]
    vn = f"{t['vanna_net']:+.0f}" if t.get('vanna_net') is not None else "?"
    dc = f"{t['dd_concentration']:.0f}%" if t.get('dd_concentration') is not None else "?"
    print(f"  {t['id']:>4} | {t['date_str']:10} | {t['time_str']:5} | {t['direction']:5} | {t['pnl']:+7.1f} | {t['max_profit']:+4.0f} | {ch:>10} | {para:18} | {vn:>12} | {dc:>5} | {t.get('vanna_alignment','?'):>10}")

# ─── 5. CROSS-TABULATION: best & worst combos ──────────────────────────────
print(f"\n{'='*100}")
print("5. CROSS-TABULATION — Every meaningful combo")
print(f"{'='*100}")

# Build a grid: (time_bucket, direction, vanna, paradigm) -> trades
def time_bucket(h):
    if h is None: return '?'
    if h < 11: return '10:xx'
    if h < 12: return '11:xx'
    if h < 13: return '12:xx'
    if h < 14: return '13:xx'
    return '14:xx+'

# Direction x Time
print(f"\n  --- DIRECTION x TIME BUCKET ---")
combos = defaultdict(list)
for t in enriched:
    key = (t['direction'].upper(), time_bucket(t.get('hour')))
    combos[key].append(t)
for key in sorted(combos.keys()):
    s = stats(combos[key], f"{key[0]:5s} @ {key[1]}")
    ps(s)

# Direction x Vanna
print(f"\n  --- DIRECTION x VANNA REGIME ---")
combos = defaultdict(list)
for t in enriched:
    key = (t['direction'].upper(), t.get('vanna_regime','?'))
    combos[key].append(t)
for key in sorted(combos.keys()):
    s = stats(combos[key], f"{key[0]:5s} x Vanna={key[1]}")
    ps(s)

# Time x Vanna
print(f"\n  --- TIME BUCKET x VANNA REGIME ---")
combos = defaultdict(list)
for t in enriched:
    key = (time_bucket(t.get('hour')), t.get('vanna_regime','?'))
    combos[key].append(t)
for key in sorted(combos.keys()):
    s = stats(combos[key], f"{key[0]:6s} x Vanna={key[1]}")
    ps(s)

# Direction x Paradigm group
print(f"\n  --- DIRECTION x PARADIGM GROUP ---")
combos = defaultdict(list)
for t in enriched:
    key = (t['direction'].upper(), t.get('para_group','?'))
    combos[key].append(t)
for key in sorted(combos.keys()):
    s = stats(combos[key], f"{key[0]:5s} x {key[1]}")
    ps(s)

# Direction x Charm bucket
print(f"\n  --- DIRECTION x CHARM BUCKET ---")
combos = defaultdict(list)
for t in enriched:
    key = (t['direction'].upper(), t.get('charm_bucket','?'))
    combos[key].append(t)
for key in sorted(combos.keys()):
    s = stats(combos[key], f"{key[0]:5s} x Charm={key[1]}")
    ps(s)

# Vanna x Charm
print(f"\n  --- VANNA REGIME x CHARM BUCKET ---")
combos = defaultdict(list)
for t in enriched:
    key = (t.get('vanna_regime','?'), t.get('charm_bucket','?'))
    combos[key].append(t)
for key in sorted(combos.keys()):
    s = stats(combos[key], f"Vanna={key[0]:3s} x Charm={key[1]}")
    ps(s)

# Direction x Time x Vanna (triple cross)
print(f"\n  --- DIRECTION x TIME x VANNA (triple cross) ---")
combos = defaultdict(list)
for t in enriched:
    key = (t['direction'].upper(), time_bucket(t.get('hour')), t.get('vanna_regime','?'))
    combos[key].append(t)
for key in sorted(combos.keys()):
    s = stats(combos[key], f"{key[0]:5s} @ {key[1]:6s} x Vanna={key[2]}")
    if s['n'] >= 3:
        ps(s)

# ─── 6. REVERSAL LOSSES — went green then lost ─────────────────────────────
print(f"\n{'='*100}")
print("6. REVERSAL LOSSES — trades that went GREEN first then reversed to LOSS")
print(f"{'='*100}")
rev = cats.get('REVERSAL_LOSS', [])
print(f"  Count: {len(rev)}")
if rev:
    print(f"  {'ID':>4} | {'Date':10} | {'Time':5} | {'Dir':5} | {'PnL':>7} | {'MaxP':>5} | {'MaxL':>5} | {'Charm':>10} | {'Paradigm':18} | {'Vanna':>12} | {'VanAlign':>8}")
    for t in rev:
        ch = f"{t['charm_value']/1e6:+.0f}M" if t.get('charm_value') is not None else "?"
        para = str(t.get('vol_paradigm') or t.get('paradigm') or '?')[:18]
        vn = f"{t['vanna_net']:+.0f}" if t.get('vanna_net') is not None else "?"
        print(f"  {t['id']:>4} | {t['date_str']:10} | {t['time_str']:5} | {t['direction']:5} | {t['pnl']:+7.1f} | {t['max_profit']:+5.0f} | {t['max_loss']:+5.0f} | {ch:>10} | {para:18} | {vn:>12} | {t.get('vanna_alignment','?'):>8}")
    # What do reversal losses have in common?
    print(f"\n  Reversal loss patterns:")
    compare(None, "VANNA in reversal losses", [
        ("WITH direction", lambda t: t.get('vanna_alignment')=='WITH'),
        ("AGAINST direction", lambda t: t.get('vanna_alignment')=='AGAINST'),
    ])

# ─── 7. DAY-BY-DAY BREAKDOWN ───────────────────────────────────────────────
print(f"\n{'='*100}")
print("7. DAY-BY-DAY PERFORMANCE")
print(f"{'='*100}")
days = defaultdict(list)
for t in enriched: days[t['date_str']].append(t)
for d in sorted(days.keys()):
    s = stats(days[d], d)
    para_set = set(t.get('vol_paradigm') or t.get('paradigm','?') for t in days[d])
    vanna_avg = [t['vanna_net'] for t in days[d] if t.get('vanna_net') is not None]
    va_str = f"vanna_avg={sum(vanna_avg)/len(vanna_avg):+.0f}" if vanna_avg else ""
    ps(s)
    print(f"    Paradigms: {', '.join(sorted(para_set)[:3])} | {va_str}")

# ─── 8. BEST FILTER COMBOS — optimized for high WR + positive PnL ─────────
print(f"\n{'='*100}")
print("8. SMART FILTER SEARCH — testing many combos to find highest WR with N>=10")
print(f"{'='*100}")

filters_pool = {
    "time<13": lambda t: t.get('hour',99)<13,
    "time<14": lambda t: t.get('hour',99)<14,
    "time 11-13": lambda t: t.get('hour',99) in (11,12),
    "no_BOFA": lambda t: t.get('para_group','') != 'BOFA',
    "vanna_neg": lambda t: t.get('vanna_regime')=='NEG',
    "vanna_with": lambda t: t.get('vanna_alignment')=='WITH',
    "charm50-200M": lambda t: t.get('charm_bucket','') in ('50-100M','100-200M'),
    "charm>=50M": lambda t: t.get('charm_value') is not None and abs(t['charm_value'])>=50e6,
    "conc<40%": lambda t: t.get('dd_concentration') is not None and t['dd_concentration']<40,
    "conc<45%": lambda t: t.get('dd_concentration') is not None and t['dd_concentration']<45,
    "short_only": lambda t: t['direction']=='short',
    "long_only": lambda t: t['direction']=='long',
    "no_10am": lambda t: t.get('hour',99) != 10,
    "11am_only": lambda t: t.get('hour',99) == 11,
    "no_BOFA_no_LIS": lambda t: t.get('para_group','') not in ('BOFA',) and 'LIS' not in str(t.get('vol_paradigm') or t.get('paradigm') or '').upper(),
}

from itertools import combinations

results = []
for r in range(1, 4):  # 1, 2, 3 filter combos
    for combo in combinations(filters_pool.items(), r):
        remaining = list(enriched)
        for fn, ff in combo:
            remaining = [t for t in remaining if ff(t)]
        if len(remaining) < 8: continue
        s = stats(remaining)
        if s['wr'] > 0:
            blocked = [t for t in enriched if t not in remaining]
            bw = sum(1 for t in blocked if t['category']=='WINNER')
            results.append({
                'label': ' + '.join(n for n,_ in combo),
                'n': s['n'],
                'wr': s['wr'],
                'pnl': s['pnl'],
                'avg': s['avg'],
                'wins': s['wins'],
                'losses': s['losses'],
                'blocked_wins': bw,
            })

# Sort by WR * sqrt(N) to balance quality and sample size
results.sort(key=lambda x: x['wr'] * (x['n']**0.5), reverse=True)
print(f"\n  Top 25 filter combos (sorted by WR * sqrt(N)):\n")
print(f"  {'Rank':>4} | {'Filter combo':55s} | {'N':>3} | {'WR':>5} | {'PnL':>8} | {'Avg':>6} | {'W':>2}/{' L':>2} | {'BlkW':>4}")
print(f"  {'-'*110}")
for i, r in enumerate(results[:25], 1):
    print(f"  {i:>4} | {r['label']:55s} | {r['n']:3d} | {r['wr']:5.1f}% | {r['pnl']:+8.1f} | {r['avg']:+6.1f} | {r['wins']:2d}/{r['losses']:>2d} | {r['blocked_wins']:4d}")

# ─── 9. THE VERDICT — actionable rules ─────────────────────────────────────
print(f"\n{'='*100}")
print("9. SPECIFIC 'ENTER' vs 'AVOID' RULES")
print(f"{'='*100}")

# Test specific actionable rules
rules = [
    ("AVOID: Long at 10:xx", lambda t: not (t['direction']=='long' and t.get('hour')==10)),
    ("AVOID: Short after 13:xx", lambda t: not (t['direction']=='short' and t.get('hour',0)>=13)),
    ("AVOID: Any trade after 14:xx", lambda t: t.get('hour',99)<14),
    ("AVOID: BOFA paradigm group", lambda t: t.get('para_group','')!='BOFA'),
    ("AVOID: Vanna AGAINST direction", lambda t: t.get('vanna_alignment','?')!='AGAINST'),
    ("AVOID: Charm 20-50M band", lambda t: t.get('charm_bucket','')!='20-50M'),
    ("AVOID: DD conc >= 55%", lambda t: t.get('dd_concentration') is None or t['dd_concentration']<55),
    ("COMBO: no Long@10 + no Short@13+ + vanna WITH", lambda t: (
        not (t['direction']=='long' and t.get('hour')==10) and
        not (t['direction']=='short' and t.get('hour',0)>=13) and
        t.get('vanna_alignment','?')!='AGAINST'
    )),
    ("COMBO: no Long@10 + no Short@13+ + no BOFA", lambda t: (
        not (t['direction']=='long' and t.get('hour')==10) and
        not (t['direction']=='short' and t.get('hour',0)>=13) and
        t.get('para_group','')!='BOFA'
    )),
    ("COMBO: vanna WITH + no BOFA + time<14", lambda t: (
        t.get('vanna_alignment','?')!='AGAINST' and
        t.get('para_group','')!='BOFA' and
        t.get('hour',99)<14
    )),
    ("BEST: vanna WITH + no BOFA + no Long@10 + no Short@13+", lambda t: (
        t.get('vanna_alignment','?')!='AGAINST' and
        t.get('para_group','')!='BOFA' and
        not (t['direction']=='long' and t.get('hour')==10) and
        not (t['direction']=='short' and t.get('hour',0)>=13)
    )),
]

baseline = stats(enriched, "BASELINE")
print(f"\n  {'Rule':65s} | {'N':>3} | {'WR':>5} | {'PnL':>8} | {'Avg':>6} | {'W/L':>5} | {'BlkW':>4}")
print(f"  {'-'*115}")
ps(baseline)
for label, fn in rules:
    kept = [t for t in enriched if fn(t)]
    blocked = [t for t in enriched if not fn(t)]
    s = stats(kept, label)
    bw = sum(1 for t in blocked if t['category']=='WINNER')
    print(f"  {label:65s} | {s['n']:3d} | {s['wr']:5.1f}% | {s['pnl']:+8.1f} | {s['avg']:+6.1f} | {s['wins']:2d}/{s['losses']:2d} | {bw:4d}")

conn.close()
print(f"\n{'='*100}")
print("DEEP ANALYSIS COMPLETE")
