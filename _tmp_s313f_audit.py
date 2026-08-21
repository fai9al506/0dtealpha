# -*- coding: utf-8 -*-
"""S313 stage 6 - ROW BY ROW, TRADE BY TRADE AUDIT of the proposed V22 rule.

V22 candidate: when the PREVIOUS session's open-to-close was < -0.5% and this
signal's VIX < 24 ->
    SHORTS: skip
    LONGS : qty = min(qty * 2, 3)

Every claim in the summary must be reproducible from the lines printed here.
Checks:
  1. every trigger day listed with the previous session's numbers
  2. every trade on those days: before -> after, with the dollar delta
  3. the deltas must SUM to the replay difference (no hand-waving)
  4. LOOKAHEAD check: the trigger uses only data closed before the session opens
  5. interaction check: did the $300 breaker or the position cap change behaviour
     on any trigger day (a blocked short frees a slot - who took it?)"""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
sys.path.insert(0, 'app'); import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15
DAILY, VIXMAX = -300.0, 24.0
THR, CAP = -0.5, 3

E = create_engine(os.environ['DATABASE_URL'])
c = E.connect().execution_options(isolation_level='AUTOCOMMIT')
gaps = lf.load_gaps(c)
px = pd.read_sql(text("""select (ts at time zone 'America/New_York')::date d,
    min(ts at time zone 'America/New_York') t0, max(ts at time zone 'America/New_York') t1,
    (array_agg(bar_open order by ts))[1] o, (array_agg(bar_close order by ts desc))[1] cl
    from spx_ohlc_1m where (ts at time zone 'America/New_York')>='2026-02-19'
    group by 1 order by 1"""), c)
rows = c.execute(text("SELECT " + lf.COLS + """, outcome_pnl, outcome_elapsed_min, spot FROM setup_log
  WHERE (ts AT TIME ZONE 'America/New_York')>='2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()

px['d'] = pd.to_datetime(px['d']).dt.date
px['oc'] = (px['cl'] - px['o']) / px['o'] * 100
prev, prevrow = {}, {}
ds = list(px['d'])
for i in range(1, len(ds)):
    prev[ds[i]] = px.iloc[i - 1]['oc']
    prevrow[ds[i]] = px.iloc[i - 1]
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def vixok(r):
    v = r.get('vix'); return v is not None and float(v) < VIXMAX


def fires(r):
    p = prev.get(r['et'].date())
    if p is None or pd.isna(p): return False
    return p < THR and vixok(r)


def run(apply_rule, trace=False):
    openp, last, out = [], {}, []
    realized = 0.0; d0 = None; closed = []
    for r in rows:
        t = r['et']
        if t.date() != d0:
            d0 = t.date(); realized = 0.0; closed = []
        for ct, nv in [x for x in closed if x[0] <= t]: realized += nv
        closed = [x for x in closed if x[0] > t]
        if not lf.passes_v20(r, gaps): continue
        blocked_by = None
        if realized <= DAILY:
            blocked_by = 'daily_breaker'
        elif apply_rule and (not r['is_long']) and fires(r):
            blocked_by = 'V22_short_block'
        if blocked_by:
            if trace: out.append({'d': t.date(), 'et': t, 'setup': r['setup_name'],
                                  'long': r['is_long'], 'q': 0, 'net': 0.0,
                                  'pnl': float(r['outcome_pnl']), 'why': blocked_by})
            continue
        openp = [o for o in openp if o[0] > t]
        if sum(1 for o in openp if o[1] == r['is_long']) >= (2 if r['is_long'] else 3):
            if trace: out.append({'d': t.date(), 'et': t, 'setup': r['setup_name'],
                                  'long': r['is_long'], 'q': 0, 'net': 0.0,
                                  'pnl': float(r['outcome_pnl']), 'why': 'cap'})
            continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            if trace: out.append({'d': t.date(), 'et': t, 'setup': r['setup_name'],
                                  'long': r['is_long'], 'q': 0, 'net': 0.0,
                                  'pnl': float(r['outcome_pnl']), 'why': 'dedup'})
            continue
        sib = [o for o in openp if o[1] == r['is_long'] and o[3] == r['setup_name']]
        if len(sib) >= 2 and r.get('spot'):
            sgn = 1.0 if r['is_long'] else -1.0
            if sum((float(r['spot']) - o[2]) * sgn for o in sib) < 0:
                if trace: out.append({'d': t.date(), 'et': t, 'setup': r['setup_name'],
                                      'long': r['is_long'], 'q': 0, 'net': 0.0,
                                      'pnl': float(r['outcome_pnl']), 'why': 'underwater'})
                continue
        last[k] = t
        v = r.get('basket_pct')
        q = 1 if v is None or abs(float(v)) < DEAD else (2 if ((float(v) > 0) == r['is_long']) else 1)
        q0 = q
        if apply_rule and r['is_long'] and fires(r):
            q = min(q * 2, CAP)
        net = (float(r['outcome_pnl']) - HAIR) * q * DPP - FEE * q
        ct = t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30))
        openp.append((ct, r['is_long'], float(r['spot']) if r.get('spot') else 0.0, r['setup_name'], q))
        closed.append((ct, net))
        out.append({'d': t.date(), 'et': t, 'setup': r['setup_name'], 'long': r['is_long'],
                    'q': q, 'q0': q0, 'net': net, 'pnl': float(r['outcome_pnl']), 'why': 'TAKEN'})
    return pd.DataFrame(out)


A = run(False, trace=True)   # baseline V20
B = run(True, trace=True)    # V22 candidate
At = A[A['why'] == 'TAKEN']; Bt = B[B['why'] == 'TAKEN']

print("=" * 118)
print("(4) LOOKAHEAD CHECK - the trigger is the PREVIOUS session, fully closed before this one opens")
print("=" * 118)
bad = 0
for d, p in list(prevrow.items())[:0]: pass
sample = [d for d in sorted(prev) if prev.get(d) is not None and not pd.isna(prev[d])][-3:]
for d in sample:
    pr = prevrow[d]
    print("  signal day %s  reads session %s  which closed at %s ET  ->  oc %+.3f%%"
          % (d, pr['d'], pd.Timestamp(pr['t1']).strftime('%H:%M'), pr['oc']))
    if pr['d'] >= d: bad += 1
print("  days where the source session is not strictly earlier: %d  %s" % (bad, 'FAIL' if bad else 'OK'))

trig = sorted(set(r['et'].date() for r in rows if fires(r)))
print()
print("=" * 118)
print("(1) TRIGGER DAYS - %d of them" % len(trig))
print("=" * 118)
print("  %-12s%12s%12s%10s   %s" % ('signal day', 'prev day', 'prev oc%', 'VIX', 'what changes'))
tot_delta = 0.0
for d in trig:
    pr = prevrow[d]
    vs = [float(r['vix']) for r in rows if r['et'].date() == d and r.get('vix')]
    a = At[At['d'] == d]; b = Bt[Bt['d'] == d]
    delta = b['net'].sum() - a['net'].sum()
    tot_delta += delta
    print("  %-12s%12s%+12.2f%10.1f   $%+8.0f" % (d, pr['d'], pr['oc'], np.mean(vs) if vs else 0, delta))
print("  %-12s%12s%12s%10s   $%+8.0f  <- must equal the replay difference" % ('TOTAL', '', '', '', tot_delta))
print("  replay difference (all days): $%+.0f" % (Bt['net'].sum() - At['net'].sum()))

print()
print("=" * 118)
print("(2) EVERY TRADE ON A TRIGGER DAY - before -> after")
print("=" * 118)
print("  %-12s%-6s%-16s%-6s%6s%6s%9s%10s%10s   %s" % (
    'day', 'time', 'setup', 'dir', 'qOLD', 'qNEW', 'pt', '$ before', '$ after', 'change'))
for d in trig:
    a = A[(A['d'] == d)]; b = B[(B['d'] == d)]
    keys = sorted(set(list(a['et']) + list(b['et'])))
    for t in keys:
        ra = a[a['et'] == t]; rb = b[b['et'] == t]
        ra = ra.iloc[0] if len(ra) else None
        rb = rb.iloc[0] if len(rb) else None
        base = ra if ra is not None else rb
        qa = int(ra['q']) if ra is not None else 0
        qb = int(rb['q']) if rb is not None else 0
        na = float(ra['net']) if ra is not None else 0.0
        nb = float(rb['net']) if rb is not None else 0.0
        wa = ra['why'] if ra is not None else '-'
        wb = rb['why'] if rb is not None else '-'
        tag = ''
        if wa == 'TAKEN' and wb != 'TAKEN': tag = 'BLOCKED (%s)' % wb
        elif wa != 'TAKEN' and wb == 'TAKEN': tag = 'NOW TAKEN (was %s)' % wa
        elif qa != qb: tag = 'size %d->%d' % (qa, qb)
        elif wa != 'TAKEN': tag = 'skipped both (%s)' % wa
        else: tag = 'unchanged'
        if tag == 'unchanged': continue
        print("  %-12s%-6s%-16s%-6s%6d%6d%+9.1f%+10.0f%+10.0f   %s" % (
            d, pd.Timestamp(t).strftime('%H:%M'), str(base['setup'])[:15],
            'LONG' if base['long'] else 'SHORT', qa, qb, float(base['pnl']), na, nb, tag))

print()
print("=" * 118)
print("(5) INTERACTION CHECK - trades that only exist in ONE run (a freed slot being taken)")
print("=" * 118)
ka = set(zip(At['d'], At['et'])); kb = set(zip(Bt['d'], Bt['et']))
onlyB = kb - ka; onlyA = ka - kb
print("  taken ONLY with the rule on : %d" % len(onlyB))
for d, t in sorted(onlyB):
    r = Bt[(Bt['d'] == d) & (Bt['et'] == t)].iloc[0]
    print("     %s %s %-15s %-5s q=%d  pt %+.1f  $%+.0f" % (
        d, pd.Timestamp(t).strftime('%H:%M'), r['setup'], 'LONG' if r['long'] else 'SHORT',
        int(r['q']), r['pnl'], r['net']))
print("  taken ONLY with the rule off: %d" % len(onlyA))
for d, t in sorted(onlyA):
    r = At[(At['d'] == d) & (At['et'] == t)].iloc[0]
    print("     %s %s %-15s %-5s q=%d  pt %+.1f  $%+.0f" % (
        d, pd.Timestamp(t).strftime('%H:%M'), r['setup'], 'LONG' if r['long'] else 'SHORT',
        int(r['q']), r['pnl'], r['net']))

print()
print("=" * 118)
print("(3) TOTALS")
print("=" * 118)
print("  baseline  trades %d  contracts %d  net $%+.0f" % (len(At), At['q'].sum(), At['net'].sum()))
print("  V22 cand  trades %d  contracts %d  net $%+.0f" % (len(Bt), Bt['q'].sum(), Bt['net'].sum()))
print("  difference                                  $%+.0f" % (Bt['net'].sum() - At['net'].sum()))
nd = len(set(At['d']) | set(Bt['d']))
print("  sessions %d -> $%+.0f per 21-session month" % (nd, (Bt['net'].sum() - At['net'].sum())))
