# -*- coding: utf-8 -*-
"""S285 — the user's challenge, and it is the right one:

  "the cap should NOT filter out bad trades. All our trades should already be filtered
   by V20, and the cap only reduces risk. So if removing a setup makes things worse,
   either our filter has an issue, or the clustered trades have an issue."

So: identify EXACTLY which trades enter the book when DD Exhaustion is removed, and
judge them on their own merits. If they are systematically bad, V20 should be blocking
them regardless of DD — and then DD can be judged honestly on its own.

Also: is the "slot 2 earns more" result real, or just a day-quality selection effect?
"""
import os, sys
import numpy as np, pandas as pd
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.path.insert(0, 'app')
import live_filter as lf

ET = ZoneInfo("America/New_York")
HAIR, FEE, DPP, DEAD = 0.6, 1.92, 5.0, 0.15

E = create_engine(os.environ["DATABASE_URL"])
c = E.connect().execution_options(isolation_level="AUTOCOMMIT")
gaps = lf.load_gaps(c)
rows = c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min
    FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
c.close()
rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')


def qty(r):
    v = r.get('basket_pct')
    if v is None:
        return 1
    v = float(v)
    return 1 if abs(v) < DEAD else (2 if ((v > 0) == r['is_long']) else 1)


def replay(pred, max_long=2, max_short=3):
    openp, last, out = [], {}, []
    for r in rows:
        if not pred(r):
            continue
        t = r['et']
        openp = [p for p in openp if p[0] > t]
        n_same = sum(1 for p in openp if p[1] == r['is_long'])
        if n_same >= (max_long if r['is_long'] else max_short):
            continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            continue
        last[k] = t
        openp.append((t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)), r['is_long']))
        q = qty(r); pts = float(r['outcome_pnl'])
        out.append({'id': r['id'], 'd': t.date(), 'et': t, 'setup': r['setup_name'],
                    'long': r['is_long'], 'grade': r.get('grade'), 'slot': n_same,
                    'vix': pd.to_numeric(r.get('vix'), errors='coerce'),
                    'para': r.get('paradigm'), 'align': r.get('greek_alignment'),
                    'q': q, 'pts': pts, 'net': (pts - HAIR) * q * DPP - FEE * q})
    return pd.DataFrame(out)


V20 = lambda r: lf.passes_v20(r, gaps)
base = replay(V20)
nodd = replay(lambda r: V20(r) and r['setup_name'] != 'DD Exhaustion')

base_ids, nodd_ids = set(base['id']), set(nodd['id'])
repl = nodd[nodd['id'].isin(nodd_ids - base_ids)]          # entered only because DD left
lost = base[base['id'].isin(base_ids - nodd_ids)]          # the DD trades themselves

print("=" * 100)
print("1. WHEN DD IS REMOVED, WHO TAKES THE FREED SLOTS?")
print("=" * 100)
print(f"  DD trades removed        : {len(lost):4d}   ${lost['net'].sum():+8,.0f}  "
      f"${lost['net'].mean():+6.1f}/t")
print(f"  NEW trades that entered  : {len(repl):4d}   ${repl['net'].sum():+8,.0f}  "
      f"${repl['net'].mean() if len(repl) else 0:+6.1f}/t")
print(f"  net book change          : ${nodd['net'].sum()-base['net'].sum():+8,.0f}")
print()
if len(repl):
    print("  THE REPLACEMENT TRADES, characterised:")
    print(f"    {'setup':18s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}")
    for s, ss in repl.groupby('setup'):
        print(f"    {s:18s}{len(ss):>5d}{ss['net'].sum():>+10,.0f}{ss['net'].mean():>+8.1f}"
              f"{(ss['pts']>0).mean()*100:>5.0f}%")
    print(f"\n    {'grade':18s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}")
    for g, ss in repl.groupby('grade'):
        print(f"    {str(g):18s}{len(ss):>5d}{ss['net'].sum():>+10,.0f}{ss['net'].mean():>+8.1f}"
              f"{(ss['pts']>0).mean()*100:>5.0f}%")
    print(f"\n    {'slot entered':18s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}")
    for sl, ss in repl.groupby('slot'):
        print(f"    slot {sl:<13d}{len(ss):>5d}{ss['net'].sum():>+10,.0f}{ss['net'].mean():>+8.1f}"
              f"{(ss['pts']>0).mean()*100:>5.0f}%")
    print(f"\n    {'direction':18s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}")
    for L, ss in repl.groupby('long'):
        print(f"    {'LONG' if L else 'SHORT':18s}{len(ss):>5d}{ss['net'].sum():>+10,.0f}"
              f"{ss['net'].mean():>+8.1f}{(ss['pts']>0).mean()*100:>5.0f}%")
    print("\n  KEY QUESTION: are these replacements WORSE than the average V20 trade?")
    print(f"    replacements  ${repl['net'].mean():+6.1f}/t   (n={len(repl)})")
    print(f"    whole V20 book ${base['net'].mean():+6.1f}/t   (n={len(base)})")
    same = base[base['setup'].isin(repl['setup'].unique())]
    print(f"    same setups, all V20 trades ${same['net'].mean():+6.1f}/t  (n={len(same)})")

print()
print("=" * 100)
print("2. IS 'SLOT 2 EARNS MORE' REAL, OR JUST A GOOD-DAY EFFECT?")
print("=" * 100)
day_tot = base.groupby('d')['net'].sum()
base['day_net'] = base['d'].map(day_tot)
base['day_n'] = base['d'].map(base.groupby('d').size())
print("  a) do stacked trades only happen on days that were good anyway?")
for sl, ss in base.groupby('slot'):
    print(f"    slot {sl}: n={len(ss):4d}  own ${ss['net'].mean():+6.1f}/t   "
          f"their days averaged ${ss['day_net'].mean():+7.0f}/day  ({ss['day_n'].mean():.1f} trades/day)")
print("\n  b) WITHIN the same day, compare slot-0 vs slot>=1 trades:")
multi = base[base['d'].isin(base[base['slot'] >= 1]['d'].unique())]
a = multi[multi['slot'] == 0]['net']; b = multi[multi['slot'] >= 1]['net']
print(f"    on days that HAD stacking: slot0 ${a.mean():+6.1f}/t (n={len(a)})  "
      f"vs slot>=1 ${b.mean():+6.1f}/t (n={len(b)})")
print(f"    difference ${b.mean()-a.mean():+.1f}/t  "
      f"-> {'STILL better within the same day = REAL' if b.mean() > a.mean() else 'gone once the day is controlled = SELECTION EFFECT'}")
print("\n  c) same test, Skew Charm only:")
sc = multi[multi['setup'] == 'Skew Charm']
a2 = sc[sc['slot'] == 0]['net']; b2 = sc[sc['slot'] >= 1]['net']
print(f"    SC slot0 ${a2.mean():+6.1f}/t (n={len(a2)})   SC slot>=1 ${b2.mean():+6.1f}/t (n={len(b2)})")
print("\n  d) and the honest one — what does the day look like BEFORE the stacked trade fires?")
firsts = base[base['slot'] == 0].groupby('d')['net'].sum()
stack_days = sorted(base[base['slot'] >= 1]['d'].unique())
flat_days = [d for d in day_tot.index if d not in stack_days]
print(f"    days WITH stacking  : {len(stack_days):3d}  avg day ${day_tot[stack_days].mean():+7.0f}")
print(f"    days WITHOUT        : {len(flat_days):3d}  avg day ${day_tot[flat_days].mean():+7.0f}")

print()
print("=" * 100)
print("3. THE REAL FINDING: the replacements are SKEW CHARM LONGS, grade B. Should V20 block them?")
print("=" * 100)
sc_long = base[(base['setup'] == 'Skew Charm') & (base['long'])]
print(f"  Skew Charm LONG in the V20 book, by grade:")
print(f"    {'grade':8s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}{'t':>7s}")
for g, s in sc_long.groupby('grade'):
    se = s['net'].std(ddof=1)/np.sqrt(len(s)) if len(s) > 1 else float('nan')
    print(f"    {str(g):8s}{len(s):>5d}{s['net'].sum():>+10,.0f}{s['net'].mean():>+8.1f}"
          f"{(s['pts']>0).mean()*100:>5.0f}%{s['net'].mean()/se if se==se and se>0 else 0:>+7.2f}")
print(f"\n  Skew Charm SHORT by grade (control — do NOT touch what works):")
sc_short = base[(base['setup'] == 'Skew Charm') & (~base['long'])]
for g, s in sc_short.groupby('grade'):
    se = s['net'].std(ddof=1)/np.sqrt(len(s)) if len(s) > 1 else float('nan')
    print(f"    {str(g):8s}{len(s):>5d}{s['net'].sum():>+10,.0f}{s['net'].mean():>+8.1f}"
          f"{(s['pts']>0).mean()*100:>5.0f}%{s['net'].mean()/se if se==se and se>0 else 0:>+7.2f}")

def is_scb_long(r):
    return (r['setup_name'] == 'Skew Charm'
            and str(r.get('direction', '')).lower() in ('long', 'bullish')
            and str(r.get('grade')) == 'B')

OPTS = {
    'V20 as-is': V20,
    'V20 - SC long grade B': lambda r: V20(r) and not is_scb_long(r),
    'V20 - SC long B, and DD off': lambda r: (V20(r) and not is_scb_long(r)
                                              and r['setup_name'] != 'DD Exhaustion'),
}
N_SESS = base['d'].nunique() + 22   # calendar sessions ~117
print(f"\n  Whole-book effect (cap replayed):")
print(f"    {'option':30s}{'trades':>8s}{'$ total':>10s}{'$/mo':>9s}{'$/t':>8s}{'MaxDD':>10s}{'red':>6s}")
store = {}
for k, f in OPTS.items():
    df = replay(f); store[k] = df
    d = df.groupby('d')['net'].sum(); eq = d.cumsum()
    print(f"    {k:30s}{len(df):>8d}{df['net'].sum():>+10,.0f}{df['net'].sum()/117*21:>+9,.0f}"
          f"{df['net'].mean():>+8.1f}{float((eq-eq.cummax()).min()):>+10,.0f}{int((d<0).sum()):>6d}")
print(f"\n  leave-one-month-out for blocking SC long grade B:")
a, b = store['V20 as-is'], store['V20 - SC long grade B']
for m in sorted(pd.to_datetime(a['d']).dt.strftime('%Y-%m').unique()):
    av = a[pd.to_datetime(a['d']).dt.strftime('%Y-%m') == m]['net'].sum()
    bv = b[pd.to_datetime(b['d']).dt.strftime('%Y-%m') == m]['net'].sum()
    print(f"      {m}  as-is ${av:+8,.0f}   blocked ${bv:+8,.0f}   {bv-av:+7,.0f}  "
          f"{'HELPS' if bv > av else ('SAME' if bv == av else 'HURTS')}")
