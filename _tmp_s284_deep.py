# -*- coding: utf-8 -*-
"""S284 — four questions:
  1. DISTRIBUTION: is the V20 average carried by one month / a few days?
  2. TRADE COUNT + risk: how much coin-flip risk does each version remove?
  3. DD Exhaustion grade by grade — gate or disable? (with cap replay both ways)
  4. Skew Charm and the cap: does the marginal concurrent SC trade earn less?
     This is the scaling question — SC-short 2x is planned for ~September.
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
SAR = 3.75

E = create_engine(os.environ["DATABASE_URL"])
c = E.connect().execution_options(isolation_level="AUTOCOMMIT")
gaps = lf.load_gaps(c)
rows = c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min
    FROM setup_log WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-01'
    AND outcome_pnl IS NOT NULL ORDER BY ts""")).mappings().all()
sessions = [r[0] for r in c.execute(text(
    """SELECT DISTINCT (ts AT TIME ZONE 'America/New_York')::date d FROM spx_ohlc_1m
       WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-01' ORDER BY d""")).all()]
c.close()

rows = [dict(r) for r in rows]
for r in rows:
    r['et'] = r['ts'].astimezone(ET).replace(tzinfo=None)
    r['is_long'] = str(r.get('direction', '')).lower() in ('long', 'bullish')
N_SESS = len(sessions)
sess_by_mo = pd.Series(sessions).groupby(pd.to_datetime(pd.Series(sessions)).dt.strftime('%Y-%m')).size()


def qty(r):
    v = r.get('basket_pct')
    if v is None:
        return 1
    v = float(v)
    return 1 if abs(v) < DEAD else (2 if ((v > 0) == r['is_long']) else 1)


def replay(pred, max_long=2, max_short=3):
    """Returns taken trades, each stamped with `slot` = how many same-side positions
    were ALREADY open when it fired (0 = it was the first)."""
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
        out.append({'d': t.date(), 'et': t, 'setup': r['setup_name'], 'long': r['is_long'],
                    'grade': r.get('grade'), 'slot': n_same, 'q': q, 'pts': pts,
                    'net': (pts - HAIR) * q * DPP - FEE * q})
    return pd.DataFrame(out)


V20 = lambda r: lf.passes_v20(r, gaps)
v20 = replay(V20)

print("=" * 100)
print("1. DISTRIBUTION — is the V20 average carried by one month, or a few days?")
print("=" * 100)
v20['mo'] = pd.to_datetime(v20['d']).dt.strftime('%Y-%m')
tot = v20['net'].sum()
print(f"\n  {'month':9s}{'sessions':>9s}{'trades':>8s}{'$ raw':>10s}{'$ per 21':>10s}{'share':>8s}")
for mo, s in v20.groupby('mo'):
    n = int(sess_by_mo.get(mo, 21))
    print(f"  {mo:9s}{n:>9d}{len(s):>8d}{s['net'].sum():>+10,.0f}{s['net'].sum()/n*21:>+10,.0f}"
          f"{s['net'].sum()/tot*100:>7.0f}%")
print(f"  {'TOTAL':9s}{N_SESS:>9d}{len(v20):>8d}{tot:>+10,.0f}{tot/N_SESS*21:>+10,.0f}")
daily = v20.groupby('d')['net'].sum().sort_values(ascending=False)
print(f"\n  best single day  ${daily.iloc[0]:+,.0f} = {daily.iloc[0]/tot*100:.0f}% of everything")
print(f"  best 3 days      ${daily.head(3).sum():+,.0f} = {daily.head(3).sum()/tot*100:.0f}%")
print(f"  best 10 days     ${daily.head(10).sum():+,.0f} = {daily.head(10).sum()/tot*100:.0f}%")
print(f"  WITHOUT the best 3 days: ${tot-daily.head(3).sum():+,.0f} "
      f"= ${(tot-daily.head(3).sum())/N_SESS*21:+,.0f}/month  <-- the honest floor")
print(f"  WITHOUT the best month:  ${tot-v20.groupby('mo')['net'].sum().max():+,.0f} "
      f"= ${(tot-v20.groupby('mo')['net'].sum().max())/(N_SESS-sess_by_mo[v20.groupby('mo')['net'].sum().idxmax()])*21:+,.0f}/month")
print(f"  green days {int((daily>0).sum())} / red {int((daily<0).sum())} / "
      f"months positive {int((v20.groupby('mo')['net'].sum()>0).sum())} of {v20['mo'].nunique()}")

print()
print("=" * 100)
print("2. TRADE COUNT = RISK TAKEN.  A filter that removes coin-flips is worth money even at flat P&L.")
print("=" * 100)
VERS = {'V16': lambda r: lf.passes_v16(r, gaps), 'V20 (live)': V20,
        'V17': lambda r: lf.passes_v17(r, gaps), 'V19': lambda r: lf.passes_v19(r, gaps)}
base_n = None
print(f"  {'version':12s}{'trades':>8s}{'per day':>9s}{'$/month':>10s}{'$/trade':>9s}{'WR':>6s}"
      f"{'MaxDD':>9s}{'vs V16 trades':>15s}")
for k, f in VERS.items():
    df = replay(f)
    d = df.groupby('d')['net'].sum()
    eq = d.cumsum(); dd = float((eq - eq.cummax()).min())
    if base_n is None:
        base_n = len(df)
    print(f"  {k:12s}{len(df):>8d}{len(df)/N_SESS:>9.1f}{df['net'].sum()/N_SESS*21:>+10,.0f}"
          f"{df['net'].mean():>+9.1f}{(df['pts']>0).mean()*100:>5.0f}%{dd:>+9,.0f}"
          f"{len(df)-base_n:>+15d}")
print("  (V20 takes 194 fewer trades than V16 for MORE money — that is 194 fewer exposures.)")

print()
print("=" * 100)
print("3. DD EXHAUSTION — grade by grade, and what removing it actually does")
print("=" * 100)
dd = v20[v20['setup'] == 'DD Exhaustion']
print(f"  all DD in the V20 book: n={len(dd)}  ${dd['net'].sum():+,.0f}  ${dd['net'].mean():+.1f}/t")
print(f"\n  {'grade':10s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}{'t-stat':>8s}")
for g, s in dd.groupby('grade'):
    se = s['net'].std(ddof=1)/np.sqrt(len(s)) if len(s) > 1 else float('nan')
    print(f"  {str(g):10s}{len(s):>5d}{s['net'].sum():>+10,.0f}{s['net'].mean():>+8.1f}"
          f"{(s['pts']>0).mean()*100:>5.0f}%{s['net'].mean()/se if se==se and se>0 else 0:>+8.2f}")
print(f"\n  {'grade':10s}" + "".join(f"{m[-2:]:>9s}" for m in sorted(dd['mo'].unique())))
for g, s in dd.groupby('grade'):
    line = f"  {str(g):10s}"
    for m in sorted(dd['mo'].unique()):
        v = s[s['mo'] == m]['net'].sum()
        line += f"{v:>+9,.0f}" if len(s[s['mo'] == m]) else f"{'-':>9s}"
    print(line)

print("\n  WHOLE-BOOK effect of each option (cap replayed — removing a setup frees slots):")
OPTS = {
    'A  V20 as-is': V20,
    'B  DD off entirely': lambda r: V20(r) and r['setup_name'] != 'DD Exhaustion',
    'C  DD grade A/A+ only': lambda r: V20(r) and (r['setup_name'] != 'DD Exhaustion'
                                                   or str(r.get('grade')) in ('A', 'A+')),
    'D  DD only VIX < 20': lambda r: V20(r) and (r['setup_name'] != 'DD Exhaustion'
                                                 or (r.get('vix') is not None and float(r['vix']) < 20)),
}
for k, f in OPTS.items():
    df = replay(f); d = df.groupby('d')['net'].sum()
    eq = d.cumsum(); ddn = float((eq - eq.cummax()).min())
    mt = df.copy(); mt['mo'] = pd.to_datetime(mt['d']).dt.strftime('%Y-%m')
    per = mt.groupby('mo')['net'].sum() / sess_by_mo.reindex(mt['mo'].unique()).values * 21
    print(f"  {k:24s} {len(df):4d}t  ${df['net'].sum():+8,.0f}  ${df['net'].sum()/N_SESS*21:+7,.0f}/mo  "
          f"MaxDD ${ddn:+8,.0f}  worst mo ${per.min():+7,.0f}  red {int((d<0).sum()):3d}")
print("\n  leave-one-month-out for option C (DD grade A/A+ only):")
a = replay(OPTS['A  V20 as-is']); cC = replay(OPTS['C  DD grade A/A+ only'])
for m in sorted(pd.to_datetime(a['d']).dt.strftime('%Y-%m').unique()):
    av = a[pd.to_datetime(a['d']).dt.strftime('%Y-%m') == m]['net'].sum()
    cv = cC[pd.to_datetime(cC['d']).dt.strftime('%Y-%m') == m]['net'].sum()
    print(f"    {m}  as-is ${av:+8,.0f}   grade-gated ${cv:+8,.0f}   {cv-av:+7,.0f}  "
          f"{'HELPS' if cv > av else ('SAME' if cv == av else 'HURTS')}")

print()
print("=" * 100)
print("4. SKEW CHARM AND THE CAP — does the marginal concurrent trade earn less?")
print("   (the scaling question: slot 0 = nothing else open on that side when it fired)")
print("=" * 100)
sc = v20[v20['setup'] == 'Skew Charm']
for side, s in [('LONG', sc[sc['long']]), ('SHORT', sc[~sc['long']])]:
    print(f"\n  Skew Charm {side}")
    print(f"    {'slot':6s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}")
    for sl, ss in s.groupby('slot'):
        print(f"    {sl:<6d}{len(ss):>5d}{ss['net'].sum():>+10,.0f}{ss['net'].mean():>+8.1f}"
              f"{(ss['pts']>0).mean()*100:>5.0f}%")
print("\n  ALL setups pooled, by slot (is the 2nd/3rd concurrent trade worth taking at all?):")
print(f"    {'slot':6s}{'n':>5s}{'$ total':>10s}{'$/t':>8s}{'WR':>6s}")
for sl, ss in v20.groupby('slot'):
    print(f"    {sl:<6d}{len(ss):>5d}{ss['net'].sum():>+10,.0f}{ss['net'].mean():>+8.1f}"
          f"{(ss['pts']>0).mean()*100:>5.0f}%")
print("\n  WHAT A BIGGER CAP WOULD DO (same signals, more slots):")
for ml, ms in [(2, 3), (3, 3), (2, 4), (3, 4), (4, 6), (8, 8)]:
    df = replay(V20, max_long=ml, max_short=ms)
    d = df.groupby('d')['net'].sum(); eq = d.cumsum()
    print(f"    cap {ml} long / {ms} short:  {len(df):4d}t  ${df['net'].sum():+8,.0f}  "
          f"${df['net'].sum()/N_SESS*21:+7,.0f}/mo  ${df['net'].mean():+6.1f}/t  "
          f"MaxDD ${float((eq-eq.cummax()).min()):+8,.0f}")
