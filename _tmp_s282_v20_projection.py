# -*- coding: utf-8 -*-
"""S282 — V20 projection (min / max / average month, always all three),
V21 = V20 + V18's GEX-wall rule, DD Exhaustion fade, Skew Charm long vs short.

Basis is PROJECTION.md's: chain `outcome_pnl`, -0.6 pt/contract, $1.92/contract RT,
basket sizing, cap 2 long / 3 short, 90s dedup, 1 MES base. A month is 21 CALENDAR
trading sessions, never "sessions that had a trade".
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
rows = c.execute(text(f"""SELECT {lf.COLS}, outcome_pnl, outcome_elapsed_min, gex_net_ceiling
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
sess_by_mo = pd.Series(sessions).groupby(
    pd.to_datetime(pd.Series(sessions)).dt.strftime('%Y-%m')).size()


def qty(r):
    v = r.get('basket_pct')
    if v is None:
        return 1
    v = float(v)
    return 1 if abs(v) < DEAD else (2 if ((v > 0) == r['is_long']) else 1)


def v18_wall_blocks(r):
    """V18: block SHORTS with a +net-GEX wall within 15 pt overhead while VIX < 22.
    Fail-open on a missing ceiling — same as the shipped V18."""
    if r['is_long']:
        return False
    nc, vx = r.get('gex_net_ceiling'), r.get('vix')
    if nc is None or vx is None or float(vx) >= 22:
        return False
    return 0 <= float(nc) <= 15


FILTERS = {
    'V16':  lambda r: lf.passes_v16(r, gaps),
    'V20 (live)': lambda r: lf.passes_v20(r, gaps),
    'V21 = V20 + wall': lambda r: lf.passes_v20(r, gaps) and not v18_wall_blocks(r),
    'V19': lambda r: lf.passes_v19(r, gaps),
}


def replay(fn):
    openp, last, out = [], {}, []
    for r in rows:
        if not fn(r):
            continue
        t = r['et']
        openp = [p for p in openp if p[0] > t]
        if sum(1 for p in openp if p[1] == r['is_long']) >= (2 if r['is_long'] else 3):
            continue
        k = (r['setup_name'], r['is_long'])
        if k in last and (t - last[k]).total_seconds() < 90:
            continue
        last[k] = t
        openp.append((t + timedelta(minutes=float(r.get('outcome_elapsed_min') or 30)), r['is_long']))
        q = qty(r); pts = float(r['outcome_pnl'])
        out.append({'d': t.date(), 'setup': r['setup_name'], 'long': r['is_long'],
                    'pts': pts, 'q': q, 'net': (pts - HAIR) * q * DPP - FEE * q})
    return pd.DataFrame(out)


def maxdd(daily):
    eq = daily.cumsum()
    return float((eq - eq.cummax()).min())


def month_table(df):
    """Per-month $, normalised to 21 CALENDAR sessions."""
    df = df.copy()
    df['mo'] = pd.to_datetime(df['d']).dt.strftime('%Y-%m')
    out = []
    for mo, s in df.groupby('mo'):
        n = int(sess_by_mo.get(mo, 21))
        out.append({'mo': mo, 'sessions': n, 'raw': s['net'].sum(),
                    'per21': s['net'].sum() / n * 21, 'trades': len(s)})
    return pd.DataFrame(out)


res = {k: replay(f) for k, f in FILTERS.items()}

print("=" * 100)
print("1. PROJECTION — MINIMUM / MAXIMUM / AVERAGE MONTH  (always all three)")
print(f"   {N_SESS} calendar sessions 2026-03-02 -> today, 1 MES, costs charged")
print("=" * 100)
for k, df in res.items():
    mt = month_table(df)
    lo, hi = mt.loc[mt['per21'].idxmin()], mt.loc[mt['per21'].idxmax()]
    avg = df['net'].sum() / N_SESS * 21
    daily = df.groupby('d')['net'].sum()
    print(f"\n  {k}")
    print(f"    AVERAGE month  ${avg:+8,.0f}   (SAR {avg*SAR:+9,.0f})")
    print(f"    WORST   month  ${lo['per21']:+8,.0f}   (SAR {lo['per21']*SAR:+9,.0f})  {lo['mo']}")
    print(f"    BEST    month  ${hi['per21']:+8,.0f}   (SAR {hi['per21']*SAR:+9,.0f})  {hi['mo']}")
    print(f"    MaxDD ${maxdd(daily):+,.0f} · {len(df)} trades · "
          f"{(df['pts']>0).mean()*100:.0f}% WR · {int((daily<0).sum())} red days")
    print("    per month: " + "  ".join(f"{r['mo'][-2:]}:{r['per21']:+,.0f}" for _, r in mt.iterrows()))

print()
print("=" * 100)
print("2. V21 = V20 + V18's GEX-WALL RULE — is it worth live tracking?")
print("=" * 100)
v20, v21 = res['V20 (live)'], res['V21 = V20 + wall']
d20, d21 = v20.groupby('d')['net'].sum(), v21.groupby('d')['net'].sum()
print(f"  V20   ${v20['net'].sum():+8,.0f}  {len(v20):4d} trades  MaxDD ${maxdd(d20):+8,.0f}  "
      f"red {int((d20<0).sum()):3d}  WR {(v20['pts']>0).mean()*100:.0f}%")
print(f"  V21   ${v21['net'].sum():+8,.0f}  {len(v21):4d} trades  MaxDD ${maxdd(d21):+8,.0f}  "
      f"red {int((d21<0).sum()):3d}  WR {(v21['pts']>0).mean()*100:.0f}%")
print(f"  delta ${v21['net'].sum()-v20['net'].sum():+8,.0f}  "
      f"({len(v20)-len(v21)} shorts removed)  MaxDD {maxdd(d21)-maxdd(d20):+,.0f}")
mt20, mt21 = month_table(v20), month_table(v21)
print("\n  leave-one-month-out (the wall must help, or at least not hurt, in every month):")
for _, a in mt20.iterrows():
    b = mt21[mt21['mo'] == a['mo']]['per21'].iloc[0]
    print(f"    {a['mo']}  V20 ${a['per21']:+8,.0f}   V21 ${b:+8,.0f}   "
          f"{b-a['per21']:+7,.0f}  {'HELPS' if b>a['per21'] else ('SAME' if b==a['per21'] else 'HURTS')}")
