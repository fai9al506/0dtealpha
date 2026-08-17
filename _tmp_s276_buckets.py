# -*- coding: utf-8 -*-
"""S276b - bucket the days: named war/geopolitical events vs the rest."""
import pandas as pd, numpy as np
m = pd.read_pickle('_tmp_s276_daily.pkl')
m['d'] = pd.to_datetime(m['d']).dt.date

# Named UNSCHEDULED geopolitical / war headline dates, 2026 (US-Iran war timeline).
# Each is the first US SESSION that could trade the headline.
EVENTS = {
 '2026-03-02': 'Mon after US/Israel strike Iran (Feb 28) + Hormuz tanker disruption',
 '2026-03-03': 'Hormuz escalation, oil surging',
 '2026-03-06': 'war selloff week',
 '2026-03-09': 'war selloff week 2',
 '2026-03-13': 'war selloff',
 '2026-03-20': 'war selloff',
 '2026-03-23': 'oil +60%, VIX 26-31 peak week',
 '2026-03-26': 'VIX peak 31.05 (Mar 27) run-up',
 '2026-03-27': 'VIX peak 31.05 - highest of the year',
 '2026-03-30': 'capitulation',
 '2026-03-31': 'one-day +2.9% relief rally',
 '2026-04-13': 'US naval blockade of Iran ports',
 '2026-04-14': 'blockade day 2',
 '2026-06-17': 'MOU signed to end the Iran war',
 '2026-06-18': 'MOU day 2',
 '2026-07-06': 'three ships attacked in the strait',
 '2026-07-07': 'Trump declares truce over, strikes resume',
 '2026-07-08': 'strikes resume day 2',
 '2026-07-23': 'oil tops $100, Houthi Red Sea attack',
 '2026-07-24': 'oil $100 day 2',
}
ev = {pd.Timestamp(k).date(): v for k, v in EVENTS.items()}
m['event'] = m['d'].map(ev)
m['is_event'] = m['event'].notna()

# data-driven shock definition, independent of the news labels
m['shock'] = (m['gap_pct'].abs() >= 0.75) | (m['range_pct'] >= 1.60)

def blk(name, sub, allm):
    if len(sub) == 0:
        print(f"  {name:34s}  (no days)"); return
    tot = sub['book_net'].sum(); avg = sub['book_net'].mean()
    grn = (sub['book_net'] > 0).mean() * 100
    print(f"  {name:34s} n={len(sub):3d}  total ${tot:+8,.0f}  avg/day ${avg:+7.0f}  green {grn:4.0f}%  "
          f"worst ${sub['book_net'].min():+7.0f}  best ${sub['book_net'].max():+7.0f}")

print("=" * 118)
print("A. NAMED WAR / GEOPOLITICAL HEADLINE DAYS  vs  EVERY OTHER SESSION   (V16 book, chain sim, costs charged)")
print("=" * 118)
blk('WAR-HEADLINE days', m[m['is_event']], m)
blk('all other sessions', m[~m['is_event']], m)
print()
print("=" * 118)
print("B. DATA-DRIVEN SHOCK DAYS (|gap| >= 0.75%  OR  intraday range >= 1.60%)")
print("=" * 118)
blk('SHOCK days', m[m['shock']], m)
blk('calm days', m[~m['shock']], m)
print()
print("  split of shock days:")
blk('   big GAP only', m[(m['gap_pct'].abs() >= 0.75) & (m['range_pct'] < 1.60)], m)
blk('   wide RANGE only', m[(m['gap_pct'].abs() < 0.75) & (m['range_pct'] >= 1.60)], m)
blk('   BOTH gap + range', m[(m['gap_pct'].abs() >= 0.75) & (m['range_pct'] >= 1.60)], m)
print()
print("=" * 118)
print("C. BY VIX LEVEL (the 'is it hot' proxy we actually have live)")
print("=" * 118)
for lo, hi in [(0, 16), (16, 18), (18, 20), (20, 22), (22, 26), (26, 99)]:
    blk(f'VIX {lo}-{hi}', m[(m['vix_max'] >= lo) & (m['vix_max'] < hi)], m)
print()
print("=" * 118)
print("D. EVERY NAMED WAR DAY, ONE LINE EACH")
print("=" * 118)
for _, r in m[m['is_event']].sort_values('d').iterrows():
    br = f"broker ${r['broker_net']:+7.0f}" if pd.notna(r['broker_net']) else "broker    n/a"
    print(f"  {r['d']}  book ${r['book_net']:+7.0f} ({r['n']:2d}t)  {br}  gap {r['gap_pct']:+5.2f}%  "
          f"rng {r['range_pct']:4.2f}%  vix {r['vix_max'] if pd.notna(r['vix_max']) else 0:4.1f}   {r['event']}")
print()
print("=" * 118)
print("E. THE MONTH THE WAR HEADLINES WERE HOTTEST (March) vs the quiet months")
print("=" * 118)
m['mo'] = pd.to_datetime(m['d']).dt.strftime('%Y-%m')
for mo, sub in m.groupby('mo'):
    print(f"  {mo}  n={len(sub):3d}  book ${sub['book_net'].sum():+8,.0f}  avg ${sub['book_net'].mean():+6.0f}/day  "
          f"green {(sub['book_net']>0).mean()*100:3.0f}%  avg VIX {sub['vix_max'].mean():4.1f}  "
          f"avg |gap| {sub['gap_pct'].abs().mean():4.2f}%")
