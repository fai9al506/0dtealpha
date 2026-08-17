# -*- coding: utf-8 -*-
"""S276c - is the news/gap effect real, or noise? Same tests the Friday gate had to pass."""
import pandas as pd, numpy as np
rng = np.random.default_rng(7)
m = pd.read_pickle('_tmp_s276_daily.pkl')
m['d'] = pd.to_datetime(m['d']).dt.date
m['mo'] = pd.to_datetime(m['d']).dt.strftime('%Y-%m')

EV = ['2026-03-02','2026-03-03','2026-03-06','2026-03-09','2026-03-13','2026-03-20','2026-03-23',
      '2026-03-26','2026-03-27','2026-03-30','2026-03-31','2026-04-13','2026-04-14','2026-06-17',
      '2026-06-18','2026-07-06','2026-07-07','2026-07-08','2026-07-23','2026-07-24']
m['is_event'] = m['d'].isin([pd.Timestamp(x).date() for x in EV])

def welch(a, b):
    a, b = np.asarray(a, float), np.asarray(b, float)
    sa, sb, na, nb = a.var(ddof=1), b.var(ddof=1), len(a), len(b)
    t = (a.mean() - b.mean()) / np.sqrt(sa/na + sb/nb)
    # permutation p, two-sided
    pool = np.concatenate([a, b]); obs = abs(a.mean() - b.mean()); hit = 0
    for _ in range(20000):
        p = rng.permutation(pool)
        if abs(p[:na].mean() - p[na:].mean()) >= obs: hit += 1
    return t, hit / 20000

print("=" * 100)
print("1. WAR-HEADLINE DAYS: is the difference real?")
print("=" * 100)
a = m.loc[m['is_event'], 'book_net']; b = m.loc[~m['is_event'], 'book_net']
t, p = welch(a, b)
print(f"  war days   n={len(a):3d}  avg ${a.mean():+7.0f}/day   sd ${a.std():.0f}")
print(f"  other days n={len(b):3d}  avg ${b.mean():+7.0f}/day   sd ${b.std():.0f}")
print(f"  difference ${a.mean()-b.mean():+7.0f}/day   t={t:+.2f}   permutation p={p:.3f}")
print(f"  -> blocking every war day would have cost ${a.sum():+,.0f} over 116 sessions")

print()
print("=" * 100)
print("2. BIG-GAP DAYS (|overnight gap| >= 0.75%) - the only bucket that looked bad")
print("=" * 100)
for thr in [0.60, 0.75, 0.90, 1.00, 1.20]:
    s = m[m['gap_pct'].abs() >= thr]; o = m[m['gap_pct'].abs() < thr]
    t, p = welch(s['book_net'], o['book_net'])
    print(f"  |gap|>={thr:.2f}%  n={len(s):3d}  avg ${s['book_net'].mean():+6.0f}/day  "
          f"vs ${o['book_net'].mean():+6.0f}  total if blocked ${-s['book_net'].sum():+7,.0f}  "
          f"t={t:+.2f}  p={p:.3f}  green {(s['book_net']>0).mean()*100:3.0f}%")

print()
print("  leave-one-month-out for the |gap|>=0.75% block (does it win in EVERY month?):")
for mo, sub in m.groupby('mo'):
    s = sub[sub['gap_pct'].abs() >= 0.75]
    if len(s) == 0:
        print(f"    {mo}: no gap days"); continue
    print(f"    {mo}: n={len(s):2d}  blocked P&L ${s['book_net'].sum():+7,.0f}  "
          f"-> blocking {'HELPS' if s['book_net'].sum()<0 else 'HURTS'}")

print()
print("=" * 100)
print("3. RANDOM CONTROL - block 20 random days, 2000 times. Where does the war-day block rank?")
print("=" * 100)
obs = -m.loc[m['is_event'], 'book_net'].sum()
sims = np.array([-m['book_net'].sample(20, random_state=int(i)).sum() for i in range(2000)])
print(f"  war-day block worth ${obs:+,.0f}   random-20 mean ${sims.mean():+,.0f}  "
      f"sd ${sims.std():,.0f}   beats {(obs>sims).mean()*100:.0f}% of random blocks")
obs2 = -m.loc[m['gap_pct'].abs() >= 0.75, 'book_net'].sum()
n2 = int((m['gap_pct'].abs() >= 0.75).sum())
sims2 = np.array([-m['book_net'].sample(n2, random_state=int(i)).sum() for i in range(2000)])
print(f"  gap-day block  worth ${obs2:+,.0f}   random-{n2} mean ${sims2.mean():+,.0f}  "
      f"sd ${sims2.std():,.0f}   beats {(obs2>sims2).mean()*100:.0f}% of random blocks")
print("  (the Friday gate beat 400/400 random blocks, p=0.000 - that is the bar)")

print()
print("=" * 100)
print("4. DOES A HOT TAPE HURT LONGS OR SHORTS? (war days, by side)")
print("=" * 100)
