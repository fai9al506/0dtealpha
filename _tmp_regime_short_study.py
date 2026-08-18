import os, sys
sys.path.insert(0, '.')
g = {}
exec(open("_tmp_filter_compare.py").read().split("def report(")[0], g)
FILTERS = g['FILTERS']; load = g['load']
v7 = FILTERS['v7']; v16sb = FILTERS['v16sb']
isLongf = g['isLongf']

def sc_dd_short_readmit(l, X):
    """A SC/DD short that v7 takes but v16-sb blocks, gated by VIX < X (regime gate)."""
    if isLongf(l['direction']): return False
    if (l['setup_name'] or '') not in ('Skew Charm', 'DD Exhaustion'): return False
    if not v7(l): return False
    if v16sb(l): return False
    return (l['vix'] or 99) < X

def candidate(l, X): return v16sb(l) or sc_dd_short_readmit(l, X)

def maxdd(sel):
    sel = sorted([l for l in sel if l['outcome_pnl'] is not None], key=lambda x: x['ts'])
    cum = 0; peak = 0; dd = 0
    for l in sel:
        cum += float(l['outcome_pnl']); peak = max(peak, cum); dd = min(dd, cum - peak)
    return dd

def stats(rows, fx):
    sel = [l for l in rows if fx(l)]
    ch = sum(float(l['outcome_pnl']) for l in sel if l['outcome_pnl'] is not None)
    me = sum(float(l['mes_sim_outcome_pnl']) for l in sel if l['mes_sim_outcome_pnl'] is not None)
    dec = [l for l in sel if l['outcome_result'] in ('WIN', 'LOSS', 'EXPIRED', 'TIMEOUT')]
    wr = 100 * sum(1 for l in dec if l['outcome_result'] == 'WIN') / max(1, len(dec))
    return len(sel), ch, me, wr, maxdd(sel)

months = ['2026-03', '2026-04', '2026-05', '2026-06', '2026-07']
data = {m: load(m) for m in months}
avgvix = {}
for m in months:
    vs = [float(l['vix']) for l in data[m] if l['vix'] is not None]
    avgvix[m] = sum(vs)/len(vs) if vs else 0

print("=== Avg VIX per month (regime context) ===")
for m in months: print(f"  {m}: avgVIX={avgvix[m]:.1f}")

print("\n=== BASELINE v16-sb vs CANDIDATE (v16sb + SC/DD shorts @ VIX<X), per month ===")
print("           n   chainPts   mesPts   WR%   maxDD(chain)")
for m in months:
    n, ch, me, wr, dd = stats(data[m], v16sb)
    print(f"{m} v16sb    {n:4d} {ch:9.1f} {me:8.1f} {wr:5.0f}   {dd:7.1f}")
    for X in [18, 19, 20, 22]:
        n, ch, me, wr, dd = stats(data[m], lambda l, X=X: candidate(l, X))
        print(f"          +sh<{X:>2} {n:4d} {ch:9.1f} {me:8.1f} {wr:5.0f}   {dd:7.1f}")
    print()

print("=== IN/OUT-OF-SAMPLE: fit X on JUNE (low-vol), test on JULY (low-vol, OOS) ===")
june, july = data['2026-06'], data['2026-07']
base_n, base_ch, base_me, base_wr, base_dd = stats(june, v16sb)
print(f"June baseline v16sb: chain={base_ch:.1f} mes={base_me:.1f} WR={base_wr:.0f} DD={base_dd:.1f}")
best = None
for X in [17, 18, 19, 20, 21, 22]:
    n, ch, me, wr, dd = stats(june, lambda l, X=X: candidate(l, X))
    marker = ''
    if best is None or ch > best[1]: best = (X, ch, me, dd)
    print(f"  June cand VIX<{X}: chain={ch:.1f} mes={me:.1f} WR={wr:.0f} DD={dd:.1f}")
bestX = best[0]
print(f"\n-> best X on June = VIX<{bestX} (chain {best[1]:.1f})")
jn, jch, jme, jwr, jdd = stats(july, v16sb)
cn, cch, cme, cwr, cdd = stats(july, lambda l: candidate(l, bestX))
print(f"\nJULY OOS baseline v16sb : n={jn} chain={jch:.1f} mes={jme:.1f} WR={jwr:.0f} DD={jdd:.1f}")
print(f"JULY OOS candidate<{bestX}: n={cn} chain={cch:.1f} mes={cme:.1f} WR={cwr:.0f} DD={cdd:.1f}")
print(f"JULY OOS delta (candidate-base): chain={cch-jch:+.1f} mes={cme-jme:+.1f}")

print("\n=== Reverse OOS: fit on JULY, test on JUNE ===")
best2 = None
for X in [17, 18, 19, 20, 21, 22]:
    n, ch, me, wr, dd = stats(july, lambda l, X=X: candidate(l, X))
    if best2 is None or ch > best2[1]: best2 = (X, ch)
bX = best2[0]
jn, jch, jme, jwr, jdd = stats(june, v16sb)
cn, cch, cme, cwr, cdd = stats(june, lambda l: candidate(l, bX))
print(f"best X on July = VIX<{bX}")
print(f"JUNE OOS baseline : chain={jch:.1f} mes={jme:.1f}")
print(f"JUNE OOS cand<{bX} : chain={cch:.1f} mes={cme:.1f}  delta chain={cch-jch:+.1f} mes={cme-jme:+.1f}")

print("\n=== Confirm high-vol (Apr+May) UNHARMED by a VIX<19 short gate ===")
for m in ['2026-04', '2026-05']:
    bn, bch, bme, bwr, bdd = stats(data[m], v16sb)
    cn, cch, cme, cwr, cdd = stats(data[m], lambda l: candidate(l, 19))
    print(f"  {m}: base chain={bch:.1f}/mes={bme:.1f}  cand<19 chain={cch:.1f}/mes={cme:.1f}  (readmit n={cn-bn})")
