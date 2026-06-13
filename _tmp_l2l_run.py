"""Tune level-to-level engine on Mar-Apr (IS), freeze best, validate May-Jun (OOS)."""
import itertools
import _tmp_l2l_engine as E

IS=("2026-03-01","2026-04-30")
OOS=("2026-05-01","2026-06-09")

# small grid (round numbers, avoid overfit): vanna strength + stop
grid=[]
for minv in (6e7, 1.0e8):
    for stop in (8, 12, 16):
        grid.append({"minv":minv,"stop":stop,"touch":5,"confirm":2,"mintgt":8,"mode":"magnet"})

print("=== IN-SAMPLE (Mar-Apr) grid ===")
print(f"{'minv':>7}{'stop':>5}   {'n':>4}{'WR':>4}{'pts':>7}{'$':>7}{'maxDD$':>8}{'avg':>6}")
results=[]
for P in grid:
    t=E.run_range(*IS,P)
    s=E.summarize(t)
    results.append((P,s,t))
    print(f"{P['minv']/1e6:>7.0f}{P['stop']:>5}   {s['n']:>4}{s['wr']:>4}{s['pts']:>7.0f}{s['usd']:>7}{s['mdd']:>8}{s['avg']:>6.1f}")

# pick robust best: positive, decent n (>=30), best $/maxDD-ish -> rank by usd then mdd
viable=[(P,s) for P,s,_ in results if s['n']>=30 and s['usd']>0]
viable.sort(key=lambda x:(x[1]['usd'], x[1]['mdd']), reverse=True)
if not viable:
    print("\nNo viable positive IS config (n>=30). Engine/rules need rework before OOS.")
else:
    best=viable[0][0]
    print(f"\n>>> SELECTED config (IS): minv={best['minv']/1e6:.0f}M stop={best['stop']}  IS={viable[0][1]}")
    print("\n=== OUT-OF-SAMPLE (May-Jun) with frozen config ===")
    t=E.run_range(*OOS,best)
    s=E.summarize(t)
    print(f"OOS: {s}")
    # by regime + by month OOS
    from collections import defaultdict
    bymo=defaultdict(list); byreg=defaultdict(list)
    for x in t: bymo[x['day'][:7]].append(x); byreg[x['reg']].append(x)
    print("  by month:", {m:E.summarize(v)['usd'] for m,v in sorted(bymo.items())})
    print("  by regime:", {r:E.summarize(v) for r,v in byreg.items()})
    # also show IS by regime for comparison
    tis=E.run_range(*IS,best)
    bregis=defaultdict(list)
    for x in tis: bregis[x['reg']].append(x)
    print("  IS by regime:", {r:E.summarize(v) for r,v in bregis.items()})
