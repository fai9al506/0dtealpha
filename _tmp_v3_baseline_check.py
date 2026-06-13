"""Gate-2 cross-check: reproduce the VALIDATED v3.1 baseline on REAL logged GEX Long
signals using the production harness. If my from-scratch generator's GEX-* bucket
(34% WR) disagrees with this, my generator is broken and its non-GEX numbers can't
be trusted as absolutes."""
import psycopg2
from app.gex_long_v3 import _features, _classify, _simulate_exit, TARGET_FLOOR

DB = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
raw = psycopg2.connect(DB)
cur = raw.cursor()

# Same selection as gex_long_v3._build_cache: real logged GEX Long signals.
cur.execute("""SELECT id, ts, ts AT TIME ZONE 'America/New_York' as t_et,
                      greek_alignment, spot, paradigm
               FROM setup_log
               WHERE setup_name='GEX Long' AND grade!='LOG' AND grade IS NOT NULL
               ORDER BY ts""")
rows = cur.fetchall()

res = {"v31": [], "v31_gexonly": []}
for lid, t_utc, t_et, al, spot, para in rows:
    if not spot:
        continue
    try:
        f = _features(cur, t_utc, spot)
    except Exception:
        f = None
    verdict = _classify(f)
    if verdict not in ('A++','A','B') or f is None:
        continue
    align = al if al is not None else 0
    hour = t_et.hour if t_et else 99
    if not (align >= 0 and hour < 15):   # v3.1 filter: ABC + align>=0 + hr<15
        continue
    entry = float(spot)
    magnet = f['gex_magnet_strike']
    target = max(magnet or 0, entry + TARGET_FLOOR)
    try:
        result, pnl, mf, reason = _simulate_exit(cur, t_utc, entry, target)
    except Exception:
        continue
    if result == 'NO_PATH':
        continue
    res["v31"].append((para, result, pnl))
    if 'GEX' in (para or '').upper():
        res["v31_gexonly"].append((para, result, pnl))

def show(sigs, label):
    if not sigs:
        print(f"{label}: n=0"); return
    n=len(sigs); w=sum(1 for _,r,_ in sigs if r=='WIN'); p=sum(x for _,_,x in sigs)
    print(f"{label}: n={n}  WR={w/n*100:.0f}%  PnL={p:+.1f}p  (~${p*5:+,.0f}@1MES)")

print("VALIDATED v3.1 harness on REAL logged GEX Long signals (Feb-Jun 2026):")
show(res["v31"], "  v3.1 filter (all paradigms in log)")
show(res["v31_gexonly"], "  v3.1 filter, GEX-* only")
print("\nExpected from gex_long_v3.py docstring: ~15-16 trades, ~80% WR, +170p")
raw.close()
