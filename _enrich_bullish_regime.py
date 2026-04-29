"""Enrich short signals with net GEX + net charm features for regime backtest."""
import os, json, pickle, sys
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from collections import defaultdict

os.environ['DATABASE_URL'] = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
eng = create_engine(os.environ['DATABASE_URL'])
ET = ZoneInfo('America/New_York')

def v12fix(setup, align, para, grade, ts_et):
    if setup in ("VIX Divergence","IV Momentum","Vanna Butterfly"): return False
    if setup == "Skew Charm" and grade in ("C","LOG"): return False
    t = ts_et.time()
    if setup in ("Skew Charm","DD Exhaustion"):
        if dtime(14,30)<=t<dtime(15,0): return False
        if t>=dtime(15,30): return False
    if setup == "BofA Scalp" and t>=dtime(14,30): return False
    if setup in ("Skew Charm","DD Exhaustion") and para == "GEX-LIS": return False
    if setup == "AG Short" and para == "AG-TARGET": return False
    if setup in ("Skew Charm","AG Short"): return True
    if setup == "DD Exhaustion" and (align or 0) != 0: return True
    return False

def compute_gex_features(chain_rows, spot):
    gex = []
    for row in chain_rows:
        try:
            strike = row[10]
            cg = (row[3] or 0) * (row[1] or 0)
            pg = (row[17] or 0) * (row[19] or 0)
            gex.append((strike, cg - pg))
        except: continue
    if not gex: return None,None,None,None,None
    net_total = sum(v for _,v in gex)
    net_near = sum(v for s,v in gex if abs(s-spot)<=40)
    above = [(s,v) for s,v in gex if s > spot]
    below = [(s,v) for s,v in gex if s < spot]
    strong_plus_above = sorted(above, key=lambda x: x[1], reverse=True)[0] if above else (None,0)
    strong_minus_below = sorted(below, key=lambda x: x[1])[0] if below else (None,0)
    return net_total, net_near, strong_plus_above[0], strong_plus_above[1], strong_minus_below[1]

def flush_print(*args, **kw):
    print(*args, **kw, flush=True)

with eng.connect() as c:
    flush_print('Pre-loading chain_snapshots...')
    chain_rows = c.execute(text("""
        SELECT ts, spot, rows FROM chain_snapshots
        WHERE ts >= '2026-03-01' AND ts <= '2026-04-16'
        ORDER BY ts
    """)).fetchall()
    flush_print(f'  loaded {len(chain_rows)} snapshots')
    # Index by ts
    chain_by_ts = {}
    for ts, sp, rows in chain_rows:
        chain_by_ts[ts] = (float(sp) if sp else None, rows if isinstance(rows, list) else json.loads(rows) if rows else None)
    chain_ts_sorted = sorted(chain_by_ts.keys())

    flush_print('Pre-loading volland charm (net by ts)...')
    charm_rows = c.execute(text("""
        SELECT ts_utc, SUM(CAST(value AS FLOAT)) AS net_charm
        FROM volland_exposure_points
        WHERE ts_utc >= '2026-03-01' AND ts_utc <= '2026-04-16'
          AND ticker = 'SPX' AND greek = 'charm'
        GROUP BY ts_utc ORDER BY ts_utc
    """)).fetchall()
    flush_print(f'  loaded {len(charm_rows)} charm ts')
    charm_by_ts = {ts: float(v) for ts, v in charm_rows}
    charm_ts_sorted = sorted(charm_by_ts.keys())

def find_nearest(target_ts, sorted_ts, max_delta_sec):
    """Binary search nearest ts within max_delta_sec."""
    import bisect
    i = bisect.bisect_left(sorted_ts, target_ts)
    candidates = []
    if i > 0: candidates.append(sorted_ts[i-1])
    if i < len(sorted_ts): candidates.append(sorted_ts[i])
    if not candidates: return None
    best = min(candidates, key=lambda x: abs((x-target_ts).total_seconds()))
    if abs((best-target_ts).total_seconds()) > max_delta_sec: return None
    return best

with eng.connect() as c:
    flush_print('Fetching signals...')
    sigs = c.execute(text("""
        SELECT id, ts, setup_name, grade, paradigm, spot, greek_alignment,
               outcome_result, outcome_pnl, vix, overvix
        FROM setup_log
        WHERE ts >= '2026-03-01' AND ts <= '2026-04-15 23:59:59'
          AND direction = 'short' AND setup_name IN ('Skew Charm','AG Short','DD Exhaustion')
          AND outcome_pnl IS NOT NULL
        ORDER BY ts
    """)).fetchall()
    flush_print(f'  {len(sigs)} raw signals')

results = []
for sig in sigs:
    sig_id, ts, setup, grade, para, spot, align, out, pnl, vix, ov = sig
    ts_et = ts.astimezone(ET)
    if not v12fix(setup, align, para, grade, ts_et): continue

    ch_ts = find_nearest(ts, chain_ts_sorted, 180)
    if not ch_ts: continue
    ch_spot, chain = chain_by_ts[ch_ts]
    if not chain or ch_spot is None: continue
    net_t, net_n, sp_above_k, sp_above_v, sm_below_v = compute_gex_features(chain, float(spot))
    if net_t is None: continue

    cm_ts = find_nearest(ts, charm_ts_sorted, 300)
    net_charm = charm_by_ts[cm_ts] if cm_ts else None

    results.append({
        'id':sig_id,'ts':ts,'ts_et':ts_et,'date':ts_et.date(),'setup':setup,'grade':grade,
        'paradigm':para,'spot':float(spot),'align':align,'out':out,'pnl':float(pnl),
        'vix':float(vix) if vix else None, 'overvix':float(ov) if ov else None,
        'net_gex':net_t, 'net_gex_near':net_n,
        'plus_above_k':sp_above_k, 'plus_above_v':sp_above_v or 0,
        'minus_below_v':sm_below_v or 0,
        'net_charm':net_charm
    })

flush_print(f'Enriched: {len(results)} signals')
with open('enriched_shorts.pkl','wb') as f: pickle.dump(results, f)
flush_print('Saved to enriched_shorts.pkl')
