"""Check if F2 (long gamma pin >20M below spot) is concentrated on specific days."""
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import timedelta
import bisect

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)

# Fetch setups
sql = text("""
    SELECT id, ts, setup_name, direction, grade, spot, outcome_result, outcome_pnl,
           greek_alignment, vix, overvix, paradigm
    FROM setup_log
    WHERE outcome_result IN ('WIN', 'LOSS')
      AND spot IS NOT NULL
      AND ts::date >= '2026-02-05'
      AND direction IN ('long', 'bullish')
    ORDER BY ts
""")
with engine.connect() as conn:
    longs = conn.execute(sql).fetchall()
print(f"Total long trades: {len(longs)}")

# Fetch gamma
sql2 = text("""
    SELECT ts_utc, strike::numeric AS strike, value::numeric AS val
    FROM volland_exposure_points
    WHERE greek = 'gamma' AND expiration_option = 'TODAY'
      AND ts_utc::date >= '2026-02-05' AND value != 0
    ORDER BY ts_utc, strike
""")
with engine.connect() as conn:
    gamma_rows = conn.execute(sql2).fetchall()

# Group gamma by snapshot
gamma_snaps = defaultdict(list)
for r in gamma_rows:
    gamma_snaps[r.ts_utc].append((float(r.strike), float(r.val)))
sorted_ts = sorted(gamma_snaps.keys())

def nearest_gamma(ts, spot):
    idx = bisect.bisect_left(sorted_ts, ts)
    cands = []
    if idx > 0: cands.append(sorted_ts[idx-1])
    if idx < len(sorted_ts): cands.append(sorted_ts[idx])
    if not cands: return None
    best = min(cands, key=lambda t: abs((t - ts).total_seconds()))
    if abs((best - ts).total_seconds()) > 300: return None
    strikes = gamma_snaps[best]
    below = [(s, v) for s, v in strikes if s <= spot and abs(s - spot) <= 30 and v > 0]
    if not below: return None
    return max(below, key=lambda x: x[1])

# V9-SC filter for longs (simplified)
def v9sc_long(s):
    if s.grade == 'LOG': return False
    if s.greek_alignment is None or s.greek_alignment < 2: return False
    if s.setup_name == 'Skew Charm': return True
    vix = float(s.vix) if s.vix else None
    overvix = float(s.overvix) if s.overvix else None
    if vix is not None and vix <= 22: return True
    if overvix is not None and overvix >= 2: return True
    return False

# Analyze
print("\n=== ALL V9-SC LONGS: Date x Gamma Pin Analysis ===\n")
v9_longs = [s for s in longs if v9sc_long(s)]
print(f"V9-SC longs: {len(v9_longs)}")

by_date = defaultdict(lambda: {'all': [], 'pinned': [], 'not_pinned': []})

for s in v9_longs:
    spot = float(s.spot)
    d = str(s.ts)[:10]
    gamma_below = nearest_gamma(s.ts, spot)

    is_pinned = gamma_below is not None and gamma_below[1] > 20e6

    entry = {
        'id': s.id, 'setup': s.setup_name, 'spot': spot,
        'result': s.outcome_result, 'pnl': float(s.outcome_pnl) if s.outcome_pnl else 0,
        'gamma_below': gamma_below,
        'is_pinned': is_pinned,
        'vix': float(s.vix) if s.vix else None,
        'paradigm': s.paradigm,
        'alignment': s.greek_alignment,
    }
    by_date[d]['all'].append(entry)
    if is_pinned:
        by_date[d]['pinned'].append(entry)
    else:
        by_date[d]['not_pinned'].append(entry)

print(f"\n{'Date':>12} {'V9 Longs':>9} {'Pinned':>7} {'Pin WR':>7} {'Pin PnL':>8} {'Not Pin':>8} {'NP WR':>6} {'NP PnL':>8} {'Gamma@pin':>10}")
print(f"{'-'*12} {'-'*9} {'-'*7} {'-'*7} {'-'*8} {'-'*8} {'-'*6} {'-'*8} {'-'*10}")

total_pinned_w = 0
total_pinned_l = 0
total_pinned_pnl = 0
total_np_w = 0
total_np_l = 0
total_np_pnl = 0

for d in sorted(by_date.keys()):
    data = by_date[d]
    p = data['pinned']
    np = data['not_pinned']

    pw = sum(1 for e in p if e['result'] == 'WIN')
    pl = len(p) - pw
    pp = sum(e['pnl'] for e in p)
    npw = sum(1 for e in np if e['result'] == 'WIN')
    npl = len(np) - npw
    npp = sum(e['pnl'] for e in np)

    total_pinned_w += pw
    total_pinned_l += pl
    total_pinned_pnl += pp
    total_np_w += npw
    total_np_l += npl
    total_np_pnl += npp

    pin_wr = f"{pw/(pw+pl)*100:.0f}%" if p else "---"
    np_wr = f"{npw/(npw+npl)*100:.0f}%" if np else "---"

    # Show gamma value at pin strike
    gamma_vals = [f"{e['gamma_below'][1]/1e6:.0f}M" for e in p if e['gamma_below']] if p else ['---']

    marker = " <--" if p else ""
    print(f"{d:>12} {len(data['all']):>9} {len(p):>7} {pin_wr:>7} {pp:>+8.1f} {len(np):>8} {np_wr:>6} {npp:>+8.1f} {gamma_vals[0]:>10}{marker}")

print(f"\nTOTAL PINNED: {total_pinned_w}W/{total_pinned_l}L ({total_pinned_w/(total_pinned_w+total_pinned_l)*100:.0f}% WR), {total_pinned_pnl:+.1f} pts")
print(f"TOTAL NOT-PIN: {total_np_w}W/{total_np_l}L ({total_np_w/(total_np_w+total_np_l)*100:.0f}% WR), {total_np_pnl:+.1f} pts")

# Show each pinned trade
print(f"\n=== ALL PINNED TRADES (gamma >20M below spot) ===\n")
print(f"{'ID':>6} {'Date':>12} {'Setup':>15} {'Spot':>8} {'Res':>5} {'PnL':>7} {'Gamma Strike':>13} {'Gamma Val':>10} {'VIX':>6} {'Align':>6} {'Paradigm':>15}")
for d in sorted(by_date.keys()):
    for e in by_date[d]['pinned']:
        gs = f"{e['gamma_below'][0]:.0f}" if e['gamma_below'] else "---"
        gv = f"{e['gamma_below'][1]/1e6:.1f}M" if e['gamma_below'] else "---"
        vix = f"{e['vix']:.1f}" if e['vix'] else "---"
        print(f"{e['id']:>6} {d:>12} {e['setup']:>15} {e['spot']:>8.1f} {e['result']:>5} {e['pnl']:>+7.1f} {gs:>13} {gv:>10} {vix:>6} {e['alignment']:>6} {str(e['paradigm']):>15}")

# Also check: what was special about Mar 12-13? Other conditions?
print(f"\n=== CONTEXT: What else was happening on heavy-pin days? ===")
for d in ['2026-03-12', '2026-03-13', '2026-03-18']:
    data = by_date.get(d, {'all': []})
    if not data['all']:
        continue
    vixes = [e['vix'] for e in data['all'] if e['vix']]
    paradigms = set(str(e['paradigm']) for e in data['all'])
    print(f"\n  {d}: {len(data['all'])} longs, VIX {min(vixes):.1f}-{max(vixes):.1f}")
    print(f"    Paradigms: {paradigms}")
    print(f"    All longs WR: {sum(1 for e in data['all'] if e['result']=='WIN')}/{len(data['all'])}")
    for e in data['all']:
        gb = f"gamma_below={e['gamma_below'][1]/1e6:.0f}M@{e['gamma_below'][0]:.0f}" if e['gamma_below'] else "no_gamma"
        print(f"      #{e['id']} {e['setup']:>12} {e['result']:>5} {e['pnl']:>+6.1f} {gb}")
