"""
Paradigm Gate Analysis — 2026-03-11
Analyzes whether filtering trades by Volland paradigm improves performance.
"""

import os
import json
import psycopg2
import psycopg2.extras
from collections import defaultdict

DATABASE_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ============================================================
# PART 1: Check today's paradigm around 12:30-14:00 ET
# ============================================================
print("=" * 100)
print("PART 1: Volland paradigm today 2026-03-11 around 12:30-14:00 ET (17:00-19:30 UTC)")
print("=" * 100)

cur.execute("""
SELECT ts, payload
FROM volland_snapshots
WHERE ts >= '2026-03-11 17:00:00' AND ts <= '2026-03-11 19:30:00'
ORDER BY ts;
""")
rows = cur.fetchall()
if rows:
    print(f"{'ts':<28} {'paradigm':<18} {'charm':<14} {'vanna_all':<18} {'svb':<10} {'dd_hedg':<12}")
    print("-" * 100)
    for r in rows:
        p = r['payload']
        if isinstance(p, str):
            p = json.loads(p)
        stats = p.get('statistics', {})
        paradigm = stats.get('paradigm', '')
        charm = str(stats.get('aggregatedCharm', ''))[:12]
        # Vanna is in exposure_points or statistics — check
        vanna_all = ''
        svb = str(stats.get('spot_vol_beta', ''))[:8]
        dd = str(stats.get('delta_decay_hedging', ''))[:10]
        print(f"{str(r['ts']):<28} {paradigm:<18} {charm:<14} {vanna_all:<18} {svb:<10} {dd:<12}")
    print(f"\nTotal snapshots in range: {len(rows)}")
else:
    print("No data found for that time range.")

# ============================================================
# PART 2: Pull all setup_log entries with outcomes
# ============================================================
print("\n" + "=" * 100)
print("PART 2: Pulling all setup_log entries with outcomes...")
print("=" * 100)

cur.execute("""
SELECT
  id, ts, setup_name, direction, grade, score,
  paradigm, spot,
  outcome_result, outcome_pnl,
  outcome_max_profit, outcome_max_loss,
  greek_alignment,
  vanna_all, vanna_weekly, vanna_monthly, spot_vol_beta
FROM setup_log
WHERE outcome_result IS NOT NULL
ORDER BY ts;
""")
trades = cur.fetchall()
print(f"Total trades with outcomes: {len(trades)}")

# Normalize direction to uppercase
for t in trades:
    if t['direction']:
        t['direction'] = t['direction'].upper()
        # normalize "bullish"/"bearish" to "LONG"/"SHORT"
        if t['direction'] == 'BULLISH':
            t['direction'] = 'LONG'
        elif t['direction'] == 'BEARISH':
            t['direction'] = 'SHORT'

# Check what paradigm values exist
paradigm_values = set()
for t in trades:
    if t['paradigm']:
        paradigm_values.add(t['paradigm'])

print(f"\nUnique paradigm values in setup_log: {sorted(paradigm_values)}")

# Count paradigm distribution
paradigm_counts = defaultdict(int)
for t in trades:
    p = t['paradigm'] or 'NULL'
    paradigm_counts[p] += 1
print(f"\nParadigm distribution:")
for p, c in sorted(paradigm_counts.items(), key=lambda x: -x[1]):
    print(f"  {p}: {c}")

# Direction distribution
dir_counts = defaultdict(int)
for t in trades:
    dir_counts[t['direction']] += 1
print(f"\nDirection distribution: {dict(dir_counts)}")

# Outcome distribution
outcome_counts = defaultdict(int)
for t in trades:
    outcome_counts[t['outcome_result']] += 1
print(f"Outcome distribution: {dict(outcome_counts)}")

# ============================================================
# Helper functions
# ============================================================
def is_win(t):
    return t['outcome_result'] in ('WIN', 'TRAIL')

def is_loss(t):
    return t['outcome_result'] in ('LOSS',)

def compute_stats(trade_list):
    total = len(trade_list)
    if total == 0:
        return {'total': 0, 'wins': 0, 'losses': 0, 'expired': 0, 'wr': 0, 'pnl': 0, 'pf': 0,
                'gross_profit': 0, 'gross_loss': 0}

    wins = sum(1 for t in trade_list if is_win(t))
    losses = sum(1 for t in trade_list if is_loss(t))
    expired = sum(1 for t in trade_list if t['outcome_result'] == 'EXPIRED')

    pnl = sum(float(t['outcome_pnl'] or 0) for t in trade_list)

    gross_profit = sum(float(t['outcome_pnl'] or 0) for t in trade_list if float(t['outcome_pnl'] or 0) > 0)
    gross_loss = abs(sum(float(t['outcome_pnl'] or 0) for t in trade_list if float(t['outcome_pnl'] or 0) < 0))

    wr = wins / total * 100 if total > 0 else 0
    pf = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    return {
        'total': total, 'wins': wins, 'losses': losses, 'expired': expired,
        'wr': wr, 'pnl': pnl, 'pf': pf,
        'gross_profit': gross_profit, 'gross_loss': gross_loss
    }

def print_stats_oneline(stats, label=""):
    pf_str = f"{stats['pf']:.2f}" if stats['pf'] != float('inf') else "inf"
    print(f"  {label:<35s} {stats['total']:>4d} trades  WR={stats['wr']:5.1f}%  PnL={stats['pnl']:>+8.1f}  PF={pf_str:>6s}")

# ============================================================
# PART 3: Paradigm gate analysis
# ============================================================
print("\n" + "=" * 100)
print("PART 3: PARADIGM GATE ANALYSIS")
print("=" * 100)

BEARISH_PARADIGMS_STRICT = {'AG', 'BOFA', 'SIDIAL'}
BULLISH_PARADIGMS_STRICT = {'GEX'}

BEARISH_PARADIGMS_SOFT = {'AG-PURE', 'AG-LIS', 'SIDIAL-PURE', 'SIDIAL-LIS'}
BULLISH_PARADIGMS_SOFT = {'GEX-PURE', 'GEX-LIS'}

def get_base_paradigm(p):
    if not p:
        return None
    return p.split('-')[0].upper()

def is_blocked_strict(t):
    p = t['paradigm']
    if not p:
        return False
    base = get_base_paradigm(p)
    direction = t['direction']
    if direction == 'LONG' and base in BEARISH_PARADIGMS_STRICT:
        return True
    if direction == 'SHORT' and base in BULLISH_PARADIGMS_STRICT:
        return True
    return False

def is_blocked_soft(t):
    p = t['paradigm']
    if not p:
        return False
    p_upper = p.upper()
    direction = t['direction']
    if direction == 'LONG' and p_upper in BEARISH_PARADIGMS_SOFT:
        return True
    if direction == 'SHORT' and p_upper in BULLISH_PARADIGMS_SOFT:
        return True
    return False

trades_with_paradigm = [t for t in trades if t['paradigm'] is not None]
trades_no_paradigm = [t for t in trades if t['paradigm'] is None]

print(f"\nTrades with paradigm data: {len(trades_with_paradigm)}")
print(f"Trades without paradigm data: {len(trades_no_paradigm)}")

no_gate_all = trades
no_gate_wp = trades_with_paradigm

strict_passed = [t for t in trades_with_paradigm if not is_blocked_strict(t)]
strict_blocked = [t for t in trades_with_paradigm if is_blocked_strict(t)]

soft_passed = [t for t in trades_with_paradigm if not is_blocked_soft(t)]
soft_blocked = [t for t in trades_with_paradigm if is_blocked_soft(t)]

print(f"\n{'Scenario':<35s} {'Trades':>7s} {'Blocked':>8s}")
print("-" * 55)
print(f"{'No gate (all trades)':<35s} {len(no_gate_all):>7d} {'0':>8s}")
print(f"{'No gate (with paradigm only)':<35s} {len(no_gate_wp):>7d} {'0':>8s}")
print(f"{'Strict gate (with paradigm only)':<35s} {len(strict_passed):>7d} {len(strict_blocked):>8d}")
print(f"{'Soft gate (with paradigm only)':<35s} {len(soft_passed):>7d} {len(soft_blocked):>8d}")

print("\n--- Overall Stats ---")
print_stats_oneline(compute_stats(no_gate_all), "No gate (ALL)")
print_stats_oneline(compute_stats(no_gate_wp), "No gate (w/ paradigm)")
print_stats_oneline(compute_stats(strict_passed), "Strict gate PASSED")
print_stats_oneline(compute_stats(strict_blocked), "Strict gate BLOCKED")
print_stats_oneline(compute_stats(soft_passed), "Soft gate PASSED")
print_stats_oneline(compute_stats(soft_blocked), "Soft gate BLOCKED")

# Breakdown by setup_name for each scenario
print("\n--- Breakdown by Setup (No Gate — all trades) ---")
by_setup = defaultdict(list)
for t in no_gate_all:
    by_setup[t['setup_name']].append(t)
for setup in sorted(by_setup.keys()):
    print_stats_oneline(compute_stats(by_setup[setup]), setup)

print("\n--- Breakdown by Setup (Strict Gate — PASSED) ---")
by_setup = defaultdict(list)
for t in strict_passed:
    by_setup[t['setup_name']].append(t)
for setup in sorted(by_setup.keys()):
    print_stats_oneline(compute_stats(by_setup[setup]), setup)

print("\n--- Breakdown by Setup (Strict Gate — BLOCKED) ---")
by_setup = defaultdict(list)
for t in strict_blocked:
    by_setup[t['setup_name']].append(t)
for setup in sorted(by_setup.keys()):
    print_stats_oneline(compute_stats(by_setup[setup]), setup)

print("\n--- Breakdown by Setup (Soft Gate — PASSED) ---")
by_setup = defaultdict(list)
for t in soft_passed:
    by_setup[t['setup_name']].append(t)
for setup in sorted(by_setup.keys()):
    print_stats_oneline(compute_stats(by_setup[setup]), setup)

print("\n--- Breakdown by Setup (Soft Gate — BLOCKED) ---")
by_setup = defaultdict(list)
for t in soft_blocked:
    by_setup[t['setup_name']].append(t)
for setup in sorted(by_setup.keys()):
    print_stats_oneline(compute_stats(by_setup[setup]), setup)

# ============================================================
# PART 4: What happens to blocked trades?
# ============================================================
print("\n" + "=" * 100)
print("PART 4: BLOCKED TRADES — What would we have missed?")
print("=" * 100)

print("\n--- STRICT GATE: Blocked trades detail ---")
print(f"{'id':>5s} {'date':<12s} {'setup':<20s} {'dir':<6s} {'paradigm':<18s} {'grade':<6s} {'outcome':<10s} {'pts':>8s}")
print("-" * 90)
for t in sorted(strict_blocked, key=lambda x: x['ts']):
    print(f"{t['id']:>5d} {str(t['ts'])[:10]:<12s} {t['setup_name']:<20s} {t['direction']:<6s} "
          f"{t['paradigm'] or '':<18s} {t['grade']:<6s} {t['outcome_result']:<10s} {float(t['outcome_pnl'] or 0):>+8.1f}")

blocked_wins = [t for t in strict_blocked if is_win(t)]
blocked_losses = [t for t in strict_blocked if is_loss(t)]
blocked_expired = [t for t in strict_blocked if t['outcome_result'] == 'EXPIRED']
print(f"\nBlocked summary: {len(strict_blocked)} total")
print(f"  Winners blocked: {len(blocked_wins)} ({sum(float(t['outcome_pnl'] or 0) for t in blocked_wins):+.1f} pts)")
print(f"  Losers blocked:  {len(blocked_losses)} ({sum(float(t['outcome_pnl'] or 0) for t in blocked_losses):+.1f} pts)")
print(f"  Expired blocked: {len(blocked_expired)} ({sum(float(t['outcome_pnl'] or 0) for t in blocked_expired):+.1f} pts)")
print(f"  Net saved by blocking: {-sum(float(t['outcome_pnl'] or 0) for t in strict_blocked):+.1f} pts")

if len(soft_blocked) > 30:
    print(f"\n--- SOFT GATE: Blocked trades ({len(soft_blocked)} total, showing summary) ---")
else:
    print(f"\n--- SOFT GATE: Blocked trades detail ---")
    print(f"{'id':>5s} {'date':<12s} {'setup':<20s} {'dir':<6s} {'paradigm':<18s} {'grade':<6s} {'outcome':<10s} {'pts':>8s}")
    print("-" * 90)
    for t in sorted(soft_blocked, key=lambda x: x['ts']):
        print(f"{t['id']:>5d} {str(t['ts'])[:10]:<12s} {t['setup_name']:<20s} {t['direction']:<6s} "
              f"{t['paradigm'] or '':<18s} {t['grade']:<6s} {t['outcome_result']:<10s} {float(t['outcome_pnl'] or 0):>+8.1f}")

blocked_wins = [t for t in soft_blocked if is_win(t)]
blocked_losses = [t for t in soft_blocked if is_loss(t)]
blocked_expired = [t for t in soft_blocked if t['outcome_result'] == 'EXPIRED']
print(f"\nBlocked summary: {len(soft_blocked)} total")
print(f"  Winners blocked: {len(blocked_wins)} ({sum(float(t['outcome_pnl'] or 0) for t in blocked_wins):+.1f} pts)")
print(f"  Losers blocked:  {len(blocked_losses)} ({sum(float(t['outcome_pnl'] or 0) for t in blocked_losses):+.1f} pts)")
print(f"  Expired blocked: {len(blocked_expired)} ({sum(float(t['outcome_pnl'] or 0) for t in blocked_expired):+.1f} pts)")
print(f"  Net saved by blocking: {-sum(float(t['outcome_pnl'] or 0) for t in soft_blocked):+.1f} pts")

# ============================================================
# PART 5: Win rates by paradigm and direction
# ============================================================
print("\n" + "=" * 100)
print("PART 5: WIN RATE BY PARADIGM x DIRECTION")
print("=" * 100)

by_paradigm_dir = defaultdict(list)
for t in trades_with_paradigm:
    base = get_base_paradigm(t['paradigm'])
    by_paradigm_dir[(base, t['direction'])].append(t)

by_subtype_dir = defaultdict(list)
for t in trades_with_paradigm:
    by_subtype_dir[(t['paradigm'], t['direction'])].append(t)

print("\n--- By BASE paradigm ---")
print(f"{'Paradigm':<12s} {'Direction':<8s} {'Trades':>7s} {'Wins':>6s} {'WR%':>7s} {'PnL':>9s} {'PF':>7s}")
print("-" * 60)
bases = sorted(set(k[0] for k in by_paradigm_dir.keys() if k[0] is not None))
for base in bases:
    for d in ['LONG', 'SHORT']:
        tl = by_paradigm_dir.get((base, d), [])
        if not tl:
            continue
        s = compute_stats(tl)
        pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
        print(f"{base:<12s} {d:<8s} {s['total']:>7d} {s['wins']:>6d} {s['wr']:>6.1f}% {s['pnl']:>+8.1f} {pf_str:>7s}")

print("\n--- By FULL paradigm subtype ---")
print(f"{'Paradigm Subtype':<20s} {'Dir':<6s} {'Trades':>6s} {'Wins':>5s} {'WR%':>7s} {'PnL':>9s} {'PF':>7s}")
print("-" * 68)
subtypes = sorted(set(k[0] for k in by_subtype_dir.keys() if k[0]))
for sub in subtypes:
    for d in ['LONG', 'SHORT']:
        tl = by_subtype_dir.get((sub, d), [])
        if not tl:
            continue
        s = compute_stats(tl)
        pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
        print(f"{sub:<20s} {d:<6s} {s['total']:>6d} {s['wins']:>5d} {s['wr']:>6.1f}% {s['pnl']:>+8.1f} {pf_str:>7s}")

print("\n--- By BASE paradigm (all directions combined) ---")
print(f"{'Paradigm':<12s} {'Trades':>7s} {'Wins':>6s} {'WR%':>7s} {'PnL':>9s} {'PF':>7s}")
print("-" * 50)
by_paradigm = defaultdict(list)
for t in trades_with_paradigm:
    base = get_base_paradigm(t['paradigm'])
    by_paradigm[base].append(t)
for base in sorted(k for k in by_paradigm.keys() if k is not None):
    s = compute_stats(by_paradigm[base])
    pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
    print(f"{base:<12s} {s['total']:>7d} {s['wins']:>6d} {s['wr']:>6.1f}% {s['pnl']:>+8.1f} {pf_str:>7s}")

# ============================================================
# PART 6: Paradigm direction affinity (which paradigm favors which direction?)
# ============================================================
print("\n" + "=" * 100)
print("PART 6: PARADIGM DIRECTION AFFINITY — Which paradigm favors which direction?")
print("=" * 100)

print(f"\n{'Paradigm':<12s} | {'LONG trades':>11s} {'LONG WR':>8s} {'LONG PnL':>9s} | {'SHORT trades':>12s} {'SHORT WR':>9s} {'SHORT PnL':>10s} | {'Verdict'}")
print("-" * 100)
for base in sorted(k for k in by_paradigm.keys() if k is not None):
    long_trades = by_paradigm_dir.get((base, 'LONG'), [])
    short_trades = by_paradigm_dir.get((base, 'SHORT'), [])
    ls = compute_stats(long_trades)
    ss = compute_stats(short_trades)

    verdict = ""
    if ls['total'] >= 3 and ss['total'] >= 3:
        if ls['wr'] > ss['wr'] + 10:
            verdict = "FAVORS LONG"
        elif ss['wr'] > ls['wr'] + 10:
            verdict = "FAVORS SHORT"
        else:
            verdict = "NEUTRAL"
    else:
        verdict = "LOW SAMPLE"

    print(f"{base:<12s} | {ls['total']:>11d} {ls['wr']:>7.1f}% {ls['pnl']:>+8.1f} | {ss['total']:>12d} {ss['wr']:>8.1f}% {ss['pnl']:>+9.1f} | {verdict}")

# ============================================================
# PART 7: Vanna sign analysis
# ============================================================
print("\n" + "=" * 100)
print("PART 7: VANNA SIGN x DIRECTION ANALYSIS")
print("=" * 100)

def parse_numeric(val):
    if val is None:
        return None
    try:
        cleaned = str(val).replace('$', '').replace(',', '').strip()
        return float(cleaned)
    except (ValueError, TypeError):
        return None

vanna_fields = [('vanna_all', 'VANNA ALL EXPIRIES'), ('vanna_weekly', 'VANNA WEEKLY'), ('vanna_monthly', 'VANNA MONTHLY')]

for field_name, label in vanna_fields:
    print(f"\n--- {label} ---")

    trades_with_field = [(t, parse_numeric(t[field_name])) for t in trades]
    trades_with_field = [(t, v) for t, v in trades_with_field if v is not None]

    if not trades_with_field:
        print(f"  No trades have {field_name} data.")
        continue

    print(f"  Trades with {field_name} data: {len(trades_with_field)}")

    for direction in ['LONG', 'SHORT']:
        pos = [t for t, v in trades_with_field if t['direction'] == direction and v > 0]
        neg = [t for t, v in trades_with_field if t['direction'] == direction and v < 0]
        zero = [t for t, v in trades_with_field if t['direction'] == direction and v == 0]

        print(f"\n  {direction}:")
        if pos:
            s = compute_stats(pos)
            pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
            print(f"    {field_name} > 0 (positive): {s['total']:>4d} trades  WR={s['wr']:5.1f}%  PnL={s['pnl']:>+8.1f}  PF={pf_str}")
        if neg:
            s = compute_stats(neg)
            pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
            print(f"    {field_name} < 0 (negative): {s['total']:>4d} trades  WR={s['wr']:5.1f}%  PnL={s['pnl']:>+8.1f}  PF={pf_str}")
        if zero:
            s = compute_stats(zero)
            pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
            print(f"    {field_name} = 0 (neutral):  {s['total']:>4d} trades  WR={s['wr']:5.1f}%  PnL={s['pnl']:>+8.1f}  PF={pf_str}")

        # Aligned vs opposed
        if pos and neg:
            if direction == 'LONG':
                aligned = pos   # positive vanna = bullish (aligned with LONG)
                opposed = neg   # negative vanna = bearish (opposed to LONG)
            else:
                aligned = neg   # negative vanna = bearish (aligned with SHORT)
                opposed = pos   # positive vanna = bullish (opposed to SHORT)
            sa = compute_stats(aligned)
            so = compute_stats(opposed)
            delta_wr = sa['wr'] - so['wr']
            delta_pnl = sa['pnl'] - so['pnl']
            print(f"    --> Aligned: WR={sa['wr']:.1f}%, PnL={sa['pnl']:+.1f} | Opposed: WR={so['wr']:.1f}%, PnL={so['pnl']:+.1f} | Delta: WR={delta_wr:+.1f}%, PnL={delta_pnl:+.1f}")

# ============================================================
# PART 8: SVB analysis
# ============================================================
print("\n" + "=" * 100)
print("PART 8: SPOT-VOL BETA x DIRECTION ANALYSIS")
print("=" * 100)

trades_with_svb = [(t, parse_numeric(t['spot_vol_beta'])) for t in trades]
trades_with_svb = [(t, v) for t, v in trades_with_svb if v is not None]
print(f"Trades with SVB data: {len(trades_with_svb)}")

for direction in ['LONG', 'SHORT']:
    pos = [t for t, v in trades_with_svb if t['direction'] == direction and v > 0]
    neg = [t for t, v in trades_with_svb if t['direction'] == direction and v < 0]
    weak_neg = [t for t, v in trades_with_svb if t['direction'] == direction and -0.5 <= v < 0]

    print(f"\n  {direction}:")
    if pos:
        s = compute_stats(pos)
        pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
        print(f"    SVB > 0 (positive):          {s['total']:>4d} trades  WR={s['wr']:5.1f}%  PnL={s['pnl']:>+8.1f}  PF={pf_str}")
    if neg:
        s = compute_stats(neg)
        pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
        print(f"    SVB < 0 (negative):          {s['total']:>4d} trades  WR={s['wr']:5.1f}%  PnL={s['pnl']:>+8.1f}  PF={pf_str}")
    if weak_neg:
        s = compute_stats(weak_neg)
        pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
        print(f"    SVB -0.5 to 0 (weak-neg):   {s['total']:>4d} trades  WR={s['wr']:5.1f}%  PnL={s['pnl']:>+8.1f}  PF={pf_str}")

# ============================================================
# PART 9: Combined paradigm + vanna gate
# ============================================================
print("\n" + "=" * 100)
print("PART 9: COMBINED GATES — Paradigm + Vanna")
print("=" * 100)

def is_blocked_combined(t):
    p = t['paradigm']
    direction = t['direction']

    blocked_by_paradigm = False
    if p:
        base = get_base_paradigm(p)
        if direction == 'LONG' and base in BEARISH_PARADIGMS_STRICT:
            blocked_by_paradigm = True
        if direction == 'SHORT' and base in BULLISH_PARADIGMS_STRICT:
            blocked_by_paradigm = True

    blocked_by_vanna = False
    v = parse_numeric(t['vanna_all'])
    if v is not None:
        if direction == 'LONG' and v < 0:
            blocked_by_vanna = True
        if direction == 'SHORT' and v > 0:
            blocked_by_vanna = True

    return blocked_by_paradigm or blocked_by_vanna

trades_both = [t for t in trades if t['paradigm'] is not None and parse_numeric(t['vanna_all']) is not None]
print(f"Trades with BOTH paradigm and vanna_all data: {len(trades_both)}")

combined_passed = [t for t in trades_both if not is_blocked_combined(t)]
combined_blocked = [t for t in trades_both if is_blocked_combined(t)]

print(f"\nCombined gate: {len(combined_passed)} passed, {len(combined_blocked)} blocked")
print_stats_oneline(compute_stats(trades_both), "No gate (both data)")
print_stats_oneline(compute_stats(combined_passed), "Combined gate PASSED")
print_stats_oneline(compute_stats(combined_blocked), "Combined gate BLOCKED")

# Vanna-only gate
print("\n--- Vanna-only gate (block LONG when vanna_all<0, SHORT when vanna_all>0) ---")
trades_vanna = [t for t in trades if parse_numeric(t['vanna_all']) is not None]
vanna_passed = [t for t in trades_vanna if not (
    (t['direction'] == 'LONG' and parse_numeric(t['vanna_all']) < 0) or
    (t['direction'] == 'SHORT' and parse_numeric(t['vanna_all']) > 0)
)]
vanna_blocked = [t for t in trades_vanna if (
    (t['direction'] == 'LONG' and parse_numeric(t['vanna_all']) < 0) or
    (t['direction'] == 'SHORT' and parse_numeric(t['vanna_all']) > 0)
)]
print(f"Trades with vanna_all: {len(trades_vanna)}, passed: {len(vanna_passed)}, blocked: {len(vanna_blocked)}")
print_stats_oneline(compute_stats(trades_vanna), "No gate")
print_stats_oneline(compute_stats(vanna_passed), "Vanna gate PASSED")
print_stats_oneline(compute_stats(vanna_blocked), "Vanna gate BLOCKED")

# ============================================================
# PART 10: Per-setup paradigm gate impact
# ============================================================
print("\n" + "=" * 100)
print("PART 10: PER-SETUP PARADIGM GATE IMPACT (strict gate)")
print("=" * 100)

setup_names = sorted(set(t['setup_name'] for t in trades_with_paradigm))
for setup in setup_names:
    setup_trades = [t for t in trades_with_paradigm if t['setup_name'] == setup]
    setup_passed = [t for t in setup_trades if not is_blocked_strict(t)]
    setup_blocked = [t for t in setup_trades if is_blocked_strict(t)]

    if not setup_blocked:
        continue  # no impact

    s_all = compute_stats(setup_trades)
    s_pass = compute_stats(setup_passed)
    s_block = compute_stats(setup_blocked)

    print(f"\n  {setup}:")
    print(f"    No gate:  {s_all['total']:>3d} trades  WR={s_all['wr']:5.1f}%  PnL={s_all['pnl']:>+8.1f}  PF={s_all['pf']:.2f}")
    print(f"    Passed:   {s_pass['total']:>3d} trades  WR={s_pass['wr']:5.1f}%  PnL={s_pass['pnl']:>+8.1f}  PF={s_pass['pf']:.2f}")
    print(f"    Blocked:  {s_block['total']:>3d} trades  WR={s_block['wr']:5.1f}%  PnL={s_block['pnl']:>+8.1f}  PF={s_block['pf']:.2f}")
    delta_pnl = s_pass['pnl'] - s_all['pnl']
    print(f"    Impact: {delta_pnl:+.1f} pts (removed {s_block['pnl']:+.1f} from blocked)")

# ============================================================
# PART 11: Summary comparison table
# ============================================================
print("\n" + "=" * 100)
print("PART 11: SUMMARY COMPARISON — ALL GATE SCENARIOS")
print("=" * 100)

scenarios = [
    ("Baseline (all trades)", no_gate_all),
    ("Baseline (w/ paradigm)", no_gate_wp),
    ("Strict paradigm gate", strict_passed),
    ("Soft paradigm gate", soft_passed),
]

if trades_vanna:
    scenarios.append(("Vanna-all gate", vanna_passed))
if trades_both:
    scenarios.append(("Combined (paradigm+vanna)", combined_passed))

print(f"\n{'Scenario':<35s} {'Trades':>7s} {'WR%':>7s} {'PnL':>10s} {'PF':>7s} {'pts/trade':>10s}")
print("-" * 82)
for label, tl in scenarios:
    s = compute_stats(tl)
    pf_str = f"{s['pf']:.2f}" if s['pf'] != float('inf') else "inf"
    avg = s['pnl'] / s['total'] if s['total'] > 0 else 0
    print(f"{label:<35s} {s['total']:>7d} {s['wr']:>6.1f}% {s['pnl']:>+9.1f} {pf_str:>7s} {avg:>+9.2f}")

# Also show "existing Greek filter" comparison
print("\n--- Comparison with existing Greek alignment filter ---")
existing_filter = [t for t in trades if t['greek_alignment'] is not None and t['greek_alignment'] >= 0]
print_stats_oneline(compute_stats(no_gate_all), "No filter (all)")
print_stats_oneline(compute_stats(existing_filter), "Greek align >= 0")
print_stats_oneline(compute_stats(strict_passed), "Strict paradigm gate")

# Combined: greek align >= 0 AND strict paradigm gate
combined_existing = [t for t in strict_passed if t['greek_alignment'] is not None and t['greek_alignment'] >= 0]
print_stats_oneline(compute_stats(combined_existing), "Greek>=0 + Strict paradigm")

conn.close()
print("\nDone.")
