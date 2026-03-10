"""
Analyze: What paradigm did we have on Mar 9? Why didn't we use it?
Then: How would a paradigm directional gate perform historically?
"""
import os
from sqlalchemy import create_engine, text
from collections import defaultdict

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# 1. What paradigm did we have on Mar 9?
print("=" * 70)
print("PARADIGM DATA ON MAR 9")
print("=" * 70)

# Check volland_snapshots for paradigm (payload is JSONB)
rows = c.execute(text("""
    SELECT ts,
           payload->'statistics'->>'paradigm' as paradigm,
           payload->'statistics'->>'lis' as lis,
           payload->'statistics'->>'aggregatedCharm' as charm,
           payload->'statistics'->>'spot' as spot
    FROM volland_snapshots
    WHERE ts::date = '2026-03-09'
    ORDER BY ts
    LIMIT 30
""")).fetchall()
print(f"\nVolland snapshots on Mar 9: {len(rows)}")
for r in rows:
    print(f"  {str(r[0])[11:16]} UTC  paradigm={r[1]}  LIS={r[2]}  charm={r[3]}  spot={r[4]}")

# Check what paradigm our setup_log recorded
print(f"\n--- Paradigm per trade on Mar 9 ---")
trades = c.execute(text("""
    SELECT to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t_et,
           to_char(ts, 'HH24:MI') as t_utc,
           setup_name, direction, paradigm, greek_alignment,
           outcome_result, outcome_pnl
    FROM setup_log
    WHERE ts::date = '2026-03-09'
    ORDER BY ts
""")).fetchall()
for t in trades:
    pnl = float(t[7] or 0)
    print(f"  {t[0]} ET ({t[1]} UTC)  {t[2]:20s} {t[3]:7s} paradigm={t[4]}  align={t[5]}  {str(t[6] or 'OPEN'):8s} {pnl:+.1f}")

# 2. HISTORICAL ANALYSIS: Paradigm as directional gate
print(f"\n{'='*70}")
print("HISTORICAL: PARADIGM DIRECTIONAL GATE")
print("Block shorts during BUY paradigm, block longs during SELL paradigm")
print("=" * 70)

all_trades = c.execute(text("""
    SELECT ts::date as d, setup_name, direction, paradigm,
           outcome_result, outcome_pnl, greek_alignment
    FROM setup_log
    WHERE outcome_result IS NOT NULL
    ORDER BY ts
""")).fetchall()

total_pnl = sum(float(t[5] or 0) for t in all_trades)
total_n = len(all_trades)

print(f"\nBaseline: {total_n} trades, PnL={total_pnl:+.1f}")

# Paradigm contains text like "BUY", "SELL", etc. Let me check unique values
paradigms = set(t[3] for t in all_trades if t[3])
print(f"Unique paradigm values: {paradigms}")

# Test different paradigm gate rules
for rule_name, block_fn in [
    ("Block shorts on BUY paradigm",
     lambda t: t[2] in ('short','bearish') and t[3] and 'BUY' in str(t[3]).upper()),
    ("Block longs on SELL paradigm",
     lambda t: t[2] in ('long','bullish') and t[3] and 'SELL' in str(t[3]).upper()),
    ("BOTH: Block shorts on BUY + longs on SELL",
     lambda t: (t[2] in ('short','bearish') and t[3] and 'BUY' in str(t[3]).upper()) or
               (t[2] in ('long','bullish') and t[3] and 'SELL' in str(t[3]).upper())),
]:
    blocked = [t for t in all_trades if block_fn(t)]
    passed = [t for t in all_trades if not block_fn(t)]
    blocked_pnl = sum(float(t[5] or 0) for t in blocked)
    passed_pnl = sum(float(t[5] or 0) for t in passed)
    blocked_w = sum(1 for t in blocked if t[4] and 'WIN' in t[4])
    blocked_l = sum(1 for t in blocked if t[4] and 'LOSS' in t[4])
    passed_w = sum(1 for t in passed if t[4] and 'WIN' in t[4])
    passed_l = sum(1 for t in passed if t[4] and 'LOSS' in t[4])

    print(f"\n  {rule_name}:")
    print(f"    Passed:  {len(passed)} trades, {passed_w}W/{passed_l}L, PnL={passed_pnl:+.1f}")
    print(f"    Blocked: {len(blocked)} trades, {blocked_w}W/{blocked_l}L, PnL={blocked_pnl:+.1f}")
    print(f"    Improvement: {passed_pnl - total_pnl:+.1f} pts")

    # Per-setup breakdown of blocked
    setup_blocked = defaultdict(lambda: {'w':0,'l':0,'pnl':0,'n':0})
    for t in blocked:
        s = setup_blocked[t[1]]
        s['n'] += 1
        s['pnl'] += float(t[5] or 0)
        if t[4] and 'WIN' in t[4]: s['w'] += 1
        if t[4] and 'LOSS' in t[4]: s['l'] += 1
    for setup, s in sorted(setup_blocked.items(), key=lambda x: x[1]['pnl']):
        wr = s['w']/(s['w']+s['l'])*100 if (s['w']+s['l']) else 0
        print(f"      {setup:20s}: {s['n']} blocked, {s['w']}W/{s['l']}L ({wr:.0f}% WR), PnL={s['pnl']:+.1f}")

# 3. Combined: Paradigm gate + existing Greek filter
print(f"\n{'='*70}")
print("COMBINED: Greek Filter + Paradigm Gate")
print("=" * 70)

def greek_blocks(t):
    """Existing Greek filter rules F1-F5"""
    setup = t[1]
    direction = t[2]
    align = t[6] if t[6] is not None else 0
    is_short = direction in ('short', 'bearish')
    is_long = direction in ('long', 'bullish')
    # We don't have charm/SVB in this query so just check alignment-based rules
    # F2: GEX Long align < 1
    if setup == 'GEX Long' and align < 1: return True
    # F3: AG Short align == -3
    if setup == 'AG Short' and align == -3: return True
    return False

def paradigm_blocks(t):
    """Paradigm directional gate"""
    direction = t[2]
    paradigm = str(t[3] or '').upper()
    is_short = direction in ('short', 'bearish')
    is_long = direction in ('long', 'bullish')
    if is_short and 'BUY' in paradigm: return True
    if is_long and 'SELL' in paradigm: return True
    return False

def combined_blocks(t):
    return greek_blocks(t) or paradigm_blocks(t)

for name, fn in [
    ("Greek filter only (F2+F3)", greek_blocks),
    ("Paradigm gate only", paradigm_blocks),
    ("Greek + Paradigm combined", combined_blocks),
]:
    passed = [t for t in all_trades if not fn(t)]
    blocked = [t for t in all_trades if fn(t)]
    passed_pnl = sum(float(t[5] or 0) for t in passed)
    blocked_pnl = sum(float(t[5] or 0) for t in blocked)
    passed_w = sum(1 for t in passed if t[4] and 'WIN' in t[4])
    passed_l = sum(1 for t in passed if t[4] and 'LOSS' in t[4])
    passed_wr = passed_w/(passed_w+passed_l)*100 if (passed_w+passed_l) else 0

    print(f"\n  {name}:")
    print(f"    Passed: {len(passed)} trades, {passed_w}W/{passed_l}L ({passed_wr:.0f}% WR), PnL={passed_pnl:+.1f}")
    print(f"    Blocked: {len(blocked)} trades, PnL={blocked_pnl:+.1f}")

# 4. What would Mar 9 look like with paradigm gate?
print(f"\n{'='*70}")
print("MAR 9 WITH PARADIGM GATE")
print("=" * 70)
for t in trades:
    paradigm = str(t[4] or '').upper()
    direction = t[3]
    is_short = direction in ('short', 'bearish')
    is_long = direction in ('long', 'bullish')
    blocked = (is_short and 'BUY' in paradigm) or (is_long and 'SELL' in paradigm)
    status = "BLOCK" if blocked else "PASS"
    pnl = float(t[7] or 0)
    print(f"  {t[0]} ET  {t[2]:20s} {t[3]:7s} paradigm={t[4]:30s} {status:5s} {str(t[6] or 'OPEN'):8s} {pnl:+.1f}")

mar9_all = sum(float(t[7] or 0) for t in trades)
mar9_passed = sum(float(t[7] or 0) for t in trades
    if not ((t[3] in ('short','bearish') and 'BUY' in str(t[4] or '').upper()) or
            (t[3] in ('long','bullish') and 'SELL' in str(t[4] or '').upper())))
print(f"\n  Mar 9 baseline: {mar9_all:+.1f} pts")
print(f"  Mar 9 with paradigm gate: {mar9_passed:+.1f} pts")
print(f"  Saved: {mar9_passed - mar9_all:+.1f} pts")

c.close()
