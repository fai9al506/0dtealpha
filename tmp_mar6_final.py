"""Final data gathering for March 6 analysis."""
import psycopg2, os, json

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

# Paradigm from volland_snapshots (payload JSONB)
print("=== PARADIGM SHIFTS (March 6) ===", flush=True)
cur.execute("""
SELECT ts AT TIME ZONE 'EST' as ts_et,
       payload->>'paradigm' as paradigm,
       payload->>'lis' as lis,
       payload->>'aggregatedCharm' as charm
FROM volland_snapshots
WHERE ts::date = '2026-03-06'
  AND payload->>'paradigm' IS NOT NULL
ORDER BY ts
""")
rows = cur.fetchall()
prev_paradigm = None
for r in rows:
    if r[1] != prev_paradigm:
        print(f"  {str(r[0])[:16]}  {r[1]:20}  LIS: {str(r[2]):>8}  Charm: {r[3]}", flush=True)
        prev_paradigm = r[1]
print(f"Total volland snapshots: {len(rows)}", flush=True)

# Options trades with full state
print("\n=== OPTIONS TRADE DETAILS (March 6) ===", flush=True)
cur.execute("""
SELECT setup_log_id, state
FROM options_trade_orders
WHERE setup_log_id >= 540
ORDER BY setup_log_id
""")
rows = cur.fetchall()
for r in rows:
    s = r[1]
    # Show all keys
    print(f"  #{r[0]}: {json.dumps({k: v for k, v in s.items() if v is not None}, default=str)}", flush=True)

# SPX price timeline
print("\n=== SPX PRICE TIMELINE (March 6) ===", flush=True)
cur.execute("""
SELECT ts AT TIME ZONE 'EST' as ts_et, round(spot::numeric, 1) as spot,
       setup_name, direction, outcome_result, round(outcome_pnl::numeric,1) as pnl,
       greek_alignment
FROM setup_log
WHERE ts::date = '2026-03-06'
ORDER BY ts
""")
for r in cur.fetchall():
    filtered = ""
    ga = r[6]
    name = r[2]
    direction = r[3]
    # Check if Greek filter would block
    if name == 'Skew Charm' and direction == 'short' and ga and ga > 0:
        filtered = " [CHARM-BLOCK]"
    elif name == 'AG Short' and ga == -3:
        filtered = " [ALIGN-BLOCK]"
    elif name == 'DD Exhaustion' and direction == 'short' and ga and ga > 0:
        filtered = " [CHARM-BLOCK]"
    print(f"  {str(r[0])[:16]}  SPX={r[1]}  {r[2]:16}  {r[3]:8}  {r[4] or 'PENDING':7}  {r[5] or 0:+6.1f}  ga={ga}{filtered}", flush=True)

# Which trades would the auto-trader have executed? (non-LOG, passes Greek filter)
print("\n=== AUTO-TRADER ELIGIBLE TRADES (March 6) ===", flush=True)
# The auto-trader uses: charm alignment gate (F1), GEX Long align>=+1 (F2), AG Short align!=-3 (F3), DD SVB block (F4)
cur.execute("""
SELECT id, setup_name, direction, outcome_result, round(outcome_pnl::numeric,1) as pnl,
       greek_alignment, spot_vol_beta, paradigm, grade
FROM setup_log
WHERE ts::date = '2026-03-06' AND grade != 'LOG' AND outcome_result IS NOT NULL
ORDER BY ts
""")
eligible_pnl = 0
eligible_count = 0
blocked_pnl = 0
blocked_count = 0
for r in cur.fetchall():
    sid, name, direction, outcome, pnl, ga, svb, paradigm, grade = r
    pnl = float(pnl) if pnl else 0
    ga = int(ga) if ga else 0
    svb = float(svb) if svb else None

    blocked = False
    reason = ""

    # F1: Charm alignment gate
    if name in ('Skew Charm', 'DD Exhaustion', 'AG Short', 'ES Absorption', 'BofA Scalp', 'GEX Long'):
        if direction in ('short', 'bearish') and ga > 0:
            blocked = True
            reason = "charm_oppose"
        elif direction in ('long', 'bullish') and ga < 0:
            blocked = True
            reason = "charm_oppose"

    # F3: AG Short align == -3
    if name == 'AG Short' and ga == -3:
        blocked = True
        reason = "ag_align_-3"

    # F4: DD SVB weak-negative
    if name == 'DD Exhaustion' and svb is not None and -0.5 <= svb <= 0:
        blocked = True
        reason = "dd_svb_weak"

    # DD after 14:00 block
    # DD BOFA-PURE block
    if name == 'DD Exhaustion' and paradigm == 'BOFA-PURE':
        blocked = True
        reason = "dd_bofa_pure"

    status = "BLOCKED" if blocked else "PASS"
    if blocked:
        blocked_pnl += pnl
        blocked_count += 1
    else:
        eligible_pnl += pnl
        eligible_count += 1

    print(f"  #{sid} {name:16} {direction:8} {outcome:7} {pnl:+6.1f} ga={ga} {status:7} {reason}", flush=True)

print(f"\nEligible: {eligible_count} trades, {eligible_pnl:+.1f} pts", flush=True)
print(f"Blocked:  {blocked_count} trades, {blocked_pnl:+.1f} pts", flush=True)

conn.close()
