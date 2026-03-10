"""Check the specific trade with max_profit 12.1 but 10pt miss."""
import os, json
from sqlalchemy import create_engine, text
engine = create_engine(os.environ['DATABASE_URL'])
conn = engine.connect()

# Find the trade - entry 6877.11 on Mar 4
rows = conn.execute(text("""
    SELECT id, setup_name, direction, grade, score, spot, target, lis,
           abs_es_price, max_plus_gex, max_minus_gex,
           bofa_stop_level, bofa_target_level,
           outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
           outcome_first_event, outcome_elapsed_min, ts
    FROM setup_log
    WHERE ts::date = '2026-03-04'
    ORDER BY ts
""")).fetchall()

for r in rows:
    rid, nm, dr, gr, sc, sp, tgt, lis = r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7]
    abs_es, mpg, mmg = r[8], r[9], r[10]
    bsl, btl = r[11], r[12]
    res, pnl, mp, ml, fe, em, ts = r[13], r[14], r[15], r[16], r[17], r[18], r[19]

    # Focus on the one with entry ~6877 and max_profit ~12
    entry = abs_es or sp
    if entry and abs(entry - 6877.11) < 1:
        print(f"=== FOUND: #{rid} ===")
        print(f"Setup: {nm}, Dir: {dr}, Grade: {gr}, Score: {sc}")
        print(f"Spot (SPX): {sp}")
        print(f"abs_es_price (ES): {abs_es}")
        print(f"Target: {tgt}")
        print(f"LIS: {lis}")
        print(f"max_plus_gex: {mpg}, max_minus_gex: {mmg}")
        print(f"bofa_stop_level: {bsl}, bofa_target_level: {btl}")
        print(f"Outcome: {res}, PnL: {pnl}, MaxP: {mp}, MaxL: {ml}")
        print(f"First event: {fe}, Elapsed: {em}")
        print(f"Time: {ts}")
        print()

        # What SHOULD the 10pt level be?
        if dr in ('short', 'bearish'):
            ten_pt = round(entry - 10, 2) if entry else None
            stop = round(entry + 12, 2) if entry else None
        else:
            ten_pt = round(entry + 10, 2) if entry else None
            stop = round(entry - 12, 2) if entry else None
        print(f"Expected 10pt level: {ten_pt}")
        print(f"Expected stop level: {stop}")
        print(f"Dashboard shows 10pt=6857, which is {entry - 6857:.1f} pts from entry")

# Also show all today's trades with their key levels
print()
print("=== ALL TODAY'S TRADES ===")
print(f"{'ID':<5} {'Setup':<16} {'Dir':<8} {'Entry':<10} {'Spot':<10} {'Target':<10} {'MaxP':<6} {'FE':<8}")
for r in rows:
    rid, nm, dr = r[0], r[1], r[2]
    sp, tgt, abs_es = r[5], r[6], r[8]
    mp, fe = r[15], r[17]
    entry = abs_es or sp
    print(f"{rid:<5} {nm:<16} {dr:<8} {entry or '--':<10} {sp or '--':<10} {tgt or '--':<10} {mp or '--':<6} {fe or '--':<8}")

conn.close()
