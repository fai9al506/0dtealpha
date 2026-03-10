import os
from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

trailing_setups = ("DD Exhaustion", "GEX Long", "AG Short", "ES Absorption", "Skew Charm")

with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, setup_name, outcome_result, outcome_pnl, outcome_max_profit
        FROM setup_log
        WHERE setup_name IN :setups
          AND outcome_result IS NOT NULL
          AND outcome_pnl IS NOT NULL
          AND outcome_max_profit >= 10
        ORDER BY id ASC
    """), {"setups": trailing_setups}).mappings().all()

    updated = 0
    total_diff = 0.0
    for r in rows:
        old_pnl = float(r["outcome_pnl"])
        new_pnl = round((10.0 + old_pnl) / 2, 1)
        
        # Skip if already correct (within 0.05 tolerance)
        if abs(new_pnl - old_pnl) < 0.05:
            continue
        
        new_result = "WIN" if new_pnl > 0 else ("LOSS" if new_pnl < 0 else "WIN")
        
        conn.execute(text("""
            UPDATE setup_log SET outcome_pnl = :pnl, outcome_result = :res
            WHERE id = :id
        """), {"pnl": new_pnl, "res": new_result, "id": r["id"]})
        
        diff = new_pnl - old_pnl
        total_diff += diff
        updated += 1
        print(f"  #{r['id']:3d} {r['setup_name']:15s} {r['outcome_result']:7s} {old_pnl:+6.1f} -> {new_result:4s} {new_pnl:+6.1f} ({diff:+.1f})")

    print(f"\nUpdated {updated} trades. Total PnL change: {total_diff:+.1f}")

# Verify totals
with engine.begin() as conn:
    totals = conn.execute(text("""
        SELECT setup_name, COUNT(*) as n,
               SUM(CASE WHEN outcome_result='WIN' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN outcome_result='LOSS' THEN 1 ELSE 0 END) as losses,
               SUM(outcome_pnl) as pnl
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        GROUP BY setup_name
        ORDER BY setup_name
    """)).mappings().all()

print(f"\n=== VERIFIED TOTALS ===")
grand = 0
for t in totals:
    pnl = float(t["pnl"] or 0)
    grand += pnl
    wr = int(t["wins"]) / max(int(t["wins"]) + int(t["losses"]), 1) * 100
    print(f"  {t['setup_name']:20s} {t['n']:3d} trades  {t['wins']}W/{t['losses']}L  WR={wr:.0f}%  PnL={pnl:+.1f}")
print(f"  {'GRAND TOTAL':20s} PnL={grand:+.1f}")
