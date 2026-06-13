"""Recompute May projection using V16 filter (not V14).

V16 = V14 base whitelist + 4 new block rules + DD Exhaustion long admit:
  R10: ES Absorption bearish blocked when hour >= 14 ET
  R5:  Skew Charm long blocked when paradigm = GEX-LIS
  R2:  Skew Charm long blocked on monthly OpEx Friday (3rd Friday)
  R12: AG Short blocked on monthly OpEx Friday
  DD long admit: V14 blocked DD longs at align gate; V16 admits them (V14 quality gates still apply)

Also compute the THREE relevant universes:
  A. All V14-eligible notified signals (what I had — overstated)
  B. V16-filtered signals (more accurate)
  C. Actual real_trade_orders placed (truest)
"""
import os
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()


def is_monthly_opex(d):
    """3rd Friday of the month."""
    # Find first Friday
    first = d.replace(day=1)
    days_to_fri = (4 - first.weekday()) % 7
    first_fri = first.day + days_to_fri
    third_fri = first_fri + 14
    return d.day == third_fri and d.weekday() == 4


def passes_v16(name, dir_, paradigm, grade, align, ts):
    """Approx V16 filter (mirrors _passes_live_filter for V16 whitelist setups)."""
    is_long = dir_ in ("long", "bullish")

    if name not in ("Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion"):
        return False

    # DD shorts blocked (S164 tonight + main.py dispatch)
    if name == "DD Exhaustion" and not is_long:
        return False

    # R5: SC long GEX-LIS blocked
    if name == "Skew Charm" and is_long and paradigm == "GEX-LIS":
        return False

    # R2: SC long OpEx Friday blocked
    if name == "Skew Charm" and is_long and is_monthly_opex(ts.date()):
        return False

    # R12: AG Short OpEx Friday blocked
    if name == "AG Short" and is_monthly_opex(ts.date()):
        return False

    # R10: ES Abs bearish hour >= 14 ET blocked
    if name == "ES Absorption" and not is_long:
        ts_et = ts.astimezone(ET)
        if ts_et.hour >= 14:
            return False

    # SC grade C/LOG blocked (V14)
    if name == "Skew Charm" and grade in ("C", "LOG"):
        return False

    # ES Abs PURE rules (V14)
    if name == "ES Absorption":
        if grade not in ("A", "A+"):
            return False
        if paradigm in ("AG-TARGET", "AG-LIS"):
            return False
        a = align or 0
        if is_long and a < 0:
            return False
        if not is_long and a > 0:
            return False

    # SIDIAL-EXTREME longs blocked (V12-fix)
    if is_long and paradigm == "SIDIAL-EXTREME":
        return False

    # DD long V14 quality gates
    if name == "DD Exhaustion" and is_long:
        a = align or 0
        if a >= 3 or a < 0:  # V16.1: align in {0,1,2} only
            return False
        if paradigm in ("GEX-LIS","AG-LIS","AG-PURE","BofA-LIS","BOFA-MESSY"):
            return False
        if grade == "C":
            return False

    # Generic V14 longs alignment gate (non-SC): >=2
    if is_long and name not in ("Skew Charm", "DD Exhaustion"):
        a = align or 0
        if a < 2:
            return False

    return True


cur.execute("""
    SELECT id, ts, setup_name, direction, paradigm, grade, greek_alignment, outcome_pnl
    FROM setup_log
    WHERE ts::date >= '2026-05-01' AND ts::date <= '2026-05-20'
      AND setup_name IN ('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion')
      AND notified = true AND outcome_pnl IS NOT NULL
""")
rows = cur.fetchall()

v14_n = len(rows)
v14_pts = sum(float(r[7]) for r in rows)

v16_signals = [r for r in rows if passes_v16(r[2], r[3], r[4], r[5], r[6], r[1])]
v16_n = len(v16_signals)
v16_pts = sum(float(r[7]) for r in v16_signals)

# Actual placed (from real_trade_orders, May 1-20)
cur.execute("""
    SELECT COUNT(*), COALESCE(SUM(sl.outcome_pnl), 0)
    FROM real_trade_orders rto
    JOIN setup_log sl ON sl.id = rto.setup_log_id
    WHERE rto.created_at >= '2026-05-01' AND rto.created_at < '2026-05-21'
""")
actual_n, actual_pts = cur.fetchone()
actual_pts = float(actual_pts)

print("=" * 70)
print("May 1-20 PROJECTION BASIS — three universes compared")
print("=" * 70)
print(f"{'Universe':<55} {'Trades':>8} {'Portal pts':>12} {'@$5 MES':>10}")
print(f"{'A. V14 whitelist notified (what I used — OVERSTATED)':<55} {v14_n:>8} {v14_pts:>12.1f} {v14_pts*5:>10.0f}")
print(f"{'B. V16 filter passed (current live)':<55} {v16_n:>8} {v16_pts:>12.1f} {v16_pts*5:>10.0f}")
print(f"{'C. Actual real_trade_orders placed':<55} {actual_n:>8} {actual_pts:>12.1f} {actual_pts*5:>10.0f}")

print()
print("=" * 70)
print("PROJECTION RECOMPUTE — using V16 (universe B) base")
print("=" * 70)
v16_daily = v16_pts * 5 / 14  # 14 trading days in May 1-20 window
v16_monthly = v16_daily * 20   # 20 trading days/mo

print(f"V16 portal sim May 1-20:          {v16_pts:.0f} pts = ${v16_pts*5:.0f} over 14 trading days")
print(f"V16 portal sim per trading day:   ${v16_daily:.2f}")
print(f"V16 portal sim per month (20td):  ${v16_monthly:.2f}")
print()
print(f"At various capture rates per month:")
for cap in [40, 60, 75, 85]:
    monthly = v16_monthly * cap / 100
    sar = monthly * 3.75
    salary_pct = sar / 18500 * 100
    print(f"  {cap:>3}% capture: ${monthly:>7.0f}/mo  |  SAR {sar:>7,.0f}/mo  |  {salary_pct:>5.1f}% of 18,500 SAR salary")

print()
print(f"At 1 ES (10x, with 92% scale haircut):")
for cap in [60, 75]:
    monthly_1es = v16_monthly * cap/100 * 10 * 0.92
    sar = monthly_1es * 3.75
    salary_mult = sar / 18500
    print(f"  {cap:>3}% capture × 10× × 92%: ${monthly_1es:>7.0f}/mo  |  SAR {sar:>7,.0f}/mo  |  {salary_mult:.1f}× salary")

print()
print("=" * 70)
print("HOW MUCH OF V16 SIGNALS ACTUALLY BECOME REAL TRADES?")
print("=" * 70)
print(f"V16 portal signals (universe B): {v16_n}")
print(f"Actual placed (universe C):       {actual_n}")
print(f"Placement rate: {actual_n/v16_n*100:.0f}% of V16-eligible become real-money trades")
print(f"  Why not 100%? Real-trader cap (2-3 concurrent), dispatch race, breaker bug, etc.")
print(f"  After tonight's S161 fix, this ratio should improve.")

cur.close(); c.close()
