"""
SKEW AS FILTER — Test whether skew compression/expansion improves existing setups.
Apollo's rule: "Skew down gives you strong momentum to the paradigm."
Hypothesis: setups that fire WITH skew confirmation should have higher WR/PnL.
"""
import os, sqlalchemy as sa
from datetime import time as dtime, datetime, timedelta
from collections import defaultdict
import pytz, json

NY = pytz.timezone("US/Eastern")
engine = sa.create_engine(os.environ["DATABASE_URL"])


def compute_skew(chain_rows, spot, put_range=(10, 20), call_range=(10, 20)):
    """Compute put IV / call IV ratio for equidistant strikes."""
    put_ivs, call_ivs = [], []
    for row in chain_rows:
        if len(row) < 21:
            continue
        strike = row[10]
        c_iv = row[2]
        p_iv = row[18]
        dist = strike - spot
        if -put_range[1] <= dist <= -put_range[0] and p_iv and p_iv > 0:
            put_ivs.append(p_iv)
        if call_range[0] <= dist <= call_range[1] and c_iv and c_iv > 0:
            call_ivs.append(c_iv)
    if not put_ivs or not call_ivs:
        return None
    avg_put = sum(put_ivs) / len(put_ivs)
    avg_call = sum(call_ivs) / len(call_ivs)
    if avg_call == 0:
        return None
    return avg_put / avg_call


# ============================================================
# STEP 1: Load all setup trades
# ============================================================
print("Loading setup trades...")
with engine.connect() as c:
    trades = c.execute(sa.text("""
        SELECT id, setup_name, ts::date as trade_date, ts as fired_at,
               direction, spot,
               outcome_result, outcome_pnl, grade,
               outcome_target_level, outcome_stop_level
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        AND outcome_result != 'OPEN'
        ORDER BY ts
    """)).fetchall()
print(f"Loaded {len(trades)} resolved trades.\n")

# ============================================================
# STEP 2: Load chain data by day (cached)
# ============================================================
print("Loading chain snapshots...")
all_chains = {}  # day -> [(ts, spot, rows), ...]

with engine.connect() as c:
    days = c.execute(sa.text(
        "SELECT DISTINCT ts::date FROM chain_snapshots "
        "WHERE ts::date >= '2026-02-01' AND spot IS NOT NULL ORDER BY ts::date"
    )).fetchall()
    days = [d[0] for d in days]

    for day in days:
        chain = c.execute(sa.text(
            "SELECT ts, spot, rows FROM chain_snapshots "
            "WHERE ts::date = :d AND spot IS NOT NULL ORDER BY ts"
        ), {"d": day}).fetchall()
        all_chains[day] = chain

print(f"Loaded chain data for {len(days)} days.\n")

# ============================================================
# STEP 3: For each trade, compute skew + skew change at fire time
# ============================================================
print("Computing skew at each trade...")

LOOKBACKS = [3, 5, 8, 10]

enriched = []
for t in trades:
    tid, setup, trade_date, fired_at, direction, spot, result, pnl, grade, target, stop = t

    if trade_date not in all_chains:
        continue

    chain = all_chains[trade_date]
    if len(chain) < 12:
        continue

    # Find closest chain snapshot to fired_at
    best_idx = None
    best_diff = 999999
    for i, (ts, cs, _) in enumerate(chain):
        diff = abs((ts - fired_at).total_seconds())
        if diff < best_diff:
            best_diff = diff
            best_idx = i

    if best_idx is None or best_diff > 120:  # within 2 min
        continue

    ts_at, spot_at, rows_at = chain[best_idx]
    skew_now = compute_skew(rows_at, spot_at)
    if skew_now is None:
        continue

    # Compute skew change for multiple lookbacks
    skew_changes = {}
    for lb in LOOKBACKS:
        prev_idx = best_idx - lb
        if prev_idx < 0:
            continue
        ts_prev, spot_prev, rows_prev = chain[prev_idx]
        skew_prev = compute_skew(rows_prev, spot_prev)
        if skew_prev is None or skew_prev == 0:
            continue
        skew_chg = (skew_now - skew_prev) / skew_prev
        skew_changes[lb] = skew_chg

    if not skew_changes:
        continue

    enriched.append({
        "id": tid,
        "setup": setup,
        "date": str(trade_date),
        "time": fired_at.astimezone(NY).strftime("%H:%M"),
        "direction": direction,
        "spot": round(spot, 1) if spot else None,
        "result": result,
        "pnl": float(pnl) if pnl else 0,
        "grade": grade,
        "skew": round(skew_now, 4),
        "skew_changes": {k: round(v * 100, 2) for k, v in skew_changes.items()},
    })

print(f"Enriched {len(enriched)} trades with skew data.\n")


# ============================================================
# HELPER: Stats printer
# ============================================================
def stats(label, trades_list):
    if not trades_list:
        print(f"  {label}: 0 trades")
        return
    wins = [t for t in trades_list if t["result"] == "WIN"]
    losses = [t for t in trades_list if t["result"] == "LOSS"]
    expired = [t for t in trades_list if t["result"] == "EXPIRED"]
    total_pnl = sum(t["pnl"] for t in trades_list)
    wr = len(wins) / len(trades_list) * 100
    print(f"  {label}: {len(trades_list)} trades | {len(wins)}W/{len(losses)}L/{len(expired)}E | "
          f"WR={wr:.0f}% | P&L={total_pnl:+.1f} | Avg={total_pnl/len(trades_list):+.1f}/trade")


# ============================================================
# ANALYSIS 1: Skew confirmation vs opposition (per setup)
# ============================================================
print("=" * 80)
print("ANALYSIS 1: SKEW CONFIRMATION vs OPPOSITION")
print("Apollo's rule: skew compressing = momentum to paradigm")
print("=" * 80)

LB = 5  # primary lookback

for setup in ["GEX Long", "AG Short", "BofA Scalp", "ES Absorption", "DD Exhaustion", "Paradigm Reversal"]:
    st = [t for t in enriched if t["setup"] == setup and LB in t["skew_changes"]]
    if not st:
        continue

    print(f"\n--- {setup} ({len(st)} trades with skew data) ---")

    # "Confirmed" = skew moving in the direction that supports the trade
    # LONG + skew dropping (fear unwinding) = confirmed
    # SHORT + skew rising (fear building) = confirmed
    confirmed = []
    opposed = []
    neutral = []

    for t in st:
        chg = t["skew_changes"][LB]
        if t["direction"] == "LONG":
            if chg < -3:  # skew dropped >3% = fear unwinding = supports long
                confirmed.append(t)
            elif chg > 3:  # skew rising = fear building = opposes long
                opposed.append(t)
            else:
                neutral.append(t)
        elif t["direction"] == "SHORT":
            if chg > 3:  # skew rising = fear building = supports short
                confirmed.append(t)
            elif chg < -3:  # skew dropping = fear unwinding = opposes short
                opposed.append(t)
            else:
                neutral.append(t)

    stats("ALL", st)
    stats("Skew CONFIRMED (>3%)", confirmed)
    stats("Skew OPPOSED (>3%)", opposed)
    stats("Skew NEUTRAL (<3%)", neutral)


# ============================================================
# ANALYSIS 2: Threshold sweep for skew filter
# ============================================================
print("\n" + "=" * 80)
print("ANALYSIS 2: SKEW FILTER THRESHOLD SWEEP")
print("Block trades where skew opposes direction by more than X%")
print("=" * 80)

for lb in [3, 5, 8]:
    print(f"\n--- Lookback = {lb} snapshots ---")
    for thresh in [2, 3, 5, 8, 10]:
        # Filter: block if skew opposes by > thresh%
        passed = []
        blocked = []
        for t in enriched:
            if lb not in t["skew_changes"]:
                continue
            chg = t["skew_changes"][lb]
            oppose = False
            if t["direction"] == "LONG" and chg > thresh:
                oppose = True
            elif t["direction"] == "SHORT" and chg < -thresh:
                oppose = True
            if oppose:
                blocked.append(t)
            else:
                passed.append(t)

        blocked_pnl = sum(t["pnl"] for t in blocked)
        stats(f"PASS (oppose<{thresh}%)", passed)
        if blocked:
            bw = len([t for t in blocked if t["result"] == "WIN"])
            bl = len([t for t in blocked if t["result"] == "LOSS"])
            print(f"    ^ Blocked {len(blocked)} trades ({bw}W/{bl}L, "
                  f"P&L={blocked_pnl:+.1f}) — net savings={-blocked_pnl:+.1f}")


# ============================================================
# ANALYSIS 3: Per-setup skew filter impact
# ============================================================
print("\n" + "=" * 80)
print("ANALYSIS 3: PER-SETUP SKEW FILTER (lb=5, oppose>5%)")
print("=" * 80)

for setup in ["GEX Long", "AG Short", "BofA Scalp", "ES Absorption", "DD Exhaustion", "Paradigm Reversal"]:
    st = [t for t in enriched if t["setup"] == setup and LB in t["skew_changes"]]
    if not st:
        continue

    passed = []
    blocked = []
    for t in st:
        chg = t["skew_changes"][LB]
        oppose = False
        if t["direction"] == "LONG" and chg > 5:
            oppose = True
        elif t["direction"] == "SHORT" and chg < -5:
            oppose = True
        if oppose:
            blocked.append(t)
        else:
            passed.append(t)

    print(f"\n--- {setup} ---")
    stats("Unfiltered", st)
    stats("After skew filter", passed)
    if blocked:
        bw = len([t for t in blocked if t["result"] == "WIN"])
        bl = len([t for t in blocked if t["result"] == "LOSS"])
        bp = sum(t["pnl"] for t in blocked)
        print(f"  Blocked: {len(blocked)} ({bw}W/{bl}L, P&L={bp:+.1f})")
        for t in blocked:
            print(f"    #{t['id']} {t['date']} {t['time']} {t['direction']} "
                  f"skew_chg={t['skew_changes'][LB]:+.1f}% → {t['result']} {t['pnl']:+.1f}")


# ============================================================
# ANALYSIS 4: Skew level (absolute) as filter
# ============================================================
print("\n" + "=" * 80)
print("ANALYSIS 4: ABSOLUTE SKEW LEVEL")
print("High skew (>1.20) vs low skew (<1.10) vs normal")
print("=" * 80)

for setup in ["GEX Long", "AG Short", "BofA Scalp", "ES Absorption", "DD Exhaustion", "Paradigm Reversal"]:
    st = [t for t in enriched if t["setup"] == setup]
    if not st:
        continue

    low = [t for t in st if t["skew"] < 1.10]
    mid = [t for t in st if 1.10 <= t["skew"] <= 1.20]
    high = [t for t in st if t["skew"] > 1.20]

    print(f"\n--- {setup} ---")
    stats("Skew < 1.10 (low fear)", low)
    stats("Skew 1.10-1.20 (normal)", mid)
    stats("Skew > 1.20 (elevated)", high)


# ============================================================
# ANALYSIS 5: Combined — skew confirming + charm aligned
# ============================================================
print("\n" + "=" * 80)
print("ANALYSIS 5: BEST COMBO FILTER (skew confirmed + not opposed)")
print("Test: block when skew opposes by >5% AND skew > 1.15 (elevated)")
print("=" * 80)

for setup in ["GEX Long", "AG Short", "BofA Scalp", "ES Absorption", "DD Exhaustion", "Paradigm Reversal"]:
    st = [t for t in enriched if t["setup"] == setup and LB in t["skew_changes"]]
    if not st:
        continue

    passed = []
    blocked = []
    for t in st:
        chg = t["skew_changes"][LB]
        block = False
        # Block if: skew elevated AND moving against trade
        if t["skew"] > 1.15:
            if t["direction"] == "LONG" and chg > 3:
                block = True
            elif t["direction"] == "SHORT" and chg < -3:
                block = True
        if block:
            blocked.append(t)
        else:
            passed.append(t)

    print(f"\n--- {setup} ---")
    stats("Unfiltered", st)
    stats("After combo filter", passed)
    if blocked:
        bw = len([t for t in blocked if t["result"] == "WIN"])
        bl = len([t for t in blocked if t["result"] == "LOSS"])
        bp = sum(t["pnl"] for t in blocked)
        print(f"  Blocked: {len(blocked)} ({bw}W/{bl}L, P&L={bp:+.1f})")


# ============================================================
# GRAND SUMMARY
# ============================================================
print("\n" + "=" * 80)
print("GRAND SUMMARY: Total portfolio impact of skew filter")
print("=" * 80)

# Test multiple filter configs
for label, lb, thresh in [
    ("lb=5, oppose>3%", 5, 3),
    ("lb=5, oppose>5%", 5, 5),
    ("lb=5, oppose>8%", 5, 8),
    ("lb=3, oppose>5%", 3, 5),
    ("lb=8, oppose>5%", 8, 5),
]:
    passed = []
    blocked = []
    for t in enriched:
        if lb not in t["skew_changes"]:
            passed.append(t)  # no data = pass
            continue
        chg = t["skew_changes"][lb]
        oppose = False
        if t["direction"] == "LONG" and chg > thresh:
            oppose = True
        elif t["direction"] == "SHORT" and chg < -thresh:
            oppose = True
        if oppose:
            blocked.append(t)
        else:
            passed.append(t)

    all_pnl = sum(t["pnl"] for t in enriched)
    pass_pnl = sum(t["pnl"] for t in passed)
    block_pnl = sum(t["pnl"] for t in blocked)
    bw = len([t for t in blocked if t["result"] == "WIN"])
    bl = len([t for t in blocked if t["result"] == "LOSS"])

    print(f"\n  Filter: {label}")
    print(f"    Baseline:  {len(enriched)} trades, P&L={all_pnl:+.1f}")
    print(f"    Filtered:  {len(passed)} trades, P&L={pass_pnl:+.1f}")
    print(f"    Blocked:   {len(blocked)} trades ({bw}W/{bl}L), P&L={block_pnl:+.1f}")
    print(f"    Net gain:  {pass_pnl - all_pnl:+.1f} pts")


print("\n\nDONE.")
