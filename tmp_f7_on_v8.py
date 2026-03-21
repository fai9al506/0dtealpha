"""
F7 Charm Support Gate — Incremental value on top of V8 filter.

Question: Of the shorts that PASS V8, how many would F7 block, and what's their WR?
"""

import os, sys
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL)

# ── V8 short rules ──
# Allowed shorts: Skew Charm (all), AG Short (all), DD Exhaustion (align != 0)
# Blocked shorts: ES Absorption, BofA Scalp, Paradigm Rev, DD align=0

def passes_v8_short(setup_name, alignment):
    """Returns True if this short trade passes V8 filter."""
    if setup_name == "Skew Charm":
        return True
    if setup_name == "AG Short":
        return True
    if setup_name == "DD Exhaustion":
        return alignment != 0
    # All other shorts blocked
    return False

def passes_v8_long(alignment, vix=None, overvix=None):
    """Returns True if this long trade passes V8 filter."""
    if alignment is None:
        return False
    if alignment < 2:
        return False
    # VIX gate: allow if VIX <= 26 OR overvix >= +2
    if vix is not None and vix <= 26:
        return True
    if overvix is not None and overvix >= 2:
        return True
    # If we don't have VIX data, assume passes (pre-V8 data)
    if vix is None:
        return True
    return False

with engine.connect() as conn:
    # Pull all resolved trades with alignment
    rows = conn.execute(text("""
        SELECT id, setup_name, direction, spot, greek_alignment as alignment,
               outcome_result, outcome_pnl, ts,
               EXTRACT(EPOCH FROM ts) as ts_epoch
        FROM setup_log
        WHERE outcome_result IN ('WIN', 'LOSS')
          AND direction IN ('short', 'bearish')
        ORDER BY ts
    """)).fetchall()

    print(f"Total SHORT trades with outcomes: {len(rows)}")

    # Filter to V8-passing shorts
    v8_shorts = []
    for r in rows:
        align = r.alignment if r.alignment is not None else 0
        if passes_v8_short(r.setup_name, align):
            v8_shorts.append(r)

    print(f"V8-passing shorts: {len(v8_shorts)}")
    print()

    # For each V8-passing short, get charm sum below spot
    f7_results = []
    no_charm_data = 0

    for r in v8_shorts:
        spot = float(r.spot) if r.spot else None
        if not spot:
            no_charm_data += 1
            continue

        # Find closest charm snapshot before this trade
        charm_row = conn.execute(text("""
            SELECT SUM(value) as charm_sum, COUNT(*) as n_strikes
            FROM volland_exposure_points
            WHERE greek = 'charm'
              AND ts_utc <= :ts
              AND ts_utc >= :ts - INTERVAL '5 minutes'
              AND strike >= :spot_low
              AND strike <= :spot_high
        """), {
            "ts": r.ts,
            "spot_low": spot - 20,
            "spot_high": spot,
        }).fetchone()

        charm_sum = float(charm_row.charm_sum) if charm_row and charm_row.charm_sum else None

        if charm_sum is None:
            # Try wider window (10 min)
            charm_row2 = conn.execute(text("""
                SELECT SUM(value) as charm_sum, COUNT(*) as n_strikes
                FROM volland_exposure_points
                WHERE greek = 'charm'
                  AND ts_utc <= :ts
                  AND ts_utc >= :ts - INTERVAL '10 minutes'
                  AND strike >= :spot_low
                  AND strike <= :spot_high
            """), {
                "ts": r.ts,
                "spot_low": spot - 20,
                "spot_high": spot,
            }).fetchone()
            charm_sum = float(charm_row2.charm_sum) if charm_row2 and charm_row2.charm_sum else None

        if charm_sum is None:
            no_charm_data += 1
            continue

        f7_results.append({
            "id": r.id,
            "setup": r.setup_name,
            "direction": r.direction,
            "spot": spot,
            "alignment": r.alignment,
            "outcome": r.outcome_result,
            "pnl": float(r.outcome_pnl) if r.outcome_pnl else 0,
            "charm_sum": charm_sum,
            "ts": r.ts,
        })

    print(f"V8 shorts with charm data: {len(f7_results)}")
    print(f"No charm data: {no_charm_data}")
    print()

    # ── Analyze F7 thresholds ──
    thresholds = [-50_000_000, -100_000_000, -200_000_000, -500_000_000]

    print("=" * 90)
    print(f"{'Threshold':>12} | {'Passed':>6} {'WR':>6} {'PnL':>8} | {'Blocked':>7} {'WR':>6} {'PnL':>8} | {'Delta':>8}")
    print("=" * 90)

    for thresh in thresholds:
        passed = [t for t in f7_results if t["charm_sum"] >= thresh]
        blocked = [t for t in f7_results if t["charm_sum"] < thresh]

        p_wins = sum(1 for t in passed if t["outcome"] == "WIN")
        p_wr = p_wins / len(passed) * 100 if passed else 0
        p_pnl = sum(t["pnl"] for t in passed)

        b_wins = sum(1 for t in blocked if t["outcome"] == "WIN")
        b_wr = b_wins / len(blocked) * 100 if blocked else 0
        b_pnl = sum(t["pnl"] for t in blocked)

        # Delta = improvement from blocking (we save the blocked losses)
        delta = -b_pnl  # removing negative PnL = positive improvement

        label = f"<{thresh/1e6:.0f}M"
        print(f"{label:>12} | {len(passed):>6} {p_wr:>5.1f}% {p_pnl:>+8.1f} | {len(blocked):>7} {b_wr:>5.1f}% {b_pnl:>+8.1f} | {delta:>+8.1f}")

    print()

    # ── Deep dive at -100M (original best threshold) ──
    thresh = -100_000_000
    blocked = [t for t in f7_results if t["charm_sum"] < thresh]
    passed = [t for t in f7_results if t["charm_sum"] >= thresh]

    print(f"\n{'='*70}")
    print(f"F7 at -100M threshold — detailed breakdown")
    print(f"{'='*70}")

    # By setup
    print(f"\n--- Blocked shorts by setup ---")
    setups = set(t["setup"] for t in blocked)
    for s in sorted(setups):
        trades = [t for t in blocked if t["setup"] == s]
        wins = sum(1 for t in trades if t["outcome"] == "WIN")
        wr = wins / len(trades) * 100 if trades else 0
        pnl = sum(t["pnl"] for t in trades)
        print(f"  {s:20s}: {len(trades):>3} trades, {wr:>5.1f}% WR, {pnl:>+8.1f} pts")

    print(f"\n--- Passed shorts by setup ---")
    setups = set(t["setup"] for t in passed)
    for s in sorted(setups):
        trades = [t for t in passed if t["setup"] == s]
        wins = sum(1 for t in trades if t["outcome"] == "WIN")
        wr = wins / len(trades) * 100 if trades else 0
        pnl = sum(t["pnl"] for t in trades)
        print(f"  {s:20s}: {len(trades):>3} trades, {wr:>5.1f}% WR, {pnl:>+8.1f} pts")

    # By time of day
    print(f"\n--- Blocked shorts by hour ---")
    from collections import defaultdict
    hour_data = defaultdict(lambda: {"n": 0, "wins": 0, "pnl": 0})
    for t in blocked:
        # Convert to ET (ts is already in ET from setup_log)
        h = t["ts"].hour if hasattr(t["ts"], "hour") else 0
        hour_data[h]["n"] += 1
        hour_data[h]["wins"] += 1 if t["outcome"] == "WIN" else 0
        hour_data[h]["pnl"] += t["pnl"]

    for h in sorted(hour_data.keys()):
        d = hour_data[h]
        wr = d["wins"] / d["n"] * 100 if d["n"] else 0
        print(f"  {h:02d}:00 — {d['n']:>3} trades, {wr:>5.1f}% WR, {d['pnl']:>+8.1f} pts")

    # Show individual blocked trades
    print(f"\n--- All blocked trades (charm_sum < -100M) ---")
    for t in sorted(blocked, key=lambda x: x["ts"]):
        print(f"  {t['ts'].strftime('%m/%d %H:%M')} | {t['setup']:20s} | align={t['alignment']:>2} | charm={t['charm_sum']/1e6:>+8.1f}M | {t['outcome']:4s} {t['pnl']:>+6.1f}")

    # ── Summary ──
    print(f"\n{'='*70}")
    print(f"SUMMARY: F7 (-100M) on top of V8")
    print(f"{'='*70}")

    total_v8 = len(f7_results)
    total_v8_wins = sum(1 for t in f7_results if t["outcome"] == "WIN")
    total_v8_pnl = sum(t["pnl"] for t in f7_results)

    v8f7_trades = passed
    v8f7_wins = sum(1 for t in v8f7_trades if t["outcome"] == "WIN")
    v8f7_pnl = sum(t["pnl"] for t in v8f7_trades)

    print(f"  V8 shorts:      {total_v8:>4} trades, {total_v8_wins/total_v8*100 if total_v8 else 0:>5.1f}% WR, {total_v8_pnl:>+8.1f} pts")
    print(f"  V8+F7 shorts:   {len(v8f7_trades):>4} trades, {v8f7_wins/len(v8f7_trades)*100 if v8f7_trades else 0:>5.1f}% WR, {v8f7_pnl:>+8.1f} pts")
    print(f"  F7 blocked:     {len(blocked):>4} trades, {sum(1 for t in blocked if t['outcome']=='WIN')/len(blocked)*100 if blocked else 0:>5.1f}% WR, {sum(t['pnl'] for t in blocked):>+8.1f} pts")
    print(f"  Improvement:    {v8f7_pnl - total_v8_pnl:>+8.1f} pts (from removing blocked losers)")
