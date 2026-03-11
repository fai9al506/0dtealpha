"""Pin Regime Detection Backtest v2 - Batch optimized

Pin = Greek structure boxes price:
1. Spot near max +GEX strike (within 10 pts)
2. Negative vanna above spot = resistance
3. Positive vanna below spot = support
4. Positive charm above spot = resistance
5. Negative charm below spot = support
"""
import os, sys
from collections import defaultdict
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# 1. Load all trades
trades = c.execute(text("""
    SELECT id, ts, to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t,
           ts::date as trade_date, setup_name, direction, grade, spot,
           outcome_result, outcome_pnl, greek_alignment
    FROM setup_log
    WHERE grade != 'LOG' AND outcome_result IS NOT NULL AND spot IS NOT NULL
    ORDER BY ts
""")).fetchall()
print("Loaded %d trades" % len(trades))

# 2. For each trade, get pin score using a single query per trade
#    Combine gamma/vanna/charm in one query
def get_pin_data(conn, ts, spot):
    """Single query to get all exposure data near trade time"""
    rows = conn.execute(text("""
        SELECT greek, expiration_option, strike, value
        FROM volland_exposure_points
        WHERE ts_utc BETWEEN :ts - INTERVAL '3 minutes' AND :ts + INTERVAL '3 minutes'
          AND ABS(strike - :spot) <= 50
          AND (
              (greek = 'gamma' AND expiration_option = 'TODAY')
              OR (greek = 'vanna' AND expiration_option = 'ALL')
              OR (greek = 'charm' AND expiration_option IS NULL)
          )
    """), {"ts": ts, "spot": float(spot)}).fetchall()
    return rows

def calc_pin_score(rows, spot):
    spot_f = float(spot)
    gamma_by_strike = {}
    vanna_above = vanna_below = 0.0
    charm_above = charm_below = 0.0

    for greek, exp, strike, value in rows:
        s = float(strike)
        v = float(value) if value else 0
        if greek == 'gamma' and exp == 'TODAY':
            gamma_by_strike[s] = v
        elif greek == 'vanna':
            if s > spot_f: vanna_above += v
            elif s < spot_f: vanna_below += v
        elif greek == 'charm':
            if s > spot_f: charm_above += v
            elif s < spot_f: charm_below += v

    score = 0
    max_gex_strike = None
    gex_dist = None

    if gamma_by_strike:
        max_gex_strike = max(gamma_by_strike, key=gamma_by_strike.get)
        max_gex_val = gamma_by_strike[max_gex_strike]
        gex_dist = abs(spot_f - max_gex_strike)
        if gex_dist <= 10 and max_gex_val > 0:
            score += 1

    if vanna_above < 0: score += 1  # negative vanna above = resistance
    if vanna_below > 0: score += 1  # positive vanna below = support
    if charm_above > 0: score += 1  # positive charm above = resistance
    if charm_below < 0: score += 1  # negative charm below = support

    return score, {
        "gex_strike": max_gex_strike, "gex_dist": gex_dist,
        "v_above": vanna_above, "v_below": vanna_below,
        "c_above": charm_above, "c_below": charm_below,
    }

# Process all trades
results = []
by_score = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0})
eval_setups = ('Skew Charm', 'DD Exhaustion', 'Paradigm Reversal', 'AG Short')
skipped = 0

for i, t in enumerate(trades):
    tid, ts, time_str, trade_date, setup, direction, grade, spot, result, pnl, align = t
    pnl_val = float(pnl) if pnl is not None else 0
    is_win = result == 'WIN' or (result == 'EXPIRED' and pnl_val > 0)
    is_loss = result == 'LOSS' or (result == 'EXPIRED' and pnl_val < 0)

    rows = get_pin_data(c, ts, spot)
    if not rows:
        skipped += 1
        continue

    score, details = calc_pin_score(rows, spot)
    is_eval = setup in eval_setups and align is not None and abs(align) >= 3

    by_score[score]["n"] += 1
    by_score[score]["pnl"] += pnl_val
    if is_win: by_score[score]["w"] += 1
    elif is_loss: by_score[score]["l"] += 1

    results.append({
        "id": tid, "time": time_str, "date": str(trade_date),
        "setup": setup, "dir": direction, "result": result,
        "pnl": pnl_val, "pin": score, "eval": is_eval,
        "gex_d": details["gex_dist"],
        "v_a": details["v_above"], "v_b": details["v_below"],
        "c_a": details["c_above"], "c_b": details["c_below"],
    })

    if (i + 1) % 50 == 0:
        sys.stderr.write("  processed %d/%d...\r" % (i + 1, len(trades)))

print("Processed: %d, skipped (no data): %d" % (len(results), skipped))

# --- Results ---
print("\n-- PIN SCORE DISTRIBUTION (all trades) --")
print("%-10s %5s %5s %5s %6s %8s %8s" % ("Score", "Total", "Wins", "Loss", "WR%", "PnL", "Avg"))
print("-" * 55)
for score in sorted(by_score.keys()):
    s = by_score[score]
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    avg = s["pnl"] / s["n"] if s["n"] else 0
    tag = " <<PIN" if score >= 4 else ""
    print("%-10s %5d %5d %5d %5.0f%% %+8.1f %+8.2f%s" % (
        score, s["n"], s["w"], s["l"], wr, s["pnl"], avg, tag))

# PIN (>=4) vs rest
pin_all = {"w": 0, "l": 0, "pnl": 0, "n": 0}
nopin_all = {"w": 0, "l": 0, "pnl": 0, "n": 0}
for r in results:
    is_win = r["result"] == "WIN" or (r["result"] == "EXPIRED" and r["pnl"] > 0)
    is_loss = r["result"] == "LOSS" or (r["result"] == "EXPIRED" and r["pnl"] < 0)
    bucket = pin_all if r["pin"] >= 4 else nopin_all
    bucket["n"] += 1
    bucket["pnl"] += r["pnl"]
    if is_win: bucket["w"] += 1
    elif is_loss: bucket["l"] += 1

print("\n-- PIN (>=4) vs NO-PIN (<4) --")
print("%-15s %5s %5s %5s %6s %8s %8s" % ("Regime", "Total", "Wins", "Loss", "WR%", "PnL", "Avg"))
print("-" * 60)
for label, s in [("NO PIN (<4)", nopin_all), ("PIN (>=4)", pin_all)]:
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    avg = s["pnl"] / s["n"] if s["n"] else 0
    print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (label, s["n"], s["w"], s["l"], wr, s["pnl"], avg))

# Also test threshold 3
pin3 = {"w": 0, "l": 0, "pnl": 0, "n": 0}
nopin3 = {"w": 0, "l": 0, "pnl": 0, "n": 0}
for r in results:
    is_win = r["result"] == "WIN" or (r["result"] == "EXPIRED" and r["pnl"] > 0)
    is_loss = r["result"] == "LOSS" or (r["result"] == "EXPIRED" and r["pnl"] < 0)
    bucket = pin3 if r["pin"] >= 3 else nopin3
    bucket["n"] += 1
    bucket["pnl"] += r["pnl"]
    if is_win: bucket["w"] += 1
    elif is_loss: bucket["l"] += 1

print("\n-- PIN (>=3) vs NO-PIN (<3) --")
for label, s in [("NO PIN (<3)", nopin3), ("PIN (>=3)", pin3)]:
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    avg = s["pnl"] / s["n"] if s["n"] else 0
    print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (label, s["n"], s["w"], s["l"], wr, s["pnl"], avg))

# Eval-eligible breakdown
print("\n-- EVAL-ELIGIBLE: PIN vs NO-PIN --")
eval_results = [r for r in results if r["eval"]]
if eval_results:
    ep = {"w": 0, "l": 0, "pnl": 0, "n": 0}
    enp = {"w": 0, "l": 0, "pnl": 0, "n": 0}
    for r in eval_results:
        is_win = r["result"] == "WIN" or (r["result"] == "EXPIRED" and r["pnl"] > 0)
        is_loss = r["result"] == "LOSS" or (r["result"] == "EXPIRED" and r["pnl"] < 0)
        bucket = ep if r["pin"] >= 4 else enp
        bucket["n"] += 1
        bucket["pnl"] += r["pnl"]
        if is_win: bucket["w"] += 1
        elif is_loss: bucket["l"] += 1
    for label, s in [("Eval NO PIN", enp), ("Eval PIN", ep)]:
        wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
        avg = s["pnl"] / s["n"] if s["n"] else 0
        print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (label, s["n"], s["w"], s["l"], wr, s["pnl"], avg))

# Mar 10 detail
print("\n-- MAR 10 ALL TRADES WITH PIN SCORES --")
mar10 = [r for r in results if r["date"] == "2026-03-10"]
print("%-6s %-18s %-6s %-8s %+7s  pin  gex_d  v_abv  v_blw  c_abv  c_blw" % (
    "Time", "Setup", "Dir", "Result", "PnL"))
print("-" * 100)
for r in mar10:
    print("%-6s %-18s %-6s %-8s %+7.1f  %d    %s  %s  %s  %s  %s" % (
        r["time"], r["setup"], r["dir"], r["result"], r["pnl"],
        r["pin"],
        "%4.0f" % r["gex_d"] if r["gex_d"] is not None else "  ? ",
        "%+6.0f" % r["v_a"] if r["v_a"] is not None else "     ?",
        "%+6.0f" % r["v_b"] if r["v_b"] is not None else "     ?",
        "%+6.0f" % r["c_a"] if r["c_a"] is not None else "     ?",
        "%+6.0f" % r["c_b"] if r["c_b"] is not None else "     ?"))

c.close()
