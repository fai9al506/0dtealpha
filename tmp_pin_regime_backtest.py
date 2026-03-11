"""Pin Regime Detection Backtest

Detects pinning from Greek exposure structure:
1. Spot near max +GEX strike (within 10 pts)
2. Negative vanna above spot = resistance ceiling
3. Positive vanna below spot = support floor
4. Positive charm above spot = resistance
5. Negative charm below spot = support

When all conditions met -> price is boxed -> directional trades fail.
"""
import os, sys, json
from collections import defaultdict
from datetime import timedelta
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
c = e.connect()

# Get all resolved trades with eval filter
trades = c.execute(text("""
    SELECT sl.id, sl.ts,
           to_char(sl.ts AT TIME ZONE 'America/New_York', 'HH24:MI') as t,
           sl.ts::date as trade_date,
           sl.setup_name, sl.direction, sl.grade, sl.spot,
           sl.outcome_result, sl.outcome_pnl, sl.greek_alignment
    FROM setup_log sl
    WHERE sl.grade != 'LOG' AND sl.outcome_result IS NOT NULL
    ORDER BY sl.ts
""")).fetchall()

print("Analyzing %d total trades for pin regime..." % len(trades))
print()

def get_pin_score(conn, trade_ts, spot):
    """Check exposure structure around spot at trade time.
    Returns (pin_score 0-5, details dict)"""
    # Find nearest exposure snapshot (within 5 min of trade)
    ts_utc = trade_ts  # already TZ-aware from DB

    details = {
        "max_gex_strike": None, "gex_dist": None,
        "vanna_above": None, "vanna_below": None,
        "charm_above": None, "charm_below": None,
    }

    # 1. Get gamma-TODAY exposure to find max +GEX strike
    gex_rows = conn.execute(text("""
        SELECT strike, value FROM volland_exposure_points
        WHERE greek = 'gamma' AND expiration_option = 'TODAY'
          AND ts_utc BETWEEN :ts - INTERVAL '3 minutes' AND :ts + INTERVAL '3 minutes'
          AND ABS(current_price - :spot) < 30
        ORDER BY value DESC
        LIMIT 5
    """), {"ts": ts_utc, "spot": float(spot)}).fetchall()

    pin_score = 0

    if gex_rows:
        max_gex_strike = float(gex_rows[0][0])
        max_gex_value = float(gex_rows[0][1])
        gex_dist = abs(float(spot) - max_gex_strike)
        details["max_gex_strike"] = max_gex_strike
        details["gex_dist"] = gex_dist
        details["max_gex_value"] = max_gex_value

        # Pin condition 1: spot within 10 pts of max +GEX
        if gex_dist <= 10 and max_gex_value > 0:
            pin_score += 1

    # 2. Get vanna-ALL exposure above and below spot
    vanna_rows = conn.execute(text("""
        SELECT strike, value FROM volland_exposure_points
        WHERE greek = 'vanna' AND expiration_option = 'ALL'
          AND ts_utc BETWEEN :ts - INTERVAL '3 minutes' AND :ts + INTERVAL '3 minutes'
          AND ABS(current_price - :spot) < 30
          AND ABS(strike - :spot) <= 50
        ORDER BY strike
    """), {"ts": ts_utc, "spot": float(spot)}).fetchall()

    if vanna_rows:
        above_sum = sum(float(r[1]) for r in vanna_rows if float(r[0]) > float(spot))
        below_sum = sum(float(r[1]) for r in vanna_rows if float(r[0]) < float(spot))
        details["vanna_above"] = above_sum
        details["vanna_below"] = below_sum

        # Pin condition 2: negative vanna above (resistance)
        if above_sum < 0:
            pin_score += 1
        # Pin condition 3: positive vanna below (support)
        if below_sum > 0:
            pin_score += 1

    # 3. Get charm exposure above and below spot
    charm_rows = conn.execute(text("""
        SELECT strike, value FROM volland_exposure_points
        WHERE greek = 'charm' AND expiration_option IS NULL
          AND ts_utc BETWEEN :ts - INTERVAL '3 minutes' AND :ts + INTERVAL '3 minutes'
          AND ABS(current_price - :spot) < 30
          AND ABS(strike - :spot) <= 50
        ORDER BY strike
    """), {"ts": ts_utc, "spot": float(spot)}).fetchall()

    if charm_rows:
        charm_above = sum(float(r[1]) for r in charm_rows if float(r[0]) > float(spot))
        charm_below = sum(float(r[1]) for r in charm_rows if float(r[0]) < float(spot))
        details["charm_above"] = charm_above
        details["charm_below"] = charm_below

        # Pin condition 4: positive charm above (resistance) — charm opposes upward
        if charm_above > 0:
            pin_score += 1
        # Pin condition 5: negative charm below (support) — charm opposes downward
        if charm_below < 0:
            pin_score += 1

    return pin_score, details


# Process trades
results = []
pin_trades = {"w": 0, "l": 0, "pnl": 0, "n": 0}
nopin_trades = {"w": 0, "l": 0, "pnl": 0, "n": 0}
by_score = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "n": 0})

# Eval-eligible filter
eval_setups = ('Skew Charm', 'DD Exhaustion', 'Paradigm Reversal', 'AG Short')

processed = 0
skipped_no_data = 0

for t in trades:
    trade_id, ts, time_str, trade_date, setup, direction, grade, spot, result, pnl, align = t

    if not spot:
        continue

    pnl_val = float(pnl) if pnl is not None else 0
    is_win = result == 'WIN' or (result == 'EXPIRED' and pnl_val > 0)
    is_loss = result == 'LOSS' or (result == 'EXPIRED' and pnl_val < 0)

    pin_score, details = get_pin_score(c, ts, spot)

    if details["max_gex_strike"] is None and details["vanna_above"] is None:
        skipped_no_data += 1
        continue

    processed += 1
    is_eval = setup in eval_setups and align is not None and abs(align) >= 3

    # PIN threshold: score >= 4 out of 5
    is_pin = pin_score >= 4

    bucket = pin_trades if is_pin else nopin_trades
    bucket["n"] += 1
    bucket["pnl"] += pnl_val
    if is_win: bucket["w"] += 1
    elif is_loss: bucket["l"] += 1

    by_score[pin_score]["n"] += 1
    by_score[pin_score]["pnl"] += pnl_val
    if is_win: by_score[pin_score]["w"] += 1
    elif is_loss: by_score[pin_score]["l"] += 1

    results.append({
        "id": trade_id, "time": time_str, "date": str(trade_date),
        "setup": setup, "dir": direction, "result": result,
        "pnl": pnl_val, "pin_score": pin_score, "is_eval": is_eval,
        "gex_dist": details.get("gex_dist"),
        "vanna_above": details.get("vanna_above"),
        "vanna_below": details.get("vanna_below"),
        "charm_above": details.get("charm_above"),
        "charm_below": details.get("charm_below"),
    })

print("Processed: %d trades (%d skipped, no exposure data)" % (processed, skipped_no_data))

# Results by pin score
print("\n-- BY PIN SCORE (0-5) --")
print("%-8s %5s %5s %5s %6s %8s %8s" % ("Score", "Total", "Wins", "Loss", "WR%", "PnL", "Avg"))
print("-" * 55)
for score in sorted(by_score.keys()):
    s = by_score[score]
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    avg = s["pnl"] / s["n"] if s["n"] else 0
    label = "%d" % score
    if score >= 4: label += " PIN"
    print("%-8s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (label, s["n"], s["w"], s["l"], wr, s["pnl"], avg))

# Pin vs No-Pin summary
print("\n-- PIN vs NO-PIN --")
print("%-15s %5s %5s %5s %6s %8s %8s" % ("Regime", "Total", "Wins", "Loss", "WR%", "PnL", "Avg"))
print("-" * 60)
for label, s in [("No Pin (0-3)", nopin_trades), ("PIN (4-5)", pin_trades)]:
    wr = s["w"] / (s["w"] + s["l"]) * 100 if (s["w"] + s["l"]) > 0 else 0
    avg = s["pnl"] / s["n"] if s["n"] else 0
    print("%-15s %5d %5d %5d %5.0f%% %+8.1f %+8.2f" % (label, s["n"], s["w"], s["l"], wr, s["pnl"], avg))

# Eval-eligible in pin regime
print("\n-- EVAL-ELIGIBLE TRADES IN PIN REGIME (score >= 4) --")
eval_pin = [r for r in results if r["is_eval"] and r["pin_score"] >= 4]
eval_nopin = [r for r in results if r["is_eval"] and r["pin_score"] < 4]

if eval_pin:
    print("%-6s %-10s %-5s %-18s %-6s %-8s %+7s  gex_d  v_abv    v_blw   c_abv    c_blw" % (
        "Time", "Date", "Dir", "Setup", "Result", "PnL", "Score"))
    print("-" * 120)
    ep_w = ep_l = 0
    ep_pnl = 0
    for r in eval_pin:
        print("%-6s %-10s %-5s %-18s %-6s %+7.1f  %d      %s  %s  %s  %s  %s" % (
            r["time"], r["date"], r["dir"], r["setup"], r["result"], r["pnl"],
            r["pin_score"],
            "%.0f" % r["gex_dist"] if r["gex_dist"] is not None else "?",
            "%.0f" % r["vanna_above"] if r["vanna_above"] is not None else "?",
            "%.0f" % r["vanna_below"] if r["vanna_below"] is not None else "?",
            "%.0f" % r["charm_above"] if r["charm_above"] is not None else "?",
            "%.0f" % r["charm_below"] if r["charm_below"] is not None else "?"))
        is_win = r["result"] == "WIN" or (r["result"] == "EXPIRED" and r["pnl"] > 0)
        is_loss = r["result"] == "LOSS" or (r["result"] == "EXPIRED" and r["pnl"] < 0)
        if is_win: ep_w += 1
        elif is_loss: ep_l += 1
        ep_pnl += r["pnl"]
    wr = ep_w / (ep_w + ep_l) * 100 if (ep_w + ep_l) > 0 else 0
    print("Eval in PIN: %d trades, %dW/%dL, WR %.0f%%, PnL %+.1f" % (len(eval_pin), ep_w, ep_l, wr, ep_pnl))
else:
    print("No eval-eligible trades in pin regime")

if eval_nopin:
    en_w = sum(1 for r in eval_nopin if r["result"] == "WIN" or (r["result"] == "EXPIRED" and r["pnl"] > 0))
    en_l = sum(1 for r in eval_nopin if r["result"] == "LOSS" or (r["result"] == "EXPIRED" and r["pnl"] < 0))
    en_pnl = sum(r["pnl"] for r in eval_nopin)
    wr = en_w / (en_w + en_l) * 100 if (en_w + en_l) > 0 else 0
    print("Eval NOT pin: %d trades, %dW/%dL, WR %.0f%%, PnL %+.1f" % (len(eval_nopin), en_w, en_l, wr, en_pnl))

# Show Mar 10 specifically
print("\n-- MAR 10 TRADES WITH PIN SCORES --")
mar10 = [r for r in results if r["date"] == "2026-03-10"]
print("%-6s %-18s %-6s %-8s %+7s  pin  gex_d" % ("Time", "Setup", "Dir", "Result", "PnL"))
print("-" * 70)
for r in mar10:
    print("%-6s %-18s %-6s %-8s %+7.1f  %d    %s" % (
        r["time"], r["setup"], r["dir"], r["result"], r["pnl"],
        r["pin_score"],
        "%.0f" % r["gex_dist"] if r["gex_dist"] is not None else "?"))

c.close()
