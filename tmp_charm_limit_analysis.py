"""
Definitive trade-by-trade comparison: Market Entry vs Charm Limit Entry
for ALL setup_log rows where charm_limit_entry IS NOT NULL.

NO DB updates. Read-only analysis.
"""

import os, sys, psycopg2, psycopg2.extras
from collections import defaultdict
from datetime import datetime

DB_URL = os.environ.get("DATABASE_URL", "")
if not DB_URL:
    print("ERROR: DATABASE_URL not set")
    sys.exit(1)

conn = psycopg2.connect(DB_URL)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# ── Step 1: Query all charm-limit trades with outcomes ──
cur.execute("""
    SELECT id,
           (ts AT TIME ZONE 'America/New_York')::date as trade_date,
           setup_name, direction, grade,
           ts AT TIME ZONE 'America/New_York' as ts_et,
           spot, charm_limit_entry, abs_es_price,
           outcome_result, outcome_pnl,
           outcome_max_profit, outcome_max_loss,
           outcome_elapsed_min, paradigm,
           outcome_stop_level, outcome_target_level
    FROM setup_log
    WHERE charm_limit_entry IS NOT NULL
    AND outcome_result IS NOT NULL
    ORDER BY ts
""")
rows = cur.fetchall()
conn.close()

print(f"Total trades with charm_limit_entry AND outcome: {len(rows)}")
print()

# ── SL distances per setup (for shorts: SL = entry + SL_dist) ──
SL_DIST = {
    "Skew Charm": 14,
    "DD Exhaustion": 12,
    "AG Short": 8,
    "BofA Scalp": None,  # uses bofa_stop_level, handled separately
    "Paradigm Reversal": 15,
    "ES Absorption": 8,
    "GEX Long": 8,
    "Vanna Pivot Bounce": 8,
    "IV Momentum": 8,
    "VIX Compression": 20,
    "SB Absorption": 8,
    "SB10 Absorption": 8,
    "SB2 Absorption": 8,
}

# ── Step 2 & 3: Process each trade ──
results = []

for r in rows:
    tid = r["id"]
    trade_date = r["trade_date"]
    setup = r["setup_name"]
    direction = r["direction"]
    grade = r["grade"]
    ts_et = r["ts_et"]
    spot = r["spot"]
    charm_limit = r["charm_limit_entry"]
    abs_es = r["abs_es_price"]
    outcome = r["outcome_result"]
    pnl = r["outcome_pnl"]
    max_profit = r["outcome_max_profit"]
    max_loss = r["outcome_max_loss"]
    elapsed = r["outcome_elapsed_min"]
    paradigm = r["paradigm"]
    db_stop = r["outcome_stop_level"]
    db_target = r["outcome_target_level"]

    is_long = direction.lower() in ("long", "bullish")
    is_es_based = setup in ("ES Absorption", "SB Absorption", "SB10 Absorption", "SB2 Absorption")

    # Time string
    time_str = ts_et.strftime("%H:%M") if ts_et else "??:??"

    # Offset: for shorts, charm_limit should be ABOVE spot
    offset = charm_limit - spot if (charm_limit and spot) else 0

    # ── FILL STATUS ──
    # The LIVE outcome tracker (main.py line 3421-3437):
    #   - Uses charm_limit as entry_price for filled shorts
    #   - Sets LIMIT_TIMEOUT for unfilled shorts (30 min or market close)
    #   - max_loss = entry_price - seen_high (shorts, negative when adverse)
    #
    # For non-TIMEOUT trades: the live tracker resolved them, meaning charm was filled.
    # entry_price used = charm_limit. So seen_high = charm_limit - max_loss.
    #
    # For LIMIT_TIMEOUT: definitely not filled.
    #
    # For the hypothetical MARKET entry comparison, we reconstruct seen_high
    # to verify, but the fill status is determined by outcome_result.

    filled = None
    seen_high = None
    fill_status = "UNKNOWN"

    if outcome == "LIMIT_TIMEOUT":
        filled = False
        fill_status = "TIMEOUT"
    elif is_es_based:
        filled = None
        fill_status = "ES-BASED"
    elif is_long:
        filled = None
        fill_status = "LONG (N/A)"
    else:
        # Short trade with WIN/LOSS/EXPIRED outcome means the live tracker
        # got past the charm fill check. It WAS filled.
        filled = True
        fill_status = "FILLED"

        # Reconstruct seen_high for verification
        # Live tracker used charm_limit as entry: max_loss = charm_limit - seen_high
        if max_loss is not None and charm_limit is not None:
            seen_high = round(charm_limit - max_loss, 2)

    # ── Compute Market Entry values (Scenario A) ──
    # What WOULD have happened with spot entry (no charm limit)
    mkt_entry = spot
    mkt_outcome = outcome
    mkt_pnl_val = pnl  # DB value — but this was computed from charm_limit entry (if filled)

    # IMPORTANT: For filled shorts, the DB pnl was computed using charm_limit as entry.
    # To get the hypothetical MARKET entry pnl, we subtract the offset.
    # Market entry pnl = charm_entry_pnl - offset (worse by the offset amount)
    if filled and not is_es_based and not is_long:
        mkt_pnl_val = round(pnl - offset, 2) if pnl is not None else None
        # Also re-derive outcome for market entry
        if mkt_pnl_val is not None:
            if outcome == "WIN" and mkt_pnl_val <= 0:
                mkt_outcome = "LOSS" if mkt_pnl_val < 0 else "EXPIRED"
            elif outcome == "LOSS" and mkt_pnl_val > 0:
                mkt_outcome = "WIN"
            # Note: for trailing setups, this is approximate (trail might behave differently)
    elif outcome == "LIMIT_TIMEOUT":
        # If limit timed out, market entry would have just entered at spot.
        # We can't know the market outcome from the data since the trade wasn't tracked.
        # But we know the price action during the 30+ min window.
        # For now, mark as "WOULD_TRADE" — we can't determine market outcome from available data.
        mkt_outcome = "UNKNOWN (no tracking)"
        mkt_pnl_val = None

    sl_dist = SL_DIST.get(setup, 12)
    if sl_dist is not None and spot is not None:
        mkt_sl = round(spot + sl_dist, 1)  # short: SL above entry
    elif db_stop is not None:
        mkt_sl = db_stop
    else:
        mkt_sl = None

    # ── Compute Limit Entry values (Scenario B) ──
    # This IS what actually happened (the DB values)
    if is_es_based:
        lim_pnl = pnl
        lim_outcome = outcome
        lim_sl = mkt_sl
        category = "ES-based (unaffected)"
    elif is_long:
        lim_pnl = pnl
        lim_outcome = outcome
        lim_sl = mkt_sl
        category = "Long (unaffected)"
    elif fill_status == "TIMEOUT":
        lim_pnl = 0.0
        lim_outcome = "LIMIT_TIMEOUT"
        lim_sl = None
        category = "Timeout"
    elif filled:
        # Filled short: the DB has the correct charm-limit-based values
        lim_pnl = pnl  # DB value IS the limit entry value
        lim_outcome = outcome

        if sl_dist is not None and charm_limit is not None:
            lim_sl = round(charm_limit + sl_dist, 1)
        else:
            lim_sl = None

        # Categorize based on market vs limit outcomes
        if mkt_pnl_val is not None and pnl is not None:
            if mkt_pnl_val <= 0 and pnl > 0:
                category = "Filled: LOSS->WIN flip"
            elif mkt_pnl_val > 0 and pnl <= 0:
                category = "Filled: WIN->LOSS flip"
            elif outcome == "WIN":
                category = "Filled: Won both"
            elif outcome == "LOSS":
                category = "Filled: Lost both (reduced)"
            elif outcome == "EXPIRED":
                if mkt_pnl_val > 0:
                    category = "Filled: EXPIRED (was WIN mkt)"
                else:
                    category = "Filled: EXPIRED (both)"
            else:
                category = f"Filled: {outcome}"
        else:
            category = "Filled (no PnL data)"
    else:
        lim_pnl = pnl
        lim_outcome = outcome
        lim_sl = mkt_sl
        category = f"Unknown fill ({fill_status})"

    # Delta
    if lim_pnl is not None and mkt_pnl_val is not None:
        delta = round(lim_pnl - mkt_pnl_val, 2)
        if delta > 0:
            change = f"+{delta:.1f} better"
        elif delta < 0:
            change = f"{delta:.1f} worse"
        else:
            change = "same"
    else:
        delta = 0
        change = "n/a"

    results.append({
        "id": tid,
        "trade_date": trade_date,
        "time": time_str,
        "setup": setup,
        "direction": direction,
        "grade": grade,
        "spot": spot,
        "charm_limit": charm_limit,
        "offset": round(offset, 1),
        "abs_es": abs_es,
        "fill_status": fill_status,
        "filled": filled,
        "mkt_sl": mkt_sl,
        "lim_sl": lim_sl,
        "mkt_outcome": mkt_outcome,
        "mkt_pnl": mkt_pnl_val,
        "lim_outcome": lim_outcome,
        "lim_pnl": lim_pnl,
        "delta": delta,
        "change": change,
        "category": category,
        "max_profit": max_profit,
        "max_loss": max_loss,
        "seen_high": seen_high,
        "elapsed": elapsed,
        "paradigm": paradigm,
    })

# ── VERIFICATION: Print trade id=1038 raw values ──
print("=" * 130)
print("VERIFICATION - Trade ID 1038 (if present)")
print("=" * 130)
t1038 = [r for r in results if r["id"] == 1038]
if t1038:
    t = t1038[0]
    print(f"  spot={t['spot']}, charm_limit={t['charm_limit']}, offset={t['offset']}")
    print(f"  DB outcome={t['lim_outcome']}, DB pnl (from charm entry)={t['lim_pnl']}")
    print(f"  max_profit={t['max_profit']}, max_loss={t['max_loss']}")
    print(f"  seen_high (reconstructed from charm entry)={t['seen_high']}")
    print(f"  fill_status={t['fill_status']}")
    print(f"  Hypothetical market entry pnl={t['mkt_pnl']}, market outcome={t['mkt_outcome']}")
    print(f"  delta (limit - market)={t['delta']}")
else:
    print("  Trade 1038 not found in results.")
    if results:
        print(f"  Available IDs include: {[r['id'] for r in results[:10]]}")
print()

# ── Step 4: Print FULL TABLE grouped by date ──
print("=" * 130)
print("FULL TRADE-BY-TRADE COMPARISON: MARKET ENTRY vs CHARM LIMIT ENTRY")
print("=" * 130)
print()
print("NOTE: 'Mkt' = hypothetical market entry at spot. 'Lim' = actual charm limit entry (DB values).")
print("For filled trades: DB pnl was computed from charm_limit, so MktPnL = DB_pnl - offset, LimPnL = DB_pnl.")
print("For timeouts: LimPnL=0 (no trade). MktPnL=UNKNOWN (not tracked at spot).")
print()

dates = sorted(set(r["trade_date"] for r in results))

for d in dates:
    day_trades = [r for r in results if r["trade_date"] == d]
    print(f"\n{'='*130}")
    print(f"=== {d.strftime('%Y-%m-%d')} ({len(day_trades)} trades) ===")
    print(f"{'='*130}")

    hdr = f"{'ID':>5} | {'Time':>5} | {'Setup':<18} | {'Gr':>3} | {'Spot':>7} | {'Limit':>7} | {'Off':>5} | {'Fill':>8} | {'MktSL':>7} | {'LimSL':>7} | {'MktOut':>8} | {'MktPnL':>7} | {'LimOut':>12} | {'LimPnL':>7} | {'Delta':>6} | Category"
    print(hdr)
    print("-" * len(hdr))

    day_mkt_pnl = 0
    day_lim_pnl = 0
    day_filled = 0
    day_total = 0

    for r in day_trades:
        day_total += 1
        mkt_p = r["mkt_pnl"] if r["mkt_pnl"] is not None else 0
        lim_p = r["lim_pnl"] if r["lim_pnl"] is not None else 0
        day_mkt_pnl += mkt_p
        day_lim_pnl += lim_p
        if r["filled"]:
            day_filled += 1

        spot_s = f"{r['spot']:.1f}" if r["spot"] else "n/a"
        cl_s = f"{r['charm_limit']:.1f}" if r["charm_limit"] else "n/a"
        off_s = f"{r['offset']:+.1f}" if r["offset"] else "0.0"
        msl_s = f"{r['mkt_sl']:.1f}" if r["mkt_sl"] else "n/a"
        lsl_s = f"{r['lim_sl']:.1f}" if r["lim_sl"] else "n/a"
        mp_s = f"{mkt_p:+.1f}" if r["mkt_pnl"] is not None else "UNK"
        lp_s = f"{lim_p:+.1f}" if r["lim_pnl"] is not None else "n/a"
        d_s = f"{r['delta']:+.1f}" if r["delta"] else "0.0"
        mo_s = r["mkt_outcome"][:8] if r["mkt_outcome"] else "n/a"

        print(f"{r['id']:>5} | {r['time']:>5} | {r['setup']:<18} | {r['grade']:>3} | {spot_s:>7} | {cl_s:>7} | {off_s:>5} | {r['fill_status']:>8} | {msl_s:>7} | {lsl_s:>7} | {mo_s:>8} | {mp_s:>7} | {r['lim_outcome']:>12} | {lp_s:>7} | {d_s:>6} | {r['category']}")

    timeout_count = sum(1 for r in day_trades if r["fill_status"] == "TIMEOUT")
    # For day delta, only count filled trades where both PnLs are known
    filled_day = [r for r in day_trades if r["filled"] and r["mkt_pnl"] is not None]
    filled_mkt = sum(r["mkt_pnl"] for r in filled_day)
    filled_lim = sum(r["lim_pnl"] or 0 for r in filled_day)
    filled_delta = round(filled_lim - filled_mkt, 1)
    print(f"\n  Date subtotal: Filled={day_filled}/{day_total}, Timeouts={timeout_count}")
    print(f"  Filled trades only: Mkt={filled_mkt:+.1f}, Lim={filled_lim:+.1f}, Delta={filled_delta:+.1f}")


# ── Step 5: Summary Tables ──

print()
print()
print("=" * 130)
print("SUMMARY TABLE 1: BY CATEGORY")
print("=" * 130)
print()
print("NOTE: Delta = LimPnL - MktPnL. Positive = limit entry was better.")
print("For TIMEOUT rows, MktPnL is unknown (counted as 0 in totals).")
print()

categories = defaultdict(lambda: {"count": 0, "mkt_pnl": 0.0, "lim_pnl": 0.0, "mkt_known": 0})
for r in results:
    c = r["category"]
    categories[c]["count"] += 1
    categories[c]["lim_pnl"] += (r["lim_pnl"] or 0)
    if r["mkt_pnl"] is not None:
        categories[c]["mkt_pnl"] += r["mkt_pnl"]
        categories[c]["mkt_known"] += 1

cat_order = [
    "Filled: Won both",
    "Filled: LOSS->WIN flip",
    "Filled: Lost both (reduced)",
    "Filled: WIN->LOSS flip",
    "Filled: EXPIRED (both)",
    "Filled: EXPIRED (was WIN mkt)",
    "Timeout",
    "ES-based (unaffected)",
    "Long (unaffected)",
]

print(f"{'Category':<35} | {'Count':>5} | {'Mkt PnL':>9} | {'Lim PnL':>9} | {'Delta':>9} | Notes")
print("-" * 115)

total_count = 0
total_mkt = 0.0
total_lim = 0.0

printed_cats = set()
for cat in cat_order:
    if cat in categories:
        c = categories[cat]
        delta = round(c["lim_pnl"] - c["mkt_pnl"], 1)
        note = ""
        if "Timeout" in cat:
            note = f"MktPnL unknown ({c['count']} trades skipped)"
        elif "LOSS->WIN" in cat:
            note = "FLIPPED!"
        elif "reduced" in cat:
            note = "same outcome, less pain"
        elif "Won both" in cat:
            note = "bigger win"
        print(f"{cat:<35} | {c['count']:>5} | {c['mkt_pnl']:>+9.1f} | {c['lim_pnl']:>+9.1f} | {delta:>+9.1f} | {note}")
        total_count += c["count"]
        total_mkt += c["mkt_pnl"]
        total_lim += c["lim_pnl"]
        printed_cats.add(cat)

for cat in sorted(categories.keys()):
    if cat not in printed_cats:
        c = categories[cat]
        delta = round(c["lim_pnl"] - c["mkt_pnl"], 1)
        print(f"{cat:<35} | {c['count']:>5} | {c['mkt_pnl']:>+9.1f} | {c['lim_pnl']:>+9.1f} | {delta:>+9.1f} |")
        total_count += c["count"]
        total_mkt += c["mkt_pnl"]
        total_lim += c["lim_pnl"]

print("-" * 115)
total_delta = round(total_lim - total_mkt, 1)
print(f"{'TOTAL':<35} | {total_count:>5} | {total_mkt:>+9.1f} | {total_lim:>+9.1f} | {total_delta:>+9.1f} |")

# Also show filled-only totals
filled_only = [r for r in results if r["filled"] and r["mkt_pnl"] is not None]
fo_mkt = sum(r["mkt_pnl"] for r in filled_only)
fo_lim = sum(r["lim_pnl"] or 0 for r in filled_only)
fo_delta = round(fo_lim - fo_mkt, 1)
print(f"{'FILLED ONLY (comparable)':<35} | {len(filled_only):>5} | {fo_mkt:>+9.1f} | {fo_lim:>+9.1f} | {fo_delta:>+9.1f} | apples-to-apples")


# ── Summary 2: By Setup ──
print()
print("=" * 130)
print("SUMMARY TABLE 2: BY SETUP")
print("=" * 130)

setups = defaultdict(lambda: {"total": 0, "filled": 0, "timeout": 0, "mkt_pnl": 0.0, "lim_pnl": 0.0,
                                "mkt_wins": 0, "mkt_losses": 0, "lim_wins": 0, "lim_losses": 0})
for r in results:
    s = r["setup"]
    setups[s]["total"] += 1
    setups[s]["lim_pnl"] += (r["lim_pnl"] or 0)
    if r["filled"]:
        setups[s]["filled"] += 1
        if r["mkt_pnl"] is not None:
            setups[s]["mkt_pnl"] += r["mkt_pnl"]
    if r["fill_status"] == "TIMEOUT":
        setups[s]["timeout"] += 1
    if r["mkt_outcome"] == "WIN":
        setups[s]["mkt_wins"] += 1
    if r["mkt_outcome"] == "LOSS":
        setups[s]["mkt_losses"] += 1
    if r["lim_outcome"] == "WIN":
        setups[s]["lim_wins"] += 1
    if r["lim_outcome"] in ("LOSS",):
        setups[s]["lim_losses"] += 1

print(f"{'Setup':<20} | {'Total':>5} | {'Filled':>6} | {'T/O':>4} | {'MktW':>4} | {'MktL':>4} | {'MktPnL':>8} | {'LimW':>4} | {'LimL':>4} | {'LimPnL':>8} | {'Delta':>8} | {'Fill%':>5}")
print("-" * 130)
for s in sorted(setups.keys()):
    d = setups[s]
    delta = round(d["lim_pnl"] - d["mkt_pnl"], 1)
    fill_pct = f"{d['filled']/d['total']*100:.0f}%" if d["total"] > 0 else "n/a"
    print(f"{s:<20} | {d['total']:>5} | {d['filled']:>6} | {d['timeout']:>4} | {d['mkt_wins']:>4} | {d['mkt_losses']:>4} | {d['mkt_pnl']:>+8.1f} | {d['lim_wins']:>4} | {d['lim_losses']:>4} | {d['lim_pnl']:>+8.1f} | {delta:>+8.1f} | {fill_pct:>5}")

print("-" * 130)
grand_total = sum(d["total"] for d in setups.values())
grand_filled = sum(d["filled"] for d in setups.values())
grand_timeout = sum(d["timeout"] for d in setups.values())
grand_mkt = sum(d["mkt_pnl"] for d in setups.values())
grand_lim = sum(d["lim_pnl"] for d in setups.values())
grand_mw = sum(d["mkt_wins"] for d in setups.values())
grand_ml = sum(d["mkt_losses"] for d in setups.values())
grand_lw = sum(d["lim_wins"] for d in setups.values())
grand_ll = sum(d["lim_losses"] for d in setups.values())
grand_delta = round(grand_lim - grand_mkt, 1)
grand_fp = f"{grand_filled/grand_total*100:.0f}%" if grand_total > 0 else "n/a"
print(f"{'TOTAL':<20} | {grand_total:>5} | {grand_filled:>6} | {grand_timeout:>4} | {grand_mw:>4} | {grand_ml:>4} | {grand_mkt:>+8.1f} | {grand_lw:>4} | {grand_ll:>4} | {grand_lim:>+8.1f} | {grand_delta:>+8.1f} | {grand_fp:>5}")


# ── Summary 3: By Date ──
print()
print("=" * 130)
print("SUMMARY TABLE 3: BY DATE")
print("=" * 130)

date_stats = defaultdict(lambda: {"total": 0, "filled": 0, "timeout": 0, "mkt_pnl": 0.0, "lim_pnl": 0.0,
                                    "mkt_wins": 0, "mkt_losses": 0, "lim_wins": 0, "lim_losses": 0})
for r in results:
    dd = r["trade_date"]
    date_stats[dd]["total"] += 1
    date_stats[dd]["lim_pnl"] += (r["lim_pnl"] or 0)
    if r["filled"]:
        date_stats[dd]["filled"] += 1
        if r["mkt_pnl"] is not None:
            date_stats[dd]["mkt_pnl"] += r["mkt_pnl"]
    if r["fill_status"] == "TIMEOUT":
        date_stats[dd]["timeout"] += 1
    if r["mkt_outcome"] == "WIN":
        date_stats[dd]["mkt_wins"] += 1
    if r["mkt_outcome"] == "LOSS":
        date_stats[dd]["mkt_losses"] += 1
    if r["lim_outcome"] == "WIN":
        date_stats[dd]["lim_wins"] += 1
    if r["lim_outcome"] in ("LOSS",):
        date_stats[dd]["lim_losses"] += 1

print(f"{'Date':<12} | {'Total':>5} | {'Filled':>6} | {'T/O':>4} | {'MktW':>4} | {'MktL':>4} | {'MktPnL':>8} | {'LimW':>4} | {'LimL':>4} | {'LimPnL':>8} | {'Delta':>8} | {'Fill%':>5}")
print("-" * 120)
for dd in sorted(date_stats.keys()):
    ds = date_stats[dd]
    delta = round(ds["lim_pnl"] - ds["mkt_pnl"], 1)
    fill_pct = f"{ds['filled']/ds['total']*100:.0f}%" if ds["total"] > 0 else "n/a"
    print(f"{dd.strftime('%Y-%m-%d'):<12} | {ds['total']:>5} | {ds['filled']:>6} | {ds['timeout']:>4} | {ds['mkt_wins']:>4} | {ds['mkt_losses']:>4} | {ds['mkt_pnl']:>+8.1f} | {ds['lim_wins']:>4} | {ds['lim_losses']:>4} | {ds['lim_pnl']:>+8.1f} | {delta:>+8.1f} | {fill_pct:>5}")

print("-" * 120)
print(f"{'TOTAL':<12} | {grand_total:>5} | {grand_filled:>6} | {grand_timeout:>4} | {grand_mw:>4} | {grand_ml:>4} | {grand_mkt:>+8.1f} | {grand_lw:>4} | {grand_ll:>4} | {grand_lim:>+8.1f} | {grand_delta:>+8.1f} | {grand_fp:>5}")


# ── Extra: Fill rate details ──
print()
print("=" * 130)
print("FILL RATE DETAIL")
print("=" * 130)
short_trades = [r for r in results if r["direction"].lower() in ("short", "bearish")
                and r["fill_status"] not in ("ES-BASED", "LONG (N/A)")]
filled_shorts = [r for r in short_trades if r["filled"]]
timeout_shorts = [r for r in short_trades if r["fill_status"] == "TIMEOUT"]
other = [r for r in short_trades if not r["filled"] and r["fill_status"] != "TIMEOUT"]

if short_trades:
    print(f"Total short trades with charm_limit: {len(short_trades)}")
    print(f"  FILLED: {len(filled_shorts)} ({len(filled_shorts)/len(short_trades)*100:.1f}%)")
    print(f"  TIMEOUT: {len(timeout_shorts)} ({len(timeout_shorts)/len(short_trades)*100:.1f}%)")
    if other:
        print(f"  OTHER/UNKNOWN: {len(other)}")

    print()
    print("Average offset (charm_limit - spot) for shorts:")
    offsets = [r["offset"] for r in short_trades if r["offset"]]
    if offsets:
        print(f"  All:     Mean={sum(offsets)/len(offsets):+.1f}, Min={min(offsets):+.1f}, Max={max(offsets):+.1f}")
    filled_offsets = [r["offset"] for r in filled_shorts if r["offset"]]
    if filled_offsets:
        print(f"  Filled:  Mean={sum(filled_offsets)/len(filled_offsets):+.1f}, Min={min(filled_offsets):+.1f}, Max={max(filled_offsets):+.1f}")
    timeout_offsets = [r["offset"] for r in timeout_shorts if r["offset"]]
    if timeout_offsets:
        print(f"  Timeout: Mean={sum(timeout_offsets)/len(timeout_offsets):+.1f}, Min={min(timeout_offsets):+.1f}, Max={max(timeout_offsets):+.1f}")

# Key losers that flipped
print()
print("=" * 130)
print("KEY FLIPS: Trades where market entry outcome differs from limit entry outcome")
print("=" * 130)
flips = [r for r in results if "flip" in r["category"].lower()]
if flips:
    for r in flips:
        print(f"  ID {r['id']} | {r['trade_date']} {r['time']} | {r['setup']:<18} | spot={r['spot']:.1f} limit={r['charm_limit']:.1f} off={r['offset']:+.1f} | mkt={r['mkt_pnl']:+.1f}({r['mkt_outcome']}) -> lim={r['lim_pnl']:+.1f}({r['lim_outcome']}) | {r['paradigm']}")
    print(f"\n  Total flip count: {len(flips)}")
else:
    print("  None found.")

# Timeouts detail
print()
print("=" * 130)
print("ALL TIMEOUTS (charm limit never reached)")
print("=" * 130)
timeouts = [r for r in results if r["fill_status"] == "TIMEOUT"]
if timeouts:
    for r in timeouts:
        print(f"  ID {r['id']} | {r['trade_date']} {r['time']} | {r['setup']:<18} | spot={r['spot']:.1f} limit={r['charm_limit']:.1f} off={r['offset']:+.1f} | elapsed={r['elapsed']}min | {r['paradigm']}")
    print(f"\n  Total timeouts: {len(timeouts)}")
else:
    print("  None found.")


print()
print("=" * 130)
print("ANALYSIS COMPLETE")
print("=" * 130)
