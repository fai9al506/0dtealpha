"""Credit spread backtest with CORRECTED direction.
Bug: original used same side as naked (call for longs, put for shorts).
Fix: credit spread uses OPPOSITE side (put for longs, call for shorts).

Bullish signal → bull PUT spread: sell ATM put, buy lower put
Bearish signal → bear CALL spread: sell ATM call, buy higher call
"""
import sqlalchemy, json, os, sys
from sqlalchemy import text
from collections import defaultdict
from datetime import datetime, timedelta

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine = sqlalchemy.create_engine(DB_URL)

SPREAD_WIDTH = 10.0  # $10 wide SPXW

# ── Get all setup outcomes ──
with engine.begin() as conn:
    setups = conn.execute(text("""
        SELECT id, setup_name, direction, spot, outcome_result, outcome_pnl,
               outcome_elapsed_min, outcome_max_profit, outcome_max_loss,
               ts, vix, overvix, greek_alignment,
               (ts AT TIME ZONE 'America/New_York')::date as trade_date
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND ts >= '2026-03-01'
          AND ts < '2026-03-19'
        ORDER BY id
    """)).mappings().all()

print(f"Total setup outcomes: {len(setups)}")

# ── Get chain snapshots for real prices ──
with engine.begin() as conn:
    chains_raw = conn.execute(text("""
        SELECT ts, rows
        FROM chain_snapshots
        WHERE ts >= '2026-03-01' AND ts < '2026-03-19'
        ORDER BY ts
    """)).fetchall()

print(f"Chain snapshots loaded: {len(chains_raw)}")

# Parse chains into list of dicts with ts + rows
chains = []
for row in chains_raw:
    ts = row[0]
    rows_data = row[1] if isinstance(row[1], list) else json.loads(row[1]) if row[1] else []
    if rows_data:
        chains.append({"ts": ts, "rows": rows_data})

print(f"Chains with rows: {len(chains)}")


# Chain row format: [call_vol, call_oi, call_iv, call_gamma, call_delta, call_bid, call_bid_qty,
#                    call_ask, call_ask_qty, call_last, STRIKE, put_last, put_ask, put_ask_qty,
#                    put_bid, put_bid_qty, put_delta, put_gamma, put_iv, put_oi, put_vol]
# Indices:           0         1        2        3           4          5         6
#                    7          8            9          10      11        12         13
#                    14        15         16         17        18       19      20

def _parse_row(r, option_side):
    """Extract strike/bid/ask/delta from chain row array."""
    strike = float(r[10]) if r[10] is not None else 0
    if option_side == "call":
        return {"strike": strike, "bid": float(r[5] or 0), "ask": float(r[7] or 0), "delta": float(r[4] or 0)}
    else:
        return {"strike": strike, "bid": float(r[14] or 0), "ask": float(r[12] or 0), "delta": float(r[16] or 0)}


def find_strike_at_delta(chain_rows, target_delta, option_side):
    """Find strike closest to target delta."""
    best = None
    best_diff = 999
    for r in chain_rows:
        parsed = _parse_row(r, option_side)
        delta = parsed["delta"]
        bid = parsed["bid"]
        if delta is None or bid is None or bid <= 0:
            continue
        diff = abs(abs(delta) - target_delta)
        if diff < best_diff:
            best_diff = diff
            best = parsed
    return best


def find_strike_price(chain_rows, strike, option_side):
    """Find exact strike price data."""
    for r in chain_rows:
        if abs(r[10] - strike) < 0.5:
            return _parse_row(r, option_side)
    return None


def find_closest_chain(chains, target_ts, after=False):
    """Find chain closest to target timestamp."""
    best = None
    best_diff = 999999
    for ch in chains:
        if after and ch["ts"] <= target_ts:
            continue
        diff = abs((ch["ts"] - target_ts).total_seconds())
        if diff < best_diff:
            best_diff = diff
            best = ch
    return best, best_diff


# ── V9-SC filter ──
def passes_v9sc(s):
    align = int(s.get("greek_alignment") or 0) if s.get("greek_alignment") is not None else 0
    is_long = s.get("direction", "").lower() in ("long", "bullish")
    setup_name = s.get("setup_name", "")
    vix = float(s.get("vix") or 0)
    overvix = float(s.get("overvix") or 0) if s.get("overvix") else None

    if is_long:
        if align < 2:
            return False
        if "Skew Charm" in setup_name:
            return True
        if vix <= 22:
            return True
        if overvix is not None and overvix >= 2:
            return True
        return False
    else:
        if "Skew Charm" in setup_name:
            return True
        if "AG Short" in setup_name:
            return True
        if "DD Exhaustion" in setup_name and align != 0:
            return True
        return False


# ── Run backtest ──
results = []
skipped = 0

for s in setups:
    if not passes_v9sc(s):
        continue

    setup_name = s["setup_name"]
    direction = s["direction"].lower()
    is_long = direction in ("long", "bullish")
    outcome = s["outcome_result"]
    pnl_pts = float(s["outcome_pnl"] or 0)
    elapsed_min = float(s["outcome_elapsed_min"] or 20)
    trade_date = str(s["trade_date"])

    # Find entry chain (closest to signal time)
    entry_chain_obj, entry_lag = find_closest_chain(chains, s["ts"])
    if not entry_chain_obj or entry_lag > 120:
        skipped += 1
        continue

    entry_rows = entry_chain_obj["rows"]

    # Naked side (same as before)
    naked_side = "call" if is_long else "put"

    # CREDIT SPREAD: use OPPOSITE side!
    credit_side = "put" if is_long else "call"

    # Find ATM (0.50 delta) on credit side
    credit_short_opt = find_strike_at_delta(entry_rows, 0.50, credit_side)
    if not credit_short_opt or credit_short_opt["bid"] <= 0:
        skipped += 1
        continue

    credit_short_strike = credit_short_opt["strike"]
    credit_short_bid = credit_short_opt["bid"]

    # Protection leg
    if credit_side == "put":
        credit_long_strike = credit_short_strike - SPREAD_WIDTH  # buy lower put
    else:
        credit_long_strike = credit_short_strike + SPREAD_WIDTH  # buy higher call

    credit_long_opt = find_strike_price(entry_rows, credit_long_strike, credit_side)
    if not credit_long_opt or not credit_long_opt["ask"] or credit_long_opt["ask"] <= 0:
        skipped += 1
        continue

    credit_long_ask = credit_long_opt["ask"]
    credit_received = credit_short_bid - credit_long_ask
    if credit_received <= 0:
        skipped += 1
        continue

    # Find exit chain
    exit_target_ts = s["ts"] + timedelta(minutes=elapsed_min)
    exit_chain_obj, exit_lag = find_closest_chain(chains, exit_target_ts, after=True)
    if not exit_chain_obj or exit_lag > 300:
        skipped += 1
        continue

    exit_rows = exit_chain_obj["rows"]

    # Exit prices
    credit_exit_short = find_strike_price(exit_rows, credit_short_strike, credit_side)
    credit_exit_long = find_strike_price(exit_rows, credit_long_strike, credit_side)
    if not credit_exit_short or not credit_exit_long:
        skipped += 1
        continue
    if credit_exit_short["ask"] is None or credit_exit_long["bid"] is None:
        skipped += 1
        continue

    # Close: buy back short at ask, sell long at bid
    c_exit_short_ask = credit_exit_short["ask"]
    c_exit_long_bid = credit_exit_long["bid"]
    credit_close_cost = c_exit_short_ask - c_exit_long_bid
    credit_pnl = (credit_received - credit_close_cost) * 100

    results.append({
        "id": s["id"],
        "date": trade_date,
        "setup": setup_name[:16],
        "dir": "L" if is_long else "S",
        "outcome": outcome,
        "spx_pnl": pnl_pts,
        "credit_side": credit_side,
        "short_strike": credit_short_strike,
        "long_strike": credit_long_strike,
        "credit_received": credit_received,
        "credit_close": credit_close_cost,
        "credit_pnl": round(credit_pnl, 2),
        "entry_str": f"{credit_short_bid:.2f}-{credit_long_ask:.2f}={credit_received:.2f}",
        "exit_str": f"{c_exit_short_ask:.2f}-{c_exit_long_bid:.2f}={credit_close_cost:.2f}",
    })

print(f"\nProcessed: {len(results)} trades, skipped: {skipped}")
print(f"{'=' * 110}")
print(f"CREDIT SPREAD — CORRECTED DIRECTION (close at setup resolution, real bid/ask)")
print(f"{'=' * 110}")

# ── Daily summary ──
total_pnl = 0
wins = 0
losses = 0
daily_pnl = defaultdict(float)
daily_count = defaultdict(int)
setup_stats = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0})

for t in results:
    pnl = t["credit_pnl"]
    total_pnl += pnl
    daily_pnl[t["date"]] += pnl
    daily_count[t["date"]] += 1
    setup_stats[t["setup"]]["pnl"] += pnl
    setup_stats[t["setup"]]["count"] += 1
    if pnl >= 0:
        wins += 1
        setup_stats[t["setup"]]["w"] += 1
    else:
        losses += 1
        setup_stats[t["setup"]]["l"] += 1

wr = wins / len(results) * 100 if results else 0
print(f"\nTrades: {len(results)}  |  Wins: {wins}  |  Losses: {losses}  |  WR: {wr:.0f}%")
print(f"SPXW PnL: ${total_pnl:>+,.0f}  |  SPY equiv: ${total_pnl/10:>+,.0f}")

w_pnl = sum(t["credit_pnl"] for t in results if t["credit_pnl"] >= 0)
l_pnl = sum(t["credit_pnl"] for t in results if t["credit_pnl"] < 0)
avg_w = w_pnl / max(1, wins)
avg_l = l_pnl / max(1, losses)
ndays = len(set(t["date"] for t in results))
print(f"Avg WIN: ${avg_w:>+,.0f}  |  Avg LOSS: ${avg_l:>+,.0f}  |  Ratio: {abs(avg_w/avg_l) if avg_l else 999:.2f}x")
print(f"Per day: ${total_pnl/ndays:>+,.0f} SPXW  |  ${total_pnl/10/ndays:>+,.0f} SPY")
print(f"Monthly (21d): ${total_pnl/ndays*21:>+,.0f} SPXW  |  ${total_pnl/10/ndays*21:>+,.0f} SPY")

# ── Daily breakdown ──
print(f"\n{'Date':<12} {'Trades':>6} {'PnL':>10} {'Cumul':>10}")
print("-" * 45)
cum = 0
for date in sorted(daily_pnl.keys()):
    cum += daily_pnl[date]
    print(f"{date:<12} {daily_count[date]:>6} ${daily_pnl[date]:>+9,.0f} ${cum:>+9,.0f}")
print(f"{'TOTAL':<12} {len(results):>6} ${total_pnl:>+9,.0f}")

# ── Per-setup ──
print(f"\n{'Setup':<20} {'Trades':>6} {'W/L':>8} {'WR':>6} {'PnL':>10}")
print("-" * 55)
for s in sorted(setup_stats.keys()):
    st = setup_stats[s]
    swr = st["w"] / st["count"] * 100 if st["count"] else 0
    print(f"  {s:<18} {st['count']:>6} {st['w']}W/{st['l']}L {swr:>5.0f}% ${st['pnl']:>+9,.0f}")

# ── Sample trades ──
print(f"\n{'=' * 110}")
print("SAMPLE TRADES (first 15)")
print(f"{'=' * 110}")
for t in results[:15]:
    print(f"  #{t['id']:<5} {t['date']} {t['setup']:<16} {t['dir']} {t['outcome']:<5} SPX{t['spx_pnl']:>+6.1f} "
          f"| {t['credit_side']} {t['short_strike']:.0f}-{t['long_strike']:.0f} "
          f"| entry {t['entry_str']} > exit {t['exit_str']} "
          f"| PnL ${t['credit_pnl']:>+,.0f}")
