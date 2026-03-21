"""FINAL credit spread study. Mar 1-18, V9-SC filter, $10-wide SPXW, 0.50 delta.
Clean daily PnL with SPXW + SPY columns. One table."""
import sqlalchemy, json
from sqlalchemy import text
from collections import defaultdict
from datetime import timedelta

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine = sqlalchemy.create_engine(DB_URL)

with engine.begin() as conn:
    setups = conn.execute(text("""
        SELECT id, setup_name, direction, spot, outcome_result, outcome_pnl,
               outcome_elapsed_min, ts, vix, overvix, greek_alignment,
               (ts AT TIME ZONE 'America/New_York')::date as trade_date
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND ts >= '2026-03-01' AND ts < '2026-03-19'
        ORDER BY id
    """)).mappings().all()

    chains_raw = conn.execute(text("""
        SELECT ts, rows FROM chain_snapshots
        WHERE ts >= '2026-03-01' AND ts < '2026-03-19' ORDER BY ts
    """)).fetchall()

chains = []
for row in chains_raw:
    rd = row[1] if isinstance(row[1], list) else json.loads(row[1]) if row[1] else []
    if rd: chains.append({"ts": row[0], "rows": rd})

def _p(r, side):
    s = float(r[10] or 0)
    if side == "call":
        return {"strike": s, "bid": float(r[5] or 0), "ask": float(r[7] or 0), "delta": float(r[4] or 0)}
    return {"strike": s, "bid": float(r[14] or 0), "ask": float(r[12] or 0), "delta": float(r[16] or 0)}

def find_delta(rows, td, side):
    best, bd = None, 999
    for r in rows:
        p = _p(r, side)
        if p["bid"] <= 0: continue
        d = abs(abs(p["delta"]) - td)
        if d < bd: bd, best = d, p
    return best

def find_strike(rows, strike, side):
    for r in rows:
        if abs(float(r[10] or 0) - strike) < 0.5: return _p(r, side)
    return None

def find_chain(chains, ts, after=False):
    best, bd = None, 999999
    for ch in chains:
        if after and ch["ts"] <= ts: continue
        d = abs((ch["ts"] - ts).total_seconds())
        if d < bd: bd, best = d, ch
    return best, bd

def passes_v9sc(s):
    align = int(s.get("greek_alignment") or 0) if s.get("greek_alignment") is not None else 0
    is_long = s.get("direction", "").lower() in ("long", "bullish")
    sn = s.get("setup_name", "")
    vix = float(s.get("vix") or 0)
    ov = float(s.get("overvix") or 0) if s.get("overvix") else None
    if is_long:
        if align < 2: return False
        if "Skew Charm" in sn: return True
        if vix <= 22: return True
        if ov is not None and ov >= 2: return True
        return False
    else:
        if "Skew Charm" in sn or "AG Short" in sn: return True
        if "DD Exhaustion" in sn and align != 0: return True
        return False

WIDTH = 10.0
results = []

for s in setups:
    if not passes_v9sc(s): continue
    is_long = s["direction"].lower() in ("long", "bullish")
    credit_side = "put" if is_long else "call"

    entry_ch, elag = find_chain(chains, s["ts"])
    if not entry_ch or elag > 120: continue
    short_opt = find_delta(entry_ch["rows"], 0.50, credit_side)
    if not short_opt or short_opt["bid"] <= 0: continue
    sk = short_opt["strike"]
    long_sk = sk - WIDTH if credit_side == "put" else sk + WIDTH
    long_opt = find_strike(entry_ch["rows"], long_sk, credit_side)
    if not long_opt or long_opt["ask"] <= 0: continue
    credit = short_opt["bid"] - long_opt["ask"]
    if credit <= 0: continue

    elapsed = float(s["outcome_elapsed_min"] or 20)
    exit_ch, xlag = find_chain(chains, s["ts"] + timedelta(minutes=elapsed), after=True)
    if not exit_ch or xlag > 300: continue
    xs = find_strike(exit_ch["rows"], sk, credit_side)
    xl = find_strike(exit_ch["rows"], long_sk, credit_side)
    if not xs or not xl or xs["ask"] is None or xl["bid"] is None: continue

    close_cost = xs["ask"] - xl["bid"]
    pnl = (credit - close_cost) * 100

    results.append({
        "date": str(s["trade_date"]), "setup": s["setup_name"],
        "dir": "L" if is_long else "S", "pnl": round(pnl, 2),
    })

# ═══════════════════════════════════════════════════
# DAILY P&L TABLE
# ═══════════════════════════════════════════════════
daily = defaultdict(lambda: {"pnl": 0, "n": 0, "setups": defaultdict(float)})
for t in results:
    daily[t["date"]]["pnl"] += t["pnl"]
    daily[t["date"]]["n"] += 1
    daily[t["date"]]["setups"][t["setup"]] += t["pnl"]

COMM_PER_TRADE = 4 * 0.65  # 4 legs x $0.65/leg per SPXW contract

print(f"CREDIT SPREAD BACKTEST — V9-SC filter, $10-wide SPXW, 0.50 delta ATM")
print(f"Period: Mar 1–18, 2026 | {len(results)} trades | Close at setup resolution | Real bid/ask")
print()
print(f"{'Date':<11} {'#':>3} {'SPXW PnL':>10} {'Comm':>7} {'SPXW Net':>10} {'SPY Net':>9} {'SPXW Cum':>10} {'SPY Cum':>9}")
print("=" * 80)

cum_spxw = 0
cum_spy = 0
total_comm = 0

for d in sorted(daily.keys()):
    dd = daily[d]
    comm = dd["n"] * COMM_PER_TRADE
    net = dd["pnl"] - comm
    spy_net = net / 10
    cum_spxw += net
    cum_spy += spy_net
    total_comm += comm
    print(f"{d:<11} {dd['n']:>3} ${dd['pnl']:>+9,.0f} ${comm:>6,.0f} ${net:>+9,.0f} ${spy_net:>+8,.0f} ${cum_spxw:>+9,.0f} ${cum_spy:>+8,.0f}")

total_gross = sum(t["pnl"] for t in results)
total_net = total_gross - total_comm
ndays = len(daily)

print("=" * 80)
print(f"{'TOTAL':<11} {len(results):>3} ${total_gross:>+9,.0f} ${total_comm:>6,.0f} ${total_net:>+9,.0f} ${total_net/10:>+8,.0f}")

# ═══════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════
wins = sum(1 for t in results if t["pnl"] >= 0)
losses = len(results) - wins
w_pnl = sum(t["pnl"] for t in results if t["pnl"] >= 0)
l_pnl = sum(t["pnl"] for t in results if t["pnl"] < 0)

print(f"\n--- SUMMARY (1 SPXW contract per signal) ---")
print(f"Win rate:     {wins}/{len(results)} = {wins/len(results)*100:.0f}%")
print(f"Avg winner:   ${w_pnl/wins:>+,.0f} SPXW  |  ${w_pnl/wins/10:>+,.0f} SPY")
print(f"Avg loser:    ${l_pnl/losses:>+,.0f} SPXW  |  ${l_pnl/losses/10:>+,.0f} SPY")
print(f"Profit factor: {abs(w_pnl/l_pnl):.2f}")
print(f"Total gross:  ${total_gross:>+,.0f} SPXW  |  ${total_gross/10:>+,.0f} SPY")
print(f"Commissions:  ${total_comm:>,.0f} ({len(results)} trades × ${COMM_PER_TRADE:.2f})")
print(f"Total net:    ${total_net:>+,.0f} SPXW  |  ${total_net/10:>+,.0f} SPY")
print(f"Per day:      ${total_net/ndays:>+,.0f} SPXW  |  ${total_net/ndays/10:>+,.0f} SPY")
print(f"Monthly (21d): ${total_net/ndays*21:>+,.0f} SPXW  |  ${total_net/ndays/10*21:>+,.0f} SPY")

# Peak and drawdown
cum = 0; peak = 0; max_dd = 0
for d in sorted(daily.keys()):
    cum += daily[d]["pnl"] - daily[d]["n"] * COMM_PER_TRADE
    peak = max(peak, cum); max_dd = max(max_dd, peak - cum)
print(f"Max drawdown: ${max_dd:>,.0f} SPXW  |  ${max_dd/10:>,.0f} SPY")

pos_days = sum(1 for d in daily if daily[d]["pnl"] - daily[d]["n"] * COMM_PER_TRADE > 0)
print(f"Positive days: {pos_days}/{ndays} ({pos_days/ndays*100:.0f}%)")

# ═══════════════════════════════════════════════════
# PER-SETUP BREAKDOWN
# ═══════════════════════════════════════════════════
print(f"\n--- PER SETUP ---")
print(f"{'Setup':<20} {'#':>4} {'WR':>5} {'Gross':>9} {'Comm':>7} {'Net SPXW':>10} {'Net SPY':>9} {'SPY/mo':>9}")
print("-" * 75)

for sn in sorted(set(t["setup"] for t in results)):
    st = [t for t in results if t["setup"] == sn]
    gross = sum(t["pnl"] for t in st)
    comm = len(st) * COMM_PER_TRADE
    net = gross - comm
    w = sum(1 for t in st if t["pnl"] >= 0)
    nd = len(set(t["date"] for t in st))
    monthly = net / ndays * 21  # use total trading days for fair comparison
    print(f"  {sn:<18} {len(st):>4} {w/len(st)*100:>4.0f}% ${gross:>+8,.0f} ${comm:>6,.0f} ${net:>+9,.0f} ${net/10:>+8,.0f} ${monthly/10:>+8,.0f}")

# ═══════════════════════════════════════════════════
# SCALING TABLE
# ═══════════════════════════════════════════════════
daily_net = total_net / ndays
print(f"\n--- SCALING (based on ${daily_net:>+,.0f} SPXW/day net) ---")
print(f"{'Qty':>5} {'Instrument':<12} {'Daily':>9} {'Monthly':>10} {'Yearly':>12} {'Capital':>10}")
print("-" * 65)
for qty, inst, div in [(1, "SPXW", 1), (1, "SPY", 10), (2, "SPY", 10), (5, "SPY", 10), (10, "SPY", 10)]:
    d = daily_net / div * qty
    m = d * 21
    y = m * 12
    cap = WIDTH / div * 100 * qty * 1.5  # 1.5x margin buffer
    print(f"{qty:>5} {inst:<12} ${d:>+8,.0f} ${m:>+9,.0f} ${y:>+11,.0f} ${cap:>9,.0f}")
