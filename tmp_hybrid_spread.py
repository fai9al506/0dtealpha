"""Hybrid spread: DEBIT for SC (buy the direction), CREDIT for DD+AG (sell premium).
SC wins have big moves → debit spreads capture them.
DD/AG have steady win rate → credit spreads collect theta."""
import sqlalchemy, json
from sqlalchemy import text
from collections import defaultdict
from datetime import timedelta

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine = sqlalchemy.create_engine(DB_URL)

with engine.begin() as conn:
    setups = conn.execute(text("""
        SELECT id, setup_name, direction, spot, outcome_result, outcome_pnl,
               outcome_elapsed_min, outcome_max_profit, outcome_max_loss,
               ts, vix, overvix, greek_alignment,
               (ts AT TIME ZONE 'America/New_York')::date as trade_date
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND ts >= '2026-03-01' AND ts < '2026-03-19'
        ORDER BY id
    """)).mappings().all()

    chains_raw = conn.execute(text("""
        SELECT ts, rows FROM chain_snapshots
        WHERE ts >= '2026-03-01' AND ts < '2026-03-19'
        ORDER BY ts
    """)).fetchall()

chains = []
for row in chains_raw:
    rd = row[1] if isinstance(row[1], list) else json.loads(row[1]) if row[1] else []
    if rd: chains.append({"ts": row[0], "rows": rd})

print(f"Setups: {len(setups)}, Chains: {len(chains)}")

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


def run_credit(s, entry_ch, exit_ch):
    """Credit spread: sell ATM on OPPOSITE side, buy protection $10 away."""
    is_long = s["direction"].lower() in ("long", "bullish")
    credit_side = "put" if is_long else "call"

    short_opt = find_delta(entry_ch["rows"], 0.50, credit_side)
    if not short_opt or short_opt["bid"] <= 0: return None

    sk = short_opt["strike"]
    long_sk = sk - WIDTH if credit_side == "put" else sk + WIDTH
    long_opt = find_strike(entry_ch["rows"], long_sk, credit_side)
    if not long_opt or long_opt["ask"] <= 0: return None

    credit = short_opt["bid"] - long_opt["ask"]
    if credit <= 0: return None

    xs = find_strike(exit_ch["rows"], sk, credit_side)
    xl = find_strike(exit_ch["rows"], long_sk, credit_side)
    if not xs or not xl or xs["ask"] is None or xl["bid"] is None: return None

    close_cost = xs["ask"] - xl["bid"]
    pnl = (credit - close_cost) * 100
    return {"pnl": round(pnl, 2), "type": "CREDIT", "entry": credit, "exit": close_cost}


def run_debit(s, entry_ch, exit_ch):
    """Debit spread: buy ATM on SAME side, sell $10 OTM."""
    is_long = s["direction"].lower() in ("long", "bullish")
    debit_side = "call" if is_long else "put"

    # Buy the 0.50 delta (ATM) leg
    long_opt = find_delta(entry_ch["rows"], 0.50, debit_side)
    if not long_opt or long_opt["ask"] <= 0: return None

    long_sk = long_opt["strike"]
    # Sell the OTM leg ($10 further OTM)
    short_sk = long_sk + WIDTH if debit_side == "call" else long_sk - WIDTH
    short_opt = find_strike(entry_ch["rows"], short_sk, debit_side)
    if not short_opt or short_opt["bid"] <= 0: return None

    debit = long_opt["ask"] - short_opt["bid"]
    if debit <= 0 or debit >= WIDTH: return None

    # Exit: sell long at bid, buy back short at ask
    xl = find_strike(exit_ch["rows"], long_sk, debit_side)
    xs = find_strike(exit_ch["rows"], short_sk, debit_side)
    if not xl or not xs or xl["bid"] is None or xs["ask"] is None: return None

    exit_val = xl["bid"] - xs["ask"]
    pnl = (exit_val - debit) * 100
    return {"pnl": round(pnl, 2), "type": "DEBIT", "entry": debit, "exit": exit_val}


# ── Run all strategies ──
strategies = {
    "A: All Credit":        lambda sn: "credit",
    "B: SC Debit + Rest Credit": lambda sn: "debit" if "Skew Charm" in sn else "credit",
    "C: All Debit":         lambda sn: "debit",
    "D: SC-Short Credit + SC-Long Debit + Rest Credit": lambda sn, d="": "debit" if "Skew Charm" in sn and d in ("long","bullish") else "credit",
}

for strat_name, strat_fn in strategies.items():
    results = []
    skipped = 0

    for s in setups:
        if not passes_v9sc(s): continue
        sn = s["setup_name"]
        direction = s["direction"].lower()
        is_long = direction in ("long", "bullish")

        entry_ch, elag = find_chain(chains, s["ts"])
        if not entry_ch or elag > 120: skipped += 1; continue

        elapsed = float(s["outcome_elapsed_min"] or 20)
        exit_ch, xlag = find_chain(chains, s["ts"] + timedelta(minutes=elapsed), after=True)
        if not exit_ch or xlag > 300: skipped += 1; continue

        # Pick strategy
        if strat_name.startswith("D:"):
            spread_type = strat_fn(sn, direction)
        else:
            spread_type = strat_fn(sn)

        if spread_type == "credit":
            r = run_credit(s, entry_ch, exit_ch)
        else:
            r = run_debit(s, entry_ch, exit_ch)

        if r is None: skipped += 1; continue

        results.append({
            "id": s["id"], "date": str(s["trade_date"]),
            "setup": sn[:16], "dir": "L" if is_long else "S",
            "outcome": s["outcome_result"],
            "spx_pnl": float(s["outcome_pnl"] or 0),
            "pnl": r["pnl"], "spread_type": r["type"],
            "entry_cost": r["entry"], "exit_val": r["exit"],
        })

    total = sum(t["pnl"] for t in results)
    wins = sum(1 for t in results if t["pnl"] >= 0)
    losses = len(results) - wins
    wr = wins / len(results) * 100 if results else 0
    ndays = len(set(t["date"] for t in results))
    daily_avg = total / ndays if ndays else 0

    w_pnl = [t["pnl"] for t in results if t["pnl"] >= 0]
    l_pnl = [t["pnl"] for t in results if t["pnl"] < 0]
    pf = abs(sum(w_pnl) / sum(l_pnl)) if l_pnl and sum(l_pnl) != 0 else 999

    print(f"\n{'='*110}")
    print(f"STRATEGY {strat_name}")
    print(f"{'='*110}")
    print(f"Trades: {len(results)}  |  {wins}W/{losses}L  |  WR: {wr:.0f}%  |  PF: {pf:.2f}")
    print(f"Total: ${total:>+,.0f} SPXW  |  ${total/10:>+,.0f} SPY")
    print(f"Per day: ${daily_avg:>+,.0f} SPXW  |  ${daily_avg/10:>+,.0f} SPY")
    print(f"Monthly: ${daily_avg*21:>+,.0f} SPXW  |  ${daily_avg/10*21:>+,.0f} SPY")

    # Daily
    daily = defaultdict(lambda: {"pnl": 0, "n": 0})
    for t in results:
        daily[t["date"]]["pnl"] += t["pnl"]
        daily[t["date"]]["n"] += 1

    print(f"\n{'Date':<12} {'#':>3} {'SPXW':>9} {'SPY':>9} {'SPXW Cum':>9} {'SPY Cum':>9}")
    print("-" * 58)
    cum = 0; peak = 0; max_dd = 0
    for d in sorted(daily.keys()):
        cum += daily[d]["pnl"]
        peak = max(peak, cum); max_dd = max(max_dd, peak - cum)
        print(f"{d:<12} {daily[d]['n']:>3} ${daily[d]['pnl']:>+8,.0f} ${daily[d]['pnl']/10:>+8,.0f} ${cum:>+8,.0f} ${cum/10:>+8,.0f}")
    print(f"Max DD: ${max_dd:>,.0f} SPXW  |  ${max_dd/10:>,.0f} SPY")

    # Per-setup
    ss = defaultdict(lambda: {"pnl": 0, "n": 0, "w": 0, "l": 0})
    for t in results:
        key = f"{t['setup']} [{t['spread_type'][0]}]"
        ss[key]["pnl"] += t["pnl"]
        ss[key]["n"] += 1
        if t["pnl"] >= 0: ss[key]["w"] += 1
        else: ss[key]["l"] += 1

    print(f"\n{'Setup [Type]':<28} {'#':>4} {'W/L':>8} {'WR':>5} {'SPXW':>9} {'SPY':>9}")
    print("-" * 68)
    for s in sorted(ss.keys()):
        st = ss[s]
        print(f"  {s:<26} {st['n']:>4} {st['w']}W/{st['l']}L {st['w']/st['n']*100:>4.0f}% ${st['pnl']:>+8,.0f} ${st['pnl']/10:>+8,.0f}")
