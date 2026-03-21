"""Credit spread backtest — CORRECTED direction + multiple delta levels.
Tests 0.50 (ATM), 0.55, 0.60, 0.65 delta for the short leg."""
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

for target_delta in [0.50, 0.55, 0.60, 0.65]:
    results = []
    credits_collected = []
    skipped = 0

    for s in setups:
        if not passes_v9sc(s): continue
        is_long = s["direction"].lower() in ("long", "bullish")
        credit_side = "put" if is_long else "call"

        entry_ch, elag = find_chain(chains, s["ts"])
        if not entry_ch or elag > 120: skipped += 1; continue

        short_opt = find_delta(entry_ch["rows"], target_delta, credit_side)
        if not short_opt or short_opt["bid"] <= 0: skipped += 1; continue

        sk = short_opt["strike"]
        long_sk = sk - WIDTH if credit_side == "put" else sk + WIDTH
        long_opt = find_strike(entry_ch["rows"], long_sk, credit_side)
        if not long_opt or long_opt["ask"] <= 0: skipped += 1; continue

        credit = short_opt["bid"] - long_opt["ask"]
        if credit <= 0: skipped += 1; continue
        credits_collected.append(credit)

        elapsed = float(s["outcome_elapsed_min"] or 20)
        exit_ch, xlag = find_chain(chains, s["ts"] + timedelta(minutes=elapsed), after=True)
        if not exit_ch or xlag > 300: skipped += 1; continue

        xs = find_strike(exit_ch["rows"], sk, credit_side)
        xl = find_strike(exit_ch["rows"], long_sk, credit_side)
        if not xs or not xl or xs["ask"] is None or xl["bid"] is None: skipped += 1; continue

        close_cost = xs["ask"] - xl["bid"]
        pnl = (credit - close_cost) * 100

        results.append({
            "id": s["id"], "date": str(s["trade_date"]),
            "setup": s["setup_name"][:16], "dir": "L" if is_long else "S",
            "outcome": s["outcome_result"], "spx_pnl": float(s["outcome_pnl"] or 0),
            "credit": credit, "close": close_cost, "pnl": round(pnl, 2),
        })

    total = sum(t["pnl"] for t in results)
    wins = sum(1 for t in results if t["pnl"] >= 0)
    losses = len(results) - wins
    wr = wins / len(results) * 100 if results else 0
    ndays = len(set(t["date"] for t in results))
    daily_spxw = total / ndays if ndays else 0
    avg_credit = sum(credits_collected) / len(credits_collected) if credits_collected else 0
    credit_pct = avg_credit / WIDTH * 100

    w_pnl = [t["pnl"] for t in results if t["pnl"] >= 0]
    l_pnl = [t["pnl"] for t in results if t["pnl"] < 0]
    avg_w = sum(w_pnl) / len(w_pnl) if w_pnl else 0
    avg_l = sum(l_pnl) / len(l_pnl) if l_pnl else 0
    pf = abs(sum(w_pnl) / sum(l_pnl)) if l_pnl and sum(l_pnl) != 0 else 999

    print(f"\n{'=' * 110}")
    print(f"DELTA {target_delta:.2f} | $10-wide SPXW | Avg Credit: ${avg_credit:.2f} ({credit_pct:.0f}% of width) | Risk: ${WIDTH-avg_credit:.2f}")
    print(f"{'=' * 110}")
    print(f"Trades: {len(results)}  |  {wins}W/{losses}L  |  WR: {wr:.0f}%  |  PF: {pf:.2f}  |  Skipped: {skipped}")
    print(f"Avg WIN: ${avg_w:>+,.0f}  |  Avg LOSS: ${avg_l:>+,.0f}  |  Ratio: {abs(avg_w/avg_l) if avg_l else 0:.2f}x")
    print(f"Total: ${total:>+,.0f} SPXW  |  ${total/10:>+,.0f} SPY")
    print(f"Per day: ${daily_spxw:>+,.0f} SPXW  |  ${daily_spxw/10:>+,.0f} SPY")
    print(f"Monthly: ${daily_spxw*21:>+,.0f} SPXW  |  ${daily_spxw/10*21:>+,.0f} SPY")

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
        ss[t["setup"]]["pnl"] += t["pnl"]
        ss[t["setup"]]["n"] += 1
        if t["pnl"] >= 0: ss[t["setup"]]["w"] += 1
        else: ss[t["setup"]]["l"] += 1
    print(f"\n{'Setup':<18} {'#':>4} {'W/L':>8} {'WR':>5} {'SPXW':>9} {'SPY':>9}")
    print("-" * 58)
    for s in sorted(ss.keys()):
        st = ss[s]
        print(f"  {s:<16} {st['n']:>4} {st['w']}W/{st['l']}L {st['w']/st['n']*100:>4.0f}% ${st['pnl']:>+8,.0f} ${st['pnl']/10:>+8,.0f}")
