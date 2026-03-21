"""Credit spread backtest — CORRECTED direction.
Shows: $10-wide SPXW + $2-wide SPY. Daily PnL for both. SC deep-dive."""
import sqlalchemy, json
from sqlalchemy import text
from collections import defaultdict
from datetime import timedelta

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine = sqlalchemy.create_engine(DB_URL)

# ── Load data ──
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
        SELECT ts, rows
        FROM chain_snapshots
        WHERE ts >= '2026-03-01' AND ts < '2026-03-19'
        ORDER BY ts
    """)).fetchall()

chains = []
for row in chains_raw:
    rows_data = row[1] if isinstance(row[1], list) else json.loads(row[1]) if row[1] else []
    if rows_data:
        chains.append({"ts": row[0], "rows": rows_data})

print(f"Setups: {len(setups)}, Chains: {len(chains)}")

# Chain row: [call_vol,oi,iv,gamma,delta,bid,bid_qty,ask,ask_qty,last, STRIKE, put_last,ask,ask_qty,bid,bid_qty,delta,gamma,iv,oi,vol]
def _parse(r, side):
    s = float(r[10] or 0)
    if side == "call":
        return {"strike": s, "bid": float(r[5] or 0), "ask": float(r[7] or 0), "delta": float(r[4] or 0)}
    return {"strike": s, "bid": float(r[14] or 0), "ask": float(r[12] or 0), "delta": float(r[16] or 0)}

def find_delta(rows, target_d, side):
    best, best_diff = None, 999
    for r in rows:
        p = _parse(r, side)
        if p["bid"] <= 0: continue
        d = abs(abs(p["delta"]) - target_d)
        if d < best_diff: best_diff, best = d, p
    return best

def find_strike(rows, strike, side):
    for r in rows:
        if abs(float(r[10] or 0) - strike) < 0.5:
            return _parse(r, side)
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
        if "Skew Charm" in sn: return True
        if "AG Short" in sn: return True
        if "DD Exhaustion" in sn and align != 0: return True
        return False


def run_backtest(spread_width, label):
    results = []
    skipped = 0
    for s in setups:
        if not passes_v9sc(s): continue
        is_long = s["direction"].lower() in ("long", "bullish")
        credit_side = "put" if is_long else "call"

        entry_ch, elag = find_chain(chains, s["ts"])
        if not entry_ch or elag > 120: skipped += 1; continue

        short_opt = find_delta(entry_ch["rows"], 0.50, credit_side)
        if not short_opt or short_opt["bid"] <= 0: skipped += 1; continue

        sk = short_opt["strike"]
        long_sk = sk - spread_width if credit_side == "put" else sk + spread_width
        long_opt = find_strike(entry_ch["rows"], long_sk, credit_side)
        if not long_opt or long_opt["ask"] <= 0: skipped += 1; continue

        credit = short_opt["bid"] - long_opt["ask"]
        if credit <= 0: skipped += 1; continue

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
            "short_k": sk, "long_k": long_sk,
        })

    return results, skipped


# ── Run both widths ──
for width, tag in [(10.0, "SPXW $10-wide"), (5.0, "SPXW $5-wide")]:
    results, skipped = run_backtest(width, tag)
    spy_div = 10  # SPY ≈ SPXW / 10 (regardless of spread width)

    total = sum(t["pnl"] for t in results)
    wins = sum(1 for t in results if t["pnl"] >= 0)
    losses = len(results) - wins
    wr = wins / len(results) * 100 if results else 0
    ndays = len(set(t["date"] for t in results))
    daily_spxw = total / ndays if ndays else 0

    w_pnl = [t["pnl"] for t in results if t["pnl"] >= 0]
    l_pnl = [t["pnl"] for t in results if t["pnl"] < 0]
    avg_w = sum(w_pnl) / len(w_pnl) if w_pnl else 0
    avg_l = sum(l_pnl) / len(l_pnl) if l_pnl else 0
    pf = abs(sum(w_pnl) / sum(l_pnl)) if l_pnl and sum(l_pnl) != 0 else 999

    print(f"\n{'=' * 100}")
    print(f"CREDIT SPREAD — {tag} — CORRECTED DIRECTION (close at setup resolution)")
    print(f"{'=' * 100}")
    print(f"Trades: {len(results)}  |  Wins: {wins}  |  Losses: {losses}  |  WR: {wr:.0f}%  |  PF: {pf:.2f}  |  Skipped: {skipped}")
    print(f"Total PnL: ${total:>+,.0f}  |  SPY equiv: ${total/spy_div:>+,.0f}")
    print(f"Avg WIN: ${avg_w:>+,.0f}  |  Avg LOSS: ${avg_l:>+,.0f}  |  Ratio: {abs(avg_w/avg_l) if avg_l else 0:.2f}x")
    print(f"Per day: ${daily_spxw:>+,.0f}  |  SPY/day: ${daily_spxw/spy_div:>+,.0f}")
    print(f"Monthly (21d): ${daily_spxw*21:>+,.0f}  |  SPY monthly: ${daily_spxw/spy_div*21:>+,.0f}")

    # ── Daily PnL ──
    daily = defaultdict(lambda: {"pnl": 0, "count": 0})
    for t in results:
        daily[t["date"]]["pnl"] += t["pnl"]
        daily[t["date"]]["count"] += 1

    print(f"\n{'Date':<12} {'#':>4} {'SPXW PnL':>10} {'SPY PnL':>10} {'SPXW Cum':>10} {'SPY Cum':>10}")
    print("-" * 60)
    cum = 0
    max_dd = 0; peak = 0
    for d in sorted(daily.keys()):
        cum += daily[d]["pnl"]
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
        print(f"{d:<12} {daily[d]['count']:>4} ${daily[d]['pnl']:>+9,.0f} ${daily[d]['pnl']/spy_div:>+9,.0f} ${cum:>+9,.0f} ${cum/spy_div:>+9,.0f}")
    print(f"{'TOTAL':<12} {len(results):>4} ${total:>+9,.0f} ${total/spy_div:>+9,.0f}")
    print(f"Max Drawdown: ${max_dd:>,.0f} SPXW  |  ${max_dd/spy_div:>,.0f} SPY")

    # ── Per-Setup ──
    setup_stats = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0})
    for t in results:
        setup_stats[t["setup"]]["pnl"] += t["pnl"]
        setup_stats[t["setup"]]["count"] += 1
        if t["pnl"] >= 0: setup_stats[t["setup"]]["w"] += 1
        else: setup_stats[t["setup"]]["l"] += 1

    print(f"\n{'Setup':<20} {'#':>4} {'W/L':>8} {'WR':>5} {'SPXW PnL':>10} {'SPY PnL':>10}")
    print("-" * 62)
    for s in sorted(setup_stats.keys()):
        st = setup_stats[s]
        swr = st["w"] / st["count"] * 100
        print(f"  {s:<18} {st['count']:>4} {st['w']}W/{st['l']}L {swr:>4.0f}% ${st['pnl']:>+9,.0f} ${st['pnl']/spy_div:>+9,.0f}")

    # ── SC Deep Dive ──
    sc_trades = [t for t in results if "Skew Charm" in t["setup"]]
    if sc_trades:
        print(f"\n{'=' * 100}")
        print(f"SKEW CHARM DEEP DIVE — {tag}")
        print(f"{'=' * 100}")

        sc_total = sum(t["pnl"] for t in sc_trades)
        sc_wins = sum(1 for t in sc_trades if t["pnl"] >= 0)
        sc_losses = len(sc_trades) - sc_wins
        sc_wr = sc_wins / len(sc_trades) * 100

        print(f"SC Trades: {len(sc_trades)}  |  {sc_wins}W/{sc_losses}L  |  WR: {sc_wr:.0f}%  |  PnL: ${sc_total:>+,.0f} SPXW  |  ${sc_total/spy_div:>+,.0f} SPY")

        # By direction
        for dname, dval in [("LONG", "L"), ("SHORT", "S")]:
            dt = [t for t in sc_trades if t["dir"] == dval]
            if not dt: continue
            dp = sum(t["pnl"] for t in dt)
            dw = sum(1 for t in dt if t["pnl"] >= 0)
            dwr = dw / len(dt) * 100
            wlist = [t["pnl"] for t in dt if t["pnl"] >= 0]
            llist = [t["pnl"] for t in dt if t["pnl"] < 0]
            aw = sum(wlist)/len(wlist) if wlist else 0
            al = sum(llist)/len(llist) if llist else 0
            print(f"  SC {dname}: {len(dt)}t  |  {dw}W/{len(dt)-dw}L  |  WR: {dwr:.0f}%  |  PnL: ${dp:>+,.0f} SPXW  |  ${dp/spy_div:>+,.0f} SPY  |  avgW: ${aw:>+,.0f}  avgL: ${al:>+,.0f}")

        # By SPX PnL magnitude (how big was the favorable/adverse move?)
        print(f"\n  SC by SPX move size:")
        for lo, hi, label in [(0, 5, "0-5 pts"), (5, 10, "5-10 pts"), (10, 15, "10-15 pts"), (15, 999, "15+ pts")]:
            # WINS by magnitude
            wt = [t for t in sc_trades if t["outcome"] == "WIN" and lo <= abs(t["spx_pnl"]) < hi]
            if wt:
                wp = sum(t["pnl"] for t in wt)
                print(f"    WIN {label}: {len(wt)}t  |  avg credit PnL: ${wp/len(wt):>+,.0f}  |  total: ${wp:>+,.0f}")
            # LOSSES by magnitude
            lt = [t for t in sc_trades if t["outcome"] != "WIN" and lo <= abs(t["spx_pnl"]) < hi]
            if lt:
                lp = sum(t["pnl"] for t in lt)
                print(f"    LOSS {label}: {len(lt)}t  |  avg credit PnL: ${lp/len(lt):>+,.0f}  |  total: ${lp:>+,.0f}")

        # SC daily
        sc_daily = defaultdict(float)
        for t in sc_trades:
            sc_daily[t["date"]] += t["pnl"]
        print(f"\n  SC Daily:")
        sc_cum = 0
        for d in sorted(sc_daily.keys()):
            sc_cum += sc_daily[d]
            print(f"    {d}: ${sc_daily[d]:>+7,.0f} SPXW (${sc_daily[d]/spy_div:>+7,.0f} SPY)  cum: ${sc_cum:>+7,.0f} (${sc_cum/spy_div:>+7,.0f})")

        # SC by credit size
        print(f"\n  SC by credit received:")
        for lo, hi, label in [(0, 3, "<$3"), (3, 5, "$3-5"), (5, 7, "$5-7"), (7, 99, "$7+")]:
            ct = [t for t in sc_trades if lo <= t["credit"] < hi]
            if ct:
                cp = sum(t["pnl"] for t in ct)
                cw = sum(1 for t in ct if t["pnl"] >= 0)
                print(f"    Credit {label}: {len(ct)}t  |  {cw}W/{len(ct)-cw}L  |  WR: {cw/len(ct)*100:.0f}%  |  PnL: ${cp:>+,.0f}")
