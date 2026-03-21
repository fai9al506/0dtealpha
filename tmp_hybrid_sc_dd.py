"""Hybrid: SC on MES futures + DD on SPXW credit spreads.
SC: single-pos MES (10 contracts, $5/pt). Trail captures big moves.
DD: credit spread per signal (no pos limit). All signals traded concurrently."""
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
               (ts AT TIME ZONE 'America/New_York')::date as trade_date,
               outcome_max_profit
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
COMM_CREDIT = 4 * 0.65  # 4 legs per credit spread
COMM_MES = 2 * 0.62 * 10  # round-trip × 10 MES contracts
MES_PER_PT = 5.0 * 10  # $5/pt × 10 contracts = $50/pt

# ═══════════════════════════════════════════════════
# DD: credit spreads (all signals, concurrent)
# ═══════════════════════════════════════════════════
dd_trades = []
for s in setups:
    if s["setup_name"] != "DD Exhaustion": continue
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

    dd_trades.append({
        "date": str(s["trade_date"]), "dir": "L" if is_long else "S",
        "outcome": s["outcome_result"], "spx_pnl": float(s["outcome_pnl"] or 0),
        "pnl": round(pnl, 2),
    })

# ═══════════════════════════════════════════════════
# SC: MES futures (single-pos simulation)
# ═══════════════════════════════════════════════════
sc_all = []
for s in setups:
    if s["setup_name"] != "Skew Charm": continue
    if not passes_v9sc(s): continue
    sc_all.append({
        "date": str(s["trade_date"]),
        "dir": "L" if s["direction"].lower() in ("long", "bullish") else "S",
        "outcome": s["outcome_result"],
        "spx_pnl": float(s["outcome_pnl"] or 0),
        "ts": s["ts"],
        "elapsed": float(s["outcome_elapsed_min"] or 20),
    })

# Simulate single-pos: only 1 SC trade at a time
sc_trades = []
current_end = None
for t in sc_all:
    if current_end and t["ts"] < current_end:
        continue  # skip, already in position
    sc_trades.append(t)
    t["mes_pnl"] = t["spx_pnl"] * MES_PER_PT  # $50/pt
    current_end = t["ts"] + timedelta(minutes=t["elapsed"])

# ═══════════════════════════════════════════════════
# SC: all signals (no pos limit, theoretical max)
# ═══════════════════════════════════════════════════
sc_all_pnl = []
for t in sc_all:
    t["mes_pnl"] = t["spx_pnl"] * MES_PER_PT
    sc_all_pnl.append(t)

# ═══════════════════════════════════════════════════
# COMBINED DAILY P&L
# ═══════════════════════════════════════════════════
dates = sorted(set(
    [t["date"] for t in dd_trades] +
    [t["date"] for t in sc_trades]
))

print(f"HYBRID STRATEGY: SC on MES (single-pos) + DD on SPXW credit spread (all signals)")
print(f"Period: Mar 1-18, 2026 | Real bid/ask | 10 MES + 1 SPXW per signal")
print(f"SC trades: {len(sc_trades)} (single-pos) of {len(sc_all)} total | DD trades: {len(dd_trades)} (all)")
print()

# ── Table 1: SC MES single-pos ──
print(f"{'='*80}")
print(f"SC on MES (10 MES, single-pos)")
print(f"{'='*80}")
sc_daily = defaultdict(lambda: {"pnl": 0, "n": 0, "w": 0})
for t in sc_trades:
    sc_daily[t["date"]]["pnl"] += t["mes_pnl"]
    sc_daily[t["date"]]["n"] += 1
    if t["mes_pnl"] > 0: sc_daily[t["date"]]["w"] += 1

print(f"{'Date':<11} {'#':>3} {'Gross $':>9} {'Comm':>6} {'Net $':>9} {'Cum $':>9}")
print("-" * 50)
sc_cum = 0; sc_total = 0; sc_comm_total = 0
for d in dates:
    dd_ = sc_daily.get(d, {"pnl": 0, "n": 0})
    comm = dd_["n"] * COMM_MES
    net = dd_["pnl"] - comm
    sc_cum += net
    sc_total += net
    sc_comm_total += comm
    if dd_["n"] > 0:
        print(f"{d:<11} {dd_['n']:>3} ${dd_['pnl']:>+8,.0f} ${comm:>5,.0f} ${net:>+8,.0f} ${sc_cum:>+8,.0f}")
sc_w = sum(1 for t in sc_trades if t["mes_pnl"] > 0)
print(f"{'TOTAL':<11} {len(sc_trades):>3} ${sum(t['mes_pnl'] for t in sc_trades):>+8,.0f} ${sc_comm_total:>5,.0f} ${sc_total:>+8,.0f}")
print(f"WR: {sc_w}/{len(sc_trades)} = {sc_w/len(sc_trades)*100:.0f}%")

# ── Table 2: DD credit spreads ──
print(f"\n{'='*80}")
print(f"DD on SPXW credit spread ($10-wide, 0.50 delta, all signals)")
print(f"{'='*80}")
dd_daily = defaultdict(lambda: {"pnl": 0, "n": 0})
for t in dd_trades:
    dd_daily[t["date"]]["pnl"] += t["pnl"]
    dd_daily[t["date"]]["n"] += 1

print(f"{'Date':<11} {'#':>3} {'Gross $':>9} {'Comm':>6} {'Net $':>9} {'Cum $':>9}")
print("-" * 50)
dd_cum = 0; dd_total_net = 0; dd_comm_total = 0
for d in dates:
    dd_ = dd_daily.get(d, {"pnl": 0, "n": 0})
    comm = dd_["n"] * COMM_CREDIT
    net = dd_["pnl"] - comm
    dd_cum += net
    dd_total_net += net
    dd_comm_total += comm
    if dd_["n"] > 0:
        print(f"{d:<11} {dd_['n']:>3} ${dd_['pnl']:>+8,.0f} ${comm:>5,.0f} ${net:>+8,.0f} ${dd_cum:>+8,.0f}")
dd_w = sum(1 for t in dd_trades if t["pnl"] >= 0)
dd_gross = sum(t["pnl"] for t in dd_trades)
print(f"{'TOTAL':<11} {len(dd_trades):>3} ${dd_gross:>+8,.0f} ${dd_comm_total:>5,.0f} ${dd_total_net:>+8,.0f}")
print(f"WR: {dd_w}/{len(dd_trades)} = {dd_w/len(dd_trades)*100:.0f}%")

# ── Table 3: COMBINED ──
print(f"\n{'='*80}")
print(f"COMBINED: SC MES + DD Credit Spread")
print(f"{'='*80}")
print(f"{'Date':<11} {'SC $':>9} {'DD $':>9} {'Total $':>9} {'Cum $':>9}")
print("-" * 52)
combo_cum = 0
combo_total = 0
peak = 0; max_dd = 0
pos_days = 0
for d in dates:
    sc_net = sc_daily.get(d, {"pnl": 0, "n": 0})["pnl"] - sc_daily.get(d, {"pnl": 0, "n": 0})["n"] * COMM_MES
    dd_net = dd_daily.get(d, {"pnl": 0, "n": 0})["pnl"] - dd_daily.get(d, {"pnl": 0, "n": 0})["n"] * COMM_CREDIT
    day_total = sc_net + dd_net
    combo_cum += day_total
    combo_total += day_total
    peak = max(peak, combo_cum)
    max_dd = max(max_dd, peak - combo_cum)
    if day_total > 0: pos_days += 1
    print(f"{d:<11} ${sc_net:>+8,.0f} ${dd_net:>+8,.0f} ${day_total:>+8,.0f} ${combo_cum:>+8,.0f}")

ndays = len(dates)
print(f"\n--- COMBINED SUMMARY ---")
print(f"SC MES net:       ${sc_total:>+,.0f}")
print(f"DD credit net:    ${dd_total_net:>+,.0f}")
print(f"COMBINED net:     ${combo_total:>+,.0f}")
print(f"Per day:          ${combo_total/ndays:>+,.0f}")
print(f"Monthly (21d):    ${combo_total/ndays*21:>+,.0f}")
print(f"Max drawdown:     ${max_dd:>,.0f}")
print(f"Positive days:    {pos_days}/{ndays} ({pos_days/ndays*100:.0f}%)")
print(f"Recovery factor:  {abs(combo_total/max_dd):.1f}x" if max_dd > 0 else "")

# ── What-if: SC all signals (no single-pos limit) ──
print(f"\n{'='*80}")
print(f"WHAT-IF: SC ALL signals on MES (no single-pos limit)")
print(f"{'='*80}")
sc_all_daily = defaultdict(lambda: {"pnl": 0, "n": 0})
for t in sc_all_pnl:
    sc_all_daily[t["date"]]["pnl"] += t["mes_pnl"]
    sc_all_daily[t["date"]]["n"] += 1

sc_all_total = sum(t["mes_pnl"] for t in sc_all_pnl) - len(sc_all_pnl) * COMM_MES
sc_all_cum = 0
print(f"{'Date':<11} {'#':>3} {'SC All $':>9} {'DD $':>9} {'Total $':>9} {'Cum $':>9}")
print("-" * 58)
combo2_total = 0; peak2 = 0; max_dd2 = 0; pos2 = 0
for d in dates:
    sc_n = sc_all_daily.get(d, {"pnl": 0, "n": 0})
    sc_net = sc_n["pnl"] - sc_n["n"] * COMM_MES
    dd_net = dd_daily.get(d, {"pnl": 0, "n": 0})["pnl"] - dd_daily.get(d, {"pnl": 0, "n": 0})["n"] * COMM_CREDIT
    day_t = sc_net + dd_net
    combo2_total += day_t
    peak2 = max(peak2, combo2_total)
    max_dd2 = max(max_dd2, peak2 - combo2_total)
    if day_t > 0: pos2 += 1
    print(f"{d:<11} {sc_n['n']:>3} ${sc_net:>+8,.0f} ${dd_net:>+8,.0f} ${day_t:>+8,.0f} ${combo2_total:>+8,.0f}")

print(f"\nSC all-signals + DD credit combined: ${combo2_total:>+,.0f}")
print(f"Per day: ${combo2_total/ndays:>+,.0f} | Monthly: ${combo2_total/ndays*21:>+,.0f}")
print(f"Max DD: ${max_dd2:>,.0f} | Positive days: {pos2}/{ndays}")
