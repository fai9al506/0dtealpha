"""Credit spread backtest — ALL setups, not just V9-SC filtered.
See which setups work best on credit spreads."""
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

WIDTH = 10.0
results = []
skipped = 0

for s in setups:
    is_long = s["direction"].lower() in ("long", "bullish")
    credit_side = "put" if is_long else "call"
    align = int(s.get("greek_alignment") or 0) if s.get("greek_alignment") is not None else 0

    entry_ch, elag = find_chain(chains, s["ts"])
    if not entry_ch or elag > 120: skipped += 1; continue

    short_opt = find_delta(entry_ch["rows"], 0.50, credit_side)
    if not short_opt or short_opt["bid"] <= 0: skipped += 1; continue

    sk = short_opt["strike"]
    long_sk = sk - WIDTH if credit_side == "put" else sk + WIDTH
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
        "setup": s["setup_name"], "dir": "L" if is_long else "S",
        "outcome": s["outcome_result"],
        "spx_pnl": float(s["outcome_pnl"] or 0),
        "align": align,
        "pnl": round(pnl, 2),
        "credit": credit,
    })

print(f"Processed: {len(results)}, skipped: {skipped}")

# ── Per-setup summary ──
print(f"\n{'='*110}")
print(f"ALL SETUPS — CREDIT SPREAD $10-wide (0.50 delta, corrected direction)")
print(f"{'='*110}")

all_setups = sorted(set(t["setup"] for t in results))
print(f"\n{'Setup':<22} {'#':>4} {'W':>4} {'L':>4} {'WR':>5} {'PF':>5} {'SPXW PnL':>10} {'SPY PnL':>9} {'AvgW':>7} {'AvgL':>7} {'SPXW/day':>9} {'SPY/mo':>9}")
print("-" * 110)

for sn in all_setups:
    st = [t for t in results if t["setup"] == sn]
    tp = sum(t["pnl"] for t in st)
    w = [t for t in st if t["pnl"] >= 0]
    l = [t for t in st if t["pnl"] < 0]
    wr = len(w) / len(st) * 100
    aw = sum(t["pnl"] for t in w) / len(w) if w else 0
    al = sum(t["pnl"] for t in l) / len(l) if l else 0
    pf = abs(sum(t["pnl"] for t in w) / sum(t["pnl"] for t in l)) if l and sum(t["pnl"] for t in l) != 0 else 999
    nd = len(set(t["date"] for t in st))
    daily = tp / nd if nd else 0
    print(f"  {sn:<20} {len(st):>4} {len(w):>4} {len(l):>4} {wr:>4.0f}% {pf:>5.2f} ${tp:>+9,.0f} ${tp/10:>+8,.0f} ${aw:>+6,.0f} ${al:>+6,.0f} ${daily:>+8,.0f} ${daily/10*21:>+8,.0f}")

# Grand total
tp = sum(t["pnl"] for t in results)
w = [t for t in results if t["pnl"] >= 0]
l = [t for t in results if t["pnl"] < 0]
nd = len(set(t["date"] for t in results))
print(f"  {'ALL':>20} {len(results):>4} {len(w):>4} {len(l):>4} {len(w)/len(results)*100:>4.0f}% -- ${tp:>+9,.0f} ${tp/10:>+8,.0f} {'':>7} {'':>7} ${tp/nd:>+8,.0f} ${tp/nd/10*21:>+8,.0f}")

# ── Per-setup + direction ──
print(f"\n{'='*110}")
print(f"BY SETUP + DIRECTION")
print(f"{'='*110}")
print(f"\n{'Setup + Dir':<28} {'#':>4} {'W':>4} {'L':>4} {'WR':>5} {'PF':>5} {'SPXW':>10} {'SPY':>9}")
print("-" * 75)

for sn in all_setups:
    for dname, dval in [("LONG", "L"), ("SHORT", "S")]:
        st = [t for t in results if t["setup"] == sn and t["dir"] == dval]
        if not st: continue
        tp = sum(t["pnl"] for t in st)
        w = [t for t in st if t["pnl"] >= 0]
        l = [t for t in st if t["pnl"] < 0]
        wr = len(w) / len(st) * 100
        pf = abs(sum(t["pnl"] for t in w) / sum(t["pnl"] for t in l)) if l and sum(t["pnl"] for t in l) != 0 else 999
        print(f"  {sn+' '+dname:<26} {len(st):>4} {len(w):>4} {len(l):>4} {wr:>4.0f}% {pf:>5.2f} ${tp:>+9,.0f} ${tp/10:>+8,.0f}")

# ── By alignment per setup ──
print(f"\n{'='*110}")
print(f"BY SETUP + ALIGNMENT (credit spread)")
print(f"{'='*110}")

for sn in all_setups:
    st = [t for t in results if t["setup"] == sn]
    if len(st) < 5: continue
    aligns = sorted(set(t["align"] for t in st))
    print(f"\n  {sn}:")
    print(f"  {'Align':>6} {'#':>4} {'W':>4} {'L':>4} {'WR':>5} {'SPXW':>9} {'SPY':>8}")
    for a in aligns:
        at = [t for t in st if t["align"] == a]
        if not at: continue
        ap = sum(t["pnl"] for t in at)
        aw = sum(1 for t in at if t["pnl"] >= 0)
        print(f"  {a:>+6} {len(at):>4} {aw:>4} {len(at)-aw:>4} {aw/len(at)*100:>4.0f}% ${ap:>+8,.0f} ${ap/10:>+7,.0f}")

# ── Best combo: which setups to include? ──
print(f"\n{'='*110}")
print(f"OPTIMAL SELECTION — cumulative by adding setups (best first)")
print(f"{'='*110}")

setup_pnl = {}
for sn in all_setups:
    st = [t for t in results if t["setup"] == sn]
    nd = len(set(t["date"] for t in st))
    daily = sum(t["pnl"] for t in st) / nd if nd else 0
    setup_pnl[sn] = {"total": sum(t["pnl"] for t in st), "daily": daily, "n": len(st)}

ranked = sorted(setup_pnl.items(), key=lambda x: x[1]["daily"], reverse=True)
cum = 0
cum_trades = 0
print(f"\n{'Setup added':<22} {'Own PnL':>9} {'Cum PnL':>9} {'SPY Cum':>8} {'SPY/mo':>9}")
print("-" * 65)
for sn, data in ranked:
    cum += data["total"]
    cum_trades += data["n"]
    nd = 13  # trading days in period
    monthly = cum / nd * 21
    print(f"  + {sn:<18} ${data['total']:>+8,.0f} ${cum:>+8,.0f} ${cum/10:>+7,.0f} ${monthly/10:>+8,.0f}")

# ── Daily PnL for top combo ──
# DD + AG + Paradigm + ES Absorption (skip SC and negatives)
print(f"\n{'='*110}")
print(f"DAILY PnL — ALL SETUPS UNFILTERED (credit spread)")
print(f"{'='*110}")
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
