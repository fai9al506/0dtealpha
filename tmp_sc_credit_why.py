"""Deep dive: WHY is Skew Charm losing on credit spreads despite 62% WR?"""
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
               (ts AT TIME ZONE 'America/New_York')::date as trade_date,
               extract(hour from ts AT TIME ZONE 'America/New_York') as hour
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND setup_name = 'Skew Charm'
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
    if is_long and align < 2: return False
    return True  # SC always passes direction filter

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
    max_loss = float(s["outcome_max_loss"] or 0)
    max_profit = float(s["outcome_max_profit"] or 0)

    results.append({
        "id": s["id"], "date": str(s["trade_date"]),
        "dir": "L" if is_long else "S",
        "outcome": s["outcome_result"],
        "spx_pnl": float(s["outcome_pnl"] or 0),
        "spx_max_loss": max_loss,
        "spx_max_profit": max_profit,
        "elapsed": elapsed,
        "hour": int(s["hour"]),
        "credit": credit, "close": close_cost,
        "pnl": round(pnl, 2),
        "short_k": sk, "long_k": long_sk,
        "credit_side": credit_side,
        "short_exit_ask": xs["ask"],
        "long_exit_bid": xl["bid"],
        "short_entry_bid": short_opt["bid"],
        "long_entry_ask": long_opt["ask"],
    })

print(f"SC trades: {len(results)}")
total = sum(t["pnl"] for t in results)
wins = [t for t in results if t["pnl"] >= 0]
losses = [t for t in results if t["pnl"] < 0]

print(f"\n{'='*100}")
print(f"SKEW CHARM CREDIT SPREAD FORENSICS (0.50 delta, $10-wide)")
print(f"{'='*100}")
print(f"Total: {len(results)}t | {len(wins)}W/{len(losses)}L | WR: {len(wins)/len(results)*100:.0f}% | PnL: ${total:>+,.0f} SPXW | ${total/10:>+,.0f} SPY")

# ── 1. WIN vs LOSS P&L distribution ──
print(f"\n--- WIN/LOSS P&L DISTRIBUTION ---")
print(f"Winners:  avg ${sum(t['pnl'] for t in wins)/len(wins):>+,.0f}  |  median ${sorted([t['pnl'] for t in wins])[len(wins)//2]:>+,.0f}")
print(f"Losers:   avg ${sum(t['pnl'] for t in losses)/len(losses):>+,.0f}  |  median ${sorted([t['pnl'] for t in losses])[len(losses)//2]:>+,.0f}")

# Histogram of P&L
print(f"\nP&L buckets:")
for lo, hi in [(-1000, -500), (-500, -300), (-300, -200), (-200, -100), (-100, 0), (0, 100), (100, 200), (200, 300), (300, 500), (500, 1000)]:
    bucket = [t for t in results if lo <= t["pnl"] < hi]
    if bucket:
        bp = sum(t["pnl"] for t in bucket)
        print(f"  ${lo:>+5} to ${hi:>+5}: {len(bucket):>3} trades  |  total ${bp:>+7,.0f}  |  avg ${bp/len(bucket):>+5,.0f}")

# ── 2. LOSS breakdown: what's the SPX pnl on losing credit spreads? ──
print(f"\n--- CREDIT SPREAD LOSSES: SPX context ---")
print(f"{'ID':>5} {'Date':<11} {'Dir':>3} {'Outcome':>5} {'SPX PnL':>8} {'MxLoss':>7} {'MxProf':>7} {'Credit':>7} {'Close':>7} {'CrdPnL':>8} {'Elapsed':>5} {'Hr':>3}")
print("-" * 100)
for t in sorted(losses, key=lambda x: x["pnl"]):
    print(f"#{t['id']:<5} {t['date']:<11} {t['dir']:>3} {t['outcome']:>5} {t['spx_pnl']:>+7.1f} "
          f"{t['spx_max_loss']:>+6.1f} {t['spx_max_profit']:>+6.1f} "
          f"${t['credit']:>5.2f} ${t['close']:>5.2f} ${t['pnl']:>+7,.0f} {t['elapsed']:>4.0f}m {t['hour']:>2}h")

# ── 3. The KEY question: why do WINNERS pay so little? ──
print(f"\n--- WINNERS: credit received vs closed ---")
# Group wins by SPX move size
for lo, hi, label in [(0, 5, "0-5 pts"), (5, 8, "5-8 pts"), (8, 10, "8-10 pts"), (10, 14, "10-14 pts"), (14, 999, "14+ pts")]:
    wt = [t for t in wins if lo <= abs(t["spx_pnl"]) < hi]
    if not wt: continue
    avg_credit = sum(t["credit"] for t in wt) / len(wt)
    avg_close = sum(t["close"] for t in wt) / len(wt)
    avg_pnl = sum(t["pnl"] for t in wt) / len(wt)
    total_p = sum(t["pnl"] for t in wt)
    # Show how much of credit we keep
    keep_pct = (1 - avg_close / avg_credit) * 100 if avg_credit > 0 else 0
    print(f"  WIN {label}: {len(wt):>3}t  |  avg credit ${avg_credit:.2f}  avg close ${avg_close:.2f}  "
          f"keep {keep_pct:.0f}%  |  avg PnL ${avg_pnl:>+,.0f}  total ${total_p:>+,.0f}")

# ── 4. Direction split ──
print(f"\n--- BY DIRECTION ---")
for dname, dval in [("LONG", "L"), ("SHORT", "S")]:
    dt = [t for t in results if t["dir"] == dval]
    if not dt: continue
    dp = sum(t["pnl"] for t in dt)
    dw = sum(1 for t in dt if t["pnl"] >= 0)
    dl = len(dt) - dw
    w_trades = [t for t in dt if t["pnl"] >= 0]
    l_trades = [t for t in dt if t["pnl"] < 0]
    avg_w = sum(t["pnl"] for t in w_trades) / len(w_trades) if w_trades else 0
    avg_l = sum(t["pnl"] for t in l_trades) / len(l_trades) if l_trades else 0
    # SPX outcomes on losses
    l_spx = [t["spx_pnl"] for t in l_trades]
    avg_l_spx = sum(l_spx) / len(l_spx) if l_spx else 0
    print(f"  {dname}: {len(dt)}t  {dw}W/{dl}L  WR:{dw/len(dt)*100:.0f}%  PnL:${dp:>+,.0f}  "
          f"avgW:${avg_w:>+,.0f}  avgL:${avg_l:>+,.0f}  avgLossSpx:{avg_l_spx:>+.1f}pts")

# ── 5. SPX outcome vs credit spread outcome comparison ──
print(f"\n--- SPX WIN that became CREDIT LOSS (the leakers) ---")
leakers = [t for t in results if t["outcome"] == "WIN" and t["pnl"] < 0]
print(f"Count: {len(leakers)} trades that WON on SPX but LOST on credit spread")
if leakers:
    lk_total = sum(t["pnl"] for t in leakers)
    print(f"Total damage: ${lk_total:>+,.0f}")
    print(f"\n{'ID':>5} {'Dir':>3} {'SPX':>6} {'Elapsed':>5} {'Credit':>7} {'Close':>7} {'CrdPnL':>7} {'ShortBid':>9}->{'ShortAsk':>9} {'LongAsk':>8}->{'LongBid':>8}")
    for t in sorted(leakers, key=lambda x: x["pnl"])[:20]:
        print(f"#{t['id']:<5} {t['dir']:>3} {t['spx_pnl']:>+5.1f} {t['elapsed']:>4.0f}m "
              f"${t['credit']:>5.2f} ${t['close']:>5.2f} ${t['pnl']:>+6,.0f}  "
              f"${t['short_entry_bid']:>7.2f}->${t['short_exit_ask']:>7.2f}  "
              f"${t['long_entry_ask']:>6.2f}->${t['long_exit_bid']:>6.2f}")

# ── 6. SPX LOSS that became CREDIT WIN (the gifts) ──
print(f"\n--- SPX LOSS that became CREDIT WIN ---")
gifts = [t for t in results if t["outcome"] != "WIN" and t["pnl"] >= 0]
print(f"Count: {len(gifts)}")
if gifts:
    gk_total = sum(t["pnl"] for t in gifts)
    print(f"Total bonus: ${gk_total:>+,.0f}")

# ── 7. Time analysis ──
print(f"\n--- BY HOUR ---")
for h in sorted(set(t["hour"] for t in results)):
    ht = [t for t in results if t["hour"] == h]
    hp = sum(t["pnl"] for t in ht)
    hw = sum(1 for t in ht if t["pnl"] >= 0)
    print(f"  {h:>2}:00  {len(ht):>3}t  {hw}W/{len(ht)-hw}L  WR:{hw/len(ht)*100:.0f}%  PnL:${hp:>+,.0f} SPXW  ${hp/10:>+,.0f} SPY")

# ── 8. SC SL size vs credit spread loss ──
print(f"\n--- SC STOP LOSS SIZE vs CREDIT LOSS ---")
print(f"SC uses SL=14 (was 20). How many losses exceed SL threshold?")
for t in losses:
    spx_loss = abs(t["spx_pnl"])
    credit_loss = abs(t["pnl"])
print(f"\nLosing trades by SPX adverse move:")
for lo, hi, label in [(0, 8, "0-8 pts"), (8, 14, "8-14 pts (SL zone)"), (14, 20, "14-20 pts"), (20, 999, "20+ pts")]:
    lt = [t for t in losses if lo <= abs(t["spx_pnl"]) < hi]
    if not lt: continue
    avg_crd_loss = sum(t["pnl"] for t in lt) / len(lt)
    total_crd = sum(t["pnl"] for t in lt)
    # What % of max loss ($WIDTH * 100) are these?
    avg_pct = abs(avg_crd_loss) / (WIDTH * 100) * 100
    print(f"  SPX loss {label}: {len(lt):>3}t  |  avg credit loss: ${avg_crd_loss:>+,.0f} ({avg_pct:.0f}% of max)  |  total: ${total_crd:>+,.0f}")
