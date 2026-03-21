"""SC full study — ALL available data since implementation (Mar 2).
Cap=2, 1 MES. Daily statement."""
import sqlalchemy
from sqlalchemy import text
from collections import defaultdict
from datetime import timedelta

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine = sqlalchemy.create_engine(DB_URL)

with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, setup_name, direction, outcome_result, outcome_pnl,
               outcome_elapsed_min, ts, greek_alignment,
               (ts AT TIME ZONE 'America/New_York')::date as trade_date
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND setup_name = 'Skew Charm'
        ORDER BY ts
    """)).mappings().all()

def passes(s):
    align = int(s.get("greek_alignment") or 0) if s.get("greek_alignment") is not None else 0
    is_long = s.get("direction", "").lower() in ("long", "bullish")
    if is_long and align < 2: return False
    return True

all_trades = []
for s in rows:
    if not passes(s): continue
    all_trades.append({
        "id": s["id"], "date": str(s["trade_date"]),
        "dir": "L" if s["direction"].lower() in ("long", "bullish") else "S",
        "outcome": s["outcome_result"],
        "pts": float(s["outcome_pnl"] or 0),
        "ts": s["ts"],
        "end_ts": s["ts"] + timedelta(minutes=float(s["outcome_elapsed_min"] or 20)),
    })

CAP = 2
QTY = 1
DPP = QTY * 5.0
COMM_RT = 2.10  # TS real: $1.05/side

traded = []
skipped = []
for t in all_trades:
    open_same = [tr for tr in traded if tr["dir"] == t["dir"] and tr["end_ts"] > t["ts"]]
    if len(open_same) >= CAP:
        skipped.append(t)
    else:
        traded.append(t)

ndays = len(set(t["date"] for t in traded))
total_pts = sum(t["pts"] for t in traded)
wins = sum(1 for t in traded if t["pts"] > 0)
losses = sum(1 for t in traded if t["pts"] <= 0)
longs = [t for t in traded if t["dir"] == "L"]
shorts = [t for t in traded if t["dir"] == "S"]
w_pts = sum(t["pts"] for t in traded if t["pts"] > 0)
l_pts = sum(t["pts"] for t in traded if t["pts"] < 0)

first = traded[0]["date"]
last = traded[-1]["date"]

print(f"{'='*80}")
print(f"SKEW CHARM on 1 MES | Cap=2 | FULL DATA since implementation")
print(f"Period: {first} to {last} | {ndays} trading days")
print(f"{'='*80}")
print(f"Signals: {len(all_trades)} total | Traded: {len(traded)} | Skipped: {len(skipped)}")
print(f"Longs: {len(longs)} | Shorts: {len(shorts)}")
print(f"Win/Loss: {wins}W / {losses}L | WR: {wins/len(traded)*100:.0f}%")
print(f"Total: {total_pts:+.1f} pts | PF: {abs(w_pts/l_pts):.2f}")
print(f"Avg win: {w_pts/wins:+.1f} | Avg loss: {l_pts/(losses if losses else 1):+.1f}")

# Daily P&L
daily = defaultdict(lambda: {"pts": 0, "n": 0, "l": 0, "s": 0, "w": 0, "skip": 0})
for t in traded:
    daily[t["date"]]["pts"] += t["pts"]
    daily[t["date"]]["n"] += 1
    if t["dir"] == "L": daily[t["date"]]["l"] += 1
    else: daily[t["date"]]["s"] += 1
    if t["pts"] > 0: daily[t["date"]]["w"] += 1
for t in skipped:
    daily[t["date"]]["skip"] += 1

print(f"\n{'Date':<11} {'#':>3} {'L':>2} {'S':>2} {'W':>2} {'Sk':>3} {'Pts':>7} {'Gross':>8} {'Comm':>6} {'Net':>8} {'Cum':>8}")
print("=" * 72)

cum = 0
peak = 0
max_dd = 0
pos_days = 0
total_gross = 0
total_comm = 0
total_net = 0

for d in sorted(daily.keys()):
    dd = daily[d]
    gross = dd["pts"] * DPP
    comm = dd["n"] * COMM_RT
    net = gross - comm
    cum += net
    peak = max(peak, cum)
    max_dd = max(max_dd, peak - cum)
    if net > 0: pos_days += 1
    total_gross += gross
    total_comm += comm
    total_net += net
    print(f"{d:<11} {dd['n']:>3} {dd['l']:>2} {dd['s']:>2} {dd['w']:>2} {dd['skip']:>3} "
          f"{dd['pts']:>+6.1f} ${gross:>+7,.0f} ${comm:>5,.0f} ${net:>+7,.0f} ${cum:>+7,.0f}")

print("=" * 72)
print(f"{'TOTAL':<11} {len(traded):>3} {len(longs):>2} {len(shorts):>2} {wins:>2} {len(skipped):>3} "
      f"{total_pts:>+6.1f} ${total_gross:>+7,.0f} ${total_comm:>5,.0f} ${total_net:>+7,.0f}")

print(f"\n--- RESULTS at 1 MES ($5/pt) ---")
print(f"Net P&L ({ndays}d):  ${total_net:>+,.0f}")
print(f"Net/day:          ${total_net/ndays:>+,.0f}")
print(f"Net/month (21d):  ${total_net/ndays*21:>+,.0f}")
print(f"Max drawdown:     ${max_dd:>,.0f}")
print(f"Positive days:    {pos_days}/{ndays} ({pos_days/ndays*100:.0f}%)")
print(f"Best day:         ${max(daily[d]['pts']*DPP - daily[d]['n']*COMM_RT for d in daily):>+,.0f}")
print(f"Worst day:        ${min(daily[d]['pts']*DPP - daily[d]['n']*COMM_RT for d in daily):>+,.0f}")

# Direction split
print(f"\n--- BY DIRECTION ---")
for label, subset in [("Longs", longs), ("Shorts", shorts)]:
    sp = sum(t["pts"] for t in subset)
    sw = sum(1 for t in subset if t["pts"] > 0)
    sl = len(subset) - sw
    wp = sum(t["pts"] for t in subset if t["pts"] > 0)
    lp = sum(t["pts"] for t in subset if t["pts"] < 0)
    pf = abs(wp/lp) if lp else 999
    print(f"  {label}: {len(subset)}t  {sw}W/{sl}L  WR:{sw/len(subset)*100:.0f}%  "
          f"{sp:+.1f}pts  PF:{pf:.2f}  ${sp*DPP - len(subset)*COMM_RT:>+,.0f} net")

# Capital
print(f"\n--- CAPITAL ($1K per account, $2K total) ---")
monthly = total_net / ndays * 21
print(f"  Monthly net:    ${monthly:>+,.0f}")
print(f"  Monthly ROI:    {monthly/2000*100:>+.0f}%")
print(f"  Yearly net:     ${total_net/ndays*252:>+,.0f}")
print(f"  Max DD:         ${max_dd:>,.0f} ({max_dd/2000*100:.0f}% of capital)")
