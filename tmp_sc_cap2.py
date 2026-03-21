"""SC MES futures — cap=2 concurrent per direction. Daily PnL report.
Two accounts: Account A (longs), Account B (shorts). Max 2 open per account."""
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
          AND ts >= '2026-03-01' AND ts < '2026-03-19'
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

# ═══════════════════════════════════════════════════
# Simulate cap=2 per direction
# ═══════════════════════════════════════════════════
CAP = 2
traded = []
skipped = []

for t in all_trades:
    # Count open same-direction trades
    open_same = [
        tr for tr in traded
        if tr["dir"] == t["dir"] and tr["end_ts"] > t["ts"]
    ]
    if len(open_same) >= CAP:
        skipped.append(t)
    else:
        traded.append(t)

COMM_RT = 2 * 0.62  # per MES round-trip

# ═══════════════════════════════════════════════════
# Print header
# ═══════════════════════════════════════════════════
ndays = len(set(t["date"] for t in traded))
total_pts = sum(t["pts"] for t in traded)
wins = sum(1 for t in traded if t["pts"] > 0)
losses = sum(1 for t in traded if t["pts"] < 0)
exp = sum(1 for t in traded if t["pts"] == 0)
w_pts = sum(t["pts"] for t in traded if t["pts"] > 0)
l_pts = sum(t["pts"] for t in traded if t["pts"] < 0)
longs = [t for t in traded if t["dir"] == "L"]
shorts = [t for t in traded if t["dir"] == "S"]

print(f"{'='*95}")
print(f"SKEW CHARM — MES FUTURES — CAP 2 CONCURRENT PER DIRECTION")
print(f"Period: {traded[0]['date']} to {traded[-1]['date']} | {ndays} trading days")
print(f"{'='*95}")
print(f"Traded:   {len(traded)} of {len(all_trades)} signals ({len(skipped)} skipped)")
print(f"Longs:    {len(longs)}  |  Shorts: {len(shorts)}")
print(f"W/L/E:    {wins}W / {losses}L / {exp}E  |  WR: {wins/len(traded)*100:.0f}%")
print(f"Total:    {total_pts:+.1f} pts  |  Per day: {total_pts/ndays:+.1f} pts")
print(f"Avg win:  {w_pts/wins:+.1f}  |  Avg loss: {l_pts/losses:+.1f}  |  PF: {abs(w_pts/l_pts):.2f}")

# ═══════════════════════════════════════════════════
# Daily P&L for each MES size
# ═══════════════════════════════════════════════════
daily = defaultdict(lambda: {"pts": 0, "n": 0, "l": 0, "s": 0, "w": 0, "skip": 0})
for t in traded:
    daily[t["date"]]["pts"] += t["pts"]
    daily[t["date"]]["n"] += 1
    if t["dir"] == "L": daily[t["date"]]["l"] += 1
    else: daily[t["date"]]["s"] += 1
    if t["pts"] > 0: daily[t["date"]]["w"] += 1
for t in skipped:
    daily[t["date"]]["skip"] += 1

for qty in [2, 4, 6, 8, 10]:
    dpp = qty * 5.0  # $/pt
    print(f"\n{'='*95}")
    print(f"{qty} MES per trade  |  ${dpp:.0f}/pt  |  Max exposure: {CAP} × {qty} = {CAP*qty} MES per direction")
    print(f"{'='*95}")
    print(f"\n{'Date':<11} {'#':>3} {'L':>2} {'S':>2} {'W':>2} {'Skip':>4} {'Pts':>7} {'Gross $':>9} {'Comm':>6} {'Net $':>9} {'Cum $':>9}")
    print("-" * 80)

    cum = 0
    peak = 0
    max_dd = 0
    total_net = 0
    total_comm = 0
    pos_days = 0

    for d in sorted(daily.keys()):
        dd = daily[d]
        gross = dd["pts"] * dpp
        comm = dd["n"] * qty * COMM_RT
        net = gross - comm
        cum += net
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
        total_net += net
        total_comm += comm
        if net > 0: pos_days += 1
        print(f"{d:<11} {dd['n']:>3} {dd['l']:>2} {dd['s']:>2} {dd['w']:>2} {dd['skip']:>4} "
              f"{dd['pts']:>+6.1f} ${gross:>+8,.0f} ${comm:>5,.0f} ${net:>+8,.0f} ${cum:>+8,.0f}")

    total_gross = total_pts * dpp
    print("-" * 80)
    print(f"{'TOTAL':<11} {len(traded):>3} {len(longs):>2} {len(shorts):>2} {wins:>2} {len(skipped):>4} "
          f"{total_pts:>+6.1f} ${total_gross:>+8,.0f} ${total_comm:>5,.0f} ${total_net:>+8,.0f}")

    print(f"\n  Net/day:       ${total_net/ndays:>+,.0f}")
    print(f"  Net/month:     ${total_net/ndays*21:>+,.0f}")
    print(f"  Max drawdown:  ${max_dd:>,.0f}")
    print(f"  Positive days: {pos_days}/{ndays} ({pos_days/ndays*100:.0f}%)")
    print(f"  Worst case:    {CAP} trades × SL 14 × {qty} MES × $5 = ${CAP * 14 * qty * 5:>,.0f}")
