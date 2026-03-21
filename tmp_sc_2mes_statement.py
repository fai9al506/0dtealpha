"""SC 2 MES — Daily Statement. Cap=2 concurrent per direction.
Account A: longs only. Account B: shorts only. 2 MES per trade."""
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
               (ts AT TIME ZONE 'America/New_York')::date as trade_date,
               to_char(ts AT TIME ZONE 'America/New_York', 'HH24:MI') as time_et
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
        "time": s["time_et"],
        "dir": "L" if s["direction"].lower() in ("long", "bullish") else "S",
        "outcome": s["outcome_result"],
        "pts": float(s["outcome_pnl"] or 0),
        "elapsed": float(s["outcome_elapsed_min"] or 20),
        "ts": s["ts"],
        "end_ts": s["ts"] + timedelta(minutes=float(s["outcome_elapsed_min"] or 20)),
    })

# Simulate cap=2
CAP = 2
QTY = 2
DPP = QTY * 5.0  # $10/pt
COMM_RT = QTY * 2 * 0.62  # $2.48 per trade round-trip
SL = 14.0

traded = []
skipped = []
for t in all_trades:
    open_same = [tr for tr in traded if tr["dir"] == t["dir"] and tr["end_ts"] > t["ts"]]
    if len(open_same) >= CAP:
        skipped.append(t)
    else:
        traded.append(t)

# ═══════════════════════════════════════════════════
print(f"{'='*100}")
print(f"DAILY TRADING STATEMENT — SKEW CHARM on 2 MES")
print(f"{'='*100}")
print(f"Strategy:    Skew Charm setups, V9-SC filter, trail SL=14")
print(f"Execution:   2 MES per trade ($10/pt), max 2 concurrent per direction")
print(f"Accounts:    Account A (longs only), Account B (shorts only)")
print(f"Period:      {traded[0]['date']} to {traded[-1]['date']} ({len(set(t['date'] for t in traded))} trading days)")
print(f"Commission:  ${COMM_RT:.2f} per trade round-trip ({QTY} MES × $0.62 × 2)")
print()

# ═══════════════════════════════════════════════════
# Per-day detailed statement
# ═══════════════════════════════════════════════════
balance = 0
peak_bal = 0
max_dd_bal = 0
daily_results = []

for d in sorted(set(t["date"] for t in traded)):
    day_trades = [t for t in traded if t["date"] == d]
    day_skipped = [t for t in skipped if t["date"] == d]

    day_gross = sum(t["pts"] for t in day_trades) * DPP
    day_comm = len(day_trades) * COMM_RT
    day_net = day_gross - day_comm
    balance += day_net
    peak_bal = max(peak_bal, balance)
    max_dd_bal = max(max_dd_bal, peak_bal - balance)

    wins = sum(1 for t in day_trades if t["pts"] > 0)
    losses = sum(1 for t in day_trades if t["pts"] <= 0)
    longs = sum(1 for t in day_trades if t["dir"] == "L")
    shorts = sum(1 for t in day_trades if t["dir"] == "S")
    day_pts = sum(t["pts"] for t in day_trades)

    daily_results.append({
        "date": d, "n": len(day_trades), "l": longs, "s": shorts,
        "w": wins, "loss": losses, "skip": len(day_skipped),
        "pts": day_pts, "gross": day_gross, "comm": day_comm,
        "net": day_net, "balance": balance,
    })

    print(f"+---------------------------------------------------------------------------------+")
    print(f"|  {d}  |  Trades: {len(day_trades)}  ({longs}L/{shorts}S)  |  {wins}W/{losses}L  |  Skipped: {len(day_skipped):<3}         |")
    print(f"+---------------------------------------------------------------------------------+")

    for t in day_trades:
        pnl_dollar = t["pts"] * DPP - COMM_RT
        marker = "+" if t["pts"] > 0 else "-" if t["pts"] < 0 else "~"
        dir_label = "LONG " if t["dir"] == "L" else "SHORT"
        print(f"|  {marker} {t['time']}  {dir_label}  {t['outcome']:<7}  {t['pts']:>+6.1f} pts  "
              f"${t['pts']*DPP:>+7.0f} gross  ${pnl_dollar:>+7.0f} net  held {t['elapsed']:>3.0f}m      |")

    print(f"+---------------------------------------------------------------------------------+")
    print(f"|  Day P&L:  {day_pts:>+6.1f} pts  |  Gross: ${day_gross:>+7,.0f}  Comm: ${day_comm:>5,.0f}  Net: ${day_net:>+7,.0f}          |")
    print(f"|  Balance:  ${balance:>+8,.0f}                                                             |")
    print(f"+---------------------------------------------------------------------------------+")
    print()

# ═══════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════
total_gross = sum(d["gross"] for d in daily_results)
total_comm = sum(d["comm"] for d in daily_results)
total_net = sum(d["net"] for d in daily_results)
total_pts = sum(d["pts"] for d in daily_results)
ndays = len(daily_results)
wins_total = sum(1 for t in traded if t["pts"] > 0)
losses_total = sum(1 for t in traded if t["pts"] <= 0)
pos_days = sum(1 for d in daily_results if d["net"] > 0)
best_day = max(daily_results, key=lambda d: d["net"])
worst_day = min(daily_results, key=lambda d: d["net"])

print(f"{'='*100}")
print(f"ACCOUNT SUMMARY")
print(f"{'='*100}")
print(f"  Period:          {traded[0]['date']} to {traded[-1]['date']} ({ndays} trading days)")
print(f"  Total trades:    {len(traded)} ({len(skipped)} skipped due to cap)")
print(f"  Longs / Shorts:  {sum(1 for t in traded if t['dir']=='L')} / {sum(1 for t in traded if t['dir']=='S')}")
print(f"  Win / Loss:      {wins_total}W / {losses_total}L ({wins_total/len(traded)*100:.0f}% WR)")
print(f"")
print(f"  Total points:    {total_pts:>+,.1f}")
print(f"  Gross P&L:       ${total_gross:>+,.0f}")
print(f"  Commissions:     ${total_comm:>,.0f}  ({len(traded)} × ${COMM_RT:.2f})")
print(f"  Net P&L:         ${total_net:>+,.0f}")
print(f"")
print(f"  Per day avg:     ${total_net/ndays:>+,.0f}")
print(f"  Per month (21d): ${total_net/ndays*21:>+,.0f}")
print(f"  Per year (252d): ${total_net/ndays*252:>+,.0f}")
print(f"")
print(f"  Best day:        {best_day['date']}  ${best_day['net']:>+,.0f}")
print(f"  Worst day:       {worst_day['date']}  ${worst_day['net']:>+,.0f}")
print(f"  Positive days:   {pos_days}/{ndays} ({pos_days/ndays*100:.0f}%)")
print(f"  Max drawdown:    ${max_dd_bal:>,.0f}")
print(f"")
print(f"  Worst case loss: 2 trades × SL {SL:.0f} × {QTY} MES × $5 = ${2*SL*QTY*5:>,.0f}")

# ═══════════════════════════════════════════════════
# CAPITAL REQUIREMENTS
# ═══════════════════════════════════════════════════
margin_per_mes = 2737  # intraday margin
max_concurrent = CAP * 2  # 2 longs + 2 shorts worst case
total_margin = max_concurrent * margin_per_mes
buffer_dd = max_dd_bal * 2  # 2x max observed DD as safety
worst_case_day = abs(worst_day["net"]) * 3  # 3x worst day

print(f"\n{'='*100}")
print(f"CAPITAL REQUIREMENTS")
print(f"{'='*100}")
print(f"")
print(f"  Intraday margin per MES:     ${margin_per_mes:>,.0f}")
print(f"  Max concurrent contracts:    {max_concurrent} ({CAP} longs + {CAP} shorts × {QTY} MES)")
print(f"  Total margin needed:         ${total_margin:>,.0f}")
print(f"")
print(f"  Observed max drawdown:       ${max_dd_bal:>,.0f}")
print(f"  Worst day observed:          ${abs(worst_day['net']):>,.0f}")
print(f"  Worst case (2×SL both dirs): ${2*2*SL*QTY*5:>,.0f}")
print(f"")
print(f"  ── Recommended Account Size ──")
print(f"  Margin + 2× max DD:          ${total_margin + buffer_dd:>,.0f}")
print(f"  Margin + 3× worst day:       ${total_margin + worst_case_day:>,.0f}")
print(f"  Conservative (margin + $5K): ${total_margin + 5000:>,.0f}")
print(f"")
print(f"  ── Monthly ROI ──")
monthly = total_net / ndays * 21
for cap_size in [10000, 15000, 20000, 25000]:
    roi = monthly / cap_size * 100
    print(f"  ${cap_size:>6,} account:  ${monthly:>+,.0f}/mo = {roi:>+.0f}% ROI")
