"""SC on MES futures — full study. All signals (no single-pos limit).
Per-direction accounts, V9-SC filter, real setup_log outcomes."""
import sqlalchemy, json
from sqlalchemy import text
from collections import defaultdict

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine = sqlalchemy.create_engine(DB_URL)

with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, setup_name, direction, outcome_result, outcome_pnl,
               outcome_elapsed_min, ts, vix, overvix, greek_alignment,
               (ts AT TIME ZONE 'America/New_York')::date as trade_date,
               extract(hour from ts AT TIME ZONE 'America/New_York') as hour
        FROM setup_log
        WHERE outcome_result IS NOT NULL
          AND setup_name = 'Skew Charm'
          AND ts >= '2026-02-11'
          AND ts < '2026-03-19'
        ORDER BY id
    """)).mappings().all()

print(f"Total SC outcomes: {len(rows)}")

# V9-SC filter
def passes(s):
    align = int(s.get("greek_alignment") or 0) if s.get("greek_alignment") is not None else 0
    is_long = s.get("direction", "").lower() in ("long", "bullish")
    if is_long and align < 2: return False
    return True

trades = []
for s in rows:
    if not passes(s): continue
    trades.append({
        "id": s["id"], "date": str(s["trade_date"]),
        "dir": "L" if s["direction"].lower() in ("long", "bullish") else "S",
        "outcome": s["outcome_result"],
        "pts": float(s["outcome_pnl"] or 0),
        "hour": int(s["hour"]),
    })

first_date = trades[0]["date"] if trades else "?"
last_date = trades[-1]["date"] if trades else "?"
ndays = len(set(t["date"] for t in trades))
longs = [t for t in trades if t["dir"] == "L"]
shorts = [t for t in trades if t["dir"] == "S"]

print(f"\n{'='*90}")
print(f"SKEW CHARM — MES FUTURES STUDY (all signals, no single-pos limit)")
print(f"Period: {first_date} to {last_date} | {ndays} trading days | V9-SC filter")
print(f"{'='*90}")

total_pts = sum(t["pts"] for t in trades)
wins = sum(1 for t in trades if t["pts"] > 0)
losses = sum(1 for t in trades if t["pts"] < 0)
expired = sum(1 for t in trades if t["pts"] == 0)
w_pts = sum(t["pts"] for t in trades if t["pts"] > 0)
l_pts = sum(t["pts"] for t in trades if t["pts"] < 0)

print(f"\nTotal trades:  {len(trades)}")
print(f"Per day avg:   {len(trades)/ndays:.1f}")
print(f"Longs:         {len(longs)} ({len(longs)/len(trades)*100:.0f}%)")
print(f"Shorts:        {len(shorts)} ({len(shorts)/len(trades)*100:.0f}%)")
print(f"Win/Loss/Exp:  {wins}W / {losses}L / {expired}E")
print(f"Win rate:      {wins/len(trades)*100:.0f}%")
print(f"Total pts:     {total_pts:+.1f}")
print(f"Avg winner:    {w_pts/wins:+.1f} pts" if wins else "")
print(f"Avg loser:     {l_pts/losses:+.1f} pts" if losses else "")
print(f"Profit factor: {abs(w_pts/l_pts):.2f}" if l_pts else "")
print(f"Pts/day:       {total_pts/ndays:+.1f}")

# ═══════════════════════════════════════════════════
# DAILY P&L TABLE (in points)
# ═══════════════════════════════════════════════════
daily = defaultdict(lambda: {"pts": 0, "n": 0, "l": 0, "s": 0, "w": 0})
for t in trades:
    daily[t["date"]]["pts"] += t["pts"]
    daily[t["date"]]["n"] += 1
    if t["dir"] == "L": daily[t["date"]]["l"] += 1
    else: daily[t["date"]]["s"] += 1
    if t["pts"] > 0: daily[t["date"]]["w"] += 1

COMM_RT = 2 * 0.62  # round-trip commission per MES contract

print(f"\n{'Date':<11} {'#':>3} {'L':>3} {'S':>3} {'W':>3} {'Pts':>8} {'Cum Pts':>8}")
print("=" * 48)
cum = 0
peak = 0; max_dd_pts = 0
pos_days = 0
for d in sorted(daily.keys()):
    dd = daily[d]
    cum += dd["pts"]
    peak = max(peak, cum)
    max_dd_pts = max(max_dd_pts, peak - cum)
    if dd["pts"] > 0: pos_days += 1
    print(f"{d:<11} {dd['n']:>3} {dd['l']:>3} {dd['s']:>3} {dd['w']:>3} {dd['pts']:>+7.1f} {cum:>+7.1f}")

print(f"\nPositive days: {pos_days}/{ndays} ({pos_days/ndays*100:.0f}%)")
print(f"Max DD (pts):  {max_dd_pts:.1f}")

# ═══════════════════════════════════════════════════
# SCALING TABLE
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"SCALING TABLE — SC on MES (all signals, {ndays} trading days)")
print(f"{'='*90}")
print(f"\n{'MES':>4} {'$/pt':>6} {'Gross/day':>10} {'Comm/day':>9} {'Net/day':>9} {'Net/mo':>10} {'Max DD':>9} {'Margin':>8}")
print("-" * 70)

avg_trades_day = len(trades) / ndays
for qty in [2, 4, 6, 8, 10]:
    dollar_per_pt = qty * 5.0  # $5/pt per MES
    gross_day = (total_pts / ndays) * dollar_per_pt
    comm_day = avg_trades_day * qty * COMM_RT
    net_day = gross_day - comm_day
    net_mo = net_day * 21
    max_dd = max_dd_pts * dollar_per_pt
    margin = qty * 2737  # $2,737 per MES
    print(f"{qty:>4} ${qty*5:>5}/pt ${gross_day:>+9,.0f} ${comm_day:>8,.0f} ${net_day:>+8,.0f} ${net_mo:>+9,.0f} ${max_dd:>8,.0f} ${margin:>7,.0f}")

# ═══════════════════════════════════════════════════
# DIRECTION SPLIT
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"BY DIRECTION")
print(f"{'='*90}")
for label, subset in [("LONGS", longs), ("SHORTS", shorts)]:
    if not subset: continue
    sp = sum(t["pts"] for t in subset)
    sw = sum(1 for t in subset if t["pts"] > 0)
    sl = sum(1 for t in subset if t["pts"] < 0)
    wp = sum(t["pts"] for t in subset if t["pts"] > 0)
    lp = sum(t["pts"] for t in subset if t["pts"] < 0)
    nd = len(set(t["date"] for t in subset))
    print(f"\n  {label}: {len(subset)} trades across {nd} days")
    print(f"  Win/Loss: {sw}W/{sl}L | WR: {sw/len(subset)*100:.0f}%")
    print(f"  Total: {sp:+.1f} pts | Per day: {sp/ndays:+.1f} pts")
    print(f"  Avg win: {wp/sw:+.1f} | Avg loss: {lp/sl:+.1f} | PF: {abs(wp/lp):.2f}" if sl and sw else "")

    # Direction daily
    dd_daily = defaultdict(float)
    for t in subset:
        dd_daily[t["date"]] += t["pts"]
    d_pos = sum(1 for v in dd_daily.values() if v > 0)
    d_neg = sum(1 for v in dd_daily.values() if v <= 0)
    print(f"  Green days: {d_pos} | Red days: {d_neg}")

# ═══════════════════════════════════════════════════
# CONCURRENCY ANALYSIS (max open at same time)
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"STACKING ANALYSIS — max concurrent same-direction trades per day")
print(f"{'='*90}")

# Group by date, count max trades that overlap
# Since we don't have exact end times easily, approximate by counting per day per direction
print(f"\n{'Date':<11} {'Total':>5} {'Longs':>6} {'Shorts':>7} {'Max Stack':>10}")
print("-" * 45)
max_stack_ever = 0
for d in sorted(daily.keys()):
    dt = [t for t in trades if t["date"] == d]
    dl = sum(1 for t in dt if t["dir"] == "L")
    ds = sum(1 for t in dt if t["dir"] == "S")
    ms = max(dl, ds)
    max_stack_ever = max(max_stack_ever, ms)
    print(f"{d:<11} {len(dt):>5} {dl:>6} {ds:>7} {ms:>10}")

print(f"\nMax concurrent same-direction in a day: {max_stack_ever}")
print(f"(Worst case exposure = {max_stack_ever} trades × MES qty per trade)")

# ═══════════════════════════════════════════════════
# WORST CASE TABLE
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"WORST-CASE EXPOSURE (max {max_stack_ever} stacked × SL=14 pts)")
print(f"{'='*90}")
SL = 14.0
print(f"\n{'MES/trade':>10} {'Max Contracts':>14} {'Max $ Loss':>11} {'E2T Daily Limit':>16} {'Safe?':>6}")
print("-" * 62)
for qty in [2, 4, 6, 8, 10]:
    max_contracts = max_stack_ever * qty
    max_loss = max_contracts * SL * 5  # $5/pt per MES
    e2t_limit = 1100
    safe = "YES" if max_loss < e2t_limit else "NO"
    print(f"{qty:>10} {max_contracts:>14} ${max_loss:>10,.0f} ${e2t_limit:>15,.0f} {safe:>6}")

# ═══════════════════════════════════════════════════
# BY HOUR
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"BY HOUR")
print(f"{'='*90}")
print(f"\n{'Hour':>5} {'#':>4} {'L':>4} {'S':>4} {'WR':>5} {'Pts':>8} {'Avg':>6}")
print("-" * 40)
for h in sorted(set(t["hour"] for t in trades)):
    ht = [t for t in trades if t["hour"] == h]
    hp = sum(t["pts"] for t in ht)
    hw = sum(1 for t in ht if t["pts"] > 0)
    hl = sum(1 for t in ht if t["dir"] == "L")
    hs = len(ht) - hl
    print(f"{h:>4}h {len(ht):>4} {hl:>4} {hs:>4} {hw/len(ht)*100:>4.0f}% {hp:>+7.1f} {hp/len(ht):>+5.1f}")
