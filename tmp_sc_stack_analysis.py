"""SC stacking analysis — do stacked signals perform better?
Track concurrent open positions, tag each trade with its stack depth."""
import sqlalchemy, json
from sqlalchemy import text
from collections import defaultdict
from datetime import timedelta

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
engine = sqlalchemy.create_engine(DB_URL)

with engine.begin() as conn:
    rows = conn.execute(text("""
        SELECT id, setup_name, direction, outcome_result, outcome_pnl,
               outcome_elapsed_min, ts, vix, overvix, greek_alignment,
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

trades = []
for s in rows:
    if not passes(s): continue
    trades.append({
        "id": s["id"], "date": str(s["trade_date"]),
        "dir": "L" if s["direction"].lower() in ("long", "bullish") else "S",
        "outcome": s["outcome_result"],
        "pts": float(s["outcome_pnl"] or 0),
        "ts": s["ts"],
        "elapsed": float(s["outcome_elapsed_min"] or 20),
        "end_ts": s["ts"] + timedelta(minutes=float(s["outcome_elapsed_min"] or 20)),
    })

# ═══════════════════════════════════════════════════
# Calculate stack depth for each trade
# Stack = how many same-direction trades are OPEN when this trade enters
# ═══════════════════════════════════════════════════
for i, t in enumerate(trades):
    # Count how many earlier trades (same direction) are still open when t enters
    concurrent = 0
    for j in range(i):
        prev = trades[j]
        if prev["dir"] == t["dir"] and prev["end_ts"] > t["ts"] and prev["date"] == t["date"]:
            concurrent += 1
    t["stack"] = concurrent + 1  # this trade is the Nth in the stack

max_stack = max(t["stack"] for t in trades)

print(f"SC STACKING ANALYSIS — {len(trades)} trades, {len(set(t['date'] for t in trades))} days")
print(f"Max stack depth: {max_stack}")

# ═══════════════════════════════════════════════════
# Stack depth distribution
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"BY STACK DEPTH (1 = no overlap, 2+ = stacked)")
print(f"{'='*90}")
print(f"\n{'Stack':>5} {'#':>4} {'W':>4} {'L':>4} {'WR':>5} {'Pts':>8} {'Avg':>6} {'AvgW':>6} {'AvgL':>6}")
print("-" * 55)

for depth in range(1, max_stack + 1):
    st = [t for t in trades if t["stack"] == depth]
    if not st: continue
    sp = sum(t["pts"] for t in st)
    sw = sum(1 for t in st if t["pts"] > 0)
    sl = sum(1 for t in st if t["pts"] < 0)
    wp = [t["pts"] for t in st if t["pts"] > 0]
    lp = [t["pts"] for t in st if t["pts"] < 0]
    aw = sum(wp)/len(wp) if wp else 0
    al = sum(lp)/len(lp) if lp else 0
    print(f"{depth:>5} {len(st):>4} {sw:>4} {sl:>4} {sw/len(st)*100:>4.0f}% {sp:>+7.1f} {sp/len(st):>+5.1f} {aw:>+5.1f} {al:>+5.1f}")

# Grouped: 1, 2, 3, 4+
print(f"\n{'Group':>7} {'#':>4} {'WR':>5} {'Pts':>8} {'Avg':>6}")
print("-" * 35)
for lo, hi, label in [(1, 1, "Solo"), (2, 2, "2-deep"), (3, 3, "3-deep"), (4, 99, "4+ deep")]:
    st = [t for t in trades if lo <= t["stack"] <= hi]
    if not st: continue
    sp = sum(t["pts"] for t in st)
    sw = sum(1 for t in st if t["pts"] > 0)
    print(f"{label:>7} {len(st):>4} {sw/len(st)*100:>4.0f}% {sp:>+7.1f} {sp/len(st):>+5.1f}")

# ═══════════════════════════════════════════════════
# How often does stacking happen?
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"STACKING FREQUENCY")
print(f"{'='*90}")

solo = sum(1 for t in trades if t["stack"] == 1)
stacked = sum(1 for t in trades if t["stack"] >= 2)
deep = sum(1 for t in trades if t["stack"] >= 4)
print(f"Solo trades (no overlap):    {solo} ({solo/len(trades)*100:.0f}%)")
print(f"Stacked (2+ concurrent):     {stacked} ({stacked/len(trades)*100:.0f}%)")
print(f"Deep stacked (4+ concurrent): {deep} ({deep/len(trades)*100:.0f}%)")

# ═══════════════════════════════════════════════════
# Daily stacking detail
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"DAILY STACKING DETAIL")
print(f"{'='*90}")
print(f"\n{'Date':<11} {'#':>3} {'Max Stack':>10} {'Solo':>5} {'2+':>4} {'4+':>4} {'Pts':>8} {'Solo Pts':>9} {'Stack Pts':>10}")
print("-" * 75)

for d in sorted(set(t["date"] for t in trades)):
    dt = [t for t in trades if t["date"] == d]
    ms = max(t["stack"] for t in dt)
    solo_t = [t for t in dt if t["stack"] == 1]
    stack_t = [t for t in dt if t["stack"] >= 2]
    deep_t = [t for t in dt if t["stack"] >= 4]
    tp = sum(t["pts"] for t in dt)
    sp = sum(t["pts"] for t in solo_t)
    stp = sum(t["pts"] for t in stack_t)
    print(f"{d:<11} {len(dt):>3} {ms:>10} {len(solo_t):>5} {len(stack_t):>4} {len(deep_t):>4} {tp:>+7.1f} {sp:>+8.1f} {stp:>+9.1f}")

# ═══════════════════════════════════════════════════
# Each stacking EVENT: show the cluster
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"ALL STACK CLUSTERS (3+ concurrent)")
print(f"{'='*90}")

# Find clusters: group overlapping same-direction trades
for d in sorted(set(t["date"] for t in trades)):
    dt = [t for t in trades if t["date"] == d]
    for direction in ["L", "S"]:
        dir_t = [t for t in dt if t["dir"] == direction]
        if len(dir_t) < 3: continue

        # Find overlapping groups
        clusters = []
        for t in dir_t:
            placed = False
            for c in clusters:
                # Check if t overlaps with any trade in cluster
                for ct in c:
                    if t["ts"] < ct["end_ts"] and t["end_ts"] > ct["ts"]:
                        c.append(t)
                        placed = True
                        break
                if placed: break
            if not placed:
                clusters.append([t])

        for c in clusters:
            if len(c) < 3: continue
            cp = sum(t["pts"] for t in c)
            cw = sum(1 for t in c if t["pts"] > 0)
            dir_name = "LONG" if direction == "L" else "SHORT"
            print(f"\n  {d} {dir_name} cluster: {len(c)} trades, {cw}W/{len(c)-cw}L, {cp:+.1f} pts")
            for t in c:
                print(f"    #{t['id']:<5} {t['outcome']:<5} {t['pts']:>+6.1f}pts  stack={t['stack']}  held {t['elapsed']:.0f}m")

# ═══════════════════════════════════════════════════
# BOTTOM LINE: cap at N concurrent vs uncapped
# ═══════════════════════════════════════════════════
print(f"\n{'='*90}")
print(f"WHAT-IF: cap max concurrent trades")
print(f"{'='*90}")

for cap in [999, 5, 4, 3, 2, 1]:
    # Simulate: skip trades that exceed cap
    sim_trades = []
    for t in trades:
        if t["stack"] <= cap:
            sim_trades.append(t)

    sp = sum(t["pts"] for t in sim_trades)
    sw = sum(1 for t in sim_trades if t["pts"] > 0)
    sl = sum(1 for t in sim_trades if t["pts"] < 0)
    ndays = len(set(t["date"] for t in sim_trades)) or 1

    # Max same-dir stacked
    max_exposed = min(cap, max_stack)

    label = "NO CAP" if cap == 999 else f"Max {cap}"
    wr = sw/len(sim_trades)*100 if sim_trades else 0
    print(f"  {label:<8} {len(sim_trades):>4} trades  {sw}W/{sl}L  WR:{wr:.0f}%  "
          f"{sp:>+7.1f} pts  {sp/ndays:>+5.1f}/day  "
          f"max exposed: {max_exposed} trades")
