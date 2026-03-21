"""Compare 0.45 delta vs 0.30 delta naked long options using real chain prices."""
import json, glob
from collections import defaultdict

dates = sorted(glob.glob("tmp_chain_2026-03-*.json"))
all_trades = []
for fp in dates:
    with open(fp) as f:
        d = json.load(f)
    for t in d.get("trades", []):
        t["date"] = d.get("date", fp[-15:-5])
        all_trades.append(t)

v9 = [t for t in all_trades if t.get("v9sc")]

daily_45 = defaultdict(float)
daily_30 = defaultdict(float)
setup_45 = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0})
setup_30 = defaultdict(lambda: {"pnl": 0, "count": 0, "w": 0, "l": 0})
total_45 = 0; total_30 = 0
w45 = 0; l45 = 0; w30 = 0; l30 = 0
all_45_pnls = []; all_30_pnls = []
count = 0

for t in v9:
    de = t.get("debit_entry", "")
    dx = t.get("debit_exit", "")
    if not de or not dx or "=" not in de or "=" not in dx:
        continue
    try:
        entry_45 = float(de.split("-")[0])
        exit_45 = float(dx.split("=")[0].split("-")[0])
    except:
        continue

    pnl_45 = (exit_45 - entry_45) * 100
    pnl_30 = t["naked_pnl"]

    total_45 += pnl_45; total_30 += pnl_30
    daily_45[t["date"]] += pnl_45
    daily_30[t["date"]] += pnl_30
    all_45_pnls.append(pnl_45)
    all_30_pnls.append(pnl_30)

    s = t["setup"]
    setup_45[s]["pnl"] += pnl_45; setup_45[s]["count"] += 1
    setup_30[s]["pnl"] += pnl_30; setup_30[s]["count"] += 1
    if pnl_45 >= 0:
        w45 += 1; setup_45[s]["w"] += 1
    else:
        l45 += 1; setup_45[s]["l"] += 1
    if pnl_30 >= 0:
        w30 += 1; setup_30[s]["w"] += 1
    else:
        l30 += 1; setup_30[s]["l"] += 1
    count += 1

wr45 = w45 / count * 100
wr30 = w30 / count * 100

w45p = [p for p in all_45_pnls if p >= 0]
l45p = [p for p in all_45_pnls if p < 0]
w30p = [p for p in all_30_pnls if p >= 0]
l30p = [p for p in all_30_pnls if p < 0]

aw45 = sum(w45p) / max(1, len(w45p))
al45 = sum(l45p) / max(1, len(l45p))
aw30 = sum(w30p) / max(1, len(w30p))
al30 = sum(l30p) / max(1, len(l30p))

r45 = abs(aw45 / al45) if al45 else 999
r30 = abs(aw30 / al30) if al30 else 999
be45 = abs(al45) / (abs(aw45) + abs(al45)) * 100 if (abs(aw45) + abs(al45)) else 0
be30 = abs(al30) / (abs(aw30) + abs(al30)) * 100 if (abs(aw30) + abs(al30)) else 0

print("=" * 95)
print("NAKED LONG: 0.45 Delta vs 0.30 Delta (V9-SC, real chain prices)")
print("=" * 95)
print(f"Trades: {count}")
print()
headers = f"{'Strategy':<25} {'WR':>6} {'SPXW PnL':>12} {'SPY':>8} {'$/day':>8} {'AvgW':>8} {'AvgL':>8} {'Ratio':>7} {'BE WR':>6} {'Edge':>6}"
print(headers)
print("-" * 95)
print(f"{'0.30 delta (current)':<25} {wr30:>5.0f}% ${total_30:>+11,.0f} ${total_30/10:>+7,.0f} ${total_30/10/12:>+7,.0f} ${aw30:>+7,.0f} ${al30:>+7,.0f} {r30:>6.2f}x {be30:>5.0f}% {wr30-be30:>+5.0f}%")
print(f"{'0.45 delta (proposed)':<25} {wr45:>5.0f}% ${total_45:>+11,.0f} ${total_45/10:>+7,.0f} ${total_45/10/12:>+7,.0f} ${aw45:>+7,.0f} ${al45:>+7,.0f} {r45:>6.2f}x {be45:>5.0f}% {wr45-be45:>+5.0f}%")
print(f"{'Improvement':<25} {wr45-wr30:>+5.0f}% ${total_45-total_30:>+11,.0f} ${(total_45-total_30)/10:>+7,.0f}")

print(f"\nDaily:")
print(f"{'Date':<12} {'0.30d':>10} {'0.45d':>10} {'Delta':>10} | {'0.30 Cum':>10} {'0.45 Cum':>10}")
print("-" * 65)
c30 = 0; c45 = 0
for d in sorted(set(list(daily_45.keys()) + list(daily_30.keys()))):
    d30 = daily_30.get(d, 0); d45 = daily_45.get(d, 0)
    c30 += d30; c45 += d45
    delta = d45 - d30
    m = " <<<" if delta > 500 else (" !!!" if delta < -500 else "")
    print(f"{d:<12} ${d30:>+9,.0f} ${d45:>+9,.0f} ${delta:>+9,.0f} | ${c30:>+9,.0f} ${c45:>+9,.0f}{m}")

print(f"\nPer setup:")
print(f"{'Setup':<22} {'0.30 PnL':>10} {'0.45 PnL':>10} {'0.45 WR':>8} {'0.30 WR':>8}")
print("-" * 60)
for s in sorted(setup_45.keys(), key=lambda x: setup_45[x]["pnl"], reverse=True):
    s45 = setup_45[s]; s30 = setup_30[s]
    wr_s45 = s45["w"] / s45["count"] * 100 if s45["count"] else 0
    wr_s30 = s30["w"] / s30["count"] * 100 if s30["count"] else 0
    print(f"  {s:<20} ${s30['pnl']:>+9,.0f} ${s45['pnl']:>+9,.0f} {wr_s45:>7.0f}% {wr_s30:>7.0f}%")
