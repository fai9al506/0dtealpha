"""Real spread analysis using actual chain_snapshots bid/ask prices."""
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

print(f"Total trades loaded: {len(all_trades)}")
print(f"Trades with debit spread data: {sum(1 for t in all_trades if t.get('debit_pnl') is not None)}")
print(f"Trades with credit spread data: {sum(1 for t in all_trades if t.get('credit_pnl') is not None)}")

# Filter V9-SC only
v9 = [t for t in all_trades if t.get("v9sc")]
print(f"V9-SC filtered trades: {len(v9)}")

# ── Daily summary ────────────────────────────────────────────
print(f"\n{'=' * 110}")
print(f"{'Date':<12} | {'Naked':>10} | {'Debit':>10} {'(entry>exit)':>16} | {'Credit':>10} {'(entry>exit)':>16} | {'Best':>6}")
print(f"{'=' * 110}")

naked_cum = 0; debit_cum = 0; credit_cum = 0
daily_data = []

for date in sorted(set(t["date"] for t in v9)):
    day = [t for t in v9 if t["date"] == date]
    n_pnl = sum(t["naked_pnl"] for t in day)
    d_pnl = sum(t["debit_pnl"] for t in day if t.get("debit_pnl") is not None)
    c_pnl = sum(t["credit_pnl"] for t in day if t.get("credit_pnl") is not None)
    d_count = sum(1 for t in day if t.get("debit_pnl") is not None)
    c_count = sum(1 for t in day if t.get("credit_pnl") is not None)
    naked_cum += n_pnl; debit_cum += d_pnl; credit_cum += c_pnl

    best = "N" if n_pnl >= d_pnl and n_pnl >= c_pnl else ("D" if d_pnl >= c_pnl else "C")
    print(f"{date:<12} | ${n_pnl:>+9,.0f} | ${d_pnl:>+9,.0f} ({d_count}t)          | ${c_pnl:>+9,.0f} ({c_count}t)          | {best}")
    daily_data.append({"date": date, "naked": n_pnl, "debit": d_pnl, "credit": c_pnl})

print(f"{'-' * 110}")
print(f"{'CUMULATIVE':<12} | ${naked_cum:>+9,.0f} | ${debit_cum:>+9,.0f}                  | ${credit_cum:>+9,.0f}                  |")

# ── Strategy comparison ──────────────────────────────────────
print(f"\n\n{'=' * 90}")
print("STRATEGY COMPARISON (V9-SC, real chain prices, both legs bid/ask)")
print(f"{'=' * 90}")

for label, key in [("Naked Long", "naked_pnl"), ("Debit Spread", "debit_pnl"), ("Credit Spread", "credit_pnl")]:
    valid = [t for t in v9 if t.get(key) is not None]
    total = sum(t[key] for t in valid)
    wins = sum(1 for t in valid if t[key] >= 0)
    losses = len(valid) - wins
    wr = wins / len(valid) * 100 if valid else 0

    w_pnl = sum(t[key] for t in valid if t[key] >= 0)
    l_pnl = sum(t[key] for t in valid if t[key] < 0)
    avg_w = w_pnl / max(1, wins)
    avg_l = l_pnl / max(1, losses)
    ratio = abs(avg_w / avg_l) if avg_l != 0 else 999
    be_wr = abs(avg_l) / (abs(avg_w) + abs(avg_l)) * 100 if (abs(avg_w) + abs(avg_l)) > 0 else 0
    edge = wr - be_wr

    print(f"\n{label}:")
    print(f"  Trades: {len(valid)}  |  Wins: {wins}  |  Losses: {losses}  |  WR: {wr:.0f}%")
    print(f"  SPXW PnL: ${total:>+,.0f}  |  SPY equiv: ${total/10:>+,.0f}")
    print(f"  Avg WIN: ${avg_w:>+,.0f}  |  Avg LOSS: ${avg_l:>+,.0f}  |  Ratio: {ratio:.2f}x")
    print(f"  Break-even WR: {be_wr:.0f}%  |  Actual: {wr:.0f}%  |  Edge: {edge:+.0f}% {'PROFITABLE' if edge > 0 else 'LOSING'}")
    if len(daily_data) > 0:
        k = label.split()[0].lower()
        daily_pnls = [d[k] if k != "naked" else d["naked"] for d in daily_data]
        pos_days = sum(1 for x in daily_pnls if x > 0)
        print(f"  Positive days: {pos_days}/{len(daily_data)}")

# ── Per-setup breakdown ──────────────────────────────────────
print(f"\n\n{'=' * 90}")
print("PER-SETUP (V9-SC, real chain prices)")
print(f"{'=' * 90}")

setups = sorted(set(t["setup"] for t in v9))
print(f"\n{'Setup':<22} | {'Naked':>10} | {'Debit':>10} | {'Credit':>10}")
print(f"{'-' * 65}")
for s in setups:
    st = [t for t in v9 if t["setup"] == s]
    n = sum(t["naked_pnl"] for t in st)
    d = sum(t["debit_pnl"] for t in st if t.get("debit_pnl") is not None)
    c = sum(t["credit_pnl"] for t in st if t.get("credit_pnl") is not None)
    cnt = len(st)
    print(f"  {s:<20} | ${n:>+9,.0f} | ${d:>+9,.0f} | ${c:>+9,.0f}  ({cnt}t)")

# ── Sample trades to verify prices ──────────────────────────
print(f"\n\n{'=' * 90}")
print("SAMPLE TRADES (verify real prices)")
print(f"{'=' * 90}")
count = 0
for t in v9:
    if t.get("debit_pnl") is not None and t.get("credit_pnl") is not None and count < 10:
        print(f"\n  #{t['id']} {t['setup']} {t['dir']} | SPX {t['outcome']} {t['spx_pnl']:+.1f}pts")
        print(f"    Naked:  {t['side']} {t['naked_strike']:.0f} | entry ${t['naked_entry']:.2f} > exit ${t['naked_exit']:.2f} | PnL ${t['naked_pnl']:+,.0f}")
        print(f"    Debit:  {t.get('debit_long_strike','?')}-{t.get('debit_short_strike','?')} | {t['debit_entry']} > {t['debit_exit']} | PnL ${t['debit_pnl']:+,.0f}")
        print(f"    Credit: {t.get('credit_short_strike','?')}-{t.get('credit_long_strike','?')} | {t['credit_entry']} > {t['credit_exit']} | PnL ${t['credit_pnl']:+,.0f}")
        count += 1
