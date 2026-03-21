"""Deep analysis: How much did we lose, why, and would V9-SC have saved it?"""
import json, csv
from collections import defaultdict

# Load trades
with open(r"C:\Users\Faisa\AppData\Local\Temp\all_trades.json") as f:
    trades = json.load(f)

# Load VIX3M daily data
vix3m_by_date = {}
with open(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\tmp_vix_history.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        vix3m_by_date[row["Date"]] = {
            "vix_close": float(row["VIX_Close"]),
            "vix3m_close": float(row["VIX3M_Close"]),
            "overvix": float(row["Overvix"]),
        }

# Enrich trades with overvix
enriched = []
for t in trades:
    date = t.get("date", "")
    vix = t.get("vix")
    result = t.get("result")
    if result not in ("WIN", "LOSS"):
        continue
    daily = vix3m_by_date.get(date)
    if daily and vix is not None:
        overvix = vix - daily["vix3m_close"]
    elif daily:
        vix = daily["vix_close"]
        overvix = daily["overvix"]
    else:
        overvix = None
    t["overvix_calc"] = overvix
    t["vix_used"] = vix
    enriched.append(t)

# ── Filter functions ─────────────────────────────────────────
def passes_unfiltered(t): return True

def passes_v8(t):
    align = t.get("alignment", 0) or 0
    direction = t.get("direction", "")
    sname = t.get("setup_name", "")
    is_long = direction in ("long", "bullish")
    # V7+AG base
    if is_long:
        if align < 2: return False
    else:
        if sname == "Skew Charm": pass
        elif sname == "AG Short": pass
        elif sname == "DD Exhaustion" and align != 0: pass
        else: return False
    # V8 VIX gate
    vix = t.get("vix_used")
    ov = t.get("overvix_calc")
    if is_long and vix is not None and vix > 26:
        if ov is None or ov < 2: return False
    return True

def passes_v9sc(t):
    align = t.get("alignment", 0) or 0
    direction = t.get("direction", "")
    sname = t.get("setup_name", "")
    is_long = direction in ("long", "bullish")
    # V7+AG base
    if is_long:
        if align < 2: return False
    else:
        if sname == "Skew Charm": pass
        elif sname == "AG Short": pass
        elif sname == "DD Exhaustion" and align != 0: pass
        else: return False
    # V9-SC: SC exempt, VIX gate at 22
    vix = t.get("vix_used")
    ov = t.get("overvix_calc")
    if is_long and sname != "Skew Charm" and vix is not None and vix > 22:
        if ov is None or ov < 2: return False
    return True

# ── Daily analysis ───────────────────────────────────────────
all_dates = sorted(set(t["date"] for t in enriched))

print("=" * 140)
print(f"{'Date':<12} {'VIX':>5} {'OV':>6} | {'--- Unfiltered ---':^25} | {'------ V8 ------':^25} | {'---- V9-SC ----':^25} | {'V9-SC vs V8':^12}")
print(f"{'':12} {'':>5} {'':>6} | {'Tr':>4} {'W':>3} {'L':>3} {'PnL':>8} {'Cum':>8} | {'Tr':>4} {'W':>3} {'L':>3} {'PnL':>8} {'Cum':>8} | {'Tr':>4} {'W':>3} {'L':>3} {'PnL':>8} {'Cum':>8} | {'Delta':>8}")
print("=" * 140)

uf_cum = 0; v8_cum = 0; v9_cum = 0
uf_peak = 0; v8_peak = 0; v9_peak = 0

for d in all_dates:
    day_trades = [t for t in enriched if t["date"] == d]
    daily = vix3m_by_date.get(d, {})
    vix_d = daily.get("vix_close", 0)
    ov_d = daily.get("overvix", 0)

    for label, fn, cum_ref in [("uf", passes_unfiltered, "uf"), ("v8", passes_v8, "v8"), ("v9", passes_v9sc, "v9")]:
        passed = [t for t in day_trades if fn(t)]
        w = sum(1 for t in passed if t["result"] == "WIN")
        l = sum(1 for t in passed if t["result"] == "LOSS")
        pnl = sum(t.get("pnl", 0) for t in passed)
        if label == "uf":
            uf_pnl = pnl; uf_w = w; uf_l = l; uf_n = len(passed)
            uf_cum += pnl
            if uf_cum > uf_peak: uf_peak = uf_cum
        elif label == "v8":
            v8_pnl = pnl; v8_w = w; v8_l = l; v8_n = len(passed)
            v8_cum += pnl
            if v8_cum > v8_peak: v8_peak = v8_cum
        else:
            v9_pnl = pnl; v9_w = w; v9_l = l; v9_n = len(passed)
            v9_cum += pnl
            if v9_cum > v9_peak: v9_peak = v9_cum

    delta = v9_pnl - v8_pnl
    marker = " <<<" if delta > 20 else (" !!!" if delta < -20 else "")
    print(f"{d:<12} {vix_d:>5.1f} {ov_d:>+6.2f} | {uf_n:>4} {uf_w:>3} {uf_l:>3} {uf_pnl:>+8.1f} {uf_cum:>+8.1f} | "
          f"{v8_n:>4} {v8_w:>3} {v8_l:>3} {v8_pnl:>+8.1f} {v8_cum:>+8.1f} | "
          f"{v9_n:>4} {v9_w:>3} {v9_l:>3} {v9_pnl:>+8.1f} {v9_cum:>+8.1f} | {delta:>+8.1f}{marker}")

print("=" * 140)
print(f"{'TOTAL':<12} {'':>5} {'':>6} | {sum(1 for t in enriched):>4} {'':>3} {'':>3} {uf_cum:>+8.1f} {'':>8} | "
      f"{'':>4} {'':>3} {'':>3} {v8_cum:>+8.1f} {'':>8} | "
      f"{'':>4} {'':>3} {'':>3} {v9_cum:>+8.1f} {'':>8} | {v9_cum-v8_cum:>+8.1f}")
print(f"\nPeak:  Unfiltered={uf_peak:+.1f}  V8={v8_peak:+.1f}  V9-SC={v9_peak:+.1f}")
print(f"Current drawdown from peak:  Unfiltered={uf_cum-uf_peak:+.1f}  V8={v8_cum-v8_peak:+.1f}  V9-SC={v9_cum-v9_peak:+.1f}")

# ── Losing streak deep dive ─────────────────────────────────
print("\n\n" + "=" * 100)
print("LOSING DAYS DEEP DIVE (Mar 11-17)")
print("=" * 100)

losing_dates = ["2026-03-11", "2026-03-12", "2026-03-13", "2026-03-16", "2026-03-17"]
total_v8_loss = 0
total_v9_loss = 0
total_v9_saved = 0

for d in losing_dates:
    day_trades = [t for t in enriched if t["date"] == d]
    daily = vix3m_by_date.get(d, {})
    print(f"\n--- {d} | VIX={daily.get('vix_close',0):.1f} VIX3M={daily.get('vix3m_close',0):.1f} OV={daily.get('overvix',0):+.2f} ---")

    v8_passed = [t for t in day_trades if passes_v8(t)]
    v9_passed = [t for t in day_trades if passes_v9sc(t)]
    v9_blocked = [t for t in day_trades if passes_v8(t) and not passes_v9sc(t)]

    v8_day_pnl = sum(t.get("pnl", 0) for t in v8_passed)
    v9_day_pnl = sum(t.get("pnl", 0) for t in v9_passed)
    total_v8_loss += v8_day_pnl
    total_v9_loss += v9_day_pnl

    v8_w = sum(1 for t in v8_passed if t["result"] == "WIN")
    v8_l = sum(1 for t in v8_passed if t["result"] == "LOSS")
    v9_w = sum(1 for t in v9_passed if t["result"] == "WIN")
    v9_l = sum(1 for t in v9_passed if t["result"] == "LOSS")

    print(f"  V8:    {len(v8_passed)}t  {v8_w}W/{v8_l}L  {v8_day_pnl:+.1f} pts")
    print(f"  V9-SC: {len(v9_passed)}t  {v9_w}W/{v9_l}L  {v9_day_pnl:+.1f} pts")
    print(f"  Delta: {v9_day_pnl - v8_day_pnl:+.1f} pts")

    if v9_blocked:
        blk_pnl = sum(t.get("pnl", 0) for t in v9_blocked)
        blk_w = sum(1 for t in v9_blocked if t["result"] == "WIN")
        blk_l = sum(1 for t in v9_blocked if t["result"] == "LOSS")
        total_v9_saved += blk_pnl if blk_pnl < 0 else 0  # count savings
        print(f"  V9-SC blocked {len(v9_blocked)} trades ({blk_w}W/{blk_l}L, {blk_pnl:+.1f} pts):")
        by_setup = defaultdict(list)
        for t in v9_blocked:
            by_setup[t["setup_name"]].append(t)
        for sname in sorted(by_setup.keys()):
            ts = by_setup[sname]
            sw = sum(1 for t in ts if t["result"] == "WIN")
            sl = sum(1 for t in ts if t["result"] == "LOSS")
            sp = sum(t.get("pnl", 0) for t in ts)
            dir_str = ts[0].get("direction", "?")
            print(f"    {sname:<22} {len(ts)}t {sw}W/{sl}L {sp:+.1f} pts ({dir_str})")

print(f"\n{'=' * 60}")
print(f"LOSING STREAK TOTALS (Mar 11-17):")
print(f"  V8 total:    {total_v8_loss:+.1f} pts")
print(f"  V9-SC total: {total_v9_loss:+.1f} pts")
print(f"  V9-SC saves: {total_v9_loss - total_v8_loss:+.1f} pts")

# ── What happened to the +700 peak? ─────────────────────────
print(f"\n\n{'=' * 80}")
print("WHERE DID THE PROFIT GO? (Unfiltered view)")
print("=" * 80)
cum = 0
peak = 0
peak_date = ""
for d in all_dates:
    day_trades = [t for t in enriched if t["date"] == d]
    pnl = sum(t.get("pnl", 0) for t in day_trades)
    cum += pnl
    if cum > peak:
        peak = cum
        peak_date = d
    if d >= "2026-03-09":
        dd = cum - peak
        print(f"  {d}: day={pnl:+.1f}  cumulative={cum:+.1f}  drawdown={dd:+.1f}")

print(f"\n  Peak: {peak:+.1f} pts on {peak_date}")
print(f"  Current: {cum:+.1f} pts")
print(f"  Lost from peak: {cum - peak:+.1f} pts")

# ── Setup-level blame for the drawdown ───────────────────────
print(f"\n\n{'=' * 80}")
print("SETUP-LEVEL BLAME (Mar 11-17 unfiltered)")
print("=" * 80)
blame = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0, "long_pnl": 0, "short_pnl": 0})
for t in enriched:
    if t["date"] < "2026-03-11": continue
    s = t["setup_name"]
    blame[s]["pnl"] += t.get("pnl", 0)
    if t["result"] == "WIN": blame[s]["w"] += 1
    else: blame[s]["l"] += 1
    if t.get("direction", "") in ("long", "bullish"):
        blame[s]["long_pnl"] += t.get("pnl", 0)
    else:
        blame[s]["short_pnl"] += t.get("pnl", 0)

for s in sorted(blame.keys(), key=lambda x: blame[x]["pnl"]):
    b = blame[s]
    n = b["w"] + b["l"]
    wr = b["w"] / n * 100 if n else 0
    print(f"  {s:<22} {n:>3}t  {b['w']}W/{b['l']}L  {wr:.0f}% WR  {b['pnl']:>+8.1f} pts  (longs:{b['long_pnl']:>+.1f} shorts:{b['short_pnl']:>+.1f})")
