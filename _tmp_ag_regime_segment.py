# AG Short regime segmentation (2026-06-07): where does AG win vs lose?
# Data: 123 graded+resolved AG Short signals Feb-Jun (_tmp_ag_regime_data.json,
# portal chain-sim outcome_pnl) + daily SPX open/close (_tmp_ag_day_context.json).
import json
from datetime import date, timedelta

trades = json.load(open("_tmp_ag_regime_data.json"))
days = json.load(open("_tmp_ag_day_context.json"))
daykeys = sorted(days.keys())

def prior_close(d):
    i = daykeys.index(d) if d in daykeys else None
    if i is None or i == 0:
        return None
    return days[daykeys[i-1]]["close"]

def seg(label, items):
    n = len(items)
    if n == 0:
        print(f"  {label:<28} n=0")
        return
    w = sum(1 for t in items if t["res"] == "WIN")
    l = sum(1 for t in items if t["res"] == "LOSS")
    pnl = sum(t["pnl"] for t in items if t["pnl"] is not None)
    wr = 100*w/max(1, w+l)
    avg = pnl/n
    print(f"  {label:<28} n={n:>3}  WR={wr:>3.0f}%  pnl={pnl:>+7.1f}  avg={avg:>+5.2f}")

for t in trades:
    d = t["et"][:10]
    t["date"] = d
    t["hh"] = t["et"][11:16]
    dc = days.get(d)
    t["day_chg"] = (t["spot"] - dc["open"]) if dc and t["spot"] else None      # intraday move at signal
    pc = prior_close(d)
    t["gap"] = (dc["open"] - pc) if dc and pc else None                        # overnight gap
    t["day_final"] = (dc["close"] - dc["open"]) if dc else None                # how the day ended
    t["mo"] = d[:7]

print("=== BY MONTH (baseline) ===")
for mo in sorted(set(t["mo"] for t in trades)):
    seg(mo, [t for t in trades if t["mo"] == mo])

print("\n=== VIX BUCKET ===")
seg("VIX >= 25", [t for t in trades if t["vix"] and t["vix"] >= 25])
seg("VIX 20-25", [t for t in trades if t["vix"] and 20 <= t["vix"] < 25])
seg("VIX < 20", [t for t in trades if t["vix"] and t["vix"] < 20])
seg("VIX null", [t for t in trades if t["vix"] is None])

print("\n=== PARADIGM SUBTYPE ===")
for p in ["AG-PURE", "AG-LIS", "AG-TARGET"]:
    seg(p, [t for t in trades if t["par"] == p])

print("\n=== INTRADAY TREND AT SIGNAL (spot vs day open) ===")
seg("down > 15 (selloff day)", [t for t in trades if t["day_chg"] is not None and t["day_chg"] < -15])
seg("down 0-15", [t for t in trades if t["day_chg"] is not None and -15 <= t["day_chg"] < 0])
seg("up 0-15", [t for t in trades if t["day_chg"] is not None and 0 <= t["day_chg"] <= 15])
seg("up > 15 (rally day)", [t for t in trades if t["day_chg"] is not None and t["day_chg"] > 15])

print("\n=== HOW THE DAY ENDED (close vs open) — hindsight regime check ===")
seg("down day (close<open-10)", [t for t in trades if t["day_final"] is not None and t["day_final"] < -10])
seg("flat day (+/-10)", [t for t in trades if t["day_final"] is not None and -10 <= t["day_final"] <= 10])
seg("up day (close>open+10)", [t for t in trades if t["day_final"] is not None and t["day_final"] > 10])

print("\n=== TIME OF DAY ===")
seg("09:30-10:30", [t for t in trades if "09:30" <= t["hh"] < "10:30"])
seg("10:30-12:00", [t for t in trades if "10:30" <= t["hh"] < "12:00"])
seg("12:00-14:00", [t for t in trades if "12:00" <= t["hh"] < "14:00"])
seg("14:00-16:00", [t for t in trades if t["hh"] >= "14:00"])

print("\n=== GAP TO LIS (entry distance below LIS) ===")
seg("near LIS (0-8)", [t for t in trades if t["gap_lis"] is not None and abs(t["gap_lis"]) <= 8])
seg("mid (8-15)", [t for t in trades if t["gap_lis"] is not None and 8 < abs(t["gap_lis"]) <= 15])
seg("far (>15, stop=cap20)", [t for t in trades if t["gap_lis"] is not None and abs(t["gap_lis"]) > 15])

print("\n=== ALIGNMENT ===")
for a in sorted(set(t["align"] for t in trades if t["align"] is not None)):
    seg(f"align {a:+d}", [t for t in trades if t["align"] == a])
seg("align null", [t for t in trades if t["align"] is None])

print("\n=== OVERVIX ===")
seg("ovx >= +1 (stress)", [t for t in trades if t["ovx"] is not None and t["ovx"] >= 1])
seg("ovx -1..+1", [t for t in trades if t["ovx"] is not None and -1 <= t["ovx"] < 1])
seg("ovx < -1 (calm)", [t for t in trades if t["ovx"] is not None and t["ovx"] < -1])

print("\n=== COMBO: the March signature — VIX>=20 AND down-trend at signal ===")
seg("VIX>=20 & day_chg<0", [t for t in trades if t["vix"] and t["vix"] >= 20 and t["day_chg"] is not None and t["day_chg"] < 0])
seg("VIX>=20 & day_chg>=0", [t for t in trades if t["vix"] and t["vix"] >= 20 and t["day_chg"] is not None and t["day_chg"] >= 0])
seg("VIX<20 & day_chg<0", [t for t in trades if t["vix"] and t["vix"] < 20 and t["day_chg"] is not None and t["day_chg"] < 0])
seg("VIX<20 & day_chg>=0", [t for t in trades if t["vix"] and t["vix"] < 20 and t["day_chg"] is not None and t["day_chg"] >= 0])

print("\n=== MAY-JUN ONLY: any segment still green? ===")
mj = [t for t in trades if t["mo"] >= "2026-05"]
seg("ALL May-Jun", mj)
seg("  down-trend at signal", [t for t in mj if t["day_chg"] is not None and t["day_chg"] < 0])
seg("  up-trend at signal", [t for t in mj if t["day_chg"] is not None and t["day_chg"] >= 0])
seg("  AG-PURE", [t for t in mj if t["par"] == "AG-PURE"])
seg("  AG-LIS", [t for t in mj if t["par"] == "AG-LIS"])
seg("  AG-TARGET", [t for t in mj if t["par"] == "AG-TARGET"])
seg("  near LIS (<=8)", [t for t in mj if t["gap_lis"] is not None and abs(t["gap_lis"]) <= 8])
seg("  far LIS (>15)", [t for t in mj if t["gap_lis"] is not None and abs(t["gap_lis"]) > 15])
seg("  morning (<12)", [t for t in mj if t["hh"] < "12:00"])
seg("  afternoon (>=12)", [t for t in mj if t["hh"] >= "12:00"])

print("\n=== MARCH ONLY: what made it golden ===")
mar = [t for t in trades if t["mo"] == "2026-03"]
seg("ALL March", mar)
seg("  down-trend at signal", [t for t in mar if t["day_chg"] is not None and t["day_chg"] < 0])
seg("  up-trend at signal", [t for t in mar if t["day_chg"] is not None and t["day_chg"] >= 0])
vals = sorted(t["vix"] for t in mar if t["vix"])
print(f"  March VIX range: {vals[0]:.1f} - {vals[-1]:.1f} (median {vals[len(vals)//2]:.1f})" if vals else "  no vix")
vals = sorted(t["vix"] for t in mj if t["vix"])
print(f"  May-Jun VIX range: {vals[0]:.1f} - {vals[-1]:.1f} (median {vals[len(vals)//2]:.1f})" if vals else "")
