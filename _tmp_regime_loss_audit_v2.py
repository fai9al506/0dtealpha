"""Deep regime/loss audit v2 — use setup_log.outcome_pnl for completeness.
Cross-check against broker-resolved subset where available.

Why setup_log not broker truth: 63% of May real trades have ghost_reconcile
close_fill_price=NULL. Broker truth is incomplete. setup_log.outcome_pnl
covers all signals with SPX-side outcome label (acknowledged: has S55-class
label errors but provides directional regime signal).

Sample: All V16.1-filtered signals May 1-20 that real_trader would have
placed (regardless of whether actually placed — captures the breaker bug
and other dispatch losses too).
"""
import os, psycopg2
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
c = psycopg2.connect(os.environ["DATABASE_URL"])
cur = c.cursor()


def is_opex(d):
    first = d.replace(day=1)
    days_to_fri = (4 - first.weekday()) % 7
    return d.day == (first.day + days_to_fri + 14) and d.weekday() == 4


def passes_v161(name, dir_, paradigm, grade, align, ts):
    """V16.1 live filter approximation."""
    is_long = dir_ in ("long", "bullish")
    if name not in ("Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption", "DD Exhaustion"):
        return False
    if name == "DD Exhaustion" and not is_long:
        return False
    if name == "Skew Charm" and is_long and paradigm == "GEX-LIS":
        return False
    if name == "Skew Charm" and is_long and is_opex(ts.date()):
        return False
    if name == "AG Short" and is_opex(ts.date()):
        return False
    if name == "ES Absorption" and not is_long and ts.astimezone(ET).hour >= 14:
        return False
    if name == "Skew Charm" and grade in ("C", "LOG"):
        return False
    if name == "ES Absorption":
        if grade not in ("A", "A+"):
            return False
        if paradigm in ("AG-TARGET", "AG-LIS"):
            return False
        a = align or 0
        if is_long and a < 0: return False
        if not is_long and a > 0: return False
    if is_long and paradigm == "SIDIAL-EXTREME":
        return False
    if name == "DD Exhaustion" and is_long:
        a = align or 0
        if a >= 3 or a < 0: return False
        if paradigm in ("GEX-LIS","AG-LIS","AG-PURE","BofA-LIS","BOFA-MESSY"): return False
        if grade == "C": return False
    if is_long and name not in ("Skew Charm", "DD Exhaustion"):
        a = align or 0
        if a < 2: return False
    return True


# Pull all signals
cur.execute("""
    SELECT id, ts, ts::date AS d, setup_name, direction, paradigm, grade,
           greek_alignment, vix, vanna_regime, outcome_pnl
    FROM setup_log
    WHERE ts::date >= '2026-05-01' AND ts::date <= '2026-05-20'
      AND setup_name IN ('Skew Charm','AG Short','Vanna Pivot Bounce','ES Absorption','DD Exhaustion')
      AND notified = true AND outcome_pnl IS NOT NULL
    ORDER BY ts
""")
all_rows = cur.fetchall()

# Apply V16.1 filter
v161 = []
for r in all_rows:
    sid, ts, d, name, dir_, para, grade, align, vix, vanna, pnl = r
    if not passes_v161(name, dir_, para, grade, align, ts):
        continue
    v161.append({
        "lid": sid, "ts": ts, "d": d, "setup": name, "dir": dir_,
        "is_long": dir_ in ("long","bullish"),
        "paradigm": para, "grade": grade, "align": align or 0,
        "vix": float(vix) if vix else None, "vanna": vanna,
        "pts": float(pnl), "dollars": float(pnl) * 5.0,
    })

print(f"Loaded {len(v161)} V16.1-filtered signals May 1-20 (out of {len(all_rows)} V14-eligible)\n")


# ─── DAILY P&L (V16.1 sim basis) ───
daily = defaultdict(float)
daily_n = defaultdict(int)
for t in v161:
    daily[t["d"]] += t["dollars"]
    daily_n[t["d"]] += 1

sorted_days = sorted(daily.items(), key=lambda x: x[1])
print("=" * 75)
print("DAILY V16.1 PORTAL P&L — May 1-20 (sorted worst to best)")
print("=" * 75)
print(f"{'Date':<12} {'Day':<5} {'P&L':>10} {'Trades':>7}")
total_v161 = 0
for d, p in sorted(daily.items()):
    dow = d.strftime("%a")
    total_v161 += p
    color_marker = "<<" if p < -100 else ">>" if p > 200 else "  "
    print(f"  {d}  {dow:<3} ${p:>+8.2f}   {daily_n[d]:>4}   {color_marker}")
print(f"\n  Total V16.1 sim:  ${total_v161:+.2f}  over {len(daily)} trading days")


# ─── WEEKLY AGGREGATE ───
print()
print("=" * 75)
print("WEEKLY V16.1 P&L (Mon-Fri)")
print("=" * 75)
weekly = defaultdict(lambda: {"days":[], "pnl":0.0, "n":0, "trades":[]})
for d, p in sorted(daily.items()):
    iso = d.isocalendar()
    wk = (iso.year, iso.week)
    weekly[wk]["days"].append((d, p))
    weekly[wk]["pnl"] += p
    weekly[wk]["n"] += daily_n[d]

for wk in sorted(weekly):
    s = weekly[wk]
    first = s["days"][0][0]
    last = s["days"][-1][0]
    print(f"\n  Week {wk[1]} ({first} --> {last}): ${s['pnl']:+.2f}, {s['n']} trades")
    for d, p in s["days"]:
        print(f"    {d.strftime('%a %m-%d')}: ${p:+.2f}")


# ─── PER-SETUP x PER-WEEK ───
print()
print("=" * 75)
print("PER-SETUP x PER-WEEK CONTRIBUTION")
print("=" * 75)
ws = defaultdict(lambda: defaultdict(lambda: {"n":0,"pnl":0.0,"w":0,"l":0}))
for t in v161:
    iso = t["d"].isocalendar()
    wk = iso.week
    key = (t["setup"], t["dir"])
    ws[wk][key]["n"] += 1
    ws[wk][key]["pnl"] += t["dollars"]
    if t["dollars"] > 0: ws[wk][key]["w"] += 1
    elif t["dollars"] < 0: ws[wk][key]["l"] += 1

for wk in sorted(ws):
    week_trades = [t for t in v161 if t["d"].isocalendar().week == wk]
    if not week_trades: continue
    first, last = min(t["d"] for t in week_trades), max(t["d"] for t in week_trades)
    total = sum(s["pnl"] for s in ws[wk].values())
    print(f"\n  Week {wk} ({first} --> {last}): total ${total:+.2f}")
    for (setup, dir_), s in sorted(ws[wk].items(), key=lambda x: x[1]["pnl"]):
        wr = s["w"] / max(1, s["w"]+s["l"]) * 100
        print(f"    {setup:<18} {dir_:<8} n={s['n']:>3} WR={wr:>4.0f}% ${s['pnl']:+8.2f}")


# ─── BLEEDER PATTERNS ───
print()
print("=" * 75)
print("BLEEDER PATTERNS — (setup x direction x paradigm), worst $$ first")
print("=" * 75)
pat = defaultdict(lambda: {"n":0,"pnl":0.0,"w":0,"l":0})
for t in v161:
    key = (t["setup"], t["dir"], t["paradigm"] or "UNKNOWN")
    pat[key]["n"] += 1
    pat[key]["pnl"] += t["dollars"]
    if t["dollars"] > 0: pat[key]["w"] += 1
    elif t["dollars"] < 0: pat[key]["l"] += 1

sorted_pats = sorted([(k,v) for k,v in pat.items() if v["n"] >= 3], key=lambda x: x[1]["pnl"])
print(f"\n  Top 10 BLEEDERS (n>=3, sorted worst):")
for (setup, dir_, para), s in sorted_pats[:10]:
    wr = s["w"]/max(1,s["w"]+s["l"])*100
    print(f"    {setup:<18} {dir_:<8} {para:<18} n={s['n']:>2} WR={wr:>4.0f}% ${s['pnl']:>+8.2f}")

print(f"\n  Top 10 WINNERS:")
for (setup, dir_, para), s in sorted_pats[-10:]:
    wr = s["w"]/max(1,s["w"]+s["l"])*100
    print(f"    {setup:<18} {dir_:<8} {para:<18} n={s['n']:>2} WR={wr:>4.0f}% ${s['pnl']:>+8.2f}")


# ─── ALIGNMENT BREAKDOWN ───
print()
print("=" * 75)
print("ALIGNMENT BREAKDOWN — (setup x direction x align)")
print("=" * 75)
ap = defaultdict(lambda: {"n":0,"pnl":0.0,"w":0,"l":0})
for t in v161:
    key = (t["setup"], t["dir"], t["align"])
    ap[key]["n"] += 1
    ap[key]["pnl"] += t["dollars"]
    if t["dollars"] > 0: ap[key]["w"] += 1
    elif t["dollars"] < 0: ap[key]["l"] += 1

for (setup, dir_, a), s in sorted(ap.items(), key=lambda x: x[1]["pnl"]):
    if s["n"] >= 3:
        wr = s["w"]/max(1,s["w"]+s["l"])*100
        marker = " !! BLEED" if s["pnl"] < -50 else (" !! WIN" if s["pnl"] > 200 else "")
        print(f"  {setup:<18} {dir_:<8} align={a:+d} n={s['n']:>3} WR={wr:>4.0f}% ${s['pnl']:>+8.2f}{marker}")


# ─── HOUR / VIX / VANNA REGIME ───
print()
print("=" * 75)
print("REGIME — bad weeks vs good day (May 20)")
print("=" * 75)
# Bad week proxy: May 12-15 (Mon-Thu)
bad_dates = set(t["d"] for t in v161 if t["d"].strftime("%Y-%m-%d") in
                ("2026-05-12","2026-05-13","2026-05-14","2026-05-15"))
good_dates = {datetime.strptime("2026-05-20","%Y-%m-%d").date()}

def show_regime(dates, label):
    ts_in = [t for t in v161 if t["d"] in dates]
    if not ts_in:
        print(f"\n[{label}] no data")
        return
    pnl = sum(t["dollars"] for t in ts_in)
    wins = sum(1 for t in ts_in if t["dollars"] > 0)
    losses = sum(1 for t in ts_in if t["dollars"] < 0)
    long_n = sum(1 for t in ts_in if t["is_long"])
    avg_vix = sum(t["vix"] for t in ts_in if t["vix"]) / max(1, sum(1 for t in ts_in if t["vix"]))
    pdist = defaultdict(int)
    vdist = defaultdict(int)
    for t in ts_in:
        pdist[t["paradigm"]] += 1
        vdist[t["vanna"]] += 1
    print(f"\n[{label}]   n={len(ts_in)} P&L=${pnl:+.2f} WR={wins/max(1,wins+losses)*100:.0f}% long/short={long_n}/{len(ts_in)-long_n}")
    print(f"  avg VIX:  {avg_vix:.1f}")
    print(f"  paradigm: " + ", ".join(f"{p}={c}" for p,c in sorted(pdist.items(), key=lambda x:-x[1])[:5]))
    print(f"  vanna:    " + ", ".join(f"{v}={c}" for v,c in sorted(vdist.items(), key=lambda x:-x[1])[:3]))

show_regime(bad_dates, "BAD week May 12-15")
show_regime(good_dates, "GOOD day May 20")


# ─── V17 CANDIDATE RULES ───
print()
print("=" * 75)
print("V17 CANDIDATE BLOCK RULES — backtest on V16.1 May data")
print("=" * 75)
baseline = sum(t["dollars"] for t in v161)
print(f"\nBASELINE V16.1: ${baseline:+.2f} over {len(v161)} trades\n")

cands = []
def test(label, block_fn):
    kept = [t for t in v161 if not block_fn(t)]
    blocked = [t for t in v161 if block_fn(t)]
    if not blocked: return
    block_pnl = sum(t["dollars"] for t in blocked)
    block_wr = sum(1 for t in blocked if t["dollars"]>0) / len(blocked) * 100
    new_total = sum(t["dollars"] for t in kept)
    delta = new_total - baseline
    cands.append((label, len(blocked), block_wr, block_pnl, delta))

# Build from bleeder patterns above
test("R17-1: SC long AG-PURE",
     lambda t: t["setup"]=="Skew Charm" and t["is_long"] and t["paradigm"]=="AG-PURE")
test("R17-2: SC long AG-LIS",
     lambda t: t["setup"]=="Skew Charm" and t["is_long"] and t["paradigm"]=="AG-LIS")
test("R17-3: SC short BOFA-MESSY",
     lambda t: t["setup"]=="Skew Charm" and not t["is_long"] and t["paradigm"]=="BOFA-MESSY")
test("R17-4: DD long BOFA-MESSY",
     lambda t: t["setup"]=="DD Exhaustion" and t["is_long"] and t["paradigm"]=="BOFA-MESSY")
test("R17-5: ES Abs bearish BOFA-PURE",
     lambda t: t["setup"]=="ES Absorption" and not t["is_long"] and t["paradigm"]=="BOFA-PURE")
test("R17-6: ES Abs bullish AG-LIS",
     lambda t: t["setup"]=="ES Absorption" and t["is_long"] and t["paradigm"]=="AG-LIS")
test("R17-7: AG Short paradigm=BOFA-PURE",
     lambda t: t["setup"]=="AG Short" and t["paradigm"]=="BOFA-PURE")
test("R17-8: SC long align=-1",
     lambda t: t["setup"]=="Skew Charm" and t["is_long"] and t["align"]==-1)
test("R17-9: SC long align=0 (no Greek conviction)",
     lambda t: t["setup"]=="Skew Charm" and t["is_long"] and t["align"]==0)
test("R17-10: All longs when VIX < 17",
     lambda t: t["is_long"] and t["vix"] is not None and t["vix"] < 17)
test("R17-11: All shorts when VIX > 22",
     lambda t: not t["is_long"] and t["vix"] is not None and t["vix"] > 22)
test("R17-12: SC long hour=15",
     lambda t: t["setup"]=="Skew Charm" and t["is_long"] and t["ts"].astimezone(ET).hour == 15)

# Combined: top N rules together
print("\nTop V17 candidates ranked by P&L improvement:\n")
cands.sort(key=lambda x: -x[4])
for label, n, wr, pnl, delta in cands:
    flag = " *** TOP" if delta > 100 else ""
    print(f"  ${delta:>+7.2f}  block {n:>2} trades WR={wr:>4.0f}% ${pnl:>+7.2f}  --  {label}{flag}")

# Combined V17 = top 5 non-overlapping rules
print()
print("=" * 75)
print("COMBINED V17 = top 5 non-overlapping block rules")
print("=" * 75)
top_rules = []
for label, n, wr, pnl, delta in cands[:5]:
    if delta > 30:
        top_rules.append(label)

# Re-test combined
def passes_v17(t):
    """V16.1 + selected V17 rules."""
    if t["setup"]=="Skew Charm" and t["is_long"] and t["paradigm"]=="AG-PURE": return False
    if t["setup"]=="Skew Charm" and t["is_long"] and t["paradigm"]=="AG-LIS": return False
    if t["setup"]=="Skew Charm" and t["is_long"] and t["align"]==-1: return False
    if t["setup"]=="DD Exhaustion" and t["is_long"] and t["paradigm"]=="BOFA-MESSY": return False
    if t["setup"]=="ES Absorption" and not t["is_long"] and t["paradigm"]=="BOFA-PURE": return False
    return True

v17 = [t for t in v161 if passes_v17(t)]
v17_pnl = sum(t["dollars"] for t in v17)
blocked = len(v161) - len(v17)
print(f"\n  V16.1 baseline:  ${baseline:+.2f} ({len(v161)} trades)")
print(f"  V17 combined:    ${v17_pnl:+.2f} ({len(v17)} trades, blocked {blocked})")
print(f"  Delta:           ${v17_pnl - baseline:+.2f}")
# Per-month check
m_v161 = defaultdict(float)
m_v17 = defaultdict(float)
for t in v161:
    m_v161[t["d"].strftime("%Y-%m")] += t["dollars"]
for t in v17:
    m_v17[t["d"].strftime("%Y-%m")] += t["dollars"]
print(f"\n  Per month:")
for m in sorted(m_v161):
    print(f"    {m}: V16.1=${m_v161[m]:+.2f} --> V17=${m_v17[m]:+.2f} (delta ${m_v17[m]-m_v161[m]:+.2f})")

cur.close(); c.close()
