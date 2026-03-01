"""Deep trade analysis across all historical data."""
import sys, os, json
from collections import defaultdict
from sqlalchemy import create_engine, text

engine = create_engine(os.getenv('DATABASE_URL'))

# Pull all trades
with engine.connect() as conn:
    rows = conn.execute(text("""
        SELECT id, setup_name, direction, grade, score, spot, paradigm, lis,
               outcome_result, outcome_pnl, outcome_max_profit, outcome_max_loss,
               outcome_target_level, outcome_stop_level, outcome_elapsed_min,
               outcome_first_event, vix,
               ts AT TIME ZONE 'America/New_York' as ts_et,
               (ts AT TIME ZONE 'America/New_York')::date as trade_date,
               EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') as hour_et,
               EXTRACT(MINUTE FROM ts AT TIME ZONE 'America/New_York') as min_et
        FROM setup_log
        WHERE outcome_result IS NOT NULL
        ORDER BY ts
    """)).mappings().all()

trades = []
for r in rows:
    t = {
        "id": r["id"],
        "date": str(r["trade_date"]),
        "setup": r["setup_name"],
        "direction": r["direction"],
        "grade": r["grade"],
        "score": float(r["score"]),
        "spot": float(r["spot"]),
        "paradigm": r["paradigm"] or "",
        "result": r["outcome_result"],
        "pnl": float(r["outcome_pnl"] or 0),
        "max_profit": float(r["outcome_max_profit"] or 0),
        "max_loss": float(r["outcome_max_loss"] or 0),
        "first_event": r["outcome_first_event"] or "",
        "elapsed": int(r["outcome_elapsed_min"] or 0),
        "target": r["outcome_target_level"],
        "stop": r["outcome_stop_level"],
        "vix": float(r["vix"]) if r["vix"] else None,
        "hour": int(r["hour_et"]),
        "minute": int(r["min_et"]),
        "hour_decimal": int(r["hour_et"]) + int(r["min_et"])/60.0,
    }
    trades.append(t)

print(f"Parsed {len(trades)} trades\n")

# ============================================================
print("=" * 70)
print("1. OVERALL SUMMARY")
print("=" * 70)
total_pnl = sum(t["pnl"] for t in trades)
wins = [t for t in trades if t["result"] == "WIN"]
losses = [t for t in trades if t["result"] == "LOSS"]
expired = [t for t in trades if t["result"] == "EXPIRED"]
print(f"Total trades: {len(trades)}")
print(f"Wins: {len(wins)}, Losses: {len(losses)}, Expired: {len(expired)}")
wr = len(wins)/(len(wins)+len(losses))*100 if (len(wins)+len(losses)) > 0 else 0
print(f"Win rate (W/(W+L)): {wr:.1f}%")
print(f"Total P&L: {total_pnl:+.1f} pts")
print(f"Avg P&L per trade: {total_pnl/len(trades):+.2f} pts")
print(f"Avg WIN: {sum(t['pnl'] for t in wins)/len(wins):+.1f} pts")
print(f"Avg LOSS: {sum(t['pnl'] for t in losses)/len(losses):+.1f} pts")
print(f"Avg EXPIRED P&L: {sum(t['pnl'] for t in expired)/len(expired) if expired else 0:+.2f} pts")

# ============================================================
print("\n" + "=" * 70)
print("2. PER-SETUP BREAKDOWN")
print("=" * 70)
setups = defaultdict(list)
for t in trades:
    setups[t["setup"]].append(t)

for name in ["GEX Long", "AG Short", "BofA Scalp", "DD Exhaustion", "ES Absorption", "Paradigm Reversal"]:
    ts = setups.get(name, [])
    if not ts:
        continue
    w = [t for t in ts if t["result"] == "WIN"]
    l = [t for t in ts if t["result"] == "LOSS"]
    e = [t for t in ts if t["result"] == "EXPIRED"]
    pnl = sum(t["pnl"] for t in ts)
    swr = len(w)/(len(w)+len(l))*100 if (len(w)+len(l)) > 0 else 0
    avg_win = sum(t["pnl"] for t in w)/len(w) if w else 0
    avg_loss = sum(t["pnl"] for t in l)/len(l) if l else 0
    avg_mp = sum(t["max_profit"] for t in ts)/len(ts)
    print(f"\n--- {name} ({len(ts)} trades) ---")
    print(f"  W/L/E: {len(w)}/{len(l)}/{len(e)}  WR: {swr:.0f}%  P&L: {pnl:+.1f}")
    print(f"  Avg win: {avg_win:+.1f}  Avg loss: {avg_loss:+.1f}  Avg max_profit: {avg_mp:.1f}")
    for label, lo, hi in [("Morning 9-12", 9, 12), ("Midday 12-14", 12, 14), ("Afternoon 14+", 14, 17)]:
        sub = [t for t in ts if lo <= t["hour_decimal"] < hi]
        if not sub:
            continue
        sw = len([t for t in sub if t["result"] == "WIN"])
        sl = len([t for t in sub if t["result"] == "LOSS"])
        se = len([t for t in sub if t["result"] == "EXPIRED"])
        sp = sum(t["pnl"] for t in sub)
        subwr = sw/(sw+sl)*100 if (sw+sl) > 0 else 0
        print(f"    {label}: {len(sub)}t, {sw}W/{sl}L/{se}E, WR={subwr:.0f}%, P&L={sp:+.1f}")
    # Paradigm
    paradigms = defaultdict(list)
    for t in ts:
        paradigms[t["paradigm"] or "UNKNOWN"].append(t)
    if len(paradigms) > 1:
        print(f"  By paradigm:")
        for p, pts in sorted(paradigms.items(), key=lambda x: sum(t["pnl"] for t in x[1]), reverse=True):
            pw = len([t for t in pts if t["result"] == "WIN"])
            pl = len([t for t in pts if t["result"] == "LOSS"])
            pp = sum(t["pnl"] for t in pts)
            pwr = pw/(pw+pl)*100 if (pw+pl) > 0 else 0
            print(f"    {p}: {len(pts)}t, WR={pwr:.0f}%, P&L={pp:+.1f}")

# ============================================================
print("\n" + "=" * 70)
print("3. TIME-OF-DAY (hourly)")
print("=" * 70)
for h in range(9, 16):
    ht = [t for t in trades if t["hour"] == h]
    if not ht:
        continue
    hw = len([t for t in ht if t["result"] == "WIN"])
    hl = len([t for t in ht if t["result"] == "LOSS"])
    he = len([t for t in ht if t["result"] == "EXPIRED"])
    hp = sum(t["pnl"] for t in ht)
    hwr = hw/(hw+hl)*100 if (hw+hl) > 0 else 0
    print(f"  {h:02d}:00  {len(ht):3d}t  {hw}W/{hl}L/{he}E  WR={hwr:5.1f}%  P&L={hp:+7.1f}")

# ============================================================
print("\n" + "=" * 70)
print("4. GRADE ANALYSIS")
print("=" * 70)
grades = defaultdict(list)
for t in trades:
    grades[t["grade"]].append(t)
for g in ["A+", "A", "A-Entry", "B", "C", "LOG"]:
    gs = grades.get(g, [])
    if not gs:
        continue
    gw = len([t for t in gs if t["result"] == "WIN"])
    gl = len([t for t in gs if t["result"] == "LOSS"])
    gp = sum(t["pnl"] for t in gs)
    gwr = gw/(gw+gl)*100 if (gw+gl) > 0 else 0
    print(f"  {g:8s}  {len(gs):3d}t  {gw}W/{gl}L  WR={gwr:5.1f}%  P&L={gp:+7.1f}")

# ============================================================
print("\n" + "=" * 70)
print("5. DIRECTION + TIME")
print("=" * 70)
for d in ["long", "short", "bullish", "bearish"]:
    ds = [t for t in trades if t["direction"].lower() == d]
    if not ds:
        continue
    dw = len([t for t in ds if t["result"] == "WIN"])
    dl = len([t for t in ds if t["result"] == "LOSS"])
    dp = sum(t["pnl"] for t in ds)
    dwr = dw/(dw+dl)*100 if (dw+dl) > 0 else 0
    print(f"\n  {d:8s}  {len(ds):3d}t  {dw}W/{dl}L  WR={dwr:5.1f}%  P&L={dp:+7.1f}")
    for label, lo, hi in [("9-12", 9, 12), ("12-14", 12, 14), ("14-16", 14, 16)]:
        sub = [t for t in ds if lo <= t["hour_decimal"] < hi]
        if not sub:
            continue
        sw = len([t for t in sub if t["result"] == "WIN"])
        sl = len([t for t in sub if t["result"] == "LOSS"])
        sp = sum(t["pnl"] for t in sub)
        subwr = sw/(sw+sl)*100 if (sw+sl) > 0 else 0
        print(f"    {label}: {len(sub)}t, WR={subwr:.0f}%, P&L={sp:+.1f}")

# ============================================================
print("\n" + "=" * 70)
print("6. DAILY P&L")
print("=" * 70)
days = defaultdict(list)
for t in trades:
    days[t["date"]].append(t)
for d in sorted(days.keys()):
    dt = days[d]
    dp = sum(t["pnl"] for t in dt)
    dw = len([t for t in dt if t["result"] == "WIN"])
    dl = len([t for t in dt if t["result"] == "LOSS"])
    print(f"  {d}  {len(dt):3d}t  {dw}W/{dl}L  P&L={dp:+7.1f}")

# ============================================================
print("\n" + "=" * 70)
print("7. EXPIRED WITH HIGH MAX PROFIT (>=5 pts)")
print("=" * 70)
big_expired = [t for t in expired if t["max_profit"] >= 5]
print(f"  {len(big_expired)} expired trades had maxP >= 5 pts")
big_exp_missed = sum(t["max_profit"] for t in big_expired)
print(f"  Total missed profit: {big_exp_missed:.1f} pts")
for t in sorted(big_expired, key=lambda x: -x["max_profit"])[:15]:
    print(f"  #{t['id']} {t['date']} {t['time']} {t['setup']:20s} {t['direction']:6s} "
          f"pnl={t['pnl']:+.1f} maxP={t['max_profit']:.1f} held={t['elapsed']}m")

# ============================================================
print("\n" + "=" * 70)
print("8. WHAT-IF SCENARIOS")
print("=" * 70)

gex = setups.get("GEX Long", [])
gex_pnl = sum(t["pnl"] for t in gex)
print(f"\n  A) Remove GEX Long: {gex_pnl:+.1f} from {len(gex)}t -> New: {total_pnl - gex_pnl:+.1f}")

dd_all = setups.get("DD Exhaustion", [])
dd_after14 = [t for t in dd_all if t["hour_decimal"] >= 14]
dd_a14p = sum(t["pnl"] for t in dd_after14)
print(f"  B) DD cutoff 14:00: removes {len(dd_after14)}t ({dd_a14p:+.1f}) -> New: {total_pnl - dd_a14p:+.1f}")

dd_after13 = [t for t in dd_all if t["hour_decimal"] >= 13]
dd_a13p = sum(t["pnl"] for t in dd_after13)
print(f"  C) DD cutoff 13:00: removes {len(dd_after13)}t ({dd_a13p:+.1f}) -> New: {total_pnl - dd_a13p:+.1f}")

bofa = setups.get("BofA Scalp", [])
bofa_pnl = sum(t["pnl"] for t in bofa)
print(f"  D) Remove BofA: {bofa_pnl:+.1f} from {len(bofa)}t -> New: {total_pnl - bofa_pnl:+.1f}")

para = setups.get("Paradigm Reversal", [])
para_pnl = sum(t["pnl"] for t in para)
print(f"  E) Remove Paradigm: {para_pnl:+.1f} from {len(para)}t -> New: {total_pnl - para_pnl:+.1f}")

# F: Best-of
keep_f = []
for t in trades:
    if t["setup"] == "AG Short":
        keep_f.append(t)
    elif t["setup"] == "DD Exhaustion" and t["hour_decimal"] < 14:
        keep_f.append(t)
    elif t["setup"] == "ES Absorption":
        keep_f.append(t)
fpnl = sum(t["pnl"] for t in keep_f)
fw = len([t for t in keep_f if t["result"] == "WIN"])
fl = len([t for t in keep_f if t["result"] == "LOSS"])
fwr = fw/(fw+fl)*100 if (fw+fl) > 0 else 0
print(f"  F) AG + DD(<14) + Absorption: {len(keep_f)}t, WR={fwr:.0f}%, P&L={fpnl:+.1f}")

# G: F + Paradigm
keep_g = keep_f + [t for t in trades if t["setup"] == "Paradigm Reversal"]
gpnl = sum(t["pnl"] for t in keep_g)
gw2 = len([t for t in keep_g if t["result"] == "WIN"])
gl2 = len([t for t in keep_g if t["result"] == "LOSS"])
gwr2 = gw2/(gw2+gl2)*100 if (gw2+gl2) > 0 else 0
print(f"  G) F + Paradigm: {len(keep_g)}t, WR={gwr2:.0f}%, P&L={gpnl:+.1f}")

# H: Morning only all setups
morning = [t for t in trades if t["hour_decimal"] < 14]
mpnl = sum(t["pnl"] for t in morning)
aftn = [t for t in trades if t["hour_decimal"] >= 14]
apnl = sum(t["pnl"] for t in aftn)
mw2 = len([t for t in morning if t["result"] == "WIN"])
ml3 = len([t for t in morning if t["result"] == "LOSS"])
mwr2 = mw2/(mw2+ml3)*100 if (mw2+ml3) > 0 else 0
print(f"  H) All setups <14:00: {len(morning)}t, WR={mwr2:.0f}%, P&L={mpnl:+.1f} (afternoon: {apnl:+.1f})")

# I: Cap 2 per setup+dir per day
capped = []
cnt_i = defaultdict(int)
for t in sorted(trades, key=lambda x: x["id"]):
    key = (t["date"], t["setup"], t["direction"].lower())
    cnt_i[key] += 1
    if cnt_i[key] <= 2:
        capped.append(t)
cpnl = sum(t["pnl"] for t in capped)
print(f"  I) Cap 2/setup+dir/day: {len(capped)}t (cut {len(trades)-len(capped)}), P&L={cpnl:+.1f}")

# J: BEST COMBO
best = []
cnt_j = defaultdict(int)
for t in sorted(trades, key=lambda x: x["id"]):
    keep = False
    if t["setup"] == "AG Short":
        keep = True
    elif t["setup"] == "DD Exhaustion" and t["hour_decimal"] < 14 and t["paradigm"] != "BOFA-PURE":
        keep = True
    elif t["setup"] == "ES Absorption":
        keep = True
    elif t["setup"] == "Paradigm Reversal":
        keep = True
    if keep:
        key = (t["date"], t["setup"], t["direction"].lower())
        cnt_j[key] += 1
        if cnt_j[key] <= 2:
            best.append(t)
bpnl = sum(t["pnl"] for t in best)
bw2 = len([t for t in best if t["result"] == "WIN"])
bl2 = len([t for t in best if t["result"] == "LOSS"])
bwr2 = bw2/(bw2+bl2)*100 if (bw2+bl2) > 0 else 0
print(f"  J) BEST: AG+DD(<14,!BOFA)+Abs+Para, cap2: {len(best)}t, WR={bwr2:.0f}%, P&L={bpnl:+.1f}")
print(f"     vs current {len(trades)}t P&L={total_pnl:+.1f} => IMPROVEMENT: {bpnl-total_pnl:+.1f}")

# K: J but also remove GEX Long losses (already excluded by J since GEX not in keep)
# L: What if DD had 60min cooldown? Simulate
dd_60 = []
last_dd_by_dir = {}
for t in sorted([x for x in dd_all], key=lambda x: x["id"]):
    d = t["direction"].lower()
    last = last_dd_by_dir.get(d)
    if last is None or (t["hour_decimal"] - last) * 60 >= 60:
        dd_60.append(t)
        last_dd_by_dir[d] = t["hour_decimal"]
        # Reset on new day
    if last and t["date"] != [x for x in dd_all if x["hour_decimal"] == last][0]["date"] if False else False:
        dd_60.append(t)
        last_dd_by_dir[d] = t["hour_decimal"]

# Simpler: just group by day
dd_60 = []
for day in sorted(set(t["date"] for t in dd_all)):
    day_dd = sorted([t for t in dd_all if t["date"] == day], key=lambda x: x["hour_decimal"])
    last_by_dir = {}
    for t in day_dd:
        d = t["direction"].lower()
        if d not in last_by_dir or (t["hour_decimal"] - last_by_dir[d]) >= 1.0:
            dd_60.append(t)
            last_by_dir[d] = t["hour_decimal"]

dd60pnl = sum(t["pnl"] for t in dd_60)
dd_orig_pnl = sum(t["pnl"] for t in dd_all)
print(f"\n  L) DD 60min cooldown: {len(dd_60)}t (from {len(dd_all)}), P&L={dd60pnl:+.1f} (was {dd_orig_pnl:+.1f})")
new_total_dd60 = total_pnl - dd_orig_pnl + dd60pnl
print(f"     New total: {new_total_dd60:+.1f}")

# M: ULTIMATE BEST: J + DD 60min cooldown
ultimate = []
cnt_m = defaultdict(int)
dd_last_m = {}
for t in sorted(trades, key=lambda x: x["id"]):
    keep = False
    if t["setup"] == "AG Short":
        keep = True
    elif t["setup"] == "DD Exhaustion" and t["hour_decimal"] < 14 and t["paradigm"] != "BOFA-PURE":
        d = t["direction"].lower()
        day_key = (t["date"], d)
        last_h = dd_last_m.get(day_key)
        if last_h is None or (t["hour_decimal"] - last_h) >= 1.0:
            keep = True
            dd_last_m[day_key] = t["hour_decimal"]
    elif t["setup"] == "ES Absorption":
        keep = True
    elif t["setup"] == "Paradigm Reversal":
        keep = True
    if keep:
        key2 = (t["date"], t["setup"], t["direction"].lower())
        cnt_m[key2] += 1
        if cnt_m[key2] <= 2:
            ultimate.append(t)

upnl = sum(t["pnl"] for t in ultimate)
uw = len([t for t in ultimate if t["result"] == "WIN"])
ul = len([t for t in ultimate if t["result"] == "LOSS"])
uwr = uw/(uw+ul)*100 if (uw+ul) > 0 else 0
print(f"\n  M) ULTIMATE: J + DD 60min cd: {len(ultimate)}t, WR={uwr:.0f}%, P&L={upnl:+.1f}")
print(f"     vs current {len(trades)}t P&L={total_pnl:+.1f} => IMPROVEMENT: {upnl-total_pnl:+.1f}")

# ============================================================
print("\n" + "=" * 70)
print("9. SIGNAL SPAM (3+ same setup+dir/day)")
print("=" * 70)
by_ds = defaultdict(list)
for t in trades:
    by_ds[(t["date"], t["setup"], t["direction"].lower())].append(t)
spam_loss = 0
spam_ct = 0
for key, ts in sorted(by_ds.items()):
    if len(ts) > 2:
        extra = sum(t["pnl"] for t in ts[2:])
        spam_loss += extra
        spam_ct += len(ts) - 2
        print(f"  {key[0]} {key[1]:20s} {key[2]:6s}: {len(ts)} sigs, 3rd+ P&L={extra:+.1f}")
print(f"\n  Spam trades: {spam_ct}, P&L: {spam_loss:+.1f}. Capping saves: {-spam_loss:+.1f}")

# ============================================================
print("\n" + "=" * 70)
print("10. PROFIT CAPTURE EFFICIENCY")
print("=" * 70)
for name in ["GEX Long", "AG Short", "BofA Scalp", "DD Exhaustion", "ES Absorption", "Paradigm Reversal"]:
    ws = [t for t in setups.get(name, []) if t["result"] == "WIN"]
    ls = [t for t in setups.get(name, []) if t["result"] == "LOSS"]
    if not ws and not ls:
        continue
    awp = sum(t["pnl"] for t in ws)/len(ws) if ws else 0
    awmp = sum(t["max_profit"] for t in ws)/len(ws) if ws else 0
    cap = awp/awmp*100 if awmp > 0 else 0
    alp = sum(t["pnl"] for t in ls)/len(ls) if ls else 0
    awt = sum(t["elapsed"] for t in ws)/len(ws) if ws else 0
    alt = sum(t["elapsed"] for t in ls)/len(ls) if ls else 0
    print(f"  {name:20s}: win={awp:+.1f} (maxP={awmp:.1f} cap={cap:.0f}% {awt:.0f}m) loss={alp:+.1f} ({alt:.0f}m)")

# ============================================================
print("\n" + "=" * 70)
print("11. VIX ANALYSIS")
print("=" * 70)
vt = [t for t in trades if t["vix"] is not None]
if vt:
    for label, lo, hi in [("VIX<16", 0, 16), ("VIX 16-20", 16, 20), ("VIX>=20", 20, 100)]:
        sub = [t for t in vt if lo <= t["vix"] < hi]
        if not sub:
            continue
        sw = len([t for t in sub if t["result"] == "WIN"])
        sl = len([t for t in sub if t["result"] == "LOSS"])
        sp = sum(t["pnl"] for t in sub)
        subwr = sw/(sw+sl)*100 if (sw+sl) > 0 else 0
        print(f"  {label}: {len(sub)}t, {sw}W/{sl}L, WR={subwr:.0f}%, P&L={sp:+.1f}")

# ============================================================
print("\n" + "=" * 70)
print("12. PARADIGM (ALL SETUPS)")
print("=" * 70)
pars = defaultdict(list)
for t in trades:
    pars[t["paradigm"] or "UNKNOWN"].append(t)
for p, pts in sorted(pars.items(), key=lambda x: sum(t["pnl"] for t in x[1]), reverse=True):
    pw = len([t for t in pts if t["result"] == "WIN"])
    pl = len([t for t in pts if t["result"] == "LOSS"])
    pp = sum(t["pnl"] for t in pts)
    pwr = pw/(pw+pl)*100 if (pw+pl) > 0 else 0
    print(f"  {p:15s}: {len(pts):3d}t, {pw}W/{pl}L, WR={pwr:.0f}%, P&L={pp:+.1f}")

sys.stdout.flush()
