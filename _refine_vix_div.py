"""
VIX Divergence Refinement Study
================================
Goal: Cut MaxDD while preserving PnL.
Analyze: what do losers have in common? Grade, VIX, time, P2 strength, day type?
Test filter combos to find optimal subset.
"""
import psycopg2
from collections import defaultdict

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"

P1_SPX_MOVE = 6; P1_VIX_REACT_MAX = 0.20; P1_WIN_MIN = 10; P1_WIN_MAX = 30
P2_VIX_COMPRESS = 0.25; P2_SPX_FLAT = 10; P2_WIN_MIN = 15; P2_WIN_MAX = 60
BAD_DATES = {"2026-03-26"}

conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

cur.execute("""
    SELECT ts AT TIME ZONE 'America/New_York' as et, spot, vix
    FROM chain_snapshots WHERE spot IS NOT NULL AND vix IS NOT NULL
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '15:30'
    ORDER BY ts;
""")
snaps = [{"ts": r[0], "date": r[0].date(), "spot": float(r[1]), "vix": float(r[2])} for r in cur.fetchall()]

cur.execute("""
    SELECT ts AT TIME ZONE 'America/New_York' as et, bar_open, bar_high, bar_low, bar_close
    FROM spx_ohlc_1m WHERE (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
    ORDER BY ts;
""")
ohlc = [{"ts": r[0], "date": r[0].date(), "open": float(r[1]), "high": float(r[2]),
         "low": float(r[3]), "close": float(r[4])} for r in cur.fetchall()]

# Get volland stats for each date (paradigm, DD, SVB)
cur.execute("""
    SELECT DISTINCT ON ((ts AT TIME ZONE 'America/New_York')::date)
      (ts AT TIME ZONE 'America/New_York')::date as dt,
      payload->'statistics'->>'paradigm' as paradigm,
      (payload->'statistics'->'spot_vol_beta'->>'correlation')::float as svb
    FROM volland_snapshots
    WHERE payload->'statistics' IS NOT NULL AND payload->'statistics'->>'paradigm' IS NOT NULL
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '10:00' AND '11:00'
    ORDER BY (ts AT TIME ZONE 'America/New_York')::date, ts DESC;
""")
day_paradigm = {}
day_svb = {}
for r in cur.fetchall():
    day_paradigm[str(r[0])] = r[1]
    if r[2] is not None:
        day_svb[str(r[0])] = float(r[2])

# Get daily SPX range (for day-type classification)
cur.execute("""
    SELECT trade_date,
      MIN(bar_low) as day_low, MAX(bar_high) as day_high,
      (SELECT bar_open FROM spx_ohlc_1m WHERE trade_date = o.trade_date ORDER BY ts LIMIT 1) as day_open,
      (SELECT bar_close FROM spx_ohlc_1m WHERE trade_date = o.trade_date ORDER BY ts DESC LIMIT 1) as day_close
    FROM spx_ohlc_1m o
    GROUP BY trade_date ORDER BY trade_date;
""")
day_stats = {}
for r in cur.fetchall():
    if r[1] and r[2] and r[3] and r[4]:
        day_stats[str(r[0])] = {
            "low": float(r[1]), "high": float(r[2]),
            "open": float(r[3]), "close": float(r[4]),
            "range": float(r[2]) - float(r[1]),
            "change": float(r[4]) - float(r[3]),
        }

conn.close()


def group_by_date(data):
    g = defaultdict(list)
    for d in data: g[d["date"]].append(d)
    return dict(g)

snaps_by_date = group_by_date(snaps)
ohlc_by_date = group_by_date(ohlc)
dates = sorted(set(snaps_by_date.keys()) & set(ohlc_by_date.keys()))


def detect_signals(snaps_day, direction):
    n = len(snaps_day)
    phase1_events = []
    for i in range(n):
        for j in range(i + 1, n):
            mins = (snaps_day[j]["ts"] - snaps_day[i]["ts"]).total_seconds() / 60
            if mins < P1_WIN_MIN: continue
            if mins > P1_WIN_MAX: break
            if direction == "long":
                sc = snaps_day[i]["spot"] - snaps_day[j]["spot"]
                vr = snaps_day[j]["vix"] - snaps_day[i]["vix"]
            else:
                sc = snaps_day[j]["spot"] - snaps_day[i]["spot"]
                vr = snaps_day[i]["vix"] - snaps_day[j]["vix"]
            if sc >= P1_SPX_MOVE and vr <= P1_VIX_REACT_MAX:
                phase1_events.append({"end_idx": j, "spx_move": sc, "vix_react": vr})
    if not phase1_events: return []
    used = set()
    for p1 in phase1_events:
        if p1["end_idx"] in used: continue
        p2s = p1["end_idx"]
        for j in range(p2s + 1, n):
            mins = (snaps_day[j]["ts"] - snaps_day[p2s]["ts"]).total_seconds() / 60
            if mins < P2_WIN_MIN: continue
            if mins > P2_WIN_MAX: break
            if direction == "long":
                vc = snaps_day[p2s]["vix"] - snaps_day[j]["vix"]
            else:
                vc = snaps_day[j]["vix"] - snaps_day[p2s]["vix"]
            sr = abs(snaps_day[j]["spot"] - snaps_day[p2s]["spot"])
            if vc >= P2_VIX_COMPRESS and sr <= P2_SPX_FLAT:
                p1s = p1["spx_move"]
                grade = "A+" if p1s >= 12 else ("A" if p1s >= 10 else ("B" if p1s >= 8 else "C"))
                used.add(p1["end_idx"])
                return [{"ts": snaps_day[j]["ts"], "spot": snaps_day[j]["spot"], "vix": snaps_day[j]["vix"],
                         "p1_spx": p1["spx_move"], "p1_vix": p1["vix_react"], "p2_vix": vc,
                         "direction": direction, "grade": grade}]
    return []


def simulate(ohlc_day, signal_ts, direction, sl, be_trigger=None, activation=None, gap=None, imm_gap=None, max_hold=120):
    si = None
    for i, bar in enumerate(ohlc_day):
        if bar["ts"] >= signal_ts: si = i; break
    if si is None: return None
    entry = ohlc_day[si]["open"]
    stop = -sl; max_p = 0.0; cur_stop = stop; mfe = 0.0; mae = 0.0
    for i in range(si, min(si + max_hold, len(ohlc_day))):
        bar = ohlc_day[i]; elapsed = i - si
        if direction == "long":
            hp = bar["high"] - entry; lp = bar["low"] - entry
        else:
            hp = entry - bar["low"]; lp = entry - bar["high"]
        mfe = max(mfe, hp); mae = max(mae, -lp if lp < 0 else 0)
        if lp <= cur_stop:
            pnl = cur_stop
            reason = "STOP" if cur_stop == stop else ("BE" if cur_stop == 0 else "TRAIL")
            return {"outcome": "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "BE"),
                    "pnl": pnl, "mfe": mfe, "mae": mae, "bars": elapsed, "reason": reason}
        max_p = max(max_p, hp)
        if be_trigger and max_p >= be_trigger and cur_stop < 0: cur_stop = 0
        if imm_gap is not None:
            tl = max_p - imm_gap
            if tl > cur_stop: cur_stop = tl
        elif activation and gap and max_p >= activation:
            tl = max_p - gap
            if tl > cur_stop: cur_stop = tl
    last = ohlc_day[min(si + max_hold - 1, len(ohlc_day) - 1)]
    pnl = (last["close"] - entry) if direction == "long" else (entry - last["close"])
    return {"outcome": "EXPIRED", "pnl": pnl, "mfe": mfe, "mae": mae, "bars": max_hold, "reason": "EXPIRED"}


# ── Collect all signals with enriched data ──
all_signals = []
for date in dates:
    if str(date) in BAD_DATES: continue
    for direction in ["short", "long"]:
        if direction == "short":
            sigs = detect_signals(snaps_by_date[date], "short")
            for sig in sigs:
                if sig["vix"] >= 26: continue  # VIX gate
                res = simulate(ohlc_by_date[date], sig["ts"], "short", 8, 8, 10, 5, None)
                if res: sig.update(res); all_signals.append(sig)
        else:
            sigs = detect_signals(snaps_by_date[date], "long")
            for sig in sigs:
                res = simulate(ohlc_by_date[date], sig["ts"], "long", 8, None, None, None, 8)
                if res: sig.update(res); all_signals.append(sig)

# Enrich with day context
for s in all_signals:
    d = str(s["ts"].date())
    s["paradigm"] = day_paradigm.get(d, "?")
    s["svb"] = day_svb.get(d, 0)
    ds = day_stats.get(d, {})
    s["day_range"] = ds.get("range", 0)
    s["day_change"] = ds.get("change", 0)
    s["day_type"] = "UP" if ds.get("change", 0) > 10 else ("DOWN" if ds.get("change", 0) < -10 else "CHOP")
    s["hour"] = s["ts"].hour

all_signals.sort(key=lambda x: x["ts"])

print(f"Total signals: {len(all_signals)}")
print()

# ── ANALYSIS 1: Winner vs Loser characteristics ──
winners = [s for s in all_signals if s["pnl"] > 0]
losers = [s for s in all_signals if s["pnl"] < 0]
flat = [s for s in all_signals if s["pnl"] == 0]

def avg(lst, key): return sum(s[key] for s in lst) / len(lst) if lst else 0

print("=== WINNER vs LOSER PROFILE ===")
print(f"{'':>20} {'Winners':>10} {'Losers':>10}")
print(f"{'Count':>20} {len(winners):>10} {len(losers):>10}")
print(f"{'Avg P1 SPX move':>20} {avg(winners, 'p1_spx'):>10.1f} {avg(losers, 'p1_spx'):>10.1f}")
print(f"{'Avg P1 VIX react':>20} {avg(winners, 'p1_vix'):>10.2f} {avg(losers, 'p1_vix'):>10.2f}")
print(f"{'Avg P2 VIX comp':>20} {avg(winners, 'p2_vix'):>10.2f} {avg(losers, 'p2_vix'):>10.2f}")
print(f"{'Avg VIX':>20} {avg(winners, 'vix'):>10.1f} {avg(losers, 'vix'):>10.1f}")
print(f"{'Avg MFE':>20} {avg(winners, 'mfe'):>10.1f} {avg(losers, 'mfe'):>10.1f}")
print(f"{'Avg MAE':>20} {avg(winners, 'mae'):>10.1f} {avg(losers, 'mae'):>10.1f}")
print(f"{'Avg day range':>20} {avg(winners, 'day_range'):>10.1f} {avg(losers, 'day_range'):>10.1f}")
print(f"{'Avg day change':>20} {avg(winners, 'day_change'):>10.1f} {avg(losers, 'day_change'):>10.1f}")
print(f"{'Avg SVB':>20} {avg(winners, 'svb'):>10.2f} {avg(losers, 'svb'):>10.2f}")
print(f"{'Avg hour':>20} {avg(winners, 'hour'):>10.1f} {avg(losers, 'hour'):>10.1f}")

# Direction split
for d in ["long", "short"]:
    dw = [s for s in winners if s["direction"] == d]
    dl = [s for s in losers if s["direction"] == d]
    print(f"\n  {d.upper()}: {len(dw)}W / {len(dl)}L")
    if dw: print(f"    Winners avg P1={avg(dw,'p1_spx'):.1f} P2v={avg(dw,'p2_vix'):.2f} VIX={avg(dw,'vix'):.1f} MFE={avg(dw,'mfe'):.1f}")
    if dl: print(f"    Losers  avg P1={avg(dl,'p1_spx'):.1f} P2v={avg(dl,'p2_vix'):.2f} VIX={avg(dl,'vix'):.1f} MFE={avg(dl,'mfe'):.1f}")

# Grade distribution
print("\n=== GRADE BREAKDOWN ===")
for g in ["A+", "A", "B", "C"]:
    gs = [s for s in all_signals if s["grade"] == g]
    if not gs: continue
    gw = sum(1 for s in gs if s["pnl"] > 0)
    gl = sum(1 for s in gs if s["pnl"] < 0)
    gp = sum(s["pnl"] for s in gs)
    print(f"  {g:>3}: {len(gs)}t {gw}W/{gl}L WR={gw/max(1,gw+gl)*100:.0f}% PnL={gp:+.1f}")

# Day type
print("\n=== DAY TYPE ===")
for dt in ["UP", "DOWN", "CHOP"]:
    ds = [s for s in all_signals if s["day_type"] == dt]
    if not ds: continue
    dw = sum(1 for s in ds if s["pnl"] > 0)
    dl = sum(1 for s in ds if s["pnl"] < 0)
    dp = sum(s["pnl"] for s in ds)
    print(f"  {dt:>5}: {len(ds)}t {dw}W/{dl}L WR={dw/max(1,dw+dl)*100:.0f}% PnL={dp:+.1f}")
    for d in ["long", "short"]:
        dds = [s for s in ds if s["direction"] == d]
        if not dds: continue
        ddw = sum(1 for s in dds if s["pnl"] > 0)
        ddl = sum(1 for s in dds if s["pnl"] < 0)
        ddp = sum(s["pnl"] for s in dds)
        print(f"    {d:>6}: {len(dds)}t {ddw}W/{ddl}L PnL={ddp:+.1f}")

# Paradigm
print("\n=== PARADIGM (morning snapshot) ===")
paradigms = set(s["paradigm"] for s in all_signals)
for p in sorted(paradigms):
    ps = [s for s in all_signals if s["paradigm"] == p]
    pw = sum(1 for s in ps if s["pnl"] > 0)
    pl = sum(1 for s in ps if s["pnl"] < 0)
    pp = sum(s["pnl"] for s in ps)
    print(f"  {p:>15}: {len(ps)}t {pw}W/{pl}L WR={pw/max(1,pw+pl)*100:.0f}% PnL={pp:+.1f}")

# Hour of signal
print("\n=== SIGNAL HOUR ===")
for h in range(10, 16):
    hs = [s for s in all_signals if s["hour"] == h]
    if not hs: continue
    hw = sum(1 for s in hs if s["pnl"] > 0)
    hl = sum(1 for s in hs if s["pnl"] < 0)
    hp = sum(s["pnl"] for s in hs)
    print(f"  {h:02d}:00: {len(hs)}t {hw}W/{hl}L WR={hw/max(1,hw+hl)*100:.0f}% PnL={hp:+.1f}")

# P2 VIX compress magnitude buckets
print("\n=== P2 VIX COMPRESS MAGNITUDE ===")
for lo, hi, label in [(0.25, 0.30, "0.25-0.30"), (0.30, 0.40, "0.30-0.40"), (0.40, 0.60, "0.40-0.60"), (0.60, 5.0, "0.60+")]:
    ps = [s for s in all_signals if lo <= s["p2_vix"] < hi]
    if not ps: continue
    pw = sum(1 for s in ps if s["pnl"] > 0)
    pl = sum(1 for s in ps if s["pnl"] < 0)
    pp = sum(s["pnl"] for s in ps)
    print(f"  {label:>10}: {len(ps)}t {pw}W/{pl}L WR={pw/max(1,pw+pl)*100:.0f}% PnL={pp:+.1f}")

# ── ANALYSIS 2: Test filter combos ──
print("\n" + "="*70)
print("=== FILTER COMBINATIONS ===")
print("="*70)

def eval_filter(signals, label):
    if not signals: return
    wins = sum(1 for s in signals if s["pnl"] > 0)
    losses = sum(1 for s in signals if s["pnl"] < 0)
    total_pnl = sum(s["pnl"] for s in signals)
    gw = sum(s["pnl"] for s in signals if s["pnl"] > 0)
    gl = abs(sum(s["pnl"] for s in signals if s["pnl"] < 0))
    pf = gw / max(0.01, gl)
    run = 0; pk = 0; mdd = 0
    for s in sorted(signals, key=lambda x: x["ts"]):
        run += s["pnl"]; pk = max(pk, run); mdd = max(mdd, pk - run)
    wr = wins / max(1, wins + losses) * 100
    # Green days
    dpnl = defaultdict(float)
    for s in signals: dpnl[s["ts"].date()] += s["pnl"]
    green = sum(1 for v in dpnl.values() if v > 0)
    red = sum(1 for v in dpnl.values() if v < 0)
    print(f"  {label:>40}: {len(signals):>3}t {wins}W/{losses}L WR={wr:>4.0f}% PnL={total_pnl:>+7.1f} PF={pf:>5.2f} MaxDD={mdd:>5.1f} {green}g/{red}r")

# Baseline (current)
eval_filter(all_signals, "BASELINE (all)")

# Filter: only B+ grade (P1 >= 8)
eval_filter([s for s in all_signals if s["grade"] in ("A+", "A", "B")], "Grade B+ only")

# Filter: block LONG when VIX < 22
eval_filter([s for s in all_signals if not (s["direction"] == "long" and s["vix"] < 22)], "Block long VIX<22")

# Filter: block LONG when VIX < 26
eval_filter([s for s in all_signals if not (s["direction"] == "long" and s["vix"] < 26)], "Block long VIX<26")

# Filter: only 10:00-12:59
eval_filter([s for s in all_signals if 10 <= s["hour"] <= 12], "10:00-12:59 only")

# Filter: only 10:00-11:59
eval_filter([s for s in all_signals if 10 <= s["hour"] <= 11], "10:00-11:59 only")

# Filter: block CHOP days
eval_filter([s for s in all_signals if s["day_type"] != "CHOP"], "Block CHOP days")

# Filter: P2 compress >= 0.30
eval_filter([s for s in all_signals if s["p2_vix"] >= 0.30], "P2 VIX >= 0.30")

# Filter: P2 compress >= 0.35
eval_filter([s for s in all_signals if s["p2_vix"] >= 0.35], "P2 VIX >= 0.35")

# Combo: Grade B+ AND P2 >= 0.30
eval_filter([s for s in all_signals if s["grade"] in ("A+", "A", "B") and s["p2_vix"] >= 0.30],
            "Grade B+ AND P2>=0.30")

# Combo: Block long VIX<22 AND P2 >= 0.30
eval_filter([s for s in all_signals if not (s["direction"] == "long" and s["vix"] < 22) and s["p2_vix"] >= 0.30],
            "No long VIX<22 + P2>=0.30")

# Combo: Grade B+ AND block long VIX<22
eval_filter([s for s in all_signals if s["grade"] in ("A+", "A", "B") and not (s["direction"] == "long" and s["vix"] < 22)],
            "Grade B+ + no long VIX<22")

# Combo: 10-12 + Grade B+
eval_filter([s for s in all_signals if 10 <= s["hour"] <= 12 and s["grade"] in ("A+", "A", "B")],
            "10-12 + Grade B+")

# SHORT only
eval_filter([s for s in all_signals if s["direction"] == "short"], "SHORT only")

# SHORT B+ only
eval_filter([s for s in all_signals if s["direction"] == "short" and s["grade"] in ("A+", "A", "B")],
            "SHORT B+ only")

# LONG VIX>=26 only
eval_filter([s for s in all_signals if s["direction"] == "long" and s["vix"] >= 26],
            "LONG VIX>=26 only")

# Combo: SHORT B+ AND LONG VIX>=26
eval_filter([s for s in all_signals if
             (s["direction"] == "short" and s["grade"] in ("A+", "A", "B")) or
             (s["direction"] == "long" and s["vix"] >= 26)],
            "SHORT B+ + LONG VIX>=26")

# Combo: SHORT + LONG VIX>=26
eval_filter([s for s in all_signals if
             s["direction"] == "short" or
             (s["direction"] == "long" and s["vix"] >= 26)],
            "All SHORT + LONG VIX>=26")

# Combo: no long VIX<26 AND grade B+
eval_filter([s for s in all_signals if s["grade"] in ("A+", "A", "B") and
             not (s["direction"] == "long" and s["vix"] < 26)],
            "Grade B+ + no long VIX<26")

# Combo: P2 >= 0.30 AND no long VIX<26
eval_filter([s for s in all_signals if s["p2_vix"] >= 0.30 and
             not (s["direction"] == "long" and s["vix"] < 26)],
            "P2>=0.30 + no long VIX<26")

# Combo: P2 >= 0.30 AND 10-12
eval_filter([s for s in all_signals if s["p2_vix"] >= 0.30 and 10 <= s["hour"] <= 12],
            "P2>=0.30 + 10:00-12:59")

# Combo: P1 VIX react <= 0.10 (stronger suppression)
eval_filter([s for s in all_signals if s["p1_vix"] <= 0.10], "P1 VIX react <= 0.10")

# Combo: P1 VIX react negative (VIX went OPPOSITE direction = strongest suppression)
eval_filter([s for s in all_signals if s["p1_vix"] <= 0.0], "P1 VIX opposite (<=0)")

# Full combo: SHORT all + LONG VIX>=26 grade B+
eval_filter([s for s in all_signals if
             s["direction"] == "short" or
             (s["direction"] == "long" and s["vix"] >= 26 and s["grade"] in ("A+", "A", "B"))],
            "SHORT all + LONG VIX>=26 B+")

# ── DETAIL: worst trades ──
print("\n=== 5 WORST TRADES ===")
worst = sorted(all_signals, key=lambda s: s["pnl"])[:5]
for s in worst:
    d = str(s["ts"].date())
    print(f"  {d} {str(s['ts'].time())[:5]} {s['direction']:>5} {s['grade']:>3} VIX={s['vix']:.1f} P1={s['p1_spx']:+.1f} P2v={s['p2_vix']:+.2f} "
          f"PnL={s['pnl']:+.1f} MFE={s['mfe']:+.1f} MAE={s['mae']:.1f} day={s['day_type']} para={s['paradigm']}")

print("\n=== 5 BEST TRADES ===")
best = sorted(all_signals, key=lambda s: -s["pnl"])[:5]
for s in best:
    d = str(s["ts"].date())
    print(f"  {d} {str(s['ts'].time())[:5]} {s['direction']:>5} {s['grade']:>3} VIX={s['vix']:.1f} P1={s['p1_spx']:+.1f} P2v={s['p2_vix']:+.2f} "
          f"PnL={s['pnl']:+.1f} MFE={s['mfe']:+.1f} MAE={s['mae']:.1f} day={s['day_type']} para={s['paradigm']}")
