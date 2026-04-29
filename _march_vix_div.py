"""March-only VIX Divergence results with recommended exits."""
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
    AND ts::date >= '2026-03-02' AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:35' AND '15:30'
    ORDER BY ts;
""")
snaps = [{"ts": r[0], "date": r[0].date(), "spot": float(r[1]), "vix": float(r[2])} for r in cur.fetchall()]

cur.execute("""
    SELECT ts AT TIME ZONE 'America/New_York' as et, bar_open, bar_high, bar_low, bar_close
    FROM spx_ohlc_1m WHERE ts::date >= '2026-03-02'
    AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN '09:30' AND '16:00'
    ORDER BY ts;
""")
ohlc = [{"ts": r[0], "date": r[0].date(), "open": float(r[1]), "high": float(r[2]),
         "low": float(r[3]), "close": float(r[4])} for r in cur.fetchall()]
conn.close()

def group_by_date(data):
    g = defaultdict(list)
    for d in data: g[d["date"]].append(d)
    return dict(g)

snaps_by_date = group_by_date(snaps)
ohlc_by_date = group_by_date(ohlc)
dates = sorted(set(snaps_by_date.keys()) & set(ohlc_by_date.keys()))
print(f"March dates: {len(dates)} ({dates[0]} to {dates[-1]})\n")


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
        if bar["ts"] >= signal_ts:
            si = i; break
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


for direction in ["short", "long"]:
    if direction == "short":
        sl, be, act, gp, imm = 8, 8, 10, 5, None
        exit_name = "BE@8 Trail@10/g5"
    else:
        sl, be, act, gp, imm = 8, None, None, None, 8
        exit_name = "IMM Trail max-8"

    all_sigs = []
    for date in dates:
        if str(date) in BAD_DATES: continue
        sigs = detect_signals(snaps_by_date[date], direction)
        for sig in sigs:
            res = simulate(ohlc_by_date[date], sig["ts"], direction, sl, be, act, gp, imm)
            if res:
                sig.update(res)
                all_sigs.append(sig)

    print(f"=== {direction.upper()} ({exit_name}) === March: {len(all_sigs)} signals")
    if not all_sigs:
        print("  0 signals\n"); continue

    wins = sum(1 for s in all_sigs if s["outcome"] == "WIN")
    losses = sum(1 for s in all_sigs if s["outcome"] == "LOSS")
    be_cnt = sum(1 for s in all_sigs if s["outcome"] == "BE")
    exp_cnt = sum(1 for s in all_sigs if s["outcome"] == "EXPIRED")
    total_pnl = sum(s["pnl"] for s in all_sigs)
    gross_w = sum(s["pnl"] for s in all_sigs if s["pnl"] > 0)
    gross_l = abs(sum(s["pnl"] for s in all_sigs if s["pnl"] < 0))
    pf = gross_w / max(0.01, gross_l)
    wr = wins / max(1, wins + losses) * 100
    run = 0; pk = 0; mdd = 0
    for s in all_sigs:
        run += s["pnl"]; pk = max(pk, run); mdd = max(mdd, pk - run)

    print(f"  W={wins} L={losses} BE={be_cnt} E={exp_cnt} | WR={wr:.0f}% | PnL={total_pnl:+.1f} | PF={pf:.2f} | MaxDD={mdd:.1f}")
    print()

    # Per-signal detail
    hdr = f"  {'Date':>12} {'Time':>6} {'Gr':>3} {'Spot':>8} {'VIX':>7} {'P1':>6} {'P2v':>5} {'Result':>8} {'PnL':>7} {'MFE':>7} {'MAE':>6} {'Why':>7} {'Cum':>8}"
    print(hdr)

    cum = 0
    for s in sorted(all_sigs, key=lambda x: x["ts"]):
        cum += s["pnl"]
        print(f"  {str(s['ts'].date()):>12} {str(s['ts'].time())[:5]:>6} {s['grade']:>3} {s['spot']:>8.1f} {s['vix']:>7.2f} "
              f"{s['p1_spx']:>+5.1f} {s['p2_vix']:>+4.2f} {s['outcome']:>8} {s['pnl']:>+6.1f} {s['mfe']:>+6.1f} {s['mae']:>5.1f} {s['reason']:>7} {cum:>+7.1f}")

    # Grade + VIX breakdowns
    print()
    for label, key, buckets in [
        ("Grade", "grade", [("A+", lambda s: s["grade"] == "A+"), ("A", lambda s: s["grade"] == "A"),
                            ("B", lambda s: s["grade"] == "B"), ("C", lambda s: s["grade"] == "C")]),
        ("VIX", "vix", [("<22", lambda s: s["vix"] < 22), ("22-26", lambda s: 22 <= s["vix"] < 26),
                        ("26-30", lambda s: 26 <= s["vix"] < 30), (">=30", lambda s: s["vix"] >= 30)])
    ]:
        print(f"  {label} breakdown:")
        for name, filt in buckets:
            gs = [s for s in all_sigs if filt(s)]
            if not gs: continue
            gw = sum(1 for s in gs if s["outcome"] == "WIN")
            gl = sum(1 for s in gs if s["outcome"] == "LOSS")
            gp = sum(s["pnl"] for s in gs)
            gwr = gw / max(1, gw + gl) * 100
            gmfe = sum(s["mfe"] for s in gs) / len(gs)
            print(f"    {name:>5}: {len(gs)}t, {gw}W/{gl}L, WR={gwr:.0f}%, PnL={gp:+.1f}, avgMFE={gmfe:+.1f}")
    print()

    # Also show combined (both SHORT + LONG) if this is the second pass
    if direction == "long":
        # Reload short signals for combined view
        short_sigs = []
        for date in dates:
            if str(date) in BAD_DATES: continue
            sigs = detect_signals(snaps_by_date[date], "short")
            for sig in sigs:
                res = simulate(ohlc_by_date[date], sig["ts"], "short", 8, 8, 10, 5, None)
                if res:
                    sig.update(res)
                    short_sigs.append(sig)

        combined = short_sigs + all_sigs
        combined.sort(key=lambda x: x["ts"])
        cw = sum(1 for s in combined if s["outcome"] == "WIN")
        cl = sum(1 for s in combined if s["outcome"] == "LOSS")
        cp = sum(s["pnl"] for s in combined)
        cwr = cw / max(1, cw + cl) * 100
        cgw = sum(s["pnl"] for s in combined if s["pnl"] > 0)
        cgl = abs(sum(s["pnl"] for s in combined if s["pnl"] < 0))
        cpf = cgw / max(0.01, cgl)
        run = 0; pk = 0; mdd = 0
        for s in combined:
            run += s["pnl"]; pk = max(pk, run); mdd = max(mdd, pk - run)

        print(f"=== COMBINED (SHORT + LONG) === March: {len(combined)} signals")
        print(f"  W={cw} L={cl} | WR={cwr:.0f}% | PnL={cp:+.1f} | PF={cpf:.2f} | MaxDD={mdd:.1f}")
        print()
        cum = 0
        print(f"  {'Date':>12} {'Time':>6} {'Dir':>6} {'Gr':>3} {'Result':>8} {'PnL':>7} {'MFE':>7} {'Cum':>8}")
        for s in combined:
            cum += s["pnl"]
            d = "SHORT" if s["direction"] == "short" else "LONG"
            print(f"  {str(s['ts'].date()):>12} {str(s['ts'].time())[:5]:>6} {d:>6} {s['grade']:>3} {s['outcome']:>8} {s['pnl']:>+6.1f} {s['mfe']:>+6.1f} {cum:>+7.1f}")

        # Green days
        day_pnl = defaultdict(float)
        for s in combined:
            day_pnl[s["ts"].date()] += s["pnl"]
        green = sum(1 for v in day_pnl.values() if v > 0)
        red = sum(1 for v in day_pnl.values() if v < 0)
        flat = sum(1 for v in day_pnl.values() if v == 0)
        print(f"\n  Daily: {green} green, {red} red, {flat} flat ({green}/{green+red+flat} = {green/(green+red+flat)*100:.0f}% green)")
