"""Deep analysis of WINNING short trades — what conditions made them work?
Focus on trades that caught tops (high max_profit, entered near day high)."""
import sqlalchemy as sa
import os
from collections import defaultdict

engine = sa.create_engine(os.environ['DATABASE_URL'])

with engine.connect() as conn:
    # =========================================================
    # PART 1: ALL winning shorts with full context
    # =========================================================
    print("=" * 90)
    print("PART 1: ALL winning SHORT trades with full Volland context")
    print("=" * 90)
    rows = conn.execute(sa.text("""
        SELECT s.id, s.ts, s.ts::date as dt, s.setup_name, s.direction, s.grade, s.score,
               s.spot, s.lis, s.paradigm, s.target,
               s.outcome_result, s.outcome_pnl, s.outcome_max_profit, s.outcome_max_loss,
               s.outcome_elapsed_min,
               s.vanna_all, s.vanna_weekly, s.vanna_monthly,
               s.spot_vol_beta, s.greek_alignment,
               s.max_plus_gex, s.max_minus_gex,
               EXTRACT(HOUR FROM s.ts AT TIME ZONE 'America/New_York') as hour_et,
               EXTRACT(MINUTE FROM s.ts AT TIME ZONE 'America/New_York') as min_et
        FROM setup_log s
        WHERE s.direction IN ('short', 'bearish')
          AND s.outcome_result LIKE :w
        ORDER BY s.outcome_pnl DESC
    """), {"w": "%WIN%"}).fetchall()

    print("Total winning shorts: {}".format(len(rows)))
    print("")

    # Collect stats
    by_setup = defaultdict(list)
    by_paradigm = defaultdict(list)
    by_align = defaultdict(list)
    by_hour = defaultdict(list)
    all_winners = []

    for r in rows:
        t = {
            "id": r.id, "ts": str(r.ts)[:16], "dt": str(r.dt),
            "setup": r.setup_name, "grade": r.grade,
            "score": float(r.score), "spot": float(r.spot) if r.spot else 0,
            "lis": float(r.lis) if r.lis else None,
            "paradigm": r.paradigm or "UNKNOWN",
            "target": float(r.target) if r.target else None,
            "pnl": float(r.outcome_pnl or 0),
            "max_profit": float(r.outcome_max_profit or 0),
            "max_loss": float(r.outcome_max_loss or 0),
            "elapsed": int(r.outcome_elapsed_min or 0),
            "vanna": float(r.vanna_all) if r.vanna_all else None,
            "vanna_w": float(r.vanna_weekly) if r.vanna_weekly else None,
            "vanna_m": float(r.vanna_monthly) if r.vanna_monthly else None,
            "svb": float(r.spot_vol_beta) if r.spot_vol_beta else None,
            "align": int(r.greek_alignment) if r.greek_alignment is not None else None,
            "plus_gex": float(r.max_plus_gex) if r.max_plus_gex else None,
            "minus_gex": float(r.max_minus_gex) if r.max_minus_gex else None,
            "hour": int(r.hour_et),
            "minute": int(r.min_et),
        }
        # Derived
        t["near_lis"] = abs(t["spot"] - t["lis"]) if t["lis"] else None
        t["above_lis"] = (t["spot"] > t["lis"]) if t["lis"] else None
        t["dist_to_plus_gex"] = (t["plus_gex"] - t["spot"]) if t["plus_gex"] else None
        all_winners.append(t)
        by_setup[t["setup"]].append(t)
        by_paradigm[t["paradigm"]].append(t)
        if t["align"] is not None:
            by_align[t["align"]].append(t)
        by_hour[t["hour"]].append(t)

    # =========================================================
    # PART 2: Top 30 biggest winners — detailed view
    # =========================================================
    print("=" * 90)
    print("PART 2: Top 30 biggest SHORT wins (detailed)")
    print("=" * 90)
    for t in all_winners[:30]:
        lis_str = ""
        if t["lis"]:
            side = "ABOVE" if t["above_lis"] else "BELOW"
            lis_str = "LIS={:.0f} ({} {:.0f}pt)".format(t["lis"], side, t["near_lis"])
        gex_str = ""
        if t["dist_to_plus_gex"] is not None:
            gex_str = "+GEX {:.0f}pt away".format(t["dist_to_plus_gex"])
        svb_str = "SVB={:+.2f}".format(t["svb"]) if t["svb"] is not None else "SVB=?"
        vanna_str = "V={:.1f}B".format(t["vanna"]/1e9) if t["vanna"] else "V=?"
        print("#{:4d} {} {:15s} {:3s} s={:3.0f} a={} | {:+5.1f} (max {:+5.1f}) {}m | {} {} | {} | {} | para={}".format(
            t["id"], t["dt"], t["setup"], t["grade"], t["score"],
            "{:+d}".format(t["align"]) if t["align"] is not None else "?",
            t["pnl"], t["max_profit"], t["elapsed"],
            svb_str, vanna_str, lis_str, gex_str, t["paradigm"]))

    # =========================================================
    # PART 3: What paradigm were the winners in?
    # =========================================================
    print("")
    print("=" * 90)
    print("PART 3: Winning shorts by PARADIGM")
    print("=" * 90)
    for para in sorted(by_paradigm.keys(), key=lambda p: -sum(t["pnl"] for t in by_paradigm[p])):
        ts = by_paradigm[para]
        total = sum(t["pnl"] for t in ts)
        avg = total / len(ts)
        avg_mp = sum(t["max_profit"] for t in ts) / len(ts)
        print("  {:20s}: {:3d} wins, {:+7.1f} pts total, {:+5.1f} avg, maxP avg={:.1f}".format(
            para, len(ts), total, avg, avg_mp))

    # =========================================================
    # PART 4: What SVB were the winners at?
    # =========================================================
    print("")
    print("=" * 90)
    print("PART 4: Winning shorts by SVB bucket")
    print("=" * 90)
    svb_buckets = [
        ("SVB < -1.5", lambda t: t["svb"] is not None and t["svb"] < -1.5),
        ("SVB -1.5 to -0.5", lambda t: t["svb"] is not None and -1.5 <= t["svb"] < -0.5),
        ("SVB -0.5 to 0", lambda t: t["svb"] is not None and -0.5 <= t["svb"] < 0),
        ("SVB 0 to +0.5", lambda t: t["svb"] is not None and 0 <= t["svb"] < 0.5),
        ("SVB +0.5 to +1.5", lambda t: t["svb"] is not None and 0.5 <= t["svb"] < 1.5),
        ("SVB > +1.5", lambda t: t["svb"] is not None and t["svb"] >= 1.5),
        ("SVB unknown", lambda t: t["svb"] is None),
    ]
    for label, fn in svb_buckets:
        sub = [t for t in all_winners if fn(t)]
        if sub:
            total = sum(t["pnl"] for t in sub)
            print("  {:20s}: {:3d} wins, {:+7.1f} pts, avg {:+5.1f}".format(
                label, len(sub), total, total/len(sub)))

    # =========================================================
    # PART 5: Near LIS? Above/below?
    # =========================================================
    print("")
    print("=" * 90)
    print("PART 5: Winning shorts — LIS position")
    print("=" * 90)
    above = [t for t in all_winners if t["above_lis"] == True]
    below = [t for t in all_winners if t["above_lis"] == False]
    no_lis = [t for t in all_winners if t["lis"] is None]
    print("  Above LIS:  {:3d} wins, {:+7.1f} pts, avg pnl {:+5.1f}, avg maxP {:.1f}".format(
        len(above), sum(t["pnl"] for t in above),
        sum(t["pnl"] for t in above)/len(above) if above else 0,
        sum(t["max_profit"] for t in above)/len(above) if above else 0))
    print("  Below LIS:  {:3d} wins, {:+7.1f} pts, avg pnl {:+5.1f}, avg maxP {:.1f}".format(
        len(below), sum(t["pnl"] for t in below),
        sum(t["pnl"] for t in below)/len(below) if below else 0,
        sum(t["max_profit"] for t in below)/len(below) if below else 0))
    print("  No LIS:     {:3d} wins, {:+7.1f} pts".format(
        len(no_lis), sum(t["pnl"] for t in no_lis)))

    # Near LIS (within 10 pts)
    near = [t for t in all_winners if t["near_lis"] is not None and t["near_lis"] <= 10]
    far = [t for t in all_winners if t["near_lis"] is not None and t["near_lis"] > 10]
    print("  Near LIS (<=10pt): {:3d} wins, {:+7.1f} pts, avg {:+5.1f}".format(
        len(near), sum(t["pnl"] for t in near),
        sum(t["pnl"] for t in near)/len(near) if near else 0))
    print("  Far from LIS:     {:3d} wins, {:+7.1f} pts, avg {:+5.1f}".format(
        len(far), sum(t["pnl"] for t in far),
        sum(t["pnl"] for t in far)/len(far) if far else 0))

    # =========================================================
    # PART 6: Time of day
    # =========================================================
    print("")
    print("=" * 90)
    print("PART 6: Winning shorts by hour (ET)")
    print("=" * 90)
    for h in sorted(by_hour.keys()):
        ts = by_hour[h]
        total = sum(t["pnl"] for t in ts)
        print("  {:2d}:00 ET: {:3d} wins, {:+7.1f} pts, avg {:+5.1f}".format(
            h, len(ts), total, total/len(ts)))

    # =========================================================
    # PART 7: Now compare with LOSING shorts — same metrics
    # =========================================================
    print("")
    print("=" * 90)
    print("PART 7: LOSING shorts — same breakdown for comparison")
    print("=" * 90)
    losers = conn.execute(sa.text("""
        SELECT s.id, s.setup_name, s.direction, s.paradigm, s.spot, s.lis,
               s.outcome_pnl, s.outcome_max_profit,
               s.vanna_all, s.spot_vol_beta, s.greek_alignment,
               s.max_plus_gex,
               EXTRACT(HOUR FROM s.ts AT TIME ZONE 'America/New_York') as hour_et
        FROM setup_log s
        WHERE s.direction IN ('short', 'bearish')
          AND s.outcome_result LIKE :l
        ORDER BY s.outcome_pnl ASC
    """), {"l": "%LOSS%"}).fetchall()

    all_losers = []
    loser_by_paradigm = defaultdict(list)
    loser_by_svb = defaultdict(list)
    for r in losers:
        t = {
            "id": r.id, "setup": r.setup_name,
            "paradigm": r.paradigm or "UNKNOWN",
            "spot": float(r.spot) if r.spot else 0,
            "lis": float(r.lis) if r.lis else None,
            "pnl": float(r.outcome_pnl or 0),
            "max_profit": float(r.outcome_max_profit or 0),
            "vanna": float(r.vanna_all) if r.vanna_all else None,
            "svb": float(r.spot_vol_beta) if r.spot_vol_beta else None,
            "align": int(r.greek_alignment) if r.greek_alignment is not None else None,
            "plus_gex": float(r.max_plus_gex) if r.max_plus_gex else None,
            "hour": int(r.hour_et),
        }
        t["near_lis"] = abs(t["spot"] - t["lis"]) if t["lis"] else None
        t["above_lis"] = (t["spot"] > t["lis"]) if t["lis"] else None
        t["dist_to_plus_gex"] = (t["plus_gex"] - t["spot"]) if t["plus_gex"] else None
        all_losers.append(t)
        loser_by_paradigm[t["paradigm"]].append(t)

    print("Total losing shorts: {}".format(len(all_losers)))

    # Paradigm comparison: win rate per paradigm for shorts
    print("\n  Paradigm win rates for shorts:")
    all_paras = set(list(by_paradigm.keys()) + list(loser_by_paradigm.keys()))
    for para in sorted(all_paras, key=lambda p: -(sum(t["pnl"] for t in by_paradigm.get(p, [])) + sum(t["pnl"] for t in loser_by_paradigm.get(p, [])))):
        w = len(by_paradigm.get(para, []))
        l = len(loser_by_paradigm.get(para, []))
        w_pnl = sum(t["pnl"] for t in by_paradigm.get(para, []))
        l_pnl = sum(t["pnl"] for t in loser_by_paradigm.get(para, []))
        wr = w / (w + l) * 100 if (w + l) else 0
        if (w + l) >= 3:  # Only show meaningful sample
            print("    {:20s}: {:3d}W/{:3d}L = {:.0f}% WR, net {:+7.1f} pts".format(
                para, w, l, wr, w_pnl + l_pnl))

    # SVB comparison
    print("\n  SVB comparison (winners vs losers):")
    for label, fn in svb_buckets:
        w_sub = [t for t in all_winners if fn(t)]
        l_sub = [t for t in all_losers if fn(t)]
        if w_sub or l_sub:
            w_n = len(w_sub)
            l_n = len(l_sub)
            wr = w_n / (w_n + l_n) * 100 if (w_n + l_n) else 0
            net = sum(t["pnl"] for t in w_sub) + sum(t["pnl"] for t in l_sub)
            print("    {:20s}: {:3d}W/{:3d}L = {:.0f}% WR, net {:+7.1f}".format(
                label, w_n, l_n, wr, net))

    # LIS comparison
    print("\n  LIS position comparison (winners vs losers):")
    w_above = len([t for t in all_winners if t["above_lis"] == True])
    l_above = len([t for t in all_losers if t["above_lis"] == True])
    w_below = len([t for t in all_winners if t["above_lis"] == False])
    l_below = len([t for t in all_losers if t["above_lis"] == False])
    print("    Above LIS: {}W/{}L = {:.0f}% WR".format(w_above, l_above,
        w_above/(w_above+l_above)*100 if (w_above+l_above) else 0))
    print("    Below LIS: {}W/{}L = {:.0f}% WR".format(w_below, l_below,
        w_below/(w_below+l_below)*100 if (w_below+l_below) else 0))

    # Hour comparison
    print("\n  Hour comparison (winners vs losers):")
    loser_by_hour = defaultdict(list)
    for t in all_losers:
        loser_by_hour[t["hour"]].append(t)
    for h in sorted(set(list(by_hour.keys()) + list(loser_by_hour.keys()))):
        w_n = len(by_hour.get(h, []))
        l_n = len(loser_by_hour.get(h, []))
        wr = w_n / (w_n + l_n) * 100 if (w_n + l_n) else 0
        w_pnl = sum(t["pnl"] for t in by_hour.get(h, []))
        l_pnl = sum(t["pnl"] for t in loser_by_hour.get(h, []))
        print("    {:2d}:00 ET: {:3d}W/{:3d}L = {:.0f}% WR, net {:+7.1f}".format(
            h, w_n, l_n, wr, w_pnl + l_pnl))

    # =========================================================
    # PART 8: Setup-specific analysis for shorts
    # =========================================================
    print("")
    print("=" * 90)
    print("PART 8: Per-setup short analysis (winners + losers)")
    print("=" * 90)

    all_shorts = conn.execute(sa.text("""
        SELECT setup_name, paradigm, spot_vol_beta, greek_alignment,
               outcome_result, outcome_pnl, outcome_max_profit,
               spot, lis, max_plus_gex,
               EXTRACT(HOUR FROM ts AT TIME ZONE 'America/New_York') as hour_et
        FROM setup_log
        WHERE direction IN ('short', 'bearish')
          AND outcome_result IS NOT NULL
        ORDER BY ts
    """)).fetchall()

    setup_shorts = defaultdict(list)
    for r in all_shorts:
        t = {
            "setup": r.setup_name, "paradigm": r.paradigm or "UNKNOWN",
            "svb": float(r.spot_vol_beta) if r.spot_vol_beta else None,
            "align": int(r.greek_alignment) if r.greek_alignment is not None else None,
            "result": r.outcome_result, "pnl": float(r.outcome_pnl or 0),
            "max_profit": float(r.outcome_max_profit or 0),
            "spot": float(r.spot) if r.spot else 0,
            "lis": float(r.lis) if r.lis else None,
            "plus_gex": float(r.max_plus_gex) if r.max_plus_gex else None,
            "hour": int(r.hour_et),
            "is_win": "WIN" in r.outcome_result,
        }
        t["above_lis"] = (t["spot"] > t["lis"]) if t["lis"] else None
        t["near_plus_gex"] = (t["plus_gex"] - t["spot"]) if t["plus_gex"] and t["spot"] else None
        setup_shorts[t["setup"]].append(t)

    for setup in ["Skew Charm", "DD Exhaustion", "AG Short", "ES Absorption", "BofA Scalp", "Paradigm Reversal"]:
        ts = setup_shorts.get(setup, [])
        if not ts:
            continue
        w = sum(1 for t in ts if t["is_win"])
        l = sum(1 for t in ts if not t["is_win"])
        pnl = sum(t["pnl"] for t in ts)
        print("\n  --- {} shorts ({} trades, {}W/{}L, {:+.1f} pts) ---".format(
            setup, len(ts), w, l, pnl))

        # Best filter candidates
        # By paradigm
        para_stats = defaultdict(lambda: {"w": 0, "l": 0, "pnl": 0})
        for t in ts:
            d = para_stats[t["paradigm"]]
            d["pnl"] += t["pnl"]
            if t["is_win"]: d["w"] += 1
            else: d["l"] += 1
        print("    By paradigm:")
        for p in sorted(para_stats.keys(), key=lambda x: -para_stats[x]["pnl"]):
            d = para_stats[p]
            n = d["w"] + d["l"]
            if n >= 2:
                wr = d["w"]/n*100
                print("      {:20s}: {:2d}W/{:2d}L {:.0f}% WR {:+6.1f}".format(
                    p, d["w"], d["l"], wr, d["pnl"]))

        # By SVB bucket
        print("    By SVB:")
        for label, lo, hi in [("< -0.5", -999, -0.5), ("-0.5 to 0", -0.5, 0),
                               ("0 to +1", 0, 1), ("> +1", 1, 999)]:
            sub = [t for t in ts if t["svb"] is not None and lo <= t["svb"] < hi]
            if sub:
                sw = sum(1 for t in sub if t["is_win"])
                sl = sum(1 for t in sub if not t["is_win"])
                sp = sum(t["pnl"] for t in sub)
                wr = sw/(sw+sl)*100 if (sw+sl) else 0
                print("      SVB {:12s}: {:2d}W/{:2d}L {:.0f}% WR {:+6.1f}".format(
                    label, sw, sl, wr, sp))

        # Near +GEX (within 50 pts)
        print("    By distance to +GEX:")
        for label, lo, hi in [("< 30pt", -999, 30), ("30-60pt", 30, 60),
                               ("60-100pt", 60, 100), ("> 100pt", 100, 999)]:
            sub = [t for t in ts if t["near_plus_gex"] is not None and lo <= t["near_plus_gex"] < hi]
            if sub:
                sw = sum(1 for t in sub if t["is_win"])
                sl = sum(1 for t in sub if not t["is_win"])
                sp = sum(t["pnl"] for t in sub)
                wr = sw/(sw+sl)*100 if (sw+sl) else 0
                print("      {:12s}: {:2d}W/{:2d}L {:.0f}% WR {:+6.1f}".format(
                    label, sw, sl, wr, sp))
