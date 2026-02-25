"""
Backtest: ES Absorption proposed_d15 with paradigm filter.
Cross-references each signal with the nearest Volland paradigm.
"""
import json
from sqlalchemy import create_engine, text
from collections import defaultdict
from datetime import time as dtime

DB_URL = 'postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway'
engine = create_engine(DB_URL)

PIVOT_LEFT = 2
PIVOT_RIGHT = 2
VOL_WINDOW = 10
MIN_VOL_RATIO = 1.4
CVD_Z_MIN = 0.5
CVD_STD_WINDOW = 20
MAX_SWING_AGE = 50
MAX_TRIGGER_DIST = 15
OUTCOME_BARS = 15
TARGET_PTS = 10
STOP_PTS = 12
SIGNAL_START = dtime(15, 0)  # 10:00 ET in UTC
RTH_END = dtime(21, 0)
COOLDOWN = 10


def detect_swings(closed, pl, pr):
    swings = []
    last_type = None
    for pos in range(pl, len(closed) - pr):
        bar = closed[pos]
        is_low = all(bar["low"] <= closed[pos - j]["low"] for j in range(1, pl + 1))
        if is_low:
            is_low = all(bar["low"] <= closed[pos + j]["low"] for j in range(1, pr + 1))
        is_high = all(bar["high"] >= closed[pos - j]["high"] for j in range(1, pl + 1))
        if is_high:
            is_high = all(bar["high"] >= closed[pos + j]["high"] for j in range(1, pr + 1))
        if not is_low and not is_high:
            continue
        if is_low and is_high:
            if last_type == "L": is_low = False
            elif last_type == "H": is_high = False
            else: is_high = False
        nt = "L" if is_low else "H"
        ns = {"type": nt, "price": bar["low"] if is_low else bar["high"],
              "cvd": bar["cvd"], "volume": bar["volume"], "bar_idx": bar["idx"]}
        if not swings or last_type is None:
            swings.append(ns); last_type = nt
        elif nt == last_type:
            if nt == "L" and ns["price"] <= swings[-1]["price"]: swings[-1] = ns
            elif nt == "H" and ns["price"] >= swings[-1]["price"]: swings[-1] = ns
        else:
            swings.append(ns); last_type = nt
    return swings


def cvd_stats(closed):
    si = max(1, len(closed) - CVD_STD_WINDOW)
    deltas = [closed[i]["cvd"] - closed[i-1]["cvd"] for i in range(si, len(closed))]
    if len(deltas) < 5: return None, None
    md = sum(deltas) / len(deltas)
    std = max(1, (sum((d - md)**2 for d in deltas) / len(deltas))**0.5)
    moves = [abs(closed[i]["close"] - closed[i-1]["close"]) for i in range(si, len(closed))]
    atr = max(0.01, sum(moves) / len(moves) if moves else 1.0)
    return std, atr


def score_div(cz, pa):
    return min(100, min(100, cz / 3.0 * 100) * min(2.0, 0.5 + pa * 0.5))


def scan(closed, swings, trigger, std, atr):
    ti = trigger["idx"]
    vols = [b["volume"] for b in closed[-(VOL_WINDOW + 1):-1]]
    if not vols: return []
    va = sum(vols) / len(vols)
    if va <= 0: return []
    vr = trigger["volume"] / va
    if vr < MIN_VOL_RATIO: return []
    active = [s for s in swings if ti - s["bar_idx"] <= MAX_SWING_AGE]
    slows = [s for s in active if s["type"] == "L"]
    shighs = [s for s in active if s["type"] == "H"]
    bull, bear = [], []
    for i in range(1, len(slows)):
        L1, L2 = slows[i-1], slows[i]
        if ti - L2["bar_idx"] > MAX_TRIGGER_DIST: continue
        cg = abs(L2["cvd"] - L1["cvd"]); cz = cg / std
        if cz < CVD_Z_MIN: continue
        ia = L2["price"] >= L1["price"] and L2["cvd"] < L1["cvd"]
        ie = L2["price"] < L1["price"] and L2["cvd"] > L1["cvd"]
        if ia or ie:
            pd = abs(L2["price"] - L1["price"]); pa = pd / atr
            bull.append({"cz": round(cz, 2), "score": round(score_div(cz, pa), 1),
                         "pat": "sell_abs" if ia else "sell_exh"})
    for i in range(1, len(shighs)):
        H1, H2 = shighs[i-1], shighs[i]
        if ti - H2["bar_idx"] > MAX_TRIGGER_DIST: continue
        cg = abs(H2["cvd"] - H1["cvd"]); cz = cg / std
        if cz < CVD_Z_MIN: continue
        ia = H2["price"] <= H1["price"] and H2["cvd"] > H1["cvd"]
        ie = H2["price"] > H1["price"] and H2["cvd"] < H1["cvd"]
        if ia or ie:
            pd = abs(H2["price"] - H1["price"]); pa = pd / atr
            bear.append({"cz": round(cz, 2), "score": round(score_div(cz, pa), 1),
                         "pat": "buy_abs" if ia else "buy_exh"})
    sigs = []
    bb = max(bull, key=lambda d: d["score"]) if bull else None
    br = max(bear, key=lambda d: d["score"]) if bear else None
    if bb and br:
        if bb["score"] >= br["score"]: sigs.append(("bullish", bb, vr))
        else: sigs.append(("bearish", br, vr))
    elif bb: sigs.append(("bullish", bb, vr))
    elif br: sigs.append(("bearish", br, vr))
    return sigs


def outcome(closed, pos, direction):
    entry = closed[pos]["close"]
    mp = ml = 0
    ep = min(pos + OUTCOME_BARS, len(closed) - 1)
    for j in range(pos + 1, ep + 1):
        b = closed[j]
        if direction == "bullish": p = b["high"] - entry; l = entry - b["low"]
        else: p = entry - b["low"]; l = b["high"] - entry
        if p > mp: mp = p
        if l > ml: ml = l
        if l >= STOP_PTS: return "LOSS", -STOP_PTS
        if p >= TARGET_PTS: return "WIN", TARGET_PTS
    if direction == "bullish": f = closed[ep]["close"] - entry
    else: f = entry - closed[ep]["close"]
    return "EXPIRED", round(f, 2)


def run():
    with engine.connect() as conn:
        # Load bars
        days_r = conn.execute(text(
            "SELECT DISTINCT trade_date FROM es_range_bars ORDER BY trade_date"
        )).fetchall()
        all_days = {}
        for (td,) in days_r:
            rows = conn.execute(text("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume, cumulative_delta, ts_end, status
                FROM es_range_bars WHERE trade_date = :d AND status = 'closed'
                ORDER BY bar_idx
            """), {"d": td}).mappings().all()
            if len(rows) < 25: continue
            all_days[str(td)] = [{
                "idx": r["bar_idx"], "open": float(r["bar_open"]),
                "high": float(r["bar_high"]), "low": float(r["bar_low"]),
                "close": float(r["bar_close"]), "volume": int(r["bar_volume"]),
                "cvd": int(r["cumulative_delta"]), "ts_end": str(r["ts_end"]),
            } for r in rows]

        # Load paradigm timeline per day
        paradigm_timeline = {}
        for day_str in all_days:
            rows = conn.execute(text("""
                SELECT ts,
                       payload->'statistics'->>'paradigm' as paradigm,
                       payload->'statistics'->>'delta_decay_hedging' as dd_hedging
                FROM volland_snapshots
                WHERE ts::date = :d
                  AND payload->'statistics'->>'paradigm' IS NOT NULL
                ORDER BY ts ASC
            """), {"d": day_str}).mappings().all()
            timeline = []
            for r in rows:
                timeline.append({
                    "ts": str(r["ts"]),
                    "paradigm": r["paradigm"],
                    "dd_hedging": r["dd_hedging"],
                })
            paradigm_timeline[day_str] = timeline

    def get_paradigm_at(day_str, bar_ts):
        """Find nearest paradigm for a bar timestamp."""
        tl = paradigm_timeline.get(day_str, [])
        if not tl:
            return "UNKNOWN", ""
        # Find latest paradigm before or at bar_ts
        best = tl[0]
        for entry in tl:
            if entry["ts"] <= bar_ts:
                best = entry
            else:
                break
        return best["paradigm"], best.get("dd_hedging", "")

    # Run signals
    min_bars = max(VOL_WINDOW, CVD_STD_WINDOW, PIVOT_LEFT + PIVOT_RIGHT + 1) + 1
    signals = []

    for day_str, bars in sorted(all_days.items()):
        cd_bull = -100
        cd_bear = -100
        for end_i in range(min_bars, len(bars)):
            trigger = bars[end_i]
            ts_str = trigger["ts_end"]
            try:
                h, m = int(ts_str[11:13]), int(ts_str[14:16])
                bt = dtime(h, m)
            except: continue
            if bt < SIGNAL_START or bt > RTH_END: continue

            closed = bars[:end_i + 1]
            swings = detect_swings(closed, PIVOT_LEFT, PIVOT_RIGHT)
            std, atr = cvd_stats(closed)
            if std is None: continue

            for direction, best, vr in scan(closed, swings, trigger, std, atr):
                dk = "bull" if direction == "bullish" else "bear"
                if direction == "bullish" and trigger["idx"] - cd_bull < COOLDOWN: continue
                if direction == "bearish" and trigger["idx"] - cd_bear < COOLDOWN: continue

                res, pnl = outcome(bars, end_i, direction)
                paradigm, dd_hedging = get_paradigm_at(day_str, ts_str)

                # Simplify paradigm to group
                pg = paradigm.upper() if paradigm else "UNKNOWN"
                if "GEX" in pg: pgroup = "GEX"
                elif "BOFA" in pg or "BOFA" in pg: pgroup = "BofA"
                elif "AG" in pg: pgroup = "AG"
                elif "SIDIAL" in pg: pgroup = "SIDIAL"
                else: pgroup = "OTHER"

                # Check direction alignment with paradigm
                if direction == "bullish":
                    aligned = pgroup in ("GEX", "BofA")  # bullish paradigms
                else:
                    aligned = pgroup in ("AG", "SIDIAL")  # bearish paradigms

                signals.append({
                    "date": day_str, "time": ts_str[11:19], "direction": direction,
                    "price": trigger["close"], "pattern": best["pat"],
                    "cz": best["cz"], "score": best["score"],
                    "outcome": res, "pnl": pnl,
                    "paradigm": paradigm, "pgroup": pgroup,
                    "aligned": aligned,
                    "dd_hedging": dd_hedging,
                })

                if direction == "bullish": cd_bull = trigger["idx"]
                else: cd_bear = trigger["idx"]

    # ── Print all signals with paradigm ──────────────────────────────
    print(f'\n{"=" * 130}')
    print(f'  PROPOSED d15 WITH PARADIGM — {len(signals)} signals')
    print(f'{"=" * 130}')
    print(f'{"Date":>12} {"Time":>8} {"Dir":>7} {"Price":>8} {"Pattern":>12} {"Z":>5} {"Result":>7} '
          f'{"PnL":>6} {"Paradigm":>18} {"Group":>7} {"Aligned":>7}')
    print('-' * 130)

    for s in signals:
        print(f'{s["date"]:>12} {s["time"]:>8} {s["direction"]:>7} {s["price"]:8.1f} '
              f'{s["pattern"]:>12} {s["cz"]:5.2f} {s["outcome"]:>7} {s["pnl"]:+6.1f} '
              f'{s["paradigm"]:>18} {s["pgroup"]:>7} {"YES" if s["aligned"] else "no":>7}')

    # ── Summary by paradigm group ────────────────────────────────────
    print(f'\n{"=" * 80}')
    print(f'  BY PARADIGM GROUP')
    print(f'{"=" * 80}')
    print(f'{"Paradigm":>10} | {"Signals":>7} | {"Wins":>4} | {"WR":>5} | {"PnL":>8} | {"Bull":>4} | {"Bear":>4}')
    print('-' * 80)

    by_pg = defaultdict(list)
    for s in signals: by_pg[s["pgroup"]].append(s)
    for pg in ["GEX", "BofA", "AG", "SIDIAL", "OTHER"]:
        ss = by_pg.get(pg, [])
        if not ss: continue
        w = sum(1 for s in ss if s["outcome"] == "WIN")
        pnl = sum(s["pnl"] for s in ss)
        bl = sum(1 for s in ss if s["direction"] == "bullish")
        br = sum(1 for s in ss if s["direction"] == "bearish")
        print(f'{pg:>10} | {len(ss):>7} | {w:>4} | {w/len(ss)*100:>4.0f}% | {pnl:>+8.1f} | {bl:>4} | {br:>4}')

    # ── By exact paradigm ────────────────────────────────────────────
    print(f'\n{"=" * 80}')
    print(f'  BY EXACT PARADIGM')
    print(f'{"=" * 80}')
    by_exact = defaultdict(list)
    for s in signals: by_exact[s["paradigm"]].append(s)
    print(f'{"Paradigm":>20} | {"Signals":>7} | {"Wins":>4} | {"WR":>5} | {"PnL":>8}')
    print('-' * 80)
    for p, ss in sorted(by_exact.items(), key=lambda x: sum(s["pnl"] for s in x[1]), reverse=True):
        w = sum(1 for s in ss if s["outcome"] == "WIN")
        pnl = sum(s["pnl"] for s in ss)
        print(f'{p:>20} | {len(ss):>7} | {w:>4} | {w/len(ss)*100:>4.0f}% | {pnl:>+8.1f}')

    # ── Aligned vs not aligned ───────────────────────────────────────
    print(f'\n{"=" * 80}')
    print(f'  PARADIGM ALIGNMENT (signal direction matches paradigm bias)')
    print(f'{"=" * 80}')
    aligned = [s for s in signals if s["aligned"]]
    not_aligned = [s for s in signals if not s["aligned"]]
    for label, ss in [("Aligned", aligned), ("Not aligned", not_aligned)]:
        if not ss: continue
        w = sum(1 for s in ss if s["outcome"] == "WIN")
        pnl = sum(s["pnl"] for s in ss)
        print(f'  {label}: {len(ss)} signals, {w}W, WR={w/len(ss)*100:.0f}%, PnL={pnl:+.1f}')

    # ── Bullish-only in bullish paradigms, bearish-only in bearish ───
    print(f'\n{"=" * 80}')
    print(f'  FILTER SIMULATIONS')
    print(f'{"=" * 80}')

    # Filter 1: Only aligned (bullish in GEX/BofA, bearish in AG/SIDIAL)
    f1 = [s for s in signals if s["aligned"]]
    w1 = sum(1 for s in f1 if s["outcome"] == "WIN")
    p1 = sum(s["pnl"] for s in f1)
    print(f'  Filter 1 — Only aligned:        {len(f1)} sigs, {w1}W, WR={w1/len(f1)*100:.0f}%, PnL={p1:+.1f}' if f1 else '  Filter 1: 0 signals')

    # Filter 2: Block AG/SIDIAL paradigms entirely
    f2 = [s for s in signals if s["pgroup"] not in ("AG", "SIDIAL")]
    w2 = sum(1 for s in f2 if s["outcome"] == "WIN")
    p2 = sum(s["pnl"] for s in f2)
    print(f'  Filter 2 — Block AG/SIDIAL:      {len(f2)} sigs, {w2}W, WR={w2/len(f2)*100:.0f}%, PnL={p2:+.1f}' if f2 else '  Filter 2: 0 signals')

    # Filter 3: Only GEX paradigms
    f3 = [s for s in signals if s["pgroup"] == "GEX"]
    w3 = sum(1 for s in f3 if s["outcome"] == "WIN")
    p3 = sum(s["pnl"] for s in f3)
    print(f'  Filter 3 — Only GEX:             {len(f3)} sigs, {w3}W, WR={w3/len(f3)*100:.0f}%, PnL={p3:+.1f}' if f3 else '  Filter 3: 0 signals')

    # Filter 4: Block bearish signals in GEX/BofA (bullish paradigm = only go long)
    f4 = [s for s in signals if not (s["direction"] == "bearish" and s["pgroup"] in ("GEX", "BofA"))]
    w4 = sum(1 for s in f4 if s["outcome"] == "WIN")
    p4 = sum(s["pnl"] for s in f4)
    print(f'  Filter 4 — No bear in GEX/BofA:  {len(f4)} sigs, {w4}W, WR={w4/len(f4)*100:.0f}%, PnL={p4:+.1f}' if f4 else '  Filter 4: 0 signals')

    # Filter 5: Block bullish signals in AG/SIDIAL (bearish paradigm = only go short)
    f5 = [s for s in signals if not (s["direction"] == "bullish" and s["pgroup"] in ("AG", "SIDIAL"))]
    w5 = sum(1 for s in f5 if s["outcome"] == "WIN")
    p5 = sum(s["pnl"] for s in f5)
    print(f'  Filter 5 — No bull in AG/SIDIAL:  {len(f5)} sigs, {w5}W, WR={w5/len(f5)*100:.0f}%, PnL={p5:+.1f}' if f5 else '  Filter 5: 0 signals')

    # Filter 6: Combine F4+F5 (respect paradigm direction)
    f6 = [s for s in signals if not (s["direction"] == "bearish" and s["pgroup"] in ("GEX", "BofA"))
                              and not (s["direction"] == "bullish" and s["pgroup"] in ("AG", "SIDIAL"))]
    w6 = sum(1 for s in f6 if s["outcome"] == "WIN")
    p6 = sum(s["pnl"] for s in f6)
    print(f'  Filter 6 — Respect paradigm dir:  {len(f6)} sigs, {w6}W, WR={w6/len(f6)*100:.0f}%, PnL={p6:+.1f}' if f6 else '  Filter 6: 0 signals')

    # Filter 7: Only MESSY paradigms (let both directions through)
    f7 = [s for s in signals if "MESSY" in s["paradigm"].upper()]
    w7 = sum(1 for s in f7 if s["outcome"] == "WIN")
    p7 = sum(s["pnl"] for s in f7)
    print(f'  Filter 7 — Only MESSY:            {len(f7)} sigs, {w7}W, WR={w7/len(f7)*100:.0f}%, PnL={p7:+.1f}' if f7 else '  Filter 7: 0 signals')

    # Baseline
    wt = sum(1 for s in signals if s["outcome"] == "WIN")
    pt = sum(s["pnl"] for s in signals)
    print(f'\n  BASELINE (no filter):             {len(signals)} sigs, {wt}W, WR={wt/len(signals)*100:.0f}%, PnL={pt:+.1f}')


if __name__ == "__main__":
    run()
