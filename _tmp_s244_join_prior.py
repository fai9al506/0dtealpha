"""S244 — join fired setups to the nearest GEX-state snapshot (no lookahead).

Writes _tmp_s244_trades_prior.pkl
"""
import os, pickle, bisect
import psycopg2

MAX_LAG_S = 300  # 2-min cadence -> accept nearest snapshot within 4 min


def main():
    with open("_tmp_s244_gexstate.pkl", "rb") as fh:
        snaps = pickle.load(fh)
    snaps.sort(key=lambda x: x["epoch"])
    ep = [s["epoch"] for s in snaps]

    conn = psycopg2.connect(os.environ["DATABASE_URL"]); conn.autocommit = True
    cur = conn.cursor()
    cur.execute(
        """SELECT id, ts, setup_name, direction, spot, outcome_pnl, outcome_result,
                  mes_sim_outcome_pnl, paradigm, grade, vix, live_pass, outcome_max_profit
           FROM setup_log
           WHERE outcome_pnl IS NOT NULL AND spot IS NOT NULL
             AND ts >= '2026-01-20'::timestamptz
           ORDER BY ts"""
    )
    rows = cur.fetchall()

    out, miss = [], 0
    for (lid, ts, name, direction, spot, pnl, res, mespnl, para, grade, vix, lp, mfe) in rows:
        e = ts.timestamp()
        i = bisect.bisect_left(ep, e)
        # STRICTLY PRIOR ONLY - no lookahead. Take the last snapshot at or before entry.
        j = i - 1
        while j >= 0 and ep[j] > e:
            j -= 1
        if j < 0 or (e - ep[j]) > MAX_LAG_S:
            miss += 1; continue
        s = snaps[j]
        spot = float(spot)
        d = (direction or "").lower()
        is_long = d in ("long", "bullish", "buy")
        rec = dict(
            lid=lid, ts=ts, name=name, is_long=is_long, spot=spot,
            pnl=float(pnl), res=res,
            mes_pnl=float(mespnl) if mespnl is not None else None,
            mfe=float(mfe) if mfe is not None else None,
            paradigm=para, grade=grade,
            vix=float(vix) if vix is not None else (s["vix"]),
            live_pass=bool(lp),
            lag=abs(ep[j] - e),
        )
        # carry the structural features, recomputed against the SETUP's own spot
        for k in ("net_gex", "net_dex", "zero_gamma", "zg_in_window", "zg_side", "zg_dist_resolved",
                  "range_pos", "from_open", "day_hi", "day_lo", "day_open", "call_wall", "put_wall",
                  "call_wall_oi", "put_wall_oi", "max_gamma", "ceil_k", "floor_k",
                  "gex_above", "gex_below", "k_min", "k_max"):
            rec[k] = s[k]
        rec["snap_spot"] = s["spot"]
        rec["head_cw"] = (s["call_wall"] - spot) if s["call_wall"] is not None else None
        rec["head_ceil"] = (s["ceil_k"] - spot) if s["ceil_k"] is not None else None
        rec["drop_pw"] = (spot - s["put_wall"]) if s["put_wall"] is not None else None
        rec["dist_zg"] = (spot - s["zero_gamma"]) if s["zero_gamma"] is not None else None
        rec["dist_mg"] = (s["max_gamma"] - spot) if s["max_gamma"] is not None else None
        # room in the direction of the trade
        if is_long:
            rec["room"] = rec["head_cw"]
        else:
            rec["room"] = rec["drop_pw"]
        # DEX agreement with the trade direction
        rec["dex_agree"] = (rec["net_dex"] > 0) == is_long
        out.append(rec)

    print(f"joined {len(out)} trades, {miss} missed (no snapshot within {MAX_LAG_S}s)")
    with open("_tmp_s244_trades_prior.pkl", "wb") as fh:
        pickle.dump(out, fh)

    from collections import Counter
    print("by setup:", Counter(x["name"] for x in out).most_common())
    print("long/short:", Counter("long" if x["is_long"] else "short" for x in out))
    print("live_pass:", Counter(x["live_pass"] for x in out))


if __name__ == "__main__":
    main()
