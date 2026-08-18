"""S244 — Build the Exelza "GEX Dealer Positioning" state engine for SPX 0DTE.

Source: chain_snapshots (TS chain, 2-min cadence, ~+-100pt strike window).
Column layout (positional): call Volume0 OI1 IV2 Gamma3 Delta4 ... Strike10 ...
                            put Delta16 Gamma17 IV18 OI19 Volume20

Computes per snapshot:
  net_gex          sum(C_Gamma*C_OI - P_Gamma*P_OI)         (TS convention, memory feedback_gex_means_ts_gamma)
  net_dex          sum(C_Delta*C_OI + P_Delta*P_OI)         (put delta already negative)
  zero_gamma       strike where cumulative net_gex (low->high) crosses zero (interpolated)
  call_wall        strike with max call gamma-exposure (C_Gamma*C_OI) anywhere in window
  put_wall         strike with max put gamma-exposure  (P_Gamma*P_OI)
  call_wall_oi     strike with max raw call OI
  put_wall_oi      strike with max raw put OI
  max_gamma        strike with max total |gamma*OI|
  plus distances, and the 11-state Exelza signal label.

Writes _tmp_s244_gexstate.pkl : list of dicts sorted by epoch.
"""
import os, json, pickle, sys
import psycopg2

C_OI, C_GAMMA, C_DELTA = 1, 3, 4
STRIKE = 10
P_DELTA, P_GAMMA, P_OI = 16, 17, 19

START = os.environ.get("S244_START", "2026-01-20")


def parse_snapshot(spot, rows):
    """Return per-strike arrays + aggregates, or None if unusable."""
    ks, cgex, pgex, ngex, dex, coi, poi = [], [], [], [], [], [], []
    for r in rows:
        try:
            k = float(r[STRIKE])
            cg = float(r[C_GAMMA] or 0.0); co = float(r[C_OI] or 0.0); cd = float(r[C_DELTA] or 0.0)
            pg = float(r[P_GAMMA] or 0.0); po = float(r[P_OI] or 0.0); pd = float(r[P_DELTA] or 0.0)
        except (TypeError, ValueError, IndexError):
            continue
        ks.append(k)
        cgex.append(cg * co)
        pgex.append(pg * po)
        ngex.append(cg * co - pg * po)
        dex.append(cd * co + pd * po)
        coi.append(co); poi.append(po)
    if len(ks) < 10:
        return None
    order = sorted(range(len(ks)), key=lambda i: ks[i])
    ks = [ks[i] for i in order]; cgex = [cgex[i] for i in order]; pgex = [pgex[i] for i in order]
    ngex = [ngex[i] for i in order]; dex = [dex[i] for i in order]
    coi = [coi[i] for i in order]; poi = [poi[i] for i in order]

    net_gex = sum(ngex)
    net_dex = sum(dex)
    call_dex = None  # kept simple; net is what the framework uses

    # --- zero gamma: cumulative net gex crossing (interpolated between strikes) ---
    zg = None
    run = 0.0
    prev_k, prev_run = None, None
    for k, g in zip(ks, ngex):
        new = run + g
        if prev_run is not None and ((prev_run <= 0 < new) or (prev_run >= 0 > new)):
            denom = (new - prev_run)
            frac = (0.0 - prev_run) / denom if denom else 0.0
            zg = prev_k + frac * (k - prev_k)
        prev_k, prev_run = k, new
        run = new
    # If the running sum never crosses, the flip sits outside the strike window.
    #   all-positive cumulative -> even the lowest strikes are call-dominated -> flip BELOW window
    #                              -> spot is ABOVE zero gamma
    #   all-negative cumulative -> flip ABOVE window -> spot is BELOW zero gamma
    zg_in_window = zg is not None
    if zg is None:
        zg_side = 1 if net_gex > 0 else -1          # +1 = spot above flip, -1 = below
        # signed distance, floored at the window edge (conservative, not extrapolated)
        zg_dist_resolved = (spot - ks[0]) if zg_side > 0 else -(ks[-1] - spot)
    else:
        zg_side = 1 if spot > zg else -1
        zg_dist_resolved = spot - zg

    def argmax_strike(vals, positive_only=True):
        best_i, best_v = None, None
        for i, v in enumerate(vals):
            if positive_only and v <= 0:
                continue
            if best_v is None or v > best_v:
                best_i, best_v = i, v
        return (ks[best_i], best_v) if best_i is not None else (None, None)

    call_wall, call_wall_v = argmax_strike(cgex)
    put_wall, put_wall_v = argmax_strike(pgex)
    call_wall_oi, _ = argmax_strike(coi)
    put_wall_oi, _ = argmax_strike(poi)
    tot_g = [a + b for a, b in zip(cgex, pgex)]
    max_gamma, _ = argmax_strike(tot_g)

    # nearest positive-net-gex ceiling above spot (the prior S-study's "magnet"), <=60pt
    ceil_k, ceil_v = None, None
    for k, g in zip(ks, ngex):
        if k > spot and (k - spot) <= 60 and g > 0 and (ceil_v is None or g > ceil_v):
            ceil_k, ceil_v = k, g
    # nearest negative-net-gex floor below spot
    floor_k, floor_v = None, None
    for k, g in zip(ks, ngex):
        if k < spot and (spot - k) <= 60 and g < 0 and (floor_v is None or g < floor_v):
            floor_k, floor_v = k, g

    gex_above = sum(g for k, g in zip(ks, ngex) if k > spot)
    gex_below = sum(g for k, g in zip(ks, ngex) if k < spot)

    return dict(
        net_gex=net_gex, net_dex=net_dex,
        zero_gamma=zg, zg_in_window=zg_in_window,
        zg_side=zg_side, zg_dist_resolved=zg_dist_resolved,
        call_wall=call_wall, put_wall=put_wall,
        call_wall_oi=call_wall_oi, put_wall_oi=put_wall_oi,
        max_gamma=max_gamma,
        ceil_k=ceil_k, floor_k=floor_k,
        gex_above=gex_above, gex_below=gex_below,
        k_min=ks[0], k_max=ks[-1],
    )


def label_state(spot, f):
    """Exelza 11-state taxonomy (page 15 glossary).

    Uses Call Wall / Put Wall as absolute chain-wide maxima (price may be above or below).
    Returns (state, bias) or (None, None) when undeterminable.
    """
    zg, cw, pw = f["zero_gamma"], f["call_wall"], f["put_wall"]
    ng, nd = f["net_gex"], f["net_dex"]
    if cw is None or pw is None:
        return None, None
    # HIGH VOLATILITY takes precedence: price within 1% of zero gamma
    if zg is not None and abs(spot - zg) / spot < 0.01:
        return "HIGH_VOLATILITY", ("buy" if nd > 0 else "sell" if nd < 0 else "neutral")
    pos_gex = ng > 0
    above_cw = spot > cw
    below_pw = spot < pw
    if pos_gex:
        if above_cw:
            return ("BREAKOUT_TEST", "buy") if nd > 0 else ("RESISTANCE", "sell")
        if below_pw:
            return ("BREAKDOWN_TEST", "sell") if nd < 0 else ("SUPPORT", "buy")
        return "MEAN_REVERSION", ("buy" if nd > 0 else "sell" if nd < 0 else "neutral")
    else:
        if above_cw:
            return ("SQUEEZE", "buy") if nd > 0 else ("FAILED_SQUEEZE", "sell")
        if below_pw:
            return ("SHORT_COVER_BOUNCE", "buy") if nd > 0 else ("ACCELERATION", "sell")
        return "CHOPPY", ("buy" if nd > 0 else "sell" if nd < 0 else "neutral")


def main():
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = True          # DB discipline: never hold a long read txn on prod
    cur = conn.cursor()
    cur.execute(
        """SELECT ts, spot, vix, rows FROM chain_snapshots
           WHERE spot IS NOT NULL AND spot > 100 AND ts >= %s::timestamptz ORDER BY ts""",
        (START,),
    )
    out = []
    n_bad = 0
    while True:
        batch = cur.fetchmany(2000)
        if not batch:
            break
        for ts, spot, vix, rows in batch:
            spot = float(spot)
            rows = rows if isinstance(rows, list) else json.loads(rows)
            f = parse_snapshot(spot, rows)
            if f is None:
                n_bad += 1
                continue
            state, bias = label_state(spot, f)
            f.update(
                epoch=ts.timestamp(), ts=ts, spot=spot,
                vix=float(vix) if vix is not None else None,
                state=state, state_bias=bias,
                head_cw=(f["call_wall"] - spot) if f["call_wall"] is not None else None,
                head_ceil=(f["ceil_k"] - spot) if f["ceil_k"] is not None else None,
                drop_pw=(spot - f["put_wall"]) if f["put_wall"] is not None else None,
                dist_zg=(spot - f["zero_gamma"]) if f["zero_gamma"] is not None else None,
            )
            out.append(f)
        print(f"  ... {len(out)} snapshots", file=sys.stderr)
    print(f"parsed {len(out)} snapshots, {n_bad} unusable", file=sys.stderr)

    # ---- running intraday range position (no lookahead: hi/lo so far today) ----
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
    cur_day, hi, lo, open_px = None, None, None, None
    for f in out:
        et = f["ts"].astimezone(ET)
        d = et.date()
        if d != cur_day:
            cur_day, hi, lo, open_px = d, f["spot"], f["spot"], f["spot"]
        hi = max(hi, f["spot"]); lo = min(lo, f["spot"])
        f["day_hi"], f["day_lo"], f["day_open"] = hi, lo, open_px
        f["range_pos"] = ((f["spot"] - lo) / (hi - lo)) if hi > lo else 0.5
        f["from_open"] = f["spot"] - open_px

    with open("_tmp_s244_gexstate.pkl", "wb") as fh:
        pickle.dump(out, fh)

    # quick sanity
    from collections import Counter
    print("\nstate distribution:")
    for s, n in Counter(x["state"] for x in out).most_common():
        print(f"  {s}: {n} ({100*n/len(out):.1f}%)")
    zgw = sum(1 for x in out if x["zg_in_window"])
    print(f"\nzero-gamma inside +-100pt window: {zgw}/{len(out)} ({100*zgw/len(out):.1f}%)")
    pos = sum(1 for x in out if x["net_gex"] > 0)
    print(f"net_gex positive: {pos}/{len(out)} ({100*pos/len(out):.1f}%)")
    posd = sum(1 for x in out if x["net_dex"] > 0)
    print(f"net_dex positive: {posd}/{len(out)} ({100*posd/len(out):.1f}%)")


if __name__ == "__main__":
    main()
