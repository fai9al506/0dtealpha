# -*- coding: utf-8 -*-
"""V19b — vectorised exit walk (numpy), proven identical to app.mes_sim_backfill.mes_walk.

The trail is path-dependent but its stop level is a monotone function of the running max
favourable excursion, so the whole walk reduces to:
    L_i  = lock level entering bar i = g(cummax(fav)[i-1])
    exit at the first bar where adverse_i >= -L_i
which numpy can evaluate in one pass per parameter set.
"""
import numpy as np, pickle, os, bisect
from datetime import timedelta
from _tmp_v19_exit import UNIV, BARS, BT, bars_to_eod, live_params, SETUPS
from app.mes_sim_backfill import mes_walk

PREP = "_tmp_v19_prep.pkl"


def prep():
    """Per trade: numpy arrays of favourable and adverse excursion per bar, plus the close."""
    if os.path.exists(PREP):
        return pickle.load(open(PREP, "rb"))
    out = {}
    for u in UNIV:
        b = bars_to_eod(u)
        if not b:
            continue
        h = np.array([x[3] for x in b], dtype=np.float64)
        l = np.array([x[4] for x in b], dtype=np.float64)
        e = u["entry"]
        if u["is_long"]:
            fav = h - e; adv = e - l
        else:
            fav = e - l; adv = h - e
        out[u["id"]] = (fav, adv, float(b[-1][5]))
    pickle.dump(out, open(PREP, "wb"))
    return out


P = prep()


def fast_walk(uid, is_long, entry, sl, be_trigger, be_lock, act, gap):
    """Returns pnl in points. Identical semantics to mes_walk (adverse-first within bar)."""
    d = P.get(uid)
    if d is None:
        return None
    fav, adv, close = d
    n = len(fav)
    F = np.maximum.accumulate(fav)
    Fprev = np.empty(n, dtype=np.float64)
    Fprev[0] = 0.0
    if n > 1:
        Fprev[1:] = F[:-1]
    # lock level entering each bar (points of profit protected; -sl = initial stop)
    L = np.full(n, -float(sl), dtype=np.float64)
    if act is not None:
        trail = Fprev - gap
        m = Fprev >= act
        L[m] = np.maximum(L[m], trail[m])
    if be_trigger is not None:
        m = (Fprev >= be_trigger) & (Fprev < (act if act is not None else 1e9))
        L[m] = np.maximum(L[m], be_lock)
    L = np.maximum.accumulate(L)          # the stop never moves backwards
    hit = adv >= -L
    idx = int(np.argmax(hit)) if hit.any() else -1
    if idx >= 0:
        return float(L[idx])
    return float(close - entry) if is_long else float(entry - close)


if __name__ == "__main__":
    print("### proving the vectorised walk == mes_walk on the live parameters")
    bad = 0; n = 0
    for u in UNIV[:1200]:
        p = live_params(u["setup"])
        b = bars_to_eod(u)
        if not b:
            continue
        mm = int((b[-1][0] - b[0][0]).total_seconds() / 60) + 2
        ref = mes_walk(b, u["entry"], u["is_long"], p[0], p[1], p[2], p[3], p[4], mm)["pnl"]
        got = fast_walk(u["id"], u["is_long"], u["entry"], *p)
        n += 1
        if abs(ref - got) > 1e-6:
            bad += 1
            if bad <= 6:
                print(f"  MISMATCH id={u['id']} {u['setup']} ref={ref:.3f} fast={got:.3f}")
    print(f"  checked {n} trades, mismatches {bad}  -> {'IDENTICAL' if bad==0 else 'DIFFERENT'}")

    import time
    t0 = time.time()
    for u in UNIV:
        fast_walk(u["id"], u["is_long"], u["entry"], 12, None, 0, 10, 5)
    print(f"  full-universe pass: {time.time()-t0:.2f}s for {len(UNIV)} trades")
