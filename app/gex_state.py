# -*- coding: utf-8 -*-
"""GEX dealer-positioning state (S244) — self-contained, fail-soft, MONITORING ONLY.

Productionises the Exelza "GEX Dealer Positioning" framework for SPX 0DTE:
six cards (net GEX, net DEX, zero gamma, call wall, put wall, max gamma) plus the
11-state signal taxonomy. Study + evidence: S244_GEX_FRAMEWORK_STUDY.md.

The SUPPORT state is the candidate gate for **GEX Long v7 ("Gamma Support")** — a gate on the
existing v6 detector, NOT a standalone trigger. v7 is NOT implemented in the trade path; this
module only stamps the states so the forward sample accumulates.

NOTHING HERE TOUCHES THE TRADE PATH. It reads `chain_snapshots` (already saved every
2 min), writes `gex_state`, and stamps `setup_log.gex_*` so the states accumulate
forward for validation. Every entry point swallows its own exceptions.

Init from main.py:  gex_state.init(engine)
Scheduler:          gex_state.capture()        every 2 min, market hours
                    gex_state.stamp_setups()   EOD, backfills setup_log

Why one `compute()`: the live path and the backfill MUST agree. The project has been
burned before by parallel implementations drifting (gex_long_v3 `_features` graded on
Volland while the live detector used TS). There is exactly one implementation here.
"""
from __future__ import annotations

import json
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import text

ET = ZoneInfo("America/New_York")

# chain_snapshots.rows positional layout (mirrored call | Strike | put).
# Verified against live data 2026-08-11 — see feedback_gex_means_ts_gamma.
C_OI, C_GAMMA, C_DELTA = 1, 3, 4
STRIKE = 10
P_DELTA, P_GAMMA, P_OI = 16, 17, 19

# Zero-gamma proximity band for the HIGH_VOLATILITY state.
# The guide says "within 1% of zero gamma"; on SPX 1% is ~77 pt, which swallowed 43%
# of all snapshots. Rescaled to 8 pt. Results are insensitive (S244 sweep: 0/5/8/12/20
# pt all land in the same place).
ZG_BAND_PTS = 8.0

_engine = None


# ====================== PURE CORE (no I/O — unit-testable) ======================
def compute(spot: float, rows: list) -> dict | None:
    """Derive the six cards + state label from one chain snapshot.

    Args:
        spot: SPX spot at the snapshot
        rows: chain_snapshots.rows (list of positional lists)
    Returns dict, or None when the snapshot is unusable.
    """
    if not spot or spot <= 100 or not rows:
        return None
    ks, cgex, pgex, ngex, dex = [], [], [], [], []
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
    if len(ks) < 10:
        return None
    order = sorted(range(len(ks)), key=lambda i: ks[i])
    ks = [ks[i] for i in order]
    cgex = [cgex[i] for i in order]; pgex = [pgex[i] for i in order]
    ngex = [ngex[i] for i in order]; dex = [dex[i] for i in order]

    net_gex = sum(ngex)
    net_dex = sum(dex)

    # --- zero gamma: where the cumulative net-GEX profile (low -> high strike) crosses 0 ---
    zg = None
    run = 0.0
    prev_k = prev_run = None
    for k, g in zip(ks, ngex):
        new = run + g
        if prev_run is not None and ((prev_run <= 0 < new) or (prev_run >= 0 > new)):
            denom = new - prev_run
            zg = prev_k + ((0.0 - prev_run) / denom if denom else 0.0) * (k - prev_k)
        prev_k, prev_run = k, new
        run = new
    zg_in_window = zg is not None
    if zg is None:
        # No crossing: the whole near-spot profile is one sign, so the flip is outside
        # the strike window. All-positive => flip is BELOW us; all-negative => ABOVE us.
        zg_side = 1 if net_gex > 0 else -1
        zg_dist = (spot - ks[0]) if zg_side > 0 else -(ks[-1] - spot)
    else:
        zg_side = 1 if spot > zg else -1
        zg_dist = spot - zg

    def _argmax(vals):
        bi = bv = None
        for i, v in enumerate(vals):
            if v > 0 and (bv is None or v > bv):
                bi, bv = i, v
        return ks[bi] if bi is not None else None

    call_wall = _argmax(cgex)
    put_wall = _argmax(pgex)
    max_gamma = _argmax([a + b for a, b in zip(cgex, pgex)])

    state = _label(spot, net_gex, net_dex, zg, call_wall, put_wall)
    return dict(
        spot=spot,
        net_gex=net_gex, net_dex=net_dex,
        zero_gamma=zg, zg_in_window=zg_in_window, zg_side=zg_side, zg_dist=zg_dist,
        call_wall=call_wall, put_wall=put_wall, max_gamma=max_gamma,
        head_call_wall=(call_wall - spot) if call_wall is not None else None,
        drop_put_wall=(spot - put_wall) if put_wall is not None else None,
        state=state,
        state_bias=BIAS.get(state),
        is_support=(state == "SUPPORT"),
        k_min=ks[0], k_max=ks[-1],
    )


BIAS = {
    "BREAKOUT_TEST": "buy", "SUPPORT": "buy", "SQUEEZE": "buy", "SHORT_COVER_BOUNCE": "buy",
    "RESISTANCE": "sell", "BREAKDOWN_TEST": "sell", "FAILED_SQUEEZE": "sell", "ACCELERATION": "sell",
}
# The guide's own reliability tiers (ch. 4). Measured on our book in S244 §3 and the
# ranking reproduced: quiet-reliable +3.07 pt/trade > explosive +2.11 > cautionary +1.67
# > conditional +0.16.
TIER = {
    "SUPPORT": "quiet-reliable", "RESISTANCE": "quiet-reliable", "MEAN_REVERSION": "quiet-reliable",
    "SQUEEZE": "explosive", "ACCELERATION": "explosive",
    "BREAKOUT_TEST": "conditional", "BREAKDOWN_TEST": "conditional",
    "HIGH_VOLATILITY": "energy-no-direction",
    "FAILED_SQUEEZE": "cautionary", "SHORT_COVER_BOUNCE": "cautionary", "CHOPPY": "cautionary",
}


def _label(spot, net_gex, net_dex, zg, call_wall, put_wall) -> str | None:
    if call_wall is None or put_wall is None:
        return None
    if zg is not None and abs(spot - zg) < ZG_BAND_PTS:
        return "HIGH_VOLATILITY"
    if net_gex > 0:
        if spot > call_wall:
            return "BREAKOUT_TEST" if net_dex > 0 else "RESISTANCE"
        if spot < put_wall:
            return "BREAKDOWN_TEST" if net_dex < 0 else "SUPPORT"
        return "MEAN_REVERSION"
    if spot > call_wall:
        return "SQUEEZE" if net_dex > 0 else "FAILED_SQUEEZE"
    if spot < put_wall:
        return "SHORT_COVER_BOUNCE" if net_dex > 0 else "ACCELERATION"
    return "CHOPPY"


# ====================== INIT / DB ======================
def init(engine):
    global _engine
    _engine = engine
    try:
        _db_init()
    except Exception:
        print(f"[gex-state] db_init failed (non-fatal): {traceback.format_exc()}", flush=True)
    print("[gex-state] initialized (monitoring-only)", flush=True)


def _db_init():
    if not _engine:
        return
    with _engine.begin() as c:
        c.execute(text("""CREATE TABLE IF NOT EXISTS gex_state (
            et            timestamp PRIMARY KEY,
            spot          double precision,
            state         text,
            net_gex       double precision,
            net_dex       double precision,
            zero_gamma    double precision,
            call_wall     double precision,
            put_wall      double precision,
            max_gamma     double precision,
            payload       jsonb)"""))
    # setup_log stamps — separate short transactions so one failure cannot block the rest
    for col, typ in (("gex_state", "text"), ("gex_net_dex", "double precision"),
                     ("gex_net_gex", "double precision"), ("gex_zero_gamma", "double precision"),
                     ("gex_call_wall", "double precision"), ("gex_put_wall", "double precision")):
        try:
            with _engine.begin() as c:
                c.execute(text(f"ALTER TABLE setup_log ADD COLUMN IF NOT EXISTS {col} {typ}"))
        except Exception:
            pass


# ====================== CAPTURE (every 2 min, market hours) ======================
def capture():
    """Read the newest chain_snapshots row, derive the state, upsert into gex_state."""
    if not _engine:
        return
    try:
        with _engine.connect() as c:
            row = c.execute(text(
                "SELECT ts, spot, rows FROM chain_snapshots "
                "WHERE spot IS NOT NULL AND spot > 100 ORDER BY ts DESC LIMIT 1")).fetchone()
        if not row:
            return
        ts, spot, rows = row
        rows = rows if isinstance(rows, list) else json.loads(rows)
        f = compute(float(spot), rows)
        if not f:
            return
        et = ts.astimezone(ET).replace(tzinfo=None)
        with _engine.begin() as c:
            c.execute(text("""INSERT INTO gex_state
                (et, spot, state, net_gex, net_dex, zero_gamma, call_wall, put_wall, max_gamma, payload)
                VALUES (:et,:spot,:state,:ng,:nd,:zg,:cw,:pw,:mg,CAST(:pl AS jsonb))
                ON CONFLICT (et) DO UPDATE SET
                  spot=EXCLUDED.spot, state=EXCLUDED.state, net_gex=EXCLUDED.net_gex,
                  net_dex=EXCLUDED.net_dex, zero_gamma=EXCLUDED.zero_gamma,
                  call_wall=EXCLUDED.call_wall, put_wall=EXCLUDED.put_wall,
                  max_gamma=EXCLUDED.max_gamma, payload=EXCLUDED.payload"""),
                dict(et=et, spot=f["spot"], state=f["state"], ng=f["net_gex"], nd=f["net_dex"],
                     zg=f["zero_gamma"], cw=f["call_wall"], pw=f["put_wall"], mg=f["max_gamma"],
                     pl=json.dumps(f)))
    except Exception:
        print(f"[gex-state] capture error (non-fatal): {traceback.format_exc()}", flush=True)


# ====================== STAMP setup_log (EOD) ======================
def stamp_setups(days: int = 3) -> int:
    """Stamp setup_log.gex_* using the last chain snapshot AT OR BEFORE each entry.

    Strictly prior — never a later snapshot — so the stamp is what was knowable at
    signal time. Returns rows stamped.
    """
    if not _engine:
        return 0
    try:
        with _engine.connect() as c:
            snaps = c.execute(text(
                "SELECT ts, spot, rows FROM chain_snapshots "
                "WHERE spot IS NOT NULL AND spot > 100 "
                "  AND ts >= now() - CAST(:d AS interval) ORDER BY ts"),
                dict(d=f"{days + 1} days")).fetchall()
            trades = c.execute(text(
                "SELECT id, ts FROM setup_log "
                "WHERE ts >= now() - CAST(:d AS interval) AND gex_state IS NULL "
                "ORDER BY ts"), dict(d=f"{days} days")).fetchall()
        if not snaps or not trades:
            return 0
        eps = [s[0].timestamp() for s in snaps]
        cache, out = {}, []
        for lid, ts in trades:
            e = ts.timestamp()
            lo, hi = 0, len(eps) - 1
            j = -1
            while lo <= hi:                       # last index with eps[idx] <= e
                mid = (lo + hi) // 2
                if eps[mid] <= e:
                    j = mid; lo = mid + 1
                else:
                    hi = mid - 1
            if j < 0 or (e - eps[j]) > 300:       # no snapshot within 5 min before entry
                continue
            if j not in cache:
                _, sp, rw = snaps[j]
                rw = rw if isinstance(rw, list) else json.loads(rw)
                cache[j] = compute(float(sp), rw)
            f = cache[j]
            if not f:
                continue
            out.append(dict(lid=lid, st=f["state"], nd=f["net_dex"], ng=f["net_gex"],
                            zg=f["zero_gamma"], cw=f["call_wall"], pw=f["put_wall"]))
        if not out:
            return 0
        with _engine.begin() as c:
            for o in out:
                c.execute(text("""UPDATE setup_log SET gex_state=:st, gex_net_dex=:nd,
                    gex_net_gex=:ng, gex_zero_gamma=:zg, gex_call_wall=:cw, gex_put_wall=:pw
                    WHERE id=:lid"""), o)
        print(f"[gex-state] stamped {len(out)} setup_log rows", flush=True)
        return len(out)
    except Exception:
        print(f"[gex-state] stamp error (non-fatal): {traceback.format_exc()}", flush=True)
        return 0


# ====================== READ ACCESSORS (portal / API) ======================
def latest() -> dict:
    """Newest state row + the derived reliability tier. {} on any failure."""
    if not _engine:
        return {}
    try:
        with _engine.connect() as c:
            r = c.execute(text("SELECT et, payload FROM gex_state ORDER BY et DESC LIMIT 1")).fetchone()
        if not r:
            return {}
        p = r[1] if isinstance(r[1], dict) else json.loads(r[1])
        p["et"] = r[0].isoformat()
        p["tier"] = TIER.get(p.get("state"))
        return p
    except Exception:
        return {}


def history(date: str | None = None) -> list:
    """All state rows for an ET date (default today). [] on any failure."""
    if not _engine:
        return []
    try:
        d = date or datetime.now(ET).date().isoformat()
        with _engine.connect() as c:
            rows = c.execute(text(
                "SELECT et, spot, state, net_gex, net_dex, zero_gamma, call_wall, put_wall, max_gamma "
                "FROM gex_state WHERE et::date = CAST(:d AS date) ORDER BY et"), dict(d=d)).fetchall()
        return [dict(et=r[0].isoformat(), spot=r[1], state=r[2], tier=TIER.get(r[2]),
                     net_gex=r[3], net_dex=r[4], zero_gamma=r[5],
                     call_wall=r[6], put_wall=r[7], max_gamma=r[8]) for r in rows]
    except Exception:
        return []
