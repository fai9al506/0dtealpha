# -*- coding: utf-8 -*-
"""V18 filter build — shared data layer. Pulls the FULL feature set from setup_log."""
import os, pickle, collections, statistics
from datetime import timedelta
from zoneinfo import ZoneInfo

os.environ.setdefault("VPB_REAL_TRADE_ENABLED", "true")
ET = ZoneInfo("America/New_York")
CACHE = "_tmp_v18_cache.pkl"
START, END = "2026-02-01", "2026-08-07"

FEATURE_COLS = """
 id, ts, setup_name, direction, grade, score, paradigm, spot, lis, target,
 max_plus_gex, max_minus_gex, gap_to_lis, upside, rr_ratio, first_hour,
 support_score, upside_score, floor_cluster_score, target_cluster_score, rr_score,
 abs_vol_ratio, abs_es_price, outcome_result, outcome_pnl, outcome_max_profit,
 outcome_max_loss, outcome_first_event, outcome_elapsed_min, vix, overvix, vix3m,
 vix_vix3m_ratio, greek_alignment, v13_gex_above, v13_dd_near, vanna_cliff_side,
 vanna_peak_side, vanna_regime, vanna_all, vanna_weekly, vanna_monthly, spot_vol_beta,
 basket_pct, mes_sim_outcome_pnl, live_pass, trail_sl, trail_activation, trail_gap
"""

REAL_SETUPS = ("Skew Charm", "AG Short", "Vanna Pivot Bounce", "ES Absorption",
               "DD Exhaustion", "VIX Divergence")


def load(refresh=False):
    if not refresh and os.path.exists(CACHE):
        with open(CACHE, "rb") as f:
            d = pickle.load(f)
        return d["rows"], d["gaps"], d["daily"]
    from sqlalchemy import create_engine, text
    from app.live_filter import load_gaps
    E = create_engine(os.environ["DATABASE_URL"])
    with E.connect().execution_options(isolation_level="AUTOCOMMIT") as c:
        gaps = load_gaps(c)
        rows = [dict(r) for r in c.execute(text(
            f"SELECT {FEATURE_COLS} FROM setup_log WHERE ts >= :a AND ts < :b ORDER BY ts"),
            {"a": START, "b": END}).mappings().all()]
        # daily SPX context: open, close, high, low, prior close
        daily = {}
        for r in c.execute(text("""
            SELECT date(ts AT TIME ZONE 'America/New_York') d,
                   MIN(spot) lo, MAX(spot) hi,
                   (array_agg(spot ORDER BY ts ASC))[1] o,
                   (array_agg(spot ORDER BY ts DESC))[1] cl
            FROM chain_snapshots WHERE spot IS NOT NULL
              AND (ts AT TIME ZONE 'America/New_York')::time >= '09:30'
              AND (ts AT TIME ZONE 'America/New_York')::time <= '16:00'
            GROUP BY 1 ORDER BY 1""")).fetchall():
            daily[str(r[0])] = {"lo": float(r[1]), "hi": float(r[2]),
                                "open": float(r[3]), "close": float(r[4])}
    ds = sorted(daily)
    for i, d in enumerate(ds):
        daily[d]["prev_close"] = daily[ds[i - 1]]["close"] if i else None
        daily[d]["range"] = daily[d]["hi"] - daily[d]["lo"]
    with open(CACHE, "wb") as f:
        pickle.dump({"rows": rows, "gaps": gaps, "daily": daily}, f)
    return rows, gaps, daily


def enrich(rows, gaps, daily):
    """Add derived, POINT-IN-TIME-SAFE features. Nothing here may use future information."""
    out = []
    for r in rows:
        et = r["ts"].astimezone(ET)
        d = et.date().isoformat()
        dd = daily.get(d, {})
        r = dict(r)
        r["et"] = et
        r["date"] = et.date()
        r["month"] = et.strftime("%Y-%m")
        r["mins"] = et.hour * 60 + et.minute
        r["hour"] = et.hour
        r["dow"] = et.weekday()
        r["is_long"] = r["direction"] in ("long", "bullish")
        r["gap"] = gaps.get(d)
        spot = float(r["spot"]) if r["spot"] is not None else None
        r["spot_f"] = spot
        # distance from the session open, in points and %, known at signal time
        r["from_open"] = (spot - dd["open"]) if (spot and dd.get("open")) else None
        r["from_open_pct"] = (r["from_open"] / dd["open"] * 100) if r["from_open"] is not None else None
        r["from_prev_close"] = (spot - dd["prev_close"]) if (spot and dd.get("prev_close")) else None
        # LIS distance (Volland level) — signed toward the trade direction
        try:
            lis = float(r["lis"]) if r["lis"] is not None else None
            r["lis_dist"] = (spot - lis) if (lis and spot) else None
            r["lis_abs"] = abs(r["lis_dist"]) if r["lis_dist"] is not None else None
        except Exception:
            r["lis_dist"] = r["lis_abs"] = None
        # target distance
        try:
            tg = float(r["target"]) if r["target"] is not None else None
            r["tgt_dist"] = abs(tg - spot) if (tg and spot) else None
        except Exception:
            r["tgt_dist"] = None
        for k in ("vix", "overvix", "vix3m", "greek_alignment", "score", "abs_vol_ratio",
                  "v13_gex_above", "v13_dd_near", "spot_vol_beta", "basket_pct",
                  "vanna_all", "vanna_weekly", "vanna_monthly", "rr_ratio"):
            v = r.get(k)
            r[k] = float(v) if v is not None else None
        r["pts"] = float(r["outcome_pnl"]) if r["outcome_pnl"] is not None else None
        r["mfe"] = float(r["outcome_max_profit"]) if r["outcome_max_profit"] is not None else None
        r["win"] = (r["pts"] > 0) if r["pts"] is not None else None
        out.append(r)
    return out


def wr(rs):
    v = [x for x in rs if x["pts"] is not None]
    return (sum(1 for x in v if x["pts"] > 0) / len(v) * 100) if v else 0.0


def tot(rs):
    return sum(x["pts"] for x in rs if x["pts"] is not None)


def ppt(rs):
    v = [x for x in rs if x["pts"] is not None]
    return (tot(v) / len(v)) if v else 0.0


def summarise(rs, lab, months=None):
    v = [x for x in rs if x["pts"] is not None]
    if not v:
        return f"  {lab:<30} (no resolved trades)"
    s = f"  {lab:<30}{len(v):>5}t  WR {wr(v):>3.0f}%  {tot(v):>+8.1f} pts  {ppt(v):>+5.2f}/t"
    if months:
        mo = collections.defaultdict(float)
        for x in v:
            mo[x["month"]] += x["pts"]
        s += "   " + " ".join(f"{m[-2:]}:{mo.get(m,0):>+6.0f}" for m in months)
        s += f"   {sum(1 for m in months if mo.get(m,0)>0)}/{len(months)}+"
    return s
