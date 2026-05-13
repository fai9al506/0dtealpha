"""
Track C — Discord-Validated High-WR Pattern Hunt
=================================================

Goal: Cross-reference setup_log signals against Volland Discord community calls
      (Apollo, DarkMatter, Wizard, BigBill, LordHelmet, etc.) to find subsets where
      pro-trader commentary aligned with our bot direction. Measure WR uplift.

Data reality (DOCUMENTED HONESTLY):
- Available Discord data is curated daily extracts (per-day AM/PM paradigm + key levels
  + named patterns), NOT raw timestamped chat for the full period.
- Raw timestamped KSA chat exists only for Mar 19-21 (1068 msgs daytrading) +
  Jan-Mar 21 (1907 msgs beginners channel).
- So precise +/- 15 min message-level matching is INFEASIBLE at scale.
- Approach used: DAY-LEVEL Discord context (Apollo paradigm AM/PM, key levels,
  pro bias, named patterns) joined to setup_log entries by trade_date.
- Coverage windows: Mar 23, Mar 27-31, Apr 1-8, May 4-12 (~ 21 trading days).

Output:
- _tmp_track_c_discord_timeline.json (manual day-level extraction, already built)
- _tmp_track_c_discord_validated.csv (signals enriched with discord tags)
- _tmp_track_c_discord_validated.html (dark theme report)
"""
import json
import os
import sys
from datetime import datetime
from collections import defaultdict
from statistics import mean, stdev

import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
TIMELINE_PATH = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_c_discord_timeline.json"
OUT_CSV = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_c_discord_validated.csv"
OUT_HTML = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_c_discord_validated.html"


def load_timeline():
    with open(TIMELINE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_signals():
    e = create_engine(DB_URL, pool_pre_ping=True)
    q = text(
        """
        SELECT
          id,
          ts AT TIME ZONE 'America/New_York' AS ts_et,
          (ts AT TIME ZONE 'America/New_York')::date AS trade_date,
          setup_name,
          direction,
          grade,
          paradigm,
          spot,
          lis,
          target,
          greek_alignment,
          overvix,
          vix,
          vix3m,
          vix_vix3m_ratio,
          outcome_result,
          outcome_pnl,
          outcome_max_profit,
          outcome_max_loss
        FROM setup_log
        WHERE ts >= '2026-03-20'
          AND ts <= '2026-05-13'
          AND outcome_result IS NOT NULL
        ORDER BY ts
        """
    )
    with e.connect() as c:
        df = pd.read_sql(q, c)
    return df


def annotate_signals(df: pd.DataFrame, tl: dict) -> pd.DataFrame:
    dates = tl["dates"]
    df = df.copy()
    # Normalize direction to uppercase for consistent comparisons
    df["direction"] = df["direction"].fillna("").str.upper()

    def get_ctx(d):
        return dates.get(str(d))

    # Discord context columns
    df["d_has_context"] = df["trade_date"].astype(str).map(lambda d: 1 if d in dates else 0)
    df["d_apollo_am"] = df["trade_date"].astype(str).map(lambda d: (dates.get(d) or {}).get("apollo_am_paradigm"))
    df["d_apollo_pm"] = df["trade_date"].astype(str).map(lambda d: (dates.get(d) or {}).get("apollo_pm_paradigm"))
    df["d_pro_bias"] = df["trade_date"].astype(str).map(lambda d: (dates.get(d) or {}).get("pro_bias"))
    df["d_regime_tags"] = df["trade_date"].astype(str).map(
        lambda d: ",".join((dates.get(d) or {}).get("regime_tags", []) or [])
    )
    df["d_named_patterns"] = df["trade_date"].astype(str).map(
        lambda d: ",".join((dates.get(d) or {}).get("named_patterns", []) or [])
    )
    df["d_key_levels"] = df["trade_date"].astype(str).map(
        lambda d: (dates.get(d) or {}).get("key_levels", []) or []
    )

    # Map bias labels to a -1/0/+1 score (negative = bearish, positive = bullish)
    BIAS_SCORE = {
        "bullish": +2,
        "bullish_caution": +1,
        "bullish_choppy": +1,
        "bullish_target_hit": +2,
        "neutral_bullish": +1,
        "neutral": 0,
        "bearish_mixed": -1,
        "bearish": -2,
        "bearish_to_bullish": +1,   # net intra-day bullish (started bearish then flipped)
        "bearish_gap_recovery": +1, # gap-fill recovery is net bullish over the day
        "bearish_fade_rally": -2,   # fade-rally regime is shorts edge
    }

    def align_with_pro(row):
        bias_raw = row["d_pro_bias"]
        if not isinstance(bias_raw, str):
            return "no_bias"
        bias = bias_raw.lower()
        direction = (row["direction"] or "").upper()
        score = BIAS_SCORE.get(bias)
        if score is None or not direction:
            return "no_bias"
        if score == 0:
            return "neutral"
        is_bull = score > 0
        if direction == "LONG":
            return "aligned" if is_bull else "opposed"
        if direction == "SHORT":
            return "aligned" if not is_bull else "opposed"
        return "no_bias"

    df["d_bias_alignment"] = df.apply(align_with_pro, axis=1)
    df["d_bias_score"] = df["d_pro_bias"].apply(lambda b: BIAS_SCORE.get((b or "").lower(), 0))

    # Apollo paradigm match: if apollo's paradigm (AM or PM) shares a token with our paradigm
    def paradigm_match(row):
        ours = (row["paradigm"] or "").upper()
        am = (row["d_apollo_am"] or "").upper()
        pm = (row["d_apollo_pm"] or "").upper()
        tokens = [t for t in [am, pm] if t]
        if not tokens or not ours:
            return 0
        hits = 0
        for t in tokens:
            # token like "GEX", "AG", "BOFA", "DD-POSITIVE", "AG-MESSY"
            t_root = t.split("-")[0]
            if t_root and t_root in ours:
                hits = 1
                break
        return hits

    df["d_apollo_paradigm_match"] = df.apply(paradigm_match, axis=1)

    # Level proximity: signal spot within +/- 8 pts of any discord level
    def level_proximity(row):
        levels = row["d_key_levels"] or []
        if not levels or not row["spot"]:
            return 0
        s = float(row["spot"])
        for lvl in levels:
            try:
                if abs(s - float(lvl)) <= 8:
                    return 1
            except Exception:
                continue
        return 0

    df["d_near_discord_level"] = df.apply(level_proximity, axis=1)

    # Composite alignment score: bias + paradigm + level proximity
    def composite_align(row):
        if row["d_has_context"] == 0:
            return None
        score = 0
        if row["d_bias_alignment"] == "aligned":
            score += 1
        if row["d_apollo_paradigm_match"] == 1:
            score += 1
        if row["d_near_discord_level"] == 1:
            score += 1
        return score

    df["d_align_score"] = df.apply(composite_align, axis=1)
    df["d_align_strong"] = df["d_align_score"].apply(lambda x: 1 if (x is not None and x >= 2) else 0)
    df["d_aligned"] = df["d_align_score"].apply(lambda x: 1 if (x is not None and x >= 1) else 0)

    # Named pattern tags split into per-pattern boolean cols (only for context days)
    all_patterns = set()
    for d, ctx in dates.items():
        for p in (ctx.get("named_patterns") or []):
            all_patterns.add(p)

    for p in all_patterns:
        df[f"d_pat_{p}"] = df["d_named_patterns"].apply(lambda s: 1 if p in (s or "") else 0)

    # Regime-tag bools
    all_regimes = set()
    for d, ctx in dates.items():
        for r in (ctx.get("regime_tags") or []):
            all_regimes.add(r)
    for r in all_regimes:
        df[f"d_reg_{r}"] = df["d_regime_tags"].apply(lambda s: 1 if r in (s or "") else 0)

    return df


def wr_stats(df_subset: pd.DataFrame):
    n = len(df_subset)
    if n == 0:
        return dict(n=0, wins=0, losses=0, wr=None, pnl=0.0, avg=None, expired=0, maxdd=None)
    wins = int((df_subset["outcome_result"] == "WIN").sum())
    losses = int((df_subset["outcome_result"] == "LOSS").sum())
    expired = int((df_subset["outcome_result"] == "EXPIRED").sum())
    decided = wins + losses
    wr = (wins / decided * 100.0) if decided else None
    pnl = float(df_subset["outcome_pnl"].fillna(0).sum())
    avg = pnl / n if n else None
    # Max drawdown approximation
    eq = df_subset["outcome_pnl"].fillna(0).cumsum()
    if len(eq):
        running_max = eq.cummax()
        dd = (eq - running_max).min()
    else:
        dd = 0
    return dict(n=n, wins=wins, losses=losses, expired=expired, wr=wr, pnl=pnl, avg=avg, maxdd=float(dd))


def bootstrap_wr_diff(df_in: pd.DataFrame, df_out: pd.DataFrame, n_iter=2000):
    """Bootstrap on WR difference, return 95% CI."""
    import random
    random.seed(42)
    if len(df_in) < 5 or len(df_out) < 5:
        return None
    pnl_in = df_in["outcome_pnl"].fillna(0).tolist()
    pnl_out = df_out["outcome_pnl"].fillna(0).tolist()
    diffs = []
    for _ in range(n_iter):
        si = [random.choice(pnl_in) for _ in range(len(pnl_in))]
        so = [random.choice(pnl_out) for _ in range(len(pnl_out))]
        diffs.append((sum(si) / len(si)) - (sum(so) / len(so)))
    diffs.sort()
    lo = diffs[int(0.025 * len(diffs))]
    hi = diffs[int(0.975 * len(diffs))]
    return (lo, hi, mean(diffs))


def main():
    print("[track-c] loading timeline...")
    tl = load_timeline()
    print(f"[track-c] timeline dates: {len(tl['dates'])}")

    print("[track-c] fetching signals from DB...")
    df = fetch_signals()
    print(f"[track-c] signals: {len(df)}")

    df = annotate_signals(df, tl)
    in_ctx = df[df["d_has_context"] == 1]
    print(f"[track-c] signals on context days: {len(in_ctx)}")
    print(f"[track-c] aligned >=1: {int(in_ctx['d_aligned'].sum())}")
    print(f"[track-c] aligned strong >=2: {int(in_ctx['d_align_strong'].sum())}")

    df.to_csv(OUT_CSV, index=False)
    print(f"[track-c] wrote {OUT_CSV}")

    # === Aggregate analyses ===
    sections = []

    # 1. Overall: context days vs non-context days
    in_decided = in_ctx[in_ctx["outcome_result"].isin(["WIN", "LOSS"])]
    not_in_ctx = df[df["d_has_context"] == 0]
    not_in_decided = not_in_ctx[not_in_ctx["outcome_result"].isin(["WIN", "LOSS"])]

    sections.append({
        "title": "Overall: Discord-Covered Days vs Other Days",
        "rows": [
            ("Covered (context days)", wr_stats(in_ctx)),
            ("Not covered", wr_stats(not_in_ctx)),
        ],
    })

    # 2. Within covered days: aligned vs not
    aligned = in_ctx[in_ctx["d_aligned"] == 1]
    not_aligned = in_ctx[in_ctx["d_aligned"] == 0]
    sections.append({
        "title": "Within Covered Days: Aligned vs Not Aligned",
        "rows": [
            ("Aligned >=1 (any of bias/paradigm/level)", wr_stats(aligned)),
            ("Not aligned", wr_stats(not_aligned)),
        ],
    })

    # 3. Strong alignment (>=2 of 3 indicators)
    strong = in_ctx[in_ctx["d_align_strong"] == 1]
    weak = in_ctx[in_ctx["d_align_strong"] == 0]
    sections.append({
        "title": "Within Covered Days: Strong Alignment (>=2 indicators)",
        "rows": [
            ("Strong aligned (>=2)", wr_stats(strong)),
            ("Weak/none", wr_stats(weak)),
        ],
    })

    # 4. Each indicator individually
    rows4 = [
        ("Bias-aligned", wr_stats(in_ctx[in_ctx["d_bias_alignment"] == "aligned"])),
        ("Bias-opposed", wr_stats(in_ctx[in_ctx["d_bias_alignment"] == "opposed"])),
        ("Apollo-paradigm-match", wr_stats(in_ctx[in_ctx["d_apollo_paradigm_match"] == 1])),
        ("Apollo-paradigm-NO-match", wr_stats(in_ctx[in_ctx["d_apollo_paradigm_match"] == 0])),
        ("Near discord level (+/- 8 pts)", wr_stats(in_ctx[in_ctx["d_near_discord_level"] == 1])),
        ("NOT near discord level", wr_stats(in_ctx[in_ctx["d_near_discord_level"] == 0])),
    ]
    sections.append({"title": "Indicator Breakdown", "rows": rows4})

    # 5. Per-setup breakdown of strong alignment
    setup_rows = []
    for setup in df["setup_name"].unique():
        sub = in_ctx[in_ctx["setup_name"] == setup]
        if len(sub) == 0:
            continue
        s_aln = sub[sub["d_align_strong"] == 1]
        s_oth = sub[sub["d_align_strong"] == 0]
        if len(s_aln) >= 5:
            setup_rows.append((f"{setup} | aligned strong", wr_stats(s_aln)))
        if len(s_oth) >= 5:
            setup_rows.append((f"{setup} | rest", wr_stats(s_oth)))
    sections.append({"title": "Per-Setup: Strong Alignment Filter (n>=5)", "rows": setup_rows})

    # 6. Per-direction (LONG vs SHORT) under bias-alignment
    sections.append({
        "title": "Direction x Bias Alignment",
        "rows": [
            ("LONG x bias-aligned", wr_stats(in_ctx[(in_ctx["direction"] == "LONG") & (in_ctx["d_bias_alignment"] == "aligned")])),
            ("LONG x bias-opposed", wr_stats(in_ctx[(in_ctx["direction"] == "LONG") & (in_ctx["d_bias_alignment"] == "opposed")])),
            ("SHORT x bias-aligned", wr_stats(in_ctx[(in_ctx["direction"] == "SHORT") & (in_ctx["d_bias_alignment"] == "aligned")])),
            ("SHORT x bias-opposed", wr_stats(in_ctx[(in_ctx["direction"] == "SHORT") & (in_ctx["d_bias_alignment"] == "opposed")])),
        ],
    })

    # 7. Named patterns: hunt high-WR combos
    pattern_cols = [c for c in df.columns if c.startswith("d_pat_")]
    pattern_rows = []
    for pc in pattern_cols:
        sub = in_ctx[in_ctx[pc] == 1]
        if len(sub) >= 5:
            pattern_rows.append((pc.replace("d_pat_", ""), wr_stats(sub)))
    pattern_rows.sort(key=lambda r: -(r[1]["wr"] or 0))
    sections.append({"title": "Named Patterns (n>=5)", "rows": pattern_rows})

    # 8. Pattern x direction combos
    combo_rows = []
    for pc in pattern_cols:
        for dirn in ["LONG", "SHORT"]:
            sub = in_ctx[(in_ctx[pc] == 1) & (in_ctx["direction"] == dirn)]
            if len(sub) >= 5:
                combo_rows.append((f"{pc.replace('d_pat_','')} x {dirn}", wr_stats(sub)))
    combo_rows.sort(key=lambda r: -(r[1]["wr"] or 0))
    sections.append({"title": "Pattern x Direction (n>=5)", "rows": combo_rows})

    # 9. Regime-tag breakdown
    regime_cols = [c for c in df.columns if c.startswith("d_reg_")]
    regime_rows = []
    for rc in regime_cols:
        sub = in_ctx[in_ctx[rc] == 1]
        if len(sub) >= 5:
            regime_rows.append((rc.replace("d_reg_", ""), wr_stats(sub)))
    regime_rows.sort(key=lambda r: -(r[1]["wr"] or 0))
    sections.append({"title": "Regime Tags (n>=5)", "rows": regime_rows})

    # 10. High-WR candidates with bootstrap (n>=20)
    candidates = []

    # Candidate 1: setup + strong alignment combos
    for setup in df["setup_name"].unique():
        for dirn in ["LONG", "SHORT"]:
            sub = in_ctx[
                (in_ctx["setup_name"] == setup)
                & (in_ctx["direction"] == dirn)
                & (in_ctx["d_align_strong"] == 1)
            ]
            if len(sub) >= 5:
                stats = wr_stats(sub)
                base = in_ctx[(in_ctx["setup_name"] == setup) & (in_ctx["direction"] == dirn)]
                base_stats = wr_stats(base)
                candidates.append({
                    "name": f"{setup} {dirn} + strong-aligned",
                    "n": stats["n"],
                    "wr": stats["wr"],
                    "pnl": stats["pnl"],
                    "avg": stats["avg"],
                    "maxdd": stats["maxdd"],
                    "base_n": base_stats["n"],
                    "base_wr": base_stats["wr"],
                    "base_pnl": base_stats["pnl"],
                })

    # Candidate 2: pattern x setup combos
    for pc in pattern_cols:
        for setup in df["setup_name"].unique():
            for dirn in ["LONG", "SHORT"]:
                sub = in_ctx[
                    (in_ctx[pc] == 1)
                    & (in_ctx["setup_name"] == setup)
                    & (in_ctx["direction"] == dirn)
                ]
                if len(sub) >= 5:
                    stats = wr_stats(sub)
                    candidates.append({
                        "name": f"{setup} {dirn} + {pc.replace('d_pat_','')}",
                        "n": stats["n"],
                        "wr": stats["wr"],
                        "pnl": stats["pnl"],
                        "avg": stats["avg"],
                        "maxdd": stats["maxdd"],
                        "base_n": None,
                        "base_wr": None,
                        "base_pnl": None,
                    })

    # Candidate 3: regime x setup x direction
    for rc in regime_cols:
        for setup in df["setup_name"].unique():
            for dirn in ["LONG", "SHORT"]:
                sub = in_ctx[
                    (in_ctx[rc] == 1)
                    & (in_ctx["setup_name"] == setup)
                    & (in_ctx["direction"] == dirn)
                ]
                if len(sub) >= 5:
                    stats = wr_stats(sub)
                    candidates.append({
                        "name": f"{setup} {dirn} + regime:{rc.replace('d_reg_','')}",
                        "n": stats["n"],
                        "wr": stats["wr"],
                        "pnl": stats["pnl"],
                        "avg": stats["avg"],
                        "maxdd": stats["maxdd"],
                        "base_n": None,
                        "base_wr": None,
                        "base_pnl": None,
                    })

    # Sort by WR desc then n
    candidates_filtered = [c for c in candidates if c["wr"] is not None and c["n"] >= 5]
    candidates_filtered.sort(key=lambda c: (-(c["wr"] or 0), -c["n"]))
    top5 = candidates_filtered[:10]

    sections.append({
        "title": "Top 10 Candidate Filters (n>=5, by WR desc) - DRAFT",
        "rows": [(c["name"], {"n": c["n"], "wr": c["wr"], "pnl": c["pnl"], "avg": c["avg"],
                              "maxdd": c["maxdd"], "wins": None, "losses": None, "expired": None})
                 for c in top5],
    })

    # 11. Per-date summary
    date_rows = []
    for d, ctx in tl["dates"].items():
        sub = df[df["trade_date"].astype(str) == d]
        if len(sub) == 0:
            continue
        s_stats = wr_stats(sub)
        date_rows.append({
            "date": d,
            "bias": ctx.get("pro_bias"),
            "paradigm_am": ctx.get("apollo_am_paradigm"),
            "paradigm_pm": ctx.get("apollo_pm_paradigm"),
            "patterns": ",".join(ctx.get("named_patterns") or []),
            "n": s_stats["n"],
            "wr": s_stats["wr"],
            "pnl": s_stats["pnl"],
        })

    # Render HTML
    html = render_html(sections, date_rows, df, in_ctx, top5, tl)
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[track-c] wrote {OUT_HTML}")

    return sections, top5, date_rows


def fmt_pct(v):
    if v is None:
        return "n/a"
    return f"{v:.0f}%"


def fmt_pts(v):
    if v is None:
        return "n/a"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.1f}"


def render_html(sections, date_rows, df, in_ctx, top5, tl):
    n_total = len(df)
    n_ctx = len(in_ctx)
    n_aligned = int(in_ctx["d_aligned"].sum())
    n_strong = int(in_ctx["d_align_strong"].sum())

    def render_row(name, st):
        wr = fmt_pct(st.get("wr"))
        pnl = fmt_pts(st.get("pnl"))
        avg = fmt_pts(st.get("avg"))
        dd = fmt_pts(st.get("maxdd"))
        n = st.get("n", 0)
        wins = st.get("wins") if st.get("wins") is not None else "-"
        losses = st.get("losses") if st.get("losses") is not None else "-"
        return f"""
        <tr>
          <td>{name}</td>
          <td>{n}</td>
          <td>{wins}</td>
          <td>{losses}</td>
          <td>{wr}</td>
          <td>{pnl}</td>
          <td>{avg}</td>
          <td>{dd}</td>
        </tr>"""

    section_html = ""
    for sec in sections:
        rows = "".join(render_row(name, st) for name, st in sec["rows"]) or "<tr><td colspan='8'>(no rows met thresholds)</td></tr>"
        section_html += f"""
        <h2>{sec['title']}</h2>
        <table>
          <thead><tr><th>Subset</th><th>N</th><th>W</th><th>L</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
        """

    # Per-date table
    date_html_rows = "".join(
        f"<tr><td>{r['date']}</td><td>{r.get('bias') or ''}</td>"
        f"<td>{r.get('paradigm_am') or ''} / {r.get('paradigm_pm') or ''}</td>"
        f"<td>{r.get('patterns') or ''}</td>"
        f"<td>{r['n']}</td><td>{fmt_pct(r['wr'])}</td><td>{fmt_pts(r['pnl'])}</td></tr>"
        for r in date_rows
    )

    top5_html = "".join(
        f"<tr><td>{c['name']}</td><td>{c['n']}</td><td>{fmt_pct(c['wr'])}</td>"
        f"<td>{fmt_pts(c['pnl'])}</td><td>{fmt_pts(c['avg'])}</td><td>{fmt_pts(c['maxdd'])}</td></tr>"
        for c in top5
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Track C — Discord-Validated Pattern Hunt</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');
body {{ font-family: 'Inter', sans-serif; background:#0a0e17; color:#c8d6e5; padding:2rem; max-width:1200px; margin:0 auto; line-height:1.6; }}
h1 {{ color:#f5f6fa; font-weight:700; border-bottom:1px solid #1a2a3a; padding-bottom:0.5rem; }}
h2 {{ color:#00d2ff; font-size:1.2rem; margin-top:2rem; border-bottom:1px solid #1a2a3a; padding-bottom:0.3rem; }}
.kpi-grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(180px,1fr)); gap:1rem; margin:1.5rem 0; }}
.kpi {{ background:#11182a; border:1px solid #1a2a3a; border-radius:10px; padding:1rem; }}
.kpi .label {{ color:#778ca3; font-size:0.8rem; text-transform:uppercase; letter-spacing:0.5px; }}
.kpi .value {{ color:#f5f6fa; font-size:1.6rem; font-weight:700; font-family:'JetBrains Mono', monospace; }}
.kpi .note {{ color:#778ca3; font-size:0.8rem; }}
table {{ width:100%; border-collapse:collapse; margin:1rem 0; font-size:0.92rem; }}
th, td {{ padding:0.5rem 0.7rem; border-bottom:1px solid #1a2a3a; text-align:left; }}
th {{ color:#ffd32a; font-weight:600; background:#11182a; }}
td:nth-child(n+2) {{ font-family:'JetBrains Mono', monospace; }}
.callout {{ background:#11182a; border-left:4px solid #ffd32a; padding:1rem 1.2rem; margin:1rem 0; border-radius:0 6px 6px 0; }}
.callout.warn {{ border-left-color:#ff5e57; }}
.callout.good {{ border-left-color:#0be881; }}
.footer {{ color:#5a6c80; font-size:0.8rem; margin-top:3rem; padding-top:1rem; border-top:1px solid #1a2a3a; }}
code {{ background:#11182a; padding:1px 5px; border-radius:3px; color:#ffd32a; font-family:'JetBrains Mono', monospace; }}
</style>
</head><body>
<h1>Track C — Discord-Validated High-WR Pattern Hunt</h1>
<div class="callout warn">
  <strong>Data caveat (read first):</strong>
  Discord references available are <strong>curated daily extracts</strong> (per-day AM/PM paradigm,
  key levels, named patterns), <strong>NOT raw timestamped chat for the full setup_log period</strong>.
  Raw message-level timestamps exist only for Mar 19-21 (1068 msgs daytrading) + Jan-Mar beginners channel.
  Therefore matching is done at <strong>day-level granularity</strong>, not the ±15 min window
  originally proposed. Coverage: Mar 23, Mar 27-31, Apr 1-8, May 4-12 (~21 trading days, {n_ctx} signals).
  All findings flagged as <em>directional / hypothesis-generating</em> per analysis validation protocol.
</div>

<div class="kpi-grid">
  <div class="kpi"><div class="label">Signals total</div><div class="value">{n_total}</div><div class="note">Mar 20 - May 13 with outcome</div></div>
  <div class="kpi"><div class="label">On covered days</div><div class="value">{n_ctx}</div><div class="note">Discord context available</div></div>
  <div class="kpi"><div class="label">Discord-aligned ≥1</div><div class="value">{n_aligned}</div><div class="note">bias OR paradigm OR level</div></div>
  <div class="kpi"><div class="label">Strong aligned ≥2</div><div class="value">{n_strong}</div><div class="note">2+ indicators</div></div>
</div>

<h2>Per-Date Summary (Covered Window)</h2>
<table>
<thead><tr><th>Date</th><th>Pro Bias</th><th>Apollo AM/PM</th><th>Named Patterns</th><th>N</th><th>WR</th><th>PnL</th></tr></thead>
<tbody>{date_html_rows}</tbody>
</table>

{section_html}

<h2>Top 10 Discord-Aligned Filter Candidates (n>=5, sorted by WR)</h2>
<table>
<thead><tr><th>Subset</th><th>N</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th></tr></thead>
<tbody>{top5_html}</tbody>
</table>

<div class="callout">
  <strong>Confidence guide:</strong>
  n &lt; 20 = directional signal only. n 20-50 = moderate. n 50+ = high confidence.
  <strong>All Discord-derived candidates here are by definition tiny samples</strong> (covered window
  is only 21 trading days, ~500 signals). Any candidate is hypothesis-generating, not ship-ready.
</div>

<div class="footer">
  Generated 2026-05-13 · TRACK C · Discord coverage: 21 trading days · Approach: day-level annotation
  (raw chat timestamps unavailable for full period) · Per <code>feedback_analysis_validation.md</code>
</div>

</body></html>"""


if __name__ == "__main__":
    main()
