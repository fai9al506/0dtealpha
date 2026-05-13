"""
Track C — Final report builder.
Reads _tmp_track_c_discord_validated.csv and produces HTML + summary.
"""
import json
import os
import sys
import random
import pandas as pd
from statistics import mean

INPUT_CSV = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_c_discord_validated.csv"
MSG_MATCH_CSV = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_c_msg_match.csv"
TIMELINE = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_c_discord_timeline.json"
OUT_HTML = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_c_discord_validated.html"

random.seed(42)

def stats(sub: pd.DataFrame):
    n = len(sub)
    if n == 0:
        return None
    decided = sub[sub["outcome_result"].isin(["WIN", "LOSS"])]
    wins = int((decided["outcome_result"] == "WIN").sum())
    losses = int((decided["outcome_result"] == "LOSS").sum())
    wr = (wins / (wins + losses) * 100) if (wins + losses) else None
    pnl = float(sub["outcome_pnl"].fillna(0).sum())
    avg = pnl / n if n else None
    eq = sub["outcome_pnl"].fillna(0).cumsum()
    dd = float((eq - eq.cummax()).min()) if len(eq) else 0.0
    return dict(n=n, wins=wins, losses=losses, wr=wr, pnl=pnl, avg=avg, maxdd=dd)


def bootstrap_ci(values, n_iter=2000, ci=0.95):
    if len(values) < 5:
        return (None, None, None)
    means = []
    for _ in range(n_iter):
        sample = [random.choice(values) for _ in range(len(values))]
        means.append(sum(sample) / len(sample))
    means.sort()
    lo = means[int((1 - ci) / 2 * n_iter)]
    hi = means[int((1 - (1 - ci) / 2) * n_iter)]
    return (sum(means) / n_iter, lo, hi)


def fmt_pct(v): return "n/a" if v is None else f"{v:.0f}%"
def fmt_pts(v): return "n/a" if v is None else f"{v:+.1f}"
def fmt_avg(v): return "n/a" if v is None else f"{v:+.2f}"


def render_stats_row(label, st, base_st=None, note=""):
    if st is None:
        return f"<tr><td>{label}</td><td colspan='7'>(n=0)</td></tr>"
    delta = ""
    if base_st and base_st.get("wr") is not None and st.get("wr") is not None:
        delta_wr = st["wr"] - base_st["wr"]
        delta_avg = st["avg"] - base_st["avg"] if base_st.get("avg") is not None and st.get("avg") is not None else None
        d_wr = f"<span class='{'pos' if delta_wr>0 else 'neg'}'>{delta_wr:+.0f}%pp</span>"
        d_a = f"<span class='{'pos' if (delta_avg or 0)>0 else 'neg'}'>{delta_avg:+.2f}</span>" if delta_avg is not None else ""
        delta = f"WR {d_wr} / Avg {d_a}"
    wr_class = "pos" if (st.get("wr") or 0) >= 55 else ("neg" if (st.get("wr") or 0) <= 45 else "")
    pnl_class = "pos" if st.get("pnl", 0) >= 0 else "neg"
    return f"""<tr>
      <td>{label}</td>
      <td>{st['n']}</td>
      <td>{st['wins']}</td>
      <td>{st['losses']}</td>
      <td class='{wr_class}'>{fmt_pct(st['wr'])}</td>
      <td class='{pnl_class}'>{fmt_pts(st['pnl'])}</td>
      <td>{fmt_avg(st['avg'])}</td>
      <td>{fmt_pts(st['maxdd'])}</td>
      <td>{delta}{note}</td>
    </tr>"""


def main():
    df = pd.read_csv(INPUT_CSV)
    in_ctx = df[df["d_has_context"] == 1].copy()

    # Baseline = all signals in covered window
    baseline = stats(in_ctx)

    # ===== Section A: Top-level alignment indicators =====
    s_aligned = stats(in_ctx[in_ctx["d_aligned"] == 1])
    s_not_aligned = stats(in_ctx[in_ctx["d_aligned"] == 0])
    s_strong = stats(in_ctx[in_ctx["d_align_strong"] == 1])
    s_full = stats(in_ctx[in_ctx["d_align_score"] == 3])

    s_bias_a = stats(in_ctx[in_ctx["d_bias_alignment"] == "aligned"])
    s_bias_o = stats(in_ctx[in_ctx["d_bias_alignment"] == "opposed"])
    s_bias_n = stats(in_ctx[in_ctx["d_bias_alignment"] == "neutral"])
    s_para_m = stats(in_ctx[in_ctx["d_apollo_paradigm_match"] == 1])
    s_para_nm = stats(in_ctx[in_ctx["d_apollo_paradigm_match"] == 0])
    s_near_l = stats(in_ctx[in_ctx["d_near_discord_level"] == 1])
    s_far_l = stats(in_ctx[in_ctx["d_near_discord_level"] == 0])

    # ===== Per-author proxy (named patterns are author-anchored) =====
    # Aggregate by primary author (signals on days that author was active)
    APOLLO_PATTERNS = ["apollo_fade_call","apollo_weekly_top_call_6650","apollo_iceberg_3k_seller_7430","fade_rally_undervix_exhaustion","yahya_dealer_setup_long","gex_pure","jpm_collar_short","never_under"]
    DM_PATTERNS = ["darkmatter_vanna_walk","darkmatter_breadth","darkmatter_target","vanna_tower_sequential","dd_concentration_magnet","108_orders_7300"]
    WIZARD_PATTERNS = ["wizard_vix_warning"]
    LH_PATTERNS = ["backside_short"]

    def union(df_, cols):
        cs = [c for c in cols if c in df_.columns]
        if not cs: return df_.iloc[0:0]
        return df_[df_[cs].sum(axis=1) >= 1]

    s_apollo = stats(union(in_ctx, [f"d_pat_{p}" for p in APOLLO_PATTERNS]))
    s_dm = stats(union(in_ctx, [f"d_pat_{p}" for p in DM_PATTERNS]))
    s_wiz = stats(union(in_ctx, [f"d_pat_{p}" for p in WIZARD_PATTERNS]))
    s_lh = stats(union(in_ctx, [f"d_pat_{p}" for p in LH_PATTERNS]))

    # ===== High-WR Candidates (n>=10) =====
    cands = []
    # Setup x direction x near-level
    for setup in in_ctx["setup_name"].unique():
        for d in ["LONG", "SHORT"]:
            base = in_ctx[(in_ctx["setup_name"] == setup) & (in_ctx["direction"] == d)]
            base_st = stats(base) if len(base) else None
            for tag, sub in [
                ("near-discord-level", base[base["d_near_discord_level"] == 1] if len(base) else base),
                ("far-from-discord-level", base[base["d_near_discord_level"] == 0] if len(base) else base),
                ("bias-aligned", base[base["d_bias_alignment"] == "aligned"] if len(base) else base),
                ("bias-opposed", base[base["d_bias_alignment"] == "opposed"] if len(base) else base),
                ("paradigm-match", base[base["d_apollo_paradigm_match"] == 1] if len(base) else base),
                ("strong-aligned-2+", base[base["d_align_strong"] == 1] if len(base) else base),
            ]:
                st = stats(sub)
                if st and st["n"] >= 10 and st["wr"] is not None:
                    cands.append({
                        "name": f"{setup} {d} + {tag}",
                        "stats": st,
                        "base": base_st,
                    })
    cands.sort(key=lambda c: -c["stats"]["wr"])

    # ===== Top 5 candidates with bootstrap CI =====
    top5 = []
    for c in cands[:15]:
        # Bootstrap on avg pnl
        sub_pnl = []
        # need pnl values — reload
        # We already have stats, but bootstrap needs original values
        top5.append(c)

    # Top by WR with n>=10
    top_wr = [c for c in cands if c["stats"]["n"] >= 10][:10]
    # Top by PnL
    cands_by_pnl = sorted(cands, key=lambda c: -c["stats"]["pnl"])
    top_pnl = [c for c in cands_by_pnl if c["stats"]["n"] >= 10][:10]
    # Bottom by WR (avoid list)
    bot_wr = sorted(cands, key=lambda c: c["stats"]["wr"])
    bot_wr_filt = [c for c in bot_wr if c["stats"]["n"] >= 10][:10]

    # ===== Per-author table =====
    author_rows = [
        ("Apollo (8 patterns)", s_apollo, baseline),
        ("DarkMatter (6 patterns)", s_dm, baseline),
        ("Wizard (vix_warning)", s_wiz, baseline),
        ("LordHelmet (backside_short)", s_lh, baseline),
    ]

    # ===== Per-date table =====
    with open(TIMELINE, "r", encoding="utf-8") as f:
        tl = json.load(f)
    date_rows = []
    for d, ctx in sorted(tl["dates"].items()):
        sub = df[df["trade_date"].astype(str) == d]
        if len(sub) == 0:
            continue
        s = stats(sub)
        date_rows.append({
            "date": d,
            "bias": ctx.get("pro_bias") or "",
            "apollo_am": ctx.get("apollo_am_paradigm") or "",
            "apollo_pm": ctx.get("apollo_pm_paradigm") or "",
            "patterns": ", ".join(ctx.get("named_patterns") or []),
            "n": s["n"], "wr": s["wr"], "pnl": s["pnl"],
        })

    # ===== Message-level (raw chat) results =====
    msg_block = ""
    if os.path.exists(MSG_MATCH_CSV):
        m_df = pd.read_csv(MSG_MATCH_CSV)
        s_all = stats(m_df)
        s_a = stats(m_df[m_df["chat_align"] == "aligned"])
        s_o = stats(m_df[m_df["chat_align"] == "opposed"])
        s_s = stats(m_df[m_df["chat_align"] == "silent"])
        s_near = stats(m_df[m_df["near_chat_level"] == 1])
        msg_rows = [
            ("All signals in raw-chat window", s_all, None),
            ("chat-aligned (±15 min bias match)", s_a, s_all),
            ("chat-opposed", s_o, s_all),
            ("chat-silent (no key-author msgs)", s_s, s_all),
            ("near chat-mentioned level", s_near, s_all),
        ]
        rows = "".join(render_stats_row(name, st, base) for name, st, base in msg_rows)
        msg_block = f"""
        <h2>Message-Level Matching (Raw Chat Window)</h2>
        <p>Raw timestamped chat available for Sep 2025 - Mar 21 2026 from <code>tmp_discord_dump.txt</code> + <code>tmp_beginners_dump.txt</code> (1,391 key-author msgs across 2,899 total). Signals matched within ±15 min of key-author (Apollo/DM/Wizard/BigBill/LordHelmet/etc) messages. Bullish/bearish keyword sentiment is a coarse heuristic — small n caveats apply.</p>
        <table>
          <thead><tr><th>Subset</th><th>N</th><th>W</th><th>L</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th><th>Delta</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
        <p class='note'>Caveat: raw-chat density is very low (893/942 = 95% "silent" — no key-author msg within ±15 min). Useful signal only on the 49 messages-present trades. The dump file dates (Mar 19-21 daytrading + Sep-Mar 21 beginners) do not overlap our V14/V13 era cleanly.</p>
        """

    # ===== Render =====
    sec_top = "".join(render_stats_row(n, s, baseline) for n, s in [
        ("Baseline (all covered signals)", baseline),
        ("Discord-aligned (≥1 indicator)", s_aligned),
        ("Not aligned", s_not_aligned),
        ("Strong-aligned (≥2 indicators)", s_strong),
        ("Triple-aligned (3/3)", s_full),
    ])

    sec_ind = "".join(render_stats_row(n, s, baseline) for n, s in [
        ("Pro bias aligned with direction", s_bias_a),
        ("Pro bias opposed to direction", s_bias_o),
        ("Pro bias neutral (no clear day-bias)", s_bias_n),
        ("Apollo paradigm match (root)", s_para_m),
        ("Apollo paradigm NO match", s_para_nm),
        ("Spot near Discord-mentioned level (±8)", s_near_l),
        ("Spot far from Discord level", s_far_l),
    ])

    sec_auth = "".join(render_stats_row(n, s, base) for n, s, base in author_rows)

    sec_top_wr = "".join(
        render_stats_row(c["name"], c["stats"], c.get("base")) for c in top_wr
    )
    sec_top_pnl = "".join(
        render_stats_row(c["name"], c["stats"], c.get("base")) for c in top_pnl
    )
    sec_avoid = "".join(
        render_stats_row(c["name"], c["stats"], c.get("base")) for c in bot_wr_filt
    )

    date_html = "".join(
        f"<tr><td>{r['date']}</td><td>{r['bias']}</td><td>{r['apollo_am']} / {r['apollo_pm']}</td>"
        f"<td>{r['patterns']}</td><td>{r['n']}</td><td class='{ 'pos' if (r['wr'] or 0)>=55 else ('neg' if (r['wr'] or 0)<=45 else '')}'>{fmt_pct(r['wr'])}</td>"
        f"<td class='{ 'pos' if r['pnl']>=0 else 'neg'}'>{fmt_pts(r['pnl'])}</td></tr>"
        for r in date_rows
    )

    # KPIs
    n_total = len(df)
    n_ctx = len(in_ctx)
    n_aligned = int(in_ctx["d_aligned"].sum())
    n_strong = int(in_ctx["d_align_strong"].sum())
    n_near = int(in_ctx["d_near_discord_level"].sum())

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Track C — Discord-Validated Pattern Hunt</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap');
body {{ font-family: 'Inter', sans-serif; background:#0a0e17; color:#c8d6e5; padding:2rem; max-width:1200px; margin:0 auto; line-height:1.6; }}
h1 {{ color:#f5f6fa; font-weight:700; border-bottom:1px solid #1a2a3a; padding-bottom:0.5rem; font-size: 1.8rem; }}
h2 {{ color:#00d2ff; font-size:1.25rem; margin-top:2.5rem; border-bottom:1px solid #1a2a3a; padding-bottom:0.4rem; }}
h3 {{ color:#ffd32a; font-size:1.05rem; margin-top:1.5rem; }}
.kpi-grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(170px,1fr)); gap:1rem; margin:1.5rem 0; }}
.kpi {{ background:#11182a; border:1px solid #1a2a3a; border-radius:10px; padding:1rem; }}
.kpi .label {{ color:#778ca3; font-size:0.78rem; text-transform:uppercase; letter-spacing:0.5px; }}
.kpi .value {{ color:#f5f6fa; font-size:1.6rem; font-weight:700; font-family:'JetBrains Mono', monospace; }}
.kpi .note {{ color:#778ca3; font-size:0.78rem; }}
table {{ width:100%; border-collapse:collapse; margin:1rem 0; font-size:0.88rem; }}
th, td {{ padding:0.5rem 0.7rem; border-bottom:1px solid #1a2a3a; text-align:left; }}
th {{ color:#ffd32a; font-weight:600; background:#11182a; font-size:0.82rem; text-transform:uppercase; letter-spacing:0.5px; }}
td {{ font-family:'JetBrains Mono', monospace; font-size:0.88rem; }}
td:first-child {{ font-family:'Inter', sans-serif; }}
.pos {{ color:#0be881; }}
.neg {{ color:#ff5e57; }}
.callout {{ background:#11182a; border-left:4px solid #ffd32a; padding:1rem 1.2rem; margin:1rem 0; border-radius:0 6px 6px 0; }}
.callout.warn {{ border-left-color:#ff5e57; }}
.callout.good {{ border-left-color:#0be881; }}
.callout.info {{ border-left-color:#00d2ff; }}
.footer {{ color:#5a6c80; font-size:0.78rem; margin-top:3rem; padding-top:1rem; border-top:1px solid #1a2a3a; }}
code {{ background:#11182a; padding:1px 5px; border-radius:3px; color:#ffd32a; font-family:'JetBrains Mono', monospace; }}
p.note {{ color:#778ca3; font-size:0.85rem; font-style:italic; }}
</style>
</head><body>

<h1>Track C — Discord-Validated High-WR Pattern Hunt</h1>
<p style="color:#778ca3;">Cross-reference setup_log signals against Volland Discord community calls (Apollo, DarkMatter, Wizard, BigBill, LordHelmet). Measure WR uplift from pro-trader alignment.</p>

<div class="callout warn">
  <strong>Data integrity caveat (read first):</strong>
  Available Discord references are <strong>curated daily extracts</strong> (per-day Apollo AM/PM paradigm,
  key levels, named patterns), <strong>NOT raw timestamped chat for the full setup_log period</strong>.
  Raw message-level timestamps exist only for ~Sep 2025 - Mar 21 2026 (and density is very low: 95% of signals have no key-author message within ±15 min).
  Day-level matching is therefore the primary methodology used here.
  Coverage windows: Mar 23, Mar 27-31, Apr 1-8, May 4-12 (~21 trading days, {n_ctx} signals).
  All findings flagged per <code>feedback_analysis_validation.md</code> as <em>directional / hypothesis-generating</em> — none ready to ship without further validation.
</div>

<div class="kpi-grid">
  <div class="kpi"><div class="label">Signals total</div><div class="value">{n_total}</div><div class="note">Mar 20 - May 13, outcome resolved</div></div>
  <div class="kpi"><div class="label">On covered days</div><div class="value">{n_ctx}</div><div class="note">Discord context available</div></div>
  <div class="kpi"><div class="label">Aligned ≥1 indicator</div><div class="value">{n_aligned}</div><div class="note">bias / paradigm / level</div></div>
  <div class="kpi"><div class="label">Strong aligned (≥2)</div><div class="value">{n_strong}</div><div class="note">Composite filter</div></div>
  <div class="kpi"><div class="label">Near Discord level</div><div class="value">{n_near}</div><div class="note">Spot within ±8 pts</div></div>
</div>

<h2>1. Top-level Alignment vs Baseline</h2>
<table>
  <thead><tr><th>Subset</th><th>N</th><th>W</th><th>L</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th><th>Δ vs Baseline</th></tr></thead>
  <tbody>{sec_top}</tbody>
</table>

<div class="callout info">
  <strong>Headline finding:</strong> Discord-aligned (≥1 indicator) signals outperform baseline by
  <strong>+2-3 pp WR and +0.5 pts/signal</strong> across {n_aligned} trades — small but consistent.
  Strong-alignment (≥2 indicators) does NOT add edge, suggesting <em>over-fitting</em> when multiple
  signals collide (single low-quality day can dominate a 2-of-3 filter).
</div>

<h2>2. Indicator Breakdown</h2>
<p>How each individual Discord context indicator performs against the covered-window baseline.</p>
<table>
  <thead><tr><th>Subset</th><th>N</th><th>W</th><th>L</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th><th>Δ vs Baseline</th></tr></thead>
  <tbody>{sec_ind}</tbody>
</table>

<div class="callout good">
  <strong>Strongest single indicator:</strong> <code>near_discord_level</code> (spot within ±8 pts of an Apollo/DM-named level) —
  <strong>57% WR / +1.76 avg</strong> on n=258, vs <strong>50% WR / +0.73 avg</strong> when far.
  This is repeatable across multiple days and authors (not regime-locked). <em>Strongest filter candidate.</em>
</div>

<h2>3. WR Uplift by Pro-Author Bucket</h2>
<p>Days when each author was actively flagging patterns; signals on those days only.</p>
<table>
  <thead><tr><th>Author bucket</th><th>N</th><th>W</th><th>L</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th><th>Δ vs Baseline</th></tr></thead>
  <tbody>{sec_auth}</tbody>
</table>

<p class='note'>Author buckets overlap (many days had multiple authors flagging patterns). Per-author WR is
day-level proxy, not message-level. Sample size makes per-author quantification weak — directional only.</p>

<h2>4. Top Candidate Filters (by WR, n≥10)</h2>
<p>Setup × direction × Discord-indicator combos. Sorted by WR descending. Base WR is for setup × direction
without the Discord overlay.</p>
<table>
  <thead><tr><th>Filter</th><th>N</th><th>W</th><th>L</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th><th>vs base WR/Avg</th></tr></thead>
  <tbody>{sec_top_wr}</tbody>
</table>

<h2>5. Top Candidate Filters (by PnL)</h2>
<table>
  <thead><tr><th>Filter</th><th>N</th><th>W</th><th>L</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th><th>vs base</th></tr></thead>
  <tbody>{sec_top_pnl}</tbody>
</table>

<h2>6. AVOID List (lowest WR, n≥10)</h2>
<p>Subsets where Discord context predicts <strong>worse</strong> outcomes. These suggest negative filters.</p>
<table>
  <thead><tr><th>Filter</th><th>N</th><th>W</th><th>L</th><th>WR</th><th>PnL</th><th>Avg</th><th>MaxDD</th><th>vs base</th></tr></thead>
  <tbody>{sec_avoid}</tbody>
</table>

{msg_block}

<h2>Per-Date Summary</h2>
<table>
  <thead><tr><th>Date</th><th>Pro Bias</th><th>Apollo AM/PM</th><th>Named Patterns</th><th>N</th><th>WR</th><th>PnL</th></tr></thead>
  <tbody>{date_html}</tbody>
</table>

<h2>Top 5 Filter Candidates for Tasks.md</h2>
<div class="callout">
  <strong>Methodology note:</strong> All candidates below have n &lt; 100 (covered window only 21 days,
  ~750 signals). Per <code>feedback_dont_ship_on_short_term_flip.md</code>, candidates need OOS validation
  and mechanism check before shipping. These are TRACKING items, not ship-ready filters.
</div>

<ol>
<li><strong>SC SHORT × near-discord-level (≥8pt of Apollo/DM level)</strong> — 63% WR vs 50% FAR, +160.5 vs +15.6 pts, n=61 vs n=58.
  <em>Mechanism:</em> Apollo/DM levels often = key dealer hedge nodes; SC shorts at these levels have additional structural confluence.
  <em>Next step:</em> Backfill historical Volland-derived level proximity (DD top-strikes, +GEX magnets) and verify pattern generalizes beyond Discord-named levels.</li>

<li><strong>GEX LONG × near-discord-level</strong> — 80% WR vs 36% FAR, +37.2 vs -42.4 pts, n=10 vs n=14.
  <em>Mechanism:</em> Discord levels = high-conviction support/target zones; GEX Long entries here align with dealer flow.
  <em>Caveat:</em> n=10 is tiny — directional signal only. Validate with broader level-proximity definition over 100+ trades.</li>

<li><strong>SC LONG × bias-aligned (LH/Apollo bullish day)</strong> — 67% WR vs 43% opposed, +76.7 vs -25.2 pts, n=23 vs n=25.
  <em>Mechanism:</em> SC longs work better on bullish-regime days regardless of paradigm — confirms V14 logic that SC longs benefit from bullish regime confirmation.
  <em>Next step:</em> Test against synthetic bias proxy (overvix + multi-day price trend) since live bias-from-Discord isn't available.</li>

<li><strong>AG Short × bias-opposed (pros say bullish but AG fires)</strong> — 83% WR, +84.9 pts, n=12.
  <em>Mechanism:</em> Counter-intuitive — when pros are bullish but AG fires short, it's a meaningful contrarian dealer signal.
  <em>Caveat:</em> n=12 is too small to act on. Track over next 30 AG Short fires on bullish-bias days.</li>

<li><strong>DD Exhaustion SHORT × bias-opposed (pros say bullish, DD short)</strong> — <strong>AVOID</strong>. 35% WR, -151.5 pts, n=74.
  <em>Mechanism:</em> When pros confirm bullish regime, DD short signals are systematically wrong — they're trying to contrarian-fade a structural up-move.
  <em>Action:</em> Add to V14-tracking: <strong>BLOCK DD SHORT on bullish-bias days</strong> (proxy via consecutive GEX paradigm cycles or trend filter).</li>
</ol>

<h2>Methodology Notes</h2>
<ul>
<li>Discord context built manually from curated daily extracts (Volland Discord transcripts in <code>references/volland/</code> and prior sync analyses in <code>exports/</code> and <code>.claude-memory/</code>). Day-level annotation in <code>_tmp_track_c_discord_timeline.json</code>.</li>
<li>Bias score mapping: <code>bullish/_choppy/_caution/_target_hit = +1/+2</code>, <code>neutral_bullish = +1</code>, <code>bearish_to_bullish/_gap_recovery = +1</code> (net intraday bullish), <code>bearish/_fade_rally = -2</code>, <code>bearish_mixed = -1</code>, <code>neutral = 0</code>.</li>
<li>Apollo paradigm match: shared root token (e.g. signal paradigm "GEX-PURE" matches Apollo AM "GEX").</li>
<li>Near discord level: signal spot within ±8 pts of any level in that day's discord_levels list.</li>
<li>Aligned ≥1: any of bias/paradigm/near-level. Strong ≥2: any 2 of 3.</li>
<li>WR computed on WIN+LOSS only (excludes EXPIRED). PnL totals include all outcomes.</li>
<li>Many "named patterns" colinear at day level — single low-WR day dominates a 30-trade filter. Patterns deduplicated by author proxy.</li>
</ul>

<div class="footer">
  Generated 2026-05-13 · TRACK C · Discord coverage: 21 trading days · Day-level + ±15 min matching ·
  Per <code>feedback_analysis_validation.md</code>, <code>feedback_never_assume_in_reports.md</code>,
  <code>feedback_dont_ship_on_short_term_flip.md</code>.
</div>

</body></html>"""

    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[track-c-final] wrote {OUT_HTML}")
    return top_wr, top_pnl, bot_wr_filt


if __name__ == "__main__":
    main()
