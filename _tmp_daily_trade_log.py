"""Daily V16 trade log HTML report — per-trade cards + comment textareas.

Usage:
    python _tmp_daily_trade_log.py [YYYY-MM-DD]
    (default: today)

Generates daily_trade_logs/YYYY-MM-DD.html with:
  - One card per V16-placed real-money trade (real_trade_orders exists)
  - Setup name, direction, grade, paradigm, alignment, entry, MFE/MAE
  - Sim P&L (portal/SPX) vs broker P&L (MES from state)
  - Comment textarea (localStorage persistence per trade id)
  - Sticky top toolbar: export comments to TXT + clear all

Workflow: user opens HTML in browser, comments on each trade, clicks
Export TXT → copy/paste to me. I read carefully + backtest any
proposed filter rule changes.
"""
import os, sys, json, html
from datetime import date, datetime
from sqlalchemy import create_engine, text

# Date arg
arg_date = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()

eng = create_engine(os.environ["DATABASE_URL"])

# Dark theme palette (Analysis #15 style)
BG = "#1a1a2e"; PANEL = "#16213e"; CARD = "#0f3460"
GREEN = "#00e676"; RED = "#ff5252"; BLUE = "#448aff"
GOLD = "#ffd740"; PURPLE = "#e040fb"
WHITE = "#ffffff"; LIGHT = "#b0bec5"; DIM = "#607d8b"
ORANGE = "#ff9800"

with eng.connect() as c:
    rows = c.execute(text("""
        SELECT sl.id, sl.setup_name, sl.direction, sl.grade, sl.paradigm,
               sl.greek_alignment, sl.vix, sl.score,
               sl.ts AT TIME ZONE 'America/New_York' AS et,
               sl.spot, sl.abs_es_price,
               sl.outcome_result, sl.outcome_pnl,
               sl.outcome_max_profit, sl.outcome_max_loss,
               sl.outcome_first_event, sl.exit_price,
               sl.lis, sl.target,
               rto.state->>'fill_price' AS fill,
               rto.state->>'close_reason' AS close_reason,
               COALESCE(
                   rto.state->>'stop_fill_price',
                   rto.state->>'target_fill_price',
                   rto.state->>'flatten_fill_price',
                   rto.state->>'close_fill_price'
               ) AS close_fill,
               rto.state->>'account_id' AS acct
        FROM setup_log sl
        JOIN real_trade_orders rto ON rto.setup_log_id = sl.id
        WHERE sl.ts::date = :d
        ORDER BY sl.ts ASC
    """), {"d": arg_date}).fetchall()

print(f"Loaded {len(rows)} trades for {arg_date}", file=sys.stderr)

if not rows:
    print("No trades today — nothing to report", file=sys.stderr)
    sys.exit(0)

# Per-trade card rendering
def trade_card(d):
    is_long = d["direction"] in ("long", "bullish")
    dir_color = GREEN if is_long else RED
    dir_arrow = "▲" if is_long else "▼"

    outcome = d["outcome_result"] or "OPEN"
    outcome_color = GREEN if outcome == "WIN" else (RED if outcome == "LOSS" else (GOLD if outcome == "EXPIRED" else LIGHT))

    sim_pnl = float(d["outcome_pnl"] or 0)
    fill = float(d["fill"]) if d["fill"] else None
    close_fp = float(d["close_fill"]) if d["close_fill"] else None
    broker_pnl_pts = None
    broker_pnl_usd = None
    if fill and close_fp:
        sign = 1 if is_long else -1
        broker_pnl_pts = sign * (close_fp - fill)
        broker_pnl_usd = broker_pnl_pts * 5  # 1 MES = $5/pt
    sim_color = GREEN if sim_pnl > 0 else (RED if sim_pnl < 0 else LIGHT)
    real_color = GREEN if (broker_pnl_usd or 0) > 0 else (RED if (broker_pnl_usd or 0) < 0 else LIGHT)
    drift = None
    if broker_pnl_pts is not None:
        drift = broker_pnl_pts - sim_pnl

    grade_color = {"A+": GREEN, "A": BLUE, "B": GOLD, "C": ORANGE, "LOG": DIM}.get(d["grade"], LIGHT)
    align = d["greek_alignment"]
    align_str = f"{align:+d}" if align is not None else "—"
    align_color = GREEN if (align or 0) > 0 else (RED if (align or 0) < 0 else LIGHT)

    mfe = d["outcome_max_profit"] or 0
    mae = d["outcome_max_loss"] or 0

    close_reason = d["close_reason"] or "—"
    reason_color = GREEN if "win" in close_reason else (RED if ("loss" in close_reason or "stop" in close_reason) else (GOLD if "ghost" in close_reason or "eod" in close_reason else LIGHT))

    # Drift label
    drift_str = ""
    if drift is not None:
        if abs(drift) > 10:
            drift_str = f"<span style='color:{ORANGE};font-weight:bold'>⚠️ drift {drift:+.1f}pt</span>"
        elif abs(drift) > 5:
            drift_str = f"<span style='color:{GOLD}'>drift {drift:+.1f}pt</span>"
        else:
            drift_str = f"<span style='color:{DIM}'>drift {drift:+.1f}pt</span>"

    return f"""
<div class="card" data-lid="{d['id']}">
  <div class="card-hdr">
    <div class="hdr-left">
      <span class="lid">#{d['id']}</span>
      <span class="time">{d['et'].strftime('%H:%M')}</span>
      <span class="setup">{html.escape(d['setup_name'])}</span>
      <span class="dir" style="color:{dir_color}">{dir_arrow} {d['direction']}</span>
      <span class="grade" style="background:{grade_color};color:#000">{d['grade']}</span>
    </div>
    <div class="hdr-right">
      <span class="outcome" style="color:{outcome_color}">{outcome}</span>
    </div>
  </div>
  <div class="card-body">
    <div class="kvs">
      <div><span class="k">Paradigm</span><span class="v">{html.escape(d['paradigm'] or '—')}</span></div>
      <div><span class="k">Align</span><span class="v" style="color:{align_color}">{align_str}</span></div>
      <div><span class="k">VIX</span><span class="v">{d['vix'] or '—'}</span></div>
      <div><span class="k">Score</span><span class="v">{d['score'] or '—'}</span></div>
      <div><span class="k">Spot</span><span class="v">{d['spot']:.2f}</span></div>
      <div><span class="k">LIS</span><span class="v">{d['lis'] or '—'}</span></div>
      <div><span class="k">Target</span><span class="v">{d['target'] or '—'}</span></div>
      <div><span class="k">Account</span><span class="v">{d['acct'] or '—'}</span></div>
    </div>
    <div class="pnl-row">
      <div class="pnl-box">
        <div class="pnl-lbl">Entry → Exit (broker)</div>
        <div class="pnl-val">{fill or '—'} → {close_fp or '—'}</div>
      </div>
      <div class="pnl-box">
        <div class="pnl-lbl">Sim P&amp;L (SPX pts)</div>
        <div class="pnl-val" style="color:{sim_color}">{sim_pnl:+.1f}</div>
      </div>
      <div class="pnl-box">
        <div class="pnl-lbl">Broker P&amp;L</div>
        <div class="pnl-val" style="color:{real_color}">{(f'{broker_pnl_pts:+.2f}pt ${broker_pnl_usd:+.2f}' if broker_pnl_usd is not None else 'n/a')}</div>
      </div>
      <div class="pnl-box">
        <div class="pnl-lbl">MFE / MAE</div>
        <div class="pnl-val"><span style="color:{GREEN}">+{mfe:.1f}</span> / <span style="color:{RED}">{mae:.1f}</span></div>
      </div>
      <div class="pnl-box">
        <div class="pnl-lbl">Close reason</div>
        <div class="pnl-val" style="color:{reason_color};font-size:13px">{html.escape(close_reason)}</div>
      </div>
      <div class="pnl-box">
        <div class="pnl-lbl">Drift</div>
        <div class="pnl-val">{drift_str or 'n/a'}</div>
      </div>
    </div>
    <textarea class="comment" data-lid="{d['id']}" placeholder="Your comment on this trade (saves automatically)..."></textarea>
  </div>
</div>
"""

cards_html = "\n".join(trade_card(dict(r._mapping)) for r in rows)

# Totals
total_sim = sum(float(r._mapping["outcome_pnl"] or 0) for r in rows)
total_broker_pts = 0.0
n_with_broker = 0
wins = sum(1 for r in rows if r._mapping["outcome_result"] == "WIN")
losses = sum(1 for r in rows if r._mapping["outcome_result"] == "LOSS")
expired = sum(1 for r in rows if r._mapping["outcome_result"] == "EXPIRED")
for r in rows:
    d = dict(r._mapping)
    fill = float(d["fill"]) if d["fill"] else None
    close_fp = float(d["close_fill"]) if d["close_fill"] else None
    if fill and close_fp:
        sign = 1 if d["direction"] in ("long", "bullish") else -1
        total_broker_pts += sign * (close_fp - fill)
        n_with_broker += 1

doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>V16 Trade Log — {arg_date}</title>
<style>
* {{ box-sizing: border-box; }}
body {{ margin:0; background:{BG}; color:{WHITE}; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; }}
.toolbar {{ position:sticky; top:0; z-index:10; background:{PANEL}; border-bottom:2px solid {BLUE}; padding:12px 20px; display:flex; gap:12px; align-items:center; }}
.toolbar h1 {{ margin:0; font-size:18px; color:{WHITE}; }}
.toolbar .summary {{ flex:1; color:{LIGHT}; font-size:13px; }}
.toolbar button {{ padding:8px 16px; border:none; border-radius:4px; cursor:pointer; font-weight:600; }}
.btn-export {{ background:{GREEN}; color:#000; }}
.btn-copy {{ background:{BLUE}; color:#fff; }}
.btn-clear {{ background:{RED}; color:#fff; }}
.toolbar button:hover {{ opacity:0.85; }}
.cards {{ padding: 16px 20px; max-width: 1100px; margin: 0 auto; }}
.card {{ background:{PANEL}; border-radius:8px; margin-bottom:16px; overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,0.3); }}
.card-hdr {{ background:{CARD}; padding:10px 16px; display:flex; justify-content:space-between; align-items:center; }}
.hdr-left {{ display:flex; gap:12px; align-items:center; }}
.lid {{ color:{DIM}; font-family:monospace; }}
.time {{ color:{LIGHT}; font-family:monospace; }}
.setup {{ font-weight:600; }}
.dir {{ font-weight:600; }}
.grade {{ padding:2px 8px; border-radius:4px; font-weight:600; font-size:12px; }}
.outcome {{ font-weight:700; font-size:15px; padding:4px 12px; border-radius:4px; background:rgba(0,0,0,0.3); }}
.card-body {{ padding:14px 16px; }}
.kvs {{ display:grid; grid-template-columns: repeat(4, 1fr); gap:8px 16px; margin-bottom:12px; }}
.kvs > div {{ display:flex; justify-content:space-between; border-bottom:1px dashed #2a3554; padding:4px 0; font-size:13px; }}
.kvs .k {{ color:{LIGHT}; }}
.kvs .v {{ color:{WHITE}; font-weight:500; }}
.pnl-row {{ display:grid; grid-template-columns: repeat(3, 1fr); gap:8px; margin-bottom:12px; }}
.pnl-box {{ background:{CARD}; padding:8px 12px; border-radius:4px; }}
.pnl-lbl {{ color:{DIM}; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }}
.pnl-val {{ font-size:15px; font-weight:600; margin-top:2px; }}
.comment {{ width:100%; min-height:60px; background:#0a1024; color:{WHITE}; border:1px solid #2a3554; border-radius:4px; padding:8px 10px; font-family:inherit; font-size:13px; resize:vertical; }}
.comment:focus {{ outline:none; border-color:{BLUE}; }}
.comment.has-content {{ border-color:{GREEN}; }}
</style>
</head>
<body>
<div class="toolbar">
  <h1>V16 Trade Log — {arg_date}</h1>
  <div class="summary">
    <strong>{len(rows)} trades</strong> · {wins}W / {losses}L / {expired} EXP ·
    Sim <span style="color:{GREEN if total_sim>=0 else RED}">{total_sim:+.1f}pt</span> ·
    Broker <span style="color:{GREEN if total_broker_pts>=0 else RED}">{total_broker_pts:+.1f}pt (${total_broker_pts*5:+.2f})</span>
    ({n_with_broker}/{len(rows)} computable)
  </div>
  <button class="btn-export" onclick="exportComments()">Export TXT</button>
  <button class="btn-copy" onclick="copyToClipboard()">Copy</button>
  <button class="btn-clear" onclick="clearAllComments()">Clear All</button>
</div>
<div class="cards">
{cards_html}
</div>
<script>
const DATE = "{arg_date}";
const STORAGE_KEY = `tradelog-comments-${{DATE}}`;

function loadComments() {{
  const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{{}}");
  document.querySelectorAll(".comment").forEach(ta => {{
    const lid = ta.dataset.lid;
    if (saved[lid]) {{
      ta.value = saved[lid];
      ta.classList.add("has-content");
    }}
    ta.addEventListener("input", () => {{
      const all = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{{}}");
      if (ta.value.trim()) {{ all[lid] = ta.value; ta.classList.add("has-content"); }}
      else {{ delete all[lid]; ta.classList.remove("has-content"); }}
      localStorage.setItem(STORAGE_KEY, JSON.stringify(all));
    }});
  }});
}}

function buildExportText() {{
  const saved = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{{}}");
  let txt = `V16 Trade Log Comments — ${{DATE}}\\n${{"=".repeat(50)}}\\n\\n`;
  document.querySelectorAll(".card").forEach(card => {{
    const lid = card.dataset.lid;
    const comment = saved[lid];
    if (!comment || !comment.trim()) return;
    const hdr = card.querySelector(".hdr-left").innerText.replace(/\\n+/g, " ");
    const outcome = card.querySelector(".outcome").innerText;
    txt += `[${{hdr}}] ${{outcome}}\\n${{comment.trim()}}\\n\\n`;
  }});
  return txt;
}}

function exportComments() {{
  const txt = buildExportText();
  const blob = new Blob([txt], {{ type: "text/plain" }});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `v16-tradelog-comments-${{DATE}}.txt`;
  a.click();
}}

function copyToClipboard() {{
  const txt = buildExportText();
  navigator.clipboard.writeText(txt).then(() => alert("Copied to clipboard."));
}}

function clearAllComments() {{
  if (!confirm("Clear ALL comments for this date?")) return;
  localStorage.removeItem(STORAGE_KEY);
  document.querySelectorAll(".comment").forEach(ta => {{ ta.value = ""; ta.classList.remove("has-content"); }});
}}

loadComments();
</script>
</body>
</html>
"""

os.makedirs("daily_trade_logs", exist_ok=True)
out_path = f"daily_trade_logs/{arg_date}.html"
with open(out_path, "w", encoding="utf-8") as f:
    f.write(doc)
print(f"Wrote {out_path} ({len(rows)} trades, {wins}W/{losses}L, broker {total_broker_pts*5:+.2f}$)", file=sys.stderr)
