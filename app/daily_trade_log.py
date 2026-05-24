"""S178 Phase 2: Daily V16 trade-log report as importable module.

Exposes `build_html(engine, date_iso) -> str` for use by:
  - FastAPI portal route `/v16-trade-log?date=YYYY-MM-DD`
  - Telegram EOD link from `_send_setup_eod_summary`
  - CLI script `_tmp_daily_trade_log.py` (preserved for local use)

Per-trade card style:
  - Setup, direction, grade, paradigm, alignment, VIX, score
  - Entry / exit prices, sim P&L (SPX pts), broker P&L (MES $)
  - MFE/MAE, close reason, drift label
  - Comment textarea with localStorage persistence per trade id
  - Sticky toolbar with Export TXT / Copy / Clear All

Self-contained: only imports stdlib + sqlalchemy. Caller passes engine.
"""
from __future__ import annotations

import html
from datetime import date as _date
from sqlalchemy import text

# Dark theme palette (Analysis #15 style)
_BG = "#1a1a2e"; _PANEL = "#16213e"; _CARD = "#0f3460"
_GREEN = "#00e676"; _RED = "#ff5252"; _BLUE = "#448aff"
_GOLD = "#ffd740"; _PURPLE = "#e040fb"
_WHITE = "#ffffff"; _LIGHT = "#b0bec5"; _DIM = "#607d8b"
_ORANGE = "#ff9800"


def _trade_card(d: dict) -> str:
    is_long = d["direction"] in ("long", "bullish")
    dir_color = _GREEN if is_long else _RED
    dir_arrow = "▲" if is_long else "▼"

    outcome = d["outcome_result"] or "OPEN"
    outcome_color = (
        _GREEN if outcome == "WIN" else
        (_RED if outcome == "LOSS" else
         (_GOLD if outcome == "EXPIRED" else _LIGHT))
    )

    sim_pnl = float(d["outcome_pnl"] or 0)
    fill = float(d["fill"]) if d["fill"] else None
    close_fp = float(d["close_fill"]) if d["close_fill"] else None
    broker_pnl_pts = None
    broker_pnl_usd = None
    if fill and close_fp:
        sign = 1 if is_long else -1
        broker_pnl_pts = sign * (close_fp - fill)
        broker_pnl_usd = broker_pnl_pts * 5  # 1 MES = $5/pt
    sim_color = _GREEN if sim_pnl > 0 else (_RED if sim_pnl < 0 else _LIGHT)
    real_color = (
        _GREEN if (broker_pnl_usd or 0) > 0 else
        (_RED if (broker_pnl_usd or 0) < 0 else _LIGHT)
    )
    drift = None
    if broker_pnl_pts is not None:
        drift = broker_pnl_pts - sim_pnl

    grade_color = {"A+": _GREEN, "A": _BLUE, "B": _GOLD,
                   "C": _ORANGE, "LOG": _DIM}.get(d["grade"], _LIGHT)
    align = d["greek_alignment"]
    align_str = f"{align:+d}" if align is not None else "—"
    align_color = (
        _GREEN if (align or 0) > 0 else
        (_RED if (align or 0) < 0 else _LIGHT)
    )

    mfe = d["outcome_max_profit"] or 0
    mae = d["outcome_max_loss"] or 0

    close_reason = d["close_reason"] or "—"
    reason_color = (
        _GREEN if "win" in close_reason else
        (_RED if ("loss" in close_reason or "stop" in close_reason) else
         (_GOLD if "ghost" in close_reason or "eod" in close_reason else _LIGHT))
    )

    drift_str = ""
    if drift is not None:
        if abs(drift) > 10:
            drift_str = f"<span style='color:{_ORANGE};font-weight:bold'>⚠️ drift {drift:+.1f}pt</span>"
        elif abs(drift) > 5:
            drift_str = f"<span style='color:{_GOLD}'>drift {drift:+.1f}pt</span>"
        else:
            drift_str = f"<span style='color:{_DIM}'>drift {drift:+.1f}pt</span>"

    spot_str = f"{d['spot']:.2f}" if d.get('spot') else "—"
    fill_str = f"{fill}" if fill else "—"
    close_str = f"{close_fp}" if close_fp else "—"

    return f"""
<div class="card" data-lid="{d['id']}">
  <div class="card-hdr">
    <div class="hdr-left">
      <span class="lid">#{d['id']}</span>
      <span class="time">{d['et'].strftime('%H:%M')}</span>
      <span class="setup">{html.escape(d['setup_name'])}</span>
      <span class="dir" style="color:{dir_color}">{dir_arrow} {d['direction']}</span>
      <span class="grade" style="background:{grade_color};color:#000">{d['grade'] or '—'}</span>
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
      <div><span class="k">Spot</span><span class="v">{spot_str}</span></div>
      <div><span class="k">LIS</span><span class="v">{d['lis'] or '—'}</span></div>
      <div><span class="k">Target</span><span class="v">{d['target'] or '—'}</span></div>
      <div><span class="k">Account</span><span class="v">{d['acct'] or '—'}</span></div>
    </div>
    <div class="pnl-row">
      <div class="pnl-box">
        <div class="pnl-lbl">Entry → Exit (broker)</div>
        <div class="pnl-val">{fill_str} → {close_str}</div>
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
        <div class="pnl-val"><span style="color:{_GREEN}">+{mfe:.1f}</span> / <span style="color:{_RED}">{mae:.1f}</span></div>
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


def build_html(engine, date_iso: str | None = None) -> str:
    """Build the V16 daily trade log HTML for the given ET date.

    Args:
        engine: SQLAlchemy engine connected to the prod DB.
        date_iso: 'YYYY-MM-DD' (default: today).

    Returns:
        Full HTML document string. Empty-state message if no trades.
    """
    if date_iso is None:
        date_iso = _date.today().isoformat()

    with engine.connect() as c:
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
        """), {"d": date_iso}).fetchall()

    if not rows:
        return _empty_doc(date_iso)

    cards_html = "\n".join(_trade_card(dict(r._mapping)) for r in rows)

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

    sim_color = _GREEN if total_sim >= 0 else _RED
    broker_color = _GREEN if total_broker_pts >= 0 else _RED

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>V16 Trade Log — {date_iso}</title>
<style>
* {{ box-sizing: border-box; }}
body {{ margin:0; background:{_BG}; color:{_WHITE}; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; }}
.toolbar {{ position:sticky; top:0; z-index:10; background:{_PANEL}; border-bottom:2px solid {_BLUE}; padding:12px 20px; display:flex; gap:12px; align-items:center; flex-wrap:wrap; }}
.toolbar h1 {{ margin:0; font-size:18px; color:{_WHITE}; }}
.toolbar .summary {{ flex:1; color:{_LIGHT}; font-size:13px; }}
.toolbar .date-nav {{ display:flex; gap:6px; align-items:center; }}
.toolbar input[type=date] {{ background:#0a1024; color:{_WHITE}; border:1px solid #2a3554; border-radius:4px; padding:6px 8px; font-family:inherit; }}
.toolbar button {{ padding:8px 16px; border:none; border-radius:4px; cursor:pointer; font-weight:600; }}
.btn-go {{ background:{_BLUE}; color:#fff; }}
.btn-export {{ background:{_GREEN}; color:#000; }}
.btn-copy {{ background:{_BLUE}; color:#fff; }}
.btn-clear {{ background:{_RED}; color:#fff; }}
.toolbar button:hover {{ opacity:0.85; }}
.cards {{ padding: 16px 20px; max-width: 1100px; margin: 0 auto; }}
.card {{ background:{_PANEL}; border-radius:8px; margin-bottom:16px; overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,0.3); }}
.card-hdr {{ background:{_CARD}; padding:10px 16px; display:flex; justify-content:space-between; align-items:center; }}
.hdr-left {{ display:flex; gap:12px; align-items:center; }}
.lid {{ color:{_DIM}; font-family:monospace; }}
.time {{ color:{_LIGHT}; font-family:monospace; }}
.setup {{ font-weight:600; }}
.dir {{ font-weight:600; }}
.grade {{ padding:2px 8px; border-radius:4px; font-weight:600; font-size:12px; }}
.outcome {{ font-weight:700; font-size:15px; padding:4px 12px; border-radius:4px; background:rgba(0,0,0,0.3); }}
.card-body {{ padding:14px 16px; }}
.kvs {{ display:grid; grid-template-columns: repeat(4, 1fr); gap:8px 16px; margin-bottom:12px; }}
.kvs > div {{ display:flex; justify-content:space-between; border-bottom:1px dashed #2a3554; padding:4px 0; font-size:13px; }}
.kvs .k {{ color:{_LIGHT}; }}
.kvs .v {{ color:{_WHITE}; font-weight:500; }}
.pnl-row {{ display:grid; grid-template-columns: repeat(3, 1fr); gap:8px; margin-bottom:12px; }}
.pnl-box {{ background:{_CARD}; padding:8px 12px; border-radius:4px; }}
.pnl-lbl {{ color:{_DIM}; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }}
.pnl-val {{ font-size:15px; font-weight:600; margin-top:2px; }}
.comment {{ width:100%; min-height:60px; background:#0a1024; color:{_WHITE}; border:1px solid #2a3554; border-radius:4px; padding:8px 10px; font-family:inherit; font-size:13px; resize:vertical; }}
.comment:focus {{ outline:none; border-color:{_BLUE}; }}
.comment.has-content {{ border-color:{_GREEN}; }}
</style>
</head>
<body>
<div class="toolbar">
  <h1>V16 Trade Log — {date_iso}</h1>
  <div class="summary">
    <strong>{len(rows)} trades</strong> · {wins}W / {losses}L / {expired} EXP ·
    Sim <span style="color:{sim_color}">{total_sim:+.1f}pt</span> ·
    Broker <span style="color:{broker_color}">{total_broker_pts:+.1f}pt (${total_broker_pts*5:+.2f})</span>
    ({n_with_broker}/{len(rows)} computable)
  </div>
  <div class="date-nav">
    <input type="date" id="date-picker" value="{date_iso}" max="{_date.today().isoformat()}">
    <button class="btn-go" onclick="navDate()">Go</button>
  </div>
  <button class="btn-export" onclick="exportComments()">Export TXT</button>
  <button class="btn-copy" onclick="copyToClipboard()">Copy</button>
  <button class="btn-clear" onclick="clearAllComments()">Clear All</button>
</div>
<div class="cards">
{cards_html}
</div>
<script>
const DATE = "{date_iso}";
const STORAGE_KEY = `tradelog-comments-${{DATE}}`;

function navDate() {{
  const v = document.getElementById('date-picker').value;
  if (v) location.search = '?date=' + v;
}}

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


def _empty_doc(date_iso: str) -> str:
    today = _date.today().isoformat()
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><title>V16 Trade Log — {date_iso}</title>
<style>
body {{ background:{_BG}; color:{_WHITE}; font-family:-apple-system,sans-serif; padding:40px; text-align:center; }}
h1 {{ color:{_WHITE}; }}
p {{ color:{_LIGHT}; font-size:14px; }}
input,button {{ padding:8px 12px; background:#0a1024; color:{_WHITE}; border:1px solid #2a3554; border-radius:4px; font-family:inherit; }}
button {{ background:{_BLUE}; cursor:pointer; }}
</style></head>
<body>
<h1>V16 Trade Log — {date_iso}</h1>
<p>No trades placed on this date.</p>
<form onsubmit="event.preventDefault();location.search='?date='+document.getElementById('d').value;">
  <input type="date" id="d" value="{date_iso}" max="{today}">
  <button>Go</button>
</form>
</body></html>
"""
