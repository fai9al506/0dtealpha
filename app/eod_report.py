"""
EOD PDF Report generator for 0DTE Alpha.
Self-contained — no imports from main.py.
Receives engine + date, returns PDF path.
"""

import io
import os
import tempfile
from datetime import datetime, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.dates as mdates
import numpy as np
import requests
from fpdf import FPDF
from sqlalchemy import text


# ── query ────────────────────────────────────────────────────────────────

def _query_trades(engine, trade_date):
    """Query all resolved trades for a given date."""
    today_start = datetime.combine(trade_date, datetime.min.time())
    tomorrow_start = today_start + timedelta(days=1)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, ts, setup_name, direction, grade, score,
                   paradigm, spot, lis, target, gap_to_lis, upside, rr_ratio,
                   support_score, upside_score, floor_cluster_score,
                   target_cluster_score, rr_score,
                   bofa_stop_level, bofa_target_level, bofa_lis_width,
                   abs_es_price, comments,
                   outcome_result, outcome_pnl, outcome_max_profit,
                   outcome_max_loss, outcome_elapsed_min,
                   outcome_stop_level, outcome_target_level
            FROM setup_log
            WHERE ts >= :today_start AND ts < :tomorrow_start
              AND outcome_result IS NOT NULL
            ORDER BY ts ASC
        """), {"today_start": today_start, "tomorrow_start": tomorrow_start}).fetchall()

    trades = []
    for r in rows:
        trades.append({
            "id": r[0], "ts": r[1], "setup_name": r[2], "direction": r[3],
            "grade": r[4], "score": r[5], "paradigm": r[6] or "",
            "spot": r[7], "lis": r[8], "target": r[9],
            "gap_to_lis": r[10], "upside": r[11], "rr_ratio": r[12],
            "support_score": r[13], "upside_score": r[14],
            "floor_cluster_score": r[15], "target_cluster_score": r[16],
            "rr_score": r[17],
            "bofa_stop_level": r[18], "bofa_target_level": r[19],
            "bofa_lis_width": r[20],
            "abs_es_price": r[21], "comments": r[22] or "",
            "outcome_result": r[23], "outcome_pnl": float(r[24]) if r[24] is not None else 0.0,
            "outcome_max_profit": r[25], "outcome_max_loss": r[26],
            "outcome_elapsed_min": r[27],
            "outcome_stop_level": r[28], "outcome_target_level": r[29],
        })
    return trades


# ── why builder ──────────────────────────────────────────────────────────

def _build_why(t):
    """Build a 1-line explanation of why the trade was entered."""
    name = t["setup_name"]
    paradigm = t["paradigm"]

    if name == "GEX Long":
        gap = f"{abs(t['gap_to_lis'] or 0):.0f}" if t.get("gap_to_lis") else "?"
        lis = f"{t['lis']:.0f}" if t.get("lis") else "?"
        rr = f"{t['rr_ratio']:.1f}" if t.get("rr_ratio") else "?"
        return f"Support {gap}pt from LIS {lis}, R:R {rr}, {paradigm}"

    if name == "AG Short":
        gap = f"{abs(t['gap_to_lis'] or 0):.0f}" if t.get("gap_to_lis") else "?"
        lis = f"{t['lis']:.0f}" if t.get("lis") else "?"
        rr = f"{t['rr_ratio']:.1f}" if t.get("rr_ratio") else "?"
        return f"Resistance {gap}pt from LIS {lis}, R:R {rr}, {paradigm}"

    if name == "BofA Scalp":
        width = f"{t['bofa_lis_width']:.0f}" if t.get("bofa_lis_width") else "?"
        return f"LIS scalp, {paradigm}, width {width}pt"

    if name == "ES Absorption":
        c = t.get("comments", "")
        if c:
            return c[:90] + ("..." if len(c) > 90 else "")
        return f"ES swing divergence, {paradigm}"

    if name == "DD Exhaustion":
        sc = f"{t['score']:.0f}" if t.get("score") else "?"
        return f"DD shift {t['direction'].lower()}, score {sc}, {paradigm}"

    if name == "Paradigm Reversal":
        gap = f"{abs(t['gap_to_lis'] or 0):.0f}" if t.get("gap_to_lis") else "?"
        return f"Paradigm shifted to {paradigm}, LIS gap {gap}pt"

    if name == "Skew Charm":
        return f"IV skew + charm alignment, {paradigm}"

    return f"{name}, {paradigm}"


# ── chart ────────────────────────────────────────────────────────────────

def _generate_pnl_chart(trades, trade_date):
    """Generate cumulative PnL line chart, return PNG bytes."""
    cum = 0.0
    xs, ys, colors = [], [], []
    for t in trades:
        cum += t["outcome_pnl"]
        ts = t["ts"]
        xs.append(ts)
        ys.append(cum)
        res = t["outcome_result"]
        if res == "WIN":
            colors.append("#22c55e")
        elif res == "LOSS":
            colors.append("#ef4444")
        else:
            colors.append("#9ca3af")

    fig, ax = plt.subplots(figsize=(8, 3), dpi=120)
    if xs:
        ax.plot(xs, ys, color="#6366f1", linewidth=1.5, zorder=1)
        ax.scatter(xs, ys, c=colors, s=30, zorder=2, edgecolors="white", linewidths=0.5)
        ax.axhline(y=0, color="#94a3b8", linewidth=0.5, linestyle="--")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.tick_params(labelsize=8)
        ax.set_ylabel("Cumulative P&L (pts)", fontsize=9)
        ax.set_title(f"0DTE Alpha — {trade_date.strftime('%B %d, %Y')}", fontsize=11, fontweight="bold")
        ax.grid(True, alpha=0.3)
    else:
        ax.text(0.5, 0.5, "No trades", ha="center", va="center", fontsize=14, color="#9ca3af")

    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ── PDF builder ──────────────────────────────────────────────────────────

# Dark theme color palette
_BG = (20, 20, 40)
_CARD = (30, 30, 55)
_ACCENT = (99, 102, 241)
_TXT = (230, 230, 240)
_MUTED = (160, 160, 185)
_WIN_C = (34, 197, 94)
_LOSS_C = (239, 68, 68)
_EXP_C = (251, 191, 36)
_WIN_ROW = (20, 45, 30)
_LOSS_ROW = (50, 20, 25)
_EXP_ROW = (40, 35, 20)
_HDR_ROW = (40, 40, 70)


def _sanitize(text):
    """Replace non-latin-1 chars so fpdf2 Helvetica doesn't crash."""
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _dark_bg(pdf):
    pdf.set_fill_color(*_BG)
    pdf.rect(0, 0, 297, 210, "F")


def _draw_table_header(pdf, headers, cw, y):
    """Draw table column headers at given y position."""
    pdf.set_fill_color(*_HDR_ROW)
    pdf.rect(8, y, 281, 5.5, "F")
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(*_MUTED)
    pdf.set_xy(8, y + 0.5)
    for i, h in enumerate(headers):
        pdf.cell(cw[i], 4.5, h, align="C")


def _build_pdf(trades, chart_png, trade_date):
    """Assemble landscape A4 PDF with dark professional theme."""
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=False)
    pdf.add_page()
    _dark_bg(pdf)

    # ── compute stats ──
    wins = sum(1 for t in trades if t["outcome_result"] == "WIN")
    losses = sum(1 for t in trades if t["outcome_result"] == "LOSS")
    expired = len(trades) - wins - losses
    total = len(trades)
    net_pnl = sum(t["outcome_pnl"] for t in trades)
    wr = f"{wins / total * 100:.0f}%" if total else "0%"
    pnl_color = _WIN_C if net_pnl >= 0 else _LOSS_C

    setup_stats = {}
    for t in trades:
        name = t["setup_name"]
        if name not in setup_stats:
            setup_stats[name] = {"count": 0, "pnl": 0.0, "w": 0, "l": 0, "e": 0}
        s = setup_stats[name]
        s["count"] += 1
        s["pnl"] += t["outcome_pnl"]
        if t["outcome_result"] == "WIN":
            s["w"] += 1
        elif t["outcome_result"] == "LOSS":
            s["l"] += 1
        else:
            s["e"] += 1
    sorted_setups = sorted(setup_stats.items(), key=lambda x: -x[1]["pnl"])

    # ═══════════════════════════════════════════════════════════════════
    # PAGE 1: Header + KPI cards + PnL chart + Setup breakdown
    # ═══════════════════════════════════════════════════════════════════

    # ── accent header bar ──
    pdf.set_fill_color(*_ACCENT)
    pdf.rect(0, 0, 297, 14, "F")
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(255, 255, 255)
    pdf.set_xy(10, 2)
    pdf.cell(0, 10, f"0DTE ALPHA   |   Daily Report   |   {trade_date.strftime('%B %d, %Y')}")

    # ── KPI cards ──
    card_y, card_h, card_w, gap = 18, 20, 55, 5
    x0 = 10
    kpis = [
        ("TOTAL TRADES", str(total), _ACCENT),
        ("WIN RATE", wr, _WIN_C if wins >= losses else _LOSS_C),
        ("NET P&L", f"{net_pnl:+.1f} pts", pnl_color),
        ("W / L / E", f"{wins}  /  {losses}  /  {expired}", _TXT),
    ]
    for i, (label, value, vc) in enumerate(kpis):
        cx = x0 + i * (card_w + gap)
        pdf.set_fill_color(*_CARD)
        pdf.rect(cx, card_y, card_w, card_h, "F")
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*_MUTED)
        pdf.set_xy(cx + 4, card_y + 2)
        pdf.cell(card_w - 8, 4, label)
        pdf.set_font("Helvetica", "B", 16)
        pdf.set_text_color(*vc)
        pdf.set_xy(cx + 4, card_y + 8)
        pdf.cell(card_w - 8, 8, value)

    # ── setup breakdown card (right side) ──
    bk_x = x0 + 4 * (card_w + gap)
    bk_w = 287 - bk_x
    pdf.set_fill_color(*_CARD)
    pdf.rect(bk_x, card_y, bk_w, card_h, "F")
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(*_MUTED)
    pdf.set_xy(bk_x + 3, card_y + 1.5)
    pdf.cell(bk_w - 6, 4, "SETUP BREAKDOWN")
    sy = card_y + 6
    for sname, s in sorted_setups:
        abbr = _SETUP_ABBREV.get(sname, sname[:8])
        cnt = s["w"] + s["l"] + s["e"]
        wle = f"{s['w']}W/{s['l']}L"
        if s["e"]:
            wle += f"/{s['e']}E"
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*_TXT)
        pdf.set_xy(bk_x + 3, sy)
        pdf.cell(20, 3, abbr)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*_MUTED)
        pdf.cell(8, 3, f"{cnt}t")
        pdf.cell(22, 3, wle)
        pdf.set_text_color(*(_WIN_C if s["pnl"] >= 0 else _LOSS_C))
        pdf.set_font("Helvetica", "B", 7)
        pdf.cell(16, 3, f"{s['pnl']:+.1f}", align="R")
        sy += 3.3
        if sy > card_y + card_h - 2:
            break

    # ── PnL chart ──
    chart_y = card_y + card_h + 4
    if chart_png:
        tmp_chart = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        tmp_chart.write(chart_png)
        tmp_chart.close()
        try:
            pdf.image(tmp_chart.name, x=10, y=chart_y, w=277)
        finally:
            try:
                os.unlink(tmp_chart.name)
            except Exception:
                pass

    # ── footer page 1 ──
    pdf.set_font("Helvetica", "I", 6)
    pdf.set_text_color(*_MUTED)
    pdf.set_xy(10, 200)
    pdf.cell(277, 4, "Generated by 0DTE Alpha", align="C")

    # ═══════════════════════════════════════════════════════════════════
    # PAGE 2+: Trade log table
    # ═══════════════════════════════════════════════════════════════════
    pdf.add_page()
    _dark_bg(pdf)

    # Section header
    pdf.set_fill_color(*_ACCENT)
    pdf.rect(8, 6, 281, 8, "F")
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(255, 255, 255)
    pdf.set_xy(12, 7)
    pdf.cell(0, 6, f"TRADE LOG   |   {total} trades   |   {trade_date.strftime('%B %d, %Y')}")

    # Column setup
    cw = [9, 14, 38, 14, 14, 20, 16, 18, 138]
    headers = ["#", "Time", "Setup", "Dir", "Grade", "Entry", "Result", "P&L", "Reason"]

    _draw_table_header(pdf, headers, cw, 15)
    row_y = 21

    for idx, t in enumerate(trades, 1):
        if row_y > 196:
            pdf.add_page()
            _dark_bg(pdf)
            _draw_table_header(pdf, headers, cw, 6)
            row_y = 12

        res = t["outcome_result"]
        pnl = t["outcome_pnl"]
        row_bg = _WIN_ROW if res == "WIN" else _LOSS_ROW if res == "LOSS" else _EXP_ROW

        pdf.set_fill_color(*row_bg)
        pdf.rect(8, row_y, 281, 5.2, "F")

        ts_str = t["ts"].strftime("%H:%M") if hasattr(t["ts"], "strftime") else ""
        entry = t.get("abs_es_price") or t.get("spot") or 0
        entry_str = f"{entry:.1f}" if entry else ""
        grade_str = (t["grade"] or "")[:5]
        setup_short = t["setup_name"]
        if len(setup_short) > 16:
            setup_short = setup_short[:15] + "."
        why = _sanitize(_build_why(t))
        if len(why) > 72:
            why = why[:69] + "..."

        pdf.set_xy(8, row_y + 0.3)

        # Standard columns
        pdf.set_font("Helvetica", "", 7.5)
        pdf.set_text_color(*_TXT)
        pdf.cell(cw[0], 4.5, str(idx), align="C")
        pdf.cell(cw[1], 4.5, ts_str, align="C")
        pdf.set_font("Helvetica", "B", 7.5)
        pdf.cell(cw[2], 4.5, setup_short)
        pdf.set_font("Helvetica", "", 7.5)
        pdf.cell(cw[3], 4.5, t["direction"][:5], align="C")
        pdf.cell(cw[4], 4.5, grade_str, align="C")
        pdf.set_text_color(*_MUTED)
        pdf.cell(cw[5], 4.5, entry_str, align="C")

        # Result + P&L colored
        rc = _WIN_C if res == "WIN" else _LOSS_C if res == "LOSS" else _EXP_C
        pdf.set_text_color(*rc)
        pdf.set_font("Helvetica", "B", 7.5)
        pdf.cell(cw[6], 4.5, res[:3], align="C")
        pdf.cell(cw[7], 4.5, f"{pnl:+.1f}", align="C")

        # Reason
        pdf.set_font("Helvetica", "", 6.5)
        pdf.set_text_color(*_MUTED)
        pdf.cell(cw[8], 4.5, why)

        row_y += 5.5

    # Footer
    pdf.set_font("Helvetica", "I", 6)
    pdf.set_text_color(*_MUTED)
    pdf.set_xy(10, 200)
    pdf.cell(277, 4, "Generated by 0DTE Alpha", align="C")

    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    pdf.output(tmp.name)
    tmp.close()
    return tmp.name


# ── telegram sender ──────────────────────────────────────────────────────

def send_telegram_pdf(pdf_path, caption, bot_token, chat_id):
    """Send PDF to Telegram via sendDocument API."""
    if not bot_token or not chat_id:
        print("[eod-pdf] no Telegram credentials, skipping send", flush=True)
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(pdf_path, "rb") as f:
        resp = requests.post(url, data={"chat_id": chat_id, "caption": caption},
                             files={"document": ("daily_report.pdf", f, "application/pdf")},
                             timeout=30)
    if resp.status_code == 200:
        print(f"[eod-pdf] sent to Telegram", flush=True)
        return True
    else:
        print(f"[eod-pdf] Telegram error {resp.status_code}: {resp.text[:200]}", flush=True)
        return False


# ── trades-on-chart picture ──────────────────────────────────────────────

# Setup name → short label for chart markers
_SETUP_ABBREV = {
    "DD Exhaustion": "DD", "ES Absorption": "CVD", "GEX Long": "GEX",
    "AG Short": "AG", "BofA Scalp": "BOFA", "Paradigm Reversal": "PAR",
    "Skew Charm": "SKW",
}

# Setup name → marker symbol (so you can tell them apart even in grayscale)
_SETUP_MARKER = {
    "DD Exhaustion": "o", "ES Absorption": "s", "GEX Long": "^",
    "AG Short": "v", "BofA Scalp": "D", "Paradigm Reversal": "P",
    "Skew Charm": "*",
}


def _query_range_bars(engine, trade_date):
    """Query ES 5-pt range bars for a given date (Rithmic preferred, fallback live)."""
    date_str = trade_date.isoformat()
    with engine.connect() as conn:
        for source in ("rithmic", "live"):
            rows = conn.execute(text("""
                SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
                       bar_volume, bar_delta, cvd_close, ts_start
                FROM es_range_bars
                WHERE trade_date = :d AND source = :src
                ORDER BY bar_idx
            """), {"d": date_str, "src": source}).fetchall()
            if rows:
                bars = []
                for r in rows:
                    ts_start = r[8]
                    # convert to ET (UTC-5)
                    if ts_start:
                        try:
                            if hasattr(ts_start, "utcoffset") and ts_start.utcoffset():
                                from datetime import timezone
                                dt_et = ts_start.astimezone(timezone(timedelta(hours=-4)))
                            else:
                                dt_et = ts_start - timedelta(hours=5)
                        except Exception:
                            dt_et = ts_start
                    else:
                        dt_et = None
                    bars.append({
                        "idx": r[0], "open": float(r[1]), "high": float(r[2]),
                        "low": float(r[3]), "close": float(r[4]),
                        "volume": int(r[5] or 0), "delta": int(r[6] or 0),
                        "cvd": int(r[7] or 0), "dt_et": dt_et,
                        "ts_raw": ts_start,  # original UTC for timestamp matching
                    })
                # filter RTH (9:30 - 16:00 ET)
                rth = [b for b in bars if b["dt_et"] and
                       (b["dt_et"].hour > 9 or (b["dt_et"].hour == 9 and b["dt_et"].minute >= 30)) and
                       b["dt_et"].hour < 16]
                return rth if rth else bars
    return []


def _find_nearest_bar(bars, bar_idx_to_x, trade_ts):
    """Find the x-position on the chart for a trade by timestamp."""
    if not bars or not trade_ts:
        return None
    # Compare in UTC — both ts_raw (bar) and trade_ts (setup_log) are TIMESTAMPTZ
    best_x, best_diff = None, None
    for i, b in enumerate(bars):
        ts_raw = b.get("ts_raw")
        if not ts_raw:
            continue
        try:
            # Use timezone-aware subtraction if both have tzinfo, else strip both
            if hasattr(ts_raw, "utcoffset") and ts_raw.utcoffset() is not None and \
               hasattr(trade_ts, "utcoffset") and trade_ts.utcoffset() is not None:
                diff = abs((trade_ts - ts_raw).total_seconds())
            else:
                b_naive = ts_raw.replace(tzinfo=None) if hasattr(ts_raw, "replace") else ts_raw
                t_naive = trade_ts.replace(tzinfo=None) if hasattr(trade_ts, "replace") else trade_ts
                diff = abs((t_naive - b_naive).total_seconds())
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best_x = i
        except Exception:
            continue
    return best_x


def generate_trades_chart(engine, trade_date):
    """Generate ES range bar chart with ALL setup entries marked. Returns PNG path or None."""
    trades = _query_trades(engine, trade_date)
    bars = _query_range_bars(engine, trade_date)

    if not trades:
        print(f"[eod-chart] no trades for {trade_date}, skipping chart", flush=True)
        return None
    if not bars:
        print(f"[eod-chart] no range bars for {trade_date}, skipping chart", flush=True)
        return None

    bar_idx_to_x = {b["idx"]: i for i, b in enumerate(bars)}

    # ── pre-compute stats ──
    wins = sum(1 for t in trades if t["outcome_result"] == "WIN")
    losses = sum(1 for t in trades if t["outcome_result"] == "LOSS")
    expired = len(trades) - wins - losses
    net = sum(t["outcome_pnl"] for t in trades)
    wr = f"{wins / len(trades) * 100:.0f}" if trades else "0"

    setup_stats = {}
    for t in trades:
        n = t["setup_name"]
        if n not in setup_stats:
            setup_stats[n] = {"w": 0, "l": 0, "e": 0, "pnl": 0.0}
        s = setup_stats[n]
        s["pnl"] += t["outcome_pnl"]
        if t["outcome_result"] == "WIN":
            s["w"] += 1
        elif t["outcome_result"] == "LOSS":
            s["l"] += 1
        else:
            s["e"] += 1

    # ── figure layout: price chart + PnL curve + stats sidebar ──
    fig = plt.figure(figsize=(32, 16))
    fig.patch.set_facecolor("#0f0f1a")

    # GridSpec: left 80% = charts, right 20% = stats panel
    gs = fig.add_gridspec(2, 2, width_ratios=[4, 1], height_ratios=[4, 1.2],
                          hspace=0.06, wspace=0.02,
                          left=0.03, right=0.97, top=0.92, bottom=0.05)
    ax1 = fig.add_subplot(gs[0, 0])   # price
    ax2 = fig.add_subplot(gs[1, 0], sharex=ax1)   # PnL curve
    ax3 = fig.add_subplot(gs[:, 1])   # stats sidebar

    for ax in [ax1, ax2]:
        ax.set_facecolor("#141428")
        ax.tick_params(colors="#b0b0c0", labelsize=8)
        ax.grid(True, alpha=0.12, color="#3a3a5a", linestyle="-")
        for spine in ax.spines.values():
            spine.set_color("#2a2a4a")
    ax3.set_facecolor("#141428")
    ax3.axis("off")

    # ── TITLE ──
    pnl_color = "#00e676" if net >= 0 else "#ff1744"
    fig.suptitle(f"0DTE ALPHA  |  {trade_date.strftime('%B %d, %Y')}  |  "
                 f"{len(trades)} Trades  |  {wr}% WR  |  Net: {net:+.1f} pts",
                 fontsize=16, fontweight="bold", color="#e8e8f0", y=0.97)

    # ── PRICE PANEL (candlesticks) ──
    for i, b in enumerate(bars):
        bullish = b["close"] >= b["open"]
        color = "#26a69a" if bullish else "#ef5350"
        body_bottom = min(b["open"], b["close"])
        body_height = max(abs(b["close"] - b["open"]), 0.25)
        ax1.plot([i, i], [b["low"], b["high"]], color=color, linewidth=0.6, alpha=0.7)
        ax1.bar(i, body_height, bottom=body_bottom, width=0.55, color=color, alpha=0.8, edgecolor=color)

    # ── MARK ALL TRADES ──
    price_range = max(b["high"] for b in bars) - min(b["low"] for b in bars)
    offset = price_range * 0.025  # 2.5% of price range for arrow offset
    label_offset = price_range * 0.045

    for t in trades:
        name = t["setup_name"]
        abbrev = _SETUP_ABBREV.get(name, name[:3].upper())
        direction = t["direction"]
        result = t["outcome_result"]
        pnl = t["outcome_pnl"]

        xi = _find_nearest_bar(bars, bar_idx_to_x, t["ts"])
        if xi is None:
            continue

        bar = bars[xi]
        is_long = direction.lower() in ("long", "bullish")

        # Color by result
        if result == "WIN":
            color, edge = "#00e676", "#00c853"
        elif result == "LOSS":
            color, edge = "#ff5252", "#d50000"
        else:
            color, edge = "#ffa726", "#ff8f00"

        # Arrow marker: triangle up for LONG, down for SHORT
        if is_long:
            marker = "^"
            arrow_y = bar["low"] - offset
            label_y = arrow_y - label_offset
            va = "top"
        else:
            marker = "v"
            arrow_y = bar["high"] + offset
            label_y = arrow_y + label_offset
            va = "bottom"

        ax1.scatter(xi, arrow_y, marker=marker, s=150, color=color,
                    edgecolors=edge, linewidths=1.2, zorder=10)

        # Compact label: "DD +16.1" or "ABS -12.0"
        label = f"{abbrev} {pnl:+.1f}"
        ax1.annotate(label, (xi, label_y), fontsize=5.5, fontweight="bold",
                     color=color, ha="center", va=va, zorder=11,
                     bbox=dict(boxstyle="round,pad=0.15", facecolor="#0f0f1a",
                               edgecolor=color, alpha=0.88, linewidth=0.4))

    ax1.set_ylabel("ES Price", color="#b0b0c0", fontsize=10, fontweight="bold")
    plt.setp(ax1.get_xticklabels(), visible=False)

    # ── CUMULATIVE PnL PANEL ──
    cum = 0.0
    pnl_xs, pnl_ys, pnl_colors = [], [], []
    for t in trades:
        cum += t["outcome_pnl"]
        xi = _find_nearest_bar(bars, bar_idx_to_x, t["ts"])
        if xi is not None:
            pnl_xs.append(xi)
            pnl_ys.append(cum)
            res = t["outcome_result"]
            pnl_colors.append("#00e676" if res == "WIN" else "#ff5252" if res == "LOSS" else "#ffa726")

    if pnl_xs:
        ax2.plot(pnl_xs, pnl_ys, color="#7c7cf7", linewidth=2, zorder=1)
        ax2.scatter(pnl_xs, pnl_ys, c=pnl_colors, s=35, zorder=2,
                    edgecolors="#1a1a2e", linewidths=0.6)
        ax2.axhline(y=0, color="#555", linewidth=0.6, linestyle="--", alpha=0.5)
        # Green fill above 0, red fill below 0
        pnl_arr = np.array(pnl_ys)
        xs_arr = np.array(pnl_xs)
        ax2.fill_between(xs_arr, pnl_arr, where=pnl_arr >= 0, alpha=0.15, color="#00e676", interpolate=True)
        ax2.fill_between(xs_arr, pnl_arr, where=pnl_arr < 0, alpha=0.15, color="#ff5252", interpolate=True)
        # End label
        ax2.annotate(f"{cum:+.1f}", (pnl_xs[-1], pnl_ys[-1]),
                     fontsize=9, fontweight="bold", color=pnl_color,
                     xytext=(8, 0), textcoords="offset points", va="center")
    ax2.set_ylabel("Cum. P&L", color="#b0b0c0", fontsize=10, fontweight="bold")

    # ── X-axis time labels ──
    tick_positions, tick_labels_list = [], []
    for i, b in enumerate(bars):
        if i % 20 == 0 and b["dt_et"]:
            tick_positions.append(i)
            tick_labels_list.append(b["dt_et"].strftime("%H:%M"))
    ax2.set_xticks(tick_positions)
    ax2.set_xticklabels(tick_labels_list, fontsize=8, color="#b0b0c0")
    ax2.set_xlabel("Time (ET)", color="#b0b0c0", fontsize=10, fontweight="bold")

    # ── STATS SIDEBAR ──
    y = 0.95
    line_h = 0.035

    # Title
    ax3.text(0.5, y, "DAILY SUMMARY", fontsize=13, fontweight="bold",
             color="#e8e8f0", ha="center", va="top", transform=ax3.transAxes)
    y -= 0.06

    # Headline stats
    ax3.text(0.5, y, f"{len(trades)} trades", fontsize=18, fontweight="bold",
             color="#e8e8f0", ha="center", va="top", transform=ax3.transAxes)
    y -= 0.055
    ax3.text(0.5, y, f"{net:+.1f} pts", fontsize=22, fontweight="bold",
             color=pnl_color, ha="center", va="top", transform=ax3.transAxes)
    y -= 0.06

    y -= 0.01
    ax3.text(0.5, y, f"{wins}W  /  {losses}L  /  {expired}E   ({wr}%)",
             fontsize=10, color="#b0b0c0", ha="center", va="top", transform=ax3.transAxes)
    y -= 0.065

    # Divider
    ax3.plot([0.1, 0.9], [y + 0.015, y + 0.015], color="#3a3a5a", linewidth=0.5,
             transform=ax3.transAxes, clip_on=False)

    # Per-setup breakdown
    ax3.text(0.5, y, "PER SETUP", fontsize=10, fontweight="bold",
             color="#e8e8f0", ha="center", va="top", transform=ax3.transAxes)
    y -= 0.045

    sorted_setups = sorted(setup_stats.items(), key=lambda x: -x[1]["pnl"])
    for sname, s in sorted_setups:
        abbr = _SETUP_ABBREV.get(sname, sname[:4])
        cnt = s["w"] + s["l"] + s["e"]
        spnl = s["pnl"]
        spnl_color = "#00e676" if spnl >= 0 else "#ff5252"
        wle = f"{s['w']}W/{s['l']}L"
        if s["e"]:
            wle += f"/{s['e']}E"

        ax3.text(0.05, y, f"{abbr}", fontsize=9, fontweight="bold",
                 color="#e8e8f0", ha="left", va="top", transform=ax3.transAxes, family="monospace")
        ax3.text(0.30, y, f"{cnt}t", fontsize=9,
                 color="#888", ha="left", va="top", transform=ax3.transAxes)
        ax3.text(0.48, y, wle, fontsize=9,
                 color="#b0b0c0", ha="left", va="top", transform=ax3.transAxes)
        ax3.text(0.95, y, f"{spnl:+.1f}", fontsize=9, fontweight="bold",
                 color=spnl_color, ha="right", va="top", transform=ax3.transAxes)
        y -= line_h

    # Legend at bottom of sidebar
    y -= 0.03
    ax3.plot([0.1, 0.9], [y + 0.015, y + 0.015], color="#3a3a5a", linewidth=0.5,
             transform=ax3.transAxes, clip_on=False)
    ax3.text(0.5, y, "LEGEND", fontsize=9, fontweight="bold",
             color="#e8e8f0", ha="center", va="top", transform=ax3.transAxes)
    y -= 0.035
    legend_items = [
        ("\u25b2  Long entry", "#b0b0c0"), ("\u25bc  Short entry", "#b0b0c0"),
        ("\u25cf  WIN", "#00e676"), ("\u25cf  LOSS", "#ff5252"), ("\u25cf  EXPIRED", "#ffa726"),
    ]
    for txt, c in legend_items:
        ax3.text(0.08, y, txt, fontsize=8, color=c, ha="left", va="top",
                 transform=ax3.transAxes)
        y -= 0.028

    # Save
    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
    fig.savefig(tmp.name, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    tmp.close()
    print(f"[eod-chart] generated chart: {len(trades)} trades, {len(bars)} bars, {tmp.name}", flush=True)
    return tmp.name


def send_telegram_photo(photo_path, caption, bot_token, chat_id):
    """Send PNG as document to Telegram (preserves full resolution, zoomable)."""
    if not bot_token or not chat_id:
        print("[eod-chart] no Telegram credentials, skipping send", flush=True)
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
    with open(photo_path, "rb") as f:
        resp = requests.post(url, data={"chat_id": chat_id, "caption": caption},
                             files={"document": ("daily_chart.png", f, "image/png")},
                             timeout=30)
    if resp.status_code == 200:
        print(f"[eod-chart] chart sent to Telegram (document)", flush=True)
        return True
    else:
        print(f"[eod-chart] Telegram error {resp.status_code}: {resp.text[:200]}", flush=True)
        return False


# ── main entry point ─────────────────────────────────────────────────────

def generate_eod_pdf(engine, trade_date):
    """Generate EOD PDF report for given date. Returns temp file path or None."""
    trades = _query_trades(engine, trade_date)
    if not trades:
        print(f"[eod-pdf] no trades for {trade_date}, skipping PDF", flush=True)
        return None

    chart_png = _generate_pnl_chart(trades, trade_date)
    pdf_path = _build_pdf(trades, chart_png, trade_date)
    print(f"[eod-pdf] generated PDF: {len(trades)} trades, {pdf_path}", flush=True)
    return pdf_path
