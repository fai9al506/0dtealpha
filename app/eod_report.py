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

_COL_WIDTHS = [10, 16, 42, 16, 14, 18, 16, 20]  # total ~152mm (landscape usable ~277mm)
_COL_WIDTHS_WIDE = [10, 16, 42, 16, 14, 18, 16, 20]

_WIN_BG = (220, 252, 231)
_LOSS_BG = (254, 226, 226)
_EXP_BG = (243, 244, 246)


def _sanitize(text):
    """Replace non-latin-1 chars so fpdf2 Helvetica doesn't crash."""
    return text.encode("latin-1", errors="replace").decode("latin-1")


def _build_pdf(trades, chart_png, trade_date):
    """Assemble landscape A4 PDF with header, chart, summary, trade table."""
    pdf = FPDF(orientation="L", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    # ── header ──
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 10, f"0DTE ALPHA - DAILY REPORT - {trade_date.strftime('%B %d, %Y')}", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # ── chart ──
    if chart_png:
        tmp_chart = tempfile.NamedTemporaryFile(suffix=".png", delete=False)
        tmp_chart.write(chart_png)
        tmp_chart.close()
        try:
            pdf.image(tmp_chart.name, x=30, w=230)
        finally:
            try:
                os.unlink(tmp_chart.name)
            except Exception:
                pass
    pdf.ln(4)

    # ── daily summary ──
    wins = sum(1 for t in trades if t["outcome_result"] == "WIN")
    losses = sum(1 for t in trades if t["outcome_result"] == "LOSS")
    expired = sum(1 for t in trades if t["outcome_result"] not in ("WIN", "LOSS"))
    total = len(trades)
    net_pnl = sum(t["outcome_pnl"] for t in trades)
    wr = f"{wins / total * 100:.0f}%" if total else "0%"

    pdf.set_font("Helvetica", "B", 11)
    pdf.set_fill_color(230, 230, 250)
    pdf.cell(0, 7, "  DAILY SUMMARY", fill=True, new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6,
             f"  Trades: {total}  |  Wins: {wins}  |  Losses: {losses}  |  Expired: {expired}"
             f"  |  Win Rate: {wr}  |  Net P&L: {net_pnl:+.1f} pts",
             new_x="LMARGIN", new_y="NEXT")

    # per-setup breakdown
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

    sorted_setups = sorted(setup_stats.items(), key=lambda x: -x[1]["count"])
    pdf.set_font("Helvetica", "", 9)
    for name, s in sorted_setups:
        wle = f"{s['w']}W/{s['l']}L"
        if s["e"]:
            wle += f"/{s['e']}E"
        trades_word = "trade" if s["count"] == 1 else "trades"
        pdf.cell(0, 5,
                 f"    {name:<22s} {s['count']:>2} {trades_word}  {s['pnl']:>+7.1f} pts  ({wle})",
                 new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # ── trade table ──
    pdf.set_font("Helvetica", "B", 11)
    pdf.set_fill_color(230, 230, 250)
    pdf.cell(0, 7, "  TRADE LOG", fill=True, new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)

    # header row
    headers = ["#", "Time", "Setup", "Dir", "Grade", "Entry", "Result", "P&L"]
    cw = [10, 18, 48, 18, 16, 22, 18, 22]
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_fill_color(200, 200, 220)
    for i, h in enumerate(headers):
        pdf.cell(cw[i], 5, h, border=1, fill=True, align="C")
    pdf.ln()

    # data rows
    for idx, t in enumerate(trades, 1):
        res = t["outcome_result"]
        if res == "WIN":
            pdf.set_fill_color(*_WIN_BG)
        elif res == "LOSS":
            pdf.set_fill_color(*_LOSS_BG)
        else:
            pdf.set_fill_color(*_EXP_BG)

        ts_str = t["ts"].strftime("%H:%M") if hasattr(t["ts"], "strftime") else ""
        entry = t.get("abs_es_price") or t.get("spot") or 0
        entry_str = f"{entry:.1f}" if entry else ""
        pnl_str = f"{t['outcome_pnl']:+.1f}"
        grade_str = (t["grade"] or "")[:5]
        setup_short = t["setup_name"]
        if len(setup_short) > 18:
            setup_short = setup_short[:17] + "."

        # Row 1: main data
        pdf.set_font("Helvetica", "", 8)
        vals = [str(idx), ts_str, setup_short, t["direction"][:5], grade_str,
                entry_str, res[:4], pnl_str]
        for i, v in enumerate(vals):
            pdf.cell(cw[i], 5, v, border="LR", fill=True, align="C")
        pdf.ln()

        # Row 2: stop/target + why
        stop = t.get("outcome_stop_level")
        tgt = t.get("outcome_target_level")
        stop_str = f"Stop: {stop:.0f}" if stop else "Stop: --"
        tgt_str = f"Tgt: {tgt:.0f}" if tgt else "Tgt: trail"
        why = _build_why(t)
        line2 = f"  {stop_str} | {tgt_str} | {why}"
        if len(line2) > 110:
            line2 = line2[:107] + "..."
        line2 = _sanitize(line2)

        pdf.set_font("Helvetica", "I", 7)
        total_w = sum(cw)
        pdf.cell(total_w, 4, line2, border="LRB", fill=True)
        pdf.ln()

    # footer
    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 8)
    pdf.cell(0, 5, "Generated by 0DTE Alpha | github.com/0dtealpha", align="C")

    # save to temp file
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
    "DD Exhaustion": "DD", "ES Absorption": "ABS", "GEX Long": "GEX",
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
    """Send PNG photo to Telegram via sendPhoto API."""
    if not bot_token or not chat_id:
        print("[eod-chart] no Telegram credentials, skipping send", flush=True)
        return False
    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    with open(photo_path, "rb") as f:
        resp = requests.post(url, data={"chat_id": chat_id, "caption": caption},
                             files={"photo": ("daily_chart.png", f, "image/png")},
                             timeout=30)
    if resp.status_code == 200:
        print(f"[eod-chart] photo sent to Telegram", flush=True)
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
