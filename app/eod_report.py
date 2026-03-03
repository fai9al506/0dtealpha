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
import matplotlib.dates as mdates
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
