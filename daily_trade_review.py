"""
Daily Trade Review PDF — 0DTE Alpha
Generates a detailed per-trade review PDF with Discord sentiment comparison.
Run: python daily_trade_review.py [YYYY-MM-DD] [--discord PATH]
"""

import json, os, sys, re, textwrap, tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import requests
from fpdf import FPDF
from sqlalchemy import create_engine

# ── CONFIG ───────────────────────────────────────────────────────────────

API_URL = "https://0dtealpha.com"
API_KEY = "qigYBFsjqZE1DaSx1K4NE0JJ0bLtVpl3fIroWt9lPvM"
DEFAULT_DISCORD = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output"

# V9-SC filter rules
def v9sc_pass(t):
    """Check if a trade passes V9-SC filter."""
    direction = t.get("direction", "")
    setup = t.get("setup_name", "")
    align = t.get("greek_alignment") or 0
    vix = t.get("vix") or 99
    overvix = t.get("overvix")

    is_long = direction in ("long", "bullish")
    is_short = direction in ("short", "bearish")

    if is_long:
        if align < 2:
            return False, f"Align {align} < 2"
        if setup == "Skew Charm":
            return True, "SC exempt"
        if vix <= 22:
            return True, f"VIX {vix:.1f} <= 22"
        if overvix is not None and overvix >= 2:
            return True, f"Overvix {overvix:+.1f} >= +2"
        return False, f"VIX {vix:.1f} > 22, no override"

    if is_short:
        if setup == "Skew Charm":
            return True, "SC whitelist"
        if setup == "AG Short":
            return True, "AG whitelist"
        if setup == "DD Exhaustion":
            if align != 0:
                return True, f"DD align={align}"
            return False, "DD align=0 blocked"
        return False, f"{setup} short blocked"

    return False, "Unknown direction"


# ── COLORS (dark theme matching eod_report.py) ──────────────────────────

def _latin1_safe(text):
    """Make text safe for Helvetica (latin-1 only)."""
    return text.encode("latin-1", "replace").decode("latin-1")


BG       = (20, 20, 40)
CARD     = (30, 30, 55)
ACCENT   = (99, 102, 241)
TEXT     = (230, 230, 240)
MUTED    = (140, 140, 165)
WIN_CLR  = (34, 197, 94)
LOSS_CLR = (239, 68, 68)
EXP_CLR  = (251, 191, 36)
DISCORD_CLR = (88, 101, 242)  # Discord blurple


# ── DATA FETCH ───────────────────────────────────────────────────────────

def fetch_signals(trade_date):
    """Fetch all signals for a date from the Railway API."""
    headers = {"Authorization": f"Bearer {API_KEY}"}
    all_signals = []
    all_outcomes = {}
    since_id = 0
    for _ in range(20):  # max 20 pages
        r = requests.get(f"{API_URL}/api/eval/signals?since_id={since_id}",
                         headers=headers, timeout=15)
        if r.status_code != 200:
            break
        data = r.json()
        signals = data.get("signals", [])
        if not signals:
            break
        all_signals.extend(signals)
        # Merge outcomes (keyed by id)
        for o in data.get("outcomes", []):
            all_outcomes[o["id"]] = o
        since_id = max(s["id"] for s in signals)

    # Merge outcome_pnl into signals
    for s in all_signals:
        o = all_outcomes.get(s["id"])
        if o:
            s["outcome_pnl"] = o.get("outcome_pnl")
            if not s.get("outcome_result"):
                s["outcome_result"] = o.get("outcome_result")

    # Filter to target date
    date_str = trade_date.strftime("%Y-%m-%d")
    day_signals = [s for s in all_signals if date_str in s.get("ts", "")]
    return day_signals


def load_discord(trade_date, discord_path=None):
    """Load Discord messages for the given date."""
    if discord_path and os.path.isfile(discord_path):
        fpath = discord_path
    else:
        # Find most recent export in default folder
        folder = discord_path or DEFAULT_DISCORD
        if os.path.isdir(folder):
            jsons = sorted(Path(folder).glob("*volland*daytrading*.json"),
                           key=os.path.getmtime, reverse=True)
            if jsons:
                fpath = str(jsons[0])
            else:
                return []
        else:
            return []

    with open(fpath, encoding="utf-8") as f:
        data = json.load(f)

    date_str = trade_date.strftime("%Y-%m-%d")
    msgs = []
    for m in data.get("messages", []):
        ts = m.get("timestamp", "")
        if date_str not in ts:
            continue
        content = m.get("content", "").strip()
        if not content:
            continue
        author = m.get("author", {}).get("nickname") or m.get("author", {}).get("name", "?")
        # Parse timestamp
        msgs.append({
            "ts": ts[:19],
            "author": author,
            "content": content,
            "roles": [r.get("name", "") for r in m.get("author", {}).get("roles", [])]
        })
    return msgs


def find_discord_context(msgs, trade_ts, window_min=10):
    """Find Discord messages within ±window_min of a trade timestamp."""
    # Parse trade time (UTC)
    try:
        t_trade = datetime.fromisoformat(trade_ts.replace("+00:00", "+00:00"))
        if t_trade.tzinfo is None:
            t_trade = t_trade.replace(tzinfo=timezone.utc)
    except:
        return [], "unknown"

    relevant = []
    for m in msgs:
        try:
            t_msg = datetime.fromisoformat(m["ts"])
            if t_msg.tzinfo is None:
                # Discord export might be local time (UTC+3 Saudi)
                from zoneinfo import ZoneInfo
                t_msg = t_msg.replace(tzinfo=ZoneInfo("Asia/Riyadh"))
            # Convert to UTC for comparison
            t_msg_utc = t_msg.astimezone(timezone.utc)
            diff_min = (t_msg_utc - t_trade).total_seconds() / 60
            if -window_min <= diff_min <= window_min:
                relevant.append({
                    **m,
                    "diff_min": diff_min,
                    "is_notable": any(r in ("Moderator", "Volland +") for r in m.get("roles", []))
                })
        except:
            continue

    # Determine community sentiment
    bullish_words = ["long", "buy", "bull", "calls", "rally", "up", "support", "bounce", "green"]
    bearish_words = ["short", "sell", "bear", "puts", "drop", "down", "resistance", "fade", "red",
                     "rug pull", "rugpull", "cooked", "breakdown"]
    bull_count = 0
    bear_count = 0
    for m in relevant:
        text_lower = m["content"].lower()
        for w in bullish_words:
            if w in text_lower:
                bull_count += 1
                break
        for w in bearish_words:
            if w in text_lower:
                bear_count += 1
                break

    if bull_count > bear_count * 1.5:
        sentiment = "BULLISH"
    elif bear_count > bull_count * 1.5:
        sentiment = "BEARISH"
    elif bull_count == 0 and bear_count == 0:
        sentiment = "NEUTRAL/QUIET"
    else:
        sentiment = "MIXED"

    return relevant, sentiment


# ── PDF BUILDER ──────────────────────────────────────────────────────────

class TradePDF(FPDF):
    def __init__(self):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.set_auto_page_break(auto=True, margin=15)

    def _bg(self):
        self.set_fill_color(*BG)
        self.rect(0, 0, 210, 297, "F")

    def header(self):
        self._bg()

    def _set_text(self, color=TEXT, size=9, bold=False):
        self.set_text_color(*color)
        self.set_font("Helvetica", "B" if bold else "", size)

    def cell(self, w=None, h=None, text="", *args, **kwargs):
        """Override to sanitize text for latin-1."""
        text = _latin1_safe(str(text))
        return super().cell(w, h, text, *args, **kwargs)

    def _outcome_color(self, result):
        if result == "WIN":
            return WIN_CLR
        elif result == "LOSS":
            return LOSS_CLR
        return EXP_CLR


def _load_range_bars(engine, trade_date):
    """Load ES 5-pt range bars with OHLC from Railway DB."""
    from sqlalchemy import text as sa_text
    date_str = str(trade_date)
    for source in ("rithmic", "live"):
        df = pd.read_sql(sa_text(
            "SELECT bar_idx, bar_open, bar_high, bar_low, bar_close, "
            "bar_volume, ts_start FROM es_range_bars "
            "WHERE trade_date = :d AND source = :src ORDER BY bar_idx"
        ), engine, params={"d": date_str, "src": source})
        if len(df) > 0:
            df['ts_start'] = pd.to_datetime(df['ts_start'], utc=True)
            for c in ['bar_open', 'bar_high', 'bar_low', 'bar_close']:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            # Filter RTH (13:30-20:00 UTC = 9:30-16:00 ET)
            rth = df[(df['ts_start'].dt.hour > 13) |
                     ((df['ts_start'].dt.hour == 13) & (df['ts_start'].dt.minute >= 30))]
            rth = rth[rth['ts_start'].dt.hour < 20]
            return rth if len(rth) > 10 else df
    return pd.DataFrame()


def _load_spx_prices(engine, trade_date):
    """Load SPX spot prices from chain_snapshots for the given date."""
    from sqlalchemy import text as sa_text
    today_start = f"{trade_date}T00:00:00"
    tomorrow_start = f"{trade_date + timedelta(days=1)}T00:00:00"
    df = pd.read_sql(sa_text(
        "SELECT ts, spot::float FROM chain_snapshots "
        "WHERE ts >= :s AND ts < :e AND spot IS NOT NULL "
        "ORDER BY ts ASC"
    ), engine, params={"s": today_start, "e": tomorrow_start})
    if len(df) > 0:
        df['ts'] = pd.to_datetime(df['ts'], utc=True)
        df['spot'] = pd.to_numeric(df['spot'], errors='coerce')
        df = df.dropna(subset=['spot'])
    return df


def _generate_price_chart(signal, spx_df, range_bars_df=None):
    """Generate ES candlestick chart with entry/exit/levels — same style as portal."""
    spot = signal.get("spot")
    if spot is None:
        return None

    stop = signal.get("stop_level")
    target = signal.get("target_level") or signal.get("target")
    lis = signal.get("lis")
    direction = signal.get("direction", "")
    is_long = direction in ("long", "bullish")
    result = signal.get("outcome_result", "")
    pnl = signal.get("outcome_pnl") or 0

    try:
        trade_ts = pd.to_datetime(signal["ts"], utc=True)
    except:
        return None

    # ── Use range bars if available (candlestick chart) ──────────
    use_candles = range_bars_df is not None and len(range_bars_df) > 0

    if use_candles:
        # Window: ~20 bars before and ~40 bars after entry
        entry_idx = (range_bars_df['ts_start'] - trade_ts).abs().idxmin()
        entry_pos = range_bars_df.index.get_loc(entry_idx)
        start = max(0, entry_pos - 20)
        end = min(len(range_bars_df), entry_pos + 40)
        bars = range_bars_df.iloc[start:end].copy().reset_index(drop=True)
        entry_bar = entry_pos - start

        if len(bars) < 5:
            use_candles = False

    if not use_candles:
        # Fallback to SPX line chart
        if spx_df is None or len(spx_df) == 0:
            return None
        t_start = trade_ts - timedelta(minutes=20)
        t_end = trade_ts + timedelta(minutes=60)
        window = spx_df[(spx_df['ts'] >= t_start) & (spx_df['ts'] <= t_end)]
        if len(window) < 3:
            return None

    # ── Build chart ──────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(8, 3.2), dpi=180)
    bg = '#141428'
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    if use_candles:
        # ── Candlestick bars (same style as eod_report.py) ──────
        for i, (_, b) in enumerate(bars.iterrows()):
            bullish = b['bar_close'] >= b['bar_open']
            color = '#26a69a' if bullish else '#ef5350'
            body_bot = min(b['bar_open'], b['bar_close'])
            body_h = max(abs(b['bar_close'] - b['bar_open']), 0.25)
            # Wick
            ax.plot([i, i], [b['bar_low'], b['bar_high']],
                    color=color, linewidth=0.7, alpha=0.7)
            # Body
            ax.bar(i, body_h, bottom=body_bot, width=0.55,
                   color=color, alpha=0.85, edgecolor=color)

        # ── Map SPX levels to ES price space ─────────────────────
        # Use abs_es_price if available (exact ES at signal time),
        # otherwise use the entry bar's OHLC midpoint
        abs_es = signal.get("abs_es_price")
        if abs_es:
            es_entry = float(abs_es)
        else:
            eb = bars.iloc[entry_bar]
            es_entry = (eb['bar_open'] + eb['bar_close']) / 2

        # All SPX levels are converted to ES by: es_level = es_entry + (spx_level - spot)
        def to_es(spx_price):
            return es_entry + (spx_price - spot)

        entry_x = entry_bar

        # Horizontal levels
        if stop:
            es_stop = to_es(stop)
            ax.axhline(y=es_stop, color='#ef4444', linewidth=1.2, linestyle='--', alpha=0.7)
            ax.text(0.5, es_stop, f' STOP {stop:.0f}', va='center',
                    fontsize=7, color='#ef4444', weight='bold')
        if target:
            es_target = to_es(target)
            ax.axhline(y=es_target, color='#22c55e', linewidth=1.2, linestyle='--', alpha=0.7)
            ax.text(0.5, es_target, f' TARGET {target:.0f}', va='center',
                    fontsize=7, color='#22c55e', weight='bold')
        if lis:
            es_lis = to_es(lis)
            ax.axhline(y=es_lis, color='#6366f1', linewidth=1, linestyle=':', alpha=0.5)
            ax.text(len(bars) - 1, es_lis, f'LIS {lis:.0f} ', va='center', ha='right',
                    fontsize=6, color='#6366f1')

        # Entry marker — placed at es_entry price on the entry bar
        marker = '^' if is_long else 'v'
        entry_color = '#22c55e' if is_long else '#ef4444'
        ax.plot(entry_x, es_entry, marker=marker, markersize=14, color=entry_color,
                markeredgecolor='white', markeredgewidth=1.2, zorder=10)
        ax.annotate(f'ENTRY {spot:.0f}', xy=(entry_x, es_entry),
                    xytext=(0, 18 if is_long else -18), textcoords='offset points',
                    fontsize=8, color='white', weight='bold', ha='center', zorder=10)

        # Exit marker — es_entry +/- pnl
        if pnl != 0:
            exit_es = es_entry + pnl if is_long else es_entry - pnl
            out_color = '#22c55e' if pnl > 0 else '#ef4444'
            exit_x = None
            for j in range(entry_bar + 1, len(bars)):
                b = bars.iloc[j]
                if is_long and pnl > 0 and b['bar_high'] >= exit_es:
                    exit_x = j; break
                elif is_long and pnl < 0 and b['bar_low'] <= exit_es:
                    exit_x = j; break
                elif not is_long and pnl > 0 and b['bar_low'] <= exit_es:
                    exit_x = j; break
                elif not is_long and pnl < 0 and b['bar_high'] >= exit_es:
                    exit_x = j; break
            if exit_x is None:
                exit_x = min(len(bars) - 1, entry_bar + 20)
            ax.plot(exit_x, exit_es, marker='X', markersize=12, color=out_color,
                    markeredgecolor='white', markeredgewidth=0.8, zorder=10)
            ax.annotate(f'{result} {pnl:+.1f}', xy=(exit_x, exit_es),
                        xytext=(0, -16 if is_long else 16), textcoords='offset points',
                        fontsize=7, color=out_color, weight='bold', ha='center', zorder=10)

        # X-axis: show time labels every ~10 bars
        tick_pos = list(range(0, len(bars), max(1, len(bars) // 8)))
        tick_labels = []
        for tp in tick_pos:
            ts = bars.iloc[tp]['ts_start']
            et = ts - timedelta(hours=4)  # UTC to ET
            tick_labels.append(et.strftime('%H:%M'))
        ax.set_xticks(tick_pos)
        ax.set_xticklabels(tick_labels, fontsize=6, color='#9090b0')

    else:
        # ── SPX line chart fallback ──────────────────────────────
        times = window['ts']
        prices = window['spot']
        ax.plot(times, prices, color='#8888cc', linewidth=1.5, zorder=3)
        ax.fill_between(times, prices.min() - 2, prices, alpha=0.08, color='#6366f1')
        if stop:
            ax.axhline(y=stop, color='#ef4444', linewidth=1.2, linestyle='--', alpha=0.8)
            ax.text(times.iloc[0], stop, f'  STOP {stop:.0f}', va='center', fontsize=7, color='#ef4444', weight='bold')
        if target:
            ax.axhline(y=target, color='#22c55e', linewidth=1.2, linestyle='--', alpha=0.8)
            ax.text(times.iloc[0], target, f'  TARGET {target:.0f}', va='center', fontsize=7, color='#22c55e', weight='bold')
        marker = '^' if is_long else 'v'
        entry_color = '#22c55e' if is_long else '#ef4444'
        ax.plot(trade_ts, spot, marker=marker, markersize=14, color=entry_color,
                markeredgecolor='white', markeredgewidth=1.2, zorder=10)
        ax.annotate(f'ENTRY {spot:.0f}', xy=(trade_ts, spot),
                    xytext=(0, 18 if is_long else -18), textcoords='offset points',
                    fontsize=8, color='white', weight='bold', ha='center', zorder=10)
        ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%H:%M', tz=timezone(timedelta(hours=-4))))

    # ── Common styling ───────────────────────────────────────────
    ax.tick_params(axis='y', colors='#9090b0', labelsize=7)
    for spine in ax.spines.values():
        spine.set_color('#2a2a4a')
    ax.grid(axis='y', color='#3a3a5a', linewidth=0.3, alpha=0.4)

    plt.tight_layout(pad=0.4)
    tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
    fig.savefig(tmp.name, dpi=180, bbox_inches='tight', pad_inches=0.1)
    plt.close(fig)
    return tmp.name


def _safe(val, fmt=".1f", default="--"):
    if val is None:
        return default
    try:
        return f"{val:{fmt}}"
    except:
        return str(val)


def build_pdf(signals, discord_msgs, trade_date, spx_prices=None, range_bars=None):
    """Build the daily trade review PDF."""
    pdf = TradePDF()

    # ── PAGE 1: DAILY SUMMARY ────────────────────────────────────────────
    pdf.add_page()

    # Title
    pdf._set_text(ACCENT, 18, True)
    pdf.set_y(12)
    pdf.cell(190, 10, f"Daily Trade Review  {trade_date.strftime('%A, %B %d, %Y')}",
             align="C", new_x="LMARGIN", new_y="NEXT")

    # Subtitle
    pdf._set_text(MUTED, 9)
    pdf.cell(190, 5, "0DTE Alpha  |  V9-SC Filter  |  Setup Detector + Discord Sentiment",
             align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # KPI cards
    total_pnl = sum(s.get("outcome_pnl") or 0 for s in signals if s.get("outcome_result"))
    wins = [s for s in signals if s.get("outcome_result") == "WIN"]
    losses = [s for s in signals if s.get("outcome_result") == "LOSS"]
    expired = [s for s in signals if s.get("outcome_result") == "EXPIRED"]
    v8_passed = [s for s in signals if v9sc_pass(s)[0]]
    v8_pnl = sum(s.get("outcome_pnl") or 0 for s in v8_passed if s.get("outcome_result"))
    total_resolved = len(wins) + len(losses) + len(expired)
    wr = f"{len(wins)/total_resolved*100:.0f}%" if total_resolved else "--"

    kpis = [
        ("Signals", str(len(signals))),
        ("Wins", str(len(wins))),
        ("Losses", str(len(losses))),
        ("Expired", str(len(expired))),
        ("Win Rate", wr),
        ("Total PnL", f"{total_pnl:+.1f} pts"),
        ("V9-SC PnL", f"{v8_pnl:+.1f} pts"),
        ("V9-SC Pass", str(len(v8_passed))),
    ]

    kpi_w = 22.5
    kpi_h = 16
    x_start = 10
    kpi_y = pdf.get_y()  # capture y ONCE before the loop
    for i, (label, value) in enumerate(kpis):
        x = x_start + i * (kpi_w + 1.2)
        pdf.set_fill_color(*CARD)
        pdf.rect(x, kpi_y, kpi_w, kpi_h, "DF")
        pdf._set_text(MUTED, 6)
        pdf.set_xy(x, kpi_y + 1)
        pdf.cell(kpi_w, 4, label, align="C")
        color = TEXT
        if label == "Total PnL":
            color = WIN_CLR if total_pnl >= 0 else LOSS_CLR
        elif label == "V9-SC PnL":
            color = WIN_CLR if v8_pnl >= 0 else LOSS_CLR
        pdf._set_text(color, 10, True)
        pdf.set_xy(x, kpi_y + 6)
        pdf.cell(kpi_w, 8, value, align="C")

    pdf.set_y(kpi_y + kpi_h + 6)

    # ── SIGNAL TABLE ─────────────────────────────────────────────────────
    pdf._set_text(ACCENT, 11, True)
    pdf.cell(190, 6, "All Signals", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)

    # Table header
    cols = [
        ("ID", 11), ("Setup", 28), ("Dir", 10), ("Grade", 11), ("Score", 11),
        ("Spot", 16), ("Align", 10), ("V9-SC", 12), ("Result", 14),
        ("PnL", 14), ("VIX", 11), ("Paradigm", 22)
    ]
    # Verify total width
    # 11+28+10+11+11+16+10+12+14+14+11+22 = 170

    pdf.set_fill_color(40, 40, 70)
    x = 10
    y = pdf.get_y()
    pdf.rect(x, y, 190, 5.5, "F")
    pdf._set_text(MUTED, 6, True)
    for label, w in cols:
        pdf.set_xy(x, y)
        pdf.cell(w, 5.5, label, align="C")
        x += w

    pdf.set_y(y + 6)

    for s in signals:
        y = pdf.get_y()
        if y > 272:
            pdf.add_page()
            y = pdf.get_y()

        v9_ok, v9_reason = v9sc_pass(s)
        result = s.get("outcome_result", "")
        pnl = s.get("outcome_pnl")
        direction = s.get("direction", "")

        # Row background — alternate
        idx = signals.index(s)
        if idx % 2 == 0:
            pdf.set_fill_color(25, 25, 48)
            pdf.rect(10, y, 190, 5.2, "F")

        x = 10
        row_data = [
            (str(s["id"]), 11, TEXT),
            (s["setup_name"], 28, TEXT),
            ("UP" if direction in ("long", "bullish") else "DN", 10,
             WIN_CLR if direction in ("long", "bullish") else LOSS_CLR),
            (s.get("grade", ""), 11, TEXT),
            (_safe(s.get("score"), ".0f"), 11, MUTED),
            (_safe(s.get("spot"), ".0f"), 16, TEXT),
            (f"{s.get('greek_alignment', 0):+d}" if s.get('greek_alignment') is not None else "--", 10, TEXT),
            ("Y" if v9_ok else "N", 12, WIN_CLR if v9_ok else LOSS_CLR),
            (result, 14, pdf._outcome_color(result)),
            (f"{pnl:+.1f}" if pnl is not None else "--", 14,
             WIN_CLR if (pnl or 0) > 0 else LOSS_CLR if (pnl or 0) < 0 else MUTED),
            (_safe(s.get("vix"), ".1f"), 11, MUTED),
            (s.get("paradigm", "")[:12], 22, MUTED),
        ]

        pdf._set_text(size=6.5)
        for val, w, color in row_data:
            pdf.set_xy(x, y)
            pdf.set_text_color(*color)
            pdf.cell(w, 5.2, val, align="C")
            x += w

        pdf.set_y(y + 5.2)

    # ── PER-TRADE DETAIL PAGES ───────────────────────────────────────────
    for s in signals:
        if not s.get("outcome_result"):
            continue

        pdf.add_page()
        y = 12

        result = s.get("outcome_result", "")
        pnl = s.get("outcome_pnl") or 0
        direction = s.get("direction", "")
        is_long = direction in ("long", "bullish")
        v9_ok, v9_reason = v9sc_pass(s)

        # ── HEADER BAR ───────────────────────────────────────────────
        header_color = WIN_CLR if result == "WIN" else LOSS_CLR if result == "LOSS" else EXP_CLR
        pdf.set_fill_color(*header_color)
        pdf.rect(10, y, 190, 10, "F")

        pdf._set_text(BG, 13, True)
        pdf.set_xy(12, y + 1)
        dir_arrow = "^" if is_long else "v"
        pdf.cell(0, 8,
                 f"#{s['id']}  {s['setup_name']}  {dir_arrow}  {result}  {pnl:+.1f} pts")
        y += 13

        # ── TRADE SUMMARY CARD ───────────────────────────────────────
        pdf.set_fill_color(*CARD)
        card_h = 38
        pdf.rect(10, y, 190, card_h, "F")

        def kv(label, val, x, y, label_w=28, val_w=35, val_color=TEXT):
            pdf._set_text(MUTED, 7)
            pdf.set_xy(x, y)
            pdf.cell(label_w, 5, label)
            pdf._set_text(val_color, 7, True)
            pdf.set_xy(x + label_w, y)
            pdf.cell(val_w, 5, str(val))

        # Row 1
        r1y = y + 2
        kv("Setup:", s["setup_name"], 12, r1y)
        kv("Grade:", s.get("grade", "--"), 80, r1y)
        kv("Score:", _safe(s.get("score"), ".0f"), 140, r1y)

        # Row 2
        r2y = r1y + 6
        kv("Direction:", f"{dir_arrow} {'LONG' if is_long else 'SHORT'}", 12, r2y,
           val_color=WIN_CLR if is_long else LOSS_CLR)
        kv("Spot:", _safe(s.get("spot"), ".2f"), 80, r2y)
        kv("Paradigm:", s.get("paradigm", "--"), 140, r2y)

        # Row 3 - Greek context
        r3y = r2y + 6
        align = s.get("greek_alignment")
        kv("Alignment:", f"{align:+d}" if align is not None else "--", 12, r3y,
           val_color=WIN_CLR if (align or 0) > 0 else LOSS_CLR if (align or 0) < 0 else MUTED)
        kv("VIX:", _safe(s.get("vix"), ".2f"), 80, r3y)
        kv("Overvix:", _safe(s.get("overvix"), "+.2f"), 140, r3y)

        # Row 4 - Levels
        r4y = r3y + 6
        kv("Target:", _safe(s.get("target_level") or s.get("target"), ".1f"), 12, r4y)
        kv("Stop:", _safe(s.get("stop_level"), ".1f"), 80, r4y)
        kv("LIS:", _safe(s.get("lis"), ".1f"), 140, r4y)

        # Row 5 - V9-SC + result
        r5y = r4y + 6
        kv("V9-SC:", f"{'PASS' if v9_ok else 'FAIL'} ({v9_reason})", 12, r5y,
           val_color=WIN_CLR if v9_ok else LOSS_CLR)
        kv("Result:", f"{result}  {pnl:+.1f} pts", 80, r5y,
           val_color=pdf._outcome_color(result))
        if s.get("charm_limit_entry"):
            kv("Charm S/R:", _safe(s["charm_limit_entry"], ".1f"), 140, r5y)

        y += card_h + 3

        # ── GREEK DETAIL CARD ────────────────────────────────────────
        pdf.set_fill_color(35, 35, 60)
        gk_h = 16
        pdf.rect(10, y, 190, gk_h, "F")
        pdf._set_text(ACCENT, 8, True)
        pdf.set_xy(12, y + 1)
        pdf.cell(50, 4, "Greek Context")

        g_y = y + 6
        kv("Vanna All:", _safe(s.get("vanna_all"), ",.0f"), 12, g_y, 22, 35)
        kv("Vanna Wk:", _safe(s.get("vanna_weekly"), ",.0f"), 75, g_y, 22, 35)
        kv("SVB:", _safe(s.get("spot_vol_beta"), ".2f"), 140, g_y, 12, 25)
        y += gk_h + 3

        # ── ES ABSORPTION SPECIFICS ──────────────────────────────────
        if s.get("abs_es_price"):
            pdf.set_fill_color(35, 35, 60)
            pdf.rect(10, y, 190, 10, "F")
            pdf._set_text(ACCENT, 8, True)
            pdf.set_xy(12, y + 1)
            pdf.cell(50, 4, "ES Absorption Detail")
            kv("ES Price:", _safe(s["abs_es_price"], ".2f"), 12, y + 5)
            kv("Vol Ratio:", _safe(s.get("abs_vol_ratio"), ".1f") + "x" if s.get("abs_vol_ratio") else "--",
               80, y + 5)
            y += 13

        # ── ES CANDLESTICK CHART with entry/exit markers ───────────
        chart_path = _generate_price_chart(s, spx_prices, range_bars)
        if chart_path:
            try:
                pdf.image(chart_path, x=10, y=y, w=190, h=40)
                y += 42
            finally:
                try:
                    os.unlink(chart_path)
                except:
                    pass

        # ── DISCORD SENTIMENT ────────────────────────────────────────
        context, sentiment = find_discord_context(discord_msgs, s["ts"], window_min=10)

        pdf.set_fill_color(35, 30, 55)
        pdf._set_text(DISCORD_CLR, 10, True)
        pdf.set_xy(12, y + 1)
        pdf.cell(60, 6, "Discord Sentiment")

        # Sentiment badge
        sent_color = WIN_CLR if "BULL" in sentiment else LOSS_CLR if "BEAR" in sentiment else MUTED
        pdf._set_text(sent_color, 10, True)
        pdf.set_xy(80, y + 1)
        agrees = ""
        if sentiment in ("BULLISH", "BEARISH"):
            if (sentiment == "BULLISH" and is_long) or (sentiment == "BEARISH" and not is_long):
                agrees = "  >> AGREES"
            else:
                agrees = "  X DISAGREES"
        pdf.cell(110, 6, f"{sentiment}{agrees}")
        y += 9

        if context:
            # Show notable messages first, then others
            notable = [m for m in context if m.get("is_notable")]
            others = [m for m in context if not m.get("is_notable")]
            shown = (notable + others)[:12]  # max 12 messages

            for m in shown:
                if y > 270:
                    pdf.add_page()
                    y = 15

                diff = m.get("diff_min", 0)
                prefix = f"[{diff:+.0f}m]"
                author = m["author"][:15]
                content = m["content"][:120]
                # Remove URLs
                content = re.sub(r'https?://\S+', '[link]', content)

                is_notable = m.get("is_notable", False)
                pdf._set_text(DISCORD_CLR if is_notable else MUTED, 6.5, is_notable)
                pdf.set_xy(12, y)
                pdf.cell(14, 4, prefix)
                pdf._set_text(DISCORD_CLR if is_notable else (180, 180, 200), 6.5, is_notable)
                pdf.set_xy(26, y)
                pdf.cell(22, 4, author)
                pdf._set_text(TEXT if is_notable else (160, 160, 180), 6.5)
                pdf.set_xy(48, y)
                # Safe content — replace non-latin1 chars
                safe_content = content.encode('latin-1', 'replace').decode('latin-1')
                pdf.cell(150, 4, safe_content)
                y += 4.5
        else:
            pdf._set_text(MUTED, 7)
            pdf.set_xy(12, y)
            pdf.cell(0, 5, "No Discord messages within +/-10 min of this signal.")
            y += 7

        # ── TRADE NOTES (blank area for iPad annotation) ─────────────
        y += 3
        if y < 230:
            notes_h = min(45, 280 - y)
            pdf.set_draw_color(*MUTED)
            pdf.set_fill_color(25, 25, 45)
            pdf.rect(10, y, 190, notes_h, "DF")
            pdf._set_text(MUTED, 8)
            pdf.set_xy(12, y + 2)
            pdf.cell(0, 5, "Notes (annotate on iPad):")
            # Draw subtle lines
            pdf.set_draw_color(40, 40, 65)
            for line_y in range(int(y + 10), int(y + notes_h - 2), 7):
                pdf.line(14, line_y, 196, line_y)

    return pdf


# ── MAIN ─────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Generate daily trade review PDF")
    parser.add_argument("date", nargs="?", default=None,
                        help="Trade date YYYY-MM-DD (default: last trading day)")
    parser.add_argument("--discord", default=None,
                        help="Path to Discord export JSON file or folder")
    parser.add_argument("--output", "-o", default=None,
                        help="Output PDF path (default: trade_review_YYYY-MM-DD.pdf)")
    args = parser.parse_args()

    # Determine trade date
    if args.date:
        trade_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        # Last trading day
        today = datetime.now().date()
        d = today - timedelta(days=1)
        while d.weekday() >= 5:  # Skip weekends
            d -= timedelta(days=1)
        trade_date = d

    print(f"Generating trade review for {trade_date} ...")

    # Fetch signals
    print("  Fetching signals from API...")
    signals = fetch_signals(trade_date)
    print(f"  Found {len(signals)} signals")

    if not signals:
        print("  No signals found for this date. Exiting.")
        return

    # Load Discord
    discord_path = args.discord or DEFAULT_DISCORD
    print(f"  Loading Discord messages...")
    discord_msgs = load_discord(trade_date, discord_path)
    print(f"  Found {len(discord_msgs)} Discord messages")

    # Load SPX price data from Railway DB
    print("  Loading SPX prices from DB...")
    spx_prices = None
    try:
        db_url = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
        db_engine = create_engine(db_url)
        spx_prices = _load_spx_prices(db_engine, trade_date)
        print(f"  Found {len(spx_prices)} SPX price points")
    except Exception as e:
        print(f"  WARNING: Could not load SPX prices: {e}")

    # Load ES range bars from Railway DB
    range_bars = None
    try:
        range_bars = _load_range_bars(db_engine, trade_date)
        print(f"  Found {len(range_bars)} ES range bars")
    except Exception as e:
        print(f"  WARNING: Could not load range bars: {e}")

    # Build PDF
    print("  Building PDF...")
    pdf = build_pdf(signals, discord_msgs, trade_date, spx_prices, range_bars)

    # Output
    out_path = args.output or f"trade_review_{trade_date}.pdf"
    pdf.output(out_path)
    print(f"  Saved: {out_path}")
    print(f"  Pages: {pdf.pages_count}")

    # Summary
    total_pnl = sum(s.get("outcome_pnl") or 0 for s in signals if s.get("outcome_result"))
    v8_passed = [s for s in signals if v9sc_pass(s)[0]]
    v8_pnl = sum(s.get("outcome_pnl") or 0 for s in v8_passed if s.get("outcome_result"))
    print(f"\n  Total PnL: {total_pnl:+.1f} pts ({len(signals)} signals)")
    print(f"  V9-SC PnL: {v8_pnl:+.1f} pts ({len(v8_passed)} passed)")


if __name__ == "__main__":
    main()
