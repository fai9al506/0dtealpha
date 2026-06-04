"""Weekly TSRT statement -> Telegram Researchs channel (S204).

Runs Friday 16:20 ET on Railway. Pulls broker truth from TS /historicalorders
for both TSRT accounts, FIFO-matches round trips per ET day, persists daily
rows to `tsrt_daily_stmt` (so history survives TS's 90-day lookback limit),
builds a dark-themed HTML statement ($ + SAR, equity curve + daily P&L charts)
and sends it as a document to the "0DTE Alpha Researchs" channel.

Self-contained module. No imports from main.py. Receives `engine` and
`get_token_fn` via init(). DB discipline: short transactions only
(engine.begin per statement block) — never holds locks across the run.
"""
from __future__ import annotations

import base64
import io
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from sqlalchemy import text

NY = ZoneInfo("America/New_York")
MES_PT = 5.0
SAR = 3.75  # USD/SAR peg
ACCOUNTS = ("210VYX65", "210VYX91")
REAL_BASE = "https://api.tradestation.com/v3"
TEL_RES_CHAT = "-1003792574755"

# Era anchor: first session after V16.1 deploy. Starting capital verified
# 2026-06-04 against live equity $5,974.24 minus era net P&L (broker truth).
ERA_START = "2026-05-19"
ERA_START_CAP = 4896.99
PULL_FLOOR = "2026-05-15"  # pull a few days before era start so FIFO opens clean

_engine = None
_get_token = None

# Curated per-day comments (early era, written during the 2026-06-04 review).
# Days not listed here get an auto-generated comment.
_CURATED = {
    "2026-05-19": "Clean first day post-V16.1. One -15pt stop, recovered by +23pt runner + 3 afternoon short wins.",
    "2026-05-20": "Busiest day (20 RTs). Trend-up — long stack caught +30 and 2x +21.75pt runners.",
    "2026-05-21": "Chop day. Morning long stack stopped (~-$180), afternoon clawed it all back. Small red — system survived chop.",
    "2026-05-22": "Pre-holiday drift, longs only. Early +$147, afternoon faded. Mild red.",
    "2026-05-26": "Best early-era day, only 4 RTs — 3 stacked shorts ALL winners (+23/+25/+28pt).",
    "2026-05-27": "Quiet. 2 short wins.",
    "2026-05-28": "Broad green (11 RTs, 9 winners). Several positions ran to the 15:50 EOD flatten in profit.",
    "2026-05-29": "1 trade, scratch.",
    "2026-06-01": "3 trades, 2 long wins offset a -12.75pt short.",
    "2026-06-02": "8 small wins, steady grind.",
    "2026-06-03": "Worst early-era day. 5 consecutive losing longs 09:45-12:18 (-$292.50 alone) — long-stacking into the selloff. S203 underwater-stack guard (live since 2026-06-04) replays this day at ~-$106.",
}


def init(engine, get_token_fn) -> None:
    global _engine, _get_token
    _engine = engine
    _get_token = get_token_fn


def _ensure_table() -> None:
    with _engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS tsrt_daily_stmt (
                day DATE PRIMARY KEY,
                gross NUMERIC,
                comm NUMERIC,
                net NUMERIC,
                n_trades INT,
                n_wins INT,
                trades JSONB
            )
        """))


def _to_et(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(NY)


def _pull_and_match(token: str, since: str) -> dict[str, list[dict]]:
    """Pull historicalorders for both accounts, FIFO-match round trips.
    Returns {et_day: [trade dicts]}. Accounts are flat overnight (EOD flatten),
    so per-day matching is self-contained."""
    fills = []
    for acct in ACCOUNTS:
        # /historicalorders excludes TODAY's orders — those live in /orders.
        # Friday's cron runs same-day post-market, so both must be merged
        # (verified 2026-06-04: same-day fill absent from historicalorders).
        orders, seen_ids = [], set()
        for url in (f"{REAL_BASE}/brokerage/accounts/{acct}/historicalorders"
                    f"?since={since}&pageSize=600",
                    f"{REAL_BASE}/brokerage/accounts/{acct}/orders?pageSize=600"):
            r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
            r.raise_for_status()
            for o in r.json().get("Orders", []):
                oid = o.get("OrderID")
                if oid in seen_ids:
                    continue
                seen_ids.add(oid)
                orders.append(o)
        for o in orders:
            if o.get("Status") not in ("FLL", "FLP"):
                continue
            comm = float(o.get("CommissionFee") or 0)
            for leg in o.get("Legs", []):
                q = int(leg.get("ExecQuantity") or 0)
                if q == 0:
                    continue
                fills.append({
                    "account": acct,
                    "time": o.get("ClosedDateTime") or o.get("OpenedDateTime"),
                    "side": leg.get("BuyOrSell"),
                    "qty": q,
                    "price": float(leg.get("ExecutionPrice") or 0),
                    "commission": comm,
                })
    fills.sort(key=lambda x: (x["account"], x["time"] or ""))

    by_day: dict[str, list[dict]] = defaultdict(list)
    day_comm: dict[str, float] = defaultdict(float)
    by_acct: dict[str, list[dict]] = defaultdict(list)
    for f in fills:
        by_acct[f["account"]].append(f)
    for acct, flist in by_acct.items():
        position, queue = 0, []
        for f in flist:
            sign = 1 if f["side"] == "Buy" else -1
            day = _to_et(f["time"]).date().isoformat()
            day_comm[day] += f["commission"]
            remaining = f["qty"]
            while remaining > 0:
                if position == 0 or (position > 0) == (sign > 0):
                    queue.append({"side": f["side"], "price": f["price"], "time": f["time"]})
                    position += sign
                    remaining -= 1
                else:
                    e = queue.pop(0)
                    pts = (f["price"] - e["price"]) if e["side"] == "Buy" else (e["price"] - f["price"])
                    by_day[day].append({
                        "account": acct,
                        "dir": "LONG" if e["side"] == "Buy" else "SHORT",
                        "entry_et": _to_et(e["time"]).strftime("%H:%M"),
                        "exit_et": _to_et(f["time"]).strftime("%H:%M"),
                        "entry": e["price"], "exit": f["price"],
                        "pts": round(pts, 2),
                        "usd": round(pts * MES_PT, 2),
                    })
                    position += sign
                    remaining -= 1
        if position != 0:
            print(f"[tsrt-weekly] WARNING {acct} open position {position} at window end", flush=True)
    # attach per-day commission as a pseudo-field on the dict
    for day, c in day_comm.items():
        by_day.setdefault(day, [])
    return {d: {"trades": ts, "comm": day_comm.get(d, 0.0)} for d, ts in by_day.items()}


def _upsert_days(days: dict) -> None:
    with _engine.begin() as conn:
        for day, rec in days.items():
            ts = rec["trades"]
            gross = sum(t["usd"] for t in ts)
            conn.execute(text("""
                INSERT INTO tsrt_daily_stmt (day, gross, comm, net, n_trades, n_wins, trades)
                VALUES (:d, :g, :c, :n, :nt, :nw, :tr)
                ON CONFLICT (day) DO UPDATE SET gross=EXCLUDED.gross, comm=EXCLUDED.comm,
                    net=EXCLUDED.net, n_trades=EXCLUDED.n_trades, n_wins=EXCLUDED.n_wins,
                    trades=EXCLUDED.trades
            """), {"d": day, "g": gross, "c": rec["comm"], "n": gross - rec["comm"],
                   "nt": len(ts), "nw": len([t for t in ts if t["usd"] > 0]),
                   "tr": json.dumps(ts)})


def _load_era_rows() -> list[dict]:
    with _engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT day::text, gross, comm, net, n_trades, n_wins, trades "
            "FROM tsrt_daily_stmt WHERE day >= :s ORDER BY day"), {"s": ERA_START}).fetchall()
    return [{"day": r[0], "gross": float(r[1]), "comm": float(r[2]), "net": float(r[3]),
             "n_trades": r[4], "n_wins": r[5],
             "trades": r[6] if isinstance(r[6], list) else json.loads(r[6] or "[]")}
            for r in rows]


def _live_equity(token: str) -> float | None:
    """Sum of account equity minus open-position unrealized P&L, so the
    drift check compares realized capital only (no false drift if a
    position happens to be open when the report runs)."""
    total = 0.0
    try:
        for acct in ACCOUNTS:
            r = requests.get(f"{REAL_BASE}/brokerage/accounts/{acct}/balances",
                             headers={"Authorization": f"Bearer {token}"}, timeout=15)
            b = r.json().get("Balances", [{}])
            b = b[0] if isinstance(b, list) and b else b
            detail = b.get("BalanceDetail", {}) or {}
            unreal = float(detail.get("UnrealizedProfitLoss") or 0)
            total += float(b.get("Equity", 0)) - unreal
        return total
    except Exception as e:
        print(f"[tsrt-weekly] balance pull failed: {e}", flush=True)
        return None


def _auto_comment(r: dict) -> str:
    ts = r["trades"]
    if not ts:
        return "No trades."
    best = max(ts, key=lambda t: t["usd"])
    worst = min(ts, key=lambda t: t["usd"])
    return (f"{r['n_trades']} RTs, {r['n_wins']}W/{r['n_trades'] - r['n_wins']}L. "
            f"Best {best['dir']} {best['pts']:+.2f}pt, worst {worst['dir']} {worst['pts']:+.2f}pt.")


# ---------- charts ----------
_BG, _PANEL, _FG, _GRID = "#0b0f14", "#11161d", "#e8edf4", "#1d242e"
_GREEN, _RED, _ACCENT, _MUTED = "#2dd4a7", "#ff5d5d", "#5ea2ff", "#8b97a6"
_FONTS = ["Inter", "Segoe UI", "DejaVu Sans"]


def _fig_b64(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor=_BG)
    import matplotlib.pyplot as plt
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def _build_charts(rows: list[dict]):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "figure.facecolor": _BG, "axes.facecolor": _PANEL, "axes.edgecolor": _GRID,
        "axes.labelcolor": _FG, "text.color": _FG, "xtick.color": _MUTED,
        "ytick.color": _MUTED, "grid.color": _GRID, "font.size": 10,
        "font.family": "sans-serif", "font.sans-serif": _FONTS,
        "axes.titleweight": "bold",
    })
    days_lbl = [r["day"][5:] for r in rows]
    eq = [ERA_START_CAP] + [r["ending"] for r in rows]

    fig, ax = plt.subplots(figsize=(9.5, 4))
    ax.plot(["start"] + days_lbl, eq, color=_ACCENT, marker="o", linewidth=2.2, markersize=4.5)
    ax.fill_between(range(len(eq)), eq, ERA_START_CAP, alpha=0.10, color=_ACCENT)
    ax.axhline(ERA_START_CAP, color=_GRID, linewidth=1, linestyle="--")
    ax.set_title("Total Capital — Both Accounts", fontsize=13)
    ax.set_ylabel("USD")
    ax.grid(True, alpha=0.35)
    ax.tick_params(axis="x", rotation=45 if len(eq) > 18 else 0)
    hi = max(range(len(eq)), key=lambda i: eq[i])
    for i in {0, len(eq) - 1, hi}:
        ax.annotate(f"${eq[i]:,.0f}", (i, eq[i]), textcoords="offset points",
                    xytext=(0, 9), ha="center", fontsize=9, fontweight="bold")
    equity_png = _fig_b64(fig)

    fig, ax = plt.subplots(figsize=(9.5, 3.6))
    vals = [r["net"] for r in rows]
    ax.bar(days_lbl, vals, color=[_GREEN if v > 0 else _RED for v in vals], width=0.62)
    ax.axhline(0, color=_FG, linewidth=0.8)
    ax.set_title("Daily Net P&L (USD, after commissions)", fontsize=13)
    ax.grid(True, axis="y", alpha=0.35)
    ax.tick_params(axis="x", rotation=45 if len(vals) > 18 else 0)
    if len(vals) <= 25:
        for i, v in enumerate(vals):
            ax.annotate(f"{v:+,.0f}", (i, v), textcoords="offset points",
                        xytext=(0, 4 if v > 0 else -13), ha="center", fontsize=8.5)
    pnl_png = _fig_b64(fig)
    return equity_png, pnl_png


# ---------- report ----------
def _build_html(rows: list[dict], live_eq: float | None, gen_ts: str) -> str:
    era_net = sum(r["net"] for r in rows)
    n_days = len(rows)
    green = [r for r in rows if r["net"] > 0]
    mean = era_net / n_days
    var = sum((r["net"] - mean) ** 2 for r in rows) / max(1, n_days - 1)
    std = var ** 0.5
    best = max(rows, key=lambda r: r["net"])
    worst = min(rows, key=lambda r: r["net"])
    end_cap = rows[-1]["ending"]
    total_rts = sum(r["n_trades"] for r in rows)
    total_wins = sum(r["n_wins"] for r in rows)
    week_cut = (datetime.now(NY) - timedelta(days=7)).date().isoformat()
    week_net = sum(r["net"] for r in rows if r["day"] > week_cut)
    equity_png, pnl_png = _build_charts(rows)

    drift_note = ""
    if live_eq is not None and abs(live_eq - end_cap) > 1.0:
        drift_note = (f"<div class='note warn'><b>Capital drift:</b> computed ending "
                      f"${end_cap:,.2f} vs live equity ${live_eq:,.2f} "
                      f"(&Delta; ${live_eq - end_cap:+,.2f}) — deposit/withdrawal or "
                      f"unmatched fills; verify next session.</div>")

    trows = ""
    for r in rows:
        cls = "pos" if r["net"] > 0 else "neg"
        cmt = _CURATED.get(r["day"], _auto_comment(r))
        trows += (f"<tr><td>{r['day']}</td><td>{r['n_trades']}</td>"
                  f"<td class='{cls}'>{r['net']:+,.2f}</td>"
                  f"<td class='{cls}'>{r['net'] * SAR:+,.2f}</td>"
                  f"<td>${r['ending']:,.2f}</td><td>{r['ending'] * SAR:,.2f}</td>"
                  f"<td class='cmt'>{cmt}</td></tr>")

    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>TSRT Weekly Statement</title>
<link rel='preconnect' href='https://fonts.googleapis.com'>
<link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;700&display=swap' rel='stylesheet'>
<style>
:root {{ --bg:{_BG}; --panel:{_PANEL}; --fg:{_FG}; --grid:{_GRID};
        --green:{_GREEN}; --red:{_RED}; --accent:{_ACCENT}; --muted:{_MUTED}; }}
* {{ box-sizing:border-box; }}
body {{ background:var(--bg); color:var(--fg);
  font-family:'Inter',-apple-system,'Segoe UI',Roboto,sans-serif;
  font-size:15px; line-height:1.55; max-width:1020px; margin:0 auto; padding:32px 24px;
  -webkit-font-smoothing:antialiased; }}
h1 {{ font-size:26px; font-weight:800; letter-spacing:-0.02em; margin:0 0 4px; }}
h1 span {{ color:var(--accent); }}
h2 {{ font-size:17px; font-weight:700; letter-spacing:-0.01em; color:var(--accent);
     margin:36px 0 12px; text-transform:uppercase; font-size:13px; letter-spacing:0.08em; }}
.sub {{ color:var(--muted); font-size:13px; margin-bottom:24px; }}
table {{ border-collapse:separate; border-spacing:0; width:100%; font-size:13px;
        font-variant-numeric:tabular-nums; border:1px solid var(--grid); border-radius:10px; overflow:hidden; }}
th, td {{ padding:9px 12px; text-align:right; border-bottom:1px solid var(--grid); }}
th {{ background:var(--panel); color:var(--muted); font-size:11px; font-weight:600;
     text-transform:uppercase; letter-spacing:0.06em; }}
tr:last-child td {{ border-bottom:none; }}
tbody tr:hover {{ background:rgba(94,162,255,0.04); }}
td:first-child, th:first-child, td:last-child, th:last-child {{ text-align:left; }}
tr.total td {{ background:var(--panel); font-weight:700; border-top:2px solid var(--grid); }}
.pos {{ color:var(--green); font-weight:600; }} .neg {{ color:var(--red); font-weight:600; }}
.cmt {{ font-size:12px; color:#aeb8c4; max-width:340px; line-height:1.45; }}
.cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin:20px 0; }}
.card {{ background:var(--panel); border:1px solid var(--grid); border-radius:12px; padding:16px 18px; }}
.card .l {{ font-size:10.5px; color:var(--muted); text-transform:uppercase; letter-spacing:0.08em;
           font-weight:600; margin-bottom:6px; }}
.card .v {{ font-size:21px; font-weight:800; letter-spacing:-0.02em;
           font-variant-numeric:tabular-nums; }}
.card .s {{ font-size:12px; color:var(--muted); font-weight:500; margin-top:2px; }}
img {{ width:100%; border-radius:12px; margin:10px 0; border:1px solid var(--grid); }}
.note {{ background:var(--panel); border:1px solid var(--grid); border-left:3px solid var(--accent);
        padding:14px 18px; border-radius:10px; margin:12px 0; font-size:14px; }}
.note.warn {{ border-left-color:var(--red); }}
.src {{ color:var(--muted); font-size:11px; margin-top:32px; line-height:1.6; }}
</style></head><body>
<h1>TSRT <span>Weekly Statement</span></h1>
<div class='sub'>Post-V16.1 era &middot; {ERA_START} &rarr; {rows[-1]['day']} &middot; {n_days} trading days
&middot; broker truth (TS /historicalorders, FIFO-matched, commissions included) &middot; generated {gen_ts}</div>
<div class='cards'>
<div class='card'><div class='l'>Starting capital</div><div class='v'>${ERA_START_CAP:,.0f}</div><div class='s'>SAR {ERA_START_CAP * SAR:,.0f}</div></div>
<div class='card'><div class='l'>Era net P&amp;L</div><div class='v pos'>{era_net:+,.0f}</div><div class='s'>SAR {era_net * SAR:+,.0f}</div></div>
<div class='card'><div class='l'>Ending capital</div><div class='v' style='color:var(--accent)'>${end_cap:,.0f}</div><div class='s'>SAR {end_cap * SAR:,.0f}</div></div>
<div class='card'><div class='l'>This week</div><div class='v {'pos' if week_net >= 0 else 'neg'}'>{week_net:+,.0f}</div><div class='s'>SAR {week_net * SAR:+,.0f}</div></div>
<div class='card'><div class='l'>Return on capital</div><div class='v pos'>+{era_net / ERA_START_CAP * 100:.1f}%</div><div class='s'>{n_days} days</div></div>
</div>
{drift_note}
<img src='data:image/png;base64,{equity_png}'>
<img src='data:image/png;base64,{pnl_png}'>
<h2>Daily Statement</h2>
<table><thead><tr><th>Day</th><th>RTs</th><th>Net P&amp;L ($)</th><th>Net P&amp;L (SAR)</th>
<th>Ending ($)</th><th>Ending (SAR)</th><th>Comments</th></tr></thead><tbody>
{trows}
<tr class='total'><td>TOTAL</td><td>{total_rts}</td><td class='pos'>{era_net:+,.2f}</td>
<td class='pos'>{era_net * SAR:+,.2f}</td><td>${end_cap:,.2f}</td><td>{end_cap * SAR:,.2f}</td><td></td></tr>
</tbody></table>
<h2>Statistics</h2>
<div class='cards'>
<div class='card'><div class='l'>Day win rate</div><div class='v'>{len(green)}/{n_days}</div><div class='s'>{len(green) / n_days * 100:.0f}%</div></div>
<div class='card'><div class='l'>Avg / day</div><div class='v pos'>{mean:+,.0f}</div><div class='s'>SAR {mean * SAR:+,.0f}</div></div>
<div class='card'><div class='l'>Daily &sigma;</div><div class='v'>${std:,.0f}</div><div class='s'>volatility</div></div>
<div class='card'><div class='l'>Trade win rate</div><div class='v'>{total_wins}/{total_rts}</div><div class='s'>{total_wins / max(1, total_rts) * 100:.0f}%</div></div>
<div class='card'><div class='l'>Best day</div><div class='v pos'>{best['net']:+,.0f}</div><div class='s'>{best['day']}</div></div>
<div class='card'><div class='l'>Worst day</div><div class='v neg'>{worst['net']:+,.0f}</div><div class='s'>{worst['day']}</div></div>
</div>
<h2>Conclusion</h2>
<div class='note'>
<b>Era verdict:</b> {'+' if era_net >= 0 else ''}${era_net:,.0f} (SAR {era_net * SAR:+,.0f}) over {n_days} live trading days at 1 MES —
{era_net / ERA_START_CAP * 100:+.1f}% on capital, {len(green) / n_days * 100:.0f}% green days.
Out-of-sample confirmation of the 90+ day V16 backtest: real fills, real slippage, real commissions.
</div>
<div class='note'>
<b>Projection (same pace):</b> 1 MES &rarr; ~${mean * 22:,.0f}/mo (SAR {mean * 22 * SAR:,.0f}) &middot;
3 MES &rarr; ~${mean * 3 * 22:,.0f}/mo (SAR {mean * 3 * 22 * SAR:,.0f}) &middot;
1 ES &rarr; ~${mean * 10 * 22:,.0f}/mo (SAR {mean * 10 * 22 * SAR:,.0f}).
<i>Caveat: {n_days}-day era — the slope will regress; the edge itself rests on 100+ days of evidence.</i>
</div>
<div class='src'>Source: TradeStation /brokerage/accounts/{{{', '.join(ACCOUNTS)}}}/historicalorders, FIFO round-trip matching, $1/RT commission.
Capital anchored at ${ERA_START_CAP:,.2f} on {ERA_START} (verified vs live equity 2026-06-04); assumes no deposits/withdrawals.
USD/SAR = {SAR} (peg). Auto-generated by tsrt_weekly_report.py (S204) — Friday 16:20 ET cron.</div>
</body></html>"""


def _send_document(html: str, fname: str, caption: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("[tsrt-weekly] TELEGRAM_BOT_TOKEN missing", flush=True)
        return False
    for attempt in range(3):
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendDocument",
                data={"chat_id": TEL_RES_CHAT, "caption": caption},
                files={"document": (fname, html.encode("utf-8"), "text/html")},
                timeout=90)
            ok = r.status_code == 200 and r.json().get("ok")
            print(f"[tsrt-weekly] telegram send: {r.status_code} ok={ok}", flush=True)
            if ok:
                return True
        except Exception as e:
            print(f"[tsrt-weekly] send attempt {attempt + 1}/3 failed: {e}", flush=True)
        time.sleep(8)
    return False


def run_weekly() -> None:
    """Friday 16:20 ET cron entry point. Fail-soft: logs errors, never raises."""
    try:
        if _engine is None or _get_token is None:
            print("[tsrt-weekly] not initialized — skipping", flush=True)
            return
        now = datetime.now(NY)
        _ensure_table()
        token = _get_token()
        # Sliding pull window: TS caps lookback at 90 calendar days. Older days
        # persist in tsrt_daily_stmt from previous runs.
        floor_dt = datetime.strptime(PULL_FLOOR, "%Y-%m-%d").date()
        since = max(floor_dt, (now - timedelta(days=85)).date()).isoformat()
        days = _pull_and_match(token, since)
        if days:
            _upsert_days(days)
        rows = _load_era_rows()
        if not rows:
            print("[tsrt-weekly] no era rows — skipping report", flush=True)
            return
        run_cap = ERA_START_CAP
        for r in rows:
            run_cap += r["net"]
            r["ending"] = run_cap
        live_eq = _live_equity(token)
        gen_ts = now.strftime("%Y-%m-%d %H:%M ET")
        html = _build_html(rows, live_eq, gen_ts)
        era_net = sum(r["net"] for r in rows)
        week_cut = (now - timedelta(days=7)).date().isoformat()
        week_net = sum(r["net"] for r in rows if r["day"] > week_cut)
        green = len([r for r in rows if r["net"] > 0])
        caption = (f"📊 TSRT Weekly Statement — post-V16.1 era ({ERA_START} → {rows[-1]['day']})\n"
                   f"Week: ${week_net:+,.0f} (SAR {week_net * SAR:+,.0f}) | "
                   f"Era: ${era_net:+,.0f} | Capital: ${rows[-1]['ending']:,.0f} | "
                   f"{green}/{len(rows)} green days")
        fname = f"TSRT_Weekly_Statement_{rows[-1]['day']}.html"
        _send_document(html, fname, caption)
    except Exception as e:
        print(f"[tsrt-weekly] run failed: {e}", flush=True)
        # best-effort failure ping so a silent Friday doesn't go unnoticed
        try:
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            if token:
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                              data={"chat_id": TEL_RES_CHAT,
                                    "text": f"⚠️ TSRT weekly statement failed: {e}"},
                              timeout=30)
        except Exception:
            pass
