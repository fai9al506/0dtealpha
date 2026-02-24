"""Pull ES ticks from Rithmic historical API, aggregate sub-fills, build range bars, plot CVD chart.

Usage:
    python tmp_rithmic_hist.py                  # defaults to 2026-02-23
    python tmp_rithmic_hist.py 2026-02-21       # specific date
"""
import asyncio
import sys
import json
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ====== CONFIG ======
RITHMIC_USER = "faisal.a.d@msn.com"
RITHMIC_PASSWORD = "7fPwgvH2$@uT2H5"
RITHMIC_SYSTEM = "Rithmic Paper Trading"
RITHMIC_URL = "wss://rprotocol.rithmic.com:443"
RANGE_PTS = 5.0


async def pull_ticks(date_str):
    from async_rithmic import RithmicClient
    from async_rithmic.enums import SysInfraType

    client = RithmicClient(
        user=RITHMIC_USER, password=RITHMIC_PASSWORD,
        system_name=RITHMIC_SYSTEM, app_name="faal:0dte_alpha",
        app_version="1.0", url=RITHMIC_URL,
    )
    # Connect ONLY to history plant (avoids multi-plant session conflicts)
    await client.connect(plants=[SysInfraType.HISTORY_PLANT])
    front_month = "ESH6"  # hardcoded — update on rollover
    print(f"Front month: {front_month}")

    # Parse date, compute RTH window (9:30-16:00 ET = 14:30-21:00 UTC)
    d = datetime.strptime(date_str, "%Y-%m-%d")
    rth_start = datetime(d.year, d.month, d.day, 14, 30, 0, tzinfo=timezone.utc)
    rth_end = datetime(d.year, d.month, d.day, 21, 0, 0, tzinfo=timezone.utc)

    # Pull in 1-minute chunks (ES does 10K+ ticks/min at open)
    raw_ticks = []
    s = rth_start
    chunk = timedelta(minutes=1)
    capped_chunks = 0
    total_chunks = 0

    while s < rth_end:
        e = min(s + chunk, rth_end)
        ticks = await client.get_historical_tick_data(front_month, "CME", s, e)
        raw_ticks.extend(ticks)
        if len(ticks) >= 9999:
            capped_chunks += 1
        total_chunks += 1
        # Compact progress
        et_hour = (s.hour - 5) % 24
        if s.minute == 0:
            print(f"\n  {s.strftime('%H:%M')} UTC ({et_hour}:00 ET): ", end="", flush=True)
        marker = "*" if len(ticks) >= 9999 else ""
        print(f"{len(ticks)}{marker} ", end="", flush=True)
        s = e

    await client.disconnect()
    print(f"\n\nRaw ticks: {len(raw_ticks)} ({capped_chunks}/{total_chunks} chunks capped)")
    return raw_ticks


def aggregate_ticks(raw_ticks):
    """Aggregate Rithmic sub-fills by timestamp + price + side.

    Rithmic unbundles CME MDP 3.0 Trade Summary into per-fill ticks.
    We reaggregate: same (ssboe, usecs, price, side) = one trade.
    Volume and delta are summed. This matches ATAS / Sierra Chart.
    """
    agg = defaultdict(lambda: {"volume": 0, "buy_vol": 0, "sell_vol": 0, "count": 0})

    for t in raw_ticks:
        ssboe = t["data_bar_ssboe"][0]
        usecs = t["data_bar_usecs"][0]
        price = t["close_price"]
        is_buy = "bid_volume" in t  # bid_volume = buyer aggressor
        side = "B" if is_buy else "A"
        vol = int(t.get("volume", 1))
        buy_v = int(t.get("bid_volume", 0))   # buyer aggressor
        sell_v = int(t.get("ask_volume", 0))   # seller aggressor

        key = (ssboe, usecs, price, side)
        agg[key]["volume"] += vol
        agg[key]["buy_vol"] += buy_v
        agg[key]["sell_vol"] += sell_v
        agg[key]["count"] += 1
        agg[key]["price"] = price
        agg[key]["ssboe"] = ssboe
        agg[key]["usecs"] = usecs

    # Convert back to list sorted by time
    trades = []
    for key, v in sorted(agg.items(), key=lambda x: (x[0][0], x[0][1])):
        trades.append({
            "price": v["price"],
            "volume": v["volume"],
            "buy_vol": v["buy_vol"],
            "sell_vol": v["sell_vol"],
            "delta": v["buy_vol"] - v["sell_vol"],
            "ssboe": v["ssboe"],
            "usecs": v["usecs"],
            "datetime": datetime.fromtimestamp(v["ssboe"], tz=timezone.utc).replace(microsecond=v["usecs"]),
            "sub_fills": v["count"],
        })

    raw_vol = sum(int(t.get("volume", 0)) for t in raw_ticks)
    agg_vol = sum(t["volume"] for t in trades)
    raw_delta = sum(int(t.get("bid_volume", 0)) - int(t.get("ask_volume", 0)) for t in raw_ticks)
    agg_delta = sum(t["delta"] for t in trades)

    print(f"Aggregated: {len(raw_ticks)} raw ticks -> {len(trades)} trades ({len(raw_ticks)/max(len(trades),1):.1f}x reduction)")
    print(f"Volume preserved: {raw_vol} -> {agg_vol} {'OK' if raw_vol == agg_vol else 'MISMATCH! MISMATCH'}")
    print(f"Delta preserved: {raw_delta} -> {agg_delta} {'OK' if raw_delta == agg_delta else 'MISMATCH! MISMATCH'}")
    return trades


def build_range_bars(trades):
    """Build 5-pt range bars from aggregated trades."""
    bars = []
    cvd = 0
    forming = None

    for t in trades:
        price = t["price"]
        vol = t["volume"]
        delta = t["delta"]
        ts = t["datetime"]

        if forming is None:
            forming = {"open": price, "high": price, "low": price, "close": price,
                       "volume": 0, "delta": 0, "buy": 0, "sell": 0,
                       "ts_start": ts, "cvd_open": cvd}

        forming["close"] = price
        forming["high"] = max(forming["high"], price)
        forming["low"] = min(forming["low"], price)
        forming["volume"] += vol
        forming["delta"] += delta
        forming["buy"] += t["buy_vol"]
        forming["sell"] += t["sell_vol"]
        forming["ts_end"] = ts
        cvd += delta

        if forming["high"] - forming["low"] >= RANGE_PTS - 0.001:
            forming["cvd_close"] = cvd
            bars.append(forming)
            forming = {"open": price, "high": price, "low": price, "close": price,
                       "volume": 0, "delta": 0, "buy": 0, "sell": 0,
                       "ts_start": ts, "cvd_open": cvd}

    print(f"Built {len(bars)} range bars")
    if bars:
        print(f"Price: {min(b['low'] for b in bars):.2f} - {max(b['high'] for b in bars):.2f}")
        print(f"CVD final: {bars[-1]['cvd_close']:+d}")
    return bars


def build_chart(bars, date_str):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from pytz import timezone as tz

    if not bars:
        print("No bars to plot")
        return

    et = tz("US/Eastern")
    times = [b["ts_start"].astimezone(et) for b in bars]
    opens = [b["open"] for b in bars]
    highs = [b["high"] for b in bars]
    lows = [b["low"] for b in bars]
    closes = [b["close"] for b in bars]
    cvds = [b["cvd_close"] for b in bars]
    deltas = [b["delta"] for b in bars]

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 10), sharex=True,
                                         gridspec_kw={"height_ratios": [3, 1, 2]})
    fig.suptitle(f"ES {date_str} — Rithmic Exchange Aggressor Data\n"
                 f"5-pt Range Bars | CVD | Bar Delta  ({len(bars)} bars)",
                 fontsize=14, fontweight="bold")

    # Price — candlesticks
    bar_width = 0.001
    for t, o, h, l, c in zip(times, opens, highs, lows, closes):
        color = "#22c55e" if c >= o else "#ef4444"
        ax1.plot([t, t], [l, h], color=color, linewidth=0.8)
        body_bottom = min(o, c)
        body_height = max(abs(c - o), 0.25)
        ax1.bar(t, body_height, bottom=body_bottom, width=bar_width,
                color=color, edgecolor=color, alpha=0.9)
    ax1.set_ylabel("ES Price", fontsize=10)
    ax1.grid(True, alpha=0.3)
    ax1.set_ylim(min(lows) - 2, max(highs) + 2)

    # Bar delta
    colors = ["#22c55e" if d >= 0 else "#ef4444" for d in deltas]
    ax2.bar(times, deltas, color=colors, width=bar_width, alpha=0.8)
    ax2.axhline(y=0, color="white", linewidth=0.5, alpha=0.5)
    ax2.set_ylabel("Bar Delta", fontsize=10)
    ax2.grid(True, alpha=0.3)

    # CVD
    cvd_color = "#22c55e" if cvds[-1] >= 0 else "#ef4444"
    ax3.plot(times, cvds, color=cvd_color, linewidth=1.5)
    ax3.fill_between(times, cvds, alpha=0.15, color=cvd_color)
    ax3.axhline(y=0, color="white", linewidth=0.5, alpha=0.5)
    ax3.set_ylabel("CVD", fontsize=10)
    ax3.grid(True, alpha=0.3)

    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M", tz=et))
    ax3.set_xlabel("Time (ET)", fontsize=10)

    # Dark theme
    for ax in [ax1, ax2, ax3]:
        ax.set_facecolor("#0d1117")
        ax.tick_params(colors="#888")
        ax.yaxis.label.set_color("#ccc")
        ax.xaxis.label.set_color("#ccc")
        for spine in ax.spines.values():
            spine.set_color("#333")
    fig.patch.set_facecolor("#0d1117")
    fig.suptitle(fig._suptitle.get_text(), color="white", fontsize=14, fontweight="bold")

    plt.tight_layout()
    out = f"tmp_es_cvd_{date_str}.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    print(f"Chart saved: {out}")


def save_data(trades, bars, date_str):
    """Save aggregated trades and bars to JSON for reuse."""
    out_dir = "historical_data"
    os.makedirs(out_dir, exist_ok=True)

    # Save trades
    trades_out = []
    for t in trades:
        trades_out.append({
            "price": t["price"], "volume": t["volume"],
            "buy_vol": t["buy_vol"], "sell_vol": t["sell_vol"],
            "delta": t["delta"], "ssboe": t["ssboe"], "usecs": t["usecs"],
            "datetime": t["datetime"].isoformat(), "sub_fills": t["sub_fills"],
        })
    trades_path = f"{out_dir}/es_trades_{date_str}.json"
    with open(trades_path, "w") as f:
        json.dump(trades_out, f)
    print(f"Trades saved: {trades_path} ({len(trades_out)} trades)")

    # Save bars
    bars_out = []
    for b in bars:
        bars_out.append({
            "open": b["open"], "high": b["high"], "low": b["low"], "close": b["close"],
            "volume": b["volume"], "delta": b["delta"], "buy": b["buy"], "sell": b["sell"],
            "cvd_close": b["cvd_close"], "cvd_open": b["cvd_open"],
            "ts_start": b["ts_start"].isoformat(), "ts_end": b["ts_end"].isoformat(),
        })
    bars_path = f"{out_dir}/es_rangebars_{date_str}.json"
    with open(bars_path, "w") as f:
        json.dump(bars_out, f)
    print(f"Bars saved: {bars_path} ({len(bars_out)} bars)")


if __name__ == "__main__":
    date_str = sys.argv[1] if len(sys.argv) > 1 else "2026-02-23"
    print(f"=== ES Historical Data: {date_str} ===\n")

    # Pull
    raw_ticks = asyncio.run(pull_ticks(date_str))

    # Aggregate sub-fills
    trades = aggregate_ticks(raw_ticks)

    # Build range bars
    bars = build_range_bars(trades)

    # Save data
    save_data(trades, bars, date_str)

    # Chart
    build_chart(bars, date_str)

    print("\nDone. Restore Railway: railway variables --set RITHMIC_USER=faisal.a.d@msn.com --service 0dtealpha")
