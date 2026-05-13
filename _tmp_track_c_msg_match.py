"""
Track C — Message-level Discord matching for the days we have RAW chat:
Mar 19, Mar 20, Mar 21 (1058 msgs daytrading), plus beginners Jan-Mar 21 (1907 msgs).

This is the HONEST application of the original ±15 min methodology — but only
on the days raw timestamped chat is available.
"""
import os
import re
import json
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from zoneinfo import ZoneInfo

DB_URL = "postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
DUMP1 = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\tmp_discord_dump.txt"
DUMP2 = r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\tmp_beginners_dump.txt"

# Per CLAUDE.md and user_timezone.md: KSA -> ET = subtract 7 hours (KSA is UTC+3, ET is UTC-4)
# But raw dump may already be in UTC. Let's inspect by comparing a few messages to known events.
# Test message: "2026-03-19T16:33  johannes3041: stacked 6600" -> dump appears to be UTC (16:33 UTC = 12:33 ET).
# That aligns with market activity at the time. CONFIRMED: raw dump timestamps are UTC.
DUMP_TZ = "UTC"

MSG_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})\]\s+([^:]+):\s+(.*)$")

# Key author tags
KEY_AUTHORS = {
    "apollo": "Apollo",
    "darkmatter": "DarkMatter",
    "darkmattertrade": "DarkMatter",
    "wizardofops": "Wizard",
    "bigbill": "BigBill",
    "bigbill8887": "BigBill",
    "l0rd.helmet": "LordHelmet",
    "lordhelmet": "LordHelmet",
    "disciple3": "Disciple3",
    "simplejack": "SimpleJack",
    "yahyaz": "Yahya",
    "phoenix": "Phoenix",
    "johannes3041": "Johannes",
    "jaytech887": "JayTech",
    "toto2229": "Toto",
}

# Directional sentiment keywords (very conservative — just flag, don't claim semantics)
BULLISH_KW = re.compile(r"\b(bull|long|buy|support|bid|gex pure|hold|rip|rally|squeeze|target up|magnet up|positive gamma|positive vanna|bullish charm|dd positive)\b", re.IGNORECASE)
BEARISH_KW = re.compile(r"\b(bear|short|sell|resistance|offer|fade|reject|breakdown|drop|crash|neg gex|negative gamma|negative vanna|bearish charm|dd negative|undervix exhaust|fade rally)\b", re.IGNORECASE)
LEVEL_RE = re.compile(r"\b(6[0-9]{3}|7[0-3][0-9]{2})\b")  # SPX-range levels

def parse_dump(path):
    out = []
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = MSG_RE.match(line.strip())
            if not m:
                continue
            date, hh, mm, user, text_ = m.groups()
            user_clean = user.strip().lower()
            tag = None
            for k, v in KEY_AUTHORS.items():
                if k in user_clean:
                    tag = v
                    break
            try:
                ts_utc = datetime.strptime(f"{date}T{hh}:{mm}", "%Y-%m-%dT%H:%M").replace(tzinfo=ZoneInfo("UTC"))
            except Exception:
                continue
            ts_et = ts_utc.astimezone(ZoneInfo("America/New_York"))
            out.append({
                "ts_utc": ts_utc,
                "ts_et": ts_et,
                "user": user.strip(),
                "tag": tag,
                "text": text_,
                "bullish": bool(BULLISH_KW.search(text_)),
                "bearish": bool(BEARISH_KW.search(text_)),
                "levels": [int(x) for x in LEVEL_RE.findall(text_)],
            })
    return out


def fetch_signals_window(start_date, end_date):
    e = create_engine(DB_URL, pool_pre_ping=True)
    q = text(
        """
        SELECT
          id, ts, (ts AT TIME ZONE 'America/New_York')::date AS trade_date,
          setup_name, direction, grade, paradigm, spot, lis, target,
          greek_alignment, overvix, vix,
          outcome_result, outcome_pnl
        FROM setup_log
        WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN :s AND :e
          AND outcome_result IS NOT NULL
        ORDER BY ts
        """
    )
    with e.connect() as c:
        return pd.read_sql(q, c, params={"s": start_date, "e": end_date})


def main():
    msgs = []
    msgs.extend(parse_dump(DUMP1))
    msgs.extend(parse_dump(DUMP2))
    # Sort
    msgs.sort(key=lambda m: m["ts_utc"])
    if not msgs:
        print("no msgs")
        return
    start = msgs[0]["ts_utc"].astimezone(ZoneInfo("America/New_York")).date()
    end = msgs[-1]["ts_utc"].astimezone(ZoneInfo("America/New_York")).date()
    print(f"[msg-match] {len(msgs)} msgs spanning {start}..{end}")
    keyed = [m for m in msgs if m["tag"]]
    print(f"[msg-match] {len(keyed)} key-author msgs")

    # Find overlap dates
    msg_dates = set(m["ts_et"].date() for m in msgs)
    print(f"[msg-match] msg dates: {sorted(msg_dates)}")

    # Fetch signals for those dates only
    sigs = fetch_signals_window(min(msg_dates).isoformat(), max(msg_dates).isoformat())
    print(f"[msg-match] signals in window: {len(sigs)}")

    # For each signal, find msgs within +/- 15 min from key authors
    sigs["ts"] = pd.to_datetime(sigs["ts"], utc=True)
    sigs["ts_et"] = sigs["ts"].dt.tz_convert("America/New_York")

    # Build msg lookup by minute bucket
    msg_idx = []
    for m in keyed:
        msg_idx.append((m["ts_utc"], m))

    enriched = []
    for _, row in sigs.iterrows():
        ts = row["ts"].to_pydatetime()
        lo = ts - timedelta(minutes=15)
        hi = ts + timedelta(minutes=15)
        nearby = [m for (t, m) in msg_idx if lo <= t <= hi]
        bull_msgs = sum(1 for m in nearby if m["bullish"] and not m["bearish"])
        bear_msgs = sum(1 for m in nearby if m["bearish"] and not m["bullish"])
        level_mentions = []
        for m in nearby:
            level_mentions.extend(m["levels"])
        spot = row["spot"]
        near_level = 0
        if spot:
            for lv in level_mentions:
                if abs(spot - lv) <= 8:
                    near_level = 1
                    break
        # net bias
        if bull_msgs > bear_msgs:
            chat_bias = "bullish"
        elif bear_msgs > bull_msgs:
            chat_bias = "bearish"
        else:
            chat_bias = "neutral" if (bull_msgs or bear_msgs) else "silent"
        direction = (row["direction"] or "").upper()
        if chat_bias == "bullish":
            chat_align = "aligned" if direction == "LONG" else ("opposed" if direction == "SHORT" else "")
        elif chat_bias == "bearish":
            chat_align = "aligned" if direction == "SHORT" else ("opposed" if direction == "LONG" else "")
        else:
            chat_align = chat_bias
        enriched.append({
            "id": row["id"], "ts_et": row["ts_et"], "trade_date": row["trade_date"],
            "setup_name": row["setup_name"], "direction": direction, "spot": spot,
            "outcome_result": row["outcome_result"], "outcome_pnl": row["outcome_pnl"],
            "nearby_msgs": len(nearby), "bull_msgs": bull_msgs, "bear_msgs": bear_msgs,
            "chat_bias": chat_bias, "chat_align": chat_align,
            "near_chat_level": near_level,
        })
    df_enr = pd.DataFrame(enriched)
    print(df_enr["chat_align"].value_counts())
    df_enr.to_csv(r"G:\My Drive\Python\MyProject\GitHub\0dtealpha\_tmp_track_c_msg_match.csv", index=False)

    # Quick stats
    print()
    for lbl, sub in [
        ("All", df_enr),
        ("chat_aligned", df_enr[df_enr["chat_align"] == "aligned"]),
        ("chat_opposed", df_enr[df_enr["chat_align"] == "opposed"]),
        ("chat_silent", df_enr[df_enr["chat_align"] == "silent"]),
        ("near_chat_level", df_enr[df_enr["near_chat_level"] == 1]),
    ]:
        if len(sub) == 0:
            print(f"  {lbl}: 0"); continue
        decided = sub[sub["outcome_result"].isin(["WIN", "LOSS"])]
        wr = (decided["outcome_result"] == "WIN").mean() * 100 if len(decided) else None
        pnl = sub["outcome_pnl"].fillna(0).sum()
        wins = int((decided["outcome_result"] == "WIN").sum())
        losses = int((decided["outcome_result"] == "LOSS").sum())
        wr_s = f"{wr:.0f}%" if wr is not None else "na"
        print(f"  {lbl}: n={len(sub)} W={wins}/L={losses} WR={wr_s} PnL={pnl:+.1f}")


if __name__ == "__main__":
    main()
