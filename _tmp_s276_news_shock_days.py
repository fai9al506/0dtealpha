# -*- coding: utf-8 -*-
"""S276 — do UNSCHEDULED breaking-news days (war / geopolitical) hurt the book?

Method:
  1. Build a per-session table: SPX overnight gap %, intraday range %, VIX.
  2. Score the V16 book on the SAME basis as PROJECTION.md / S275:
       chain `outcome_pnl`, -0.6 pt/contract haircut, $1.92/contract round-turn,
       basket sizing (0/0/1 sizeonly stamp), cap 2 long / 3 short, 90s dedup.
     Points -> $ at $5/pt/contract.
  3. Cross-check against BROKER truth (tsrt_daily_stmt) on the 37 real days.
  4. Classify days: shock (data-driven) + named war/geopolitical event dates.
"""
import os, sys, json, collections
import pandas as pd, numpy as np
from datetime import timedelta
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text

sys.path.insert(0, 'app')
import live_filter as lf

ET = ZoneInfo("America/New_York")
DOLLAR_PER_PT = 5.0
HAIRCUT_PT = 0.6
FEE_PER_RT = 1.92
DEADBAND = 0.15
MAX_LONG, MAX_SHORT = 2, 3
DEDUP_S = 90

E = create_engine(os.environ["DATABASE_URL"])
conn = E.connect().execution_options(isolation_level="AUTOCOMMIT")

# ---------- 1. SPX daily stats from the 1-min path ----------
px = pd.read_sql(text("""
    SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
           ts AT TIME ZONE 'America/New_York' AS et,
           bar_open, bar_high, bar_low, bar_close
    FROM spx_ohlc_1m ORDER BY ts"""), conn)
px['d'] = pd.to_datetime(px['d']).dt.date
g = px.groupby('d')
daily_px = pd.DataFrame({
    'open':  g['bar_open'].first(),
    'high':  g['bar_high'].max(),
    'low':   g['bar_low'].min(),
    'close': g['bar_close'].last(),
}).reset_index()
daily_px['prev_close'] = daily_px['close'].shift(1)
daily_px['gap_pct']   = (daily_px['open'] - daily_px['prev_close']) / daily_px['prev_close'] * 100
daily_px['range_pct'] = (daily_px['high'] - daily_px['low']) / daily_px['open'] * 100
daily_px['ret_pct']   = (daily_px['close'] - daily_px['open']) / daily_px['open'] * 100

# ---------- 2. VIX per day (max + mean of the stamped signal vix) ----------
vix = pd.read_sql(text("""
    SELECT (ts AT TIME ZONE 'America/New_York')::date AS d,
           max(vix) vix_max, avg(vix) vix_avg
    FROM setup_log WHERE vix IS NOT NULL GROUP BY 1"""), conn)
vix['d'] = pd.to_datetime(vix['d']).dt.date

# ---------- 3. V16 book, priced ----------
gaps = lf.load_gaps(conn)
rows = conn.execute(text(f"""
    SELECT {lf.COLS}, outcome_pnl, id
    FROM setup_log
    WHERE (ts AT TIME ZONE 'America/New_York') >= '2026-03-01'
      AND outcome_pnl IS NOT NULL
    ORDER BY ts""")).mappings().all()

book = []
for r in rows:
    if not lf.passes_v16(r, gaps):
        continue
    d = dict(r)
    d['et'] = d['ts'].astimezone(ET).replace(tzinfo=None)
    d['is_long'] = str(d.get('direction', '')).lower() in ('long', 'bullish')
    book.append(d)
book.sort(key=lambda x: x['et'])

# basket sizing 0/0/1 "sizeonly": contradict -> 1x, confirm -> 2x, neutral/no-data -> 1x
def qty_of(r):
    b = r.get('basket_pct')
    if b is None:
        return 1
    b = float(b)
    if abs(b) < DEADBAND:
        return 1
    return 2 if ((b > 0) == r['is_long']) else 1

# cap + dedup replay
open_pos = []          # (exit_et, is_long)
last_fire = {}
taken = []
for r in book:
    t = r['et']
    open_pos = [p for p in open_pos if p[0] > t]
    n_same = sum(1 for p in open_pos if p[1] == r['is_long'])
    cap = MAX_LONG if r['is_long'] else MAX_SHORT
    if n_same >= cap:
        continue
    k = (r['setup_name'], r['is_long'])
    if k in last_fire and (t - last_fire[k]).total_seconds() < DEDUP_S:
        continue
    last_fire[k] = t
    mins = r.get('outcome_elapsed_min') or 30
    open_pos.append((t + timedelta(minutes=float(mins)), r['is_long']))
    q = qty_of(r)
    pts = float(r['outcome_pnl'])
    net = (pts - HAIRCUT_PT) * q * DOLLAR_PER_PT - FEE_PER_RT * q
    taken.append({'d': t.date(), 'et': t, 'setup': r['setup_name'],
                  'long': r['is_long'], 'qty': q, 'pts': pts, 'net$': net})

tk = pd.DataFrame(taken)
book_daily = tk.groupby('d').agg(book_net=('net$', 'sum'), n=('net$', 'size'),
                                 pts=('pts', 'sum')).reset_index()

# ---------- 4. broker truth ----------
brk = pd.read_sql(text("SELECT day AS d, net AS broker_net, n_trades FROM tsrt_daily_stmt"), conn)
brk['d'] = pd.to_datetime(brk['d']).dt.date
brk['broker_net'] = brk['broker_net'].astype(float)

m = daily_px.merge(vix, on='d', how='left').merge(book_daily, on='d', how='left').merge(brk, on='d', how='left')
m = m[m['d'] >= pd.Timestamp('2026-03-01').date()].copy()
m['book_net'] = m['book_net'].fillna(0.0)
m['n'] = m['n'].fillna(0).astype(int)
conn.close()

m.to_pickle('_tmp_s276_daily.pkl')
print(f"sessions: {len(m)}  {m['d'].min()} -> {m['d'].max()}")
print(f"book total ${m['book_net'].sum():+,.0f}   broker days {m['broker_net'].notna().sum()} "
      f"total ${m['broker_net'].sum():+,.0f}")

print("\n=== 15 BIGGEST OVERNIGHT GAPS (|gap %|) ===")
for _, r in m.reindex(m['gap_pct'].abs().sort_values(ascending=False).index).head(15).iterrows():
    print(f"  {r['d']}  gap {r['gap_pct']:+6.2f}%  range {r['range_pct']:5.2f}%  "
          f"vix {r['vix_max'] if pd.notna(r['vix_max']) else float('nan'):5.1f}  "
          f"book ${r['book_net']:+7.0f} ({r['n']}t)")

print("\n=== 15 WIDEST INTRADAY RANGES ===")
for _, r in m.sort_values('range_pct', ascending=False).head(15).iterrows():
    print(f"  {r['d']}  range {r['range_pct']:5.2f}%  gap {r['gap_pct']:+6.2f}%  "
          f"book ${r['book_net']:+7.0f} ({r['n']}t)")

print("\n=== 15 WORST BOOK DAYS ===")
for _, r in m.sort_values('book_net').head(15).iterrows():
    print(f"  {r['d']}  book ${r['book_net']:+7.0f} ({r['n']}t)  gap {r['gap_pct']:+6.2f}%  "
          f"range {r['range_pct']:5.2f}%  vix {r['vix_max']:5.1f}")
