import os, json, sys
from sqlalchemy import create_engine, text

engine = create_engine(os.environ['DATABASE_URL'])
conn = engine.connect()

rows = conn.execute(text("""
    SELECT bar_idx, bar_open, bar_high, bar_low, bar_close,
           bar_volume, bar_delta, bar_buy_volume, bar_sell_volume,
           cumulative_delta, cvd_open, cvd_high, cvd_low, cvd_close,
           ts_start, ts_end
    FROM es_range_bars
    WHERE trade_date = '2026-02-26'
      AND source = 'live'
    ORDER BY bar_idx
""")).fetchall()

print(f"Total live bars on Feb 26: {len(rows)}")

target_es = 6879.25
trigger_idx = None
for r in rows:
    if abs(r.bar_close - target_es) < 1:
        trigger_idx = r.bar_idx
        break

if not trigger_idx:
    print(f"Exact match not found for {target_es}, showing bars 6870-6890:")
    for r in rows:
        if 6870 <= r.bar_close <= 6890:
            print(f"  [{r.bar_idx:3d}] O={r.bar_open:.2f} H={r.bar_high:.2f} L={r.bar_low:.2f} C={r.bar_close:.2f} V={r.bar_volume:4d} D={r.bar_delta:+5d} CVD={r.cumulative_delta:+7d}")
else:
    start = max(0, trigger_idx - 30)
    for r in rows:
        if start <= r.bar_idx <= trigger_idx + 3:
            marker = " <-- TRIGGER" if r.bar_idx == trigger_idx else ""
            print(f"  [{r.bar_idx:3d}] O={r.bar_open:.2f} H={r.bar_high:.2f} L={r.bar_low:.2f} C={r.bar_close:.2f} V={r.bar_volume:4d} D={r.bar_delta:+5d} CVD={r.cumulative_delta:+7d} BV={r.bar_buy_volume:4d} SV={r.bar_sell_volume:4d}{marker}")
