import os, json
from sqlalchemy import create_engine, text

e = create_engine(os.environ['DATABASE_URL'])
with e.connect() as conn:
    rows = conn.execute(text("""
        SELECT trade_date, bar_idx, bar_open, bar_high, bar_low, bar_close,
               bar_volume, bar_delta, cumulative_delta, ts_start, ts_end
        FROM es_range_bars
        WHERE source = 'rithmic' AND status = 'closed'
        ORDER BY trade_date, bar_idx
    """)).fetchall()
    print(f"Fetched {len(rows)} bars")

    data = []
    for r in rows:
        data.append({
            "td": str(r[0]),
            "idx": r[1],
            "o": float(r[2]),
            "h": float(r[3]),
            "l": float(r[4]),
            "c": float(r[5]),
            "vol": int(r[6]),
            "delta": int(r[7]),
            "cvd": int(r[8]),
            "ts_s": str(r[9]),
            "ts_e": str(r[10]),
        })

    with open("tmp_rithmic_bars.json", "w") as f:
        json.dump(data, f)
    print(f"Saved {len(data)} bars to tmp_rithmic_bars.json")
