"""Download 5-min intraday bars for SPY, QQQ, IWM.
Run AFTER chain downloads are complete and terminal is running."""
import requests, json, os, time
from datetime import date, timedelta

THETA_URL = "http://127.0.0.1:25510"
DATA_DIR = r"C:\Users\Faisa\stock_gex_data"

for sym in ["SPY", "QQQ", "IWM"]:
    sym_dir = os.path.join(DATA_DIR, sym.lower())
    os.makedirs(os.path.join(sym_dir, "intraday"), exist_ok=True)
    out_file = os.path.join(sym_dir, "intraday", f"{sym}_5min.json")

    if os.path.exists(out_file):
        with open(out_file) as f:
            existing = json.load(f)
        print(f"{sym}: Already exists ({len(existing)} bars)")
        continue

    print(f"{sym}: Downloading 5-min bars...")
    end_dt = date.today()
    start_dt = end_dt - timedelta(days=365)
    all_bars = []

    d = start_dt
    while d < end_dt:
        chunk_end = min(d + timedelta(days=30), end_dt)
        d_int = int(d.strftime("%Y%m%d"))
        e_int = int(chunk_end.strftime("%Y%m%d"))

        try:
            r = requests.get(f"{THETA_URL}/v2/hist/stock/ohlc",
                             params={"root": sym, "start_date": d_int, "end_date": e_int,
                                     "ivl": 300000},
                             timeout=60)
            js = r.json()
            resp = js.get("response", [])
            for t in resp:
                if isinstance(t, list) and len(t) >= 8:
                    ms = t[0]
                    h = ms // 3600000
                    m = (ms % 3600000) // 60000
                    all_bars.append({
                        "date": t[7], "time": f"{h:02d}:{m:02d}",
                        "ms_of_day": ms,
                        "open": t[1], "high": t[2], "low": t[3], "close": t[4],
                        "volume": t[5],
                    })
            dates_in_chunk = len(set(t[7] for t in resp if isinstance(t, list)))
            print(f"  {d} to {chunk_end}: {len(resp)} bars, {dates_in_chunk} days")
        except Exception as e:
            print(f"  {d} to {chunk_end}: ERROR {e}")

        time.sleep(0.5)
        d = chunk_end + timedelta(days=1)

    with open(out_file, "w") as f:
        json.dump(all_bars, f)
    dates = set(b["date"] for b in all_bars)
    print(f"  Saved {len(all_bars)} bars, {len(dates)} days\n")

print("Done!")
