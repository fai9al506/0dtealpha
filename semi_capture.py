"""semi_capture.py — capture the semi/mega-cap basket strength to Postgres.

Dark Matter's #1 execution rule: index LONGS work when semis are strong, SHORTS
when semis are weak. Validated on our setups (OOS May-Jun: longs 67% WR when semis
strong vs 49% when weak). This collects the signal live so the live filter can use it.

Basket = avg %-from-session-open across NVDA/AMD/AVGO/META/MSFT/GOOGL on 15-min bars.
Idempotent (ON CONFLICT) — re-running every ~15 min during market hours IS the live
capture. Run STANDALONE on the VPS (yfinance/Yahoo blocks datacenter IPs like Railway,
so do NOT run on Railway — mirror the volland_worker / eval_trader VPS pattern).

Usage:   python semi_capture.py            # live: last 5 days
         python semi_capture.py --backfill # history: last 60 days
Schedule: every 15 min, 09:30-16:00 ET (VPS cron / Task Scheduler).
"""
import os, sys, warnings, math, json
warnings.filterwarnings("ignore")
import yfinance as yf
from sqlalchemy import create_engine, text

TKRS = ['NVDA', 'AMD', 'AVGO', 'META', 'MSFT', 'GOOGL']
DDL = """
CREATE TABLE IF NOT EXISTS semi_basket (
    et          timestamp PRIMARY KEY,
    basket_pct  numeric,
    n_names     int,
    details     jsonb
);"""

def run(period="5d"):
    eng = create_engine(os.environ['DATABASE_URL'])
    with eng.begin() as c:
        c.execute(text(DDL))
    df = yf.download(TKRS, period=period, interval='15m', progress=False, auto_adjust=True)
    if df is None or df.empty:
        print("[semi_capture] no data from yfinance", flush=True); return 0
    close = df['Close'].copy()
    if close.index.tz is not None:
        close.index = close.index.tz_convert('America/New_York').tz_localize(None)
    close = close.between_time("09:30", "16:00")
    rows = []
    for day, g in close.groupby(close.index.normalize()):
        opens = {t: g[t].dropna().iloc[0] for t in TKRS if g[t].dropna().shape[0] > 0}
        for ts, row in g.iterrows():
            per = {t: round(float((row[t]-opens[t])/opens[t]*100), 3)
                   for t in TKRS if t in opens and not math.isnan(row[t])}
            if not per:
                continue
            rows.append({"et": ts.to_pydatetime(),
                         "basket_pct": round(float(sum(per.values())/len(per)), 3),
                         "n": int(len(per)), "details": json.dumps(per)})
    if not rows:
        print("[semi_capture] no bars", flush=True); return 0
    with eng.begin() as c:
        c.execute(text("""
            INSERT INTO semi_basket(et,basket_pct,n_names,details)
            VALUES(:et,:basket_pct,:n,CAST(:details AS jsonb))
            ON CONFLICT (et) DO UPDATE SET basket_pct=EXCLUDED.basket_pct,
              n_names=EXCLUDED.n_names, details=EXCLUDED.details"""), rows)
    latest = rows[-1]
    print(f"[semi_capture] upserted {len(rows)} bars; latest {latest['et']} "
          f"basket={latest['basket_pct']:+.2f}% ({latest['details']})", flush=True)
    return len(rows)

if __name__ == "__main__":
    run(period="60d" if "--backfill" in sys.argv else "5d")
