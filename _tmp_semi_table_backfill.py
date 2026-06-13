"""Create semi_basket table + backfill historical 15m basket from yfinance.
Gives immediate history so forward-validation starts now; live job appends going forward.
"""
import os, warnings, math, json
warnings.filterwarnings("ignore")
import yfinance as yf
from sqlalchemy import create_engine, text
eng=create_engine(os.environ['DATABASE_URL'])

TKRS=['NVDA','AMD','AVGO','META','MSFT','GOOGL']
DDL="""
CREATE TABLE IF NOT EXISTS semi_basket (
    et          timestamp PRIMARY KEY,   -- ET 15-min bar time
    basket_pct  numeric,                 -- avg %-from-session-open across the 6 names
    n_names     int,
    details     jsonb                    -- per-symbol pct-from-open
);
"""
with eng.begin() as c:
    c.execute(text(DDL))
print("table semi_basket ready", flush=True)

print("fetching 15m semis...", flush=True)
df=yf.download(TKRS, period='60d', interval='15m', progress=False, auto_adjust=True)
close=df['Close'].copy()
if close.index.tz is not None:
    close.index=close.index.tz_convert('America/New_York').tz_localize(None)
close=close.between_time("09:30","16:00")

rows=[]
for day,g in close.groupby(close.index.normalize()):
    opens={t:g[t].dropna().iloc[0] for t in TKRS if g[t].dropna().shape[0]>0}
    for ts,row in g.iterrows():
        per={t:round(float((row[t]-opens[t])/opens[t]*100),3) for t in TKRS if t in opens and not math.isnan(row[t])}
        if not per: continue
        rows.append({"et":ts.to_pydatetime(),"basket_pct":round(float(sum(per.values())/len(per)),3),
                     "n":int(len(per)),"details":json.dumps(per)})
print(f"upserting {len(rows)} bars...", flush=True)
with eng.begin() as c:
    for r in rows:
        c.execute(text("""INSERT INTO semi_basket(et,basket_pct,n_names,details)
            VALUES(:et,:basket_pct,:n,CAST(:details AS jsonb))
            ON CONFLICT (et) DO UPDATE SET basket_pct=EXCLUDED.basket_pct,
              n_names=EXCLUDED.n_names, details=EXCLUDED.details"""), r)
cov=eng.connect().execute(text("SELECT MIN(et),MAX(et),COUNT(*) FROM semi_basket")).fetchone()
print("semi_basket coverage:", cov)
