# 0DTE Alpha — live chain + 5-min history (FastAPI + APScheduler + Postgres)
from fastapi import FastAPI, Response, Query
from datetime import datetime, time as dtime
import os, time, json, requests, pandas as pd, pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, text

# ====== CONFIG ======
USE_LIVE = True
BASE = "https://api.tradestation.com/v3" if USE_LIVE else "https://sim-api.tradestation.com/v3"
AUTH_DOMAIN = "https://signin.tradestation.com"

CID     = os.getenv("TS_CLIENT_ID", "")
SECRET  = os.getenv("TS_CLIENT_SECRET", "")
RTOKEN  = os.getenv("TS_REFRESH_TOKEN", "")
DB_URL  = os.getenv("DATABASE_URL", "")  # Railway Postgres

PULL_EVERY = 15  # seconds
SAVE_EVERY_MIN = 5

app = FastAPI()
NY = pytz.timezone("US/Eastern")

latest_df: pd.DataFrame | None = None
last_run_status = {"ts": None, "ok": False, "msg": "boot"}
_last_saved_at = 0.0

# ====== DB ======
engine = create_engine(DB_URL, pool_pre_ping=True) if DB_URL else None

def db_init():
    if not engine:
        return
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS chain_snapshots (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL,
            exp DATE,
            spot DOUBLE PRECISION,
            columns JSONB NOT NULL,
            rows JSONB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_chain_snapshots_ts ON chain_snapshots (ts DESC);
        """))

# ====== Auth (refresh-token only, headless) ======
_access_token = None
_access_exp_at = 0.0

def ts_access_token() -> str:
    global _access_token, _access_exp_at
    now = time.time()
    if _access_token and now < _access_exp_at - 60:
        return _access_token
    if not (CID and SECRET and RTOKEN):
        raise RuntimeError("Missing env: TS_CLIENT_ID / TS_CLIENT_SECRET / TS_REFRESH_TOKEN")
    r = requests.post(f"{AUTH_DOMAIN}/oauth/token", data={
        "grant_type":"refresh_token",
        "refresh_token": RTOKEN,
        "client_id": CID,
        "client_secret": SECRET,
    }, timeout=15)
    r.raise_for_status()
    tok = r.json()
    _access_token = tok["access_token"]
    _access_exp_at = now + int(tok.get("expires_in", 900))
    return _access_token

def api_get(path, params=None, stream=False, timeout=10):
    hdrs = {"Authorization": f"Bearer {ts_access_token()}"}
    r = requests.get(f"{BASE}{path}", headers=hdrs, params=params or {}, timeout=timeout, stream=stream)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {path} [{r.status_code}] {r.text[:300]}")
    return r

# ====== Time helpers ======
def now_et():
    return datetime.now(NY)

def market_open_now() -> bool:
    t = now_et()
    if t.weekday() >= 5:
        return False
    return dtime(9,30) <= t.time() <= dtime(16,0)

# ====== TS API ======
def get_spx_last() -> float:
    js = api_get("/marketdata/quotes/%24SPX.X", timeout=8).json()
    for q in js.get("Quotes", []):
        if q.get("Symbol") == "$SPX.X":
            v = q.get("Last") or q.get("Close")
            try: return float(v)
            except: return 0.0
    return 0.0

def get_0dte_exp() -> str:
    ymd = now_et().date().isoformat()
    try:
        js = api_get("/marketdata/options/expirations/%24SPXW.X", timeout=10).json()
        for e in js.get("Expirations", []):
            d = str(e.get("Date") or e.get("Expiration") or "")[:10]
            if d == ymd:
                return d
    except: pass
    return ymd

def snapshot_chain(exp_ymd: str, spot: float) -> list[dict]:
    params = {
        "symbol": "$SPXW.X",
        "expiration": exp_ymd,
        "enableGreeks": "true",
        "optionType": "All",
        "priceCenter": f"{spot:.2f}" if spot else "",
        "strikeProximity": 50,
        "strikeInterval": 1,
        "spreadType": "Single",
    }
    js = api_get("/marketdata/options/chains", params=params, timeout=15).json()
    rows = []
    for it in js.get("Options", []):
        legs = it.get("Legs") or []
        leg0 = legs[0] if legs else {}
        side = (leg0.get("OptionType") or it.get("OptionType") or "").lower()
        side = "C" if side.startswith("c") else "P" if side.startswith("p") else "?"
        def fnum(x):
            if x in (None, "", "-", "NaN", "nan"): return None
            try: return float(str(x).replace(",",""))
            except: return None
        rows.append({
            "Type": side,
            "Strike": fnum(leg0.get("StrikePrice")),
            "Bid": fnum(it.get("Bid")), "Ask": fnum(it.get("Ask")), "Last": fnum(it.get("Last")),
            "BidSize": it.get("BidSize"), "AskSize": it.get("AskSize"),
            "Delta": fnum(it.get("Delta") or it.get("TheoDelta")),
            "Gamma": fnum(it.get("Gamma") or it.get("TheoGamma")),
            "Theta": fnum(it.get("Theta") or it.get("TheoTheta")),
            "IV": fnum(it.get("ImpliedVolatility") or it.get("TheoIV")),
            "Vega": fnum(it.get("Vega")),
            "Volume": it.get("TotalVolume") or it.get("Volume"),
            "OpenInterest": it.get("OpenInterest") or it.get("DailyOpenInterest"),
        })
    return rows

CANONICAL_COLS = [
    "C_Volume","C_OpenInterest","C_IV","C_Gamma","C_Delta","C_Bid","C_BidSize","C_Ask","C_AskSize","C_Last",
    "Strike",
    "P_Last","P_Ask","P_AskSize","P_Bid","P_BidSize","P_Delta","P_Gamma","P_IV","P_OpenInterest","P_Volume"
]
DISPLAY_COLS = [
    "Volume","Open Int","IV","Gamma","Delta","BID","BID QTY","ASK","ASK QTY","LAST",
    "Strike",
    "LAST","ASK","ASK QTY","BID","BID QTY","Delta","Gamma","IV","Open Int","Volume"
]

def to_side_by_side(rows: list[dict]) -> pd.DataFrame:
    calls, puts = {}, {}
    for r in rows:
        if r.get("Strike") is None: continue
        (calls if r["Type"]=="C" else puts)[r["Strike"]] = r
    strikes = sorted(set(calls)|set(puts))
    recs=[]
    for k in strikes:
        c, p = calls.get(k,{}), puts.get(k,{})
        recs.append({
            "C_Volume": c.get("Volume"), "C_OpenInterest": c.get("OpenInterest"), "C_IV": c.get("IV"),
            "C_Gamma": c.get("Gamma"), "C_Delta": c.get("Delta"), "C_Bid": c.get("Bid"),
            "C_BidSize": c.get("BidSize"), "C_Ask": c.get("Ask"), "C_AskSize": c.get("AskSize"),
            "C_Last": c.get("Last"),
            "Strike": k,
            "P_Last": p.get("Last"), "P_Ask": p.get("Ask"), "P_AskSize": p.get("AskSize"),
            "P_Bid": p.get("Bid"), "P_BidSize": p.get("BidSize"),
            "P_Delta": p.get("Delta"), "P_Gamma": p.get("Gamma"), "P_IV": p.get("IV"),
            "P_OpenInterest": p.get("OpenInterest"), "P_Volume": p.get("Volume"),
        })
    return pd.DataFrame.from_records(recs, columns=CANONICAL_COLS).sort_values("Strike").reset_index(drop=True)

# ====== Jobs ======
def run_market_job():
    global latest_df, last_run_status
    try:
        if not market_open_now():
            last_run_status = {"ts": now_et().isoformat(), "ok": True, "msg": "outside market hours"}
            return
        spot = get_spx_last()
        exp  = get_0dte_exp()
        df   = to_side_by_side(snapshot_chain(exp, spot))
        if spot and not df.empty:
            df = df.iloc[(df["Strike"]-spot).abs().sort_values().index[:40]].sort_values("Strike").reset_index(drop=True)
        latest_df = df
        last_run_status = {"ts": now_et().isoformat(), "ok": True, "msg": f"exp={exp} spot={round(spot,2)} rows={len(df)}"}
    except Exception as e:
        last_run_status = {"ts": now_et().isoformat(), "ok": False, "msg": f"error: {e}"}

def save_history_job():
    """Every 5 minutes: persist latest_df to Postgres for later analysis/tests."""
    global _last_saved_at
    if not engine:  # no DB configured
        return
    if latest_df is None or latest_df.empty:
        return
    # avoid double saves within same minute if scheduler overlaps
    if time.time() - _last_saved_at < 60:
        return
    try:
        df = latest_df.copy()
        df.columns = DISPLAY_COLS
        payload = {"columns": df.columns.tolist(), "rows": df.fillna("").values.tolist()}
        # spot/exp info from status
        msg = (last_run_status.get("msg") or "")
        spot = None
        exp  = None
        try:
            # parse "exp=YYYY-MM-DD spot=1234.56 rows=40"
            parts = dict(s.split("=") for s in msg.split() if "=" in s)
            spot = float(parts.get("spot",""))
            exp  = parts.get("exp")
        except: pass

        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO chain_snapshots (ts, exp, spot, columns, rows) VALUES (:ts, :exp, :spot, :columns, :rows)"),
                {"ts": now_et(), "exp": exp, "spot": spot,
                 "columns": json.dumps(payload["columns"]),
                 "rows": json.dumps(payload["rows"])}
            )
        _last_saved_at = time.time()
    except Exception as e:
        # keep silent in API; check logs on Railway if needed
        pass

def start_scheduler():
    sch = BackgroundScheduler(timezone="US/Eastern")
    sch.add_job(run_market_job, "interval", seconds=PULL_EVERY, id="pull")
    sch.add_job(save_history_job, "cron", minute=f"*/{SAVE_EVERY_MIN}", id="save")  # every 5 min
    sch.start()

@app.on_event("startup")
def on_startup():
    if engine: db_init()
    start_scheduler()

# ====== Endpoints ======
@app.get("/")
def health():
    return {"status": "ok", "last": last_run_status}

@app.get("/api/snapshot")
def snapshot():
    if latest_df is None or latest_df.empty:
        return {"columns": DISPLAY_COLS, "rows": []}
    df = latest_df.copy()
    df.columns = DISPLAY_COLS
    return {"columns": df.columns.tolist(), "rows": df.fillna("").values.tolist()}

@app.get("/table")
def html_table():
    if latest_df is None or latest_df.empty:
        body = "<p>No data yet. If market is open, it will appear within ~15s.</p>"
    else:
        df = latest_df.copy(); df.columns = DISPLAY_COLS
        body = df.to_html(index=False).replace('class="dataframe"', 'class="table"')
    page = f"""
    <html><head><meta charset="utf-8"><title>0DTE Alpha</title>
    <style>body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#0a0a0a;color:#e5e5e5;padding:20px}}
    table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #333;padding:6px 8px;text-align:right}}
    th{{background:#111;position:sticky;top:0}}td:first-child,th:first-child{{text-align:center}}</style>
    </head><body>
      <h2>SPXW 0DTE — live table</h2>
      <small>Last run: {last_run_status.get("ts")} — {last_run_status.get("msg")}</small>
      {body}
      <script>setTimeout(()=>location.reload(), 15000);</script>
    </body></html>"""
    return Response(content=page, media_type="text/html")

@app.get("/api/history")
def api_history(limit: int = Query(288, ge=1, le=5000)):
    """
    Last N snapshots (default 288 ≈ one trading day @5-min cadence).
    Returns: [{ts, exp, spot, columns, rows}]
    """
    if not engine:
        return {"error":"DATABASE_URL not set"}
    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT ts, exp, spot, columns, rows FROM chain_snapshots ORDER BY ts DESC LIMIT :lim"
        ), {"lim": limit}).mappings().all()
    for r in rows:
        r["columns"] = json.loads(r["columns"])
        r["rows"]    = json.loads(r["rows"])
        r["ts"]      = r["ts"].isoformat()
    return rows

@app.get("/download/history.csv")
def download_history_csv(limit: int = Query(288, ge=1, le=5000)):
    if not engine:
        return Response("DATABASE_URL not set", media_type="text/plain", status_code=500)
    with engine.begin() as conn:
        recs = conn.execute(text(
            "SELECT ts, exp, spot, columns, rows FROM chain_snapshots ORDER BY ts DESC LIMIT :lim"
        ), {"lim": limit}).mappings().all()
    # flatten
    out = []
    for r in recs:
        cols = json.loads(r["columns"]); rows = json.loads(r["rows"])
        for arr in rows:
            obj = {"ts": r["ts"].isoformat(), "exp": r["exp"], "spot": r["spot"]}
            obj.update({cols[i]: arr[i] for i in range(len(cols))})
            out.append(obj)
    df = pd.DataFrame(out)
    csv = df.to_csv(index=False)
    return Response(csv, media_type="text/csv", headers={"Content-Disposition":"attachment; filename=history.csv"})
