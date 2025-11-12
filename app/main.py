# 0DTE Alpha — live chain + 5-min history (FastAPI + APScheduler + Postgres + Plotly front-end)
from fastapi import FastAPI, Response, Query
from fastapi.responses import HTMLResponse
from datetime import datetime, time as dtime
import os, time, json, requests, pandas as pd, pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, text
from threading import Lock

# ====== CONFIG ======
USE_LIVE = True
BASE = "https://api.tradestation.com/v3" if USE_LIVE else "https://sim-api.tradestation.com/v3"
AUTH_DOMAIN = "https://signin.tradestation.com"

CID     = os.getenv("TS_CLIENT_ID", "")
SECRET  = os.getenv("TS_CLIENT_SECRET", "")
RTOKEN  = os.getenv("TS_REFRESH_TOKEN", "")
DB_URL  = os.getenv("DATABASE_URL", "")  # Railway Postgres

# SQLAlchemy psycopg v3 URI
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

# Cadence
PULL_EVERY     = 30   # seconds
SAVE_EVERY_MIN = 5    # minutes

# Chain window
STREAM_SECONDS = 2.0
TARGET_STRIKES = 40

# ====== APP ======
app = FastAPI()
NY = pytz.timezone("US/Eastern")

latest_df: pd.DataFrame | None = None
last_run_status = {"ts": None, "ok": False, "msg": "boot"}
_last_saved_at = 0.0
_df_lock = Lock()

# ====== DB ======
engine = create_engine(DB_URL, pool_pre_ping=True) if DB_URL else None

def db_init():
    if not engine:
        print("[db] DATABASE_URL missing; history disabled", flush=True)
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
    print("[db] ready", flush=True)

# ====== Auth ======
REFRESH_EARLY_SEC = 300
_access_token = None
_access_exp_at = 0.0
_refresh_token = RTOKEN or ""

def _stamp_token(exp_in: int):
    global _access_exp_at
    _access_exp_at = time.time() + int(exp_in or 900) - REFRESH_EARLY_SEC

def ts_access_token() -> str:
    global _access_token, _refresh_token
    now = time.time()
    if _access_token and now < _access_exp_at - 60:
        return _access_token
    if not (CID and SECRET and _refresh_token):
        raise RuntimeError("Missing env: TS_CLIENT_ID / TS_CLIENT_SECRET / TS_REFRESH_TOKEN")
    r = requests.post(
        f"{AUTH_DOMAIN}/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": _refresh_token,
            "client_id": CID,
            "client_secret": SECRET,
        },
        timeout=15,
    )
    if r.status_code >= 400:
        raise RuntimeError(f"token refresh [{r.status_code}] {r.text[:300]}")
    tok = r.json()
    _access_token = tok["access_token"]
    if tok.get("refresh_token"):
        _refresh_token = tok["refresh_token"]
    _stamp_token(tok.get("expires_in", 900))
    print("[auth] token refreshed; expires_in:", tok.get("expires_in"), flush=True)
    return _access_token

def api_get(path, params=None, stream=False, timeout=10):
    def do_req(h):
        return requests.get(f"{BASE}{path}", headers=h, params=params or {}, timeout=timeout, stream=stream)
    token = ts_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    r = do_req(headers)
    if r.status_code == 401:
        try:
            _ = ts_access_token()
            headers["Authorization"] = f"Bearer {_access_token}"
            r = do_req(headers)
        except Exception:
            pass
    if stream:
        if r.status_code != 200:
            raise RuntimeError(f"STREAM {path} [{r.status_code}] {r.text[:300]}")
        return r
    if r.status_code >= 400:
        raise RuntimeError(f"GET {path} [{r.status_code}] {r.text[:300]}")
    return r

# ====== Time helpers ======
def now_et():
    return datetime.now(NY)

def fmt_et(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M %Z")

def market_open_now() -> bool:
    t = now_et()
    if t.weekday() >= 5:
        return False
    return dtime(9,30) <= t.time() <= dtime(16,0)

# ====== TS helpers ======
def get_spx_last() -> float:
    js = api_get("/marketdata/quotes/%24SPX.X", timeout=8).json()
    for q in js.get("Quotes", []):
        if q.get("Symbol") == "$SPX.X":
            v = q.get("Last") or q.get("Close")
            try:
                return float(v)
            except:
                return 0.0
    return 0.0

def get_0dte_exp() -> str:
    ymd = now_et().date().isoformat()
    try:
        js = api_get("/marketdata/options/expirations/%24SPXW.X", timeout=10).json()
        for e in js.get("Expirations", []):
            d = str(e.get("Date") or e.get("Expiration") or "")[:10]
            if d == ymd:
                return d
    except Exception as e:
        print("[exp] lookup failed; using today", ymd, "|", e, flush=True)
    return ymd

def _expiration_variants(ymd: str):
    yield ymd
    try:
        yield datetime.strptime(ymd, "%Y-%m-%d").strftime("%m-%d-%Y")
    except Exception:
        pass
    yield ymd + "T00:00:00Z"

def _fnum(x):
    if x in (None, "", "-", "NaN", "nan"):
        return None
    try:
        return float(str(x).replace(",", ""))
    except:
        return None

def _consume_chain_stream(r, max_seconds: float) -> list[dict]:
    out, start = [], time.time()
    try:
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                if time.time() - start > max_seconds:
                    break
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict) and obj.get("StreamStatus") == "EndSnapshot":
                break
            if isinstance(obj, dict):
                out.append(obj)
            if time.time() - start > max_seconds:
                break
    finally:
        try:
            r.close()
        except Exception:
            pass
    return out

def get_chain_rows(exp_ymd: str, spot: float) -> list[dict]:
    params_stream = {
        "spreadType": "Single",
        "enableGreeks": "true",
        "priceCenter": f"{spot:.2f}" if spot else "",
        "strikeProximity": 50,
        "optionType": "All",
        "strikeInterval": 1
    }
    last_err = None
    for exp in _expiration_variants(exp_ymd):
        try:
            p = dict(params_stream); p["expiration"] = exp
            r = api_get("/marketdata/stream/options/chains/%24SPXW.X", params=p, stream=True, timeout=8)
            objs = _consume_chain_stream(r, max_seconds=STREAM_SECONDS)
            if objs:
                rows = []
                for it in objs:
                    legs = it.get("Legs") or []
                    leg0 = legs[0] if legs else {}
                    side = (leg0.get("OptionType") or it.get("OptionType") or "").lower()
                    side = "C" if side.startswith("c") else "P" if side.startswith("p") else "?"
                    rows.append({
                        "Type": side,
                        "Strike": _fnum(leg0.get("StrikePrice")),
                        "Bid": _fnum(it.get("Bid")), "Ask": _fnum(it.get("Ask")), "Last": _fnum(it.get("Last")),
                        "BidSize": it.get("BidSize"), "AskSize": it.get("AskSize"),
                        "Delta": _fnum(it.get("Delta") or it.get("TheoDelta")),
                        "Gamma": _fnum(it.get("Gamma") or it.get("TheoGamma")),
                        "Theta": _fnum(it.get("Theta") or it.get("TheoTheta")),
                        "IV": _fnum(it.get("ImpliedVolatility") or it.get("TheoIV")),
                        "Vega": _fnum(it.get("Vega")),
                        "Volume": _fnum(it.get("TotalVolume") or it.get("Volume")),
                        "OpenInterest": it.get("OpenInterest") or it.get("DailyOpenInterest"),
                    })
                if rows:
                    return rows
        except Exception as e:
            last_err = e
            continue

    params_snap = {
        "symbol": "$SPXW.X",
        "enableGreeks": "true",
        "optionType": "All",
        "priceCenter": f"{spot:.2f}" if spot else "",
        "strikeProximity": 50,
        "strikeInterval": 1,
        "spreadType": "Single",
    }
    for exp in _expiration_variants(exp_ymd):
        try:
            p = dict(params_snap); p["expiration"] = exp
            js = api_get("/marketdata/options/chains", params=p, timeout=12).json()
            rows = []
            for it in js.get("Options", []):
                legs = it.get("Legs") or []
                leg0 = legs[0] if legs else {}
                side = (leg0.get("OptionType") or it.get("OptionType") or "").lower()
                side = "C" if side.startswith("c") else "P" if side.startswith("p") else "?"
                rows.append({
                    "Type": side,
                    "Strike": _fnum(leg0.get("StrikePrice")),
                    "Bid": _fnum(it.get("Bid")), "Ask": _fnum(it.get("Ask")), "Last": _fnum(it.get("Last")),
                    "BidSize": it.get("BidSize"), "AskSize": it.get("AskSize"),
                    "Delta": _fnum(it.get("Delta") or it.get("TheoDelta")),
                    "Gamma": _fnum(it.get("Gamma") or it.get("TheoGamma")),
                    "Theta": _fnum(it.get("Theta") or it.get("TheoTheta")),
                    "IV": _fnum(it.get("ImpliedVolatility") or it.get("TheoIV")),
                    "Vega": _fnum(it.get("Vega")),
                    "Volume": _fnum(it.get("TotalVolume") or it.get("Volume")),
                    "OpenInterest": it.get("OpenInterest") or it.get("DailyOpenInterest"),
                })
            if rows:
                return rows
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"SPXW chain fetch failed; last_err={last_err}")

# ====== shaping ======
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
        if r.get("Strike") is None:
            continue
        (calls if r["Type"]=="C" else puts)[r["Strike"]] = r
    strikes = sorted(set(calls) | set(puts))
    recs = []
    for k in strikes:
        c, p = calls.get(k, {}), puts.get(k, {})
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
    df = pd.DataFrame.from_records(recs, columns=CANONICAL_COLS)
    if not df.empty:
        df = df.sort_values("Strike").reset_index(drop=True)
    return df

def pick_centered(df: pd.DataFrame, spot: float, n: int) -> pd.DataFrame:
    if df is None or df.empty or not spot:
        return df
    idx = (df["Strike"] - spot).abs().sort_values().index[:n]
    return df.loc[sorted(idx)].reset_index(drop=True)

# ====== jobs ======
def run_market_job():
    global latest_df, last_run_status
    try:
        if not market_open_now():
            last_run_status = {"ts": fmt_et(now_et()), "ok": True, "msg": "outside market hours"}
            print("[pull] skipped (closed)", last_run_status["ts"], flush=True)
            return
        spot = get_spx_last()
        exp  = get_0dte_exp()
        rows = get_chain_rows(exp, spot)
        df   = pick_centered(to_side_by_side(rows), spot, TARGET_STRIKES)
        with _df_lock:
            latest_df = df.copy()
        last_run_status = {"ts": fmt_et(now_et()), "ok": True, "msg": f"exp={exp} spot={round(spot or 0,2)} rows={len(df)}"}
        print("[pull] OK", last_run_status["msg"], flush=True)
    except Exception as e:
        last_run_status = {"ts": fmt_et(now_et()), "ok": False, "msg": f"error: {e}"}
        print("[pull] ERROR", e, flush=True)

def save_history_job():
    global _last_saved_at
    if not engine:
        return
    with _df_lock:
        if latest_df is None or latest_df.empty:
            return
        df_copy = latest_df.copy()
    if time.time() - _last_saved_at < 60:
        return
    try:
        df = df_copy
        df.columns = DISPLAY_COLS
        payload = {"columns": df.columns.tolist(), "rows": df.fillna("").values.tolist()}
        msg = (last_run_status.get("msg") or "")
        spot = None; exp  = None
        try:
            parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
            spot = float(parts.get("spot",""))
            exp  = parts.get("exp")
        except:
            pass
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO chain_snapshots (ts, exp, spot, columns, rows) VALUES (:ts, :exp, :spot, :columns, :rows)"),
                {"ts": now_et(), "exp": exp, "spot": spot,
                 "columns": json.dumps(payload["columns"]),
                 "rows": json.dumps(payload["rows"])}
            )
        _last_saved_at = time.time()
        print("[save] snapshot inserted", flush=True)
    except Exception as e:
        print("[save] failed:", e, flush=True)

def start_scheduler():
    sch = BackgroundScheduler(timezone="US/Eastern")
    sch.add_job(run_market_job, "interval", seconds=PULL_EVERY, id="pull", coalesce=True, max_instances=1)
    sch.add_job(save_history_job, "cron", minute=f"*/{SAVE_EVERY_MIN}", id="save", coalesce=True, max_instances=1)
    sch.start()
    print("[sched] started; pull every", PULL_EVERY, "s; save every", SAVE_EVERY_MIN, "min", flush=True)
    return sch

REQUIRED_ENVS = ["TS_CLIENT_ID","TS_CLIENT_SECRET","TS_REFRESH_TOKEN","DATABASE_URL"]
def missing_envs():
    return [k for k in REQUIRED_ENVS if not os.getenv(k)]

scheduler: BackgroundScheduler | None = None

@app.on_event("startup")
def on_startup():
    miss = missing_envs()
    if miss:
        print("[env] missing:", miss, flush=True)
    if engine:
        db_init()
    else:
        print("[db] engine not created (no DATABASE_URL)", flush=True)
    global scheduler
    scheduler = start_scheduler()

@app.on_event("shutdown")
def on_shutdown():
    global scheduler
    if scheduler:
        scheduler.shutdown()
        print("[sched] stopped", flush=True)

# ====== API ======
@app.get("/api/series")
def api_series():
    with _df_lock:
        df = None if (latest_df is None or latest_df.empty) else latest_df.copy()
    if df is None or df.empty:
        return {
            "strikes": [], "callVol": [], "putVol": [], "callOI": [], "putOI": [],
            "callGEX": [], "putGEX": [], "netGEX": [], "spot": None
        }
    sdf = df.sort_values("Strike")
    s  = pd.to_numeric(sdf["Strike"], errors="coerce").fillna(0.0).astype(float)
    call_vol = pd.to_numeric(sdf["C_Volume"],       errors="coerce").fillna(0.0).astype(float)
    put_vol  = pd.to_numeric(sdf["P_Volume"],       errors="coerce").fillna(0.0).astype(float)
    call_oi  = pd.to_numeric(sdf["C_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
    put_oi   = pd.to_numeric(sdf["P_OpenInterest"], errors="coerce").fillna(0.0).astype(float)
    c_gamma  = pd.to_numeric(sdf["C_Gamma"], errors="coerce").fillna(0.0).astype(float)
    p_gamma  = pd.to_numeric(sdf["P_Gamma"], errors="coerce").fillna(0.0).astype(float)
    call_gex = ( c_gamma * call_oi * 100.0).astype(float)
    put_gex  = (-p_gamma * put_oi  * 100.0).astype(float)
    net_gex  = (call_gex + put_gex).astype(float)
    spot = None
    try:
        parts = dict(splt.split("=", 1) for splt in (last_run_status.get("msg") or "").split() if "=" in splt)
        spot = float(parts.get("spot",""))
    except:
        spot = None
    return {
        "strikes": s.tolist(),
        "callVol": call_vol.tolist(), "putVol": put_vol.tolist(),
        "callOI":  call_oi.tolist(),  "putOI":  put_oi.tolist(),
        "callGEX": call_gex.tolist(), "putGEX": put_gex.tolist(), "netGEX": net_gex.tolist(),
        "spot": spot
    }

@app.get("/api/health")
def api_health():
    return {"status": "ok", "last": last_run_status}

@app.get("/status")
def status():
    return last_run_status

@app.get("/api/snapshot")
def snapshot():
    with _df_lock:
        df = None if (latest_df is None or latest_df.empty) else latest_df.copy()
    if df is None or df.empty:
        return {"columns": DISPLAY_COLS, "rows": []}
    df.columns = DISPLAY_COLS
    return {"columns": df.columns.tolist(), "rows": df.fillna("").values.tolist()}

@app.get("/api/history")
def api_history(limit: int = Query(288, ge=1, le=5000)):
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

# ====== TABLE & DASHBOARD HTML TEMPLATES ======

TABLE_HTML_TEMPLATE = """
<html><head><meta charset="utf-8"><title>0DTE Alpha</title>
<style>
  body { font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
         background:#0a0a0a; color:#e5e5e5; padding:20px; }
  .last { color:#9ca3af; font-size:12px; line-height:1.25; margin:0 0 10px 0; }
  table.table { border-collapse:collapse; width:100%; font-size:12px; }
  .table th,.table td { border:1px solid #333; padding:6px 8px; text-align:right; }
  .table th { background:#111; position:sticky; top:0; z-index:1; }
  .table td:nth-child(7), .table th:nth-child(7) { background:#111; text-align:center; }
  .table td:first-child, .table th:first-child { text-align:center; }
  .table tr.atm td { background:#1a2634; }
</style>
</head><body>
  <h2>SPXW 0DTE — live table</h2>
  <div class="last">
    Last run: __TS__<br>exp=__EXP__<br>spot=__SPOT__<br>rows=__ROWS__
  </div>
  __BODY__
  <script>setTimeout(()=>location.reload(), __PULL_MS__);</script>
</body></html>
"""

DASH_HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>SPXW 0DTE — Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <script type="text/javascript" src="https://s3.tradingview.com/tv.js"></script>
  <style>
    :root {
      --bg:#0b0c10; --panel:#121417; --muted:#8a8f98; --text:#e6e7e9; --border:#23262b;
      --green:#22c55e; --red:#ef4444; --blue:#60a5fa;
    }
    * { box-sizing: border-box; }
    body { margin:0; background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; }
    .layout { display: grid; grid-template-columns: 240px 1fr; min-height: 100vh; }
    .sidebar {
      background: var(--panel); border-right: 1px solid var(--border); padding: 20px 16px; position: sticky; top:0; height:100vh;
    }
    .brand { font-weight: 700; margin-bottom: 6px; }
    .small { color: var(--muted); font-size: 12px; margin-bottom: 16px; }
    .status { display:flex; gap:10px; align-items:center; padding:10px; border:1px solid var(--border); border-radius:10px; background:#0f1216; margin-bottom:14px; }
    .dot { width:10px; height:10px; border-radius:999px; background:__STATUS_COLOR__; }
    .nav { display: grid; gap: 8px; margin-top: 8px; }
    .btn { display:block; width:100%; text-align:left; padding:10px 12px; border-radius:10px; border:1px solid var(--border); background:transparent; color:var(--text); cursor:pointer; }
    .btn.active { background:#121a2e; border-color:#2a3a57; }
    .content { padding: 18px; }
    .panel { background: var(--panel); border:1px solid var(--border); border-radius:14px; padding:12px; overflow:hidden; }
    .header { display:flex; align-items:center; justify-content:space-between; padding:6px 10px 12px; border-bottom:1px solid var(--border); margin-bottom:10px;}
    .pill { font-size:12px; padding:4px 8px; border:1px solid var(--border); border-radius:999px; color:var(--muted); }

    .charts { display:flex; flex-direction:column; gap:24px; }
    iframe { width:100%; height: calc(100vh - 180px); border:0; background:#0f1115; }
    #volChart, #oiChart, #gexChart { width:100%; height:480px; }

    .spot-grid { display:grid; grid-template-columns: 2fr 1fr 1fr; gap:12px; align-items:stretch; }
    .card { background: var(--panel); border:1px solid var(--border); border-radius:14px; padding:12px; min-height:420px; display:flex; flex-direction:column }
    .card h3 { margin:0 0 8px; font-size:14px; color:var(--muted); font-weight:600 }
    .plot { width:100%; height:100% }
    @media (max-width: 1200px) { .spot-grid { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <div class="layout">
    <aside class="sidebar">
      <div class="brand">SPXW 0DTE</div>
      <div class="small">Live chain + charts</div>
      <div class="status">
        <span class="dot"></span>
        <div>
          <div style="font-weight:600;">__STATUS_TEXT__</div>
          <div class="small">Last run: __LAST_TS__ — __LAST_MSG__</div>
        </div>
      </div>
      <div class="nav">
        <button class="btn active" id="tabTable">Table</button>
        <button class="btn" id="tabCharts">Charts</button>
        <button class="btn" id="tabSpot">Spot</button>
      </div>
      <div class="small" style="margin-top:12px">Charts auto-refresh while visible.</div>
      <div class="small" style="margin-top:18px">
        <a href="/api/snapshot" style="color:var(--muted)">Current JSON</a> ·
        <a href="/api/history"  style="color:var(--muted)">History</a> ·
        <a href="/download/history.csv" style="color:var(--muted)">CSV</a>
      </div>
    </aside>

    <main class="content">
      <div id="viewTable" class="panel">
        <div class="header"><div><strong>Live Chain Table</strong></div><div class="pill">auto-refresh</div></div>
        <iframe id="tableFrame" src="/table"></iframe>
      </div>

      <div id="viewCharts" class="panel" style="display:none">
        <div class="header"><div><strong>GEX, Volume & Open Interest</strong></div><div class="pill">green = calls · red = puts · blue = net</div></div>
        <div class="charts">
          <div id="gexChart"></div>
          <div id="volChart"></div>
          <div id="oiChart"></div>
        </div>
      </div>

      <div id="viewSpot" class="panel" style="display:none">
        <div class="header"><div><strong>Spot</strong></div><div class="pill">TradingView price + GEX & VOL by strike</div></div>
        <div class="spot-grid">
          <div class="card">
            <h3>SPX Price — TradingView</h3>
            <div id="tvContainer" class="plot"></div>
          </div>
          <div class="card"><h3>Net GEX by Strike</h3><div id="gexSidePlot" class="plot"></div></div>
          <div class="card"><h3>VOL by Strike (Calls vs Puts)</h3><div id="volSidePlot" class="plot"></div></div>
        </div>
      </div>
    </main>
  </div>

  <script>
    const PULL_EVERY = __PULL_MS__;

    // Tabs
    const tabTable=document.getElementById('tabTable'),
          tabCharts=document.getElementById('tabCharts'),
          tabSpot=document.getElementById('tabSpot');
    const viewTable=document.getElementById('viewTable'),
          viewCharts=document.getElementById('viewCharts'),
          viewSpot=document.getElementById('viewSpot');

    function setActive(btn){
      [tabTable,tabCharts,tabSpot].forEach(b=>b.classList.remove('active'));
      btn.classList.add('active');
    }
    function showTable(){ setActive(tabTable); viewTable.style.display=''; viewCharts.style.display='none'; viewSpot.style.display='none'; stopCharts(); stopSpot(); }
    function showCharts(){ setActive(tabCharts); viewTable.style.display='none'; viewCharts.style.display=''; viewSpot.style.display='none'; startCharts(); stopSpot(); }
    function showSpot(){ setActive(tabSpot); viewTable.style.display='none'; viewCharts.style.display='none'; viewSpot.style.display=''; startSpot(); stopCharts(); }
    tabTable.addEventListener('click', showTable);
    tabCharts.addEventListener('click', showCharts);
    tabSpot.addEventListener('click', showSpot);

    // ===== Main charts =====
    const volDiv=document.getElementById('volChart'),
          oiDiv=document.getElementById('oiChart'),
          gexDiv=document.getElementById('gexChart');
    let chartsTimer=null, firstDraw=true;

    async function fetchSeries(){ const r=await fetch('/api/series',{cache:'no-store'}); return await r.json(); }

    function verticalSpotShape(spot,yMax){
      if(spot==null) return null;
      return {type:'line',x0:spot,x1:spot,y0:0,y1:yMax,line:{color:'#9aa0a6',width:2,dash:'dot'},xref:'x',yref:'y'};
    }

    function buildLayout(title,xTitle,yTitle,spot,yMax){
      const shape=verticalSpotShape(spot,yMax);
      return {
        title:{text:title,font:{size:16}},
        xaxis:{title:xTitle,gridcolor:'#20242a',tickfont:{size:11}},
        yaxis:{title:yTitle,gridcolor:'#20242a',tickfont:{size:11}},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        font:{color:'#e6e7e9'},
        margin:{t:40,r:16,b:48,l:48},
        barmode:'group',
        shapes:shape?[shape]:[]
      };
    }

    function tracesForBars(strikes,callArr,putArr,yLabel){
      return [
        {type:'bar', name:'Calls '+yLabel, x:strikes, y:callArr, marker:{color:'#22c55e'}, offsetgroup:'calls',
         hovertemplate:"Strike %{x}<br>Calls: %{y}<extra></extra>"},
        {type:'bar', name:'Puts '+yLabel,  x:strikes, y:putArr,  marker:{color:'#ef4444'}, offsetgroup:'puts',
         hovertemplate:"Strike %{x}<br>Puts: %{y}<extra></extra>"}
      ];
    }

    function tracesForGEX(strikes,callGEX,putGEX,netGEX){
      return [
        {type:'bar', name:'Call GEX', x:strikes, y:callGEX, marker:{color:'#22c55e'}, offsetgroup:'call_gex',
         hovertemplate:"Strike %{x}<br>Call GEX: %{y:.2f}<extra></extra>"},
        {type:'bar', name:'Put GEX',  x:strikes, y:putGEX,  marker:{color:'#ef4444'}, offsetgroup:'put_gex',
         hovertemplate:"Strike %{x}<br>Put GEX: %{y:.2f}<extra></extra>"},
        {type:'bar', name:'Net GEX',  x:strikes, y:netGEX, marker:{color:'#60a5fa'}, offsetgroup:'net_gex', opacity:0.85,
         hovertemplate:"Strike %{x}<br>Net GEX: %{y:.2f}<extra></extra>"}
      ];
    }

    async function drawOrUpdate(){
      const data = await fetchSeries();
      if (!data || !data.strikes || data.strikes.length === 0) return;

      const strikes = data.strikes, spot = data.spot;
      const vMax = Math.max(0, ...data.callVol, ...data.putVol) * 1.05;
      const oiMax= Math.max(0, ...data.callOI,  ...data.putOI ) * 1.05;
      const gAbs = [...data.callGEX, ...data.putGEX, ...data.netGEX].map(v=>Math.abs(v));
      const gMax = (gAbs.length? Math.max(...gAbs):0) * 1.05;

      const gexLayout = buildLayout('Gamma Exposure (GEX)','Strike','GEX',spot,gMax);
      const volLayout = buildLayout('Volume','Strike','Volume',spot,vMax);
      const oiLayout  = buildLayout('Open Interest','Strike','Open Interest',spot,oiMax);

      const gexTraces = tracesForGEX(strikes, data.callGEX, data.putGEX, data.netGEX);
      const volTraces = tracesForBars(strikes, data.callVol, data.putVol, 'Vol');
      const oiTraces  = tracesForBars(strikes, data.callOI,  data.putOI,  'OI');

      if (firstDraw){
        Plotly.newPlot(gexDiv, gexTraces, gexLayout, {displayModeBar:false,responsive:true});
        Plotly.newPlot(volDiv, volTraces, volLayout, {displayModeBar:false,responsive:true});
        Plotly.newPlot(oiDiv,  oiTraces,  oiLayout,  {displayModeBar:false,responsive:true});
        firstDraw=false;
      } else {
        Plotly.react(gexDiv, gexTraces, gexLayout, {displayModeBar:false,responsive:true});
        Plotly.react(volDiv, volTraces, volLayout, {displayModeBar:false,responsive:true});
        Plotly.react(oiDiv,  oiTraces,  oiLayout,  {displayModeBar:false,responsive:true});
      }
    }
    function startCharts(){ drawOrUpdate(); if (chartsTimer) clearInterval(chartsTimer); chartsTimer=setInterval(drawOrUpdate, PULL_EVERY); }
    function stopCharts(){ if (chartsTimer){ clearInterval(chartsTimer); chartsTimer=null; } }

    // ===== Spot: TradingView + GEX/VOL =====
    const gexSideDiv=document.getElementById('gexSidePlot'),
          volSideDiv=document.getElementById('volSidePlot');
    let spotTimer=null;
    let tvInitialized=false;

    function renderTV(){
      if (tvInitialized) return;
      if (typeof TradingView === 'undefined') return;
      if (!document.getElementById('tvContainer')) return;
      tvInitialized = true;
      new TradingView.widget({
        "symbol": "BLACKBULL:SPX500",
        "interval": "3",
        "container_id": "tvContainer",
        "autosize": true,
        "theme": "dark",
        "style": "1",
        "locale": "en",
        "hide_legend": false,
        "hide_side_toolbar": false,
        "hide_top_toolbar": false
      });
    }

    function renderSpotFromSeries(data){
      const strikes = data.strikes || [];
      if (!strikes.length) return;
      const yMin = Math.min(...strikes), yMax = Math.max(...strikes);
      const pad = (yMax - yMin) * 0.02;

      const gexNet = data.netGEX || [];
      const gMax = Math.max(1, ...gexNet.map(v=>Math.abs(v))) * 1.1;
      const gex = {
        type:'bar',
        orientation:'h',
        x:gexNet,
        y:strikes,
        marker:{color:'#60a5fa'},
        hovertemplate:'Strike %{y}<br>Net GEX %{x:.0f}<extra></extra>'
      };
      Plotly.react(gexSideDiv, [gex], {
        margin:{l:60,r:16,t:10,b:30},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        xaxis:{title:'Net GEX', gridcolor:'#20242a', range:[-gMax, gMax]},
        yaxis:{title:'Strike', range:[yMin-pad, yMax+pad], gridcolor:'#20242a'},
        font:{color:'#e6e7e9'}
      }, {displayModeBar:false,responsive:true});

      const callVol = data.callVol || [];
      const putVol  = data.putVol  || [];
      const vMax = Math.max(1, ...callVol, ...putVol) * 1.1;
      const volCalls = {
        type:'bar',
        orientation:'h',
        name:'Calls',
        x:callVol,
        y:strikes,
        marker:{color:'#22c55e'},
        hovertemplate:'Strike %{y}<br>Calls %{x}<extra></extra>'
      };
      const volPuts = {
        type:'bar',
        orientation:'h',
        name:'Puts',
        x:putVol,
        y:strikes,
        marker:{color:'#ef4444'},
        hovertemplate:'Strike %{y}<br>Puts %{x}<extra></extra>'
      };
      Plotly.react(volSideDiv, [volCalls, volPuts], {
        margin:{l:60,r:16,t:10,b:30},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        xaxis:{title:'VOL', gridcolor:'#20242a', range:[0, vMax]},
        yaxis:{title:'Strike', range:[yMin-pad, yMax+pad], gridcolor:'#20242a'},
        barmode:'group',
        font:{color:'#e6e7e9'}
      }, {displayModeBar:false,responsive:true});
    }

    async function tickSpot(){
      const data = await fetchSeries();
      renderSpotFromSeries(data);
    }
    function startSpot(){
      renderTV();
      tickSpot();
      if (spotTimer) clearInterval(spotTimer);
      spotTimer = setInterval(tickSpot, PULL_EVERY);
    }
    function stopSpot(){
      if (spotTimer){ clearInterval(spotTimer); spotTimer=null; }
    }

    // default
    showTable();
  </script>
</body>
</html>
"""

# ====== TABLE ENDPOINT ======
@app.get("/table")
def html_table():
    ts  = last_run_status.get("ts") or ""
    msg = last_run_status.get("msg") or ""
    parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
    exp  = parts.get("exp", "")
    spot_str = parts.get("spot", "")
    rows = parts.get("rows", "")

    with _df_lock:
        df_src = None if (latest_df is None or latest_df.empty) else latest_df.copy()

    if df_src is None or df_src.empty:
        body_html = "<p>No data yet. If market is open, it will appear within ~30s.</p>"
    else:
        base = df_src
        wanted = [
            "C_Volume","C_OpenInterest","C_IV","C_Gamma","C_Delta","C_Last",
            "Strike",
            "P_Last","P_Delta","P_Gamma","P_IV","P_OpenInterest","P_Volume",
        ]
        df = base[wanted].copy()
        df.columns = [
            "Volume","Open Int","IV","Gamma","Delta","LAST",
            "Strike",
            "LAST","Delta","Gamma","IV","Open Int","Volume",
        ]
        try:
            spot_val = float(spot_str)
        except:
            spot_val = None
        atm_idx = None
        if spot_val:
            try:
                atm_idx = (df["Strike"] - spot_val).abs().idxmin()
            except:
                pass

        comma_cols = {"Volume", "Open Int"}
        def fmt_val(col, v):
            if pd.isna(v):
                return ""
            if col in comma_cols:
                try:
                    f = float(v)
                    return f"{int(f):,}" if abs(f - int(f)) < 1e-9 else f"{f:,.2f}"
                except:
                    return str(v)
            return str(v)

        thead = "<tr>" + "".join(f"<th>{h}</th>" for h in df.columns) + "</tr>"
        trs = []
        for i, row in enumerate(df.itertuples(index=False), start=0):
            cls = ' class="atm"' if (atm_idx is not None and i == atm_idx) else ""
            tds = [f"<td>{fmt_val(col, v)}</td>" for col, v in zip(df.columns, row)]
            trs.append(f"<tr{cls}>" + "".join(tds) + "</tr>")
        body_html = f'<table class="table"><thead>{thead}</thead><tbody>{"".join(trs)}</tbody></table>'

    html = (TABLE_HTML_TEMPLATE
            .replace("__TS__", ts)
            .replace("__EXP__", exp)
            .replace("__SPOT__", spot_str)
            .replace("__ROWS__", rows)
            .replace("__BODY__", body_html)
            .replace("__PULL_MS__", str(PULL_EVERY * 1000)))
    return Response(content=html, media_type="text/html")

# ====== DASHBOARD ENDPOINT ======
@app.get("/", response_class=HTMLResponse)
def spxw_dashboard():
    open_now = market_open_now()
    status_text = "Market OPEN" if open_now else "Market CLOSED"
    status_color = "#10b981" if open_now else "#ef4444"

    # Make sure these are always strings
    last_ts  = last_run_status.get("ts")  or ""
    last_msg = last_run_status.get("msg") or ""

    html = (DASH_HTML_TEMPLATE
            .replace("__STATUS_COLOR__", status_color)
            .replace("__STATUS_TEXT__", status_text)
            .replace("__LAST_TS__", str(last_ts))
            .replace("__LAST_MSG__", str(last_msg))
            .replace("__PULL_MS__", str(PULL_EVERY * 1000)))
    return HTMLResponse(html)

