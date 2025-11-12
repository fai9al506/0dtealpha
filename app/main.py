# 0DTE Alpha — live chain + 5-min history (FastAPI + APScheduler + Postgres)
from fastapi import FastAPI, Response, Query
from datetime import datetime, time as dtime, timedelta
import os, time, json, requests, pandas as pd, pytz, base64
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

# Force SQLAlchemy to use psycopg (v3) driver
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

# Cadence (override via Railway variables)
PULL_EVERY     = int(os.getenv("PULL_EVERY", "15"))     # seconds
SAVE_EVERY_MIN = int(os.getenv("SAVE_EVERY_MIN", "5"))  # minutes

# Stream window like TS.py
STREAM_SECONDS = float(os.getenv("STREAM_SECONDS", "2.5"))
TARGET_STRIKES = int(os.getenv("TARGET_STRIKES", "40"))

# ====== APP ======
app = FastAPI()
NY = pytz.timezone("US/Eastern")

latest_df: pd.DataFrame | None = None
last_run_status = {"ts": None, "ok": False, "msg": "boot"}
_last_saved_at = 0.0

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

# ====== Auth (refresh-token with early refresh like TS.py) ======
REFRESH_EARLY_SEC = 300  # refresh 5 min before expiry
_access_token = None
_access_exp_at = 0.0
_refresh_token = RTOKEN or ""  # allow rotating refresh tokens if TS returns a new one

def _stamp_token(exp_in: int):
    global _access_exp_at
    now = time.time()
    _access_exp_at = now + int(exp_in or 900) - REFRESH_EARLY_SEC

def ts_access_token() -> str:
    """Refresh with RTOKEN; rotate if API returns a new refresh_token (rare but allowed)."""
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
        # rotate if TradeStation returns a new one
        _refresh_token = tok["refresh_token"]
    _stamp_token(tok.get("expires_in", 900))
    print("[auth] token refreshed; expires_in:", tok.get("expires_in"), flush=True)
    return _access_token

def api_get(path, params=None, stream=False, timeout=10):
    """Auto-refresh, retry once on 401, mirror TS.py resilience."""
    def do_req(h):
        return requests.get(f"{BASE}{path}", headers=h, params=params or {}, timeout=timeout, stream=stream)
    token = ts_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    r = do_req(headers)

    # Retry once on 401 with a forced refresh
    if r.status_code == 401:
        try:
            _ = ts_access_token()  # force refresh
            headers["Authorization"] = f"Bearer {_access_token}"
            r = do_req(headers)
        except Exception:
            pass

    # Final status check
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

# ====== TS API helpers ======
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
    except Exception as e:
        print("[exp] lookup failed; using today", ymd, "|", e, flush=True)
    return ymd

def _expiration_variants(ymd: str):
    yield ymd  # 2025-11-12
    try:
        yield datetime.strptime(ymd, "%Y-%m-%d").strftime("%m-%d-%Y")  # 11-12-2025
    except Exception:
        pass
    yield ymd + "T00:00:00Z"

def _fnum(x):
    if x in (None, "", "-", "NaN", "nan"): return None
    try: return float(str(x).replace(",",""))
    except: return None

def _consume_chain_stream(r, max_seconds: float) -> list[dict]:
    out, start = [], time.time()
    try:
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                if time.time() - start > max_seconds: break
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            # StreamStatus EndSnapshot usually means we've got the snapshot header/footer
            if isinstance(obj, dict) and obj.get("StreamStatus") == "EndSnapshot":
                break
            if isinstance(obj, dict):
                out.append(obj)
            if time.time() - start > max_seconds:
                break
    finally:
        try: r.close()
        except Exception: pass
    return out

def get_chain_rows(exp_ymd: str, spot: float) -> list[dict]:
    """Stream-first (forgiving), fallback to snapshot; try multiple expiration formats."""
    # 1) Stream
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
                        "Volume": it.get("TotalVolume") or it.get("Volume"),
                        "OpenInterest": it.get("OpenInterest") or it.get("DailyOpenInterest"),
                    })
                if rows:
                    return rows
        except Exception as e:
            last_err = e
            continue

    # 2) Snapshot fallback
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
                    "Volume": it.get("TotalVolume") or it.get("Volume"),
                    "OpenInterest": it.get("OpenInterest") or it.get("DailyOpenInterest"),
                })
            if rows:
                return rows
        except Exception as e:
            last_err = e
            continue

    raise RuntimeError(f"SPXW chain fetch failed (formats tried); last_err={last_err}")

# ====== Frame shaping (TS.py style) ======
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
    df = pd.DataFrame.from_records(recs, columns=CANONICAL_COLS)
    if not df.empty:
        df = df.sort_values("Strike").reset_index(drop=True)
    return df

def pick_centered(df: pd.DataFrame, spot: float, n: int) -> pd.DataFrame:
    if df is None or df.empty or not spot:
        return df
    idx = (df["Strike"] - spot).abs().sort_values().index[:n]
    return df.loc[sorted(idx)].reset_index(drop=True)

# ====== Jobs ======
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
        df   = to_side_by_side(rows)
        df   = pick_centered(df, spot, TARGET_STRIKES)

        latest_df = df
        last_run_status = {"ts": fmt_et(now_et()), "ok": True, "msg": f"exp={exp} spot={round(spot or 0,2)} rows={len(df)}"}
        print("[pull] OK", last_run_status["msg"], flush=True)
    except Exception as e:
        last_run_status = {"ts": fmt_et(now_et()), "ok": False, "msg": f"error: {e}"}
        print("[pull] ERROR", e, flush=True)

def save_history_job():
    """Every N minutes: persist latest_df to Postgres for later analysis/tests."""
    global _last_saved_at
    if not engine or latest_df is None or latest_df.empty:
        return
    # avoid hammering DB if scheduler overlaps or pulls got too frequent
    if time.time() - _last_saved_at < 60:
        return
    try:
        df = latest_df.copy()
        df.columns = DISPLAY_COLS
        payload = {"columns": df.columns.tolist(), "rows": df.fillna("").values.tolist()}
        msg = (last_run_status.get("msg") or "")
        spot = None
        exp  = None
        try:
            parts = dict(s.split("=") for s in msg.split() if "=" in s)
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

# ====== Endpoints ======
@app.get("/api/health")
def api_health():
    return {"status": "ok", "last": last_run_status}

@app.get("/debug/env")
def debug_env():
    return {"missing": missing_envs()}

@app.get("/status")
def status():
    return last_run_status

@app.get("/")
def home():
    open_now = market_open_now()
    status_text = "Market OPEN" if open_now else "Market CLOSED"
    status_color = "#10b981" if open_now else "#ef4444"
    last_msg = last_run_status.get("msg", "")
    last_ts  = last_run_status.get("ts", "")

    html = f"""
    <html>
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>0DTE Alpha</title>
      <style>
        :root {{
          --bg:#0a0a0a; --card:#121212; --text:#e5e5e5; --muted:#9ca3af; --ring:#2d2d2d;
        }}
        * {{ box-sizing:border-box; }}
        body {{
          margin:0; padding:24px; background:var(--bg); color:var(--text);
          font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
        }}
        .wrap {{ max-width: 880px; margin: 0 auto; }}
        h1 {{ font-size:28px; margin:0 0 10px; }}
        .sub {{ color:var(--muted); margin-bottom:20px; }}
        .status {{
          display:inline-flex; align-items:center; gap:10px; padding:10px 14px; border:1px solid var(--ring);
          border-radius:12px; background:var(--card); margin-bottom:20px;
        }}
        .dot {{ width:10px; height:10px; border-radius:999px; background:{status_color}; display:inline-block; }}
        .grid {{
          display:grid; gap:14px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          margin-top:16px;
        }}
        .btn {{
          display:flex; align-items:center; justify-content:center; text-decoration:none; color:var(--text);
          border:1px solid var(--ring); background:var(--card); border-radius:14px; padding:16px;
          transition: transform .05s ease, border-color .15s ease;
        }}
        .btn:hover {{ border-color:#3b3b3b; transform: translateY(-1px); }}
        .btn small {{ display:block; color:var(--muted); margin-top:6px; }}
        .foot {{ color:var(--muted); font-size:12px; margin-top:18px; }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <h1>0DTE Alpha</h1>
        <div class="sub">SPXW 0DTE live data & history</div>

        <div class="status">
          <span class="dot"></span>
          <div>
            <div style="font-weight:600;">{status_text}</div>
            <div style="color:var(--muted); font-size:12px;">Last run: {last_ts} — {last_msg}</div>
          </div>
        </div>

        <div class="grid">
          <a class="btn" href="/table">
            <div>
              <div style="font-weight:600;">Live Table</div>
              <small>Auto-refresh every {PULL_EVERY}s</small>
            </div>
          </a>

          <a class="btn" href="/api/snapshot">
            <div>
              <div style="font-weight:600;">Current JSON</div>
              <small>For programmatic access</small>
            </div>
          </a>

          <a class="btn" href="/api/history">
            <div>
              <div style="font-weight:600;">History (JSON)</div>
              <small>Saved every {SAVE_EVERY_MIN} minutes</small>
            </div>
          </a>

          <a class="btn" href="/download/history.csv">
            <div>
              <div style="font-weight:600;">Download CSV</div>
              <small>Quick export for analysis</small>
            </div>
          </a>
        </div>

        <div class="foot">Page refreshes status every 30s.</div>
      </div>

      <script>setTimeout(()=>location.reload(), 30000);</script>
    </body>
    </html>
    """
    return Response(content=html, media_type="text/html")

@app.get("/api/snapshot")
def snapshot():
    if latest_df is None or latest_df.empty:
        return {"columns": DISPLAY_COLS, "rows": []}
    df = latest_df.copy()
    df.columns = DISPLAY_COLS
    return {"columns": df.columns.tolist(), "rows": df.fillna("").values.tolist()}

@app.get("/table")
def html_table():
    ts  = last_run_status.get("ts") or ""
    msg = last_run_status.get("msg") or ""
    parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
    exp  = parts.get("exp", "")
    spot = parts.get("spot", "")
    rows = parts.get("rows", "")

    if latest_df is None or latest_df.empty:
        body = "<p>No data yet. If market is open, it will appear within ~15s.</p>"
    else:
        df = latest_df.copy()
        df.columns = DISPLAY_COLS

        # Format volume columns with commas
        for col in ["Volume", "Volume "]:  # handles both call & put sides
            for c in df.columns:
                if c.strip().lower() == col.strip().lower():
                    df[c] = df[c].apply(lambda x: f"{int(x):,}" if pd.notna(x) and str(x).isdigit() else x)

        body = df.to_html(index=False).replace('class="dataframe"', 'class="table"')

    page = f"""
    <html><head><meta charset="utf-8"><title>0DTE Alpha</title>
    <style>
      body {{
        font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
        background:#0a0a0a; color:#e5e5e5; padding:20px;
      }}
      .last {{
        color:#9ca3af; font-size:12px; line-height:1.25; margin:0 0 10px 0;
      }}
      table {{ border-collapse:collapse; width:100%; font-size:12px; }}
      th,td {{ border:1px solid #333; padding:6px 8px; text-align:right; }}
      th {{ background:#111; position:sticky; top:0; z-index:1; }}
      /* Strike column shaded */
      td:nth-child(11), th:nth-child(11) {{ background:#111; text-align:center; }}
      /* Leftmost column centered */
      td:first-child, th:first-child {{ text-align:center; }}
    </style>
    </head><body>
      <h2>SPXW 0DTE — live table</h2>
      <div class="last">
        Last run: {ts}<br>
        exp={exp}<br>
        spot={spot}<br>
        rows={rows}
      </div>
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
