# 0DTE Alpha – live chain + 5-min history (FastAPI + APScheduler + Postgres + Plotly front-end)
from fastapi import FastAPI, Response, Query
from fastapi.responses import HTMLResponse, JSONResponse
from datetime import datetime, time as dtime
import os, time, json, requests, pandas as pd, pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, text
from threading import Lock
from typing import Any, Optional

# ====== CONFIG ======
USE_LIVE = True
BASE = "https://api.tradestation.com/v3" if USE_LIVE else "https://sim-api.tradestation.com/v3"
AUTH_DOMAIN = "https://signin.tradestation.com"

CID     = os.getenv("TS_CLIENT_ID", "")
SECRET  = os.getenv("TS_CLIENT_SECRET", "")
RTOKEN  = os.getenv("TS_REFRESH_TOKEN", "")
DB_URL  = os.getenv("DATABASE_URL", "")  # Railway Postgres

# Volland storage (already scraped into Postgres)
VOLLAND_TABLE       = os.getenv("VOLLAND_TABLE", "volland_exposures")
VOLLAND_TS_COL      = os.getenv("VOLLAND_TS_COL", "ts")
VOLLAND_PAYLOAD_COL = os.getenv("VOLLAND_PAYLOAD_COL", "payload")

# Volland vanna by-strike view/table (you already have this in Postgres)
VOLLAND_VANNA_POINTS = os.getenv("VOLLAND_VANNA_POINTS", "public.volland_vanna_points_dedup")
VOLLAND_VANNA_TS_COL = os.getenv("VOLLAND_VANNA_TS_COL", "ts_utc")

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

        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {VOLLAND_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            {VOLLAND_TS_COL} TIMESTAMPTZ NOT NULL DEFAULT now(),
            {VOLLAND_PAYLOAD_COL} JSONB NOT NULL
        );
        """))
        conn.execute(text(f"""
        CREATE INDEX IF NOT EXISTS ix_{VOLLAND_TABLE}_{VOLLAND_TS_COL}
        ON {VOLLAND_TABLE} ({VOLLAND_TS_COL} DESC);
        """))

    print("[db] ready", flush=True)

def _json_load_maybe(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "ignore")
        except Exception:
            pass
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return v
    return v

def db_latest_volland() -> Optional[dict]:
    if not engine:
        return None
    q = text(f"SELECT {VOLLAND_TS_COL} AS ts, {VOLLAND_PAYLOAD_COL} AS payload FROM {VOLLAND_TABLE} ORDER BY {VOLLAND_TS_COL} DESC LIMIT 1")
    with engine.begin() as conn:
        r = conn.execute(q).mappings().first()
    if not r:
        return None
    payload = _json_load_maybe(r["payload"])
    ts = r["ts"]
    return {"ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts), "payload": payload}

def db_volland_history(limit: int = 500) -> list[dict]:
    if not engine:
        return []
    q = text(f"SELECT {VOLLAND_TS_COL} AS ts, {VOLLAND_PAYLOAD_COL} AS payload FROM {VOLLAND_TABLE} ORDER BY {VOLLAND_TS_COL} DESC LIMIT :lim")
    with engine.begin() as conn:
        rows = conn.execute(q, {"lim": int(limit)}).mappings().all()
    out = []
    for r in rows:
        payload = _json_load_maybe(r["payload"])
        ts = r["ts"]
        out.append({"ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts), "payload": payload})
    return out

def db_volland_vanna_window(limit: int = 40) -> dict:
    """
    Returns latest 'limit' strikes around the "mid" strike where abs(vanna) is max.
    (UI draws the vertical line at SPOT; mid info is returned for reference.)
    """
    if not engine:
        raise RuntimeError("DATABASE_URL not set")

    lim = int(limit)
    if lim < 5: lim = 5
    if lim > 200: lim = 200

    sql = text(f"""
    WITH latest AS (
      SELECT max({VOLLAND_VANNA_TS_COL}) AS ts_utc
      FROM {VOLLAND_VANNA_POINTS}
    ),
    mid AS (
      SELECT v.strike::numeric AS mid_strike,
             v.vanna::numeric  AS mid_vanna
      FROM {VOLLAND_VANNA_POINTS} v
      JOIN latest l ON v.{VOLLAND_VANNA_TS_COL} = l.ts_utc
      ORDER BY abs(v.vanna::numeric) DESC
      LIMIT 1
    ),
    ranked AS (
      SELECT
        v.{VOLLAND_VANNA_TS_COL} AS ts_utc,
        v.strike::numeric AS strike,
        v.vanna::numeric  AS vanna,
        m.mid_strike,
        m.mid_vanna,
        (v.strike::numeric - m.mid_strike) AS rel,
        ROW_NUMBER() OVER (
          ORDER BY abs(v.strike::numeric - m.mid_strike), v.strike::numeric
        ) AS rn
      FROM {VOLLAND_VANNA_POINTS} v
      JOIN latest l ON v.{VOLLAND_VANNA_TS_COL} = l.ts_utc
      CROSS JOIN mid m
    )
    SELECT ts_utc, strike, vanna, mid_strike, mid_vanna, rel
    FROM ranked
    WHERE rn <= :lim
    ORDER BY strike;
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"lim": lim}).mappings().all()

    if not rows:
        return {"ts_utc": None, "mid_strike": None, "mid_vanna": None, "points": []}

    ts_utc = rows[0]["ts_utc"]
    mid_strike = rows[0]["mid_strike"]
    mid_vanna  = rows[0]["mid_vanna"]

    pts = []
    for r in rows:
        pts.append({
            "strike": float(r["strike"]) if r["strike"] is not None else None,
            "vanna":  float(r["vanna"])  if r["vanna"]  is not None else None,
            "rel":    float(r["rel"])    if r["rel"]    is not None else None,
        })

    return {
        "ts_utc": ts_utc.isoformat() if hasattr(ts_utc, "isoformat") else str(ts_utc),
        "mid_strike": float(mid_strike) if mid_strike is not None else None,
        "mid_vanna":  float(mid_vanna)  if mid_vanna  is not None else None,
        "points": pts
    }

def db_volland_stats() -> Optional[dict]:
    """
    Get Volland statistics from the latest snapshot.
    Reads the 'statistics' field that volland_worker saves.
    Statistics persist even when market is closed.
    """
    if not engine:
        return None
    
    # Get the most recent snapshot that has statistics (no time limit - persist after hours)
    q = text("""
        SELECT ts, payload 
        FROM volland_snapshots 
        WHERE payload->>'error_event' IS NULL
          AND payload->'statistics' IS NOT NULL
        ORDER BY ts DESC 
        LIMIT 10
    """)
    
    with engine.begin() as conn:
        rows = conn.execute(q).mappings().all()
    
    if not rows:
        return {"ts": None, "stats": None, "error": "No statistics found"}
    
    stats = {
        "paradigm": None,
        "target": None,
        "lines_in_sand": None,
        "delta_decay_hedging": None,
        "opt_volume": None,
        "page_url": None,
        "has_statistics": False,
    }
    
    ts = None
    
    # Search through recent snapshots for statistics data
    for row in rows:
        payload = _json_load_maybe(row["payload"])
        if not payload or not isinstance(payload, dict):
            continue
        
        # Check for statistics field
        statistics = payload.get("statistics", {})
        if statistics and isinstance(statistics, dict):
            # Check if we have any actual data
            has_data = any(v for k, v in statistics.items() if v)
            if has_data:
                stats["has_statistics"] = True
                stats["paradigm"] = statistics.get("paradigm")
                stats["target"] = statistics.get("target")
                stats["lines_in_sand"] = statistics.get("lines_in_sand")
                stats["delta_decay_hedging"] = statistics.get("delta_decay_hedging")
                stats["opt_volume"] = statistics.get("opt_volume")
                ts = row["ts"]
                break
    
    if not ts and rows:
        ts = rows[0]["ts"]
    
    return {
        "ts": ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts else None,
        "stats": stats
    }

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
    return dtime(9, 30) <= t.time() <= dtime(16, 0)

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
        (calls if r["Type"] == "C" else puts)[r["Strike"]] = r
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
        spot = None; exp = None
        try:
            parts = dict(s.split("=", 1) for s in msg.split() if "=" in s)
            spot = float(parts.get("spot", ""))
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

REQUIRED_ENVS = ["TS_CLIENT_ID", "TS_CLIENT_SECRET", "TS_REFRESH_TOKEN", "DATABASE_URL"]
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
        spot = float(parts.get("spot", ""))
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
        return {"error": "DATABASE_URL not set"}
    with engine.begin() as conn:
        rows = conn.execute(text(
            "SELECT ts, exp, spot, columns, rows FROM chain_snapshots ORDER BY ts DESC LIMIT :lim"
        ), {"lim": limit}).mappings().all()
    for r in rows:
        r["columns"] = json.loads(r["columns"]) if isinstance(r["columns"], str) else r["columns"]
        r["rows"]    = json.loads(r["rows"])    if isinstance(r["rows"], str) else r["rows"]
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
        cols = json.loads(r["columns"]) if isinstance(r["columns"], str) else r["columns"]
        rows = json.loads(r["rows"])    if isinstance(r["rows"], str) else r["rows"]
        for arr in rows:
            obj = {"ts": r["ts"].isoformat(), "exp": r["exp"], "spot": r["spot"]}
            obj.update({cols[i]: arr[i] for i in range(len(cols))})
            out.append(obj)
    df = pd.DataFrame(out)
    csv = df.to_csv(index=False)
    return Response(csv, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=history.csv"})

# ===== Volland API (from Postgres) =====
@app.get("/api/volland/latest")
def api_volland_latest():
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        r = db_latest_volland()
        if not r:
            return {"ts": None, "payload": None}
        return r
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/volland/history")
def api_volland_history(limit: int = Query(500, ge=1, le=5000)):
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        return db_volland_history(limit=limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/volland/vanna_window")
def api_volland_vanna_window(limit: int = Query(40, ge=5, le=200)):
    """
    Latest strikes around mid_strike (mid_strike = strike where abs(vanna) is max).
    UI draws the vertical line at SPOT (from /api/series).
    """
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        return db_volland_vanna_window(limit=limit)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/volland/stats")
def api_volland_stats():
    """Get SPX statistics from Volland snapshots. Persists after market close."""
    try:
        if not engine:
            return JSONResponse({"error": "DATABASE_URL not set"}, status_code=500)
        result = db_volland_stats()
        if not result:
            return {"ts": None, "stats": None, "error": "No stats available"}
        return result
    except Exception as e:
        import traceback
        return JSONResponse({"error": str(e), "trace": traceback.format_exc()[-500:]}, status_code=500)

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
  <h2>SPXW 0DTE - Live Table</h2>
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
  <title>SPXW 0DTE - Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg:#0b0c10; --panel:#121417; --muted:#8a8f98; --text:#e6e7e9; --border:#23262b;
      --green:#22c55e; --red:#ef4444; --blue:#60a5fa;
    }
    * { box-sizing: border-box; }
    body {
      margin:0;
      background: var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
      font-size: 13px;
    }

    .layout {
      display: grid;
      grid-template-columns: 240px 1fr;
      min-height: 100vh;
    }
    .sidebar {
      background: var(--panel);
      border-right: 1px solid var(--border);
      padding: 18px 14px;
      position: sticky;
      top:0;
      height:100vh;
    }
    .brand { font-weight: 700; margin-bottom: 4px; font-size:14px; }
    .small { color: var(--muted); font-size: 11px; margin-bottom: 12px; }
    .status {
      display:flex;
      gap:8px;
      align-items:center;
      padding:8px;
      border:1px solid var(--border);
      border-radius:10px;
      background:#0f1216;
      margin-bottom:12px;
    }
    .dot { width:9px; height:9px; border-radius:999px; background:__STATUS_COLOR__; }
    .nav { display: grid; gap: 6px; margin-top: 6px; }
    .btn {
      display:block;
      width:100%;
      text-align:left;
      padding:8px 10px;
      border-radius:9px;
      border:1px solid var(--border);
      background:transparent;
      color:var(--text);
      cursor:pointer;
      font-size:12px;
    }
    .btn.active { background:#121a2e; border-color:#2a3a57; }

    .content { padding: 14px 16px; }
    .panel {
      background: var(--panel);
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px;
      overflow:hidden;
    }
    .header {
      display:flex;
      align-items:center;
      justify-content:space-between;
      padding:4px 6px 8px;
      border-bottom:1px solid var(--border);
      margin-bottom:8px;
      font-size:13px;
    }
    .pill {
      font-size:11px;
      padding:3px 7px;
      border:1px solid var(--border);
      border-radius:999px;
      color:var(--muted);
    }

    .charts { display:flex; flex-direction:column; gap:18px; }
    iframe {
      width:100%;
      height: calc(100vh - 190px);
      border:0;
      background:#0f1115;
    }
    #volChart, #oiChart, #gexChart, #vannaChart { width:100%; height:420px; }

    .unified-grid {
      display:grid;
      grid-template-columns: 2fr 1fr 1fr 1fr;
      gap:8px;
      align-items:stretch;
      height: calc(100vh - 140px);
      min-height:500px;
    }
    .unified-card {
      background: var(--panel);
      border:1px solid var(--border);
      border-radius:10px;
      padding:8px;
      display:flex;
      flex-direction:column;
      overflow:hidden;
    }
    .unified-card h3 {
      margin:0 0 4px;
      font-size:11px;
      color:var(--muted);
      font-weight:600;
      text-transform:uppercase;
      letter-spacing:0.5px;
    }
    .unified-plot {
      flex:1;
      width:100%;
      min-height:0;
    }
    .tv-container {
      flex:1;
      width:100%;
      min-height:0;
    }
    .tv-container iframe {
      width:100%;
      height:100%;
      border:0;
    }
    .spot-grid {
      display:grid;
      grid-template-columns: 2fr 1fr 1fr;
      gap:10px;
      align-items:stretch;
    }
    .card {
      background: var(--panel);
      border:1px solid var(--border);
      border-radius:12px;
      padding:10px;
      min-height:360px;
      display:flex;
      flex-direction:column;
    }
    .card h3 {
      margin:0 0 6px;
      font-size:13px;
      color:var(--muted);
      font-weight:600;
    }
    .plot { width:100%; height:100% }

    @media (max-width: 900px) {
      .layout { display:block; min-height:0; }
      .sidebar {
        position:static;
        height:auto;
        border-right:none;
        border-bottom:1px solid var(--border);
        padding:10px 10px 6px;
      }
      .status { margin-bottom:8px; }
      .nav {
        grid-auto-flow:column;
        grid-auto-columns:1fr;
        overflow-x:auto;
      }
      .btn { text-align:center; padding:7px 5px; font-size:11px; white-space:nowrap; }
      .content { padding:10px; }
      .panel { padding:8px; border-radius:10px; }
      iframe { height:60vh; }
      #volChart, #oiChart, #gexChart, #vannaChart { height:340px; }
      .spot-grid { grid-template-columns:1fr; }
      .card { min-height:260px; }
      .unified-grid { grid-template-columns:1fr; height:auto; }
      .unified-card { min-height:300px; }
    }
    .stats-box { margin-top:14px; padding:10px; border:1px solid var(--border); border-radius:10px; background:#0f1216; }
    .stats-box h4 { margin:0 0 8px; font-size:12px; font-weight:600; }
    .stats-row { display:flex; justify-content:space-between; font-size:11px; padding:4px 0; border-bottom:1px solid var(--border); }
    .stats-row:last-child { border-bottom:none; }
    .stats-label { color:var(--muted); }
    .stats-value { color:var(--text); font-weight:500; text-align:right; }
    .stats-value.green { color:var(--green); }
    .stats-value.red { color:var(--red); }
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
          <div style="font-weight:600; font-size:12px;">__STATUS_TEXT__</div>
          <div class="small">Last run: __LAST_TS__</div>
        </div>
      </div>
      <div class="nav">
        <button class="btn active" id="tabTable">Table</button>
        <button class="btn" id="tabCharts">Charts</button>
        <button class="btn" id="tabSpot">Unified</button>
      </div>
      <div class="small" style="margin-top:10px">Charts auto-refresh while visible.</div>
      <div class="small" style="margin-top:14px">
        <a href="/api/snapshot" style="color:var(--muted)">JSON</a> |
        <a href="/api/history" style="color:var(--muted)">History</a> |
        <a href="/api/volland/latest" style="color:var(--muted)">Exposure</a> |
        <a href="/download/history.csv" style="color:var(--muted)">CSV</a>
      </div>
      <div class="stats-box">
        <h4>SPX Statistics</h4>
        <div id="statsContent" style="color:var(--muted);font-size:11px">Loading...</div>
      </div>
    </aside>

    <main class="content">
      <div id="viewTable" class="panel">
        <div class="header">
          <div><strong>Live Chain Table</strong></div>
          <div class="pill">auto-refresh</div>
        </div>
        <iframe id="tableFrame" src="/table"></iframe>
      </div>

      <div id="viewCharts" class="panel" style="display:none">
        <div class="header">
          <div><strong>GEX, Volume, Open Interest + Volland Vanna</strong></div>
          <div class="pill">spot line = dotted</div>
        </div>
        <div class="charts">
          <div id="gexChart"></div>
          <div id="volChart"></div>
          <div id="oiChart"></div>
          <div id="vannaChart"></div>
        </div>
      </div>

      <div id="viewSpot" class="panel" style="display:none">
        <div class="header">
          <div><strong>Unified View</strong></div>
          <div class="pill">SPX candles + GEX / Charm / Volume by strike</div>
        </div>
        <div class="unified-grid">
          <div class="unified-card">
            <h3>SPX 15m</h3>
            <div class="tv-container" id="tvContainer">
              <!-- TradingView Widget BEGIN -->
              <div class="tradingview-widget-container" style="height:100%;width:100%">
                <div id="tradingview_spx" style="height:100%;width:100%"></div>
              </div>
              <!-- TradingView Widget END -->
            </div>
          </div>
          <div class="unified-card">
            <h3>Net GEX</h3>
            <div id="unifiedGexPlot" class="unified-plot"></div>
          </div>
          <div class="unified-card">
            <h3>Charm</h3>
            <div id="unifiedCharmPlot" class="unified-plot"></div>
          </div>
          <div class="unified-card">
            <h3>Volume</h3>
            <div id="unifiedVolPlot" class="unified-plot"></div>
          </div>
        </div>
      </div>

    </main>
  </div>

  <script>
    const PULL_EVERY = __PULL_MS__;

    // ===== SPX Statistics (persists after market close) =====
    const statsContent = document.getElementById('statsContent');
    async function fetchStats() {
      try {
        const r = await fetch('/api/volland/stats', {cache: 'no-store'});
        const data = await r.json();
        renderStats(data);
      } catch (err) {
        statsContent.innerHTML = '<span style="color:#ef4444">Error: ' + err.message + '</span>';
      }
    }
    function renderStats(data) {
      if (!data || data.error) {
        statsContent.innerHTML = '<span style="color:#ef4444">' + (data?.error || 'No data') + '</span>';
        return;
      }
      const s = data.stats || {};
      let h = '';
      
      // Paradigm
      if (s.paradigm) {
        h += '<div class="stats-row"><span class="stats-label">Paradigm</span><span class="stats-value">' + s.paradigm + '</span></div>';
      }
      
      // Target
      if (s.target) {
        h += '<div class="stats-row"><span class="stats-label">Target</span><span class="stats-value">' + s.target + '</span></div>';
      }
      
      // Lines in the Sand
      if (s.lines_in_sand) {
        h += '<div class="stats-row"><span class="stats-label">Lines in Sand</span><span class="stats-value">' + s.lines_in_sand + '</span></div>';
      }
      
      // Delta Decay Hedging
      if (s.delta_decay_hedging) {
        const ddh = s.delta_decay_hedging;
        const isNeg = ddh.includes('-') || ddh.startsWith('-');
        h += '<div class="stats-row"><span class="stats-label">DD Hedging</span><span class="stats-value ' + (isNeg ? 'red' : 'green') + '">' + ddh + '</span></div>';
      }
      
      // Options Volume
      if (s.opt_volume) {
        h += '<div class="stats-row"><span class="stats-label">0DTE Volume</span><span class="stats-value">' + s.opt_volume + '</span></div>';
      }
      
      // If no statistics found
      if (!s.paradigm && !s.target && !s.lines_in_sand && !s.delta_decay_hedging && !s.opt_volume) {
        h += '<div style="color:var(--muted);font-size:10px">No statistics available</div>';
      }
      
      // Timestamp
      const ts = data.ts ? new Date(data.ts).toLocaleTimeString() : 'N/A';
      h += '<div class="stats-row" style="margin-top:6px;font-size:10px"><span class="stats-label">Updated</span><span class="stats-value">' + ts + '</span></div>';
      
      statsContent.innerHTML = h;
    }
    fetchStats();
    setInterval(fetchStats, 60000); // Refresh stats every 60 seconds

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

    // ===== Shared fetch for options series (includes spot) =====
    async function fetchSeries(){
      const r=await fetch('/api/series',{cache:'no-store'});
      return await r.json();
    }

    // ===== Volland vanna window =====
    async function fetchVannaWindow(){
      const r = await fetch('/api/volland/vanna_window?limit=40', {cache:'no-store'});
      return await r.json();
    }

    // ===== Main charts (GEX / VOL / OI / VANNA) =====
    const volDiv=document.getElementById('volChart'),
          oiDiv=document.getElementById('oiChart'),
          gexDiv=document.getElementById('gexChart'),
          vannaDiv=document.getElementById('vannaChart');

    let chartsTimer=null, firstDraw=true;

    function verticalSpotShape(spot,yMin,yMax){
      if(spot==null) return null;
      return {type:'line', x0:spot, x1:spot, y0:yMin, y1:yMax, line:{color:'#9aa0a6', width:2, dash:'dot'}, xref:'x', yref:'y'};
    }

    function buildLayout(title,xTitle,yTitle,spot,yMin,yMax,dtick=5){
      const shape=verticalSpotShape(spot,yMin,yMax);
      return {
        title:{text:title,font:{size:14}},
        xaxis:{title:xTitle,gridcolor:'#20242a',tickfont:{size:10},dtick:dtick},
        yaxis:{title:yTitle,gridcolor:'#20242a',tickfont:{size:10}},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        font:{color:'#e6e7e9',size:11},
        margin:{t:32,r:12,b:40,l:44},
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

    function drawVannaWindow(w, spot){
      if (!w || w.error) {
        const msg = w && w.error ? w.error : "no data";
        Plotly.react(vannaDiv, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:10,b:30},
          annotations:[{text:"Vanna error: "+msg, x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9'}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }

      const pts = w.points || [];
      if (!pts.length) {
        Plotly.react(vannaDiv, [], {
          paper_bgcolor:'#121417', plot_bgcolor:'#0f1115',
          margin:{l:40,r:10,t:10,b:30},
          annotations:[{text:"No vanna points returned yet", x:0.5, y:0.5, xref:'paper', yref:'paper', showarrow:false, font:{color:'#e6e7e9'}}],
          font:{color:'#e6e7e9'}
        }, {displayModeBar:false,responsive:true});
        return;
      }

      const strikes = pts.map(p=>p.strike);
      const vanna   = pts.map(p=>p.vanna);

      // green for +, red for -
      const colors = vanna.map(v => (v >= 0 ? '#22c55e' : '#ef4444'));

      let yMin = Math.min(...vanna);
      let yMax = Math.max(...vanna);
      if (yMin === yMax){
        const pad0 = Math.max(1, Math.abs(yMin)*0.05);
        yMin -= pad0; yMax += pad0;
      } else {
        const pad = (yMax - yMin) * 0.05;
        yMin -= pad; yMax += pad;
      }

      const shapes = [];
      if (spot != null) {
        shapes.push({
          type:'line', x0:spot, x1:spot, y0:yMin, y1:yMax,
          xref:'x', yref:'y',
          line:{color:'#9aa0a6', width:2, dash:'dot'}
        });
      }

      const trace = {
        type:'bar',
        x: strikes,
        y: vanna,
        marker:{color: colors},
        hovertemplate:"Strike %{x}<br>Vanna %{y}<extra></extra>"
      };

      Plotly.react(vannaDiv, [trace], {
        title:{text:'Charm', font:{size:14}},
        paper_bgcolor:'#121417',
        plot_bgcolor:'#0f1115',
        margin:{l:55,r:10,t:32,b:40},
        xaxis:{title:'Strike', gridcolor:'#20242a', tickfont:{size:10}, dtick:5},
        yaxis:{title:'Charm',  gridcolor:'#20242a', tickfont:{size:10}, range:[yMin,yMax]},
        shapes: shapes,
        font:{color:'#e6e7e9',size:11}
      }, {displayModeBar:false,responsive:true});
    }

    async function drawOrUpdate(){
  // 1) Fetch the fast data first (DO NOT wait for vanna)
  const data = await fetchSeries();
  if (!data || !data.strikes || data.strikes.length === 0) return;

  const strikes = data.strikes, spot = data.spot;

  const vMax = Math.max(0, ...data.callVol, ...data.putVol) * 1.05;
  const oiMax= Math.max(0, ...data.callOI,  ...data.putOI ) * 1.05;
  const gAbs = [...data.callGEX, ...data.putGEX, ...data.netGEX].map(v=>Math.abs(v));
  const gMax = (gAbs.length ? Math.max(...gAbs) : 0) * 1.05;

  const gexLayout = buildLayout('Gamma Exposure (GEX)','Strike','GEX',spot,-gMax,gMax,5);
  const volLayout = buildLayout('Volume','Strike','Volume',spot,0,vMax,5);
  const oiLayout  = buildLayout('Open Interest','Strike','Open Interest',spot,0,oiMax,5);

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

  // 2) Show a quick "loading" state for vanna (optional but recommended)
  if (!window.__vannaLoadingShown) {
    window.__vannaLoadingShown = true;
    drawVannaWindow({ error: "Loading Vanna..." }, spot); // your function will render the message
  }

  // 3) Fetch vanna in the background (doesn't block charts)
  fetchVannaWindow()
    .then(vannaW => drawVannaWindow(vannaW, spot))
    .catch(err => drawVannaWindow({ error: String(err) }, spot));
}


    function startCharts(){
      drawOrUpdate();
      if (chartsTimer) clearInterval(chartsTimer);
      chartsTimer = setInterval(drawOrUpdate, PULL_EVERY);
    }
    function stopCharts(){
      if (chartsTimer){
        clearInterval(chartsTimer);
        chartsTimer=null;
      }
    }

    // ===== Unified View: TradingView + GEX/Charm/Volume bar charts =====
    const unifiedGexDiv = document.getElementById('unifiedGexPlot'),
          unifiedCharmDiv = document.getElementById('unifiedCharmPlot'),
          unifiedVolDiv = document.getElementById('unifiedVolPlot');
    let unifiedTimer = null;
    let tvWidgetInitialized = false;

    // Initialize TradingView widget
    function initTradingView() {
      if (tvWidgetInitialized) return;
      tvWidgetInitialized = true;

      const script = document.createElement('script');
      script.src = 'https://s3.tradingview.com/tv.js';
      script.onload = function() {
        new TradingView.widget({
          "autosize": true,
          "symbol": "OANDA:SPX500USD",
          "interval": "15",
          "timezone": "America/New_York",
          "theme": "dark",
          "style": "1",
          "locale": "en",
          "toolbar_bg": "#121417",
          "enable_publishing": false,
          "hide_top_toolbar": false,
          "hide_legend": true,
          "save_image": false,
          "container_id": "tradingview_spx",
          "backgroundColor": "#0f1115",
          "gridColor": "#20242a"
        });
      };
      document.head.appendChild(script);
    }

    // Compute shared Y range from strikes
    function computeStrikeRange(strikes) {
      if (!strikes || !strikes.length) return null;
      let yMin = Math.min(...strikes);
      let yMax = Math.max(...strikes);
      const pad = (yMax - yMin) * 0.02 || 1;
      return { min: yMin - pad, max: yMax + pad };
    }

    // Create horizontal spot line shape
    function horizontalSpotShape(spot, xMin, xMax) {
      if (spot == null) return null;
      return {
        type: 'line',
        y0: spot, y1: spot,
        x0: xMin, x1: xMax,
        xref: 'x', yref: 'y',
        line: { color: '#60a5fa', width: 2, dash: 'dot' }
      };
    }

    // Render Net GEX horizontal bar chart
    function renderUnifiedGex(strikes, netGEX, yRange, spot) {
      if (!strikes.length) return;

      // Color bars: green for positive, red for negative
      const colors = netGEX.map(v => v >= 0 ? '#22c55e' : '#ef4444');
      const gMax = Math.max(1, ...netGEX.map(v => Math.abs(v))) * 1.1;

      const shapes = [];
      const spotShape = horizontalSpotShape(spot, -gMax, gMax);
      if (spotShape) shapes.push(spotShape);

      const trace = {
        type: 'bar',
        orientation: 'h',
        x: netGEX,
        y: strikes,
        marker: { color: colors },
        hovertemplate: 'Strike %{y}<br>Net GEX %{x:,.0f}<extra></extra>'
      };

      Plotly.react(unifiedGexDiv, [trace], {
        margin: { l: 50, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { title: '', gridcolor: '#20242a', tickfont: { size: 9 }, zeroline: true, zerolinecolor: '#333' },
        yaxis: { title: '', range: [yRange.min, yRange.max], gridcolor: '#20242a', tickfont: { size: 9 }, dtick: 5 },
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true });
    }

    // Render Charm horizontal bar chart (from vanna_window)
    function renderUnifiedCharm(vannaData, yRange, spot) {
      if (!vannaData || vannaData.error || !vannaData.points || !vannaData.points.length) {
        Plotly.react(unifiedCharmDiv, [], {
          paper_bgcolor: '#121417', plot_bgcolor: '#0f1115',
          margin: { l: 50, r: 8, t: 4, b: 24 },
          annotations: [{ text: vannaData?.error || 'No charm data', x: 0.5, y: 0.5, xref: 'paper', yref: 'paper', showarrow: false, font: { color: '#e6e7e9' } }],
          font: { color: '#e6e7e9' }
        }, { displayModeBar: false, responsive: true });
        return;
      }

      const pts = vannaData.points;
      const strikes = pts.map(p => p.strike);
      const vanna = pts.map(p => p.vanna);

      // Color bars: green for positive, red for negative
      const colors = vanna.map(v => v >= 0 ? '#22c55e' : '#ef4444');
      const vMax = Math.max(1, ...vanna.map(v => Math.abs(v))) * 1.1;

      const shapes = [];
      const spotShape = horizontalSpotShape(spot, -vMax, vMax);
      if (spotShape) shapes.push(spotShape);

      const trace = {
        type: 'bar',
        orientation: 'h',
        x: vanna,
        y: strikes,
        marker: { color: colors },
        hovertemplate: 'Strike %{y}<br>Charm %{x:,.0f}<extra></extra>'
      };

      Plotly.react(unifiedCharmDiv, [trace], {
        margin: { l: 50, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { title: '', gridcolor: '#20242a', tickfont: { size: 9 }, zeroline: true, zerolinecolor: '#333' },
        yaxis: { title: '', range: [yRange.min, yRange.max], gridcolor: '#20242a', tickfont: { size: 9 }, dtick: 5 },
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true });
    }

    // Render Volume horizontal bar chart (mirrored: puts go left, calls go right)
    function renderUnifiedVolume(strikes, callVol, putVol, yRange, spot) {
      if (!strikes.length) return;

      // Negate puts so they go left (negative x), calls stay positive (right)
      const putsNegative = putVol.map(v => -v);
      const vMax = Math.max(1, ...callVol, ...putVol) * 1.1;

      const shapes = [];
      const spotShape = horizontalSpotShape(spot, -vMax, vMax);
      if (spotShape) shapes.push(spotShape);

      const traceCalls = {
        type: 'bar',
        orientation: 'h',
        name: 'Calls',
        x: callVol,
        y: strikes,
        marker: { color: '#22c55e' },
        hovertemplate: 'Strike %{y}<br>Calls %{x:,}<extra></extra>'
      };

      const tracePuts = {
        type: 'bar',
        orientation: 'h',
        name: 'Puts',
        x: putsNegative,
        y: strikes,
        marker: { color: '#ef4444' },
        hovertemplate: 'Strike %{y}<br>Puts %{customdata:,}<extra></extra>',
        customdata: putVol
      };

      Plotly.react(unifiedVolDiv, [tracePuts, traceCalls], {
        margin: { l: 50, r: 8, t: 4, b: 24 },
        paper_bgcolor: '#121417',
        plot_bgcolor: '#0f1115',
        xaxis: { title: '', gridcolor: '#20242a', tickfont: { size: 9 }, range: [-vMax, vMax], zeroline: true, zerolinecolor: '#333' },
        yaxis: { title: '', range: [yRange.min, yRange.max], gridcolor: '#20242a', tickfont: { size: 9 }, dtick: 5 },
        barmode: 'overlay',
        showlegend: false,
        font: { color: '#e6e7e9', size: 10 },
        shapes: shapes
      }, { displayModeBar: false, responsive: true });
    }

    // Main tick function for unified view
    async function tickUnified() {
      try {
        // Fetch series data (strikes, GEX, volume, spot)
        const data = await fetchSeries();
        if (!data || !data.strikes || !data.strikes.length) return;

        const strikes = data.strikes;
        const spot = data.spot;
        const yRange = computeStrikeRange(strikes);
        if (!yRange) return;

        // Render GEX
        renderUnifiedGex(strikes, data.netGEX || [], yRange, spot);

        // Render Volume
        renderUnifiedVolume(strikes, data.callVol || [], data.putVol || [], yRange, spot);

        // Fetch and render Charm (vanna_window) - async, non-blocking
        fetchVannaWindow()
          .then(vannaData => renderUnifiedCharm(vannaData, yRange, spot))
          .catch(err => renderUnifiedCharm({ error: String(err) }, yRange, spot));

      } catch (err) {
        console.error('Unified view error:', err);
      }
    }

    function startSpot() {
      initTradingView();
      tickUnified();
      if (unifiedTimer) clearInterval(unifiedTimer);
      unifiedTimer = setInterval(tickUnified, PULL_EVERY);
    }

    function stopSpot() {
      if (unifiedTimer) {
        clearInterval(unifiedTimer);
        unifiedTimer = null;
      }
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

    last_ts  = last_run_status.get("ts")  or ""
    last_msg = last_run_status.get("msg") or ""

    html = (DASH_HTML_TEMPLATE
            .replace("__STATUS_COLOR__", status_color)
            .replace("__STATUS_TEXT__", status_text)
            .replace("__LAST_TS__", str(last_ts))
            .replace("__LAST_MSG__", str(last_msg))
            .replace("__PULL_MS__", str(PULL_EVERY * 1000)))
    return HTMLResponse(html)
