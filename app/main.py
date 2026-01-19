@ -1,7 +1,8 @@
# 0DTE Alpha — live chain + 5-min history (FastAPI + APScheduler + Postgres + Plotly front-end)
# WITH MOCK DATA SUPPORT - Automatically uses latest saved data when market is closed
from fastapi import FastAPI, Response, Query
from fastapi.responses import HTMLResponse, JSONResponse
from datetime import datetime, time as dtime, timedelta 
from datetime import datetime, time as dtime, timedelta
import os, time, json, requests, pandas as pd, pytz
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import create_engine, text
@ -39,12 +40,15 @@ SAVE_EVERY_MIN = 5    # minutes
STREAM_SECONDS = 2.0
TARGET_STRIKES = 40

# Mock data override (set USE_MOCK_DATA=true in Railway to force mock mode)
USE_MOCK_DATA_OVERRIDE = os.getenv("USE_MOCK_DATA", "false").lower() == "true"

# ====== APP ======
app = FastAPI()
NY = pytz.timezone("US/Eastern")

latest_df: pd.DataFrame | None = None
last_run_status = {"ts": None, "ok": False, "msg": "boot"}
last_run_status = {"ts": None, "ok": False, "msg": "boot", "is_mock": False}
_last_saved_at = 0.0
_df_lock = Lock()

@ -241,6 +245,78 @@ def db_volland_vanna_window(limit: int = 40) -> dict:
        "points": pts
    }

# ====== Mock Data Support ======
def get_latest_saved_chain_as_mock():
    """
    Get the most recent saved chain snapshot from DB to use as mock data.
    Returns (df, spot, exp) or (None, None, None) if no data available.
    """
    if not engine:
        return None, None, None
    
    try:
        with engine.begin() as conn:
            result = conn.execute(text(
                "SELECT ts, exp, spot, columns, rows FROM chain_snapshots ORDER BY ts DESC LIMIT 1"
            )).mappings().first()
        
        if not result:
            return None, None, None
        
        columns = json.loads(result["columns"]) if isinstance(result["columns"], str) else result["columns"]
        rows = json.loads(result["rows"]) if isinstance(result["rows"], str) else result["rows"]
        
        # Reconstruct DataFrame with proper column names
        df = pd.DataFrame(rows, columns=columns)
        spot = float(result["spot"]) if result["spot"] else None
        exp = result["exp"]
        
        print(f"[mock] Loaded saved chain: {len(df)} rows, spot={spot}, exp={exp}", flush=True)
        return df, spot, exp
        
    except Exception as e:
        print(f"[mock] Failed to load latest chain: {e}", flush=True)
        return None, None, None


def should_use_mock_data() -> bool:
    """
    Determine if we should use mock data.
    Returns True if:
    - Manual override is set (USE_MOCK_DATA_OVERRIDE=true)
    - Market is closed (outside 9:30-16:00 ET Mon-Fri)
    """
    if USE_MOCK_DATA_OVERRIDE:
        return True
    
    # Check if market is open
    if not market_open_now():
        return True
    
    return False


def get_mock_data_info() -> dict:
    """
    Returns information about mock data status.
    Used for displaying warnings in the UI.
    """
    if not should_use_mock_data():
        return {"is_mock": False}
    
    reasons = []
    if USE_MOCK_DATA_OVERRIDE:
        reasons.append("Manual override enabled")
    if not market_open_now():
        reasons.append("Market closed")
    
    return {
        "is_mock": True,
        "reasons": reasons,
        "timestamp": fmt_et(now_et())
    }


# ====== Auth ======
REFRESH_EARLY_SEC = 300
_access_token = None
@ -502,24 +578,74 @@ def pick_centered(df: pd.DataFrame, spot: float, n: int) -> pd.DataFrame:
    idx = (df["Strike"] - spot).abs().sort_values().index[:n]
    return df.loc[sorted(idx)].reset_index(drop=True)


# ====== jobs ======
def run_market_job():
    global latest_df, last_run_status
    try:
        if not market_open_now():
            last_run_status = {"ts": fmt_et(now_et()), "ok": True, "msg": "outside market hours"}
            print("[pull] skipped (closed)", last_run_status["ts"], flush=True)
        use_mock = should_use_mock_data()
        
        if use_mock:
            # Try to use latest saved data as mock
            df, spot, exp = get_latest_saved_chain_as_mock()
            
            if df is not None and not df.empty:
                # Convert back to CANONICAL_COLS format if needed
                if 'Volume' in df.columns:  # Already in DISPLAY_COLS format
                    # Map back to CANONICAL_COLS
                    df.columns = CANONICAL_COLS
                
                with _df_lock:
                    latest_df = df.copy()
                
                reasons = []
                if USE_MOCK_DATA_OVERRIDE:
                    reasons.append("override")
                if not market_open_now():
                    reasons.append("market closed")
                reason_str = ", ".join(reasons)
                
                last_run_status = {
                    "ts": fmt_et(now_et()), 
                    "ok": True, 
                    "msg": f"MOCK ({reason_str}): exp={exp} spot={round(spot or 0, 2)} rows={len(df)}",
                    "is_mock": True
                }
                print("[pull] MOCK DATA (using latest saved)", last_run_status["msg"], flush=True)
            else:
                last_run_status = {
                    "ts": fmt_et(now_et()), 
                    "ok": True, 
                    "msg": "MOCK: No saved data available yet",
                    "is_mock": True
                }
                print("[pull] MOCK: No saved data available", flush=True)
            return
        
        # Regular TradeStation data fetch (market is open)
        spot = get_spx_last()
        exp  = get_0dte_exp()
        exp = get_0dte_exp()
        rows = get_chain_rows(exp, spot)
        df   = pick_centered(to_side_by_side(rows), spot, TARGET_STRIKES)
        df = pick_centered(to_side_by_side(rows), spot, TARGET_STRIKES)
        
        with _df_lock:
            latest_df = df.copy()
        last_run_status = {"ts": fmt_et(now_et()), "ok": True, "msg": f"exp={exp} spot={round(spot or 0,2)} rows={len(df)}"}
        
        last_run_status = {
            "ts": fmt_et(now_et()), 
            "ok": True, 
            "msg": f"exp={exp} spot={round(spot or 0, 2)} rows={len(df)}",
            "is_mock": False
        }
        print("[pull] OK", last_run_status["msg"], flush=True)
        
    except Exception as e:
        last_run_status = {"ts": fmt_et(now_et()), "ok": False, "msg": f"error: {e}"}
        last_run_status = {
            "ts": fmt_et(now_et()), 
            "ok": False, 
            "msg": f"error: {e}",
            "is_mock": False
        }
        print("[pull] ERROR", e, flush=True)

def save_history_job():
@ -628,9 +754,16 @@ def api_series():
def api_health():
    return {"status": "ok", "last": last_run_status}

@app.get("/api/mock_status")
def api_mock_status():
    """Returns information about whether mock data is being used"""
    return get_mock_data_info()

@app.get("/status")
def status():
    return last_run_status
    status_data = dict(last_run_status)
    status_data["is_mock"] = status_data.get("is_mock", False)
    return status_data

@app.get("/api/snapshot")
def snapshot():
@ -833,6 +966,9 @@ DASH_HTML_TEMPLATE = """
    :root {
      --bg:#0b0c10; --panel:#121417; --muted:#8a8f98; --text:#e6e7e9; --border:#23262b;
      --green:#22c55e; --red:#ef4444; --blue:#60a5fa;
      --warning-bg: #fef3c7;
      --warning-border: #fbbf24;
      --warning-text: #92400e;
    }
    * { box-sizing: border-box; }
    body {
@ -843,6 +979,22 @@ DASH_HTML_TEMPLATE = """
      font-size: 13px;
    }

    .mock-warning {
      background: var(--warning-bg);
      border: 2px solid var(--warning-border);
      color: var(--warning-text);
      padding: 12px 16px;
      border-radius: 8px;
      margin: 0 0 16px 0;
      font-weight: 600;
      display: none;
      font-size: 14px;
      text-align: center;
    }
    .mock-warning.show {
      display: block;
    }

    .layout {
      display: grid;
      grid-template-columns: 240px 1fr;
@ -993,6 +1145,12 @@ DASH_HTML_TEMPLATE = """
    </aside>

    <main class="content">
      <!-- Mock Data Warning Banner -->
      <div id="mockWarning" class="mock-warning">
        <span>⚠️</span>
        <span id="mockWarningText">Using mock data - Market is closed</span>
      </div>
      
      <div id="viewTable" class="panel">
        <div class="header">
          <div><strong>Live Chain Table</strong></div>
@ -1190,48 +1348,44 @@ DASH_HTML_TEMPLATE = """
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
    drawVannaWindow({ error: "Loading Vanna…" }, spot); // your function will render the message
  }

  // 3) Fetch vanna in the background (doesn't block charts)
  fetchVannaWindow()
    .then(vannaW => drawVannaWindow(vannaW, spot))
    .catch(err => drawVannaWindow({ error: String(err) }, spot));
}
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

      if (!window.__vannaLoadingShown) {
        window.__vannaLoadingShown = true;
        drawVannaWindow({ error: "Loading Vanna…" }, spot);
      }

      fetchVannaWindow()
        .then(vannaW => drawVannaWindow(vannaW, spot))
        .catch(err => drawVannaWindow({ error: String(err) }, spot));
    }

    function startCharts(){
      drawOrUpdate();
@ -1245,7 +1399,7 @@ DASH_HTML_TEMPLATE = """
      }
    }

    // ===== Spot: expects /api/spot (keep your existing endpoint) =====
    // ===== Spot =====
    const spotPriceDiv=document.getElementById('spotPricePlot'),
          gexSideDiv=document.getElementById('gexSidePlot'),
          volSideDiv=document.getElementById('volSidePlot');
@ -1381,6 +1535,30 @@ DASH_HTML_TEMPLATE = """
      }
    }

    // ===== Mock Data Status Check =====
    async function checkMockStatus() {
      try {
        const r = await fetch('/api/mock_status', {cache: 'no-store'});
        const status = await r.json();
        const warning = document.getElementById('mockWarning');
        const warningText = document.getElementById('mockWarningText');
        
        if (status.is_mock) {
          const reasons = status.reasons ? status.reasons.join(', ') : 'Market closed';
          warningText.innerHTML = `⚠️ Using latest saved data - ${reasons}`;
          warning.classList.add('show');
        } else {
          warning.classList.remove('show');
        }
      } catch (e) {
        console.error('Failed to check mock status:', e);
      }
    }

    // Check mock status on load and every 60 seconds
    checkMockStatus();
    setInterval(checkMockStatus, 60000);

    // default
    showTable();
  </script>
