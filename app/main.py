# main.py
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from sqlalchemy import create_engine, text
import os, json, re
from typing import Optional, Dict, Any, List

app = FastAPI()

# ====== DB CONFIG ======
DB_URL = os.getenv("DATABASE_URL", "")
if DB_URL.startswith("postgresql://"):
    DB_URL = DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(DB_URL, pool_pre_ping=True)

# If you know the table name, set CAP_TABLE env var.
# Otherwise we auto-detect from common names.
CANDIDATE_TABLES = [
    os.getenv("CAP_TABLE", "").strip(),
    "volland_captures",
    "captures",
    "volland_fetches",
    "snapshots",
]
CANDIDATE_TABLES = [t for t in CANDIDATE_TABLES if t]


def table_exists(table: str) -> bool:
    q = text("""
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema='public' AND table_name=:t
        ) AS ok
    """)
    with engine.connect() as c:
        return bool(c.execute(q, {"t": table}).scalar())


def pick_table() -> str:
    for t in CANDIDATE_TABLES:
        if table_exists(t):
            return t
    raise RuntimeError(f"No known capture table found. Tried: {CANDIDATE_TABLES}")


def get_latest_row() -> Optional[Dict[str, Any]]:
    t = pick_table()
    # IMPORTANT: do NOT filter by date; just pull the latest row
    q = text(f"""
        SELECT ts_utc, captures
        FROM {t}
        ORDER BY ts_utc DESC
        LIMIT 1
    """)
    with engine.connect() as c:
        row = c.execute(q).mappings().first()
        return dict(row) if row else None


def extract_exposure_payload(captures: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Finds the fetch entry where url contains '/api/v1/data/exposure' and parses its JSON body.
    In your DB, body is stored as a STRING, so we json.loads it.
    """
    if not captures:
        return None

    fetch_top = captures.get("fetch_top") or []
    # search all fetch entries (top ones already include exposure in your example)
    for entry in fetch_top:
        url = (entry or {}).get("url", "") or ""
        if "/api/v1/data/exposure" in url:
            body = (entry or {}).get("body", "")
            if not body:
                return None
            try:
                return json.loads(body)  # body is JSON string
            except Exception:
                # Sometimes body can contain leading junk; try to salvage
                m = re.search(r"(\{.*\})", body, re.S)
                if m:
                    try:
                        return json.loads(m.group(1))
                    except Exception:
                        return None
                return None
    return None


@app.get("/api/debug/latest")
def api_debug_latest():
    """
    Use this to confirm the WEB is reading the same data you see in Postgres.
    """
    row = get_latest_row()
    if not row:
        return JSONResponse({"ok": False, "error": "no rows found"}, headers={"Cache-Control": "no-store"})

    # Return only small parts to avoid huge payloads
    captures = row.get("captures") or {}
    out = {
        "ok": True,
        "ts_utc": row.get("ts_utc"),
        "counts": (captures.get("counts") or {}),
        "fetch_top_count": len(captures.get("fetch_top") or []),
        "xhr_top_count": len(captures.get("xhr_top") or []),
        "ws_tail_count": len(captures.get("ws_tail") or []),
        "first_fetch_url": ((captures.get("fetch_top") or [{}])[0] or {}).get("url"),
    }
    return JSONResponse(jsonable_encoder(out), headers={"Cache-Control": "no-store"})


@app.get("/api/exposure/latest")
def api_exposure_latest():
    row = get_latest_row()
    if not row:
        return JSONResponse({"ok": False, "items": []}, headers={"Cache-Control": "no-store"})

    captures = row.get("captures") or {}
    payload = extract_exposure_payload(captures)
    items = (payload or {}).get("items") or []

    # Normalize x/y to numbers for plotting
    norm = []
    for p in items:
        try:
            x = float(p.get("x"))
            y = float(p.get("y"))
            norm.append({"x": x, "y": y})
        except Exception:
            continue

    return JSONResponse(
        jsonable_encoder({"ok": True, "ts_utc": row.get("ts_utc"), "items": norm}),
        headers={"Cache-Control": "no-store"},
    )


@app.get("/", response_class=HTMLResponse)
def home():
    # no-store prevents Cloudflare/browser caching “empty” pages
    html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Volland Exposure (Latest)</title>
  <script src="https://cdn.plot.ly/plotly-2.30.0.min.js"></script>
  <style>
    body{font-family:Arial, sans-serif; margin:16px;}
    #meta{margin:10px 0; color:#444;}
    #chart{width:100%; height:78vh;}
    .err{color:#b00020;}
  </style>
</head>
<body>
  <h3>Latest Exposure (from Postgres)</h3>
  <div id="meta">Loading...</div>
  <div id="chart"></div>

<script>
async function load(){
  const meta = document.getElementById("meta");
  try{
    // cache-bust + no-store
    const r = await fetch("/api/exposure/latest?t=" + Date.now(), {cache: "no-store"});
    const j = await r.json();
    if(!j.ok){
      meta.innerHTML = '<span class="err">No data from backend</span>';
      return;
    }

    meta.textContent = "ts_utc: " + j.ts_utc + " | points: " + (j.items?.length || 0);

    const xs = j.items.map(p => p.x);
    const ys = j.items.map(p => p.y);

    const trace = {x: xs, y: ys, mode: "lines", name: "Exposure"};
    const layout = {
      margin: {l: 50, r: 20, t: 20, b: 40},
      xaxis: {title: "Strike"},
      yaxis: {title: "Exposure"},
    };

    Plotly.newPlot("chart", [trace], layout, {displayModeBar: true, responsive: true});
  }catch(e){
    meta.innerHTML = '<span class="err">JS error: ' + (e?.message || e) + '</span>';
  }
}
load();
setInterval(load, 15000);
</script>
</body>
</html>
"""
    return HTMLResponse(html, headers={"Cache-Control": "no-store"})
