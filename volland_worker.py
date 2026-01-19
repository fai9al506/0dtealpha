# volland_worker.py - FIXED VERSION
# Properly extracts Volland exposure data and stores it in structured format
#
# Changes from original:
# 1. Extracts actual exposure data from /api/v1/data/exposure responses
# 2. Parses into structured rows (ts, strike, value, greek, ticker, etc.)
# 3. Saves to volland_exposure_points table (not just raw snapshots)
# 4. Maintains backward compatibility with volland_snapshots table

import os
import json
import time
import traceback
import re
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright

# ============ CONFIG ============
DB_URL = os.getenv("DATABASE_URL", "")
EMAIL = os.getenv("VOLLAND_EMAIL", "")
PASS = os.getenv("VOLLAND_PASSWORD", "")
URL = os.getenv("VOLLAND_URL", "")

PULL_EVERY = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "60"))
WAIT_AFTER_GOTO_SEC = float(os.getenv("VOLLAND_WAIT_AFTER_GOTO_SEC", "8"))

# Safety caps
MAX_CAPTURE_ITEMS = int(os.getenv("VOLLAND_MAX_CAPTURE_ITEMS", "50"))
MAX_BODY_CHARS = int(os.getenv("VOLLAND_MAX_BODY_CHARS", "50000"))

# ============ DATABASE ============
def db():
    return psycopg.connect(DB_URL, autocommit=True, row_factory=dict_row)


def ensure_tables():
    with db() as conn, conn.cursor() as cur:
        # Drop view first if it exists (views can't be altered, only replaced)
        cur.execute("DROP VIEW IF EXISTS volland_vanna_points_dedup CASCADE;")
        
        # Create main table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volland_exposure_points (
            id BIGSERIAL PRIMARY KEY,
            ts_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
            strike NUMERIC NOT NULL,
            value NUMERIC NOT NULL
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volland_exposure_points_ts ON volland_exposure_points(ts_utc DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volland_exposure_points_strike ON volland_exposure_points(strike);")
        
        # Create deduped view
        cur.execute("""
        CREATE VIEW volland_vanna_points_dedup AS
        WITH latest_ts AS (
            SELECT MAX(ts_utc) AS ts_utc
            FROM volland_exposure_points
        ),
        ranked AS (
            SELECT 
                v.ts_utc,
                v.strike,
                v.value AS vanna,
                ROW_NUMBER() OVER (
                    PARTITION BY v.strike 
                    ORDER BY v.id DESC
                ) AS rn
            FROM volland_exposure_points v
            JOIN latest_ts l ON v.ts_utc = l.ts_utc
        )
        SELECT ts_utc, strike, vanna
        FROM ranked
        WHERE rn = 1;
        """)
        
        # Legacy snapshots table
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volland_snapshots (
            id BIGSERIAL PRIMARY KEY,
            ts TIMESTAMPTZ NOT NULL DEFAULT now(),
            payload JSONB NOT NULL
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volland_snapshots_ts ON volland_snapshots(ts DESC);")

def save_raw_snapshot(payload: dict):
    """Save raw snapshot for debugging/backup."""
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO volland_snapshots(payload) VALUES (%s::jsonb)",
            (json.dumps(payload),)
        )


def save_exposure_points(
    points: List[Dict],
    greek: str,
    ticker: str = "SPX",
    expiration_option: str = None,
    current_price: float = None,
    last_modified: str = None,
    expirations: List[str] = None
):
    """Save structured exposure points to the database."""
    if not points:
        return 0

    ts_utc = datetime.now(timezone.utc)
    last_mod_dt = None
    if last_modified:
        try:
            last_mod_dt = datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
        except:
            pass

    expirations_json = json.dumps(expirations) if expirations else None

    with db() as conn, conn.cursor() as cur:
        inserted = 0
        for pt in points:
            try:
                strike = float(pt.get("x", 0))
                value = float(pt.get("y", 0))
                cur.execute("""
                    INSERT INTO volland_exposure_points 
                    (ts_utc, ticker, greek, expiration_option, strike, value, current_price, last_modified, expirations)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """, (ts_utc, ticker, greek, expiration_option, strike, value, current_price, last_mod_dt, expirations_json))
                inserted += 1
            except Exception as e:
                print(f"[db] skip point {pt}: {e}", flush=True)
        return inserted


# ============ BROWSER AUTOMATION ============
def handle_session_limit_modal(page) -> bool:
    """Handle the 'session limit' modal that sometimes appears."""
    btn = page.locator(
        "button[data-cy='confirmation-modal-confirm-button'], button:has-text('Continue')"
    ).first
    if btn.count() == 0:
        return False
    try:
        btn.wait_for(state="visible", timeout=3000)
        btn.click()
        page.wait_for_timeout(1200)
        print("[login] session modal: clicked Continue", flush=True)
        return True
    except Exception:
        return False


def login_if_needed(page):
    """Login to Volland if needed."""
    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    if "/sign-in" not in page.url and page.locator("input[name='password'], input[type='password']").count() == 0:
        return

    email_box = page.locator("input[data-cy='sign-in-email-input'], input[name='email']").first
    pwd_box = page.locator("input[data-cy='sign-in-password-input'], input[name='password'], input[type='password']").first

    email_box.wait_for(state="visible", timeout=90000)
    pwd_box.wait_for(state="visible", timeout=90000)

    email_box.fill(EMAIL)
    pwd_box.fill(PASS)

    page.locator("button:has-text('Log In'), button:has-text('Login'), button[type='submit']").first.click()
    handle_session_limit_modal(page)

    deadline = time.time() + 90
    while time.time() < deadline:
        handle_session_limit_modal(page)
        if "/sign-in" not in page.url:
            return
        page.wait_for_timeout(500)

    body = ""
    try:
        body = (page.locator("body").inner_text() or "")[:600]
    except Exception:
        pass
    raise RuntimeError(f"Login did not complete. Still on: {page.url}. Body: {body}")


# ============ NETWORK CAPTURE ============
INIT_CAPTURE_JS = r"""
(() => {
  const MAX = 200;
  const cap = { fetch: [], xhr: [], ws: [], note: [] };

  function push(arr, item) {
    try {
      arr.push(item);
      if (arr.length > MAX) arr.shift();
    } catch(e) {}
  }

  function safeText(t) {
    try {
      if (!t) return "";
      t = String(t);
      return t.length > 50000 ? t.slice(0, 50000) : t;
    } catch(e) { return ""; }
  }

  // Fetch hook
  const _fetch = window.fetch;
  window.fetch = async function(...args) {
    const url = (args && args[0] && args[0].url) ? args[0].url : String(args[0] || "");
    const t0 = Date.now();
    try {
      const res = await _fetch.apply(this, args);
      const ct = (res.headers.get("content-type") || "").toLowerCase();
      let body = "";
      try {
        if (ct.includes("json") || ct.includes("text") || ct.includes("graphql")) {
          const clone = res.clone();
          body = safeText(await clone.text());
        }
      } catch(e) {}
      push(cap.fetch, { url, status: res.status, ct, ms: Date.now() - t0, body });
      return res;
    } catch(e) {
      push(cap.fetch, { url, status: -1, ct: "", ms: Date.now() - t0, body: "FETCH_ERR: " + String(e) });
      throw e;
    }
  };

  // XHR hook
  const _open = XMLHttpRequest.prototype.open;
  const _send = XMLHttpRequest.prototype.send;

  XMLHttpRequest.prototype.open = function(method, url, ...rest) {
    this.__cap = { method, url: String(url || ""), t0: Date.now() };
    return _open.call(this, method, url, ...rest);
  };

  XMLHttpRequest.prototype.send = function(body) {
    const xhr = this;
    function done() {
      try {
        const ct = (xhr.getResponseHeader("content-type") || "").toLowerCase();
        let text = "";
        try { text = safeText(xhr.responseText || ""); } catch(e) {}
        push(cap.xhr, {
          url: (xhr.__cap && xhr.__cap.url) ? xhr.__cap.url : "",
          method: (xhr.__cap && xhr.__cap.method) ? xhr.__cap.method : "",
          status: xhr.status,
          ct,
          ms: Date.now() - ((xhr.__cap && xhr.__cap.t0) ? xhr.__cap.t0 : Date.now()),
          body: text
        });
      } catch(e) {}
    }
    xhr.addEventListener("loadend", done);
    return _send.call(this, body);
  };

  // WebSocket hook
  const _WS = window.WebSocket;
  window.WebSocket = function(url, protocols) {
    const ws = protocols ? new _WS(url, protocols) : new _WS(url);
    try {
      push(cap.ws, { url: String(url || ""), event: "open_attempt", t: Date.now() });
      ws.addEventListener("message", (ev) => {
        const data = safeText(ev.data);
        push(cap.ws, { url: String(url || ""), event: "message", t: Date.now(), data });
      });
    } catch(e) {}
    return ws;
  };
  window.WebSocket.prototype = _WS.prototype;

  window.__volland_cap = cap;
  window.__volland_cap_reset = () => {
    cap.fetch = [];
    cap.xhr = [];
    cap.ws = [];
    cap.note = [];
  };
})();
"""


def get_captures(page) -> dict:
    """Pull captured fetch/xhr/ws from the browser."""
    try:
        data = page.evaluate("() => window.__volland_cap || null")
        if not data:
            return {"fetch": [], "xhr": [], "ws": []}
        return data
    except Exception:
        return {"fetch": [], "xhr": [], "ws": []}


def reset_captures(page):
    """Reset the capture buffers."""
    try:
        page.evaluate("() => window.__volland_cap_reset && window.__volland_cap_reset()")
    except Exception:
        pass


# ============ DATA EXTRACTION ============
def extract_exposure_data(captures: dict) -> List[Dict]:
    """
    Extract exposure data from captured network requests.
    Returns list of dicts with: items, greek, ticker, currentPrice, lastModified, expirations, url
    """
    results = []
    
    all_requests = []
    for item in captures.get("fetch", []):
        all_requests.append(item)
    for item in captures.get("xhr", []):
        all_requests.append(item)

    for req in all_requests:
        url = req.get("url", "")
        body = req.get("body", "")
        status = req.get("status", 0)

        # Look for exposure data endpoint
        if "/api/v1/data/exposure" not in url:
            continue
        if status != 200:
            continue
        if not body:
            continue

        try:
            data = json.loads(body)
            items = data.get("items") or data.get("data") or []
            
            if not items:
                continue

            # Try to determine greek from URL params or data
            greek = "unknown"
            if "greek=vanna" in url.lower():
                greek = "vanna"
            elif "greek=charm" in url.lower():
                greek = "charm"
            elif "greek=gamma" in url.lower():
                greek = "gamma"
            elif "greek=delta" in url.lower():
                greek = "delta"
            else:
                # Default based on workspace - we'll use 'charm' as that's what was configured
                greek = "charm"

            # Try to determine ticker
            ticker = "SPX"
            if "ticker=spy" in url.lower() or "SPY" in url.upper():
                ticker = "SPY"
            elif "ticker=qqq" in url.lower() or "QQQ" in url.upper():
                ticker = "QQQ"

            results.append({
                "items": items,
                "greek": greek,
                "ticker": ticker,
                "currentPrice": data.get("currentPrice"),
                "lastModified": data.get("lastModified"),
                "expirations": data.get("expirations"),
                "url": url
            })

        except json.JSONDecodeError:
            continue
        except Exception as e:
            print(f"[extract] error parsing exposure: {e}", flush=True)
            continue

    return results


def infer_greek_from_url_or_workspace(page_url: str, workspace_config: dict = None) -> str:
    """Try to infer which greek is being displayed from URL or workspace config."""
    # Check URL for workspace hints
    if workspace_config:
        widgets = workspace_config.get("widgets", [])
        for w in widgets:
            config = w.get("configuration", {})
            if config.get("greek"):
                return config.get("greek")
    return "charm"  # default


# ============ MAIN LOOP ============
def run():
    if not DB_URL or not EMAIL or not PASS or not URL:
        raise RuntimeError("Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD / VOLLAND_URL")

    ensure_tables()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(90000)

        # Install hooks before navigation
        page.add_init_script(INIT_CAPTURE_JS)

        # Login once
        login_if_needed(page)
        print("[volland] logged in successfully", flush=True)

        while True:
            try:
                reset_captures(page)

                page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(int(WAIT_AFTER_GOTO_SEC * 1000))

                # Handle re-login if needed
                if "/sign-in" in page.url:
                    login_if_needed(page)
                    reset_captures(page)
                    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(int(WAIT_AFTER_GOTO_SEC * 1000))

                cap = get_captures(page)

                # Extract structured exposure data
                exposure_results = extract_exposure_data(cap)

                total_points_saved = 0
                for exp_data in exposure_results:
                    items = exp_data["items"]
                    if items:
                        saved = save_exposure_points(
                            points=items,
                            greek=exp_data["greek"],
                            ticker=exp_data["ticker"],
                            current_price=exp_data["currentPrice"],
                            last_modified=exp_data["lastModified"],
                            expirations=exp_data["expirations"]
                        )
                        total_points_saved += saved
                        print(f"[volland] saved {saved} {exp_data['greek']} points for {exp_data['ticker']}, price={exp_data['currentPrice']}", flush=True)

                # Also save raw snapshot for debugging
                raw_payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "page_url": page.url,
                    "exposure_count": len(exposure_results),
                    "total_points": total_points_saved,
                    "captures": {
                        "counts": {
                            "fetch": len(cap.get("fetch", [])),
                            "xhr": len(cap.get("xhr", [])),
                            "ws": len(cap.get("ws", []))
                        }
                    }
                }
                save_raw_snapshot(raw_payload)

                print(
                    f"[volland] cycle complete: {total_points_saved} points saved, "
                    f"{len(exposure_results)} exposure responses captured",
                    flush=True
                )

            except Exception as e:
                err_payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "page_url": getattr(page, "url", ""),
                    "error": str(e),
                    "trace": traceback.format_exc()[-4000:]
                }
                try:
                    save_raw_snapshot({"error_event": err_payload})
                except Exception:
                    pass
                print(f"[volland] error: {e}", flush=True)
                traceback.print_exc()

            time.sleep(PULL_EVERY)


if __name__ == "__main__":
    run()
