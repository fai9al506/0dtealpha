# volland_worker.py
# Fixed version that captures BOTH charm/exposure data AND statistics
import os, json, time, traceback, re
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright


DB_URL   = os.getenv("DATABASE_URL", "")
EMAIL    = os.getenv("VOLLAND_EMAIL", "")
PASS     = os.getenv("VOLLAND_PASSWORD", "")
URL      = os.getenv("VOLLAND_URL", "")  # Charm workspace
STATS_URL = os.getenv("VOLLAND_STATS_URL", "https://vol.land/app/workspace/696fcf236547cfa9b4d09267")

PULL_EVERY   = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "60"))
WAIT_AFTER_GOTO_SEC = float(os.getenv("VOLLAND_WAIT_AFTER_GOTO_SEC", "6"))

# safety caps (avoid huge DB rows)
MAX_CAPTURE_ITEMS = int(os.getenv("VOLLAND_MAX_CAPTURE_ITEMS", "40"))
MAX_BODY_CHARS    = int(os.getenv("VOLLAND_MAX_BODY_CHARS", "20000"))


def db():
    return psycopg.connect(DB_URL, autocommit=True, row_factory=dict_row)


def ensure_tables():
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS volland_snapshots (
          id BIGSERIAL PRIMARY KEY,
          ts TIMESTAMPTZ NOT NULL DEFAULT now(),
          payload JSONB NOT NULL
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_volland_snapshots_ts ON volland_snapshots(ts DESC);")


def save_snapshot(payload: dict):
    with db() as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO volland_snapshots(payload) VALUES (%s::jsonb)", (json.dumps(payload),))


def handle_session_limit_modal(page) -> bool:
    btn = page.locator(
        "button[data-cy='confirmation-modal-confirm-button'], button:has-text('Continue')"
    ).first
    if btn.count() == 0:
        return False
    try:
        btn.wait_for(state="visible", timeout=3000)
        btn.click()
        page.wait_for_timeout(1200)
        print("[login] session modal: clicked Continue")
        return True
    except Exception:
        return False


def login_if_needed(page):
    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    if "/sign-in" not in page.url and page.locator("input[name='password'], input[type='password']").count() == 0:
        return

    email_box = page.locator("input[data-cy='sign-in-email-input'], input[name='email']").first
    pwd_box   = page.locator("input[data-cy='sign-in-password-input'], input[name='password'], input[type='password']").first

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


# ---- Install hooks BEFORE any navigation ----
INIT_CAPTURE_JS = r"""
(() => {
  const MAX = 200; // in-browser buffer
  const cap = {
    fetch: [],
    xhr: [],
    ws: [],
    note: []
  };

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
      return t.length > 20000 ? t.slice(0, 20000) : t;
    } catch(e) { return ""; }
  }

  // --- fetch hook ---
  const _fetch = window.fetch;
  window.fetch = async function(...args) {
    const url = (args && args[0] && args[0].url) ? args[0].url : String(args[0] || "");
    const t0 = Date.now();
    try {
      const res = await _fetch.apply(this, args);
      const ct = (res.headers.get("content-type") || "").toLowerCase();

      // clone to read body
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

  // --- XHR hook ---
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

  // --- WebSocket hook (optional) ---
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
    try:
        page.evaluate("() => window.__volland_cap_reset && window.__volland_cap_reset()")
    except Exception:
        pass


def filter_and_score(items):
    """Keep likely relevant endpoints and score candidates."""
    out = []
    for it in items:
        url = (it.get("url") or "").lower()
        if not url:
            continue

        # drop noisy analytics
        if any(x in url for x in ("sentry.io", "gleap.io", "googletagmanager", "google-analytics")):
            continue

        body = it.get("body") or ""
        s = (url + "\n" + body).lower()

        score = 0
        for w in ["exposure", "gamma", "vanna", "charm", "dealer", "hedg", "notional", "strike", "expiration", "spx", "spy"]:
            if w in s:
                score += 3

        # JSON-ish
        if "json" in (it.get("ct") or "").lower():
            score += 2
        if "graphql" in s:
            score += 2
        if len(body) > 200:
            score += 1

        it2 = dict(it)
        it2["score"] = score
        out.append(it2)

    out.sort(key=lambda x: x.get("score", 0), reverse=True)
    return out[:MAX_CAPTURE_ITEMS]


def trim_body(item):
    it = dict(item)
    b = it.get("body") or ""
    if len(b) > MAX_BODY_CHARS:
        it["body"] = b[:MAX_BODY_CHARS]
    return it


def parse_statistics(page) -> dict:
    """
    Parse SPX statistics from the statistics page using text extraction.
    """
    stats = {
        "paradigm": None,
        "target": None,
        "lines_in_sand": None,
        "delta_decay_hedging": None,
        "opt_volume": None
    }
    
    try:
        # Wait for content to load
        page.wait_for_timeout(3000)
        
        # Get all text content
        all_text = page.inner_text("body")
        
        # Parse using simple string matching
        lines = all_text.split('\n')
        for i, line in enumerate(lines):
            line_clean = line.strip()
            
            # Paradigm
            if 'Paradigm' in line_clean and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and next_line != 'Target' and len(next_line) < 50:
                    stats["paradigm"] = next_line
                    print(f"[stats] Found Paradigm: {stats['paradigm']}", flush=True)
            
            # Target
            if line_clean == 'Target' and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and 'Lines' not in next_line and len(next_line) < 50:
                    stats["target"] = next_line
                    print(f"[stats] Found Target: {stats['target']}", flush=True)
            
            # Lines in the Sand
            if 'Lines in the Sand' in line_clean and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and len(next_line) < 50:
                    stats["lines_in_sand"] = next_line
                    print(f"[stats] Found Lines: {stats['lines_in_sand']}", flush=True)
            
            # Total 0DTE Delta Decay Hedging
            if 'Delta Decay Hedging' in line_clean and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and len(next_line) < 50:
                    stats["delta_decay_hedging"] = next_line
                    print(f"[stats] Found Delta: {stats['delta_decay_hedging']}", flush=True)
            
            # Total 0DTE Opt Volume
            if 'Opt Volume' in line_clean and i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and len(next_line) < 50:
                    stats["opt_volume"] = next_line
                    print(f"[stats] Found Volume: {stats['opt_volume']}", flush=True)
        
    except Exception as e:
        print(f"[stats] Parse error: {e}", flush=True)
    
    return stats


def run():
    if not DB_URL or not EMAIL or not PASS or not URL:
        raise RuntimeError("Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD / VOLLAND_URL")

    ensure_tables()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(90000)

        # hooks must be installed before any navigation
        page.add_init_script(INIT_CAPTURE_JS)

        # login once
        login_if_needed(page)

        while True:
            try:
                # ========== STEP 1: Scrape STATISTICS page FIRST ==========
                # (Do this first so it doesn't interfere with charm captures)
                stats_data = {}
                if STATS_URL:
                    try:
                        print(f"[volland] Fetching statistics from {STATS_URL}", flush=True)
                        page.goto(STATS_URL, wait_until="domcontentloaded", timeout=120000)
                        page.wait_for_timeout(int(WAIT_AFTER_GOTO_SEC * 1000))
                        
                        if "/sign-in" in page.url:
                            login_if_needed(page)
                            page.goto(STATS_URL, wait_until="domcontentloaded", timeout=120000)
                            page.wait_for_timeout(int(WAIT_AFTER_GOTO_SEC * 1000))
                        
                        stats_data = parse_statistics(page)
                        print(f"[stats] Paradigm={stats_data.get('paradigm')}, Target={stats_data.get('target')}, Lines={stats_data.get('lines_in_sand')}", flush=True)
                    except Exception as e:
                        print(f"[stats] Failed to scrape statistics: {e}", flush=True)
                        stats_data = {}

                # ========== STEP 2: Reset captures and go to CHARM page ==========
                reset_captures(page)
                
                print(f"[volland] Fetching charm data from {URL}", flush=True)
                page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(int(WAIT_AFTER_GOTO_SEC * 1000))

                if "/sign-in" in page.url:
                    login_if_needed(page)
                    reset_captures(page)
                    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(int(WAIT_AFTER_GOTO_SEC * 1000))

                # ========== STEP 3: Get captures from CHARM page ==========
                cap = get_captures(page)

                fetch_scored = filter_and_score(cap.get("fetch", []))
                xhr_scored   = filter_and_score(cap.get("xhr", []))

                # ws messages can be huge; keep only top few
                ws = cap.get("ws", [])
                ws = ws[-10:] if isinstance(ws, list) else []

                # ========== STEP 4: Build and save payload ==========
                payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "page_url": URL,  # Use the charm URL, not whatever page we're on
                    "statistics": stats_data,  # Statistics from step 1
                    "captures": {
                        "fetch_top": [trim_body(x) for x in fetch_scored],
                        "xhr_top":   [trim_body(x) for x in xhr_scored],
                        "ws_tail":   ws,
                        "counts": {
                            "fetch": len(cap.get("fetch", []) if isinstance(cap.get("fetch", []), list) else []),
                            "xhr":   len(cap.get("xhr", []) if isinstance(cap.get("xhr", []), list) else []),
                            "ws":    len(cap.get("ws", []) if isinstance(cap.get("ws", []), list) else []),
                        }
                    }
                }

                save_snapshot(payload)

                print(
                    "[volland] saved",
                    payload["ts_utc"],
                    "fetch=", payload["captures"]["counts"]["fetch"],
                    "xhr=", payload["captures"]["counts"]["xhr"],
                    "ws=", payload["captures"]["counts"]["ws"],
                    "top_fetch_score=", (fetch_scored[0]["score"] if fetch_scored else None),
                    "stats=", bool(stats_data.get("paradigm") or stats_data.get("target")),
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
                    save_snapshot({"error_event": err_payload})
                except Exception:
                    pass
                print("[volland] error:", e)
                traceback.print_exc()

            time.sleep(PULL_EVERY)


if __name__ == "__main__":
    run()
