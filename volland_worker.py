# volland_worker.py
import os, json, time, traceback
from datetime import datetime, timezone
from bisect import bisect_left

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright


DB_URL   = os.getenv("DATABASE_URL", "")
EMAIL    = os.getenv("VOLLAND_EMAIL", "")
PASS     = os.getenv("VOLLAND_PASSWORD", "")
URL      = os.getenv("VOLLAND_URL", "")

PULL_EVERY           = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "60"))
WAIT_AFTER_GOTO_SEC  = float(os.getenv("VOLLAND_WAIT_AFTER_GOTO_SEC", "6"))

# keep 20 below + ATM + 20 above
KEEP_BELOW = int(os.getenv("VOLLAND_KEEP_STRIKES_BELOW", "20"))
KEEP_ABOVE = int(os.getenv("VOLLAND_KEEP_STRIKES_ABOVE", "20"))

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
  const cap = { fetch: [], xhr: [], ws: [], note: [] };

  function push(arr, item) {
    try { arr.push(item); if (arr.length > MAX) arr.shift(); } catch(e) {}
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
  window.__volland_cap_reset = () => { cap.fetch = []; cap.xhr = []; cap.ws = []; cap.note = []; };
})();
"""


def get_captures(page) -> dict:
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
    """
    Keep likely relevant endpoints and score candidates.
    """
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


# ---------- strike limiting / JSON shrinking ----------

def _to_float(x):
    try:
        return float(x)
    except Exception:
        return None


def limit_strikes_around_spot(points, spot, below=20, above=20, include_atm=True):
    """
    points: list of dicts like {"x":"6900","y":-164833242.48}
    spot  : float
    returns filtered list (keeps closest strikes around spot)
    """
    if not isinstance(points, list) or not points or spot is None:
        return points

    parsed = []
    for it in points:
        if not isinstance(it, dict):
            continue
        k = _to_float(it.get("x"))
        v = _to_float(it.get("y"))
        if k is None or v is None:
            continue
        parsed.append((int(round(k)), float(v)))

    if not parsed:
        return points

    parsed.sort(key=lambda t: t[0])
    strikes = [k for k, _ in parsed]

    i = bisect_left(strikes, spot)
    if i == 0:
        atm_idx = 0
    elif i >= len(strikes):
        atm_idx = len(strikes) - 1
    else:
        atm_idx = i if abs(strikes[i] - spot) < abs(strikes[i - 1] - spot) else i - 1

    left = max(0, atm_idx - below)
    right = min(len(strikes), atm_idx + above + 1)

    window = parsed[left:right]
    if not include_atm:
        atm_strike = strikes[atm_idx]
        window = [t for t in window if t[0] != atm_strike]

    return [{"x": str(k), "y": v} for k, v in window]


def _is_xy_points_list(lst) -> bool:
    if not isinstance(lst, list) or len(lst) < 10:
        return False
    # must be list of dicts with x/y mostly numeric-ish
    ok = 0
    for it in lst[:20]:
        if isinstance(it, dict) and ("x" in it) and ("y" in it):
            if _to_float(it.get("x")) is not None and _to_float(it.get("y")) is not None:
                ok += 1
    return ok >= max(3, min(10, len(lst) // 3))


def _find_spot(obj):
    """
    Search for a spot-like value in dicts.
    """
    if not isinstance(obj, dict):
        return None

    for k in ("spot", "underlying", "underlyingPrice", "underlying_price", "last", "price", "px"):
        if k in obj:
            v = _to_float(obj.get(k))
            if v is not None and v > 0:
                return v

    # common nesting
    for k in ("meta", "metadata", "underlyingData", "underlying_data"):
        v = obj.get(k)
        if isinstance(v, dict):
            s = _find_spot(v)
            if s is not None:
                return s

    return None


def _shrink_json(obj, spot=None):
    """
    Recursively:
    - when we detect strike-point arrays [{x,y}...], keep only 20 below + 20 above around spot
    - leave other content as-is
    """
    if isinstance(obj, dict):
        spot_here = spot if spot is not None else _find_spot(obj)
        out = {}
        for k, v in obj.items():
            if _is_xy_points_list(v) and spot_here is not None:
                out[k] = limit_strikes_around_spot(v, spot_here, below=KEEP_BELOW, above=KEEP_ABOVE, include_atm=True)
            else:
                out[k] = _shrink_json(v, spot_here)
        return out

    if isinstance(obj, list):
        # keep list as-is, but recurse inside
        return [_shrink_json(v, spot) for v in obj]

    return obj


def optimize_and_trim_item(item: dict) -> dict:
    """
    - If body is JSON and contains strike series, shrink to 20/20 around spot.
    - Always cap body length.
    """
    it = dict(item)
    body = it.get("body") or ""
    ct = (it.get("ct") or "").lower()

    # try JSON optimization
    if body and ("json" in ct or body.lstrip().startswith("{") or body.lstrip().startswith("[")):
        try:
            obj = json.loads(body)
            obj2 = _shrink_json(obj, None)
            # compact JSON
            body2 = json.dumps(obj2, separators=(",", ":"), ensure_ascii=False)
            it["body"] = body2
        except Exception:
            # keep original body if parsing fails
            pass

    # final cap
    b = it.get("body") or ""
    if len(b) > MAX_BODY_CHARS:
        it["body"] = b[:MAX_BODY_CHARS]
    return it


def run():
    if not DB_URL or not EMAIL or not PASS or not URL:
        raise RuntimeError("Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD / VOLLAND_URL")

    ensure_tables()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(90000)

        # ✅ hooks must be installed before any navigation
        page.add_init_script(INIT_CAPTURE_JS)

        # login once
        login_if_needed(page)

        while True:
            try:
                reset_captures(page)

                page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(int(WAIT_AFTER_GOTO_SEC * 1000))

                if "/sign-in" in page.url:
                    login_if_needed(page)
                    reset_captures(page)
                    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(int(WAIT_AFTER_GOTO_SEC * 1000))

                cap = get_captures(page)

                fetch_scored = filter_and_score(cap.get("fetch", []))
                xhr_scored   = filter_and_score(cap.get("xhr", []))

                # ws messages can be huge; keep only tail
                ws = cap.get("ws", [])
                ws = ws[-10:] if isinstance(ws, list) else []

                payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "page_url": page.url,
                    "captures": {
                        "fetch_top": [optimize_and_trim_item(x) for x in fetch_scored],
                        "xhr_top":   [optimize_and_trim_item(x) for x in xhr_scored],
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
                    "fetch=",
                    payload["captures"]["counts"]["fetch"],
                    "xhr=",
                    payload["captures"]["counts"]["xhr"],
                    "ws=",
                    payload["captures"]["counts"]["ws"],
                    "top_fetch_score=",
                    (fetch_scored[0]["score"] if fetch_scored else None),
                    "top_xhr_score=",
                    (xhr_scored[0]["score"] if xhr_scored else None),
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
