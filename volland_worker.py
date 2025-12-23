import os, json, time, traceback, re
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright


# ========= ENV =========
DB_URL   = os.getenv("DATABASE_URL", "")
EMAIL    = os.getenv("VOLLAND_EMAIL", "")
PASS     = os.getenv("VOLLAND_PASSWORD", "")
URL      = os.getenv("VOLLAND_URL", "")  # workspace URL

PULL_EVERY     = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "60"))
SNIFF_SECONDS  = float(os.getenv("VOLLAND_SNIFF_SECONDS", "8"))   # request logging window
MAX_POINTS     = int(os.getenv("VOLLAND_MAX_POINTS", "6000"))     # safety cap


# ========= DB =========
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


# ========= LOGIN =========
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

    # already logged in
    if "/sign-in" not in page.url and page.locator("input[name='password'], input[type='password']").count() == 0:
        return

    email_box = page.locator("input[data-cy='sign-in-email-input'], input[name='email']").first
    pwd_box   = page.locator("input[data-cy='sign-in-password-input'], input[name='password'], input[type='password']").first

    email_box.wait_for(state="visible", timeout=90000)
    pwd_box.wait_for(state="visible", timeout=90000)

    email_box.fill(EMAIL)
    pwd_box.fill(PASS)

    submit = page.locator(
        "button:has-text('Log In'), button:has-text('Login'), button[type='submit']"
    ).first
    submit.click()

    handle_session_limit_modal(page)

    deadline = time.time() + 90
    while time.time() < deadline:
        handle_session_limit_modal(page)
        if "/sign-in" not in page.url:
            return
        page.wait_for_timeout(500)

    # still on sign-in
    body = ""
    try:
        body = (page.locator("body").inner_text() or "")[:600]
    except Exception:
        pass
    raise RuntimeError(f"Login did not complete. Still on: {page.url}. Body: {body}")


# ========= REQUEST URL SNIFF (to discover hidden endpoints) =========
def sniff_request_urls(page, seconds: float):
    urls = []

    def on_req(req):
        try:
            u = req.url
            # ignore noise
            if any(x in u for x in ("sentry.io", "gleap.io", "googletagmanager", "google-analytics")):
                return
            # keep likely relevant
            if ("vol.land" in u) or ("graphql" in u) or ("/api/" in u) or ("ws" in u):
                urls.append({"url": u, "type": req.resource_type})
        except Exception:
            return

    page.on("request", on_req)

    end = time.time() + seconds
    while time.time() < end:
        page.wait_for_timeout(200)

    try:
        page.remove_listener("request", on_req)
    except Exception:
        pass

    # de-dupe preserve order
    seen = set()
    out = []
    for item in urls:
        if item["url"] in seen:
            continue
        seen.add(item["url"])
        out.append(item)
        if len(out) >= 200:
            break
    return out


# ========= REAL DATA (read from chart library objects) =========
def extract_chart_series_js(page):
    """
    Try to extract real series points from Highcharts or ECharts objects.
    Returns dict: {highcharts, echarts, series:[...], meta:{...}}
    """
    js = r"""
() => {
  const out = { highcharts:false, echarts:false, series:[], meta:{} };

  // --- Highcharts ---
  try {
    if (window.Highcharts && window.Highcharts.charts) {
      out.highcharts = true;
      const charts = window.Highcharts.charts.filter(Boolean);
      out.meta.highcharts_charts = charts.length;

      charts.forEach((c, ci) => {
        (c.series || []).forEach((s, si) => {
          const ptsRaw = (s.points && s.points.length) ? s.points : (s.data || []);
          const pts = [];

          for (const p of ptsRaw) {
            const x = (p && p.x !== undefined) ? p.x : (p && p.category !== undefined ? p.category : undefined);
            const y = (p && p.y !== undefined) ? p.y : (p && p.value !== undefined ? p.value : undefined);
            if (x !== undefined && y !== undefined && y !== null) pts.push([x, y]);
          }

          if (pts.length) {
            out.series.push({
              lib: "highcharts",
              chart_index: ci,
              series_index: si,
              name: s.name || "",
              type: s.type || "",
              points: pts
            });
          }
        });
      });
    }
  } catch (e) {
    out.meta.highcharts_error = String(e);
  }

  // --- ECharts ---
  try {
    if (window.echarts) {
      out.echarts = true;

      const insts = [];
      // try common containers
      const candidates = Array.from(document.querySelectorAll("div, canvas, svg"));
      for (const el of candidates) {
        try {
          const inst = window.echarts.getInstanceByDom(el);
          if (inst) insts.push(inst);
        } catch (e) {}
      }
      out.meta.echarts_instances = insts.length;

      insts.forEach((inst, ci) => {
        try {
          const opt = inst.getOption();
          const sers = (opt && opt.series) ? opt.series : [];
          sers.forEach((s, si) => {
            const data = s.data || [];
            const pts = [];

            for (const p of data) {
              if (Array.isArray(p) && p.length >= 2) {
                pts.push([p[0], p[1]]);
              } else if (p && typeof p === "object") {
                const x = (p.name !== undefined) ? p.name : (p.x !== undefined ? p.x : undefined);
                const y = (p.value !== undefined) ? p.value : (p.y !== undefined ? p.y : undefined);
                if (x !== undefined && y !== undefined) pts.push([x, y]);
              }
            }

            if (pts.length) {
              out.series.push({
                lib: "echarts",
                chart_index: ci,
                series_index: si,
                name: s.name || "",
                type: s.type || "",
                points: pts
              });
            }
          });
        } catch (e) {
          out.meta.echarts_error = String(e);
        }
      });
    }
  } catch (e) {
    out.meta.echarts_error2 = String(e);
  }

  // diagnostics
  try { out.meta.svg_paths = document.querySelectorAll("svg path").length; } catch(e) {}
  try { out.meta.svg_count = document.querySelectorAll("svg").length; } catch(e) {}

  return out;
}
"""
    return page.evaluate(js)


def cap_series_points(series_list, max_points: int):
    """
    Cap total points to avoid huge DB rows.
    """
    total = 0
    out = []
    for s in series_list:
        pts = s.get("points", [])
        if not pts:
            continue
        remaining = max_points - total
        if remaining <= 0:
            break
        if len(pts) > remaining:
            s = dict(s)
            s["points"] = pts[:remaining]
            out.append(s)
            total += remaining
            break
        out.append(s)
        total += len(pts)
    return out, total


def run():
    if not DB_URL or not EMAIL or not PASS or not URL:
        raise RuntimeError("Missing env vars: DATABASE_URL / VOLLAND_EMAIL / VOLLAND_PASSWORD / VOLLAND_URL")

    ensure_tables()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(90000)

        login_if_needed(page)

        while True:
            try:
                page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(2500)

                if "/sign-in" in page.url:
                    login_if_needed(page)
                    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(2500)

                # 1) log request URLs (to find hidden API)
                req_urls = sniff_request_urls(page, seconds=SNIFF_SECONDS)

                # 2) extract real series from JS objects
                series_pack = extract_chart_series_js(page)
                series_pack["series"], total_pts = cap_series_points(series_pack.get("series", []), MAX_POINTS)

                payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "page_url": page.url,
                    "request_urls_sample": req_urls[:60],
                    "chart_series": series_pack,   # ✅ THIS is what we want (points!)
                    "points_total": total_pts,
                }

                save_snapshot(payload)
                print("[volland] saved", payload["ts_utc"], "series=", len(series_pack.get("series", [])), "pts=", total_pts)

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
