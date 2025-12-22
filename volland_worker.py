import os, json, time, traceback, base64, re
from datetime import datetime, timezone

import psycopg
from psycopg.rows import dict_row
from playwright.sync_api import sync_playwright


DB_URL = os.getenv("DATABASE_URL", "")
EMAIL  = os.getenv("VOLLAND_EMAIL", "")
PASS   = os.getenv("VOLLAND_PASSWORD", "")
URL    = os.getenv("VOLLAND_URL", "")
PULL_EVERY = int(os.getenv("VOLLAND_PULL_EVERY_SEC", "60"))


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
        cur.execute(
            "INSERT INTO volland_snapshots(payload) VALUES (%s::jsonb)",
            (json.dumps(payload),)
        )


def login_if_needed(page):
    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    # already logged in
    if "/sign-in" not in page.url and page.locator("input[name='password'], input[type='password']").count() == 0:
        return

    # Volland login fields (email is type="text")
    email_box = page.locator(
        "input[data-cy='sign-in-email-input'], input[name='email']"
    ).first
    pwd_box = page.locator(
        "input[data-cy='sign-in-password-input'], input[name='password'], input[type='password']"
    ).first

    email_box.wait_for(state="visible", timeout=90000)
    pwd_box.wait_for(state="visible", timeout=90000)

    email_box.fill(EMAIL)
    pwd_box.fill(PASS)

    # Click "Log In"
    btn = page.get_by_role("button", name=re.compile(r"^log in$", re.I))
    if btn.count() > 0:
        btn.first.click()
    else:
        page.locator("button[type='submit']").first.click()

    # Wait until we leave /sign-in
    page.wait_for_url(lambda u: "/sign-in" not in u, timeout=90000)


def extract_tooltip(page) -> dict:
    """
    Finds a chart element (canvas/svg/etc) in main page or iframes, hovers it,
    and grabs text from likely tooltip/container selectors.

    Note: In Volland this often returns the "accessible chart text" (big text).
    We'll parse it into structured fields using parse_volland_text().
    """
    def make_debug(reason: str) -> dict:
        try:
            png = page.screenshot(full_page=False)
            shot_b64 = base64.b64encode(png).decode("ascii")
        except Exception:
            shot_b64 = ""

        frame_urls = []
        try:
            frame_urls = [f.url for f in page.frames]
        except Exception:
            pass

        body_sample = ""
        try:
            body_sample = (page.locator("body").inner_text() or "")[:800]
        except Exception:
            pass

        return {
            "tooltip_raw": "",
            "debug": {
                "reason": reason,
                "url": page.url,
                "title": page.title(),
                "frame_urls": frame_urls[:20],
                "body_text_sample": body_sample,
                "canvas_count_main": page.locator("canvas").count(),
                "svg_count_main": page.locator("svg").count(),
                "screenshot_b64_png": shot_b64
            }
        }

    try:
        page.wait_for_load_state("networkidle", timeout=120000)
    except Exception:
        pass
    page.wait_for_timeout(2000)

    chart_selectors = [
        "canvas",
        "svg",
        "[role='img']",
        "div[class*='chart' i]",
        "div[class*='canvas' i]",
    ]

    # These are "best effort" selectors; Volland seems to expose text around "Gamma/Vanna/Charm"
    tooltip_selectors = [
        "[role='tooltip']",
        ".tooltip",
        "[data-tooltip]",
        "div:has-text('Gamma')",
        "div:has-text('Vanna')",
        "div:has-text('Charm')",
        "div:has-text('Notional Hedging Requirement')",
    ]

    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]

    best_text = ""
    best_used = {"frame_url": None, "chart_sel": None, "tooltip_sel": None}

    for fr in frames:
        for chart_sel in chart_selectors:
            loc = fr.locator(chart_sel).first
            try:
                loc.wait_for(state="visible", timeout=5000)
            except Exception:
                continue

            box = loc.bounding_box()
            if not box:
                continue

            points = [(0.55, 0.35), (0.65, 0.40), (0.75, 0.45), (0.60, 0.55)]
            for (rx, ry) in points:
                x = box["x"] + box["width"] * rx
                y = box["y"] + box["height"] * ry

                page.mouse.move(x, y)
                page.wait_for_timeout(400)

                candidates = []
                for sel in tooltip_selectors:
                    # same frame
                    try:
                        tloc = fr.locator(sel).first
                        if tloc.count() > 0:
                            candidates.append((sel, (tloc.inner_text() or "").strip()))
                    except Exception:
                        pass
                    # page-level
                    try:
                        tloc2 = page.locator(sel).first
                        if tloc2.count() > 0:
                            candidates.append((sel, (tloc2.inner_text() or "").strip()))
                    except Exception:
                        pass

                for sel, txt in candidates:
                    if len(txt) > len(best_text):
                        best_text = txt
                        best_used = {"frame_url": fr.url, "chart_sel": chart_sel, "tooltip_sel": sel}

                if len(best_text) >= 20:
                    return {
                        "tooltip_raw": best_text,
                        "debug": {
                            "url": page.url,
                            "title": page.title(),
                            "frame_url_used": best_used["frame_url"],
                            "chart_selector_used": best_used["chart_sel"],
                            "tooltip_selector_used": best_used["tooltip_sel"],
                            "canvas_count_main": page.locator("canvas").count(),
                            "svg_count_main": page.locator("svg").count(),
                        }
                    }

    return make_debug("No visible chart target (canvas/svg/chart div) found in main page or frames.")


def parse_volland_text(raw: str) -> dict:
    """
    Extracts useful fields from Volland's accessible chart text.
    This is not the full curve values, but it gives:
      - spot
      - as_of_utc
      - expiration_label
      - strikes list
      - exposure_ticks_m ($-200M ... etc)
    """
    out = {}

    # spot like $6876.26
    m = re.search(r"\$([0-9]+\.[0-9]+)", raw)
    if m:
        try:
            out["spot"] = float(m.group(1))
        except Exception:
            pass

    # as-of timestamp like: as of 2025-12-22T23:00:00Z
    m = re.search(r"as of\s+([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:]+Z)", raw, flags=re.I)
    if m:
        out["as_of_utc"] = m.group(1)

    # Expiration label like: Expiration: Dec 26
    m = re.search(r"Expiration:\s*([A-Za-z]{3}\s+\d{1,2})", raw)
    if m:
        out["expiration_label"] = m.group(1)

    # Collect strike ladder numbers like 5250.00, 5550.00, ...
    strikes = re.findall(r"\b(\d{4,5}\.\d{2})\b", raw)
    strike_vals = []
    for s in strikes:
        try:
            v = float(s)
        except Exception:
            continue
        if 2000 <= v <= 9000:
            strike_vals.append(v)

    # de-duplicate while preserving order
    seen = set()
    dedup = []
    for v in strike_vals:
        if v not in seen:
            seen.add(v)
            dedup.append(v)

    out["strikes"] = dedup[:250]

    # Exposure scale ticks like $-200M ... $100M
    ticks = re.findall(r"\$(-?\d+)M", raw)
    if ticks:
        vals = []
        for x in ticks:
            try:
                vals.append(int(x))
            except Exception:
                pass
        if vals:
            out["exposure_ticks_m"] = vals

    return out


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

        # login once at start
        login_if_needed(page)

        while True:
            try:
                page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                page.wait_for_timeout(2000)

                # ensure we didn't get kicked to sign-in
                if "/sign-in" in page.url:
                    login_if_needed(page)
                    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
                    page.wait_for_timeout(2000)

                tip = extract_tooltip(page)
                raw = tip.get("tooltip_raw", "") or ""
                parsed = parse_volland_text(raw) if raw else {}

                payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "raw": raw,            # keep for debugging
                    "parsed": parsed,      # ✅ this is what your web should use
                    "debug": tip.get("debug", {})
                }

                save_snapshot(payload)
                print(
                    "[volland] saved",
                    payload["ts_utc"],
                    "raw_len=",
                    len(raw),
                    "parsed_keys=",
                    list(parsed.keys())
                )

            except Exception as e:
                err_payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "raw": "",
                    "parsed": {},
                    "debug": {
                        "reason": "exception",
                        "error": str(e),
                        "url": getattr(page, "url", "")
                    }
                }
                try:
                    save_snapshot(err_payload)
                except Exception:
                    pass

                print("[volland] error:", e)
                traceback.print_exc()

            time.sleep(PULL_EVERY)


if __name__ == "__main__":
    run()
