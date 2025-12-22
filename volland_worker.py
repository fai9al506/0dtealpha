import os, json, time, traceback, base64
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
    # Go directly to workspace; if not logged in, it should redirect to login
    page.goto(URL, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    # If a "Sign in / Log in" button exists, click it
    for text in ["Sign in", "Log in", "Login"]:
        btn = page.get_by_role("button", name=text)
        if btn.count() > 0:
            btn.first.click()
            page.wait_for_timeout(1200)
            break

    # If we are already inside the app (no password field), skip
    if page.locator("input[type='password']").count() == 0:
        return

    # Flexible selectors for email field
    email_label = page.get_by_label("Email")
    if email_label.count() > 0:
        email_box = email_label.first
    else:
        email_box = page.locator(
            "input[type='email'], input[name='email'], input[autocomplete='email'], input[placeholder*='mail' i]"
        ).first

    pwd_box = page.locator("input[type='password']").first

    try:
        email_box.wait_for(timeout=90000)
        pwd_box.wait_for(timeout=90000)
    except Exception:
        print("[login] could not find login fields")
        print("[login] url:", page.url)
        try:
            print("[login] title:", page.title())
        except Exception:
            pass
        page.screenshot(path="debug_login.png", full_page=True)
        raise

    email_box.fill(EMAIL)
    pwd_box.fill(PASS)

    # Submit (try common patterns)
    submit = page.locator(
        "button[type='submit'], button:has-text('Sign in'), button:has-text('Log in')"
    ).first
    submit.click()
    page.wait_for_timeout(2500)


def extract_tooltip(page) -> dict:
    """
    Robust: looks for chart target in main page or iframes, tries hover and reads tooltip.
    If nothing found, returns debug data + screenshot (base64) so we can see what loaded.
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

    # Give the app time to render
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

    tooltip_selectors = [
        "[role='tooltip']",
        ".tooltip",
        "[data-tooltip]",
        "div:has-text('Strike')",
        "div:has-text('Gamma')",
        "div:has-text('Vanna')",
        "div:has-text('Charm')",
    ]

    frames = [page.main_frame] + [f for f in page.frames if f != page.main_frame]

    best_tooltip = ""
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
                    try:
                        tloc = fr.locator(sel).first
                        if tloc.count() > 0:
                            candidates.append((sel, (tloc.inner_text() or "").strip()))
                    except Exception:
                        pass
                    try:
                        tloc2 = page.locator(sel).first
                        if tloc2.count() > 0:
                            candidates.append((sel, (tloc2.inner_text() or "").strip()))
                    except Exception:
                        pass

                for sel, txt in candidates:
                    if len(txt) > len(best_tooltip):
                        best_tooltip = txt
                        best_used = {"frame_url": fr.url, "chart_sel": chart_sel, "tooltip_sel": sel}

                if len(best_tooltip) >= 10:
                    return {
                        "tooltip_raw": best_tooltip,
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
                page.wait_for_timeout(2000)

                tip = extract_tooltip(page)

                payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "tooltip_raw": tip.get("tooltip_raw", ""),
                    "debug": tip.get("debug", {})
                }

                save_snapshot(payload)
                print("[volland] saved", payload["ts_utc"], "tooltip_len=", len(payload["tooltip_raw"]))

            except Exception as e:
                err_payload = {
                    "ts_utc": datetime.now(timezone.utc).isoformat(),
                    "tooltip_raw": "",
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
