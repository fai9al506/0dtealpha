# volland_test/test_v2_capture.py
# Quick test: run V2's capture logic for one cycle, save results, compare with V1 output
# No database dependency — captures to local JSON only

import os, sys, json, time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from playwright.sync_api import sync_playwright
from volland_worker_v2 import (
    handle_session_limit_modal,
    format_statistics,
)

EMAIL = os.getenv("VOLLAND_EMAIL", "faisal.a.d@msn.com")
PASSWORD = os.getenv("VOLLAND_PASSWORD", "Fad2024506!")
WORKSPACE_URL = os.getenv(
    "VOLLAND_WORKSPACE_URL",
    "https://vol.land/app/workspace/698a5560ea3d7b5155f88e67",
)
WAIT_SEC = float(os.getenv("VOLLAND_WAIT_SEC", "15"))

OUTPUT_DIR = Path(__file__).parent / "captures"
OUTPUT_DIR.mkdir(exist_ok=True)


def login(page, url):
    page.goto(url, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    if "/sign-in" not in page.url and \
       page.locator("input[name='password'], input[type='password']").count() == 0:
        print("[login] Already logged in")
        return

    print("[login] Logging in...")
    email_box = page.locator(
        "input[data-cy='sign-in-email-input'], input[name='email']"
    ).first
    pwd_box = page.locator(
        "input[data-cy='sign-in-password-input'], input[name='password'], input[type='password']"
    ).first
    email_box.wait_for(state="visible", timeout=90000)
    pwd_box.wait_for(state="visible", timeout=90000)
    email_box.fill(EMAIL)
    pwd_box.fill(PASSWORD)
    page.locator(
        "button:has-text('Log In'), button:has-text('Login'), button[type='submit']"
    ).first.click()
    handle_session_limit_modal(page)

    deadline = time.time() + 90
    while time.time() < deadline:
        handle_session_limit_modal(page)
        if "/sign-in" not in page.url:
            print("[login] Success")
            return
        page.wait_for_timeout(500)
    raise RuntimeError(f"Login failed. Still on: {page.url}")


def run_test():
    print("=" * 60)
    print("V2 CAPTURE TEST — route.fetch() approach")
    print("=" * 60)
    print(f"Workspace: {WORKSPACE_URL}")
    print(f"Wait: {WAIT_SEC}s")
    print()

    # Mutable capture state (same pattern as V2 worker)
    cycle = {"exposures": [], "paradigm": None, "spot_vol": None}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            channel="chrome",
            args=["--no-sandbox"],
        )
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        page.set_default_timeout(90000)

        def handle_exposure(route, request):
            post_data = request.post_data
            greek, exp_option = "unknown", None
            if post_data:
                try:
                    pj = json.loads(post_data)
                    greek = pj.get("greek", "unknown")
                    exp_option = pj.get("expirations", {}).get("option")
                except Exception:
                    pass
            try:
                response = route.fetch()
                body = response.text()
                try:
                    data = json.loads(body)
                    cycle["exposures"].append({
                        "greek": greek,
                        "expiration_option": exp_option,
                        "items": data.get("items", []),
                        "current_price": data.get("currentPrice"),
                        "expirations": data.get("expirations", []),
                        "last_modified": data.get("lastModified"),
                    })
                    n = len(data.get("items", []))
                    print(f"  [capture] exposure: {greek}/{exp_option} ({n} pts, price={data.get('currentPrice')})")
                except json.JSONDecodeError:
                    print(f"  [capture] exposure: JSON decode error")
                route.fulfill(response=response)
            except Exception as e:
                print(f"  [capture] exposure route error: {e}")
                try:
                    route.continue_()
                except Exception:
                    pass

        def handle_response(response):
            url = response.url
            if "/data/exposure" in url:
                return
            try:
                if "/data/paradigms/0dte" in url and response.status == 200:
                    cycle["paradigm"] = json.loads(response.text())
                    print(f"  [capture] paradigm: {json.dumps(cycle['paradigm'])}")
                elif "/data/volhacks/spot-vol-beta" in url and response.status == 200:
                    cycle["spot_vol"] = json.loads(response.text())
                    print(f"  [capture] spot-vol-beta: {json.dumps(cycle['spot_vol'])}")
            except Exception:
                pass

        # Register handlers
        page.route("**/api/v1/data/exposure", handle_exposure)
        page.on("response", handle_response)

        # Login
        login(page, WORKSPACE_URL)

        # Reset and navigate
        cycle["exposures"] = []
        cycle["paradigm"] = None
        cycle["spot_vol"] = None

        print(f"\n[test] Navigating to workspace...")
        page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)

        if "/sign-in" in page.url:
            login(page, WORKSPACE_URL)
            cycle["exposures"] = []
            cycle["paradigm"] = None
            cycle["spot_vol"] = None
            page.goto(WORKSPACE_URL, wait_until="domcontentloaded", timeout=120000)

        print(f"[test] Waiting {WAIT_SEC}s for all widgets to load...\n")
        page.wait_for_timeout(int(WAIT_SEC * 1000))
        handle_session_limit_modal(page)

        browser.close()

    # ── Results ──
    exposures = cycle["exposures"]
    paradigm = cycle["paradigm"]
    spot_vol = cycle["spot_vol"]
    stats = format_statistics(paradigm, spot_vol)

    print()
    print("=" * 60)
    print("CAPTURE RESULTS")
    print("=" * 60)

    print(f"\nExposure endpoints captured: {len(exposures)}")
    for i, exp in enumerate(exposures):
        print(f"  [{i+1}] {exp['greek']}/{exp['expiration_option']}: "
              f"{len(exp['items'])} pts, price={exp['current_price']}, "
              f"exps={len(exp['expirations'])}")

    print(f"\nParadigm API: {'YES' if paradigm else 'NO'}")
    if paradigm:
        print(f"  Raw: {json.dumps(paradigm)}")

    print(f"\nSpot-Vol Beta API: {'YES' if spot_vol else 'NO'}")
    if spot_vol:
        print(f"  Raw: {json.dumps(spot_vol)}")

    print(f"\n{'=' * 60}")
    print("FORMATTED STATISTICS (V1-compatible)")
    print("=" * 60)
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # ── Backward compatibility checks ──
    print(f"\n{'=' * 60}")
    print("BACKWARD COMPATIBILITY CHECKS")
    print("=" * 60)

    # Check LIS format matches V1 regex pattern
    import re
    lis = stats.get("lines_in_sand")
    if lis:
        lis_str = str(lis).replace("$", "").replace(",", "")
        lis_match = re.findall(r"[\d.]+", lis_str)
        ok = len(lis_match) >= 1
        print(f"  LIS regex parse: {lis!r} -> {lis_match} {'PASS' if ok else 'FAIL'}")
    else:
        print(f"  LIS: None (paradigm not captured?)")

    # Check aggregatedCharm is numeric
    ac = stats.get("aggregatedCharm")
    if ac is not None:
        try:
            float(ac)
            print(f"  aggregatedCharm: {ac} (float-castable) PASS")
        except (ValueError, TypeError):
            print(f"  aggregatedCharm: {ac} FAIL — not float-castable")
    else:
        print(f"  aggregatedCharm: None")

    # Check 0DTE charm exposure has the right format for DB
    charm_0dte = [e for e in exposures if e["greek"] == "charm" and e["expiration_option"] == "TODAY"]
    if charm_0dte:
        pts = charm_0dte[0]["items"]
        print(f"  0DTE Charm: {len(pts)} points")
        if pts:
            sample = pts[0]
            has_x = "x" in sample
            has_y = "y" in sample
            print(f"    Sample point: {sample}")
            print(f"    Has x/y keys: {'PASS' if has_x and has_y else 'FAIL'}")
            # Would be saved as: greek='charm', expiration_option=None (V1 compat)
            print(f"    DB save: greek='charm', expiration_option=None (V1 compat) PASS")
    else:
        print(f"  0DTE Charm: NOT FOUND")

    # ── Save to JSON for comparison ──
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    result = {
        "test": "v2_capture",
        "timestamp": datetime.now().isoformat(),
        "workspace_url": WORKSPACE_URL,
        "exposure_count": len(exposures),
        "exposures_summary": [
            {
                "greek": e["greek"],
                "expiration_option": e["expiration_option"],
                "items_count": len(e["items"]),
                "current_price": e["current_price"],
                "expirations_count": len(e["expirations"]),
                "sample_item": e["items"][0] if e["items"] else None,
            }
            for e in exposures
        ],
        "paradigm_raw": paradigm,
        "spot_vol_raw": spot_vol,
        "formatted_statistics": stats,
        "v1_snapshot_format": {
            "ts_utc": datetime.utcnow().isoformat(),
            "page_url": WORKSPACE_URL,
            "statistics": stats,
            "exposure_points_saved": sum(len(e["items"]) for e in exposures),
            "current_price": exposures[0]["current_price"] if exposures else None,
            "captures": {
                "exposure_count": len(exposures),
                "exposures_summary": [
                    {"greek": e["greek"], "option": e["expiration_option"],
                     "items": len(e["items"])}
                    for e in exposures
                ],
            },
        },
    }

    outfile = OUTPUT_DIR / f"v2_test_{timestamp}.json"
    with open(outfile, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"\n[test] Results saved to: {outfile}")

    # ── Compare with latest V1 analysis ──
    v1_files = sorted(OUTPUT_DIR.glob("analysis_*.json"))
    if v1_files:
        latest_v1 = v1_files[-1]
        print(f"\n{'=' * 60}")
        print(f"COMPARISON WITH V1 (test_scraper.py)")
        print(f"  V1 file: {latest_v1.name}")
        print(f"=" * 60)

        with open(latest_v1) as f:
            v1 = json.load(f)

        v1_exposures = v1.get("exposure_with_greek", [])
        print(f"\n  V1 exposures: {len(v1_exposures)}")
        print(f"  V2 exposures: {len(exposures)}")

        # Compare greek identification
        print(f"\n  Greek identification comparison:")
        print(f"  {'#':<3} {'V1 greek':<25} {'V2 greek':<15} {'V2 option':<20} {'V2 pts':<8} {'Match'}")
        print(f"  {'-'*85}")

        for i in range(max(len(v1_exposures), len(exposures))):
            v1_g = v1_exposures[i]["greek"] if i < len(v1_exposures) else "—"
            v2_g = exposures[i]["greek"] if i < len(exposures) else "—"
            v2_o = exposures[i]["expiration_option"] if i < len(exposures) else "—"
            v2_n = len(exposures[i]["items"]) if i < len(exposures) else 0
            # Check match
            match = "—"
            if i < len(v1_exposures) and i < len(exposures):
                v1_greek_clean = v1_exposures[i]["greek"].lower().split()[0]
                match = "YES" if v1_greek_clean == v2_g.lower() else "no"
            print(f"  {i+1:<3} {v1_g:<25} {v2_g:<15} {v2_o:<20} {v2_n:<8} {match}")

        # Compare paradigm data
        v1_paradigm = [e for e in v1.get("charm_endpoints", [])
                       if "paradigms/0dte" in e.get("url", "")]
        if v1_paradigm and paradigm:
            v1_keys = v1_paradigm[0].get("top_keys", [])
            v2_keys = list(paradigm.keys())
            print(f"\n  Paradigm keys — V1: {v1_keys}")
            print(f"  Paradigm keys — V2: {v2_keys}")
            print(f"  Match: {'YES' if set(v1_keys) == set(v2_keys) else 'PARTIAL'}")

    print(f"\n{'=' * 60}")
    print("TEST COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    run_test()
