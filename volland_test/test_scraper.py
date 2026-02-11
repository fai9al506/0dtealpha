# volland_test/test_scraper.py
# Isolated test script - NO connection to production database
# Captures Volland network requests and saves to local JSON files

import os
import json
import time
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright

# ===== CONFIGURATION =====
EMAIL = os.getenv("VOLLAND_EMAIL", "faisal.a.d@msn.com")
PASSWORD = os.getenv("VOLLAND_PASSWORD", "Fad2024506!")
TEST_URL = os.getenv("VOLLAND_TEST_URL", "https://vol.land/app/workspace/698a5560ea3d7b5155f88e67")

# Output directory (local files only - no database!)
OUTPUT_DIR = Path(__file__).parent / "captures"
OUTPUT_DIR.mkdir(exist_ok=True)

# Store captured responses at browser level (not JS hooks)
CAPTURED_RESPONSES = []

# Store request POST bodies in order for matching with responses
REQUEST_POST_BODIES = []

# Store widget labels extracted from DOM
WIDGET_LABELS = []


def handle_session_modal(page) -> bool:
    """Handle 'session limit' modal if it appears."""
    btn = page.locator("button[data-cy='confirmation-modal-confirm-button'], button:has-text('Continue')").first
    if btn.count() == 0:
        return False
    try:
        btn.wait_for(state="visible", timeout=3000)
        btn.click()
        page.wait_for_timeout(1200)
        print("[login] Session modal: clicked Continue")
        return True
    except Exception:
        return False


def login(page, url):
    """Login to Volland."""
    print(f"[login] Navigating to {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=120000)
    page.wait_for_timeout(1500)

    if "/sign-in" not in page.url and page.locator("input[name='password'], input[type='password']").count() == 0:
        print("[login] Already logged in!")
        return

    print("[login] Login required, filling credentials...")
    email_box = page.locator("input[data-cy='sign-in-email-input'], input[name='email']").first
    pwd_box = page.locator("input[data-cy='sign-in-password-input'], input[name='password'], input[type='password']").first

    email_box.wait_for(state="visible", timeout=90000)
    pwd_box.wait_for(state="visible", timeout=90000)

    email_box.fill(EMAIL)
    pwd_box.fill(PASSWORD)

    page.locator("button:has-text('Log In'), button:has-text('Login'), button[type='submit']").first.click()
    handle_session_modal(page)

    deadline = time.time() + 90
    while time.time() < deadline:
        handle_session_modal(page)
        if "/sign-in" not in page.url:
            print("[login] Login successful!")
            return
        page.wait_for_timeout(500)

    raise RuntimeError(f"Login failed. Still on: {page.url}")


def extract_widget_labels(page) -> list:
    """Extract widget labels from DOM using h3.css-1yon14q elements."""
    labels = []
    try:
        # Find all h3 elements with the chart label class
        h3_elements = page.locator("h3.css-1yon14q").all()
        print(f"[dom] Found {len(h3_elements)} h3.css-1yon14q elements")

        for i, h3 in enumerate(h3_elements):
            try:
                text = h3.inner_text().strip()
                if text:
                    # Try to get parent container info
                    parent_info = {}
                    try:
                        # Get bounding box for position
                        box = h3.bounding_box()
                        if box:
                            parent_info["position"] = {"x": box["x"], "y": box["y"]}
                    except:
                        pass

                    labels.append({
                        "index": i,
                        "label": text,
                        "position": parent_info.get("position", {})
                    })
                    print(f"[dom] Widget {i}: '{text}' at {parent_info.get('position', 'unknown')}")
            except Exception as e:
                print(f"[dom] Error reading h3 {i}: {e}")

        # Also try alternative selectors if needed
        if not labels:
            print("[dom] Trying alternative selectors...")
            # Try finding by text content
            for greek in ["Charm", "Vanna", "Delta", "Gamma", "Theta"]:
                elements = page.locator(f"h3:has-text('{greek}')").all()
                for el in elements:
                    try:
                        text = el.inner_text().strip()
                        if text and text not in [l["label"] for l in labels]:
                            labels.append({"label": text, "index": len(labels)})
                            print(f"[dom] Found via text: '{text}'")
                    except:
                        pass

    except Exception as e:
        print(f"[dom] Error extracting labels: {e}")

    return labels


def extract_widget_details(page) -> list:
    """Extract detailed widget info including expiration settings from DOM."""
    widgets = []
    try:
        # JavaScript to extract widget info from the page
        js_code = """
        () => {
            const widgets = [];

            // Find all chart containers (looking for h3 with greek names)
            const h3s = document.querySelectorAll('h3');
            const greeks = ['Charm', 'Vanna', 'Delta', 'Gamma', 'Theta'];

            h3s.forEach((h3, idx) => {
                const text = h3.innerText.trim();
                if (greeks.some(g => text.includes(g))) {
                    const widget = {
                        index: idx,
                        label: text,
                        className: h3.className,
                        position: null,
                        parentId: null,
                        siblingText: []
                    };

                    // Get position
                    const rect = h3.getBoundingClientRect();
                    widget.position = { x: rect.x, y: rect.y, width: rect.width, height: rect.height };

                    // Try to find parent container with data or id
                    let parent = h3.parentElement;
                    for (let i = 0; i < 5 && parent; i++) {
                        if (parent.id) {
                            widget.parentId = parent.id;
                            break;
                        }
                        if (parent.dataset && Object.keys(parent.dataset).length > 0) {
                            widget.parentData = {...parent.dataset};
                            break;
                        }
                        parent = parent.parentElement;
                    }

                    // Get sibling text (might contain expiration info)
                    const container = h3.closest('div');
                    if (container) {
                        const texts = container.querySelectorAll('span, p, div');
                        texts.forEach(el => {
                            const t = el.innerText.trim();
                            if (t && t.length < 100 && t !== text) {
                                widget.siblingText.push(t);
                            }
                        });
                        // Dedupe and limit
                        widget.siblingText = [...new Set(widget.siblingText)].slice(0, 10);
                    }

                    widgets.push(widget);
                }
            });

            return widgets;
        }
        """
        widgets = page.evaluate(js_code)
        print(f"[dom] Extracted {len(widgets)} widget details via JS")
        for w in widgets:
            print(f"[dom]   {w.get('label')}: pos=({w.get('position', {}).get('x', '?')}, {w.get('position', {}).get('y', '?')})")

    except Exception as e:
        print(f"[dom] Error in JS extraction: {e}")

    return widgets


def on_response(response):
    """Capture network responses at browser level."""
    url = response.url

    # Skip analytics and other noise
    skip_patterns = [
        "sentry.io", "gleap.io", "googletagmanager", "google-analytics",
        "intercom", "fonts.googleapis", "fonts.gstatic", ".png", ".jpg",
        ".svg", ".woff", ".css", "favicon"
    ]
    if any(x in url.lower() for x in skip_patterns):
        return

    try:
        content_type = response.headers.get("content-type", "")

        # Only capture JSON/text responses
        if "json" in content_type or "text" in content_type or "graphql" in url.lower():
            body = ""
            try:
                body = response.text()
            except:
                pass

            # Get request info including POST body from route interception
            request_info = {}
            try:
                req = response.request
                request_info["method"] = req.method
                post_data = req.post_data
                # For exposure calls, match by order with intercepted POST bodies
                if not post_data and "exposure" in url and REQUEST_POST_BODIES:
                    # Count how many exposure responses we've seen so far
                    exposure_idx = sum(1 for r in CAPTURED_RESPONSES if "exposure" in r.get("url", ""))
                    if exposure_idx < len(REQUEST_POST_BODIES):
                        post_data = REQUEST_POST_BODIES[exposure_idx]
                request_info["post_data"] = post_data[:5000] if post_data else None
            except:
                pass

            CAPTURED_RESPONSES.append({
                "url": url,
                "status": response.status,
                "content_type": content_type,
                "body": body[:100000] if body else "",  # Cap at 100k chars
                "timestamp": datetime.now().isoformat(),
                "request_method": request_info.get("method"),
                "request_post_data": request_info.get("post_data"),
            })

            # Log interesting ones immediately
            interesting_keywords = ["exposure", "gamma", "vanna", "charm", "delta", "theta", "spot", "vol", "beta"]
            if any(k in url.lower() or k in body[:500].lower() for k in interesting_keywords):
                print(f"[capture] INTERESTING: {url[:100]}... ({len(body)} chars)")
                if request_info.get("post_data"):
                    print(f"[capture]   POST data: {request_info['post_data'][:200]}...")
    except Exception as e:
        pass  # Silently skip errors


def analyze_captures(widget_labels: list) -> dict:
    """Analyze captured responses and correlate with widget labels."""
    analysis = {
        "total_responses": len(CAPTURED_RESPONSES),
        "widget_labels": widget_labels,
        "exposure_endpoints": [],
        "exposure_with_greek": [],  # New: exposure data matched with greek type
        "vanna_endpoints": [],
        "charm_endpoints": [],
        "spot_vol_endpoints": [],
        "graphql_endpoints": [],
        "other_interesting": [],
    }

    # Build widget order from labels (top to bottom, left to right)
    sorted_widgets = sorted(widget_labels, key=lambda w: (w.get("position", {}).get("y", 0), w.get("position", {}).get("x", 0)))
    widget_order = [w.get("label", "Unknown") for w in sorted_widgets]
    analysis["widget_order"] = widget_order
    print(f"[analyze] Widget order: {widget_order}")

    keywords_exposure = ["exposure"]
    keywords_greek = ["vanna", "charm", "gamma", "delta", "theta"]
    keywords_spot = ["spot", "vol", "beta", "vix"]

    exposure_responses = []

    for item in CAPTURED_RESPONSES:
        url = (item.get("url") or "").lower()
        body = item.get("body") or ""
        combined = (url + "\n" + body[:1000]).lower()
        post_data = item.get("request_post_data") or ""

        info = {
            "url": item.get("url", "")[:300],
            "status": item.get("status"),
            "content_type": item.get("content_type", "")[:50],
            "body_length": len(body),
            "has_items": '"items"' in body,
            "has_currentPrice": '"currentprice"' in body.lower(),
            "timestamp": item.get("timestamp"),
            "request_method": item.get("request_method"),
            "request_post_data": post_data[:500] if post_data else None,
        }

        # Try to extract greek from POST data
        if post_data:
            try:
                post_json = json.loads(post_data)
                if "greek" in post_json:
                    info["greek_from_request"] = post_json["greek"]
                if "expirations" in post_json:
                    info["expirations_from_request"] = post_json["expirations"]
            except:
                pass

        # Try to extract data structure from response body
        if body:
            try:
                data = json.loads(body)
                if isinstance(data, dict):
                    info["top_keys"] = list(data.keys())[:15]
                    if "items" in data and isinstance(data["items"], list):
                        info["items_count"] = len(data["items"])
                        if data["items"]:
                            info["sample_item"] = str(data["items"][0])[:200]
                    if "currentPrice" in data:
                        info["current_price"] = data["currentPrice"]
                    if "expirations" in data:
                        info["expirations"] = data["expirations"]
                        info["expirations_count"] = len(data["expirations"]) if isinstance(data["expirations"], list) else 1
                    if "greek" in data:
                        info["greek"] = data["greek"]
                    if "expiration" in data:
                        info["expiration"] = data["expiration"]
            except:
                pass

        # Categorize
        if "graphql" in url:
            analysis["graphql_endpoints"].append(info)
        elif "exposure" in url:
            analysis["exposure_endpoints"].append(info)
            exposure_responses.append(info)
        elif any(k in combined for k in keywords_greek):
            if "vanna" in combined:
                analysis["vanna_endpoints"].append(info)
            elif "charm" in combined:
                analysis["charm_endpoints"].append(info)
            else:
                analysis["other_interesting"].append(info)
        elif any(k in combined for k in keywords_spot):
            analysis["spot_vol_endpoints"].append(info)
        elif len(body) > 500 and ('"items"' in body or '"data"' in body):
            analysis["other_interesting"].append(info)

    # Try to correlate exposure responses with widget labels
    # Sort exposure responses by timestamp
    exposure_responses.sort(key=lambda x: x.get("timestamp", ""))

    print(f"[analyze] Correlating {len(exposure_responses)} exposure responses with {len(widget_order)} widgets")

    for i, exp in enumerate(exposure_responses):
        exp_count = exp.get("expirations_count", 0)
        items_count = exp.get("items_count", 0)

        # Determine expiration type based on count
        if exp_count == 1:
            exp_type = "0DTE"
        elif exp_count <= 5:
            exp_type = "Weekly"
        elif exp_count <= 25:
            exp_type = "Monthly"
        else:
            exp_type = "All"

        # Try to match with widget
        greek = exp.get("greek_from_request") or "Unknown"
        if greek == "Unknown":
            # Try to infer from widget order and expiration type
            # Count how many 0DTE we've seen to determine if it's Charm or Vanna
            if exp_type == "0DTE":
                # First 0DTE is likely Charm, second is Vanna (based on workspace config)
                zero_dte_count = sum(1 for e in exposure_responses[:i] if e.get("expirations_count", 0) == 1)
                if zero_dte_count == 0:
                    greek = "Charm (inferred - 1st 0DTE)"
                else:
                    greek = "Vanna (inferred - 2nd+ 0DTE)"
            else:
                greek = "Vanna (inferred - non-0DTE)"

        analysis["exposure_with_greek"].append({
            "order": i + 1,
            "greek": greek,
            "expiration_type": exp_type,
            "expirations_count": exp_count,
            "items_count": items_count,
            "current_price": exp.get("current_price"),
            "timestamp": exp.get("timestamp"),
            "expirations": exp.get("expirations", [])[:5],  # First 5 expiration dates
        })

    return analysis


def run_test():
    """Main test function."""
    global CAPTURED_RESPONSES, WIDGET_LABELS
    CAPTURED_RESPONSES = []
    WIDGET_LABELS = []

    if not EMAIL or not PASSWORD:
        print("ERROR: Set VOLLAND_EMAIL and VOLLAND_PASSWORD environment variables")
        return
    if not TEST_URL:
        print("ERROR: Set VOLLAND_TEST_URL environment variable (your test workspace URL)")
        return

    print("=" * 60)
    print("VOLLAND TEST SCRAPER - WITH DOM WIDGET DETECTION")
    print("=" * 60)
    print(f"Email: {EMAIL}")
    print(f"Test URL: {TEST_URL}")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, channel="chrome", args=["--no-sandbox"])
        page = browser.new_page(viewport={"width": 1600, "height": 1000})
        page.set_default_timeout(90000)

        # Intercept exposure POST requests to capture the greek from the body
        def intercept_exposure(route, request):
            post = request.post_data
            if post:
                REQUEST_POST_BODIES.append(post)
                print(f"[intercept] Exposure POST: {post[:200]}")
            route.continue_()

        page.route("**/api/v1/data/exposure", intercept_exposure)
        page.on("response", on_response)
        print("[test] Network capture enabled (with route interception for POST bodies)")

        # Login
        login(page, TEST_URL)

        # Wait for page to fully load
        print("[test] Waiting for page to fully load (20 seconds)...")
        page.wait_for_timeout(20000)

        # ===== NEW: Extract widget labels from DOM =====
        print()
        print("[test] Extracting widget labels from DOM...")
        WIDGET_LABELS = extract_widget_labels(page)
        widget_details = extract_widget_details(page)

        print(f"[test] Found {len(WIDGET_LABELS)} widget labels")
        for w in WIDGET_LABELS:
            print(f"  - {w.get('label')} at position {w.get('position', 'unknown')}")

        print(f"[test] Captured {len(CAPTURED_RESPONSES)} responses so far")
        print()

        # Now try zooming to trigger additional data loads
        print("[test] Triggering zoom interactions on widgets...")

        # Widget positions (assuming 2 rows of 3)
        zoom_positions = [
            (300, 300),   # Widget 1 (0DTE Charm)
            (800, 300),   # Widget 2 (0DTE Vanna)
            (1300, 300),  # Widget 3 (Weekly Vanna)
            (300, 650),   # Widget 4 (Monthly Vanna)
            (800, 650),   # Widget 5 (All-Exp Vanna)
            (1300, 650),  # Widget 6 (Spot-Vol Beta)
        ]

        for i, (x, y) in enumerate(zoom_positions):
            print(f"[zoom] Widget {i+1}: zooming at ({x}, {y})")
            page.mouse.move(x, y)
            page.wait_for_timeout(500)
            # Zoom in
            page.mouse.wheel(0, -300)
            page.wait_for_timeout(1000)
            # Zoom out
            page.mouse.wheel(0, 300)
            page.wait_for_timeout(1000)

        print()
        print("[test] Zoom interactions complete. Waiting for additional data...")
        page.wait_for_timeout(5000)

        # Final count
        print(f"[test] Total captured responses: {len(CAPTURED_RESPONSES)}")

        # Save raw captures
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_file = OUTPUT_DIR / f"raw_captures_{timestamp}.json"
        with open(raw_file, "w") as f:
            json.dump(CAPTURED_RESPONSES, f, indent=2, default=str)
        print(f"[test] Saved raw captures to: {raw_file}")

        # Analyze with widget labels
        analysis = analyze_captures(WIDGET_LABELS + widget_details)
        analysis_file = OUTPUT_DIR / f"analysis_{timestamp}.json"
        with open(analysis_file, "w") as f:
            json.dump(analysis, f, indent=2, default=str)
        print(f"[test] Saved analysis to: {analysis_file}")

        # Print summary
        print()
        print("=" * 60)
        print("CAPTURE SUMMARY")
        print("=" * 60)
        print(f"Total responses captured: {analysis['total_responses']}")
        print()

        # Print widget order detected
        print("Widget Order Detected:")
        for i, label in enumerate(analysis.get('widget_order', [])):
            print(f"  {i+1}. {label}")
        print()

        # Print exposure data with greek identification
        print("Exposure Data with Greek Identification:")
        for exp in analysis.get('exposure_with_greek', []):
            print(f"  [{exp['order']}] {exp['greek']}")
            print(f"      Type: {exp['expiration_type']}, Items: {exp['items_count']}, Price: {exp.get('current_price', 'N/A')}")
            if exp.get('expirations'):
                print(f"      Dates: {exp['expirations']}")
        print()

        for category in ["exposure_endpoints", "vanna_endpoints", "charm_endpoints",
                         "spot_vol_endpoints", "graphql_endpoints", "other_interesting"]:
            items = analysis.get(category, [])
            if items:
                print(f"{category}: {len(items)} found")
                for i, ep in enumerate(items[:5]):
                    print(f"  [{i+1}] {ep['url'][:80]}...")
                    if ep.get('items_count'):
                        print(f"      Items: {ep['items_count']}, Price: {ep.get('current_price', 'N/A')}")
                    if ep.get('greek_from_request'):
                        print(f"      Greek (from request): {ep['greek_from_request']}")
                    if ep.get('expirations_count'):
                        print(f"      Expirations: {ep['expirations_count']} dates")
                    if ep.get('sample_item'):
                        print(f"      Sample: {ep['sample_item'][:100]}...")
                print()

        print("=" * 60)
        print("TEST COMPLETE")
        print("=" * 60)
        print(f"Check the files in: {OUTPUT_DIR}")
        print()

        # Keep browser open for manual inspection
        print("[test] Browser staying open for 30 seconds for manual inspection...")
        print("[test] You can manually interact with widgets if needed")
        print("[test] Close the browser window or press Ctrl+C to finish early")
        try:
            page.wait_for_timeout(30000)
        except KeyboardInterrupt:
            print("\n[test] Closing browser...")

        browser.close()


if __name__ == "__main__":
    run_test()
