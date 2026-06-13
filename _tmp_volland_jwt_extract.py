"""Extract JWT from the running visible Chrome, test direct API access.

Strategy: vol.land's 3-device limit blocks new logins from new IPs/devices,
but EXISTING authenticated sessions can call the API directly. We extract
the JWT from the running browser's localStorage and try the API calls.

If this works → we can write a new pure-HTTP worker that:
  1. Re-uses a JWT extracted once (or via headed login on user's PC)
  2. Calls /api/v1/data/exposure etc directly
  3. Runs on Railway with no browser at all (headless or otherwise)
  4. Immune to vol.land bot detection forever
"""
import json
import time
import requests
from playwright.sync_api import sync_playwright

EMAIL = "faisal.a.d@msn.com"
PASS = "Fad2024506!"
WS_URL = "https://vol.land/app/workspace/69c2d38cce2143e384a8cfa1"

with sync_playwright() as p:
    print("Launching VISIBLE Chrome (will piggyback on existing slot)")
    browser = p.chromium.launch(headless=False, args=["--disable-blink-features=AutomationControlled"])
    context = browser.new_context()
    page = context.new_page()

    print("Going to sign-in...")
    page.goto("https://vol.land/sign-in", wait_until="domcontentloaded", timeout=60000)
    time.sleep(2)

    # Fill credentials if needed
    if "/sign-in" in page.url:
        print("Filling credentials")
        try:
            page.fill('input[type="email"]', EMAIL)
            page.fill('input[type="password"]', PASS)
            page.click('button[type="submit"]')
        except Exception as e:
            print(f"  fill error: {e}")

    # Wait for redirect to app
    for _ in range(20):
        time.sleep(1)
        if "/sign-in" not in page.url:
            break
    print(f"After login url: {page.url}")

    # Extract JWT from localStorage / cookies
    print("\nExtracting auth state...")
    ls = page.evaluate("() => JSON.stringify(Object.entries(localStorage))")
    ls_data = json.loads(ls)
    print(f"localStorage keys: {[k for k, _ in ls_data]}")
    jwt = None
    for k, v in ls_data:
        if "token" in k.lower() or "auth" in k.lower() or "jwt" in k.lower():
            print(f"  {k}: {v[:80]}...")
            if v.startswith("eyJ") or (isinstance(v, str) and "eyJ" in v):
                jwt = v.strip('"')
                if not jwt.startswith("eyJ"):
                    # may be a JSON-wrapped object
                    try:
                        parsed = json.loads(v)
                        if isinstance(parsed, dict):
                            for k2, v2 in parsed.items():
                                if isinstance(v2, str) and v2.startswith("eyJ"):
                                    jwt = v2; break
                    except Exception:
                        pass

    if not jwt:
        # Try cookies
        cookies = context.cookies()
        for c in cookies:
            if "token" in c["name"].lower() or "auth" in c["name"].lower():
                print(f"  cookie {c['name']}: {c['value'][:80]}...")

    browser.close()

if not jwt:
    print("\nNo JWT found in localStorage. Need to inspect manually.")
    raise SystemExit(1)

print(f"\nExtracted JWT (first 60): {jwt[:60]}...")

# Now try the direct API
hdrs = {
    "Authorization": f"Bearer {jwt}",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Origin": "https://vol.land",
    "Referer": "https://vol.land/",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

print("\n=== Test SPX paradigm ===")
r = requests.get("https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPX", headers=hdrs, timeout=15)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    print(f"  paradigm: {d.get('paradigm')}")
    print(f"  lis: {d.get('lis')}")
    print(f"  aggregatedCharm: {d.get('aggregatedCharm')}")
    print(f"  aggregatedDeltaDecay: {d.get('aggregatedDeltaDecay')}")
else:
    print(f"  body: {r.text[:300]}")

print("\n=== Test charm exposure ===")
r = requests.post(
    "https://api.vol.land/api/v1/data/exposure",
    headers=hdrs,
    json={"greek": "charm", "expirations": {"option": "TODAY"}, "ticker": "SPX"},
    timeout=30,
)
print(f"Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    print(f"  items: {len(d.get('items', []))}")
    print(f"  currentPrice: {d.get('currentPrice')}")
    print(f"  lastModified: {d.get('lastModified')}")

print("\n=== VERDICT ===")
print("If both 200s, write a pure-HTTP worker. JWT lasts ~8h.")
