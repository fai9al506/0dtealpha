"""Test if vol.land's REST API works without a browser at all.

If this works, we can replace volland_worker_v2.py with a 50-line
requests-only worker that's immune to headless detection.
"""
import os
import json
import requests

EMAIL = "faisal.a.d@msn.com"
PASSWORD = "Fad2024506!"

print("=== Step 1: Login via POST /api/v1/auth/login ===")
r = requests.post(
    "https://api.vol.land/api/v1/auth/login",
    json={"email": EMAIL, "password": PASSWORD},
    headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Origin": "https://vol.land",
        "Referer": "https://vol.land/",
        "Accept": "application/json",
        "Content-Type": "application/json",
    },
    timeout=15,
)
print(f"  Status: {r.status_code}")
if r.status_code != 200:
    print(f"  Body: {r.text[:300]}")
    raise SystemExit("Login failed — different endpoint or CF blocking direct API call")

# Look for JWT
data = r.json()
print(f"  Response keys: {list(data.keys())}")
token = data.get("token") or data.get("accessToken") or data.get("jwt")
if not token:
    # Some APIs put token in nested object
    for k, v in data.items():
        if isinstance(v, str) and v.startswith("eyJ"):
            token = v
            print(f"  Found JWT in key '{k}'")
            break
        if isinstance(v, dict):
            for k2, v2 in v.items():
                if isinstance(v2, str) and v2.startswith("eyJ"):
                    token = v2
                    print(f"  Found JWT in key '{k}.{k2}'")
                    break
if not token:
    print(f"  Full body: {r.text[:500]}")
    raise SystemExit("No JWT in response")
print(f"  JWT (first 50): {token[:50]}...")

# Common auth headers used by the browser
auth_headers = {
    "Authorization": f"Bearer {token}",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Origin": "https://vol.land",
    "Referer": "https://vol.land/",
    "Accept": "application/json",
}

print("\n=== Step 2: Fetch paradigm via GET /api/v1/data/paradigms/0dte?ticker=SPX ===")
r = requests.get(
    "https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPX",
    headers=auth_headers,
    timeout=15,
)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    para = r.json()
    print(f"  paradigm: {para.get('paradigm')}")
    print(f"  lis: {para.get('lis')}")
    print(f"  target: {para.get('target')}")
    print(f"  aggregatedCharm: {para.get('aggregatedCharm')}")
    print(f"  aggregatedDeltaDecay: {para.get('aggregatedDeltaDecay')}")
    print(f"  totalZeroDteOptionVolume: {para.get('totalZeroDteOptionVolume')}")
else:
    print(f"  Body: {r.text[:300]}")

print("\n=== Step 3: Fetch SPY paradigm ===")
r = requests.get(
    "https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPY",
    headers=auth_headers, timeout=15,
)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    para = r.json()
    print(f"  spy_paradigm: {para.get('paradigm')!r}")
    print(f"  spy_lis: {para.get('lis')}")
    print(f"  spy_aggregatedDeltaDecay: {para.get('aggregatedDeltaDecay')}")

print("\n=== Step 4: Fetch charm exposure via POST /api/v1/data/exposure ===")
r = requests.post(
    "https://api.vol.land/api/v1/data/exposure",
    headers=auth_headers,
    json={"greek": "charm", "expirations": {"option": "TODAY"}, "ticker": "SPX"},
    timeout=30,
)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    exp = r.json()
    items = exp.get("items", [])
    print(f"  items: {len(items)}")
    print(f"  currentPrice: {exp.get('currentPrice')}")
    print(f"  lastModified: {exp.get('lastModified')}")
    if items:
        print(f"  first item: {items[0]}")
        print(f"  last item: {items[-1]}")
else:
    print(f"  Body: {r.text[:500]}")

print("\n=== Step 5: spot-vol-beta ===")
r = requests.get(
    "https://api.vol.land/api/v1/data/volhacks/spot-vol-beta?ticker=SPX",
    headers=auth_headers, timeout=15,
)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    print(f"  body: {r.text[:200]}")

print("\n=== VERDICT ===")
print("If all 4 steps returned 200, we can replace the browser with pure HTTP.")
print("If any returned 401/403/blocked, vol.land detects direct API access too.")
