"""Test the JWT against vol.land's API and validate HTTP worker is viable."""
import os, json, base64, requests
from datetime import datetime, timezone

JWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY2YWEzNTc2MDkwYmIyOWM5ODRiNzRjOCIsImVtYWlsIjoiZmFpc2FsLmEuZEBtc24uY29tIiwiZnVsbE5hbWUiOiJGYWlzYWwgQWwgRGVtYWlqaSIsInN1YnNjcmlwdGlvbnMiOlt7ImlkIjoicHJvZF9ObHhyNlkzZzRnUElYVCIsIm5hbWUiOiJWb2xsYW5kIEluc2lnaHQiLCJtb3N0UG9wdWxhciI6bnVsbCwiZGVzY3JpcHRpb24iOm51bGwsImljb24iOm51bGwsInByaWNlIjpudWxsLCJmZWF0dXJlcyI6bnVsbH1dLCJhcGlTdWJzY3JpcHRpb25zIjpbXSwic2Vzc2lvbklkIjoiNmEwZjJmYThhYTE2MGIzZGYyNzk3NDE5IiwiaWF0IjoxNzc5MzgwMTM2LCJuYmYiOjE3NzkzODAxMzYsImV4cCI6MTc3OTQwODkzNiwiYXVkIjpbImh0dHBzOi8vd3d3LnZvbC5sYW5kIl0sImlzcyI6InZvbGxhbmQtdWktYXV0aCJ9.MNHUnn3VttgV3fp1vFutmvjJywxRFpLER0FLRtmKXbs"

# Parse JWT expiry
body_b64 = JWT.split(".")[1]
body_b64 += "=" * (-len(body_b64) % 4)
body = json.loads(base64.urlsafe_b64decode(body_b64))
exp = datetime.fromtimestamp(body["exp"], tz=timezone.utc)
now = datetime.now(timezone.utc)
remaining = (exp - now).total_seconds() / 3600
print(f"JWT info:")
print(f"  email: {body['email']}")
print(f"  session: {body['sessionId']}")
print(f"  expires: {exp.isoformat()} ({remaining:.2f}h remaining)")

hdrs = {
    "Authorization": f"Bearer {JWT}",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
    "Origin": "https://vol.land",
    "Referer": "https://vol.land/",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Test 1: SPX paradigm
print("\n[1] GET /api/v1/data/paradigms/0dte?ticker=SPX")
r = requests.get("https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPX", headers=hdrs, timeout=15)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    print(f"  paradigm: {d.get('paradigm')}")
    print(f"  lis: {d.get('lis')}")
    print(f"  target: {d.get('target')}")
    print(f"  aggregatedCharm: {d.get('aggregatedCharm')}")
    print(f"  aggregatedDeltaDecay: {d.get('aggregatedDeltaDecay')}")
    print(f"  totalZeroDteOptionVolume: {d.get('totalZeroDteOptionVolume')}")
else:
    print(f"  body: {r.text[:300]}")

# Test 2: SPY paradigm
print("\n[2] GET /api/v1/data/paradigms/0dte?ticker=SPY")
r = requests.get("https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPY", headers=hdrs, timeout=15)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    print(f"  paradigm: {d.get('paradigm')!r}")
    print(f"  aggregatedDeltaDecay: {d.get('aggregatedDeltaDecay')}")

# Test 3: spot-vol-beta
print("\n[3] GET /api/v1/data/volhacks/spot-vol-beta?ticker=SPX")
r = requests.get("https://api.vol.land/api/v1/data/volhacks/spot-vol-beta?ticker=SPX", headers=hdrs, timeout=15)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    print(f"  correlation: {d.get('correlation')}")
    print(f"  body: {str(d)[:200]}")

# Test 4: charm exposure
print("\n[4] POST /api/v1/data/exposure (charm/TODAY)")
r = requests.post("https://api.vol.land/api/v1/data/exposure", headers=hdrs,
                  json={"greek": "charm", "expirations": {"option": "TODAY"}, "ticker": "SPX"}, timeout=20)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    print(f"  items: {len(d.get('items', []))}")
    print(f"  currentPrice: {d.get('currentPrice')}")
    print(f"  lastModified: {d.get('lastModified')}")
    if d.get('items'):
        print(f"  first item: {d['items'][0]}")
else:
    print(f"  body: {r.text[:300]}")

# Test 5: vanna ALL exposure
print("\n[5] POST /api/v1/data/exposure (vanna/ALL)")
r = requests.post("https://api.vol.land/api/v1/data/exposure", headers=hdrs,
                  json={"greek": "vanna", "expirations": {"option": "ALL"}, "ticker": "SPX"}, timeout=20)
print(f"  Status: {r.status_code}")
if r.status_code == 200:
    d = r.json()
    print(f"  items: {len(d.get('items', []))}")
else:
    print(f"  body: {r.text[:300]}")

print("\n=== VERDICT ===")
print("All 5 should be 200 with non-empty data → HTTP worker is GO.")
