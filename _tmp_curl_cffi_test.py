"""Test if curl_cffi bypasses vol.land's bot detection from Railway IP.

curl_cffi impersonates real Chrome's TLS/HTTP2 fingerprint (JA3/JA4). If
vol.land's 409 was driven by TLS fingerprint detection, this gets past it.
If 409 returns because of NEW-IP device check, this won't help.

Test from inside Railway via `railway run --service Volland`.
"""
import sys, json

try:
    from curl_cffi import requests as cffi_requests
    print("[test] curl_cffi available")
except ImportError:
    print("[test] installing curl_cffi...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "curl_cffi"])
    from curl_cffi import requests as cffi_requests
    print("[test] installed")

# Test 1: vanilla login
print("\n[1] Login attempt with chrome124 impersonation")
r = cffi_requests.post(
    "https://api.vol.land/api/v1/auth/login",
    json={"email": "faisal.a.d@msn.com", "password": "Fad2024506!"},
    headers={
        "Origin": "https://vol.land",
        "Referer": "https://vol.land/",
        "Accept": "application/json",
        "Content-Type": "application/json",
    },
    impersonate="chrome124",
    timeout=30,
)
print(f"  Status: {r.status_code}")
print(f"  Body: {r.text[:400]}")

if r.status_code == 200:
    data = r.json()
    print(f"  Keys: {list(data.keys())}")
    # Find JWT
    jwt = None
    for k, v in data.items():
        if isinstance(v, str) and v.startswith("eyJ"):
            jwt = v
            print(f"  GOT JWT in '{k}'")
            break
        if isinstance(v, dict):
            for k2, v2 in v.items():
                if isinstance(v2, str) and v2.startswith("eyJ"):
                    jwt = v2
                    print(f"  GOT JWT in '{k}.{k2}'")
                    break
    if jwt:
        print(f"\n[2] Testing paradigm endpoint with new JWT")
        r = cffi_requests.get(
            "https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPX",
            headers={
                "Authorization": f"Bearer {jwt}",
                "Origin": "https://vol.land",
                "Referer": "https://vol.land/",
                "Accept": "application/json",
            },
            impersonate="chrome124",
            timeout=15,
        )
        print(f"  Status: {r.status_code}")
        if r.status_code == 200:
            d = r.json()
            print(f"  paradigm: {d.get('paradigm')}")
            print(f"  lis: {d.get('lis')}")
            print("\n=== SUCCESS — Railway can fully self-serve with curl_cffi ===")
        else:
            print(f"  Body: {r.text[:300]}")
