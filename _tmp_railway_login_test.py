"""Test if Railway's IP can complete vol.land API login.

If yes → Railway HTTP worker is fully viable.
If 409 device-challenge → Railway needs its own pre-verified session storage,
or we proxy through user IP.
"""
import os, json, requests, sys

r = requests.post(
    "https://api.vol.land/api/v1/auth/login",
    json={"email": "faisal.a.d@msn.com", "password": "Fad2024506!"},
    headers={
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
        "Origin": "https://vol.land",
        "Referer": "https://vol.land/",
        "Accept": "application/json",
        "Content-Type": "application/json",
    },
    timeout=30,
)
print(f"Status: {r.status_code}")
print(f"Headers: {dict(r.headers)}")
print(f"Body: {r.text[:1000]}")
if r.status_code == 200:
    try:
        data = r.json()
        for k, v in data.items():
            if isinstance(v, str) and v.startswith("eyJ"):
                print(f"\nGOT JWT in key '{k}': {v[:80]}...")
                break
            if isinstance(v, dict):
                for k2, v2 in v.items():
                    if isinstance(v2, str) and v2.startswith("eyJ"):
                        print(f"\nGOT JWT in key '{k}.{k2}': {v2[:80]}...")
                        break
    except Exception as e:
        print(f"parse err: {e}")
