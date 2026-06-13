"""Single-shot test: does a fresh user-issued JWT work from Railway IP?"""
import os, json
from curl_cffi import requests as cffi_requests

JWT = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY2YWEzNTc2MDkwYmIyOWM5ODRiNzRjOCIsImVtYWlsIjoiZmFpc2FsLmEuZEBtc24uY29tIiwiZnVsbE5hbWUiOiJGYWlzYWwgQWwgRGVtYWlqaSIsInN1YnNjcmlwdGlvbnMiOlt7ImlkIjoicHJvZF9ObHhyNlkzZzRnUElYVCIsIm5hbWUiOiJWb2xsYW5kIEluc2lnaHQiLCJtb3N0UG9wdWxhciI6bnVsbCwiZGVzY3JpcHRpb24iOm51bGwsImljb24iOm51bGwsInByaWNlIjpudWxsLCJmZWF0dXJlcyI6bnVsbH1dLCJhcGlTdWJzY3JpcHRpb25zIjpbXSwic2Vzc2lvbklkIjoiNmEwZjJmMTAwMjA0NTQwNTNmYWQ2NWY0IiwiaWF0IjoxNzc5Mzc5OTg0LCJuYmYiOjE3NzkzNzk5ODQsImV4cCI6MTc3OTQwODc4NCwiYXVkIjpbImh0dHBzOi8vd3d3LnZvbC5sYW5kIl0sImlzcyI6InZvbGxhbmQtdWktYXV0aCJ9.Y4yCIltuCYmoMfAnRTA1bSWci775CxeDMIw3BmjBDrQ"

# What's our IP from Railway?
r = cffi_requests.get("https://api.ipify.org?format=json", impersonate="chrome124", timeout=10)
print(f"Railway IP: {r.text}")

# Try paradigm with user's JWT
print("\n[1] GET paradigms/0dte?ticker=SPX with user's JWT from Railway")
r = cffi_requests.get(
    "https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPX",
    headers={
        "Authorization": f"Bearer {JWT}",
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
    print(f"  aggregatedCharm: {d.get('aggregatedCharm')}")
    print(f"  aggregatedDeltaDecay: {d.get('aggregatedDeltaDecay')}")
    print("\n*** SUCCESS — JWT works from Railway! HTTP worker on Railway viable ***")
else:
    print(f"  Body: {r.text[:300]}")
    print("\n*** JWT IP-bound — Railway path blocked, falling back to VPS ***")
