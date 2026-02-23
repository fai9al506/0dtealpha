"""Generate TradeStation OAuth authorization URL with all scopes,
then exchange auth code for new refresh token."""
import os, sys, requests
from urllib.parse import urlencode

CID = os.getenv("TS_CLIENT_ID", "")
SECRET = os.getenv("TS_CLIENT_SECRET", "")

if not CID or not SECRET:
    print("Missing TS_CLIENT_ID or TS_CLIENT_SECRET env vars")
    exit(1)

# IMPORTANT: This must match exactly what's registered in your TS developer app.
# Check at https://developer.tradestation.com -> Your App -> Redirect URI
# Common values: "http://localhost" or your Railway URL
REDIRECT_URI = "http://localhost"

# Step 1: Generate authorization URL
scopes = "openid offline_access MarketData ReadAccount Trade OptionSpreads"
auth_url = f"https://signin.tradestation.com/authorize?" + urlencode({
    "response_type": "code",
    "client_id": CID,
    "redirect_uri": REDIRECT_URI,
    "scope": scopes,
    "audience": "https://api.tradestation.com",
})

print("=" * 60)
print("STEP 1: Open this URL in your browser and authorize:")
print("=" * 60)
print(auth_url)
print()
print("After authorizing, you'll be redirected to a URL like:")
print(f"  {REDIRECT_URI}?code=XXXXX")
print()
print("Copy the 'code' value from the URL.")
print()

# Step 2: Exchange code for tokens
if len(sys.argv) > 1:
    code = sys.argv[1]
    print(f"Exchanging code: {code[:20]}...")
    r = requests.post("https://signin.tradestation.com/oauth/token", data={
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CID,
        "client_secret": SECRET,
        "redirect_uri": REDIRECT_URI,
    }, timeout=15)

    if r.status_code == 200:
        tok = r.json()
        print("\nSUCCESS!")
        print(f"  access_token: {tok.get('access_token', '')[:50]}...")
        print(f"  refresh_token: {tok.get('refresh_token', '')[:50]}...")
        print(f"  scope: {tok.get('scope', '')}")
        print(f"  expires_in: {tok.get('expires_in')}")
        print(f"\nUpdate TS_REFRESH_TOKEN on Railway to:")
        print(tok.get('refresh_token', ''))
    else:
        print(f"\nERROR: {r.status_code}")
        print(r.text)
else:
    print("STEP 2: Run this script again with the code:")
    print(f"  railway run python _tmp_ts_auth.py YOUR_CODE_HERE")
