"""
One-time TradeStation OAuth re-authorization script.
Gets a new refresh token with Trade scope for SIM auto-trading.

Usage:
  python _ts_reauth.py

Prerequisites:
  - Set TS_CLIENT_ID and TS_CLIENT_SECRET env vars (or edit below)
"""

import os, sys, webbrowser, json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, urlencode
import requests

# Config — edit these if env vars aren't set
CLIENT_ID = os.getenv("TS_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("TS_CLIENT_SECRET", "")
AUTH_DOMAIN = "https://signin.tradestation.com"
AUDIENCE = "https://api.tradestation.com"

# Default allowed redirect URIs per TS docs:
# http://localhost, http://localhost:80, http://localhost:3000,
# http://localhost:3001, http://localhost:8080, http://localhost:31022
PORT = 8080
REDIRECT_URI = "http://localhost:8080"

# Trade scope is included by default per TS docs
SCOPES = "openid profile MarketData ReadAccount Trade offline_access"


class CallbackHandler(BaseHTTPRequestHandler):
    """Handles the OAuth callback."""

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if "error" in params:
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            error = params["error"][0]
            desc = params.get("error_description", [""])[0]
            self.wfile.write(f"<h2>OAuth Error: {error}</h2><p>{desc}</p>".encode())
            print(f"\nERROR: {error} - {desc}")
            self.server._code = None
            return

        code = params.get("code", [None])[0]
        if not code:
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<h2>No code received</h2>")
            self.server._code = None
            return

        # Exchange code for tokens
        print(f"\nGot auth code: {code[:20]}...")
        print("Exchanging for tokens...")
        try:
            payload = {
                "grant_type": "authorization_code",
                "code": code,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "redirect_uri": REDIRECT_URI,
            }
            print(f"  POST {AUTH_DOMAIN}/oauth/token")
            print(f"  redirect_uri={REDIRECT_URI}")

            r = requests.post(
                f"{AUTH_DOMAIN}/oauth/token",
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=payload,
                timeout=15,
            )

            print(f"  Response: {r.status_code}")
            print(f"  Body: {r.text[:500]}")

            if r.status_code >= 400:
                self.send_response(400)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(f"<h2>Token exchange failed ({r.status_code})</h2><pre>{r.text[:500]}</pre>".encode())
                self.server._code = None
                return

            tok = r.json()
            refresh_token = tok.get("refresh_token", "")
            scopes = tok.get("scope", "")
            access_token = tok.get("access_token", "")

            # Quick test: try to list accounts to verify Trade scope
            test_msg = ""
            if access_token:
                try:
                    tr = requests.get(
                        f"{AUDIENCE}/v3/brokerage/accounts",
                        headers={"Authorization": f"Bearer {access_token}"},
                        timeout=10,
                    )
                    accounts = tr.json().get("Accounts", [])
                    sim_accounts = [a["AccountID"] for a in accounts if "SIM" in a.get("AccountID", "")]
                    test_msg = f"Accounts found: {len(accounts)} (SIM: {sim_accounts})"
                    print(f"  Account test: {test_msg}")
                except Exception as e:
                    test_msg = f"Account test failed: {e}"

            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            html = f"""<html><body style="font-family:monospace;background:#0d1117;color:#e6edf3;padding:40px">
            <h2 style="color:#22c55e">SUCCESS</h2>
            <p><b>Scopes:</b> {scopes}</p>
            <p><b>{test_msg}</b></p>
            <hr>
            <p>Copy the refresh token below and set it as <code>TS_REFRESH_TOKEN</code> on Railway:</p>
            <pre style="background:#161b22;padding:12px;border-radius:6px;user-select:all;overflow-x:auto">{refresh_token}</pre>
            <p style="color:#22c55e">You can close this window.</p>
            </body></html>"""
            self.wfile.write(html.encode())

            self.server._code = refresh_token
            self.server._scopes = scopes

        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(f"<h2>Error: {e}</h2>".encode())
            print(f"  Exception: {e}")
            self.server._code = None

    def log_message(self, format, *args):
        pass  # Suppress default logging


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: Set TS_CLIENT_ID and TS_CLIENT_SECRET environment variables first.")
        print("  Or edit CLIENT_ID and CLIENT_SECRET at the top of this script.")
        sys.exit(1)

    # Build authorize URL
    params = urlencode({
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "audience": AUDIENCE,
        "scope": SCOPES,
    })
    auth_url = f"{AUTH_DOMAIN}/authorize?{params}"

    print("=" * 60)
    print("TradeStation OAuth Re-Authorization")
    print("=" * 60)
    print(f"Client ID:    {CLIENT_ID[:8]}...")
    print(f"Redirect URI: {REDIRECT_URI}")
    print(f"Scopes:       {SCOPES}")
    print(f"\nOpening browser...\n")

    # Start local server to catch callback
    server = HTTPServer(("localhost", PORT), CallbackHandler)
    server._code = None
    server._scopes = ""

    # Open browser
    webbrowser.open(auth_url)
    print(f"If browser didn't open, go to:\n{auth_url}\n")
    print("Waiting for callback...")

    # Handle one request (the callback)
    server.handle_request()

    if server._code:
        print("\n" + "=" * 60)
        print("SUCCESS! Scopes:", server._scopes)
        print("=" * 60)
        print(f"\nNew refresh token:\n")
        print(server._code)
        print(f"\nSet this on Railway:")
        print(f'  railway variables set TS_REFRESH_TOKEN="{server._code}" -s 0dtealpha')
        print("\nOr paste it manually in Railway dashboard > 0dtealpha > Variables")
    else:
        print("\nFailed. See error above.")


if __name__ == "__main__":
    main()
