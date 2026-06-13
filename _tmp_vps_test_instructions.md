# VPS Volland JWT Test — 3 steps

## Step 1: Get a fresh JWT
1. Sign in to https://vol.land in your normal browser (the one at your home).
2. F12 → Network → reload the page.
3. Click any `api.vol.land/api/v1/data/...` request.
4. Copy the `authorization` header value (the part after "Bearer ").

## Step 2: SSH into your VPS

```bash
ssh <your-vps-user>@<your-vps-host>
```

## Step 3: Paste these 2 lines on the VPS (replace JWT_HERE with what you copied)

```bash
# Get VPS public IP
curl -s https://api.ipify.org && echo

# Test JWT against vol.land (replace JWT_HERE)
curl -s -o /dev/null -w "HTTP %{http_code}\n" \
  -H "Authorization: Bearer JWT_HERE" \
  -H "Origin: https://vol.land" \
  -H "Referer: https://vol.land/" \
  "https://api.vol.land/api/v1/data/paradigms/0dte?ticker=SPX"
```

## Step 4: Paste both outputs here

I'll tell you instantly:
- **HTTP 200** → VPS works, we deploy the HTTP worker there
- **HTTP 401** → VPS is also blocked (same as Railway), we need residential proxy
- **HTTP 409** → VPS got the device-verification challenge

**Important:** stay signed in on your browser during the test so the session stays alive. If you sign out before pasting the JWT, the JWT dies.
