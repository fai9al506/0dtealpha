import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN")
chat="-1003792574755"  # 0DTE Alpha Researchs channel
if not token: print("NO TOKEN"); raise SystemExit(1)
path="daily_trade_logs/tsrt_2factor_regimes.html"
cap=("2-Factor Sizing across regimes (Mar-Jun 2026) — semi + gamma-for-longs.\n"
     "Portal P&L @1MES, 1511 trades, 71 days: Baseline +$16,291 -> 2-Factor +$30,505 (~1.9x).\n"
     "Improves EVERY month: Mar +$3.8k, Apr +$5.0k, May +$3.0k, Jun +$2.4k. "
     "Chart shows it works in both volatile (Mar/Jun) and grind (Apr/May) regimes. Open in a browser.")
with open(path,"rb") as f:
    r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
        data={"chat_id":chat,"caption":cap},
        files={"document":("tsrt_2factor_regimes.html", f, "text/html")}, timeout=60)
print("HTTP",r.status_code,"ok=",r.json().get("ok"),"desc=",r.json().get("description"))
