import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN")
chat="-1003792574755"  # Researchs
if not token: print("NO TOKEN"); raise SystemExit(1)
path="daily_trade_logs/tsrt_2factor_sizing.html"
cap=("TSRT 2-Factor Sizing — AUDITED (no look-ahead, broker $). Daily breakdown.\n"
     "158 placed trades, May 18-Jun 10: baseline +$480 -> semi +$1,639 -> 2-factor +$1,936.\n"
     "Replaces the earlier look-ahead version. Cuts red-tech bleed days, presses green days. "
     "Daily table + chart + honest projection (~1.4-1.8x durable) inside.")
with open(path,"rb") as f:
    r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
        data={"chat_id":chat,"caption":cap},
        files={"document":("tsrt_2factor_sizing.html", f, "text/html")}, timeout=60)
print("HTTP",r.status_code,"ok=",r.json().get("ok"),"desc=",r.json().get("description"))
