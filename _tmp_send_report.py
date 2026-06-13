import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN")
chat=os.environ.get("TELEGRAM_CHAT_ID_SETUPS","-1003886332593")
if not token:
    print("NO TOKEN"); raise SystemExit(1)
path="daily_trade_logs/dark_matter_framework_study.html"
caption=("Dark Matter Framework - Study, Worked Examples & Action Plan\n"
         "8 weekly plans (Apr 13 - Jun 8) studied + backtested vs our full history.\n"
         "Open in a browser. Key finding: regime decides which side pays - in high-vol "
         "regimes the short side ~4x. Plan + projection inside.")
try:
    with open(path,"rb") as f:
        r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id":chat,"caption":caption},
            files={"document":("dark_matter_framework_study.html", f, "text/html")},
            timeout=60)
    print("HTTP", r.status_code)
    j=r.json()
    print("ok=", j.get("ok"), "| desc=", j.get("description"))
except Exception as e:
    print("SEND FAILED:", type(e).__name__, str(e)[:200])
