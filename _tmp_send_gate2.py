import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN")
chat="-1003792574755"  # Researchs
if not token: print("NO TOKEN"); raise SystemExit(1)
path="daily_trade_logs/gate2_baseline_validation.html"
cap=("Gate-2 Validation: baseline P&L vs broker truth (daily).\n"
     "Per-lid +$1,166 vs tsrt_daily_stmt gross +$1,144 = 2% diff -> PASS. Confirms the "
     "2-factor sizing study is built on real broker money, not sim. Daily table + chart inside.")
with open(path,"rb") as f:
    r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
        data={"chat_id":chat,"caption":cap},
        files={"document":("gate2_baseline_validation.html", f, "text/html")}, timeout=60)
print("HTTP",r.status_code,"ok=",r.json().get("ok"),"desc=",r.json().get("description"))
