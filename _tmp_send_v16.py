import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN"); chat="-1003792574755"
if not token: raise SystemExit("NO TOKEN")
cap=("Semi-Sizing on the CORRECT V16 set (920 trades, live_pass=true).\n"
 "Baseline +$17,041 -> Semi-only +$23,569 (1.38x), positive EVERY month, top day 17%.\n"
 "DRAWDOWN: semi-only maxDD $415 vs baseline $775 -> sizing BOTH boosts return AND halves drawdown "
 "(best return/DD 57x). Gamma adds nothing (+$105 PnL) and does NOT cut DD (442 vs 415) -> dropped. "
 "Daily breakdown + underwater chart inside.")
f=open("daily_trade_logs/semi_sizing_v16.html","rb")
r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",data={"chat_id":chat,"caption":cap},files={"document":("semi_sizing_v16.html",f,"text/html")},timeout=90)
print("HTTP",r.status_code,r.json().get("ok"),r.json().get("description"))
