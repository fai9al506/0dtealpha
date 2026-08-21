import os, requests
token=os.environ.get("TELEGRAM_BOT_TOKEN"); chat="-1003792574755"
assert token
docs=[("0DTE_Projection_V21.html",
       "V21 PROJECTION — open this file in any browser, no login needed.\n\n"
       "Average month +$2,372 (SAR 8,895) · worst month +$530 · best +$4,906.\n"
       "Every month positive. Drawdown cut 43%. 250 fewer trades than V16 for $654/mo more.\n\n"
       "The assumption that matters: it assumes 21 sessions a month. We trade 6-11."),
      ("0DTE_Six_Months_Flat.html",
       "SIX MONTHS, FLAT — the story, as a file. Open it in any browser.\n\n"
       "February watching only, March first order, April 75 trades, May +$828, June -$1,088,\n"
       "July one single trading day, August restart at +$85/day.\n\n"
       "Equity curve plotted from the 37 real statement days: peak +$482 on 4 June,\n"
       "trough -$1,307 on 24 June, +$76 today against the $6,000 deposit.")]
for fn,cap in docs:
    with open(fn,"rb") as f:
        r=requests.post(f"https://api.telegram.org/bot{token}/sendDocument",
            data={"chat_id":chat,"caption":cap},
            files={"document":(fn,f,"text/html")},timeout=120)
    j=r.json(); print(fn,"HTTP",r.status_code,"ok=",j.get("ok"),j.get("description"))
