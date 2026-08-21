import os, requests, sys
tok=os.environ.get("TELEGRAM_BOT_TOKEN")
if not tok:
    print("NO_TOKEN"); sys.exit(2)
path=r"C:/Users/Faisa/Downloads/basket_momentum_report.html"
url=f"https://api.telegram.org/bot{tok}/sendDocument"
try:
    with open(path,"rb") as f:
        r=requests.post(url,data={"chat_id":"-1003792574755",
            "caption":"📊 Basket SB study — Momentum vs Open (all scalings, walk-forward). Conclusion: switch to 30-min momentum + 0/1/2; retire live 0/0/1; real edge ~1.1x capital-adj (not 1.4x)."},
            files={"document":("basket_momentum_report.html",f,"text/html")},timeout=30)
    print("HTTP",r.status_code, r.json().get("ok"), str(r.json())[:160])
except Exception as e:
    print("NETERR", repr(e)[:160])
