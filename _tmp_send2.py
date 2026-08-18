import os, requests
tok=os.environ["TELEGRAM_BOT_TOKEN"]
with open(r"C:/Users/Faisa/Downloads/basket_SB_CORRECTED.html","rb") as f:
    r=requests.post(f"https://api.telegram.org/bot{tok}/sendDocument",
        data={"chat_id":"-1003792574755","caption":"✅ CORRECTED Basket SB study (supersedes prior). Portal-verified. FILTER: open 0/0/1 = defensive (same $, 1/10 DD). SIZING: open 0/1/2 = 2.26× at tiny DD (the winner). Drop momentum. Gate works live (June -243→+95), capture healthy, 0 bypass."},
        files={"document":("basket_SB_CORRECTED.html",f,"text/html")},timeout=30)
    print("HTTP",r.status_code,r.json().get("ok"),"msg",r.json().get("result",{}).get("message_id"))
