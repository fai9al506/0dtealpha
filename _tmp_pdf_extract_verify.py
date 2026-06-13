import pdfplumber
p=r"C:\Users\Faisa\.claude\projects\G--My-Drive-Python-MyProject-GitHub-0dtealpha\d57f42eb-4e43-460c-b9a1-5d92f4db0ced\tool-results\webfetch-1780534038346-nj32bj.pdf"
with pdfplumber.open(p) as pdf:
    full="".join((pg.extract_text() or "") for pg in pdf.pages)
full=full.encode("ascii","replace").decode("ascii")
print("TOTAL LEN", len(full))
print("TITLE AREA:", full[:400].replace("\n"," "))
for kw in ["0.55","0.85","quartile","exponential","tightening","below zero","standard deviation","variance"]:
    i=full.lower().find(kw.lower())
    print("\n=== KW:",kw,"@",i)
    if i>=0:
        print(full[max(0,i-350):i+350].replace("\n"," "))
