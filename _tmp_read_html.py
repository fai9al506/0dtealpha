import re
t=open(r"C:/Users/Faisa/Downloads/semi_sizing_v16.html",encoding="utf-8").read()
t=re.sub(r'data:image[^"\')]+','[img]',t)
t=re.sub(r'<style.*?</style>','',t,flags=re.S)
t=re.sub(r'<script.*?</script>','',t,flags=re.S)
txt=re.sub(r'<[^>]+>','\n',t)
txt=re.sub(r'\n{2,}','\n',txt)
lines=[l.strip() for l in txt.split('\n') if l.strip() and l.strip()!='[img]']
open(r"_tmp_html_text.txt","w",encoding="utf-8").write("\n".join(lines))
print("wrote",len(lines),"lines")
