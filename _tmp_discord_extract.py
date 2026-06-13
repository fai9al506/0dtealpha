import re, html as H, glob, sys, io
sys.stdout=io.TextIOWrapper(sys.stdout.buffer,encoding='utf-8',errors='replace')
pat = r"C:/Users/Faisa/OneDrive/Desktop/DiscordChatExporter.win-x64/Output/*central*1362818*after 2026-06-09*.html"
files = glob.glob(pat)
raw = open(files[0], encoding='utf-8').read()
print('file size', len(raw))
txt=re.sub(r'<style.*?</style>',' ',raw,flags=re.S)
txt=re.sub(r'<script.*?</script>',' ',txt,flags=re.S)
txt=re.sub(r'<[^>]+>','\n',txt)
txt=H.unescape(txt)
lines=[l.strip() for l in txt.splitlines() if l.strip()]
open('_tmp_discord_0610.txt','w',encoding='utf-8').write('\n'.join(lines))
print('nonempty lines:', len(lines))
