import glob, os, re, html as _html
DIR = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output"
weeks = ["4_13","4_20","4_27","5_4","5_11","5_18","5_25","6_1","6_8"]
files=[]
for w in weeks:
    g=[f for f in glob.glob(os.path.join(DIR,"*.html")) if f"{w} ES_SPX" in f]
    if g: files.append((w,g[0]))

def strip_html(h):
    h = re.sub(r"<head.*?</head>", "", h, flags=re.S|re.I)
    h = re.sub(r"<script.*?</script>", "", h, flags=re.S|re.I)
    h = re.sub(r"<style.*?</style>", "", h, flags=re.S|re.I)
    # keep image alt/filenames as markers
    h = re.sub(r"<img[^>]*alt=\"([^\"]*)\"[^>]*>", r" [IMG:\1] ", h, flags=re.I)
    h = re.sub(r"<[^>]+>", " ", h)
    h = _html.unescape(h)
    h = re.sub(r"[ \t]+", " ", h)
    h = re.sub(r"\n\s*\n\s*\n+", "\n\n", h)
    return h.strip()

for w,f in files:
    out = f"_tmp_dm_{w.replace('_','-')}.txt"
    with open(f, encoding="utf-8") as fh: raw=fh.read()
    txt = strip_html(raw)
    # collapse the DCE chrome: drop lines that are pure nav/boilerplate
    lines=[l.strip() for l in txt.split("\n")]
    lines=[l for l in lines if l and not re.fullmatch(r"[\s·•\-—|]+", l)]
    with open(out,"w",encoding="utf-8") as o: o.write("\n".join(lines))
    print(f"{w}: {len(txt)} chars -> {out}")
