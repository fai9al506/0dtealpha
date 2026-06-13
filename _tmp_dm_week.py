import json, glob, os, re
DIR = r"C:\Users\Faisa\OneDrive\Desktop\DiscordChatExporter.win-x64\Output"
f = [x for x in glob.glob(os.path.join(DIR,"*.json")) if "6_8 ES_SPX" in x][0]
with open(f, encoding="utf-8") as fh: data = json.load(fh)
OUT = open("_tmp_dm_week.txt","w",encoding="utf-8")
def w(s): OUT.write(s+"\n")

msgs = data.get("messages", [])
w(f"Channel: {data.get('channel',{}).get('name')}")
w(f"Messages: {len(msgs)}\n")
n_img=0; n_att=0
for i,m in enumerate(msgs):
    ts = m.get("timestamp","")[:16].replace("T"," ")
    author = (m.get("author",{}) or {}).get("name","?")
    content = (m.get("content") or "").strip()
    w(f"\n===== MSG {i+1} [{ts}] {author} =====")
    if content: w(content)
    # attachments (images)
    for a in m.get("attachments", []):
        n_att+=1
        fn=a.get("fileName") or a.get("url","").split("/")[-1]
        if re.search(r"\.(png|jpg|jpeg|gif|webp)$", fn, re.I): n_img+=1
        w(f"  [ATTACHMENT] {fn}  ({a.get('fileSizeBytes','?')} bytes)  url={a.get('url','')[:120]}")
    # embeds
    for e in m.get("embeds", []):
        if e.get("title"): w(f"  [EMBED TITLE] {e['title']}")
        if e.get("description"): w(f"  [EMBED DESC] {e['description']}")
        for fl in e.get("fields", []):
            w(f"  [EMBED FIELD] {fl.get('name','')}: {fl.get('value','')}")
        if e.get("image",{}).get("url"): w(f"  [EMBED IMAGE] {e['image']['url'][:120]}")
w(f"\n\n==== totals: attachments={n_att}, images={n_img} ====")
OUT.close()
print(f"done. msgs={len(msgs)} attachments={n_att} images={n_img}")
