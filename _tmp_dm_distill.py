import re, glob, os
weeks=["4-13","4-20","4-27","5-4","5-11","5-18","5-25","6-1","6-8"]
OUT=open("_tmp_dm_distilled.txt","w",encoding="utf-8")
def w(s): OUT.write(s+"\n")

def grab(txt, patterns, span=320):
    """find first match of any pattern, return that line + following span chars."""
    for p in patterns:
        m=re.search(p, txt, re.I)
        if m:
            seg=txt[m.start():m.start()+span]
            seg=re.sub(r"\s+"," ",seg).strip()
            return seg
    return None

for wk in weeks:
    fn=f"_tmp_dm_{wk}.txt"
    if not os.path.exists(fn): continue
    txt=open(fn,encoding="utf-8").read()
    w("\n"+"="*90)
    w(f"WEEK {wk}")
    w("="*90)
    # Bias direction + confidence
    for label, pats in [
        ("TREND REGIME", [r"Trend Regime[:\*\s]+[^\n]{0,140}"]),
        ("BIAS", [r"\bDirection[:\*\s]+\**\s*(SHORT|LONG|NEUTRAL|SHORT/|LONG/|range|pin)[^\n]{0,80}",
                  r"Bias[:\*\s]+\**\s*(SHORT|LONG|NEUTRAL|range|pin|BEARISH|BULLISH)[^\n]{0,120}"]),
        ("CONFIDENCE", [r"Confidence[:\*\s]+\**\s*\w+"]),
        ("VOL REGIME", [r"Regime classification[:\*\s]+\**\s*\w+[^\n]{0,160}",
                        r"Vol Regime[^\n]{0,40}\n?[^\n]{0,200}",
                        r"Regime[:\*\s]+\**\s*(EXTREME|ELEVATED|NORMAL|COMPRESSED)[^\n]{0,160}"]),
        ("DEALER HEDGING", [r"(Total dealer hedging|dealer hedging now reads|Total[^\n]{0,20}hedging)[^\n]{0,200}",
                            r"-?\$[\d.]+B[^\n]{0,120}(amplif|hedg|vanna)"]),
        ("PARADIGM", [r"amplification regime[^\n]{0,160}", r"positive-gamma[^\n]{0,160}", r"## Paradigm[^\n]{0,260}"]),
    ]:
        g=grab(txt, pats)
        w(f"  {label:<16}: {g if g else '—'}")
    # Setups: directions + grades
    setups=re.findall(r"Setup [A-D][^\n]{0,170}", txt)
    seen=set(); sl=[]
    for s in setups:
        key=s[:8]
        if key in seen: continue
        seen.add(key)
        s=re.sub(r"\s+"," ",s).strip()
        sl.append(s)
    if sl:
        w("  SETUPS:")
        for s in sl[:6]: w(f"     - {s[:150]}")
    # Quick reference spot + ranges
    qr=grab(txt,[r"Spot[:\*\s]+SPX[^\n]{0,120}", r"Spot[:\*\s]+[\d,]+[^\n]{0,120}"])
    w(f"  SPOT/QR         : {qr if qr else '—'}")
    rng=grab(txt,[r"range expectation[:\*\s]+[^\n]{0,80}", r"Today.s range[^\n]{0,80}"])
    w(f"  RANGE EXP       : {rng if rng else '—'}")

OUT.close()
print("done -> _tmp_dm_distilled.txt")
