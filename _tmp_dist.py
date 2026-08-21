import os, sys, statistics, importlib.util, io, contextlib
sys.argv=['x']
spec=importlib.util.spec_from_file_location("m","_tmp_sb_block_analysis.py")
m=importlib.util.module_from_spec(spec)
with contextlib.redirect_stdout(io.StringIO()): spec.loader.exec_module(m)

def dist(pol,cap,label):
    r=m.run(pol,cap,cap); d=r["daily"]; days=sorted(d)
    v=[d[x] for x in days]; tot=sum(v); n=len(v)
    wins=[x for x in v if x>0]; losses=[x for x in v if x<0]
    srt=sorted(v,reverse=True)
    # streaks
    cs=ms=0
    for x in v:
        cs = cs+1 if x<0 else 0
        ms=max(ms,cs)
    print(f"\n### {label}  [{pol} cap {cap}/{cap}]   {n} sessions, {r['n']} trades, WR {r['wr']:.0f}%")
    print(f"  TOTAL ${tot:,.0f}   MaxDD ${r['dd']:,.0f}   green days {len(wins)}/{n} ({len(wins)/n*100:.0f}%)")
    print(f"  avg win day ${statistics.mean(wins):,.0f}   avg loss day ${statistics.mean(losses):,.0f}"
          f"   median day ${statistics.median(v):,.0f}   worst day ${min(v):,.0f}   best ${max(v):,.0f}")
    print(f"  max consecutive losing days: {ms}")
    print(f"  top1 {srt[0]/tot*100:.0f}%  top3 {sum(srt[:3])/tot*100:.0f}%  top5 {sum(srt[:5])/tot*100:.0f}%"
          f"   ex-top3 ${tot-sum(srt[:3]):,.0f}   ex-top5 ${tot-sum(srt[:5]):,.0f}")
    # buckets
    b=[("< -200",lambda x:x<-200),("-200..-50",lambda x:-200<=x<-50),("-50..+50",lambda x:-50<=x<=50),
       ("+50..+200",lambda x:50<x<=200),("+200..+500",lambda x:200<x<=500),("> +500",lambda x:x>500)]
    print("  daily distribution: " + "  ".join(f"{nm}:{sum(1 for x in v if f(x))}" for nm,f in b))
    # equity curve, weekly
    import collections, datetime as dt
    wk=collections.OrderedDict()
    for x in days:
        mon=x-dt.timedelta(days=x.weekday()); wk[str(mon)]=wk.get(str(mon),0)+d[x]
    print("  weekly: " + " | ".join(f"{k[5:]} ${round(val):+,}" for k,val in wk.items()))
    cum=0; curve=[]
    for x in days: cum+=d[x]; curve.append(round(cum))
    print("  equity curve: " + " ".join(str(c) for c in curve))
    return r

W=os.environ.get('CF_START')
print(f"================ WINDOW {W} -> 2026-08-06 ================")
for pol,cap,lab in [("base012",2,"RECOMMENDED: V16 base, basket SIZING-only, GEX off"),
                    ("base012",3,"same, cap 3/3"),
                    ("sb012",3,"CURRENT LIVE (basket blocks), cap 3/3")]:
    dist(pol,cap,lab)
