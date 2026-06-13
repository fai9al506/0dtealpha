"""Build GEX Long trade-log HTML for one-by-one review (find WR improvements).
Clean v3.2 signals. Per-trade card: TS-GEX + charm chart with entry/SL/TP(magnet)/
MFE/MAE markers, plus the discriminating features (verdict, align, paradigm, regime,
CORE flags, VIX, hour). Re-simulated with the NEW exit: SL14 / target=magnet / trail15/5.
Dark theme (Analysis #15 style)."""
import json
from collections import defaultdict
from datetime import datetime
from sqlalchemy import create_engine, text
from app.gex_long_v3 import _build_cache
DB="postgresql://postgres:JwLVqJOvxdzflxJsCZHrPzcdPUYrmVYY@nozomi.proxy.rlwy.net:55417/railway"
iS,iCOI,iCG,iPG,iPOI=10,1,3,17,19
SL,TACT,TGAP=14.0,15.0,5.0
engine=create_engine(DB)
overlay=_build_cache(engine)
v32=[lid for lid,o in overlay.items() if o.get('pass_v32') and o.get('result') is not None]

def Q(sql,p=None):
    with engine.begin() as cx: return list(cx.execute(text(sql),p or {}))

# day paths
DP=defaultdict(list)
for d,ts,sp in Q("""SELECT (ts AT TIME ZONE 'America/New_York')::date,ts,spot FROM chain_snapshots
  WHERE (ts AT TIME ZONE 'America/New_York')::date BETWEEN '2026-02-01' AND '2026-06-08'
  AND (ts AT TIME ZONE 'America/New_York')::time<'16:00' AND spot IS NOT NULL ORDER BY ts"""):
    DP[d].append((ts,float(sp)))

def sim(day,ts,entry,target):
    path=[sp for (t2,sp) in DP.get(day,[]) if t2>=ts]
    if not path: return None
    s=entry-SL; mfe=0; mae=0; ta=False; tstop=s; exitp=entry; reason='eod'
    for sp in path:
        mfe=max(mfe,sp-entry); mae=min(mae,sp-entry); stop=tstop if ta else s
        if sp<=stop: exitp=stop; reason='trail' if ta else 'stop'; break
        if target and sp>=target: exitp=target; reason='target'; break
        if not ta and mfe>=TACT: ta=True; tstop=entry+(mfe-TGAP)
        elif ta:
            nt=entry+(mfe-TGAP)
            if nt>tstop: tstop=nt
    else: exitp=path[-1]
    pnl=exitp-entry
    return ('WIN' if pnl>0 else ('LOSS' if pnl<0 else 'FLAT')),round(pnl,1),round(mfe,1),round(mae,1),reason

trades=[]
for lid in v32:
    r=Q("""SELECT id,ts,(ts AT TIME ZONE 'America/New_York') t, spot, grade, paradigm, vix,
        greek_alignment, max_plus_gex FROM setup_log WHERE id=:i""",{"i":lid})
    if not r: continue
    _,ts,t,spot,grade,para,vix,align,mpg=r[0]
    spot=float(spot); mpg=float(mpg) if mpg else None
    d=t.date()
    # chain nearest entry for TS GEX + magnet
    cr=Q("""SELECT rows FROM chain_snapshots WHERE ts BETWEEN :a AND :b
        ORDER BY abs(extract(epoch FROM (ts-:c))) LIMIT 1""",
        {"a":ts.replace(tzinfo=ts.tzinfo)-__import__('datetime').timedelta(seconds=90),
         "b":ts+__import__('datetime').timedelta(seconds=90),"c":ts})
    gexbars=[]
    if cr and cr[0][0]:
        rows=cr[0][0] if isinstance(cr[0][0],list) else json.loads(cr[0][0])
        for rr in rows:
            try: s=float(rr[iS])
            except: continue
            if abs(s-spot)>40: continue
            net=float(rr[iCG] or 0)*float(rr[iCOI] or 0)-float(rr[iPG] or 0)*float(rr[iPOI] or 0)
            gexbars.append((s,net/1e3))  # scale
    ga=[(s,v) for s,v in gexbars if s>spot and v>0]
    magnet=max(ga,key=lambda x:x[1])[0] if ga else (mpg or spot+15)
    gb=[(s,v) for s,v in gexbars if s<spot]
    support=min(gb,key=lambda x:x[1])[0] if gb else None  # most negative below
    # charm nearest
    chr_=Q("""SELECT strike,value FROM volland_exposure_points WHERE greek='charm'
        AND ts_utc=(SELECT ts_utc FROM volland_exposure_points WHERE greek='charm'
        AND ts_utc BETWEEN :a AND :b ORDER BY abs(extract(epoch FROM(ts_utc-:c))) LIMIT 1)
        AND strike BETWEEN :lo AND :hi ORDER BY strike""",
        {"a":ts-__import__('datetime').timedelta(minutes=6),"b":ts,"c":ts,"lo":spot-40,"hi":spot+40})
    charm=[(float(s),float(v)/1e6) for s,v in chr_]
    target=max(magnet, spot+5)
    res=sim(d,ts,spot,target)
    if not res: continue
    outcome,pnl,mfe,mae,reason=res
    o=overlay[lid]
    trades.append(dict(id=lid,ts=str(t)[:16],date=str(d),spot=spot,grade=grade,para=para or '?',
        vix=float(vix) if vix else None,align=align,verdict=o.get('verdict'),
        magnet=magnet,support=support,sl=spot-SL,target=target,
        outcome=outcome,pnl=pnl,mfe=mfe,mae=mae,reason=reason,
        gex=sorted(gexbars),charm=sorted(charm),hour=t.hour))
trades.sort(key=lambda x:x['ts'])

# ---- HTML ----
BG="#1a1a2e";PANEL="#16213e";CARD="#0f3460";GREEN="#00e676";RED="#ff5252";BLUE="#448aff"
GOLD="#ffd740";WHITE="#fff";LIGHT="#b0bec5";DIM="#607d8b"
n=len(trades); w=sum(1 for t in trades if t['outcome']=='WIN'); tot=sum(t['pnl'] for t in trades)

def card(t):
    oc=GREEN if t['outcome']=='WIN' else (RED if t['outcome']=='LOSS' else GOLD)
    pc=GREEN if t['pnl']>0 else RED
    gx=[s for s,_ in t['gex']]; gy=[v for _,v in t['gex']]; gcol=[GREEN if v>=0 else RED for v in gy]
    cx=[s for s,_ in t['charm']]; cy=[v for _,v in t['charm']]
    why=(f"+GEX magnet at <b>{int(t['magnet'])}</b> (target, {t['magnet']-t['spot']:+.0f}pt). "
         f"{'−GEX support at <b>'+str(int(t['support']))+'</b> below. ' if t['support'] else ''}"
         f"verdict <b>{t['verdict']}</b>, align <b>{t['align']:+d}</b>, {t['para']}. ")
    if t['outcome']=='WIN': why+=f"Rode to {t['reason']} (+{t['pnl']:.0f}, MFE +{t['mfe']:.0f})."
    else: why+=f"Stopped ({t['reason']}, {t['pnl']:+.0f}). MFE only +{t['mfe']:.0f}, MAE {t['mae']:+.0f} → magnet never pulled."
    cid=f"c{t['id']}"
    vixs=f"{t['vix']:.1f}" if t['vix'] else "n/a"
    return f"""
<div class="tc" data-lid="{t['id']}">
 <div class="th"><span class="id">#{t['id']}</span><span class="ts">{t['ts']} (hr {t['hour']})</span>
  <span class="gr">[{t['verdict']}/{t['grade']}]</span>
  <span style="color:{oc};font-weight:bold">{t['outcome']}</span>
  <span style="color:{pc};font-weight:bold">{t['pnl']:+.1f}pt · ${t['pnl']*5:+.0f}</span></div>
 <div class="tm"><span>spot <b>{t['spot']:.1f}</b></span>
  <span style="color:{GREEN}">TP/magnet <b>{t['magnet']:.0f}</b></span>
  <span style="color:{RED}">SL <b>{t['sl']:.1f}</b> (−14)</span>
  <span>MFE <b>{t['mfe']:+.1f}</b></span><span>MAE <b>{t['mae']:+.1f}</b></span>
  <span>align <b>{t['align']:+d}</b></span><span>{t['para']}</span>
  <span>VIX <b>{vixs}</b></span></div>
 <div class="tr"><span class="rl">WHY:</span> {why}</div>
 <div id="{cid}" class="ch"></div>
 <script>
 Plotly.newPlot('{cid}',[
  {{x:{json.dumps(gx)},y:{json.dumps(gy)},type:'bar',name:'TS GEX',marker:{{color:{json.dumps(gcol)}}},opacity:.8,
    hovertemplate:'strike %{{x}}<br>GEX %{{y:.0f}}k<extra></extra>'}},
  {{x:{json.dumps(cx)},y:{json.dumps(cy)},type:'bar',name:'Volland charm',yaxis:'y2',
    marker:{{color:'rgba(224,64,251,0.38)',line:{{color:'#e040fb',width:0.4}}}},
    hovertemplate:'strike %{{x}}<br>charm %{{y:.0f}}M<extra>charm</extra>'}}
 ],{{barmode:'overlay',paper_bgcolor:'{PANEL}',plot_bgcolor:'{PANEL}',
   font:{{color:'{LIGHT}',size:11}},height:300,
   title:{{text:'TS GEX (bars) + charm (line) at entry',font:{{color:'{GOLD}',size:12}}}},
   xaxis:{{title:'Strike',gridcolor:'#2a3a5a'}},
   yaxis:{{title:'TS GEX (k)',gridcolor:'#2a3a5a',zerolinecolor:'#fff'}},
   yaxis2:{{title:'charm (M$)',overlaying:'y',side:'right',showgrid:false}},
   margin:{{l:55,r:55,t:46,b:42}},showlegend:true,legend:{{x:.02,y:.98,bgcolor:'rgba(15,52,96,.6)'}},
   shapes:[
    {{type:'line',xref:'x',yref:'paper',x0:{t['spot']},x1:{t['spot']},y0:0,y1:1,line:{{color:'{BLUE}',width:2}}}},
    {{type:'line',xref:'x',yref:'paper',x0:{t['magnet']},x1:{t['magnet']},y0:0,y1:1,line:{{color:'{GREEN}',width:1.5,dash:'dash'}}}},
    {{type:'line',xref:'x',yref:'paper',x0:{t['sl']},x1:{t['sl']},y0:0,y1:1,line:{{color:'{RED}',width:1.5,dash:'dash'}}}},
    {{type:'line',xref:'x',yref:'paper',x0:{t['spot']+t['mfe']},x1:{t['spot']+t['mfe']},y0:0,y1:1,line:{{color:'{GREEN}',width:1,dash:'dot'}}}},
    {{type:'line',xref:'x',yref:'paper',x0:{t['spot']+t['mae']},x1:{t['spot']+t['mae']},y0:0,y1:1,line:{{color:'{RED}',width:1,dash:'dot'}}}}],
   annotations:[
    {{x:{t['spot']},y:1.02,xref:'x',yref:'paper',text:'ENTRY',showarrow:false,font:{{color:'{BLUE}',size:9}}}},
    {{x:{t['magnet']},y:.96,xref:'x',yref:'paper',text:'magnet/TP',showarrow:false,font:{{color:'{GREEN}',size:9}}}},
    {{x:{t['sl']},y:.92,xref:'x',yref:'paper',text:'SL',showarrow:false,font:{{color:'{RED}',size:9}}}}]
  }},{{displayModeBar:false,responsive:true}});
 </script>
 <textarea class="comment" data-lid="{t['id']}" placeholder="Your comment on this trade (saves automatically)..."></textarea>
</div>"""

cards="\n".join(card(t) for t in trades)
losses=[t for t in trades if t['outcome']=='LOSS']
HTML=f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>GEX Long v3.2 — Trade Log</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:{BG};color:{WHITE};font-family:Inter,Segoe UI,sans-serif;padding:24px;max-width:1300px;margin:0 auto}}
h1{{color:{GOLD};border-bottom:2px solid {GOLD};padding-bottom:8px;margin-bottom:8px}}
.sub{{color:{LIGHT};font-size:13px;margin-bottom:20px}}
.panel{{background:{PANEL};border:1px solid {DIM};border-radius:8px;padding:16px;margin-bottom:20px}}
.big{{font-size:26px;color:{GOLD};font-weight:bold}}
.tc{{background:{PANEL};border:1px solid {DIM};border-radius:8px;padding:12px 16px;margin-bottom:16px}}
.th{{display:flex;gap:14px;align-items:center;flex-wrap:wrap;padding-bottom:6px;border-bottom:1px solid {DIM}}}
.th .id{{color:{GOLD};font-weight:bold}}.th .ts{{color:{LIGHT};font-size:13px}}
.th .gr{{background:{CARD};padding:2px 8px;border-radius:3px;font-size:12px}}
.tm{{display:flex;gap:16px;font-size:12px;color:{LIGHT};margin-top:8px;flex-wrap:wrap}}.tm b{{color:{WHITE}}}
.tr{{font-size:13px;color:{WHITE};margin-top:8px;padding:8px 12px;background:rgba(255,215,64,.08);border-left:3px solid {GOLD};border-radius:3px}}
.tr .rl{{color:{GOLD};font-weight:bold;margin-right:6px}}.tr b{{color:{GOLD}}}
.ch{{margin-top:10px}}
.comment{{width:100%;margin-top:10px;min-height:48px;background:#0d1424;color:{WHITE};border:1px solid {DIM};
  border-radius:5px;padding:8px 10px;font-family:Inter,sans-serif;font-size:13px;resize:vertical}}
.comment:focus{{outline:none;border-color:{GOLD}}}
.comment.has-content{{border-color:{GREEN};background:#0d1f17}}
.toolbar{{position:sticky;top:0;z-index:50;background:{PANEL};border:1px solid {DIM};border-radius:8px;
  padding:10px 14px;margin-bottom:16px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}}
.toolbar button{{cursor:pointer;border:none;border-radius:5px;padding:7px 14px;font-weight:600;font-size:13px}}
.btn-export{{background:{GREEN};color:#000}}.btn-copy{{background:{BLUE};color:#000}}.btn-clear{{background:{RED};color:#000}}
table{{width:100%;border-collapse:collapse;margin:10px 0}}
th,td{{padding:7px 10px;text-align:left;border-bottom:1px solid {DIM};font-size:13px}}
th{{background:{CARD};color:{GOLD}}}td{{color:{LIGHT}}}
.g{{color:{GREEN}}}.r{{color:{RED}}}
</style></head><body>
<h1>GEX Long — TS-SELECTED Trade Log (the set live actually fires)</h1>
<div class="sub">0DTE Alpha · generated {datetime.now().strftime('%Y-%m-%d')} · {n} signals selected on <b>TS GEX</b> (chain_snapshots, = live source) · re-simulated SL −14 / target = magnet / trail +15/5 · <b>replaces the Volland-selected v3.2 review</b></div>
<div class="toolbar">
  <span style="color:{GOLD};font-weight:bold">Comments:</span>
  <button class="btn-export" onclick="exportComments()">Export TXT</button>
  <button class="btn-copy" onclick="copyComments()">Copy</button>
  <button class="btn-clear" onclick="clearComments()">Clear All</button>
  <span style="color:{LIGHT};font-size:12px">your notes save automatically in this browser</span>
</div>
<div class="panel">
 <span class="big">{n}</span> trades &nbsp;·&nbsp; <span class="big g">{w/n*100:.0f}%</span> WR &nbsp;·&nbsp;
 <span class="big g">{tot:+.1f}p</span> (${tot*5:+,.0f} @1MES) &nbsp;·&nbsp; {len(losses)} losses to study
 <p style="color:{LIGHT};margin-top:10px;font-size:13px">Look for what the <span class="r">LOSSES</span> share: align, paradigm, hour, VIX, gap-to-magnet, support-below present? Each chart shows TS GEX (bars, green=+ red=−), charm (gold line), and ENTRY/magnet/SL/MFE/MAE markers.</p>
</div>
<div class="panel"><h3 style="color:{GOLD};margin-bottom:8px">Losses at a glance</h3>
<table><thead><tr><th>#</th><th>date</th><th>hr</th><th>verdict</th><th>align</th><th>paradigm</th><th>VIX</th><th>magnet gap</th><th>support below?</th><th>MFE</th><th>pnl</th></tr></thead><tbody>
{"".join(f"<tr><td>#{t['id']}</td><td>{t['date']}</td><td>{t['hour']}</td><td>{t['verdict']}</td><td>{t['align']:+d}</td><td>{t['para']}</td><td>{(f'{t['vix']:.1f}' if t['vix'] else 'n/a')}</td><td>{t['magnet']-t['spot']:+.0f}</td><td>{'yes' if t['support'] else 'no'}</td><td>+{t['mfe']:.0f}</td><td class='r'>{t['pnl']:+.0f}</td></tr>" for t in losses)}
</tbody></table></div>
<h2 style="color:{GOLD};margin:20px 0 12px">All {n} signals (chronological)</h2>
{cards}
<p style="color:{DIM};font-size:11px;margin-top:30px;text-align:center">0DTE Alpha · GEX Long v3.2 trade log · for setup-config review</p>
<script>
const SK="gexlong-v32-comments";
function load(){{
  const s=JSON.parse(localStorage.getItem(SK)||"{{}}");
  document.querySelectorAll(".comment").forEach(ta=>{{
    const lid=ta.dataset.lid;
    if(s[lid]){{ta.value=s[lid];ta.classList.add("has-content");}}
    ta.addEventListener("input",()=>{{
      const a=JSON.parse(localStorage.getItem(SK)||"{{}}");
      if(ta.value.trim()){{a[lid]=ta.value;ta.classList.add("has-content");}}else{{delete a[lid];ta.classList.remove("has-content");}}
      localStorage.setItem(SK,JSON.stringify(a));
    }});
  }});
}}
function buildTxt(){{
  const s=JSON.parse(localStorage.getItem(SK)||"{{}}");
  let txt="GEX Long v3.2 — trade comments\\n"+"=".repeat(50)+"\\n\\n";
  document.querySelectorAll(".tc").forEach(c=>{{
    const lid=c.dataset.lid;const cm=s[lid];if(!cm||!cm.trim())return;
    const hdr=c.querySelector(".th").innerText.replace(/\\n+/g," ");
    txt+="["+hdr+"]\\n"+cm.trim()+"\\n\\n";
  }});
  return txt;
}}
function exportComments(){{
  const b=new Blob([buildTxt()],{{type:"text/plain"}});const a=document.createElement("a");
  a.href=URL.createObjectURL(b);a.download="gexlong-v32-comments.txt";a.click();
}}
function copyComments(){{navigator.clipboard.writeText(buildTxt()).then(()=>alert("Copied."));}}
function clearComments(){{
  if(!confirm("Clear ALL comments?"))return;
  localStorage.removeItem(SK);
  document.querySelectorAll(".comment").forEach(ta=>{{ta.value="";ta.classList.remove("has-content");}});
}}
load();
</script>
</body></html>"""
import os
os.makedirs("daily_trade_logs",exist_ok=True)
out="daily_trade_logs/gex_long_TS_review.html"
open(out,"w",encoding="utf-8").write(HTML)
print(f"Wrote {out} ({len(HTML):,} bytes) — {n} trades, {w} wins, {len(losses)} losses, {tot:+.1f}p")
