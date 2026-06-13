# -*- coding: utf-8 -*-
"""Interactive Jun 3 & 4 gamma explorer: time slider over per-strike 0DTE gamma,
with setups marked + their gamma-at-entry. Self-contained Plotly HTML."""
import os, json
from datetime import timedelta, time as dtime
from collections import defaultdict
from sqlalchemy import create_engine, text
C=create_engine(os.environ['DATABASE_URL']).connect().execution_options(isolation_level="AUTOCOMMIT")
basket=[(r[0],float(r[1])) for r in C.execute(text("SELECT et,basket_pct FROM semi_basket ORDER BY et")).fetchall()]
bd=defaultdict(list)
for et,v in basket: bd[et.date().isoformat()].append((et,v))
def semi_at(day,t):
    co=t-timedelta(minutes=20); a=bd.get(day,[]); p=[v for (x,v) in a if x<=co]; return p[-1] if p else None

DAYS=["2026-06-03","2026-06-04"]
data={}
for DAY in DAYS:
    # gamma snapshots near spot
    gr=C.execute(text("""SELECT (ts_utc AT TIME ZONE 'America/New_York') et, strike, value, current_price
      FROM volland_exposure_points WHERE greek='gamma' AND expiration_option='TODAY'
        AND (ts_utc AT TIME ZONE 'America/New_York')::date=DATE :d AND strike BETWEEN 7400 AND 7750 ORDER BY ts_utc"""),{"d":DAY}).fetchall()
    snaps=defaultdict(dict); spots={}
    for et,k,v,cp in gr:
        key=et.replace(second=0,microsecond=0); snaps[key][float(k)]=float(v)/1e6
        if cp: spots[key]=float(cp)
    times=sorted(snaps); allk=sorted({k for t in times for k in snaps[t]})
    frames=[]
    for t in times:
        frames.append({"t":t.strftime("%H:%M"),"spot":spots.get(t),
                       "g":[round(snaps[t].get(k,0),1) for k in allk]})
    # price
    sp=C.execute(text("""SELECT (ts AT TIME ZONE 'America/New_York') et, spot FROM chain_snapshots
      WHERE (ts AT TIME ZONE 'America/New_York')::date=DATE :d AND spot IS NOT NULL
        AND (ts AT TIME ZONE 'America/New_York')::time BETWEEN TIME '09:30' AND TIME '16:00' ORDER BY ts"""),{"d":DAY}).fetchall()
    price=[{"t":r[0].strftime("%H:%M"),"p":float(r[1])} for r in sp]
    # setups
    import json as _j
    rows=C.execute(text("""SELECT (sl.ts AT TIME ZONE 'America/New_York') et, sl.setup_name, sl.direction, rto.state, sl.spot
      FROM setup_log sl JOIN real_trade_orders rto ON rto.setup_log_id=sl.id
      WHERE (sl.ts AT TIME ZONE 'America/New_York')::date=DATE :d ORDER BY sl.ts"""),{"d":DAY}).fetchall()
    setups=[]
    for et,setup,direction,st,spot in rows:
        if not isinstance(st,dict):
            try: st=_j.loads(st)
            except: st={}
        en=st.get('fill_price'); ex=st.get('close_fill_price')
        if en is None or ex is None: continue
        sh=('bear' in (direction or ''))or(direction=='short')or(setup=='AG Short'); L=not sh
        pts=(en-ex) if sh else (ex-en); usd=pts*5
        etn=et.replace(tzinfo=None); prior=[t for t in times if t<=etn]
        g30=g60="-"
        if prior:
            tt=prior[-1]; s0=spots.get(tt) or float(spot)
            g30=round(sum(v for k,v in snaps[tt].items() if abs(k-s0)<=30),0)
            g60=round(sum(v for k,v in snaps[tt].items() if abs(k-s0)<=60),0)
        setups.append({"t":et.strftime("%H:%M"),"setup":setup,"dir":"L" if L else "S","pnl":round(usd),
                       "g30":g30,"g60":g60,"semi":round(semi_at(DAY,etn),2) if semi_at(DAY,etn) is not None else None})
    data[DAY]={"strikes":allk,"frames":frames,"price":price,"setups":setups,
               "open":price[0]["p"] if price else 0,"close":price[-1]["p"] if price else 0,
               "hi":max(p["p"] for p in price) if price else 0,"lo":min(p["p"] for p in price) if price else 0}

J=json.dumps(data)
html="""<!doctype html><html><head><meta charset="utf-8"><title>Jun 3/4 Gamma Explorer</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>body{background:#0e1117;color:#e6edf3;font-family:Inter,Segoe UI,Arial;max-width:1040px;margin:0 auto;padding:20px}
h2{color:#58a6ff} table{width:100%;border-collapse:collapse;font-size:12.5px;margin:6px 0}
td,th{border:1px solid #30363d;padding:5px 8px;text-align:right}th{background:#1c2230;color:#8b949e}td:first-child,td:nth-child(2){text-align:left}
.win{color:#3fb950}.loss{color:#f85149}.tag{font-size:11px;color:#8b949e}textarea{width:100%;background:#161b22;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:6px}
.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 16px;margin:10px 0}</style></head><body>
<h1>📉 Jun 3 vs Jun 4 — 0DTE Gamma Explorer</h1>
<div class="card">Slide the time bar to watch the per-strike 0DTE gamma profile evolve. Setups below show their gamma at entry. Jun 3 = longs LOST (neg gamma all day, down-drift); Jun 4 = longs WON (up-trend). Use this to refine the gamma rule and add comments.</div>
<div id="root"></div>
<script>
const DATA=__DATA__;
function render(day, el){
  const d=DATA[day]; const N=d.frames.length;
  const gdiv=document.createElement('div'); el.appendChild(gdiv);
  const traces=[{x:d.strikes,y:d.frames[0].g,type:'bar',marker:{color:d.frames[0].g.map(v=>v>=0?'#3fb950':'#f85149')},name:'gamma $M'}];
  const frames=d.frames.map((f,i)=>{const mn=Math.min(...f.g,0),mx=Math.max(...f.g,0);const pad=Math.max((mx-mn)*0.15,15);
    return {name:''+i,data:[{y:f.g,marker:{color:f.g.map(v=>v>=0?'#3fb950':'#f85149')}}],
     layout:{yaxis:{range:[mn-pad,mx+pad],title:'0DTE gamma ($M)',gridcolor:'#30363d'},
     shapes:[{type:'line',x0:f.spot,x1:f.spot,yref:'paper',y0:0,y1:1,line:{color:'#d29922',width:2,dash:'dot'}}],
     title:day+' — '+f.t+' ET   spot '+(f.spot?f.spot.toFixed(0):'?')}};});
  const steps=d.frames.map((f,i)=>({label:f.t,method:'animate',args:[[''+i],{mode:'immediate',frame:{duration:0,redraw:true},transition:{duration:0}}]}));
  const f0=d.frames[0],mn0=Math.min(...f0.g,0),mx0=Math.max(...f0.g,0),pad0=Math.max((mx0-mn0)*0.15,15);
  Plotly.newPlot(gdiv,traces,{paper_bgcolor:'#0e1117',plot_bgcolor:'#161b22',font:{color:'#e6edf3'},
     height:440,title:day+' — '+f0.t+' ET   spot '+(f0.spot?f0.spot.toFixed(0):'?'),
     xaxis:{title:'strike',gridcolor:'#30363d'},yaxis:{title:'0DTE gamma ($M)',gridcolor:'#30363d',range:[mn0-pad0,mx0+pad0]},
     shapes:[{type:'line',x0:f0.spot,x1:f0.spot,yref:'paper',y0:0,y1:1,line:{color:'#d29922',width:2,dash:'dot'}}],
     sliders:[{active:0,currentvalue:{prefix:'time: ',font:{color:'#d29922'}},steps:steps,pad:{t:30}}]},{responsive:true}).then(()=>Plotly.addFrames(gdiv,frames));
  // price chart with setup markers
  const pdiv=document.createElement('div'); el.appendChild(pdiv);
  const sx=d.setups.map(s=>s.t), sy=d.setups.map(s=>{const pp=d.price.find(p=>p.t===s.t);return pp?pp.p:null;});
  Plotly.newPlot(pdiv,[{x:d.price.map(p=>p.t),y:d.price.map(p=>p.p),type:'scatter',mode:'lines',line:{color:'#58a6ff'},name:'SPX'},
     {x:sx,y:sy,type:'scatter',mode:'markers+text',text:d.setups.map(s=>s.dir),textposition:'top center',
      marker:{size:10,color:d.setups.map(s=>s.pnl>=0?'#3fb950':'#f85149'),symbol:d.setups.map(s=>s.dir==='L'?'triangle-up':'triangle-down')},name:'setups'}],
    {paper_bgcolor:'#0e1117',plot_bgcolor:'#161b22',font:{color:'#e6edf3'},height:260,title:day+' price + setups',
     xaxis:{gridcolor:'#30363d'},yaxis:{title:'SPX',gridcolor:'#30363d'}},{responsive:true});
  // setups table
  let h='<table><tr><th>Time</th><th>Setup</th><th>Dir</th><th>P&L$</th><th>semi%</th><th>gamma±30M</th><th>gamma±60M</th></tr>';
  d.setups.forEach(s=>{h+=`<tr><td>${s.t}</td><td>${s.setup}</td><td>${s.dir}</td>`+
    `<td class="${s.pnl>=0?'win':'loss'}">${s.pnl>=0?'+':''}${s.pnl}</td><td>${s.semi===null?'-':s.semi}</td>`+
    `<td class="${s.g30>=0?'win':'loss'}">${s.g30}</td><td class="${s.g60>=0?'win':'loss'}">${s.g60}</td></tr>`;});
  h+='</table>'; const td=document.createElement('div'); td.innerHTML=h; el.appendChild(td);
  const note=document.createElement('div'); note.innerHTML='<div class="tag">Your comments:</div><textarea rows=3 placeholder="notes on '+day+'..."></textarea>'; el.appendChild(note);
}
['2026-06-03','2026-06-04'].forEach(day=>{const h=document.createElement('h2');h.textContent=day;document.getElementById('root').appendChild(h);
  const el=document.createElement('div');document.getElementById('root').appendChild(el);render(day,el);});
</script></body></html>"""
html=html.replace("__DATA__",J)
open("daily_trade_logs/gamma_explorer_jun34.html","w",encoding="utf-8").write(html)
print("wrote daily_trade_logs/gamma_explorer_jun34.html  (", len(html),"bytes )")
print("Jun3 setups:",len(data["2026-06-03"]["setups"]),"| Jun4 setups:",len(data["2026-06-04"]["setups"]))
