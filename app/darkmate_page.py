# -*- coding: utf-8 -*-
"""Dark Mate results dashboard — semi/gamma/2-factor sizing vs baseline vs real-TSRT,
on the live (V16) set. Live tracking + history. Read-only (does not touch trading)."""

DARKMATE_HTML = r"""<!doctype html><html><head><meta charset="utf-8">
<title>Dark Mate — Sizing Results</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body{background:#0e1117;color:#e6edf3;font-family:Inter,Segoe UI,Arial;max-width:1080px;margin:0 auto;padding:18px;line-height:1.5}
h1{font-size:21px} a{color:#58a6ff}
.bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:10px 0}
input,select,button{background:#161b22;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:6px 10px}
button{cursor:pointer} .cards{display:flex;gap:10px;flex-wrap:wrap;margin:12px 0}
.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:12px 16px;min-width:150px;flex:1}
.card .lbl{color:#8b949e;font-size:12px} .card .val{font-size:20px;font-weight:700;margin-top:3px}
table{width:100%;border-collapse:collapse;font-size:12.5px;margin:8px 0} td,th{border:1px solid #30363d;padding:5px 8px;text-align:right}
th{background:#1c2230;color:#8b949e} td:first-child,td:nth-child(2){text-align:left}
.win{color:#3fb950}.loss{color:#f85149}.mut{color:#8b949e;font-size:12px}
.sum{max-width:480px;font-size:13.5px} .sum td,.sum th{padding:7px 14px} .sum td:nth-child(2),.sum td:nth-child(3){text-align:right}
.tag{font-size:10px;padding:1px 5px;border-radius:4px;background:#1c2230;color:#8b949e}
</style></head><body>
<h1>⚛️ Dark Mate — Sizing Results</h1>
<div class="bar">
  <label>Date <input type="date" id="dt"></label>
  <label>Breaker $<input type="number" id="cap" value="300" step="25" style="width:72px"></label>
  <button onclick="load()">Load</button>
  <button onclick="loadHist()">History (20d)</button>
  <span id="status" class="mut"></span>
</div>
<div class="cards" id="cards"></div>
<div id="chart" style="height:340px"></div>
<h3>Trades</h3>
<div id="tbl"></div>
<script>
const F=x=>x==null?'-':(x>=0?'+':'')+Math.round(x);
const cls=x=>x==null?'mut':(x>=0?'win':'loss');
function card(l,v,c){return `<div class="card"><div class="lbl">${l}</div><div class="val ${c||''}">${v}</div></div>`;}
async function load(){
  const d=document.getElementById('dt').value;
  const cap=document.getElementById('cap').value||300;
  document.getElementById('status').textContent='loading...';
  const r=await fetch('/api/darkmate/results?cap='+cap+(d?('&date='+d):''),{cache:'no-store'});
  const j=await r.json();
  if(j.error){document.getElementById('status').textContent='err: '+j.error.slice(0,120);return;}
  document.getElementById('status').textContent=j.date+' · '+j.n+' V16 trades · breaker $'+cap;
  const t=j.totals;
  const Dol=v=>(v>=0?'+$':'-$')+Math.abs(Math.round(v));
  function srow(lab,v,isBase){
    const d=(v==null||isBase)?'—':Dol(v-t.base);
    const dc=(v==null||isBase)?'mut':cls(v-t.base);
    return `<tr><td>${lab}</td><td class="${cls(v)}">$${F(v)}</td><td class="${dc}"><b>${d}</b></td></tr>`;
  }
  document.getElementById('cards').innerHTML=
    '<table class="sum"><tr><th>Scheme</th><th>Total</th><th>Δ vs Baseline</th></tr>'+
    srow('Baseline (1×)',t.base,true)+
    srow('REAL TSRT',t.real)+
    srow('Semi',t.semi)+
    srow('Semi (capped)',t.semi_cap)+
    srow('Semi+Gamma',t.two)+
    srow('Semi+G (capped)',t.two_cap)+
    '</table>';
  // layout: trade | signals (why) | outcomes (what each made -> truth)
  let h='<table><tr><th>Time</th><th>Setup</th><th>Dir</th>'+
    '<th>Tech %</th><th>Semi×</th><th>Gamma-fav</th>'+
    '<th>Base$</th><th>Semi$</th><th>Semi+Gamma$</th><th>REAL$</th><th></th></tr>';
  j.trades.forEach(x=>{h+=`<tr><td>${x.time}</td><td>${x.setup}</td><td>${x.dir}</td>`+
    `<td>${x.basket==null?'-':(x.basket>=0?'+':'')+x.basket+'%'}</td>`+
    `<td>${x.semi_mult}×</td>`+
    `<td>${x.gamma_fav==null?'-':x.gamma_fav}</td>`+
    `<td class="${cls(x.base)}">${F(x.base)}</td>`+
    `<td class="${cls(x.semi)}">${F(x.semi)}</td>`+
    `<td class="${cls(x.two)}">${F(x.two)}</td>`+
    `<td class="${cls(x.real)}"><b>${x.real==null?'-':F(x.real)}</b></td>`+
    `<td>${x.placed?'<span class="tag">TSRT</span>':''}</td></tr>`;});
  h+='</table>'; document.getElementById('tbl').innerHTML=h;
  // delta vs baseline (above 0 = sizing helped, below = hurt)
  const labels=['REAL TSRT','Semi','Semi (cap)','Semi+Gamma','Semi+G (cap)'];
  const dv=[t.real,t.semi,t.semi_cap,t.two,t.two_cap].map(v=>v==null?null:v-t.base);
  Plotly.newPlot('chart',[{x:labels,y:dv,type:'bar',
     marker:{color:dv.map(v=>(v==null?'#30363d':(v>=0?'#3fb950':'#f85149')))},
     text:dv.map(v=>(v==null?'':Dol(v))),textposition:'outside',cliponaxis:false}],
    {paper_bgcolor:'#0e1117',plot_bgcolor:'#161b22',font:{color:'#e6edf3'},
     margin:{t:24,b:34,l:50,r:10},title:{text:j.date+' — Δ vs baseline (breaker $'+cap+') · above 0 = sizing helped',font:{size:13}},
     xaxis:{gridcolor:'#30363d'},yaxis:{title:'Δ $ vs baseline',gridcolor:'#30363d',zeroline:true,zerolinecolor:'#555'}},{responsive:true});
}
async function loadHist(){
  document.getElementById('status').textContent='loading history...';
  const r=await fetch('/api/darkmate/results-history?days=20',{cache:'no-store'});
  const j=await r.json();
  if(j.error){document.getElementById('status').textContent='err';return;}
  const d=j.days, x=d.map(r=>r.date.slice(5));
  const cum=(k)=>{let c=0;return d.map(r=>c+=(r[k]||0));};
  Plotly.newPlot('chart',[
    {x,y:cum('base'),name:'Baseline',line:{color:'#8b949e'}},
    {x,y:cum('semi'),name:'Semi',line:{color:'#58a6ff',width:3}},
    {x,y:cum('two'),name:'Semi+Gamma',line:{color:'#3fb950'}},
    {x,y:cum('real'),name:'Real TSRT',line:{color:'#d29922',dash:'dot'}}
  ],{paper_bgcolor:'#0e1117',plot_bgcolor:'#161b22',font:{color:'#e6edf3'},title:'Cumulative $ — last 20 V16 days',
     xaxis:{gridcolor:'#30363d'},yaxis:{gridcolor:'#30363d'},height:340},{responsive:true});
  document.getElementById('status').textContent=j.days.length+' days';
  let h='<table><tr><th>Day</th><th>Base$</th><th>Semi$</th><th>Semi+Gamma$</th><th>REAL$</th><th>n</th></tr>';
  d.slice().reverse().forEach(r=>{h+=`<tr><td>${r.date}</td><td class="${cls(r.base)}">${F(r.base)}</td>`+
    `<td class="${cls(r.semi)}">${F(r.semi)}</td><td class="${cls(r.two)}">${F(r.two)}</td>`+
    `<td class="${cls(r.real)}">${F(r.real)}</td><td>${r.n}</td></tr>`;});
  h+='</table>'; document.getElementById('tbl').innerHTML=h;
}
load();
</script></body></html>"""
