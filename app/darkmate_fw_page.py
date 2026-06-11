# -*- coding: utf-8 -*-
"""Dark Mate FW — live framework map: multi-expiry gamma+vanna cluster levels.
+G = barrier (support below / resist above), -G = accelerator. Live + history. Manual-trade aid."""

DARKMATE_FW_HTML = r"""<!doctype html><html><head><meta charset="utf-8">
<title>Dark Mate FW — Levels</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
body{background:#0e1117;color:#e6edf3;font-family:Inter,Segoe UI,Arial;max-width:1080px;margin:0 auto;padding:18px;line-height:1.5}
h1{font-size:21px} a{color:#58a6ff}
.bar{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:10px 0}
select,button,input{background:#161b22;color:#e6edf3;border:1px solid #30363d;border-radius:6px;padding:6px 10px}
button{cursor:pointer} .mut{color:#8b949e;font-size:12px}
.legend{font-size:12px;color:#8b949e;margin:6px 0}
.key{display:flex;gap:10px;flex-wrap:wrap;margin:8px 0}
.kb{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:8px 12px;font-size:13px}
table{width:100%;border-collapse:collapse;font-size:12px;margin:8px 0} td,th{border:1px solid #30363d;padding:4px 7px;text-align:right}
th{background:#1c2230;color:#8b949e}
</style></head><body>
<h1>⚛️ Dark Mate FW — Framework Map <span class="mut">(multi-expiry gamma/vanna · barriers & accelerators)</span></h1>
<div class="mut"><b>+Gamma = barrier</b> (support below spot / resistance above) · <b>−Gamma = accelerator</b> (price runs through). Vanna walls = where dealers defend. <b>Cluster ✦</b> = expiries agree (high-conviction). <a href="/darkmate">→ Sizing results</a></div>
<div class="bar">
  <label>Greek <select id="greek"><option value="gamma">Gamma</option><option value="vanna">Vanna</option></select></label>
  <label>Range <select id="rng"><option value="100">±100</option><option value="150" selected>±150</option><option value="200">±200</option><option value="250">±250</option></select></label>
  <label><input type="checkbox" id="live" checked> Live (auto-refresh 60s)</label>
  <label>History <input type="datetime-local" id="at"></label>
  <button onclick="load()">Load</button>
  <span id="status" class="mut"></span>
</div>
<div class="key" id="key"></div>
<div id="chart" style="height:460px"></div>
<div class="legend">Incremental slices (Volland buckets nest) summing to the true total (ALL): <span style="color:#3fb950">0DTE</span> + <span style="color:#58a6ff">Weekly</span> + <span style="color:#bc8cff">Monthly</span> + <span style="color:#8b949e">Far</span>. Above 0 = positive, below = negative (Gamma: +G barrier / −G accelerator). Gold dotted = spot. Color mix shows which expiry drives each level (e.g. a big 0DTE pocket offset by weekly).</div>
<div id="tbl"></div>
<script>
let timer=null;
async function load(){
  const g=document.getElementById('greek').value;
  const at=document.getElementById('at').value;
  const rng=document.getElementById('rng').value;
  document.getElementById('status').textContent='loading...';
  const u='/api/darkmate/levels?greek='+g+'&rng='+rng+(at?('&at='+encodeURIComponent(new Date(at).toISOString())):'');
  const r=await fetch(u,{cache:'no-store'}); const j=await r.json();
  if(j.error){document.getElementById('status').textContent='err: '+(j.error||'').slice(0,100);return;}
  const p=j.profile, spot=j.spot;
  // INCREMENTAL slices (buckets nest) that sum to the true total (ALL): 0DTE + +Weekly + +Monthly + +Far
  const EXP=[['dte0','0DTE','#3fb950'],['weekly','Weekly','#58a6ff'],['monthly','Monthly','#bc8cff'],['far','Far (>30d)','#8b949e']];
  const traces=EXP.map(([k,nm,c])=>({x:p.map(s=>s.strike),y:p.map(s=>s[k]),type:'bar',name:nm,marker:{color:c},
     hovertemplate:'%{x} · '+nm+': %{y}M<extra></extra>'}));
  Plotly.newPlot('chart',traces,
    {paper_bgcolor:'#0e1117',plot_bgcolor:'#161b22',font:{color:'#e6edf3'},barmode:'relative',hovermode:'x unified',
     title:`${g} by expiry · spot ${spot}${j.spot_live?' (live)':''} · levels @ ${j.snap_ts?j.snap_ts.slice(11,16)+'Z (~2min Volland)':''}`,
     xaxis:{title:'strike',gridcolor:'#30363d'},yaxis:{title:g+' ($M) — stacked by expiry',gridcolor:'#30363d'},
     legend:{orientation:'h',y:1.08},
     shapes:[{type:'line',x0:spot,x1:spot,yref:'paper',y0:0,y1:1,line:{color:'#d29922',width:2,dash:'dot'}}],height:480},{responsive:true});
  const k=j.key;
  document.getElementById('key').innerHTML=
    `<div class="kb">spot <b>${spot}</b></div>`+
    `<div class="kb">+G barrier above: <b>${k.barrier_above||'—'}</b></div>`+
    `<div class="kb">+G support below: <b>${k.barrier_below||'—'}</b></div>`+
    `<div class="kb">−G accelerator above: <b>${k.accel_above||'—'}</b></div>`;
  document.getElementById('status').textContent='spot '+spot+(j.spot_live?' (live 30s)':'')+' · '+p.length+' strikes · levels '+(j.snap_ts?j.snap_ts.slice(11,16)+'Z (Volland ~2min)':'');
  // cluster table (agree>=2)
  let h='<table><tr><th>Strike</th><th>Total$M (ALL)</th><th>0DTE</th><th>+Weekly</th><th>+Monthly</th><th>+Far</th><th>Driven by</th></tr>';
  p.filter(s=>Math.abs(s.total)>=15).sort((a,b)=>Math.abs(b.total)-Math.abs(a.total)).slice(0,14).forEach(s=>{
    const parts=[['0DTE',s.dte0],['Weekly',s.weekly],['Monthly',s.monthly],['Far',s.far]];
    const dom=parts.reduce((a,b)=>Math.abs(b[1])>Math.abs(a[1])?b:a);
    const denom=(Math.abs(s.dte0)+Math.abs(s.weekly)+Math.abs(s.monthly)+Math.abs(s.far))||1;
    const domShare=Math.round(100*Math.abs(dom[1])/denom);
    h+=`<tr><td>${s.strike}</td><td style="color:${s.total>=0?'#3fb950':'#f85149'}">${s.total}</td>`+
    `<td>${s.dte0}</td><td>${s.weekly}</td><td>${s.monthly}</td><td>${s.far}</td><td><b>${dom[0]}</b> ${domShare}%</td></tr>`;});
  h+='</table>'; document.getElementById('tbl').innerHTML=h;
}
function tick(){ if(document.getElementById('live').checked && !document.getElementById('at').value) load(); }
document.getElementById('greek').addEventListener('change',load);
document.getElementById('rng').addEventListener('change',load);
load(); timer=setInterval(tick,60000);
</script></body></html>"""
