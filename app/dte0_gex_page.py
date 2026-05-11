"""0DTE GEX dashboard page — visualizes the dte0_gex_scanner data."""

DTE0_GEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>0DTE GEX — SPX / SPY / QQQ / IWM</title>
<style>
  :root { --bg:#0b0d12; --card:#161a23; --border:#232938; --text:#e6eaf2;
          --muted:#8892a0; --green:#00d97e; --red:#ff5360; --gold:#fcd34d; }
  *{box-sizing:border-box}
  body{margin:0;padding:20px;background:var(--bg);color:var(--text);
       font-family:-apple-system,Segoe UI,sans-serif}
  h1{margin:0 0 8px;font-size:22px}
  .sub{color:var(--muted);font-size:13px;margin-bottom:16px}
  .row{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:14px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px}
  .card h2{margin:0 0 4px;font-size:18px;display:flex;justify-content:space-between;align-items:baseline}
  .spot{font-size:20px;color:var(--gold);font-weight:600}
  .meta{font-size:11px;color:var(--muted);margin-bottom:10px}
  table{width:100%;border-collapse:collapse;font-size:12px;margin-top:6px}
  th{text-align:left;color:var(--muted);font-weight:500;padding:4px 6px;border-bottom:1px solid var(--border)}
  td{padding:3px 6px;border-bottom:1px solid #1d222c}
  .pos{color:var(--green)} .neg{color:var(--red)}
  .strongest{font-weight:600;font-size:13px;margin:8px 0}
  .totals{display:flex;gap:14px;font-size:11px;color:var(--muted);margin-top:8px}
  .empty{color:var(--muted);font-style:italic}
  button{background:#1f2738;border:1px solid var(--border);color:var(--text);
         padding:6px 12px;border-radius:5px;cursor:pointer;font-size:12px}
  button:hover{background:#2a3344}
  .err{color:var(--red);background:#2a0e12;padding:8px;border-radius:5px;font-size:12px}
</style>
</head>
<body>
  <h1>0DTE GEX — SPX / SPY / QQQ / IWM</h1>
  <div class="sub">
    Snapshot every 30 min during market hours · scans data only · scanner status:
    <span id="status">loading...</span>
    &nbsp; · &nbsp;
    <button onclick="trigger()">Trigger scan now</button>
    <button onclick="load()">Refresh</button>
  </div>
  <div id="root" class="row"></div>

<script>
function fmt(n, p=0) {
  if (n === null || n === undefined) return '-';
  return Number(n).toLocaleString(undefined, {minimumFractionDigits: p, maximumFractionDigits: p});
}
function fmtGex(g) {
  if (g === null || g === undefined) return '-';
  const abs = Math.abs(g);
  if (abs > 1e9) return (g/1e9).toFixed(2) + 'B';
  if (abs > 1e6) return (g/1e6).toFixed(1) + 'M';
  if (abs > 1e3) return (g/1e3).toFixed(1) + 'K';
  return g.toFixed(0);
}
function cls(v) { return v > 0 ? 'pos' : v < 0 ? 'neg' : ''; }
function row(d, label) {
  if (!d) return `<tr><td colspan="2" class="empty">none</td></tr>`;
  const items = Array.isArray(d) ? d.slice(0, 6) : [d];
  if (items.length === 0) return `<tr><td colspan="2" class="empty">none</td></tr>`;
  return items.map(x => `<tr><td>${fmt(x.strike, 0)}</td><td class="${cls(x.gex)}">${fmtGex(x.gex)}</td></tr>`).join('');
}

async function load() {
  try {
    const [lvlR, stR] = await Promise.all([
      fetch('/api/dte0-gex/levels'),
      fetch('/api/dte0-gex/status'),
    ]);
    const lvl = await lvlR.json();
    const st = await stR.json();
    const lastTs = st?.last_scan?.ts || 'never';
    const lastMsg = st?.last_scan?.msg || '';
    document.getElementById('status').textContent = `${lastMsg} (${lastTs})`;

    const root = document.getElementById('root');
    const order = ['SPX','SPY','QQQ','IWM'];
    if (!lvl || lvl.error) {
      root.innerHTML = `<div class="err">${lvl?.error || 'no data'}</div>`;
      return;
    }
    root.innerHTML = order.map(sym => {
      const d = lvl[sym];
      if (!d) return `<div class="card"><h2>${sym}</h2><div class="empty">no scan yet</div></div>`;
      const sp = d.strongest_positive, sn = d.strongest_negative;
      return `
        <div class="card">
          <h2>${sym} <span class="spot">${fmt(d.spot, 2)}</span></h2>
          <div class="meta">exp ${d.expiration || '-'} · scanned ${d.scanned_at?.slice(0,19) || '-'}</div>

          ${sp ? `<div class="strongest pos">+GEX magnet: ${fmt(sp.strike,0)} (${fmtGex(sp.gex)})</div>` : ''}
          ${sn ? `<div class="strongest neg">-GEX support: ${fmt(sn.strike,0)} (${fmtGex(sn.gex)})</div>` : ''}

          <table>
            <tr><th>+GEX magnets above</th><th></th></tr>
            ${row(d.magnets_above)}
            <tr><th>+GEX magnets below</th><th></th></tr>
            ${row(d.magnets_below)}
            <tr><th>-GEX support (below)</th><th></th></tr>
            ${row(d.support)}
            <tr><th>-GEX resistance (above)</th><th></th></tr>
            ${row(d.resistance_above)}
          </table>

          <div class="totals">
            <span>GEX above: <span class="${cls(d.gex_above_spot)}">${fmtGex(d.gex_above_spot)}</span></span>
            <span>GEX below: <span class="${cls(d.gex_below_spot)}">${fmtGex(d.gex_below_spot)}</span></span>
          </div>
        </div>
      `;
    }).join('');
  } catch (e) {
    document.getElementById('root').innerHTML = `<div class="err">load failed: ${e.message}</div>`;
  }
}

async function trigger() {
  try {
    const r = await fetch('/api/dte0-gex/scan', {method: 'POST'});
    const j = await r.json();
    document.getElementById('status').textContent = j.status || j.error || 'triggered';
    setTimeout(load, 30000);
  } catch (e) {
    document.getElementById('status').textContent = 'trigger failed';
  }
}

load();
setInterval(load, 60000);
</script>
</body>
</html>"""
