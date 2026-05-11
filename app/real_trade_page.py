"""Real-trade status page — surfaces TS margin detail (InitialMargin / DayTradeMargin)
so the day-rate-vs-overnight diagnosis is visible at a glance."""

REAL_TRADE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Real Trade — Margin & Status</title>
<style>
  :root { --bg:#0b0d12; --card:#161a23; --border:#232938; --text:#e6eaf2;
          --muted:#8892a0; --green:#00d97e; --red:#ff5360; --gold:#fcd34d; --blue:#60a5fa; }
  *{box-sizing:border-box}
  body{margin:0;padding:20px;background:var(--bg);color:var(--text);
       font-family:-apple-system,Segoe UI,sans-serif}
  h1{margin:0 0 8px;font-size:22px}
  .sub{color:var(--muted);font-size:13px;margin-bottom:16px}
  .row{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:14px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:14px}
  .card h2{margin:0 0 10px;font-size:17px;display:flex;justify-content:space-between;align-items:baseline}
  .label{color:var(--muted);font-size:12px;font-weight:500}
  .acct{font-size:14px;color:var(--blue)}
  table{width:100%;border-collapse:collapse;font-size:13px;margin-top:4px}
  td{padding:4px 8px;border-bottom:1px solid #1d222c}
  td:first-child{color:var(--muted);width:50%}
  td.val{text-align:right;font-variant-numeric:tabular-nums;font-weight:500}
  .pos{color:var(--green)} .neg{color:var(--red)} .warn{color:var(--gold)}
  .verdict{margin-top:10px;padding:8px;border-radius:5px;font-size:12px;line-height:1.4}
  .verdict.intraday{background:#0e2418;border:1px solid #16432a;color:var(--green)}
  .verdict.overnight{background:#2a0e12;border:1px solid #5a1620;color:var(--red)}
  .verdict.unknown{background:#1a1a24;border:1px solid #2a2a3a;color:var(--muted)}
  .pos-card{margin-top:10px;padding:8px;background:#0f1521;border-radius:5px;font-size:12px}
  .err{color:var(--red);background:#2a0e12;padding:8px;border-radius:5px;font-size:12px}
  button{background:#1f2738;border:1px solid var(--border);color:var(--text);
         padding:6px 12px;border-radius:5px;cursor:pointer;font-size:12px}
  button:hover{background:#2a3344}
</style>
</head>
<body>
  <h1>Real Trade — Status & Margin Diagnosis</h1>
  <div class="sub">
    Live TradeStation account state for 210VYX65 (longs) + 210VYX91 (shorts).
    <button onclick="load()">Refresh</button>
  </div>
  <div id="root" class="row">loading...</div>

<script>
const fmt = (v, p=2) => v == null ? '-' : Number(v).toLocaleString(undefined, {minimumFractionDigits: p, maximumFractionDigits: p});
const usd = (v, p=0) => v == null ? '-' : '$' + fmt(v, p);
const sign = v => v == null ? '' : v > 0 ? 'pos' : v < 0 ? 'neg' : '';

function marginVerdict(acct) {
  // MES intraday = $264.80, overnight short = $2,499 / long = $2,648
  const im = acct.initial_margin || 0;
  const dtm = acct.day_trade_margin || 0;
  if (im === 0 && dtm === 0 && !acct.position) return null; // flat, no useful read
  if (im === 0 && dtm > 0) {
    return {cls: 'intraday', msg:
      `✓ INTRADAY RATE applied — DayTradeMargin = ${usd(dtm)}, InitialMargin = $0. Day-rate ~$265/MES.`};
  }
  if (im > 0 && dtm === 0) {
    return {cls: 'overnight', msg:
      `✗ OVERNIGHT RATE applied — InitialMargin = ${usd(im)}, DayTradeMargin = $0. ` +
      `Should be ~$265/MES (intraday). TS sees position as needing overnight margin (~$2,499/MES short, $2,648/MES long).`};
  }
  if (im > 0 && dtm > 0) {
    return {cls: 'unknown', msg:
      `MIXED: InitialMargin = ${usd(im)} AND DayTradeMargin = ${usd(dtm)}. Investigate.`};
  }
  return null;
}

function renderAccount(a) {
  if (a.balance_error) {
    return `<div class="card"><h2>${a.account_id}</h2><div class="err">balance error: ${a.balance_error}</div></div>`;
  }
  const v = marginVerdict(a);
  const pos = a.position && typeof a.position === 'object' ? a.position : null;
  return `
    <div class="card">
      <h2><span class="acct">${a.account_id}</span> <span class="label">${a.position && a.position !== 'flat' ? 'OPEN' : 'flat'}</span></h2>

      <table>
        <tr><td>Cash Balance</td><td class="val">${usd(a.cash)}</td></tr>
        <tr><td>Equity</td><td class="val">${usd(a.equity)}</td></tr>
        <tr><td>Buying Power</td><td class="val">${usd(a.buying_power)}</td></tr>
        <tr><td>Today P&amp;L</td><td class="val ${sign(a.today_pnl)}">${usd(a.today_pnl)}</td></tr>
        <tr><td>Realized P&amp;L</td><td class="val ${sign(a.realized_pnl)}">${usd(a.realized_pnl)}</td></tr>
        <tr><td>Unrealized P&amp;L</td><td class="val ${sign(a.unrealized_pnl)}">${usd(a.unrealized_pnl)}</td></tr>
      </table>

      <table style="margin-top:12px;border-top:2px solid var(--border);padding-top:6px">
        <tr><td colspan="2" style="color:var(--gold);font-weight:600;padding-top:4px">Margin Detail</td></tr>
        <tr><td>Initial Margin</td><td class="val ${a.initial_margin > 1000 ? 'warn' : ''}">${usd(a.initial_margin)}</td></tr>
        <tr><td>Day Trade Margin</td><td class="val ${a.day_trade_margin > 0 ? 'pos' : ''}">${usd(a.day_trade_margin)}</td></tr>
        <tr><td>Maintenance Margin</td><td class="val">${usd(a.maintenance_margin)}</td></tr>
        <tr><td>Required Margin</td><td class="val">${usd(a.required_margin)}</td></tr>
        <tr><td>Day Trade Excess</td><td class="val">${usd(a.day_trade_excess)}</td></tr>
      </table>

      ${v ? `<div class="verdict ${v.cls}">${v.msg}</div>` : '<div class="verdict unknown">No open position — margin verdict requires a live trade.</div>'}

      ${pos ? `<div class="pos-card">
        <b>Open Position:</b> ${pos.symbol || pos.Symbol || ''} qty=${pos.quantity || pos.Quantity || ''}
        ${pos.long_short || pos.LongShort ? '(' + (pos.long_short || pos.LongShort) + ')' : ''}
      </div>` : ''}
    </div>
  `;
}

async function load() {
  try {
    const r = await fetch('/api/real-trade/status');
    const d = await r.json();
    if (d.error) {
      document.getElementById('root').innerHTML = `<div class="err">${d.error}</div>`;
      return;
    }
    const accounts = d.accounts || [];
    if (!accounts.length) {
      document.getElementById('root').innerHTML = `<div class="err">no accounts in status</div>`;
      return;
    }
    document.getElementById('root').innerHTML = accounts.map(renderAccount).join('');
  } catch (e) {
    document.getElementById('root').innerHTML = `<div class="err">load failed: ${e.message}</div>`;
  }
}

load();
setInterval(load, 30000);
</script>
</body>
</html>"""
