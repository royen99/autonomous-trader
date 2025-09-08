/* globals Chart */
const fmt = new Intl.NumberFormat(undefined, { maximumFractionDigits: 6 });
const fmt2 = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });

const el = (id) => document.getElementById(id);
let ws = null;
let chart = null;
let currentSymbol = null;

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}

function upKpis(summary) {
  if (!summary) return;
  el('usdtBal').textContent = `USDT ${fmt2.format(summary.usdt || 0)}`;
  if (summary.delta_24h == null) {
    el('usdtDelta').textContent = '24h: —';
  } else {
    const d = summary.delta_24h;
    const sign = d > 0 ? '+' : '';
    el('usdtDelta').innerHTML = `24h: <span class="${d >= 0 ? 'text-green-400' : 'text-red-400'}">${sign}${fmt2.format(d)}</span>`;
  }
  if (summary.positions_value_usdt != null) {
    el('posValue').textContent = `USDT ${fmt2.format(summary.positions_value_usdt)}`;
  }
  el('openOrders').textContent = summary.open_orders ?? '0';
  el('lastTrade').textContent = summary.last_trade || '—';
}

function badgeSide(side) {
  const color = side === 'BUY' ? 'text-green-300' : 'text-red-300';
  return `<span class="${color}">${side}</span>`;
}
function badgeStatus(st) {
  const map = {
    NEW: 'bg-blue-900 text-blue-200',
    PARTIALLY_FILLED: 'bg-amber-900 text-amber-200',
    FILLED: 'bg-green-900 text-green-200',
    CANCELED: 'bg-neutral-800 text-neutral-200'
  };
  const cls = map[st] || 'bg-neutral-800 text-neutral-200';
  return `<span class="px-2 py-0.5 rounded text-xs ${cls}">${st}</span>`;
}

function renderOrders(rows) {
  const tb = el('ordersTbody');
  tb.innerHTML = (rows || []).map(r => `
    <tr class="border-b border-neutral-800/60">
      <td class="py-2">${new Date(r.created_at).toLocaleString()}</td>
      <td>${r.symbol}</td>
      <td>${badgeSide(r.side)}</td>
      <td class="text-right">${fmt.format(r.qty)}</td>
      <td class="text-right">${r.price != null ? fmt.format(r.price) : '—'}</td>
      <td>${badgeStatus(r.status)}</td>
    </tr>
  `).join('');
}

function renderTrades(rows) {
  const tb = el('tradesTbody');
  tb.innerHTML = (rows || []).map(r => `
    <tr class="border-b border-neutral-800/60">
      <td class="py-2">${new Date(r.ts).toLocaleString()}</td>
      <td>${r.symbol}</td>
      <td>${badgeSide(r.side)}</td>
      <td class="text-right">${fmt.format(r.qty)}</td>
      <td class="text-right">${fmt.format(r.price)}</td>
      <td class="text-right">${r.fee ? fmt.format(r.fee) : '—'} ${r.fee_asset || ''}</td>
    </tr>
  `).join('');
}

function renderPositions(rows) {
  const tb = el('positionsTbody');
  tb.innerHTML = (rows || []).map(r => {
    const pct = (r.upnl_pct || 0) * 100;
    const color = pct >= 0 ? 'text-green-400' : 'text-red-400';
    const sign = pct >= 0 ? '+' : '';
    return `
      <tr class="border-b border-neutral-800/60">
        <td class="py-2">${r.symbol}</td>
        <td class="text-right">${fmt.format(r.qty)}</td>
        <td class="text-right">${fmt.format(r.avg_entry)}</td>
        <td class="text-right ${color}">${sign}${fmt2.format(pct)}%</td>
      </tr>
    `;
  }).join('');
}

function initChart() {
  const canvas = el('kline');
  if (!canvas) { console.warn('kline <canvas id="kline"> not found'); return; }
  const ctx = canvas.getContext('2d');

  canvas.style.width = '100%';
  canvas.height = 512;

  if (chart) chart.destroy();
  chart = new Chart(ctx, {
    type: 'candlestick',
    data: { datasets: [{
      type: 'candlestick',
      label: 'Price',
      data: [],
      upColor: 'rgba(34,197,94,1)',
      downColor: 'rgba(239,68,68,1)',
      borderUpColor: 'rgba(34,197,94,1)',
      borderDownColor: 'rgba(239,68,68,1)',
      wickColor: 'rgba(229,231,235,0.7)'
    }]},
    options: {
      parsing: true,
      animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          enabled: true,
          callbacks: {
            title(items) {
              // show time from x
              const r = items[0]?.raw;
              return r?.x ? new Date(r.x).toLocaleString() : '';
            },
            label(ctx) {
              const r = ctx.raw || {};
              const o = r.o, h = r.h, l = r.l, c = r.c;
              // guard against undefined
              if (o == null || h == null || l == null || c == null) return '';
              return `O: ${fmt.format(o)}  H: ${fmt.format(h)}  L: ${fmt.format(l)}  C: ${fmt.format(c)}`;
            }
          }
        }
      },
      scales: {
        x: { type: 'time', time: { tooltipFormat: 'yyyy-MM-dd HH:mm' } },
        y: {
            type: 'linear',
            beginAtZero: false,
            ticks: {
                color: 'rgba(229,231,235,0.7)',
                callback: (v) => fmt.format(v)
            },
            grid: {
                display: true,
                color: 'rgba(255,255,255,0.06)',
                borderColor: 'rgba(255,255,255,0.12)',
                drawTicks: false,
                lineWidth: 1
            }
        }
      }
    }
  });
}

function yRangeFromCandles(candles) {
  if (!candles || !candles.length) return { min: 0, max: 1 };
  let min = Number.POSITIVE_INFINITY, max = Number.NEGATIVE_INFINITY;
  for (const k of candles) {
    if (k.l < min) min = k.l;
    if (k.h > max) max = k.h;
  }
  // pad a bit so candles don't touch the edges
  const pad = (max - min) * 0.08 || (min * 0.02);
  min = min - pad;
  max = max + pad;
  return { min, max };
}

function updateChart(symbol, candles) {
  el('chartTitle').textContent = symbol;
  if (!chart) initChart();
  chart.data.datasets[0].data = candles || [];

  // autoscale y to your data
  const { min, max } = yRangeFromCandles(candles);
  chart.options.scales.y.min = min;
  chart.options.scales.y.max = max;

  chart.update('none');
}

function wsConnect(symbol) {
  if (!symbol) return;
  currentSymbol = symbol;
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const url = `${proto}://${location.host}/ws`;
  ws = new WebSocket(url);

  ws.addEventListener('open', () => {
    el('wsDot').className = 'h-2.5 w-2.5 rounded-full bg-green-500';
    el('wsText').textContent = 'Live';
    ws.send(JSON.stringify({ symbol }));
  });

  ws.addEventListener('message', (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'tick') {
        upKpis(msg.summary);
        renderOrders(msg.orders);
        renderTrades(msg.trades);
        updateChart(msg.symbol, msg.candles);
        if (msg.positions) renderPositions(msg.positions);
      } else if (msg.type === 'error') {
        el('wsText').textContent = 'Error';
        el('wsDot').className = 'h-2.5 w-2.5 rounded-full bg-red-500';
      }
    } catch {}
  });

  ws.addEventListener('close', () => {
    el('wsDot').className = 'h-2.5 w-2.5 rounded-full bg-neutral-600';
    el('wsText').textContent = 'Reconnecting…';
    setTimeout(() => wsConnect(currentSymbol), 1500);
  });

  ws.addEventListener('error', () => {
    try { ws.close(); } catch {}
  });
}

async function boot() {
  initChart();
  const symbols = await fetchJSON('/api/symbols').catch(() => []);
  const sel = el('symbolSelect');
  sel.innerHTML = symbols.map(s => `<option value="${s}">${s}</option>`).join('') || `<option>BTCUSDT</option>`;
  const symbol = symbols[0] || 'BTCUSDT';

  // initial REST load (so UI fills instantly)
  const [summary, candles, orders, trades, positions] = await Promise.all([
    fetchJSON('/api/summary').catch(() => ({})),
    fetchJSON(`/api/candles?symbol=${encodeURIComponent(symbol)}&limit=180`).catch(() => []),
    fetchJSON('/api/orders?limit=20').catch(() => []),
    fetchJSON('/api/trades?limit=20').catch(() => []),
    fetchJSON('/api/positions').catch(() => []),
  ]);
  upKpis(summary);
  renderOrders(orders);
  renderTrades(trades);
  renderPositions(positions);
  updateChart(symbol, candles);

  wsConnect(symbol);

  sel.onchange = async (e) => {
    const sym = e.target.value;
    // refresh chart immediately
    const nc = await fetchJSON(`/api/candles?symbol=${encodeURIComponent(sym)}&limit=180`).catch(() => []);
    updateChart(sym, nc);
    // reconnect ws for new symbol
    try { ws && ws.close(); } catch {}
    wsConnect(sym);
  };
}

document.addEventListener('DOMContentLoaded', boot);
