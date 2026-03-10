"""Generate ES range bar chart with all March 2 Absorption trades marked."""
import json
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch
from datetime import datetime, timedelta
import numpy as np

data = json.load(open('chart_data.json'))
bars = data['bars']
trades = data['trades']

print(f"Loaded {len(bars)} bars, {len(trades)} trades")

# Parse bar timestamps to ET
for b in bars:
    try:
        dt = datetime.fromisoformat(b['ts_start'])
        b['dt_et'] = dt - timedelta(hours=5)
        b['time_et'] = b['dt_et'].strftime('%H:%M')
    except:
        b['dt_et'] = None
        b['time_et'] = ''

# Filter to RTH only (9:30 - 16:00 ET)
rth_bars = [b for b in bars if b['dt_et'] and
            b['dt_et'].hour >= 9 and
            (b['dt_et'].hour < 16 or (b['dt_et'].hour == 16 and b['dt_et'].minute == 0))]
if not rth_bars:
    rth_bars = bars  # fallback

print(f"RTH bars: {len(rth_bars)}")

# Create figure with 3 subplots: Price, CVD, Volume
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(28, 16),
                                      gridspec_kw={'height_ratios': [4, 2, 1]},
                                      sharex=True)
fig.patch.set_facecolor('#1a1a2e')

for ax in [ax1, ax2, ax3]:
    ax.set_facecolor('#16213e')
    ax.tick_params(colors='#e0e0e0', labelsize=7)
    ax.grid(True, alpha=0.15, color='#555')
    for spine in ax.spines.values():
        spine.set_color('#333')

# X positions = sequential indices
x = list(range(len(rth_bars)))
bar_idx_to_x = {b['idx']: i for i, b in enumerate(rth_bars)}

# ---- PRICE PANEL (candlesticks) ----
for i, b in enumerate(rth_bars):
    color = '#26a69a' if b['close'] >= b['open'] else '#ef5350'  # green/red
    body_bottom = min(b['open'], b['close'])
    body_height = abs(b['close'] - b['open'])
    if body_height < 0.25:
        body_height = 0.25  # minimum visible body

    # Wick
    ax1.plot([i, i], [b['low'], b['high']], color=color, linewidth=0.7, alpha=0.8)
    # Body
    ax1.bar(i, body_height, bottom=body_bottom, width=0.6, color=color, alpha=0.85, edgecolor=color)

# ---- MARK TRADES ----
for t in trades:
    trig_idx = t.get('bar_idx')
    if trig_idx is None:
        continue
    xi = bar_idx_to_x.get(trig_idx)
    if xi is None:
        continue

    bar = rth_bars[xi] if xi < len(rth_bars) else None
    if not bar:
        continue

    es_price = t['es_price'] or bar['close']
    result = t['result'] or 'OPEN'
    direction = t['direction']
    pnl = t['pnl']
    tid = t['id']
    pattern = t['pattern']

    # Arrow direction and color
    if direction == 'bullish':
        arrow_y = bar['low'] - 3
        arrow_dy = 2
        marker = '^'
    else:
        arrow_y = bar['high'] + 3
        arrow_dy = -2
        marker = 'v'

    # Color by result
    if result == 'WIN':
        color = '#00e676'
        edge = '#00c853'
    elif result == 'LOSS':
        color = '#ff1744'
        edge = '#d50000'
    else:
        color = '#ffab00'
        edge = '#ff8f00'

    # Plot entry marker
    ax1.scatter(xi, arrow_y, marker=marker, s=120, color=color,
                edgecolors=edge, linewidths=1.5, zorder=10)

    # Label: #ID result pnl
    label = f"#{tid}\n{result}\n{pnl:+.0f}"
    label_y = arrow_y - 4 if direction == 'bullish' else arrow_y + 4
    va = 'top' if direction == 'bullish' else 'bottom'

    ax1.annotate(label, (xi, label_y), fontsize=5.5, fontweight='bold',
                 color=color, ha='center', va=va, zorder=11,
                 bbox=dict(boxstyle='round,pad=0.2', facecolor='#1a1a2e',
                          edgecolor=color, alpha=0.85, linewidth=0.5))

    # Mark swing pairs
    best = t.get('best_swing', {})
    ref_sw = best.get('ref_swing', {})
    rec_sw = best.get('swing', {})

    for sw, sw_color, sw_label in [(ref_sw, '#ff9800', 'Sw1'), (rec_sw, '#2196f3', 'Sw2')]:
        sw_bar_idx = sw.get('bar_idx')
        if sw_bar_idx is not None:
            sw_xi = bar_idx_to_x.get(sw_bar_idx)
            if sw_xi is not None:
                sw_type = sw.get('type', '')
                if sw_type == 'L' or (sw_type == 'Z' and direction == 'bullish'):
                    sw_y = rth_bars[sw_xi]['low'] - 1.5
                else:
                    sw_y = rth_bars[sw_xi]['high'] + 1.5

                ax1.scatter(sw_xi, sw_y, marker='D', s=30, color=sw_color,
                           edgecolors='white', linewidths=0.5, zorder=9, alpha=0.8)

    # Draw line between swing pair
    sw1_xi = bar_idx_to_x.get(ref_sw.get('bar_idx'))
    sw2_xi = bar_idx_to_x.get(rec_sw.get('bar_idx'))
    if sw1_xi is not None and sw2_xi is not None:
        sw1_cvd = ref_sw.get('cvd', 0)
        sw2_cvd = rec_sw.get('cvd', 0)
        sw1_price = ref_sw.get('price', 0)
        sw2_price = rec_sw.get('price', 0)
        if sw1_price and sw2_price:
            ax1.plot([sw1_xi, sw2_xi], [sw1_price, sw2_price],
                    color='#aaa', linewidth=0.7, linestyle='--', alpha=0.5, zorder=5)

ax1.set_ylabel('ES Price', color='#e0e0e0', fontsize=10)
ax1.set_title('ES 5-pt Range Bars — March 2, 2026 — ES Absorption Signals',
              color='#e0e0e0', fontsize=14, fontweight='bold', pad=10)

# ---- CVD PANEL ----
cvd_vals = [b['cvd'] for b in rth_bars]
cvd_colors = ['#26a69a' if b['delta'] >= 0 else '#ef5350' for b in rth_bars]
ax2.plot(x, cvd_vals, color='#42a5f5', linewidth=1.2, alpha=0.9)
ax2.fill_between(x, cvd_vals, alpha=0.15, color='#42a5f5')

# Mark CVD at swing points for each trade
for t in trades:
    best = t.get('best_swing', {})
    for sw, sw_color in [(best.get('ref_swing', {}), '#ff9800'), (best.get('swing', {}), '#2196f3')]:
        sw_bar_idx = sw.get('bar_idx')
        sw_cvd = sw.get('cvd')
        if sw_bar_idx is not None and sw_cvd is not None:
            sw_xi = bar_idx_to_x.get(sw_bar_idx)
            if sw_xi is not None:
                ax2.scatter(sw_xi, sw_cvd, marker='D', s=25, color=sw_color,
                           edgecolors='white', linewidths=0.5, zorder=9, alpha=0.8)

ax2.set_ylabel('CVD', color='#e0e0e0', fontsize=10)

# ---- VOLUME PANEL ----
volumes = [b['volume'] for b in rth_bars]
vol_colors = ['#26a69a' if b['delta'] >= 0 else '#ef5350' for b in rth_bars]
ax3.bar(x, volumes, color=vol_colors, alpha=0.6, width=0.8)

# Mark high-volume trigger bars
for t in trades:
    trig_idx = t.get('bar_idx')
    if trig_idx:
        xi = bar_idx_to_x.get(trig_idx)
        if xi is not None:
            ax3.bar(xi, rth_bars[xi]['volume'], color='#ffab00', alpha=0.9, width=0.8,
                   edgecolor='white', linewidth=0.5)

ax3.set_ylabel('Volume', color='#e0e0e0', fontsize=10)

# X-axis time labels (every 30 bars)
tick_positions = []
tick_labels = []
for i, b in enumerate(rth_bars):
    if i % 30 == 0:
        tick_positions.append(i)
        tick_labels.append(b['time_et'])
ax3.set_xticks(tick_positions)
ax3.set_xticklabels(tick_labels, rotation=45, fontsize=7, color='#e0e0e0')
ax3.set_xlabel('Time (ET)', color='#e0e0e0', fontsize=10)

# Legend
legend_elements = [
    mpatches.Patch(facecolor='#00e676', edgecolor='#00c853', label='WIN (bullish=▲, bearish=▼)'),
    mpatches.Patch(facecolor='#ff1744', edgecolor='#d50000', label='LOSS'),
    mpatches.Patch(facecolor='#ffab00', edgecolor='#ff8f00', label='EXPIRED'),
    plt.Line2D([0], [0], marker='D', color='#ff9800', label='Sw1 (ref swing)', markersize=6, linestyle='None'),
    plt.Line2D([0], [0], marker='D', color='#2196f3', label='Sw2 (recent swing)', markersize=6, linestyle='None'),
    mpatches.Patch(facecolor='#ffab00', edgecolor='white', label='Trigger bar (volume)', alpha=0.9),
]
ax1.legend(handles=legend_elements, loc='upper left', fontsize=7,
           facecolor='#1a1a2e', edgecolor='#555', labelcolor='#e0e0e0',
           ncol=3)

plt.tight_layout()
out = 'ES_Absorption_Chart_Mar2.png'
plt.savefig(out, dpi=180, bbox_inches='tight', facecolor=fig.get_facecolor())
print(f"Saved chart to {out}")
plt.close()
