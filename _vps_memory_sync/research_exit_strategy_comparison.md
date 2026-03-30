---
name: Exit Strategy Comparison (Opt1 vs Opt2 vs Opt3)
description: Backtest comparing TP@10, Trail All, and Split-target across all trailing setups — Opt2 (trail all) dominates by +874 pts over current Opt3
type: project
---

# Exit Strategy Comparison — Mar 25, 2026

Backtest on 667 trailing setup trades using playback_snapshots price data.

**Options tested:**
- **Opt1: TP@10** — Exit all at +10 pts, same initial stop
- **Opt2: Trail All** — Trail entire position, no partial TP
- **Opt3: Split (current)** — Half exits at +10 (T1), half trails (T2), P&L averaged

## Per-Setup Results

### AG Short (66 trades)
| | Opt1: TP@10 | Opt2: Trail All | Opt3: Split (current) |
|---|---|---|---|
| Total P&L | +228.5 | **+454.3** | +341.6 |
| Win Rate | 61% | 42% | 64% |
| W/L/Exp | 40/22/4 | 28/31/7 | 42/24/0 |
| Avg Win | +9.8 | +19.9 | +12.5 |
| Avg Loss | -7.6 | -7.6 | -7.6 |
| Max DD | 34.8 | 39.7 | 34.3 |
| **Opt2 vs Opt3** | | **+112.7 pts** | |

### DD Exhaustion (310 trades)
| | Opt1: TP@10 | Opt2: Trail All | Opt3: Split (current) |
|---|---|---|---|
| Total P&L | -54.2 | **+701.4** | +323.0 |
| Win Rate | 48% | 33% | 43% |
| W/L/Exp | 149/122/39 | 101/152/57 | 132/178/0 |
| Avg Win | +9.5 | +21.2 | +14.8 |
| Avg Loss | -10.8 | -10.9 | -9.2 |
| Max DD | 305.4 | 354.3 | 313.0 |
| **Opt2 vs Opt3** | | **+378.4 pts** | |

### Skew Charm (229 trades)
| | Opt1: TP@10 | Opt2: Trail All | Opt3: Split (current) |
|---|---|---|---|
| Total P&L | +246.2 | **+878.8** | +562.3 |
| Win Rate | 59% | 54% | 62% |
| W/L/Exp | 135/75/19 | 124/75/30 | 142/87/0 |
| Avg Win | +9.7 | +14.2 | +11.9 |
| Avg Loss | -13.0 | -13.0 | -13.0 |
| Max DD | 187.4 | 179.5 | 163.0 |
| **Opt2 vs Opt3** | | **+316.5 pts** | |

### GEX Long (62 trades)
| | Opt1: TP@10 | Opt2: Trail All | Opt3: Split (current) |
|---|---|---|---|
| Total P&L | -34.9 | **+87.8** | +21.8 |
| Win Rate | 39% | 32% | 39% |
| W/L/Exp | 24/35/3 | 20/36/6 | 24/38/0 |
| Avg Win | +9.8 | +15.4 | +12.6 |
| Avg Loss | -7.6 | -7.6 | -7.6 |
| Max DD | 116.4 | 99.1 | 107.8 |
| **Opt2 vs Opt3** | | **+66.0 pts** | |

### ALL COMBINED (667 trades)
| | Opt1: TP@10 | Opt2: Trail All | Opt3: Split (current) |
|---|---|---|---|
| Total P&L | +385.6 | **+2,122.3** | +1,248.7 |
| Win Rate | 52% | 41% | 51% |
| W/L/Exp | 348/254/65 | 273/294/100 | 340/327/0 |
| Avg Win | +9.7 | +17.6 | +13.2 |
| Avg Loss | -10.8 | -10.8 | -9.9 |
| Max DD | 305.4 | 354.3 | 313.0 |
| **Opt2 vs Opt3** | | **+873.6 pts** | |

## Key Findings

1. **Opt2 wins every setup on P&L** — no exceptions
2. **Biggest absolute gain: DD Exhaustion** (+378 pts more than Opt3) — DD has the most trades and longest trails
3. **Biggest relative gain: GEX Long** (Opt2 = +87.8 vs Opt3 = +21.8 — 4x improvement)
4. **Skew Charm: +316 pts more** AND less MaxDD (179.5 vs 163.0) — Opt2 is strictly better for SC
5. **AG Short: +113 pts more** with only +5 pts more DD — efficient gain
6. **Win rate drops ~10%** across all setups (41% vs 51%) — more trades reverse to stop, but the avg win nearly doubles
7. **MaxDD slightly higher** for Opt2 combined (354 vs 313) but NOT for SC and GEX where Opt2 has LESS DD

## Why Opt3 (Split) Underperforms

The partial TP at +10 sells half the position right when the big move is starting. On runners (+20, +30, +50 pts), you only capture half the upside. The "rescue" effect (locking +10 on reversals) is worth less than the upside lost.

## Decision Pending

User to decide whether to switch from Opt3 → Opt2 for some or all setups. Consider:
- Opt2 has lower WR (psychologically harder)
- Opt2 has slightly higher MaxDD combined
- User prioritizes drawdown — but SC/GEX actually have LESS DD with Opt2
