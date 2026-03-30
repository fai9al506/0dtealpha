---
name: VIX Compression Setup (REPLACED)
description: REPLACED by VIX Divergence v2 on 2026-03-29. Old single-phase detector (VIX drops while SPX flat). Failed: 2 signals, 2 losses due to SVB/vanna gates and no Phase 1.
type: project
---

## VIX Compression — REPLACED by VIX Divergence (2026-03-29)

This setup has been replaced. See `project_vix_divergence.md` for the active implementation.

**Why it was replaced:**
- Only 2 live signals ever fired, both LOSSES (-20 pts each)
- SVB/vanna gates killed 9 of 11 valid signals
- Without gates: 73% WR, +56 pts (11 signals) — gates were overfit to 8 trades
- No Phase 1 detection (VIX suppression during SPX move) — fired on noise
- LONG only — missed the powerful SHORT direction (79% WR unfiltered)
