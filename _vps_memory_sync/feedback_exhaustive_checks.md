---
name: Exhaustive checks for completeness
description: When checking if all items are covered (e.g., all setup names in a dropdown), do a comprehensive scan — don't rely on partial searches
type: feedback
---

When verifying completeness (e.g., all setup names in a filter dropdown), grep for ALL unique values in the codebase rather than checking only the ones already known. First search missed SB Absorption because I only looked for setups I already knew about.

**Why:** User caught a missing setup (SB Absorption) that a thorough grep of `setup_name` across all files would have found immediately.

**How to apply:** For any "are all X covered?" question, do a comprehensive search for all instances of X in the codebase first, then compare against the list being checked.
