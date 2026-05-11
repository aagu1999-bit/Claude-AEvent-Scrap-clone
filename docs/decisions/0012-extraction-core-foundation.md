# 0012 — extraction_core.py foundation

**Status:** Foundation landed; migration PRs to follow
**Date:** 2026-05-10
**Context:** Closes the divergence between `main.py` and `events_from_ids.py`.

## Problem

Over the course of today, `events_from_ids.py` grew a substantial safety net:
- 4-tier extraction ladder (caption / image / OCR-text / Pro)
- 13 sanity check flags
- Canonical NJ municipalities lookup (region auto-fix)
- Canonical venue lookup
- QUALITY_FLAGS column with per-cell yellow formatting
- RECURRENCE_PATTERN column for structured recurring queries
- Apify dataset caching
- Parallelism with thread-safe stats

Meanwhile `main.py` (the production weekly cron) kept running with the
old extraction logic. Every weekly run creates new rows WITHOUT region
auto-fix, sanity flags, or any of the quality work landed today.

**Net effect:** events_from_ids.py is cleaning up historical mess that
main.py keeps re-creating. Not sustainable.

## Decision

Create `extraction_core.py` as a shared library. Both tools eventually
import from it. Single source of truth for:
- Sanity check functions
- Region / venue lookups
- Tier ladder constants
- Cost estimates
- Flag-to-column mapping
- Light-yellow color constant

**This PR lands the foundation only.** The module is fully implemented
and unit-callable, but neither `main.py` nor `events_from_ids.py` yet
imports from it. Migrating each consumer is staged in follow-up PRs to
keep each change focused and reviewable.

## Migration sequence (planned)

| PR | Scope |
|----|-------|
| #14 (this) | Create `extraction_core.py`. Smoke-tested via direct imports. Nothing else changes. |
| Future | Migrate `events_from_ids.py` to import from `extraction_core`. Pure import-swap. Easier — the code already matches the canonical shape. |
| Future | Migrate `main.py` to import from `extraction_core`. Bigger lift — main.py has its own ways of doing things and needs careful incremental migration. |
| Future | Add the tier ladder + QUALITY_FLAGS + RECURRENCE_PATTERN to `main.py`. Production cron starts producing safety-net'd data. |

## Why land the foundation now (vs. all at once)

Three reasons:

1. **Risk isolation.** Each migration step is reviewable on its own.
   Mistakes in the events_from_ids migration don't risk main.py's cron.

2. **Iteration.** As we migrate, we may discover the canonical shape
   needs adjustment (different default behavior, missing helper, etc.).
   Landing the module first lets us iterate without breaking either
   consumer.

3. **Documentation.** The module's existence in the repo signals "this
   is the canonical implementation; future contributors edit here first."

## What's NOT in the foundation

- The `process_one_post` orchestration logic (tier ladder, escalation
  rules) — stays in `events_from_ids.py` for now. It's intertwined
  with that tool's threading + sheet-write logic. Will migrate later
  if both tools end up wanting the same orchestration.

- The OCR + Gemini call helpers (`ocr_post`, `call_gemini`,
  `extract_tier_*`) — these have side effects (API calls, stats
  mutation) and depend on the per-tool `ctx` dict shape. Landing them
  in the shared module requires harmonizing the ctx convention first.

- The prompt itself (`build_prompt`) — could move to extraction_core,
  but the prompt is currently the most actively-iterated piece. Easier
  to keep it where the most-iterated tool can reach it directly until
  the prompt stabilizes.

## Open questions

- **Should `audit_regions.py` and `dedup_all_events.py` also import from
  `extraction_core`?** They currently inline their own lookup-loading
  logic. Probably yes in a future cleanup, but very low priority — those
  are one-off scripts.

- **Should main.py's existing analytics tools (`recurring_accounts.py`,
  `account_hygiene.py`, `audit.py`) be affected?** No — those don't do
  extraction. They're orthogonal.

## References

- ADR 0010 — incremental Processed_Log writes (related: PR #6's
  flush-vs-save asymmetry that drove orphan creation)
- ADR 0011 — events_from_ids.py design rationale (the tool this
  consolidation is unifying)
