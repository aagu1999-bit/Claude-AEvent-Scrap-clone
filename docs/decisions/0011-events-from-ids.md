# 0011 — events_from_ids.py: targeted re-extraction tool

**Status:** Draft (skeleton landed; implementation pending)
**Date:** 2026-05-08
**Context:** This decision documents the design rationale for a new tool
that fills a gap not covered by the existing tooling (`main.py`,
`recover_events.py`, `reprocess_weekends.py`).

## Problem

Multiple recurring needs require re-running OCR + Gemini extraction on a
*known list of post IDs*, not a fresh Apify scrape:

1. **Orphan recovery.** When `save_data()` writes events to memory and
   marks `Processed_Log = events_found` via PR #6's incremental flush
   (ADR 0010), but the run is killed before the events reach All_Events,
   posts become "orphaned" — marked done with no event rows. Today's Pro
   run produced 892 such orphans; a separate Apr 23 incident produced
   944 more. Total: 1,843 orphan posts as of 2026-05-08.
2. **Manual re-extraction.** When a team member spots a wrong extraction
   in All_Events (wrong date, off-by-one venue, missed events), they
   need to re-run that specific post through the pipeline.
3. **Pro fallback retries.** A 3-tier extraction ladder (cheap → expensive)
   needs an entry point that takes a post-ID and runs higher-tier extraction.
4. **A/B testing.** Iterating on prompts or models requires running the
   same fixed set of posts across variants.

None of these are well-served by existing tools:
- `main.py` requires a fresh Apify scrape; can't target specific post IDs.
- `recover_events.py` uploads existing CSVs to the sheet; doesn't re-extract.
- `reprocess_weekends.py` is weekend-scoped and triggers Apify scrapes.

## Decision

Build `events_from_ids.py` as a single tool with two modes:

### Mode 1: extract (`--from-ids <csv>`)
Take a CSV listing post IDs. For each post, look up source data in
provided Apify dataset(s), run OCR + Gemini through a 3-tier ladder,
apply sanity checks, append events to All_Events.

### Mode 2: upload (`--from-events <csv>`)
Take a pre-extracted Events_*.csv. Dedup against All_Events. Append.
Used when `save_data()` wrote a CSV but Sheets push failed.
**This mode replaces `recover_events.py`** which is being deleted.

Both modes share: sheet connection, batching, rate-limit handling,
conditional formatting on flagged rows.

## Key design choices

### 1. Bypass Processed_Log entirely
Recovery posts are already marked `events_found` in Processed_Log.
We don't re-mark them. We just write events to All_Events. This avoids
exposure to the PR #6 flush-vs-save asymmetry (ADR 0010 follow-up B1).

### 2. Incremental writes (every 25 posts)
`main.py:save_data()` only writes events at end-of-run. A killed run
loses all unsaved extractions. `events_from_ids.py` flushes to
All_Events every 25 posts, so a killed run preserves ~95% of progress.

### 3. Three-tier extraction ladder
Cost-aware quality:
- **Tier 1: Flash-Lite + OCR text.** Cheapest, default attempt. Works
  for most posts.
- **Tier 2: Flash-Lite + image (multimodal).** Same model, sends image
  instead of OCR-flat text. Addresses spatial layout bugs (e.g., the
  kinnectnj 2-column off-by-one mispairing) without escalating cost.
- **Tier 3: Pro + image.** Last resort for stubborn cases.

Estimated cost on 1,712 orphans:
- Tier 1 alone: $2-3
- + Tier 2 on flagged (~10-15%): +$1-2
- + Tier 3 on still-flagged (~2-5%): +$1-2
- **Total: ~$4-7**, vs $17 for Pro-everywhere.

### 4. Sanity checks drive escalation AND surface for humans
A row flagged by any sanity check (DATE_DAY_MISMATCH,
VENUE_CITY_MISMATCH, CALENDAR_LOW_EVENTS, etc.) escalates to the next
tier. Flags that survive all tiers are written to a new
`QUALITY_FLAGS` column on All_Events, and the row gets light-yellow
conditional formatting. This makes "needs human review" rows visible
without polluting cell values.

### 5. Authoritative city → region lookup
Gemini's per-call region inference is unreliable (Newark tagged CENTRAL
747 times in existing data; West Orange 50/50 split). Replaced with a
canonical `data/nj_municipalities.json` derived from authoritative NJ
municipality data. When extraction returns a different region than the
lookup, region is silently corrected and `REGION_AUTOFIXED` is added
to flags (transparent during initial rollout, can be silenced later).

Out-of-state cities (Brooklyn, Philadelphia, etc.) get `NON_NJ` in the
lookup; their region is cleared rather than mistagged.

### 6. Venue → city lookup, derived + curated
Built from existing All_Events history then human-reviewed. Top-100
ambiguous venues (those appearing with multiple cities) flagged for
operator review. Cleaned mapping saved as `data/venue_city_canonical.json`.
Used for `VENUE_CITY_MISMATCH` flag.

### 7. Tool consolidation
`upload_recovery.py` is deleted (it's an unsafe earlier version of
`recover_events.py` lacking dedup and rate-limiting).
`recover_events.py` is deleted (its functionality is absorbed into the
new tool's `--from-events` mode).
Net: 2 narrow tools → 1 multi-mode tool.

## Sanity checks (full list)

| Flag | Trigger | Class |
|---|---|---|
| `DATE_DAY_MISMATCH` | Source mentions day-name; extracted date is different day | Per-row, escalates |
| `VENUE_CITY_MISMATCH` | Venue→city pair conflicts with lookup | Per-row, escalates |
| `LOW_CONFIDENCE` | event.confidence < 0.5 | Per-row, escalates |
| `CALENDAR_LOW_EVENTS` | Caption keyword + ≤1 event | Post-level, escalates |
| `CAROUSEL_LOW_EVENTS` | 3+ slides + ≤1 event | Post-level, escalates |
| `OCR_RICH_LOW_EVENTS` | 1500+ char OCR + ≤1 event | Post-level, escalates |
| `ACCOUNT_PATTERN_DROP` | Account avg ≥3 events but this post ≤1 | Post-level, escalates |
| `REGION_AUTOFIXED` | Region corrected from canonical lookup | Auto-fix, no escalation |
| `CITY_NOT_IN_NJ_LOOKUP` | Out-of-NJ or unknown city | Auto-fix (clear region), no escalation |

Corpus-level checks (week-over-week drops, cross-account corroboration,
volatility outliers) are out of scope for this tool — they require
seeing many extractions together. Reserved for a separate audit tool.

## Tradeoffs accepted

- **Doesn't touch Processed_Log.** Posts marked `events_found` stay marked,
  even if recovery extraction yields zero events. This is intentional: the
  flag column captures the recovery outcome; rewriting Processed_Log isn't
  the job of this tool.
- **Dedup is composite-key (post_id + event_name + date).** Same as
  `recover_events.py`. Acceptable false-negative risk if a re-extraction
  produces a slightly different event_name; we'd write a duplicate row.
  The QUALITY_FLAGS column makes such cases visible.
- **Per-row format API call is slow.** Each flagged row triggers one
  `ws.format()` call with a 0.3s pause. For 50 flagged rows, that's
  ~15 seconds extra per batch. Acceptable for the visibility benefit.

## Future work (out of scope)

- **B1 (PR #6 fix): two-phase commit.** Don't mark Processed_Log
  `events_found` until events are confirmed in All_Events. Prevents
  future orphan creation. Separate decision record.
- **B4 (Apify URL archival):** Pipeline should log the dataset URL and
  cache the response for every run, not just when `apify_enabled=true`.
  Enables future retroactive recovery.
- **A6 (prompt audit):** Review the Gemini prompt at `main.py:784`
  for explicit handling of column-grid layouts, anti-hallucination
  guardrails, and confidence-gated output. Separate decision record.
- **`audit_regions.py`:** One-shot cleanup of existing All_Events rows
  with mistagged regions. Uses the same canonical lookup.
- **Corpus-level audit tool:** Implements the deferred sanity checks
  (week-over-week drops, cross-account corroboration, volatility).

## References

- ADR 0010 — incremental Processed_Log writes (the bug this tool routes around)
- ADR 0009 — pipeline reliability lessons
- ADR 0006 — dedup race condition (related: PR #4)
- ADR 0008 — skip Apify shell records
- `outputs/orphan_recovery_queue.csv` — empirical orphan set this tool's first run targets
- `outputs/venue_city_review.csv` — venue ambiguity audit (operator review)
- `outputs/nj_cities_for_review.csv` — city → region lookup draft (operator review)
