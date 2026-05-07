# ADR 0002: Apify `onlyPostsNewerThan` window = 14 days

Date: 2026-05-07
Status: Accepted

## Context

`apify_newer_than_days` controls the `onlyPostsNewerThan` parameter passed to
the Apify Instagram-post-scraper. Posts older than this date are filtered at
the source before reaching the pipeline. Default values inherited from earlier
agent recommendations were 7, then 10 days, with no recorded rationale.

The window is the practical horizon for catching event announcements. Most
announcements go up 1–6 weeks before the event; many of the misses we have
diagnosed (e.g., `DWg2XDvkY06`, posted Mar 30 for a May 2 event) were already
out of window the moment the live Apify scrape went online.

The user runs the pipeline manually with a 14-day window, and that is the
preference that should be encoded as the default. Lowering it again should
require a documented reason here, not silent agent edits.

## Decision

`apify_newer_than_days` default = **14**.

## Consequences

- Slightly more Apify quota consumed per run (≤ 2× vs. 7-day window in the
  worst case for very active accounts; in practice posts-per-profile cap of 9
  bounds the increase).
- Reduces the per-account post-date blind spot from 7 to 14 days.
- Does **not** address newly-added accounts whose key announcement is older
  than 14 days — that is the onboarding-backfill problem deferred in
  [ADR 0001](0001-reliable-to-active-promotion.md).

## Future agents — read this before changing the default

If you are tempted to drop this value to "save Apify quota", consult the user
first. The 14-day window is a recorded preference, not a guess.
