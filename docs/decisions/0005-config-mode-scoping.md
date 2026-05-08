# ADR 0005: Config mode-scoping for `history_max_age_days` + explicit scheduler timezone

Date: 2026-05-08
Status: Accepted

## Context

While investigating duplicate rows in `All_Events` after the May 7 incident,
two separate config-vs-code issues surfaced:

**1. `history_max_age_days` was applied universally, not Mode-scoped.**

User intent (verified verbally with the operator): the setting was originally
designed for **live Apify auto-run mode** (`apify_enabled: true`). When the
pipeline triggers its own Apify scrape, each run pulls a fresh dataset
filtered by `apify_newer_than_days`, and the matching `history_max_age_days`
keeps the `Processed_Log` dedup set bounded so it doesn't grow unbounded.

The code, however, applied the cutoff in `setup_sheets()` on every pipeline
start, regardless of `apify_enabled`. Combined with Mode 2's behavior (read
from a fixed `instagram_data_url`), this caused posts in the static dataset
to silently re-enter processing every week as their post-date aged past the
30-day cutoff. Since `save_data()` does `append_rows()` to `All_Events` with
no dedup-on-write, the same events were appended multiple times across
weeks — accumulating duplicate rows in the master sheet.

The May 8 reprocess run produced 47 duplicate composite keys (55 excess
rows). The race-condition cause for that specific run is documented in
[ADR 0006](0006-dedup-race-condition.md) (forthcoming). The mode-scoping
issue is a parallel, longer-running source of duplicates.

**2. `schedule_time` had no declared timezone.**

`start_scheduler()` compared `CONF["schedule_time"]` (a string like `"14:00"`)
against `datetime.now()` with no timezone. Replit's container local time was
undeclared, leading to surprising firing times that didn't match what was
written in `config.json`.

## Decision

### `history_max_age_days` becomes Mode-1 only

In `setup_sheets()`:

```python
apify_live = bool(CONF.get("apify_enabled", False))
if apify_live:
    max_age_days = int(CONF.get("history_max_age_days", 30))
    cutoff = (datetime.now() - timedelta(days=max_age_days)).date()
else:
    cutoff = None  # Mode 2: keep full Processed_Log dedup history
```

The downstream cutoff check guards on `cutoff is not None`. When `cutoff`
is `None`, the Processed_Log loader keeps every entry in the dedup set
regardless of post age.

This matches the operator's mental model and stops the Mode-2 duplicate
accumulation.

### `schedule_time` interpreted in `America/New_York`

`start_scheduler()` now creates a `ZoneInfo("America/New_York")` and uses
`datetime.now(SCHEDULE_TZ)` for both the day-of-week check and the
HH:MM comparison. The startup banner prints the timezone explicitly so
it's visible in run logs.

To change the zone, edit the `SCHEDULE_TZ` line in `start_scheduler()`.
The choice is hardcoded rather than configurable to avoid yet another
config field that can drift.

## Consequences

- **Mode 2 runs preserve full Processed_Log dedup.** Static-URL runs
  no longer accumulate duplicate rows in `All_Events` from age-based
  re-processing. The race-condition path (ADR 0006) is a separate fix.
- **`Processed_Log` will grow without the age-based pruning** when running
  in Mode 2 long-term. Acceptable — Sheets handles tens of thousands of
  rows comfortably, and the next major architectural cleanup (a real
  database backing) is a separate planned change.
- **Scheduler firing times become predictable.** New York time is the
  operator's local time and matches the cadence the pipeline was
  configured for in past weeks.
- **Existing duplicate rows in `All_Events` are not affected by this ADR.**
  A separate one-off cleanup script will collapse `(POST ID, EVENT NAME, DATE)`
  composite-key duplicates to single rows. That script is being prepared
  separately (see TODO in CHANGELOG).

## What this ADR does NOT do

- **Does not solve the race-condition duplicate path.** That's ADR 0006:
  the dedup check and dedup add in `process_post()` are separated by
  10–30 seconds of work where the lock is released, allowing two threads
  with the same post ID to both pass the check.
- **Does not flip `apify_enabled`.** The operator runs in Mode 2 by
  preference. This ADR makes Mode 2 well-behaved on its own terms.
- **Does not add a configurable timezone.** Hardcoded New York is
  acceptable for a single-operator tool.

## Future agents — read this before editing config defaults

The two settings touched here have specific, documented intent:

- `history_max_age_days` is **Mode-1 only**. Don't reintroduce a universal
  cutoff. If you need to bound `Processed_Log` growth in Mode 2, do it via
  an explicit cleanup script, not via a silent age-based filter.
- `schedule_time` is interpreted in **`America/New_York`** unless you change
  `SCHEDULE_TZ` in `start_scheduler()`. Don't switch back to `datetime.now()`
  without a zone — that re-introduces the surprise firing-time bug.
