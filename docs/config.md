# `config.json` — settings reference

The pipeline reads `config.json` on every run. Settings split into three groups
based on whether they apply to **live Apify mode** (`apify_enabled: true`),
**static-URL mode** (`apify_enabled: false`), or **both**.

## Universal — apply in any mode

| Key | Type | Default | Behavior |
|---|---|---|---|
| `run_now` | bool | `false` | One-shot trigger. When `true`, the next scheduler tick runs the pipeline immediately, then resets to `false`. |
| `schedule_day` | str | `"Thursday"` | Day of week the scheduler fires. |
| `schedule_time` | str | `"14:00"` | Time the scheduler fires. **Interpreted in `America/New_York` time** (hardcoded in `main.py` `start_scheduler()` — change there if you need a different zone). |
| `apify_enabled` | bool | `true` | Mode switch. `true` = live Apify auto-run on each pipeline trigger. `false` = read from the fixed `instagram_data_url` dataset. |
| `sheet_name` | str | `"Instagram_Events_Master"` | Target Google Sheet. |
| `gemini_model` | str | `"gemini-2.0-flash-lite"` | Extraction model. |
| `max_workers` | int | `10` | `ThreadPoolExecutor` concurrency for `process_post()`. Capped at 20. Higher = faster but more concurrency races. |
| `rate_limit_delay` | float | `0.5` | Per-post sleep before the Gemini call, in seconds. Increases automatically on 429 errors. |
| `post_offset` | int | `0` | Skip the first N posts in the input list before extraction. Useful for resuming from a known position. |
| `max_posts` | int | `0` | Cap on posts extracted. `0` = no cap (process all). Useful for testing on a small slice. |

## Mode 1 only — used when `apify_enabled: true`

| Key | Type | Default | Behavior |
|---|---|---|---|
| `apify_posts_per_profile` | int | `9` | Apify `resultsLimit` param — max posts to fetch per IG account per run. |
| `apify_newer_than_days` | int | `14` | Apify `onlyPostsNewerThan` filter — exclude posts older than this from the live scrape. |
| `history_max_age_days` | int | `30` | Posts whose `Post Date` is older than this drop out of the `Processed_Log` dedup set on load. Combined with the per-run Apify scrape filter, this keeps the dedup set bounded over time. **Mode 1 only as of 2026-05-08** — see [docs/decisions/0005-config-mode-scoping.md](decisions/0005-config-mode-scoping.md). In Mode 2, the cutoff is skipped and the full Processed_Log is used for dedup. |

## Mode 2 only — used when `apify_enabled: false`

| Key | Type | Default | Behavior |
|---|---|---|---|
| `instagram_data_url` | str | (manual) | URL to a previously-generated Apify dataset (`https://api.apify.com/v2/datasets/<id>/items?...`). Manually updated — there is no auto-rotation. The pipeline downloads this dataset's contents on each run instead of triggering a fresh Apify scrape. |

## When you'd use which mode

- **Mode 1 (`apify_enabled: true`)** — production weekly cron. The pipeline triggers Apify itself, gets a fresh dataset for the past N days, and writes a raw dump to `outputs/apify_raw_<ts>.json` (see ADR 0003) so post-hoc forensics work.
- **Mode 2 (`apify_enabled: false`)** — manual / debug runs. You generate an Apify dataset by hand, paste its items URL into `instagram_data_url`, and let the pipeline process that fixed snapshot. No Apify quota consumed by the tool itself.

The mode-scoping of `history_max_age_days` exists because in Mode 2 the source data doesn't rotate. If we applied the age cutoff there, posts in the static dataset would silently re-enter the processing queue every week as they crossed the threshold — generating duplicate rows in `All_Events` indefinitely. Mode 1 doesn't have this problem because each run fetches a fresh dataset.
