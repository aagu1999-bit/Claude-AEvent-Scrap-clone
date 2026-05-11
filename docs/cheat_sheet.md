# Tool Cheat Sheet

Quick lookup: what scenario you're in → which tool to run.

Keep this file updated as tools evolve. Last updated: 2026-05-11.

---

## "What just happened? What should I do?"

| Situation | Tool | One-line command |
|---|---|---|
| It's Thursday afternoon, the cron just fired | `orphan_check.py` then `quality_metrics.py --compare` | See "After every weekly cron" below |
| I see weird yellow on All_Events that doesn't match the flag column | `reset_quality_formatting.py --apply` | `python reset_quality_formatting.py --apply` |
| I see cities tagged with wrong region (e.g. wrong SOUTH/NORTH) | `audit_regions.py --apply` | `python audit_regions.py --apply --highlight` |
| I notice duplicate event rows | `dedup_all_events.py --apply` | `python dedup_all_events.py --apply` |
| Posts marked done in Processed_Log but missing from All_Events ("orphans") | Recover via `events_from_ids.py` | `python events_from_ids.py --from-ids outputs/orphan_check_<ts>.csv --source-datasets <ids>` |
| The team flagged a wrong extraction on a specific post | `events_from_ids.py --from-ids` | Same as above with just that post in the CSV |
| The cron ran but produced 0 events for a normally-busy account | Check `Silent_Failures` tab + run `account_hygiene.py` | `python account_hygiene.py` |
| I want to A/B test a prompt or model change | `compare_extractors.py` | `python compare_extractors.py --n 10` |
| I'm about to merge a PR and want to verify the safety net works | `smoke_test.py` | `python smoke_test.py` |
| I want to know if quality changed after a code change | `quality_metrics.py --compare <prior_json>` | See "Comparing quality over time" below |

---

## After every weekly cron (Thursday afternoon)

Two commands. Takes ~1 minute total.

```bash
python orphan_check.py
python quality_metrics.py --compare outputs/quality_metrics_20260511_103733.json
```

**What to look for:**
- Orphan count should stay flat (or near flat) — PR A's orphan hardening means new orphans shouldn't appear from cron runs
- New rows added (delta in `rows_considered`) should match the cron's "events extracted" count
- `hallucinated_city` and `conf out-of-range` should NOT grow — PR B's prompt fix means new rows shouldn't have these
- If anything jumps, dig into the new flagged rows

---

## Comparing quality over time

You'll accumulate `outputs/quality_metrics_*.json` files. Compare any two:

```bash
python quality_metrics.py --compare outputs/quality_metrics_20260511_103733.json
```

The current run becomes the "after". The argument file is the "before". Deltas printed inline.

---

## TOOL CATALOG — by category

### 🔄 Production (auto-runs Thursday 14:00 ET — you don't run these)

| Tool | Reusable? | Purpose |
|---|---|---|
| `main.py` | Recurring | The weekly cron itself. Apify → OCR → Gemini → All_Events |
| `recurring_accounts.py` | Recurring (auto-called at end of cron) | Refreshes `Reliable_Accounts` tab from event history |
| `audit.py` | Recurring (auto-called at end of cron) | Detects accounts that returned zero posts. Writes to `Silent_Failures` and `Run_Audit` tabs |

### 🔧 Recovery & re-extraction (run when you need them — reusable forever)

| Tool | Reusable? | Purpose |
|---|---|---|
| `events_from_ids.py` | **Yes — keep forever** | Re-extracts events for a list of known post IDs. Uses 4-tier ladder + sanity checks. **This is your primary recovery tool.** |
| `merge_extract_runs.py` | Yes — keep forever | Combines partial run summaries when `events_from_ids.py` was interrupted + resumed |
| `reprocess_weekends.py` | Yes — keep forever | Fresh Apify scrape of Friday/Saturday/Sunday posts, then runs the pipeline |

### 🧹 One-shot data cleanup (run when you notice a specific problem)

| Tool | Reusable? | Purpose |
|---|---|---|
| `audit_regions.py` | **Yes — reusable** | Fixes mistagged SECTION OF NJ cells against canonical NJ lookup. Run after editing `data/nj_municipalities.json` |
| `dedup_all_events.py` | **Yes — reusable** | Removes duplicate All_Events rows. Composite key = POST ID + EVENT NAME + DATE |
| `reset_quality_formatting.py` | **Yes — reusable** | Refreshes yellow-cell formatting on All_Events. Run after editing `FLAG_TO_COLUMNS` or when drift appears |
| `account_hygiene.py` | **Yes — reusable** | Audits Accounts tab for dead/typo'd handles. Run periodically |
| `migrate_history.py` | One-off (likely) | Historical data migration. Status TBD |

### 🔍 Diagnostics & verification (built 2026-05-11 — reusable forever)

| Tool | Reusable? | Purpose |
|---|---|---|
| `smoke_test.py` | **Yes — run before every PR merge** | Verifies extraction_core safety net is wired (no API cost, ~5 sec) |
| `orphan_check.py` | **Yes — run after every cron + before/after PRs** | Detects orphans (Processed_Log entries with no matching All_Events rows) |
| `quality_metrics.py` | **Yes — run before/after PRs + monthly** | Snapshots All_Events quality state. Supports `--compare` for delta vs prior baseline |
| `compare_extractors.py` | **Yes — run before prompt-change PRs** | Diffs main.py vs events_from_ids.py extractions on same posts. Costs ~$0.01 per run |

### 📚 Shared modules (NOT standalone tools — imported by others)

| Module | What it provides |
|---|---|
| `extraction_core.py` | Sanity checks, lookups, `FLAG_TO_COLUMNS`, `ESCALATION_FLAGS`, `build_prompt()`, cost estimates |
| `ig_lookup.py` | Instagram handle lookups (used by `audit.py`) |

---

## Tools that ARE one-time / sit unused

Honestly looking at the inventory, NONE of the tools are "one-time" in the sense that they'd never run again. They all answer a recurring class of question:

- `audit_regions.py` — needed every time NJ lookup data changes
- `dedup_all_events.py` — needed if dedup logic ever has a gap
- `reset_quality_formatting.py` — needed every time `FLAG_TO_COLUMNS` mapping changes
- `events_from_ids.py` — needed every time you find missing posts
- All 4 diagnostic tools — needed before every PR + after every cron

`migrate_history.py` might be the one exception — purpose unclear, might be safe to delete after confirming nothing else imports it.

---

## How to add a new tool (for future Ada)

When you build a new tool:
1. Add it to the table above with category + reusability + one-line purpose
2. If it's a verification tool, document what scenarios trigger it
3. If it's a one-shot, mark when it's safe to delete

---

## Common pitfalls

- **Don't run any of these against production unless `--apply` is in the command.** They default to dry-run for safety
- **`main.py` and `events_from_ids.py` are NOT competing tools.** main.py is the cron; events_from_ids.py is recovery. They share `extraction_core.py` (same prompt, same sanity checks) but serve different purposes
- **`outputs/apify_cache/` is your forensics backup.** Every Apify dataset fetched gets cached there. Don't delete it — recovery tools depend on it
- **Production sheet is `Instagram_Events_Master`.** None of these tools touch a scratch sheet by default; they all read/write production
