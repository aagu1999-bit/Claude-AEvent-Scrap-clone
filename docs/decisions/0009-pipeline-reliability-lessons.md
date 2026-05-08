# ADR 0009: Pipeline reliability — lessons from the May 7-8 incident investigation

Date: 2026-05-08
Status: Accepted

## Context

A multi-day investigation into "why was this Instagram post never extracted?"
expanded into a full audit of the pipeline's data integrity, configuration
correctness, and operational resilience. By the time it concluded, eight
distinct issues had been surfaced and fixed across PRs 1-4 on this repo and
PR 3 on the original repo. ADRs 0001-0008 document the specific decisions.

This ADR is different. It captures the **transferable lessons** — principles
that apply to any future agent working on this pipeline, or on similar
data-extraction systems. Specifics of May 7-8 are evidence; the lessons are
the takeaway.

## Lessons

### 1. Distinguish verified facts from inferences in every claim

**The pattern that hurt:** confident claims based on plausible-sounding
reasoning. Examples that turned out to be wrong: "12-hour runtime at
max_workers=2" (actual: 36 minutes), "CDN failures are the systemic
issue" (actual: 0.85% rate, noise), "Apify dataset isn't recoverable"
(actual: it's a public URL, just no live API behind it).

**The fix:** every report ends with three explicitly-labeled buckets:

- **Verified:** facts read directly from a file, command output, or query
  result, with file:line / commit / source pointer.
- **Inferred:** interpretations that fit the verified facts but aren't
  independently confirmed.
- **Open:** questions where the data isn't available or the claim couldn't
  be tested.

When an agent (or human) is tempted to hedge with "probably" or "likely,"
that's the signal to either verify it or move it to **Inferred**.

### 2. Run the grep before theorizing

**The pattern that hurt:** "If 10% of CDN downloads are failing, that's a
gap; if 40%, it's a crisis." 30 minutes of speculation followed.

**The fix:** when a question has a quantitative answer accessible via a
single shell command, file read, or API call, run it first. Theory comes
after numbers, not before.

Concretely: `grep -c "Max retries exceeded" outputs/run_*.log` is a
five-second answer to a question that was generating thirty-minute
philosophical arguments.

### 3. Top-3-concerns guardrail before deep investigation

**The pattern that hurt:** four rounds of investigation into 2 orphan post
IDs (representing 0.09% of a single run's data) while a 503-row duplicate
problem and a CDN-failure question waited untouched.

**The fix:** before going more than ~30 minutes deep on any single thread,
explicitly ask: "is this in the user's top 3 concerns right now?" If not,
file it as **Open** and move to a higher-priority item.

Engineering rabbit holes are seductive specifically because they're
tractable. Tractability is not the same as importance.

### 4. Observability is the highest-leverage investment

**The pattern that hurt:** answering "why was this post missed?" required
re-scraping from Apify, reading code paths, and building case-by-case
hypotheses. The investigation could not progress without raw data.

**The fix (already shipped — see ADR 0003):** save what you'd need for
forensics *during normal runs*, not when something goes wrong.

- Per-run log files (stdout/stderr teed to disk)
- Per-post anomaly summaries (caption, OCR preview, AI raw response, reason)
- Raw upstream API dumps (Apify response verbatim)

These artifacts cost ~3MB per run combined and turned hour-long forensics
into seconds-long greps. **Build the observability surface first; the
features come second.**

### 5. Default to dry-run for any script that mutates state

**The pattern that helped:** the cleanup script (ADR 0007) was structured
with dry-run as default and `--apply` as an explicit flag. The dry-run
output revealed three different schema variants in `Processed_Log` that
no one had documented. Without dry-run, the first invocation would have
silently corrupted ~12,000 rows.

**The principle:** if a script writes, deletes, or otherwise mutates,
running it without flags should be a preview, not the action. The user
opts in to destruction with an explicit flag they have to type.

### 6. Empirical data trumps heuristic detection

**The pattern that hurt:** the initial pseudo-ID hypothesis was "Apify
sometimes returns malformed posts." The implied fix was retry logic or
content-hashing.

**What the data actually showed (verified by inspecting the static
dataset directly — ADR 0008):** Apify returns "shell records" for
accounts that produced no posts. These aren't malformed; they're
explicit signals that an account is dead, private, suspended, or
typo'd. Different problem, completely different fix (skip + surface
affected accounts instead of retry/hash).

**The principle:** when a fix design depends on a hypothesis about
upstream behavior, verify the upstream behavior empirically before
designing. The fix you'd ship for a misdiagnosed problem can be both
unnecessary and harmful.

### 7. Schema drift compounds silently — detect by content, not position

**The pattern that hurt:** three distinct row layouts had written to
`Processed_Log` over the pipeline's lifetime. By the time it was
discovered, ~38% of rows were in non-canonical layouts. Position-based
detection ("col 3 is Source") would have silently misinterpreted
12,000 rows.

**The fix:** content fingerprints. The cleanup script identifies row
schema by the *value* in specific columns ("Migration Script" in col 2,
"Auto-Bot" in col 2 with col 3 empty, etc.) — not by position. Rows
that don't match any fingerprint land in an "unknown" bucket and are
flagged for manual review rather than silently transformed.

**The principle:** when multiple writers contribute to the same data
sink over time, assume drift will happen and design detection to be
robust to it. Position-based access is a footgun.

### 8. Atomic operations require co-location of check + mutation

**The pattern that hurt:** dedup check and dedup claim were in two
separate lock blocks separated by 10-30 seconds of network/AI work.
Two threads with the same post ID could both pass the check before
either claimed. Result: 47 duplicate composite keys, 55 excess rows
in a single run (May 8 reprocess).

**The fix (ADR 0006):** check-and-claim happen in the *same* lock
block. The second thread sees the claim and short-circuits.

**The principle:** any "look up state, do work, modify state" sequence
where state-modification depends on the lookup result must be atomic.
If the lookup and the modification can interleave with another thread,
they're not atomic, regardless of how short the work between them is.

### 9. Mode-scoped configuration must be enforced by code, not convention

**The pattern that hurt:** `history_max_age_days` was intended for live
Apify mode (auto-scrape, dataset rotates each run). It was being applied
in static-URL mode (single fixed dataset, never rotates). The result
was duplicate event rows accumulating weekly as posts in the static
dataset crossed the age boundary and re-entered processing.

**The fix (ADR 0005):** the cutoff is now explicitly conditional on
`apify_enabled`. The intent matches the code.

**The principle:** when config settings have mode-dependent semantics,
the code must check the mode and skip the setting in non-applicable
modes. "We won't enable that mode" is not a guarantee — modes get
flipped, configurations get edited, and the silent bug surfaces three
months later. Document mode scoping in `docs/config.md`; enforce it in
code.

### 10. Cleanup scripts must batch from day one

**The pattern that hurt:** the first version of `cleanup_pseudo_ids_and_duplicates.py`
made one Sheets API call per row inside each "batch." With 100 rows per
batch and no inter-call delay, peak rate was ~67 calls/second — well
over the 60-writes-per-minute Sheets API quota. Result: thousands of
429 errors during the first apply run, partial-cleanup state, and
required a second pass to finish.

**The fix:** all three passes now use `batch_update` to send N row
operations in a single API call.

**The principle:** for any script touching an external API at scale,
the per-call vs per-batch decision is not an optimization — it's
correctness. Per-call almost always exceeds rate limits at scale even
with inter-batch sleeps. Default to batch APIs; only fall back to
per-call when no batch endpoint exists.

### 11. Idempotency is mandatory for cleanup scripts

**The pattern that helped:** when the first cleanup attempt died from
429 errors mid-run, ~half the work was complete. The script's
classification was based on content, not row position, so re-running
correctly identified what was still to do and skipped what was already
done.

**The principle:** every cleanup script should be safely re-runnable.
"Already-cleaned-up state" should be a no-op, not a duplicate or an
error. If you can't articulate "what happens if this is run twice,"
the script isn't ready to run once.

### 12. Distinguish kills mid-extraction from kills post-completion

**The pattern that hurt:** "the run was killed" was used to describe
three different scenarios on May 7-8. Two of them were actually
successful runs that received SIGINT during post-save cleanup
(`recurring_accounts.refresh()` running after `save_data()` had
completed). Treating them as failures led to incorrect remediation
proposals.

**The fix:** when investigating a kill, look at *which step had been
reached* before the kill, not just whether the process exited cleanly.
If `save_data()` completed (CSV exists, stats JSON exists, All_Events
has rows from this run), the kill is post-completion noise.

**The principle:** a non-zero exit code is necessary but not sufficient
evidence of a failure. Always check what was reached before the exit.

### 13. Hand off scripts in the form they'll be run in

**The pattern that hurt:** Python snippets pasted directly into bash
fail every time, in the same way, until someone explicitly notes
"wrap in `python3 << 'EOF'` ... `EOF`."

**The fix:** when handing off a Python script for the user (or another
agent) to run via shell, wrap it in the heredoc form by default. Don't
expect them to figure out the wrapper.

More broadly: the form of a handoff matters as much as the content. A
correct script in the wrong form is a failed handoff.

### 14. Multi-agent collaboration needs explicit ground rules

**The pattern that hurt:** without explicit conventions, two agents
working on the same project produced reports of varying rigor, with
some claims labeled and some unlabeled, some with file:line cites and
some without. Cross-checking required reading both reports plus the
underlying code.

**The fix:** establish ground rules at the start of any multi-agent
collaboration:

- Verified vs Inferred labels on every claim, with cite for verified
- File:line references for any code claim
- Top-3-concerns check before deep dives
- Don't substitute theory for empirical checks
- Hand-offs include the form (heredoc, shell, etc.) the receiver needs

When ground rules emerge mid-collaboration, document them and hold the
line for the rest of the project. Future agents read those rules and
slot in faster.

## What this ADR does NOT do

- **Does not detail the May 7-8 incident.** Specifics live in ADRs
  0005-0008 and the run-log files. This ADR's scope is principles, not
  recap.
- **Does not prescribe one investigation methodology.** The lessons
  here are about discipline, not about a specific tool or workflow.
  Different investigators may apply them differently; the goal is the
  outcome, not the process.

## Future agents — read this before starting any non-trivial investigation

If you're an agent picking up work on this pipeline (or any similar
system) and there's a mystery to debug:

1. Run the grep before theorizing.
2. Label every claim Verified / Inferred / Open.
3. Surface any deep dive past 30 minutes for top-3 confirmation.
4. Default to dry-run for anything that mutates.
5. Verify upstream behavior empirically before designing fixes that
   depend on it.

If you find yourself making confident claims you can't trace to a
file:line or a query result, stop and verify.
