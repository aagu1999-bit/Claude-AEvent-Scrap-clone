#!/usr/bin/env python3
"""
audit.py — End-of-run pipeline auditor.

Called by main.py after each pipeline run. Identifies "silent failures":
input handles in the Accounts tab that returned ZERO posts from Apify,
classifies them, and writes results to two new sheet tabs.

Two outputs written to the existing Google Sheet:
  • Run_Audit       — appended each run; time-series of pipeline health.
  • Silent_Failures — overwritten each run; current snapshot of bad-input handles.

Classification logic for each zero-post handle:
  • PRODUCTIVE_BEFORE → handle has appeared in Processed_Log before
                        (i.e., Apify USED to scrape it; now it suddenly stopped).
  • NEVER_PRODUCED    → handle has never appeared in Processed_Log.
                        Could be a quiet account or a bad handle.
  • IG_404            → IG profile endpoint returned Page Not Found.
                        These are typos / deleted accounts.

For NEW silent failures only (handles not in last run's failure snapshot),
we hit Instagram's public profile JSON endpoint to detect IG_404 — keeps
network volume low and avoids rate limits.
"""

import re
import time
from datetime import datetime
from typing import Optional

import gspread

from ig_lookup import normalize_handle as _norm, owner_from_post as _owner_from_post, check_handle as _check_ig_handle_helper

# ─────────────────────────────────────────────────────────────
# Tab schemas
# ─────────────────────────────────────────────────────────────

RUN_AUDIT_TAB = "Run_Audit"
RUN_AUDIT_HEADER = [
    "run_timestamp",
    "input_count",
    "accounts_returned_posts",
    "accounts_returned_zero",
    "new_silent_failures",
    "persistent_silent_failures",
    "confirmed_404_this_run",
    "total_posts_received",
    "events_extracted",
    "notes",
]

SILENT_FAILURES_TAB = "Silent_Failures"
SILENT_FAILURES_HEADER = [
    "handle",
    "classification",          # PRODUCTIVE_BEFORE | NEVER_PRODUCED | IG_404
    "runs_failed_in_a_row",
    "ig_status",               # real | missing | private | unknown (from IG check; blank if not checked)
    "followers",
    "post_count",
    "last_checked",
]

ACCOUNTS_TAB = "Accounts"
PROCESSED_LOG_TAB = "Processed_Log"

# ─────────────────────────────────────────────────────────────
# Helpers (handle normalization + IG lookup live in ig_lookup.py)
# ─────────────────────────────────────────────────────────────


def _get_or_create_tab(spreadsheet, name: str, header: list, rows: int = 1000):
    """Get worksheet by name; create with header if missing."""
    try:
        ws = spreadsheet.worksheet(name)
        return ws, False
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=max(rows, 100), cols=len(header))
        ws.append_row(header, value_input_option="USER_ENTERED")
        return ws, True


# ─────────────────────────────────────────────────────────────
# Sheet readers
# ─────────────────────────────────────────────────────────────


def _read_accounts(spreadsheet) -> list:
    """Read the Accounts tab. Returns list of normalized handles."""
    try:
        ws = spreadsheet.worksheet(ACCOUNTS_TAB)
    except gspread.WorksheetNotFound:
        return []
    col = ws.col_values(1)
    if col and col[0].strip().lower() in ("username", "handle", "account"):
        col = col[1:]
    return [_norm(c) for c in col if c.strip()]


def _read_productive_handles(spreadsheet) -> set:
    """
    Read Processed_Log tab and return set of handles that have EVER produced posts.
    Processed_Log column layout (from main.py: [pid, account, date, ...]):
      A=post_id, B=account, C=date_processed, ...
    """
    try:
        ws = spreadsheet.worksheet(PROCESSED_LOG_TAB)
    except gspread.WorksheetNotFound:
        return set()
    accounts = ws.col_values(2)  # column B
    if accounts and accounts[0].strip().lower() in ("account", "username", "handle"):
        accounts = accounts[1:]
    return {_norm(a) for a in accounts if a.strip()}


def _read_prior_silent_failures(spreadsheet) -> set:
    """Return set of handles that were in last run's Silent_Failures snapshot."""
    try:
        ws = spreadsheet.worksheet(SILENT_FAILURES_TAB)
    except gspread.WorksheetNotFound:
        return set()
    col = ws.col_values(1)
    if col and col[0].strip().lower() == "handle":
        col = col[1:]
    return {_norm(h) for h in col if h.strip()}


def _read_run_count_per_handle(spreadsheet) -> dict:
    """
    Walk the Run_Audit tab to count consecutive failed runs per handle.
    Since Run_Audit only stores aggregates (not per-handle), we approximate
    "runs failed in a row" by looking at prior Silent_Failures snapshot only.

    Returns dict: handle → 1 if previously failing, 0 if not (snapshot-based).
    For richer history we'd need a per-handle log; this is intentionally simple.
    """
    prior = _read_prior_silent_failures(spreadsheet)
    return {h: 1 for h in prior}


# ─────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────


def run_audit(
    spreadsheet,
    raw_posts: list,
    events_extracted: int,
    run_started_at: datetime,
    max_ig_checks: int = 30,
    ig_check_delay_sec: float = 2.0,
):
    """
    Run end-of-pipeline audit. Reads the Accounts tab, classifies silent
    failures, writes to Run_Audit (append) and Silent_Failures (overwrite).

    Args:
        spreadsheet:        gspread Spreadsheet object (already opened).
        raw_posts:          The list of post dicts returned by Apify this run.
        events_extracted:   How many events the pipeline extracted overall.
        run_started_at:     Timestamp the run kicked off.
        max_ig_checks:      Cap on IG endpoint calls per audit (rate-limit safety).
        ig_check_delay_sec: Sleep between IG checks.

    Returns:
        Summary dict (also printed to console).
    """
    print("\n" + "=" * 60)
    print("📊 RUN AUDIT")
    print("=" * 60)

    # 1. Read inputs
    inputs = _read_accounts(spreadsheet)
    if not inputs:
        print("⚠ Audit: Accounts tab empty or missing — skipping.")
        return None
    input_set = set(inputs)
    print(f"  Input accounts in tab:        {len(input_set)}")

    # 2. Owners that returned at least one post this run
    returned_owners = {o for p in (raw_posts or []) if (o := _owner_from_post(p))}
    productive_this_run = input_set & returned_owners
    silent_this_run = input_set - returned_owners
    print(f"  Returned posts this run:      {len(productive_this_run)}")
    print(f"  Returned ZERO posts (silent): {len(silent_this_run)}")

    # 3. Classify based on history
    productive_ever = _read_productive_handles(spreadsheet)
    prior_failures = _read_prior_silent_failures(spreadsheet)

    classified = []
    for h in sorted(silent_this_run):
        if h in productive_ever:
            cls = "PRODUCTIVE_BEFORE"
        else:
            cls = "NEVER_PRODUCED"
        runs_in_row = 2 if h in prior_failures else 1
        classified.append({
            "handle": h,
            "classification": cls,
            "runs_failed_in_a_row": runs_in_row,
            "ig_status": "",
            "followers": "",
            "post_count": "",
            "last_checked": "",
        })

    # 4. Identify NEW silent failures (not in last run's snapshot) and
    #    sanity-check them with IG profile endpoint to flag obvious 404s.
    new_failures = [c for c in classified if c["handle"] not in prior_failures]
    persistent_failures = [c for c in classified if c["handle"] in prior_failures]
    print(f"  New since last run:           {len(new_failures)}")
    print(f"  Persistent (failed before):   {len(persistent_failures)}")

    confirmed_404 = 0
    to_check = new_failures[:max_ig_checks]
    if to_check:
        print(f"  Probing {len(to_check)} new failures against IG endpoint...")
    for entry in to_check:
        info = _check_ig_handle_helper(entry["handle"])
        entry["ig_status"] = info["status"]
        entry["followers"] = info["followers"] if info["followers"] is not None else ""
        entry["post_count"] = info["posts"] if info["posts"] is not None else ""
        entry["last_checked"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if info["status"] == "missing":
            entry["classification"] = "IG_404"
            confirmed_404 += 1
        time.sleep(ig_check_delay_sec)

    # 5. Write Run_Audit row (append)
    audit_ws, created = _get_or_create_tab(
        spreadsheet, RUN_AUDIT_TAB, RUN_AUDIT_HEADER, rows=500
    )
    if created:
        print(f"  ✓ Created '{RUN_AUDIT_TAB}' tab")

    notes = ""
    if len(to_check) < len(new_failures):
        notes = f"IG-checked first {len(to_check)} of {len(new_failures)} new failures"

    audit_row = [
        run_started_at.strftime("%Y-%m-%d %H:%M:%S"),
        len(input_set),
        len(productive_this_run),
        len(silent_this_run),
        len(new_failures),
        len(persistent_failures),
        confirmed_404,
        len(raw_posts or []),
        events_extracted,
        notes,
    ]
    audit_ws.append_row(audit_row, value_input_option="USER_ENTERED")
    print(f"  ✓ Appended row to '{RUN_AUDIT_TAB}'")

    # 6. Overwrite Silent_Failures with current snapshot
    sf_ws, sf_created = _get_or_create_tab(
        spreadsheet, SILENT_FAILURES_TAB, SILENT_FAILURES_HEADER,
        rows=max(len(classified) + 50, 100),
    )
    if sf_created:
        print(f"  ✓ Created '{SILENT_FAILURES_TAB}' tab")

    snapshot_rows = [SILENT_FAILURES_HEADER] + [
        [c["handle"], c["classification"], c["runs_failed_in_a_row"],
         c["ig_status"], c["followers"], c["post_count"], c["last_checked"]]
        for c in classified
    ]
    # Pad to clear old rows
    existing_rows = sf_ws.row_count
    while len(snapshot_rows) < existing_rows:
        snapshot_rows.append([""] * len(SILENT_FAILURES_HEADER))
    sf_ws.update("A1", snapshot_rows, value_input_option="USER_ENTERED")
    print(f"  ✓ Overwrote '{SILENT_FAILURES_TAB}' with {len(classified)} rows")

    # 7. Console summary
    summary = {
        "input_count": len(input_set),
        "returned_posts": len(productive_this_run),
        "silent": len(silent_this_run),
        "new_failures": len(new_failures),
        "persistent_failures": len(persistent_failures),
        "confirmed_404": confirmed_404,
    }
    print("\n  Summary:")
    for k, v in summary.items():
        print(f"    {k:<22} {v}")
    print("=" * 60)
    return summary
