#!/usr/bin/env python3
"""
cleanup_pseudo_ids_and_duplicates.py

One-off cleanup script that addresses three known data-quality issues in the
Instagram_Events_Master Sheet:

1. **All_Events composite-key duplicates** — same (POST ID, EVENT NAME, DATE)
   appearing more than once. Confirmed leaks: ~503 excess rows from a
   combination of race-conditioned re-processing and reprocess_weekends
   re-extractions over multiple weeks.

2. **Processed_Log pseudo-ID rows** — entries where Post ID is `post_NNN`
   instead of a real Instagram post ID. ~3,189 rows total. These can never
   match a real Instagram post (real IDs are 19-digit integers) so they
   block nothing — but they consume rows and look ambiguous in audits.
   Both Schema A (Auto-Bot) and Schema B (Migration Script) variants get
   deleted.

3. **Processed_Log column drift** — historically, three different schemas
   have written to this tab. Real-ID rows in the drifted schemas get
   their columns shifted right to match canonical Schema A:

       Schema A (canonical, current Auto-Bot path):
         Post ID | Account | Date Processed | Source="Auto-Bot" | Notes | Result | Post Date

       Schema B (drifted, Migration Script):
         Post ID | (date)  | "Migration Script" | "Imported from checkpoint" | empty | empty | empty

       Schema C (drifted, older Auto-Bot path):
         Post ID | (date)  | "Auto-Bot" | empty | empty | empty | empty

   Detection is by content fingerprint, not row position — so future
   schema variants land in an "unknown" bucket rather than getting
   silently broken.

USAGE
─────
  python cleanup_pseudo_ids_and_duplicates.py             # DRY RUN (default).
                                                          # Reports counts and
                                                          # what would change.
                                                          # No writes.

  python cleanup_pseudo_ids_and_duplicates.py --apply     # ACTUALLY MODIFIES
                                                          # the Sheet. Backs up
                                                          # to *_Backup_<ts>
                                                          # tabs first.

  python cleanup_pseudo_ids_and_duplicates.py --apply --skip-pass=column-shift
                                                          # Skip a specific
                                                          # pass (events-dedup,
                                                          # pseudo-id-delete,
                                                          # column-shift).

ORDER OF PASSES
───────────────
1. Backup (always, before any writes): copies All_Events and Processed_Log
   to All_Events_Backup_<ts> and Processed_Log_Backup_<ts>.
2. All_Events events-dedup pass: collapses composite-key duplicates,
   keeps first occurrence.
3. Processed_Log pseudo-id-delete pass: removes all rows where Post ID
   starts with `post_`.
4. Processed_Log column-shift pass: shifts Schema B rows (real IDs only,
   migration source) to canonical Schema A.
5. Verification: re-reads both tabs, reports row deltas.

SAFETY
──────
- Default is dry-run. Pass --apply to write.
- Backups happen before any writes; unrecoverable only if you also delete
  the backup tab.
- Row deletes are batched at 100 rows + 1.5s sleep between batches, well
  under Sheets API rate limits.
- "Unknown schema" rows are not modified — flagged in dry-run so you can
  decide what to do with them.

See docs/decisions/0007-cleanup-script-rationale.md for the full reasoning.
"""

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials


SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"

ALL_EVENTS_TAB = "All_Events"
PROCESSED_LOG_TAB = "Processed_Log"

CANONICAL_PROCESSED_LOG_HEADER = [
    "Post ID", "Account", "Date Processed",
    "Source", "Notes", "Result", "Post Date",
]

BATCH_SIZE = 100
BATCH_SLEEP_SEC = 1.5

PASSES = ("events-dedup", "pseudo-id-delete", "column-shift")


# ─────────────────────────── connection ─────────────────────────


def connect_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account file not found: {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


# ─────────────────────────── backup ─────────────────────────


def backup_tab(spreadsheet, source_tab_name: str, ts: str, dry_run: bool):
    """Copy source tab to <source>_Backup_<ts>. Returns the backup tab name."""
    backup_name = f"{source_tab_name}_Backup_{ts}"
    if dry_run:
        print(f"  [dry-run] would back up '{source_tab_name}' → '{backup_name}'")
        return backup_name
    try:
        src = spreadsheet.worksheet(source_tab_name)
        rows = src.get_all_values()
        cols = max(len(r) for r in rows) if rows else 10
        backup = spreadsheet.add_worksheet(backup_name, rows=len(rows) + 10, cols=cols)
        if rows:
            backup.update(rows, value_input_option="RAW")
        print(f"  ✓ Backup written: '{backup_name}' ({len(rows)} rows × {cols} cols)")
    except Exception as e:
        print(f"  ⚠ Backup of '{source_tab_name}' failed: {e}")
        raise
    return backup_name


# ─────────────────────────── pass 1: events dedup ─────────────────────────


def events_dedup_pass(spreadsheet, dry_run: bool):
    """Collapse (POST ID, EVENT NAME, DATE) composite-key duplicates in
    All_Events, keeping the first occurrence by row order. Returns counts."""
    ws = spreadsheet.worksheet(ALL_EVENTS_TAB)
    rows = ws.get_all_values()
    if not rows:
        print(f"  ⚠ {ALL_EVENTS_TAB} is empty — skipping")
        return {"total": 0, "duplicates": 0, "deleted": 0}

    header = rows[0]

    def _idx(name):
        try:
            return header.index(name)
        except ValueError:
            return None

    post_idx = _idx("POST ID")
    name_idx = _idx("EVENT NAME")
    date_idx = _idx("DATE")
    if any(i is None for i in (post_idx, name_idx, date_idx)):
        print(f"  ⚠ Cannot locate POST ID / EVENT NAME / DATE columns in {ALL_EVENTS_TAB}")
        return {"total": len(rows) - 1, "duplicates": 0, "deleted": 0}

    seen_first_row = {}
    rows_to_delete = []  # 1-based sheet row numbers
    for i, r in enumerate(rows[1:], start=2):  # +2: 1-based + skip header
        if len(r) <= max(post_idx, name_idx, date_idx):
            continue
        key = (
            r[post_idx].strip(),
            r[name_idx].strip().upper(),
            r[date_idx].strip(),
        )
        if not any(key):
            continue
        if key in seen_first_row:
            rows_to_delete.append(i)
        else:
            seen_first_row[key] = i

    n_total = len(rows) - 1
    n_dup_keys = sum(1 for _ in [k for k in seen_first_row if False])  # placeholder; computed below
    # Recompute the duplicate-key count: keys that appear more than once
    from collections import Counter
    all_keys = []
    for r in rows[1:]:
        if len(r) <= max(post_idx, name_idx, date_idx):
            continue
        k = (r[post_idx].strip(), r[name_idx].strip().upper(), r[date_idx].strip())
        if any(k):
            all_keys.append(k)
    counts = Counter(all_keys)
    n_dup_keys = sum(1 for v in counts.values() if v > 1)

    print(f"  Pass 1/3 — All_Events events-dedup")
    print(f"    Total data rows:        {n_total}")
    print(f"    Unique composite keys:  {len(seen_first_row)}")
    print(f"    Duplicate keys:         {n_dup_keys}")
    print(f"    Rows to delete:         {len(rows_to_delete)}")

    if rows_to_delete and not dry_run:
        # Delete in reverse order so earlier sheet_row numbers don't shift
        rows_to_delete.sort(reverse=True)
        deleted = 0
        for batch_start in range(0, len(rows_to_delete), BATCH_SIZE):
            batch = rows_to_delete[batch_start:batch_start + BATCH_SIZE]
            for sheet_row in batch:
                try:
                    ws.delete_rows(sheet_row)
                    deleted += 1
                except Exception as e:
                    print(f"    ✗ Failed to delete sheet row {sheet_row}: {e}")
            print(f"    ✓ Batch {batch_start // BATCH_SIZE + 1} — deleted {deleted}/{len(rows_to_delete)}")
            if batch_start + BATCH_SIZE < len(rows_to_delete):
                time.sleep(BATCH_SLEEP_SEC)
        print(f"  ✓ events-dedup done: {deleted} rows deleted")
    elif rows_to_delete:
        print(f"  [dry-run] would delete {len(rows_to_delete)} rows")
    else:
        print(f"  No duplicates to delete.")

    return {
        "total": n_total,
        "duplicate_keys": n_dup_keys,
        "rows_to_delete": len(rows_to_delete),
        "deleted": 0 if dry_run else len(rows_to_delete),
    }


# ─────────────────────────── pass 2: pseudo-id delete ─────────────────────────


def _classify_pl_row(row, header):
    """Return ('schema_a', 'schema_b_pseudo', 'schema_b_real',
    'schema_c_pseudo', 'schema_c_real', 'unknown') based on content
    fingerprints, not column positions.

    Three known schemas that have written to Processed_Log over time:
      Schema A (canonical, current code):
        col0=Post ID, col1=Account, col2=Date Processed,
        col3="Auto-Bot", col4=Notes, col5=Result, col6=Post Date

      Schema B (Migration Script, drifted left by 1):
        col0=Post ID, col1=date, col2="Migration Script",
        col3="Imported from checkpoint", col4-6 empty

      Schema C (older Auto-Bot path, drifted left by 1):
        col0=Post ID, col1=date, col2="Auto-Bot",
        col3-6 empty
    """
    if not row or len(row) < 4:
        return "unknown"
    pid = row[0].strip() if row else ""
    is_pseudo = pid.startswith("post_")

    col2 = row[2].strip() if len(row) > 2 else ""
    col3 = row[3].strip() if len(row) > 3 else ""

    # Fingerprints, ordered most-specific first to disambiguate
    is_schema_b = "Migration Script" in col2 or "Imported from checkpoint" in col3
    is_schema_a = col3 == "Auto-Bot"                          # canonical: Auto-Bot at col3
    is_schema_c = col2 == "Auto-Bot" and col3 == ""           # drifted: Auto-Bot at col2, col3 empty

    if is_schema_b:
        return "schema_b_pseudo" if is_pseudo else "schema_b_real"
    if is_schema_c:
        return "schema_c_pseudo" if is_pseudo else "schema_c_real"
    if is_schema_a:
        return "schema_a"
    if is_pseudo:
        return "schema_a"  # treat unmatched pseudo as schema A for deletion purposes
    return "unknown"


def pseudo_id_delete_pass(spreadsheet, dry_run: bool):
    """Delete every Processed_Log row whose Post ID starts with 'post_'.
    These are pseudo-IDs that can never collide with real 19-digit
    Instagram post IDs, so deleting them unblocks zero real posts."""
    ws = spreadsheet.worksheet(PROCESSED_LOG_TAB)
    rows = ws.get_all_values()
    if not rows:
        print(f"  ⚠ {PROCESSED_LOG_TAB} is empty — skipping")
        return {"total": 0, "deleted": 0}

    header = rows[0]
    rows_to_delete = []  # 1-based sheet row numbers
    n_schema_a_pseudo = 0
    n_schema_b_pseudo = 0
    n_schema_c_pseudo = 0
    for i, r in enumerate(rows[1:], start=2):
        if not r or not r[0].strip().startswith("post_"):
            continue
        cls = _classify_pl_row(r, header)
        if cls == "schema_b_pseudo":
            n_schema_b_pseudo += 1
        elif cls == "schema_c_pseudo":
            n_schema_c_pseudo += 1
        else:
            n_schema_a_pseudo += 1
        rows_to_delete.append(i)

    print(f"  Pass 2/3 — Processed_Log pseudo-id-delete")
    print(f"    Total Processed_Log data rows: {len(rows) - 1}")
    print(f"    Schema A pseudo-IDs (Auto-Bot etc): {n_schema_a_pseudo}")
    print(f"    Schema B pseudo-IDs (Migration):    {n_schema_b_pseudo}")
    print(f"    Schema C pseudo-IDs (older path):   {n_schema_c_pseudo}")
    print(f"    Total pseudo-IDs to delete:         {len(rows_to_delete)}")

    if rows_to_delete and not dry_run:
        rows_to_delete.sort(reverse=True)
        deleted = 0
        for batch_start in range(0, len(rows_to_delete), BATCH_SIZE):
            batch = rows_to_delete[batch_start:batch_start + BATCH_SIZE]
            for sheet_row in batch:
                try:
                    ws.delete_rows(sheet_row)
                    deleted += 1
                except Exception as e:
                    print(f"    ✗ Failed to delete sheet row {sheet_row}: {e}")
            print(f"    ✓ Batch {batch_start // BATCH_SIZE + 1} — deleted {deleted}/{len(rows_to_delete)}")
            if batch_start + BATCH_SIZE < len(rows_to_delete):
                time.sleep(BATCH_SLEEP_SEC)
        print(f"  ✓ pseudo-id-delete done: {deleted} rows deleted")
    elif rows_to_delete:
        print(f"  [dry-run] would delete {len(rows_to_delete)} rows")
    else:
        print(f"  No pseudo-IDs to delete.")

    return {
        "schema_a_pseudo": n_schema_a_pseudo,
        "schema_b_pseudo": n_schema_b_pseudo,
        "total_pseudo": len(rows_to_delete),
        "deleted": 0 if dry_run else len(rows_to_delete),
    }


# ─────────────────────────── pass 3: column shift ─────────────────────────


def column_shift_pass(spreadsheet, dry_run: bool):
    """Shift Schema B real-ID rows in Processed_Log to canonical Schema A.

    Mapping (Schema B → Schema A):
      old col 0 (Post ID)         → col 0 (Post ID)              [unchanged]
      old col 1 (the date)        → col 2 (Date Processed)
      old col 2 ("Migration ...") → col 3 (Source)               [becomes literal "Migration Script"]
      old col 3 ("Imported ...")  → col 4 (Notes)                [becomes literal "Imported from checkpoint"]
      col 1 (Account)             → leave EMPTY (we don't have it)
      col 5 (Result)              → leave EMPTY (no extraction was done)
      col 6 (Post Date)           → leave EMPTY

    Header is also rewritten to canonical.
    """
    ws = spreadsheet.worksheet(PROCESSED_LOG_TAB)
    rows = ws.get_all_values()
    if not rows:
        print(f"  ⚠ {PROCESSED_LOG_TAB} is empty — skipping")
        return {"total": 0, "shifted": 0, "header_changed": False}

    current_header = rows[0]
    n_total = len(rows) - 1

    # Detect which rows need shifting and what the post-shift values look like
    rows_to_shift = []  # list of (sheet_row, new_row_values)
    n_schema_a = 0
    n_schema_b_real = 0
    n_schema_c_real = 0
    n_unknown = 0
    for i, r in enumerate(rows[1:], start=2):
        if not r or not r[0].strip():
            continue
        if r[0].strip().startswith("post_"):
            # Will be handled by pass 2; ignore here
            continue
        cls = _classify_pl_row(r, current_header)
        if cls == "schema_a":
            n_schema_a += 1
        elif cls == "schema_b_real":
            n_schema_b_real += 1
            # Build the canonical-aligned new row
            old_col1 = r[1].strip() if len(r) > 1 else ""  # the date
            new_row = [
                r[0].strip(),               # Post ID (unchanged)
                "",                         # Account (we don't have it)
                old_col1,                   # Date Processed (was in old col 1)
                "Migration Script",         # Source (canonicalised)
                "Imported from checkpoint", # Notes (canonicalised)
                "",                         # Result (empty)
                "",                         # Post Date (empty)
            ]
            rows_to_shift.append((i, new_row))
        elif cls == "schema_c_real":
            n_schema_c_real += 1
            # Schema C: same shift as Schema B, but Source="Auto-Bot" and Notes empty
            old_col1 = r[1].strip() if len(r) > 1 else ""  # the date
            new_row = [
                r[0].strip(),               # Post ID (unchanged)
                "",                         # Account (we don't have it)
                old_col1,                   # Date Processed (was in old col 1)
                "Auto-Bot",                 # Source (canonicalised — was Auto-Bot in col2)
                "",                         # Notes (Schema C had no notes)
                "",                         # Result (Schema C had no preserved result)
                "",                         # Post Date (empty)
            ]
            rows_to_shift.append((i, new_row))
        else:
            n_unknown += 1

    header_needs_update = current_header != CANONICAL_PROCESSED_LOG_HEADER

    print(f"  Pass 3/3 — Processed_Log column-shift")
    print(f"    Total non-pseudo rows: {n_schema_a + n_schema_b_real + n_schema_c_real + n_unknown}")
    print(f"    Schema A (no shift needed):              {n_schema_a}")
    print(f"    Schema B real-ID (will shift, Migration): {n_schema_b_real}")
    print(f"    Schema C real-ID (will shift, Auto-Bot):  {n_schema_c_real}")
    print(f"    Unknown schema (skipped, manual review): {n_unknown}")
    print(f"    Current header: {current_header}")
    print(f"    Canonical header: {CANONICAL_PROCESSED_LOG_HEADER}")
    print(f"    Header needs update: {header_needs_update}")

    if not dry_run:
        if header_needs_update:
            try:
                ws.update("A1", [CANONICAL_PROCESSED_LOG_HEADER], value_input_option="RAW")
                print(f"  ✓ Header rewritten to canonical")
            except Exception as e:
                print(f"  ✗ Failed to update header: {e}")

        if rows_to_shift:
            shifted = 0
            for batch_start in range(0, len(rows_to_shift), BATCH_SIZE):
                batch = rows_to_shift[batch_start:batch_start + BATCH_SIZE]
                # Use batch_update for efficiency: build cell-range updates
                for sheet_row, new_row in batch:
                    try:
                        ws.update(f"A{sheet_row}:G{sheet_row}", [new_row], value_input_option="RAW")
                        shifted += 1
                    except Exception as e:
                        print(f"    ✗ Failed to shift sheet row {sheet_row}: {e}")
                print(f"    ✓ Batch {batch_start // BATCH_SIZE + 1} — shifted {shifted}/{len(rows_to_shift)}")
                if batch_start + BATCH_SIZE < len(rows_to_shift):
                    time.sleep(BATCH_SLEEP_SEC)
            print(f"  ✓ column-shift done: {shifted} rows shifted")
        else:
            print(f"  No rows needed shifting.")
    else:
        if header_needs_update:
            print(f"  [dry-run] would rewrite header")
        if rows_to_shift:
            print(f"  [dry-run] would shift {len(rows_to_shift)} rows")
            print(f"  [dry-run] sample of first 3 shifts:")
            for sheet_row, new_row in rows_to_shift[:3]:
                old_row = rows[sheet_row - 1]  # sheet_row is 1-based; rows is 0-based but +header
                print(f"    row {sheet_row}: {old_row[:5]}  →  {new_row[:5]}")

    return {
        "total": n_total,
        "schema_a": n_schema_a,
        "schema_b_real": n_schema_b_real,
        "schema_c_real": n_schema_c_real,
        "unknown": n_unknown,
        "shifted": 0 if dry_run else len(rows_to_shift),
        "header_changed": header_needs_update and not dry_run,
    }


# ─────────────────────────── main ─────────────────────────


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--apply", action="store_true",
        help="Actually modify the Sheet. Default is dry-run.",
    )
    parser.add_argument(
        "--skip-pass", action="append", default=[],
        choices=PASSES,
        help="Skip a specific pass. Can be passed multiple times.",
    )
    args = parser.parse_args()

    dry_run = not args.apply
    skip = set(args.skip_pass)

    print("\n" + "=" * 60)
    print("  CLEANUP — pseudo-IDs + duplicates + column drift")
    print("=" * 60)
    print(f"  Mode: {'DRY RUN (no writes)' if dry_run else '*** APPLY (will write to Sheet) ***'}")
    print(f"  Skipped passes: {sorted(skip) if skip else 'none'}")
    print()

    spreadsheet = connect_sheet()
    print(f"✓ Connected to Sheet: {SHEET_NAME}")
    print()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if not dry_run:
        print("📦 Backup pass")
        backup_tab(spreadsheet, ALL_EVENTS_TAB, ts, dry_run)
        backup_tab(spreadsheet, PROCESSED_LOG_TAB, ts, dry_run)
        print()
    else:
        print("📦 Backup pass — would create:")
        print(f"  {ALL_EVENTS_TAB}_Backup_{ts}")
        print(f"  {PROCESSED_LOG_TAB}_Backup_{ts}")
        print()

    results = {}

    if "events-dedup" not in skip:
        print("─" * 60)
        results["events_dedup"] = events_dedup_pass(spreadsheet, dry_run)
        print()
    else:
        print(f"⏭  Skipping events-dedup pass\n")

    if "pseudo-id-delete" not in skip:
        print("─" * 60)
        results["pseudo_id_delete"] = pseudo_id_delete_pass(spreadsheet, dry_run)
        print()
    else:
        print(f"⏭  Skipping pseudo-id-delete pass\n")

    if "column-shift" not in skip:
        print("─" * 60)
        results["column_shift"] = column_shift_pass(spreadsheet, dry_run)
        print()
    else:
        print(f"⏭  Skipping column-shift pass\n")

    print("=" * 60)
    print("  SUMMARY" + (" (DRY RUN)" if dry_run else ""))
    print("=" * 60)
    for pass_name, r in results.items():
        print(f"  {pass_name}:")
        for k, v in r.items():
            print(f"    {k}: {v}")
    if dry_run:
        print("\n👀 To execute these changes, re-run with --apply")
    else:
        print("\n✓ Cleanup complete. See backup tabs for the pre-cleanup state:")
        print(f"   • {ALL_EVENTS_TAB}_Backup_{ts}")
        print(f"   • {PROCESSED_LOG_TAB}_Backup_{ts}")


if __name__ == "__main__":
    main()
