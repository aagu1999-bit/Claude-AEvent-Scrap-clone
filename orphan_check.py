#!/usr/bin/env python3
"""
orphan_check.py — detect Processed_Log entries marked `events_found`
that have no corresponding rows in All_Events.

WHAT IS AN ORPHAN?
──────────────────
A post is "orphaned" when:
  1. Processed_Log says: result=events_found  (the bot extracted events)
  2. All_Events has: zero rows with that POST ID  (events never persisted)

This is the exact failure mode that caused today's 1,843 orphans —
incremental Processed_Log flushes happened but the events were
never written to All_Events before the run died.

WHEN TO RUN
───────────
- After every production cron run (within 1 minute, post-cron)
- Before merging PRs that touch the flush logic (compare baseline ↔ post-PR)
- Periodically as a health check

USAGE
─────
  python orphan_check.py                  # full scan, prints summary + dumps CSV
  python orphan_check.py --quiet          # CSV-only output (for cron post-hooks)
  python orphan_check.py --since 18000    # only scan PL rows from row N onward
                                          # (useful when running on a partial cron)

OUTPUT
──────
- Console: tally + sample of orphan pids
- File: outputs/orphan_check_<ts>.csv with one row per orphan:
    post_id, account, date_processed, result_tag, post_date

This is READ-ONLY. Never modifies your sheets.
"""

import argparse
import csv
import sys
import time
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
PROCESSED_LOG_TAB = "Processed_Log"
ALL_EVENTS_TAB = "All_Events"
OUTPUT_DIR = Path("outputs")


def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found: {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def main():
    p = argparse.ArgumentParser(
        description="Detect Processed_Log entries with no matching All_Events rows.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--quiet", action="store_true",
                   help="Suppress per-pid console output (CSV still written)")
    p.add_argument("--since", type=int, default=None,
                   help="Only scan Processed_Log rows from this row number onward")
    args = p.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    if not args.quiet:
        print(f"━━━ orphan_check.py ━━━")
        print(f"  Quiet mode:  {args.quiet}")
        print(f"  Since row:   {args.since or '(all)'}")
        print()

    sh = setup_sheet()

    # ─── Step 1: read Processed_Log, collect events_found pids ────────────
    t_pl = time.time()
    try:
        pl = sh.worksheet(PROCESSED_LOG_TAB)
    except gspread.WorksheetNotFound:
        print(f"❌ '{PROCESSED_LOG_TAB}' tab not found", file=sys.stderr)
        sys.exit(1)

    pl_data = pl.get_all_values()
    pl_header = pl_data[0] if pl_data else []
    if 'Post ID' not in pl_header:
        print(f"❌ '{PROCESSED_LOG_TAB}' missing 'Post ID' column", file=sys.stderr)
        sys.exit(1)

    pid_col = pl_header.index('Post ID')
    result_col = pl_header.index('Result') if 'Result' in pl_header else None
    acct_col = pl_header.index('Account') if 'Account' in pl_header else None
    date_col = pl_header.index('Date Processed') if 'Date Processed' in pl_header else None
    post_date_col = pl_header.index('Post Date') if 'Post Date' in pl_header else None

    if not args.quiet:
        print(f"  Read {len(pl_data)-1} Processed_Log rows in {time.time()-t_pl:.1f}s")

    # Build pid → {result, account, date_processed, post_date} for events_found rows.
    # If a pid appears multiple times (shouldn't, but…), the LAST entry wins
    # since that's the most recent status.
    pl_events_found = {}
    skipped_below_since = 0
    for i, row in enumerate(pl_data[1:], start=2):
        if args.since and i < args.since:
            skipped_below_since += 1
            continue
        if not row or len(row) <= pid_col:
            continue
        pid = (row[pid_col] or '').strip()
        if not pid:
            continue
        result = (row[result_col] or '').strip().lower() if result_col is not None and len(row) > result_col else ''
        if result != 'events_found':
            continue
        pl_events_found[pid] = {
            'result':         result,
            'account':        row[acct_col].strip() if acct_col is not None and len(row) > acct_col else '',
            'date_processed': row[date_col].strip() if date_col is not None and len(row) > date_col else '',
            'post_date':      row[post_date_col].strip() if post_date_col is not None and len(row) > post_date_col else '',
        }

    if not args.quiet and skipped_below_since:
        print(f"  Skipped {skipped_below_since} PL rows below --since={args.since}")

    # ─── Step 2: read All_Events, collect set of pids with at least 1 event ───
    t_ae = time.time()
    try:
        ae = sh.worksheet(ALL_EVENTS_TAB)
    except gspread.WorksheetNotFound:
        print(f"❌ '{ALL_EVENTS_TAB}' tab not found", file=sys.stderr)
        sys.exit(1)

    ae_data = ae.get_all_values()
    ae_header = ae_data[0] if ae_data else []
    if 'POST ID' not in ae_header:
        print(f"❌ '{ALL_EVENTS_TAB}' missing 'POST ID' column", file=sys.stderr)
        sys.exit(1)

    ae_pid_col = ae_header.index('POST ID')
    ae_pids = set()
    for row in ae_data[1:]:
        if not row or len(row) <= ae_pid_col:
            continue
        pid = (row[ae_pid_col] or '').strip()
        if pid:
            ae_pids.add(pid)

    if not args.quiet:
        print(f"  Read {len(ae_data)-1} All_Events rows in {time.time()-t_ae:.1f}s")
        print(f"  Distinct POST IDs in All_Events: {len(ae_pids)}")
        print()

    # ─── Step 3: compute orphans ─────────────────────────────────────────
    orphans = {pid: meta for pid, meta in pl_events_found.items() if pid not in ae_pids}

    print(f"━━━ FINDINGS ━━━")
    print(f"  Processed_Log entries marked events_found:  {len(pl_events_found)}")
    print(f"  Of those, present in All_Events:            {len(pl_events_found) - len(orphans)}")
    print(f"  ORPHANS (events_found in PL, missing in AE): {len(orphans)}")
    if pl_events_found:
        rate = 100.0 * len(orphans) / len(pl_events_found)
        print(f"  Orphan rate: {rate:.2f}%")
    print()

    # ─── Step 4: dump CSV (always — even if zero orphans, for record) ────
    csv_path = OUTPUT_DIR / f'orphan_check_{ts}.csv'
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['post_id', 'account', 'date_processed', 'result_tag', 'post_date',
                    'post_url'])
        for pid, meta in sorted(orphans.items()):
            w.writerow([
                pid, meta['account'], meta['date_processed'],
                meta['result'], meta['post_date'],
                f"https://www.instagram.com/p/{pid}/",
            ])
    print(f"✓ Wrote {csv_path}")

    # ─── Step 5: sample for console ──────────────────────────────────────
    if orphans and not args.quiet:
        sample = sorted(orphans.items())[:20]
        print(f"\n  Sample of orphan pids (first 20):")
        for pid, meta in sample:
            acct = meta['account'] or '?'
            print(f"    {pid:18}  @{acct:20}  {meta['post_date']}")
        if len(orphans) > 20:
            print(f"    ... and {len(orphans) - 20} more (see CSV)")

    # ─── Step 6: exit code reflects state — useful for cron post-hooks ───
    if orphans:
        print(f"\n⚠ Orphans detected. To recover, run:")
        print(f"    python events_from_ids.py --from-ids {csv_path}")
        sys.exit(2)
    else:
        print(f"\n✓ No orphans. Processed_Log and All_Events are in sync.")
        sys.exit(0)


if __name__ == "__main__":
    main()
