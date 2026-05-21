#!/usr/bin/env python3
"""
recover_from_csv_xlsx.py — recover events missing from All_Events.

Scans outputs/Events_*.csv and outputs/Events_*.xlsx (the pipeline's local
ground truth), compares against the live All_Events sheet, and reports
(or applies) events that exist locally but never made it to the sheet.

Also backfills Processed_Log so dedup logic stays consistent for future runs.

By default skips events that match prior purge_events_by_date.py decisions
(reads outputs/purge_events_*.csv). Pass --include-purged to re-add them.

KEY DESIGN CHOICES
──────────────────
- Canonical key: (POST ID, EVENT NAME upper-cased, DATE iso-normalized).
  Same as audit_log_vs_sheet.py and repair_flags_from_log.py.
- When the same key appears in multiple archive files, the LATEST file
  by mtime wins. Reasoning: later runs typically have enriched data
  (venue/city resolved, descriptions written by Gemini).
- CSV row → sheet row mapping is by COLUMN NAME (case-insensitive),
  not by position. Missing columns become blank cells. Extra CSV columns
  are dropped. So this works even if the CSV schema is older than current
  sheet schema (or vice versa).
- Processed_Log backfill uses the source file's mtime as the processed
  date (NOT today). Otherwise the timeline gets corrupted.

USAGE
─────
  # dry-run, see what would be added
  python3 recover_from_csv_xlsx.py

  # only files since a date (e.g., skip pre-May archive)
  python3 recover_from_csv_xlsx.py --since 2026-05-01

  # cap to first 500 — smoke test before doing the full run
  python3 recover_from_csv_xlsx.py --max-events 500 --apply

  # full apply
  python3 recover_from_csv_xlsx.py --apply

  # re-add events you previously deleted (rare; usually you don't want this)
  python3 recover_from_csv_xlsx.py --include-purged

OUTPUT
──────
- outputs/recovery_pending_<ts>.csv  (dry-run mode)
- outputs/recovery_applied_<ts>.csv  (with --apply)

Each row in the output CSV includes a _source_file and _source_mtime
column showing which archive file the recovered row came from.
"""

import argparse
import csv
import sys
import time
from datetime import datetime, date
from pathlib import Path

import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"
PROCESSED_LOG_TAB = "Processed_Log"

OUTPUTS_DIR = Path("outputs")
APPEND_BATCH_SIZE = 200
BATCH_DELAY_SEC = 1.0

DATE_FMTS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y")


def parse_date_iso(s):
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ''
    s = str(s).strip()
    if s in ('', 'None', 'nan', 'NaN'):
        return ''
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return s


def make_key(post_id, name, date_str):
    pid = str(post_id or '').strip()
    nm  = str(name or '').strip().upper()
    dt  = parse_date_iso(date_str)
    return (pid, nm, dt)


def col_index(header, *names):
    for nm in names:
        for i, h in enumerate(header):
            if h.strip().upper() == nm.upper():
                return i
    return None


def load_archive_files(since_iso=None):
    """Return [(path, mtime, kind)] for all Events_*.{csv,xlsx} in outputs/.
    Sorted by mtime ascending so latest writes win when we walk and overwrite."""
    files = []
    for pattern, kind in [("Events_*.csv", "csv"), ("Events_*.xlsx", "xlsx")]:
        for p in OUTPUTS_DIR.glob(pattern):
            mtime = datetime.fromtimestamp(p.stat().st_mtime)
            if since_iso and mtime.date().isoformat() < since_iso:
                continue
            files.append((p, mtime, kind))
    files.sort(key=lambda t: t[1])
    return files


def read_archive_file(path, kind):
    try:
        if kind == "csv":
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
        else:
            df = pd.read_excel(path, dtype=str, engine='openpyxl').fillna('')
        return df.to_dict('records')
    except Exception as e:
        print(f"  ⚠ Could not read {path.name}: {e}", file=sys.stderr)
        return []


def load_purge_keys():
    """Read outputs/purge_events_*.csv (audit trail from purge_events_by_date.py).
    Columns: section, sheet_row, post_id, date, event_name, handle, post_url."""
    keys = set()
    files_read = 0
    for p in OUTPUTS_DIR.glob("purge_events_*.csv"):
        try:
            df = pd.read_csv(p, dtype=str, keep_default_na=False)
            files_read += 1
            for _, row in df.iterrows():
                pid = (row.get('post_id') or '').strip()
                nm  = (row.get('event_name') or '').strip()
                dt  = (row.get('date') or '').strip()
                if pid and nm:
                    keys.add(make_key(pid, nm, dt))
        except Exception as e:
            print(f"  ⚠ Could not read purge audit {p.name}: {e}", file=sys.stderr)
    return keys, files_read


def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def main():
    ap = argparse.ArgumentParser(
        description="Recover events from local CSV/XLSX archive into All_Events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--apply", action="store_true",
                    help="Actually write rows to the sheet. Default: dry-run.")
    ap.add_argument("--since", default=None,
                    help="Only files modified on or after this ISO date (e.g., 2026-05-01).")
    ap.add_argument("--max-events", type=int, default=None,
                    help="Cap the recovery batch size at N events.")
    ap.add_argument("--include-purged", action="store_true",
                    help="Re-add events that match prior purge audit. Default: skip.")
    args = ap.parse_args()

    print(f"━━━ recover_from_csv_xlsx.py ━━━")
    print(f"  Apply mode:       {args.apply}")
    print(f"  Since:            {args.since or '(all files)'}")
    print(f"  Max events:       {args.max_events or '(no cap)'}")
    print(f"  Include purged:   {args.include_purged}")
    print()

    # ── ARCHIVE FILES ─────────────────────────────────────────────────
    archive_files = load_archive_files(since_iso=args.since)
    if not archive_files:
        print("❌ No Events_*.csv or Events_*.xlsx files in outputs/.", file=sys.stderr)
        sys.exit(1)
    print(f"  Loading {len(archive_files)} archive files...")

    by_key = {}  # canonical key → (mtime, row_dict, source_file_name)
    files_read = 0
    rows_total = 0
    for path, mtime, kind in archive_files:
        rows = read_archive_file(path, kind)
        if not rows:
            continue
        files_read += 1
        rows_total += len(rows)
        for row in rows:
            pid = (row.get('POST ID') or '').strip()
            nm  = (row.get('EVENT NAME') or '').strip()
            dt  = (row.get('DATE') or '').strip()
            if not pid or not nm:
                continue
            by_key[make_key(pid, nm, dt)] = (mtime, row, path.name)

    print(f"    {files_read} files parsed, {rows_total:,} total rows, "
          f"{len(by_key):,} unique events (by POST ID × EVENT NAME × DATE)")
    print()

    # ── PURGE AUDIT ───────────────────────────────────────────────────
    if args.include_purged:
        purged = set()
        print(f"  Including purged events (--include-purged)")
    else:
        purged, purge_files = load_purge_keys()
        if purge_files:
            print(f"  Loaded {len(purged):,} purged keys from {purge_files} purge audit file(s)")
        else:
            print(f"  No purge_events_*.csv files in outputs/ — nothing to skip")
    print()

    # ── SHEET STATE ───────────────────────────────────────────────────
    print(f"  Connecting to sheet...")
    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)
    print(f"  Reading {ALL_EVENTS_TAB}...")
    all_values = ws.get_all_values()
    if not all_values:
        print(f"❌ {ALL_EVENTS_TAB} is empty.", file=sys.stderr)
        sys.exit(1)
    header = all_values[0]
    data_rows = all_values[1:]

    pid_col = col_index(header, 'POST ID')
    name_col = col_index(header, 'EVENT NAME')
    date_col = col_index(header, 'DATE')
    if None in (pid_col, name_col, date_col):
        print(f"❌ Missing required columns in {ALL_EVENTS_TAB} "
              f"(need POST ID, EVENT NAME, DATE).", file=sys.stderr)
        sys.exit(1)

    sheet_keys = set()
    for row in data_rows:
        if len(row) <= max(pid_col, name_col, date_col):
            continue
        sheet_keys.add(make_key(row[pid_col], row[name_col], row[date_col]))
    print(f"    {len(sheet_keys):,} existing keys in sheet")
    print()

    # Processed_Log
    print(f"  Reading {PROCESSED_LOG_TAB}...")
    try:
        pl_ws = sh.worksheet(PROCESSED_LOG_TAB)
        pl_values = pl_ws.get_all_values()
    except gspread.WorksheetNotFound:
        print(f"  ⚠ {PROCESSED_LOG_TAB} not found — backfill will be skipped.")
        pl_ws = None
        pl_values = []

    pl_header = pl_values[0] if pl_values else []
    pl_pid_col = col_index(pl_header, 'POST ID') if pl_header else None
    pl_result_col = col_index(pl_header, 'RESULT', 'STATUS') if pl_header else None
    pl_date_col = col_index(pl_header, 'DATE PROCESSED', 'PROCESSED AT', 'DATE') if pl_header else None
    pl_post_ids = set()
    if pl_values and pl_pid_col is not None:
        for row in pl_values[1:]:
            if len(row) > pl_pid_col:
                pid = (row[pl_pid_col] or '').strip()
                if pid:
                    pl_post_ids.add(pid)
    print(f"    {len(pl_post_ids):,} existing post IDs in {PROCESSED_LOG_TAB}")
    print()

    # ── DELTA ─────────────────────────────────────────────────────────
    candidates = []   # [(key, mtime, row_dict, source_file)]
    skipped_in_sheet = 0
    skipped_purged = 0
    # Sort by mtime DESC so the newest events are processed first
    # (matters when --max-events is set)
    for key, (mtime, row_dict, source_file) in sorted(
        by_key.items(), key=lambda kv: kv[1][0], reverse=True
    ):
        if key in sheet_keys:
            skipped_in_sheet += 1
            continue
        if key in purged:
            skipped_purged += 1
            continue
        candidates.append((key, mtime, row_dict, source_file))

    if args.max_events:
        candidates = candidates[:args.max_events]

    candidate_post_ids = {k[0] for k, _, _, _ in candidates}
    new_to_pl = candidate_post_ids - pl_post_ids

    print(f"━━━ DELTA ━━━")
    print(f"  Archive unique events:        {len(by_key):>7,}")
    print(f"  Already in sheet:             {skipped_in_sheet:>7,}")
    print(f"  Skipped (purged):             {skipped_purged:>7,}")
    print(f"  → Recoverable events:         {len(candidates):>7,}")
    print(f"  → New post_ids for {PROCESSED_LOG_TAB}: {len(new_to_pl):>7,}")
    print()

    # ── DRY-RUN / APPLIED CSV ─────────────────────────────────────────
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = "applied" if args.apply else "pending"
    out_csv = OUTPUTS_DIR / f"recovery_{suffix}_{ts}.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    out_fieldnames = list(header) + ['_source_file', '_source_mtime']
    with open(out_csv, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=out_fieldnames, extrasaction='ignore')
        w.writeheader()
        for key, mtime, row_dict, source_file in candidates:
            out_row = {col: row_dict.get(col, '') for col in header}
            out_row['_source_file'] = source_file
            out_row['_source_mtime'] = mtime.isoformat()
            w.writerow(out_row)
    print(f"  CSV written: {out_csv}")
    print()

    if not candidates:
        print(f"  OK: nothing to recover. Sheet is in sync with the archive.")
        return

    if not args.apply:
        print(f"  [dry-run] No sheet changes made. Re-run with --apply to commit.")
        return

    # ── APPLY: append rows to All_Events ──────────────────────────────
    print(f"━━━ APPLYING ━━━")
    print(f"  Appending {len(candidates):,} rows to {ALL_EVENTS_TAB}...")

    rows_to_append = [
        [row_dict.get(col, '') for col in header]
        for (_, _, row_dict, _) in candidates
    ]

    for start in range(0, len(rows_to_append), APPEND_BATCH_SIZE):
        chunk = rows_to_append[start:start + APPEND_BATCH_SIZE]
        ws.append_rows(chunk, value_input_option='USER_ENTERED')
        end = start + len(chunk)
        print(f"    Appended {end:,} / {len(rows_to_append):,}")
        if end < len(rows_to_append):
            time.sleep(BATCH_DELAY_SEC)

    # ── APPLY: backfill Processed_Log ─────────────────────────────────
    if pl_ws is None:
        print(f"  Skipped {PROCESSED_LOG_TAB} backfill (tab not found).")
    elif not new_to_pl:
        print(f"  No new post_ids for {PROCESSED_LOG_TAB} — all candidate "
              f"post_ids already present.")
    else:
        print(f"  Backfilling {len(new_to_pl):,} post IDs into {PROCESSED_LOG_TAB}...")
        # Per-post: use the latest mtime among that post's candidate events.
        post_mtime = {}
        for key, mtime, _, _ in candidates:
            pid = key[0]
            if pid not in post_mtime or mtime > post_mtime[pid]:
                post_mtime[pid] = mtime

        pl_rows = []
        for pid in sorted(new_to_pl):
            r = [''] * len(pl_header)
            if pl_pid_col is not None:
                r[pl_pid_col] = pid
            if pl_result_col is not None:
                r[pl_result_col] = 'events_found'
            if pl_date_col is not None:
                r[pl_date_col] = post_mtime[pid].date().isoformat()
            pl_rows.append(r)

        for start in range(0, len(pl_rows), APPEND_BATCH_SIZE):
            chunk = pl_rows[start:start + APPEND_BATCH_SIZE]
            pl_ws.append_rows(chunk, value_input_option='USER_ENTERED')
            end = start + len(chunk)
            print(f"    Backfilled {end:,} / {len(pl_rows):,}")
            if end < len(pl_rows):
                time.sleep(BATCH_DELAY_SEC)

    print(f"\n  OK: recovery complete. Audit trail: {out_csv}")


if __name__ == "__main__":
    main()
