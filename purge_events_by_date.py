#!/usr/bin/env python3
"""
purge_events_by_date.py — delete event rows from All_Events whose DATE falls
in one or more target windows, then drop the affected POST IDs from
Processed_Log so the pipeline can re-extract them on its next run.

USE CASE
────────
You want to wipe out a specific weekend's (or any date range's) extracted
events without running the full re-scrape + re-extract pipeline. After
running this with --apply, the next pipeline run will re-extract those
posts fresh because they are no longer in Processed_Log.

DELETE SCOPE
────────────
STRICT — only rows whose DATE falls in a target window are deleted. Rows
from the same post but on other dates are LEFT IN PLACE. (Different from
reprocess_weekends.py, which sweeps every row sharing a post_id.)

Caveat: if a post has events both inside and outside your window, removing
its post_id from Processed_Log lets the pipeline re-extract the whole post
on next run, which can reinsert duplicates of the surviving rows. The
script warns when this case is detected. Use dedup_all_events.py afterward
if needed.

USAGE
─────
  # Default: next 2 weekends, dry-run preview
  python purge_events_by_date.py

  # Explicit windows (repeatable), dry-run preview
  python purge_events_by_date.py --range 2026-05-15:2026-05-17 \\
                                  --range 2026-05-22:2026-05-24

  # N upcoming weekends from today (Fri/Sat/Sun)
  python purge_events_by_date.py --next-weekends 3

  # Commit deletions
  python purge_events_by_date.py --range 2026-05-15:2026-05-17 --apply

OUTPUT
──────
- Preview of every row that will be deleted (sheet row, post_id, date,
  event name, post URL).
- Counts BEFORE: All_Events / Processed_Log row totals.
- Counts AFTER (apply mode): same totals, plus delta.
- CSV audit trail at outputs/purge_events_<ts>.csv.
"""

import argparse
import csv
import sys
import time
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"
PROCESSED_LOG_TAB = "Processed_Log"

DELETE_BATCH_SIZE = 500
BATCH_DELAY_SEC = 1.0

DATE_FMTS = ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%B %d, %Y")


def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def col_index(header, *names):
    for name in names:
        for i, h in enumerate(header):
            if h.strip().upper() == name.upper():
                return i
    return None


def parse_date(s):
    s = (s or "").strip()
    if not s:
        return None
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_range(spec):
    """Parse 'YYYY-MM-DD:YYYY-MM-DD' into (start_date, end_date), inclusive."""
    try:
        lo_s, hi_s = spec.split(":", 1)
        lo = datetime.strptime(lo_s.strip(), "%Y-%m-%d").date()
        hi = datetime.strptime(hi_s.strip(), "%Y-%m-%d").date()
    except (ValueError, AttributeError):
        raise argparse.ArgumentTypeError(
            f"--range must be YYYY-MM-DD:YYYY-MM-DD (got {spec!r})"
        )
    if hi < lo:
        raise argparse.ArgumentTypeError(f"--range end < start in {spec!r}")
    return (lo, hi)


def expand_ranges(ranges):
    """Flatten list of (lo, hi) tuples into a set of date objects."""
    out = set()
    for lo, hi in ranges:
        d = lo
        while d <= hi:
            out.add(d)
            d += timedelta(days=1)
    return out


def next_n_weekends(n, today=None):
    """Return list of (Fri, Sun) range tuples for the next n weekends from today."""
    today = today or date.today()
    out = []
    week_starts_seen = 0
    d = today
    while week_starts_seen < n:
        # Find the upcoming Friday
        days_to_fri = (4 - d.weekday()) % 7
        fri = d + timedelta(days=days_to_fri)
        sun = fri + timedelta(days=2)
        out.append((fri, sun))
        d = sun + timedelta(days=1)
        week_starts_seen += 1
    return out


def find_rows_in_window(ws, target_dates):
    """Return (header, rows_in_window, post_id_counts_total) where:
       - rows_in_window: list of dicts for each in-window row
       - post_id_counts_total: dict {post_id: total_row_count_in_all_events}
    """
    all_values = ws.get_all_values()
    if not all_values:
        return [], [], {}

    header = all_values[0]
    data = all_values[1:]

    date_idx = col_index(header, "DATE", "EVENT DATE")
    pid_idx = col_index(header, "POST ID")
    url_idx = col_index(header, "INSTAGRAM POST URL", "POST URL")
    handle_idx = col_index(header, "INSTAGRAM HANDLE", "HANDLE", "ACCOUNT")
    name_idx = col_index(header, "EVENT NAME")

    if date_idx is None:
        print(f"❌ Could not find DATE column in {ALL_EVENTS_TAB}: {header}", file=sys.stderr)
        sys.exit(1)
    if pid_idx is None:
        print(f"❌ Could not find POST ID column in {ALL_EVENTS_TAB}: {header}", file=sys.stderr)
        sys.exit(1)

    rows_in_window = []
    post_id_counts_total = defaultdict(int)

    for i, row in enumerate(data, start=2):  # 2 = first data row in sheet
        pid = (row[pid_idx].strip() if len(row) > pid_idx else "")
        if pid:
            post_id_counts_total[pid] += 1

        raw = row[date_idx].strip() if len(row) > date_idx else ""
        d = parse_date(raw)
        if d is None or d not in target_dates:
            continue

        rows_in_window.append({
            "sheet_row": i,
            "post_id": pid,
            "date": raw,
            "event_name": (row[name_idx].strip() if name_idx is not None and len(row) > name_idx else ""),
            "post_url":   (row[url_idx].strip()  if url_idx  is not None and len(row) > url_idx  else ""),
            "handle":     (row[handle_idx].strip() if handle_idx is not None and len(row) > handle_idx else ""),
        })

    return header, rows_in_window, dict(post_id_counts_total)


def find_processed_log_rows(ws, post_ids):
    """Return list of sheet row numbers in Processed_Log matching any of post_ids."""
    all_values = ws.get_all_values()
    if not all_values or not post_ids:
        return []
    header = all_values[0]
    pid_idx = col_index(header, "POST ID") or 0
    targets = set(post_ids)
    matches = []
    for i, row in enumerate(all_values[1:], start=2):
        pid = (row[pid_idx].strip() if len(row) > pid_idx else "")
        if pid in targets:
            matches.append(i)
    return matches


def batch_delete_rows(ws, sheet_rows):
    """Delete the given sheet rows in reverse order, batched."""
    if not sheet_rows:
        return
    rows_desc = sorted(set(sheet_rows), reverse=True)

    # Coalesce consecutive rows into ranges
    ranges = []
    i = 0
    while i < len(rows_desc):
        high = rows_desc[i]
        low = high
        while i + 1 < len(rows_desc) and rows_desc[i] - rows_desc[i + 1] == 1:
            i += 1
            low = rows_desc[i]
        ranges.append({
            "deleteDimension": {
                "range": {
                    "sheetId": ws.id,
                    "dimension": "ROWS",
                    "startIndex": low - 1,
                    "endIndex": high,
                }
            }
        })
        i += 1

    spreadsheet = ws.spreadsheet
    for start in range(0, len(ranges), DELETE_BATCH_SIZE):
        batch = ranges[start : start + DELETE_BATCH_SIZE]
        spreadsheet.batch_update({"requests": batch})
        if start + DELETE_BATCH_SIZE < len(ranges):
            time.sleep(BATCH_DELAY_SEC)


def write_audit_csv(rows_in_window, processed_log_rows, target_dates):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = Path("outputs") / f"purge_events_{ts}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["section", "sheet_row", "post_id", "date", "event_name", "handle", "post_url"])
        for r in rows_in_window:
            w.writerow(["all_events", r["sheet_row"], r["post_id"], r["date"],
                        r["event_name"], r["handle"], r["post_url"]])
        for sr in processed_log_rows:
            w.writerow(["processed_log", sr, "", "", "", "", ""])
    return path


def main():
    p = argparse.ArgumentParser(
        description="Delete event rows from All_Events by date window, then unlock from Processed_Log.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--range", dest="ranges", action="append", default=[], type=parse_range,
                   help="Date window YYYY-MM-DD:YYYY-MM-DD (inclusive). Repeatable.")
    p.add_argument("--next-weekends", type=int, default=None,
                   help="Use the next N upcoming Fri-Sun weekends from today.")
    p.add_argument("--max-posts", type=int, default=None,
                   help="Cap the number of unique post IDs targeted "
                        "(takes the first N by sheet row order). Useful for "
                        "small-batch test runs.")
    p.add_argument("--apply", action="store_true",
                   help="Commit deletions (default: dry-run).")
    args = p.parse_args()

    # Resolve target windows
    ranges = list(args.ranges)
    if args.next_weekends is not None:
        ranges.extend(next_n_weekends(args.next_weekends))
    if not ranges:
        ranges = next_n_weekends(2)  # default: this + next weekend

    target_dates = expand_ranges(ranges)

    print("━━━ purge_events_by_date.py ━━━")
    print(f"  Apply mode: {args.apply}  (dry-run is default)")
    print(f"  Target windows ({len(ranges)}):")
    for lo, hi in ranges:
        if lo == hi:
            print(f"    • {lo}  ({lo.strftime('%a')})")
        else:
            print(f"    • {lo} → {hi}  ({lo.strftime('%a')}–{hi.strftime('%a')})")
    print(f"  Total target days: {len(target_dates)}")
    print()

    print("  Connecting to Google Sheet...")
    sh = setup_sheet()
    try:
        ae_ws = sh.worksheet(ALL_EVENTS_TAB)
    except gspread.WorksheetNotFound:
        print(f"❌ Worksheet {ALL_EVENTS_TAB!r} not found.", file=sys.stderr)
        sys.exit(1)
    try:
        pl_ws = sh.worksheet(PROCESSED_LOG_TAB)
    except gspread.WorksheetNotFound:
        pl_ws = None
        print(f"⚠ Worksheet {PROCESSED_LOG_TAB!r} not found — will skip the unlock step.")

    # ── COUNTS BEFORE ─────────────────────────────────────────────────
    before_ae = ae_ws.row_count  # total grid rows; we want data rows
    before_ae_data = len(ae_ws.col_values(1)) - 1  # -1 for header
    before_pl_data = (len(pl_ws.col_values(1)) - 1) if pl_ws else 0

    print(f"\n━━━ COUNTS BEFORE ━━━")
    print(f"  All_Events data rows:    {before_ae_data}")
    print(f"  Processed_Log data rows: {before_pl_data}")

    # ── SCAN ──────────────────────────────────────────────────────────
    print(f"\n  Scanning {ALL_EVENTS_TAB} for in-window rows...")
    header, rows_in_window, post_id_counts_total = find_rows_in_window(ae_ws, target_dates)

    if not rows_in_window:
        print("\n✓ No in-window rows found. Nothing to delete.")
        return

    # ── Optional cap: limit to first N unique post IDs by sheet row order ──
    if args.max_posts is not None and args.max_posts > 0:
        kept_pids = []
        seen = set()
        for r in rows_in_window:
            pid = r["post_id"]
            if not pid or pid in seen:
                continue
            seen.add(pid)
            kept_pids.append(pid)
            if len(kept_pids) >= args.max_posts:
                break
        kept_pids_set = set(kept_pids)
        original_count = len(rows_in_window)
        rows_in_window = [r for r in rows_in_window if r["post_id"] in kept_pids_set]
        print(f"\n  --max-posts {args.max_posts}: kept first {len(kept_pids)} unique post ID(s), "
              f"trimmed {original_count} → {len(rows_in_window)} rows.")

    # Collect post IDs touched by in-window rows
    touched_post_ids = sorted({r["post_id"] for r in rows_in_window if r["post_id"]})

    # Identify "partial" posts: those with surviving rows outside the window
    # (in-window count < total count for that post_id)
    in_window_count_by_pid = defaultdict(int)
    for r in rows_in_window:
        if r["post_id"]:
            in_window_count_by_pid[r["post_id"]] += 1
    partial_posts = [
        pid for pid in touched_post_ids
        if post_id_counts_total.get(pid, 0) > in_window_count_by_pid[pid]
    ]

    # Find Processed_Log matches
    pl_rows_to_delete = find_processed_log_rows(pl_ws, touched_post_ids) if pl_ws else []

    # ── PREVIEW ───────────────────────────────────────────────────────
    print(f"\n━━━ PREVIEW — rows targeted for deletion ━━━")
    print(f"  {'Sheet Row':<10} {'Post ID':<22} {'Date':<12} {'Event Name':<35} Handle")
    print(f"  {'─'*10} {'─'*22} {'─'*12} {'─'*35} {'─'*15}")
    for r in rows_in_window[:60]:
        ev = (r["event_name"][:33] + "..") if len(r["event_name"]) > 35 else r["event_name"]
        pid = (r["post_id"][:20] + "..") if len(r["post_id"]) > 22 else r["post_id"]
        print(f"  {r['sheet_row']:<10} {pid:<22} {r['date']:<12} {ev:<35} @{r['handle']}")
    if len(rows_in_window) > 60:
        print(f"  ... and {len(rows_in_window) - 60} more")

    print(f"\n  Summary:")
    print(f"   • All_Events rows in window:    {len(rows_in_window)}")
    print(f"   • Unique post IDs affected:     {len(touched_post_ids)}")
    print(f"   • Processed_Log rows to remove: {len(pl_rows_to_delete)}")

    if partial_posts:
        print(f"\n⚠ DUPLICATE-RISK WARNING")
        print(f"  {len(partial_posts)} post(s) have events BOTH inside and outside the window.")
        print(f"  Strict mode will delete only in-window rows, but Processed_Log will be cleared")
        print(f"  for these post IDs — so on next pipeline run the surviving rows could be")
        print(f"  re-extracted as duplicates. Run dedup_all_events.py afterward if this happens.")
        print(f"  Affected post IDs (first 10): {partial_posts[:10]}")
        if len(partial_posts) > 10:
            print(f"    ... and {len(partial_posts) - 10} more")

    # ── AUDIT CSV ─────────────────────────────────────────────────────
    csv_path = write_audit_csv(rows_in_window, pl_rows_to_delete, target_dates)
    print(f"\n  ✓ Audit CSV: {csv_path}")

    if not args.apply:
        print(f"\n  [dry-run] No sheet changes made. Re-run with --apply to commit.")
        return

    # ── APPLY ─────────────────────────────────────────────────────────
    print(f"\n━━━ DELETING {len(rows_in_window)} rows from {ALL_EVENTS_TAB} ━━━")
    batch_delete_rows(ae_ws, [r["sheet_row"] for r in rows_in_window])
    print(f"  ✓ {ALL_EVENTS_TAB} deletions committed.")

    if pl_ws and pl_rows_to_delete:
        print(f"\n━━━ DELETING {len(pl_rows_to_delete)} rows from {PROCESSED_LOG_TAB} ━━━")
        batch_delete_rows(pl_ws, pl_rows_to_delete)
        print(f"  ✓ {PROCESSED_LOG_TAB} deletions committed.")

    # ── COUNTS AFTER ──────────────────────────────────────────────────
    after_ae_data = len(ae_ws.col_values(1)) - 1
    after_pl_data = (len(pl_ws.col_values(1)) - 1) if pl_ws else 0

    print(f"\n━━━ COUNTS AFTER ━━━")
    print(f"  All_Events data rows:    {before_ae_data} → {after_ae_data}  "
          f"(Δ {after_ae_data - before_ae_data:+d})")
    print(f"  Processed_Log data rows: {before_pl_data} → {after_pl_data}  "
          f"(Δ {after_pl_data - before_pl_data:+d})")
    print(f"\n✓ Done. Audit trail: {csv_path}")


if __name__ == "__main__":
    main()
