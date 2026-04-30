#!/usr/bin/env python3
"""
recover_events.py — Re-upload events from a local CSV to the All_Events tab
when the live pipeline's Sheets write failed but the CSV backup saved fine.

The pipeline always writes outputs/Events_<timestamp>.csv before attempting
the Sheets upload (see save_data in main.py), so a Sheets failure leaves
you with a complete CSV you can replay.

USAGE
─────
  python recover_events.py                       # pick latest outputs/Events_*.csv
  python recover_events.py outputs/Events_X.csv  # specify a file
  python recover_events.py --no-header-fix       # skip header relabel

WHAT IT DOES
────────────
1. Loads the CSV.
2. Reads existing All_Events rows; dedups by POST ID so re-runs are safe.
3. Appends new rows in batches of 500 (avoids Sheets API row-size limits).
4. Fixes header labels in row 1 (legacy schema drift left col 21/22 mislabeled).

Safe to run multiple times — dedup means it only ever writes net-new events.
"""

import argparse
import sys
import time
from pathlib import Path

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
TAB = "All_Events"

# Canonical header — matches what save_data() in main.py produces today.
CORRECT_HEADER = [
    "INSTAGRAM HANDLE", "EVENT NAME", "DATE", "START TIME",
    "VENUE NAME", "CITY", "SECTION OF NJ", "NEWSLETTER DESCRIPTION",
    "INSTAGRAM POST URL", "DISPLAY URL", "POST URL", "INSTAGRAM PROFILE URL",
    "EVENT TYPE", "ACCOUNT NAME", "DESCRIPTION", "PERFORMER", "PRICE",
    "CONFIDENCE", "POST ID", "HAD OCR", "FROM CALENDAR",
    "IS RECURRING", "PROCESSED TIMESTAMP",
]

BATCH_SIZE = 500           # rows per append_rows call
BATCH_DELAY_SEC = 1.5      # gentle pause between batches to avoid quota


def connect_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def fix_header(ws):
    """Overwrite row 1 with canonical column labels."""
    print("\n🔧 Fixing All_Events header labels...")
    current = ws.row_values(1)
    diffs = [(i + 1, current[i] if i < len(current) else "(missing)", CORRECT_HEADER[i])
             for i in range(len(CORRECT_HEADER))
             if i >= len(current) or current[i].strip() != CORRECT_HEADER[i]]
    if not diffs:
        print("   ✓ Header already correct — no changes needed")
        return
    print(f"   Will update {len(diffs)} header labels:")
    for col, was, will in diffs:
        print(f"     col {col}:  {was!r:<25} → {will!r}")
    ws.update("A1", [CORRECT_HEADER], value_input_option="USER_ENTERED")
    print("   ✓ Header rewritten")


def find_csv(arg_path: str | None) -> Path:
    if arg_path:
        p = Path(arg_path)
        if not p.exists():
            print(f"❌ Not found: {p}", file=sys.stderr)
            sys.exit(1)
        return p
    candidates = sorted(Path("outputs").glob("Events_*.csv"))
    candidates += sorted(Path("outputs").glob("emergency_events_*.csv"))
    if not candidates:
        print("❌ No Events_*.csv or emergency_events_*.csv found in outputs/", file=sys.stderr)
        sys.exit(1)
    # Most recent by mtime
    return max(candidates, key=lambda p: p.stat().st_mtime)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", nargs="?", help="Path to Events_*.csv (default: latest in outputs/)")
    ap.add_argument("--no-header-fix", action="store_true", help="Skip header relabel")
    ap.add_argument("--dry-run", action="store_true", help="Don't write to sheet")
    args = ap.parse_args()

    csv_path = find_csv(args.csv)
    print(f"📄 CSV source: {csv_path}  ({csv_path.stat().st_size:,} bytes)")

    df = pd.read_csv(csv_path)
    print(f"   Loaded {len(df)} rows × {len(df.columns)} cols")

    sh = connect_sheet()
    ws = sh.worksheet(TAB)

    # Header fix first so dedup uses correct column names
    if not args.no_header_fix and not args.dry_run:
        fix_header(ws)

    # Read current state (after potential header fix)
    print("\n📥 Reading current All_Events for dedup check...")
    existing_rows = ws.get_all_values()
    if not existing_rows:
        print("   ⚠ Sheet appears empty — appending everything")
        existing_post_ids = set()
        existing_keys = set()
    else:
        header = existing_rows[0]
        post_id_idx = header.index("POST ID") if "POST ID" in header else None
        name_idx = header.index("EVENT NAME") if "EVENT NAME" in header else None
        date_idx = header.index("DATE") if "DATE" in header else None
        existing_post_ids = set()
        existing_keys = set()  # fallback dedup: post_id+name+date
        for r in existing_rows[1:]:
            pid = r[post_id_idx] if post_id_idx is not None and post_id_idx < len(r) else ""
            if pid:
                existing_post_ids.add(str(pid).strip())
            n = r[name_idx] if name_idx is not None and name_idx < len(r) else ""
            d = r[date_idx] if date_idx is not None and date_idx < len(r) else ""
            existing_keys.add((str(pid).strip(), str(n).strip().upper(), str(d).strip()))
        print(f"   Existing rows: {len(existing_rows) - 1}")
        print(f"   Unique POST IDs: {len(existing_post_ids)}")

    # Filter CSV to net-new
    if "POST ID" in df.columns:
        # POST ID alone isn't unique (one post can yield multiple events), so use composite key
        df["_key"] = df.apply(
            lambda r: (str(r.get("POST ID", "")).strip(),
                       str(r.get("EVENT NAME", "")).strip().upper(),
                       str(r.get("DATE", "")).strip()),
            axis=1,
        )
        new = df[~df["_key"].isin(existing_keys)].drop(columns=["_key"])
    else:
        print("   ⚠ CSV has no POST ID column — appending all rows (no dedup)")
        new = df

    skipped = len(df) - len(new)
    print(f"   Net-new rows: {len(new)} (skipping {skipped} dupes)")

    if len(new) == 0:
        print("\n✅ Nothing to append — All_Events already has these events.")
        return

    if args.dry_run:
        print(f"\n🌵 --dry-run: would append {len(new)} rows. Sample:")
        print(new.head(3).to_string())
        return

    # Append in batches
    print(f"\n⬆️  Appending {len(new)} rows in batches of {BATCH_SIZE}...")
    rows = new.fillna("").astype(str).values.tolist()
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        n = i // BATCH_SIZE + 1
        total = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"   ✓ batch {n}/{total} ({len(batch)} rows)")
        if i + BATCH_SIZE < len(rows):
            time.sleep(BATCH_DELAY_SEC)

    print(f"\n✅ Recovered {len(new)} events to All_Events")


if __name__ == "__main__":
    main()
