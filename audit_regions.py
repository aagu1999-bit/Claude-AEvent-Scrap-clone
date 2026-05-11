#!/usr/bin/env python3
"""
audit_regions.py — find and fix historically-mistagged SECTION OF NJ cells in All_Events.

PURPOSE
───────
The canonical lookup at data/nj_municipalities.json was added recently
(via events_from_ids.py). Existing All_Events rows predate that lookup
and were tagged by Gemini's per-call region inference, which is
unreliable.

Today's audit found 79 cities tagged with conflicting regions across
existing rows. Examples:
  NEWARK:  1857 NORTH + 747 CENTRAL (and 1 ESSEX)  ← should always be NORTH
  WEST ORANGE: 57 NORTH + 54 CENTRAL              ← coin-flip
  MONTCLAIR:   346 NORTH + 149 CENTRAL            ← 30% wrong

This tool sweeps All_Events, identifies rows whose SECTION OF NJ
disagrees with the canonical lookup, and corrects them.

USAGE
─────
  # Preview what would change (default — no writes)
  python audit_regions.py

  # Apply corrections to the sheet
  python audit_regions.py --apply

  # Limit to a city subset (for testing)
  python audit_regions.py --city NEWARK --apply

  # Optional: also apply light-yellow formatting to changed cells
  python audit_regions.py --apply --highlight

SAFETY
──────
- Dry-run by default. Nothing changes unless --apply is passed.
- Writes a CSV audit trail to outputs/audit_regions_<ts>.csv listing
  every row touched (old → new region).
- Uses batch_update for efficiency (single API call per N rows).
- City must be in the NJ lookup with a real region (NORTH/CENTRAL/SOUTH)
  to trigger a change. NON_NJ cities have their region cleared but
  that's surfaced separately and requires --include-non-nj.
"""

import argparse
import csv
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"
NJ_MUNICIPALITIES_JSON = "data/nj_municipalities.json"

BATCH_SIZE = 100         # rows per batch_update call
BATCH_DELAY_SEC = 1.0    # gentle pause between batches

# Light yellow for highlighted changes
HIGHLIGHT_BG = {"red": 1.0, "green": 0.97, "blue": 0.78}


def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def load_lookup():
    p = Path(NJ_MUNICIPALITIES_JSON)
    if not p.exists():
        print(f"❌ Lookup not found: {p}", file=sys.stderr)
        sys.exit(1)
    return json.loads(p.read_text())


def col_letter(idx_0based):
    if idx_0based < 26:
        return chr(ord('A') + idx_0based)
    return chr(ord('A') + (idx_0based // 26) - 1) + chr(ord('A') + (idx_0based % 26))


def main():
    p = argparse.ArgumentParser(
        description="Fix historically-mistagged SECTION OF NJ cells in All_Events.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--apply", action="store_true",
                   help="Actually apply corrections (default: dry-run preview)")
    p.add_argument("--city", action="append", default=None,
                   help="Limit to specific city (UPPERCASE). Repeat for multiple.")
    p.add_argument("--include-non-nj", action="store_true",
                   help="Also clear region for cities marked NON_NJ in the lookup")
    p.add_argument("--highlight", action="store_true",
                   help="Apply light-yellow formatting to changed cells")
    p.add_argument("--limit", type=int, default=None,
                   help="Only process first N rows (for testing)")
    args = p.parse_args()

    print(f"━━━ audit_regions.py ━━━")
    print(f"  Apply mode:        {args.apply}")
    print(f"  City filter:       {args.city or '(all)'}")
    print(f"  Include NON_NJ:    {args.include_non_nj}")
    print(f"  Highlight changes: {args.highlight}")
    print(f"  Limit:             {args.limit or '(none)'}")
    print()

    # Load canonical NJ municipalities lookup
    region_lookup = load_lookup()
    print(f"  NJ lookup: {len(region_lookup)} cities")

    # Connect to sheet
    print(f"  Connecting to sheet...")
    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)

    header = ws.row_values(1)
    try:
        city_idx = header.index("CITY")
        region_idx = header.index("SECTION OF NJ")
    except ValueError as e:
        print(f"❌ Header missing required column: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  Reading All_Events rows...")
    all_data = ws.get_all_values()
    print(f"  {len(all_data)-1} data rows")

    # Build correction set
    corrections = []  # list of (row_num, city, old_region, new_region, action)
    city_filter = set(c.upper() for c in args.city) if args.city else None

    for i, row in enumerate(all_data[1:], start=2):  # row_num is 2-indexed (1=header)
        if args.limit and (i - 1) > args.limit:
            break
        if len(row) <= max(city_idx, region_idx):
            continue
        city = (row[city_idx] or '').strip().upper()
        current_region = (row[region_idx] or '').strip().upper()

        if not city:
            continue
        if city_filter and city not in city_filter:
            continue

        canonical = region_lookup.get(city)
        if canonical is None:
            continue  # city not in lookup — leave alone

        if canonical == "NON_NJ":
            if not args.include_non_nj:
                continue
            if current_region:
                corrections.append((i, city, current_region, "", "clear_non_nj"))
            continue

        # NJ city with a canonical region
        if current_region != canonical:
            corrections.append((i, city, current_region, canonical, "fix_region"))

    # Summary
    print(f"\n━━━ FINDINGS ━━━")
    print(f"  Rows scanned:      {min(len(all_data)-1, args.limit or len(all_data)-1)}")
    print(f"  Corrections found: {len(corrections)}")

    action_counts = Counter(c[4] for c in corrections)
    for action, n in action_counts.most_common():
        print(f"    {action:<20} {n}")

    by_city_summary = Counter()
    by_correction = defaultdict(Counter)
    for row_num, city, old, new, action in corrections:
        by_city_summary[city] += 1
        by_correction[city][(old, new)] += 1

    print(f"\n  Top 20 cities by correction count:")
    for city, n in by_city_summary.most_common(20):
        changes = ', '.join(f"{old!r}→{new!r}: {cnt}"
                            for (old, new), cnt in by_correction[city].most_common())
        print(f"    {city:<30} {n:>5}  ({changes})")

    if not corrections:
        print("\n✓ No corrections needed.")
        return

    # Write audit CSV (always — even on dry-run, this is the change preview)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("outputs") / f"audit_regions_{ts}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['row_num', 'city', 'old_region', 'new_region', 'action'])
        for c in corrections:
            w.writerow(c)
    print(f"\n  ✓ Audit CSV: {csv_path}")

    if not args.apply:
        print(f"\n  [dry-run] No sheet changes made. Re-run with --apply to commit.")
        return

    # Build batch_update payload
    print(f"\n━━━ APPLYING {len(corrections)} CHANGES ━━━")
    region_col = col_letter(region_idx)

    # Group into batches
    total_batches = (len(corrections) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_idx in range(0, len(corrections), BATCH_SIZE):
        batch = corrections[batch_idx:batch_idx + BATCH_SIZE]
        # Build batch_update data: each cell as its own range
        batch_data = []
        for row_num, city, old, new, action in batch:
            batch_data.append({
                'range': f'{region_col}{row_num}',
                'values': [[new]],
            })
        try:
            ws.batch_update(batch_data, value_input_option="USER_ENTERED")
            n = batch_idx // BATCH_SIZE + 1
            print(f"  ✓ batch {n}/{total_batches}  ({len(batch)} cells)")
        except Exception as e:
            print(f"  ✗ batch {batch_idx//BATCH_SIZE + 1} failed: {str(e)[:120]}")
            print(f"  Stopping. Already-applied batches are persisted in the sheet.")
            sys.exit(1)
        if batch_idx + BATCH_SIZE < len(corrections):
            time.sleep(BATCH_DELAY_SEC)

    print(f"\n  ✓ All {len(corrections)} region cells updated.")

    # Optional: highlight changed cells
    if args.highlight:
        print(f"\n  Applying light-yellow highlight to {len(corrections)} cells...")
        fmt_requests = [
            {'range': f'{region_col}{c[0]}',
             'format': {'backgroundColor': HIGHLIGHT_BG}}
            for c in corrections
        ]
        try:
            ws.batch_format(fmt_requests)
            print(f"  ✓ Formatting applied")
        except Exception as e:
            print(f"  ⚠ Format apply failed: {str(e)[:120]}")

    print(f"\n✓ Done. Audit trail: {csv_path}")


if __name__ == "__main__":
    main()
