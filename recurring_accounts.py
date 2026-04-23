#!/usr/bin/env python3
"""
Reliable Accounts Detection — Recurring Series
Scans all historical event CSVs in outputs/ to identify Instagram accounts
whose event names signal recurrence via three patterns:
  1. Day-of-week plural  (e.g. "Fridays", "Saturdays")
  2. Recurrence keyword  (e.g. "Weekly", "Every", "Nights", "Series", "Season")
  3. Repeated branded name — same exact event name 3+ times from same account
     across different source CSV files

Accounts matching at least one pattern are written to the Reliable_Accounts
tab in Google Sheets and saved to outputs/reliable_accounts.csv.

Usage:
    python recurring_accounts.py          # full run + sheets sync
    python recurring_accounts.py --local  # local CSV only, no sheets
"""

import os
import re
import sys
import glob
import json
import argparse
from pathlib import Path

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
OUTPUTS_DIR = Path("outputs")
LOCAL_CSV = OUTPUTS_DIR / "reliable_accounts.csv"

TAB_HEADERS = [
    "Account", "Display Name", "Example Series Names",
    "Pattern Types Matched", "Occurrences"
]

DAY_PLURAL_RE = re.compile(
    r'\b(Mondays|Tuesdays|Wednesdays|Thursdays|Fridays|Saturdays|Sundays)\b',
    re.IGNORECASE
)

RECURRENCE_KW_RE = re.compile(
    r'\b(Weekly|Every|Nightly|Nights|Series|Season)\b',
    re.IGNORECASE
)

BRANDED_REPEAT_THRESHOLD = 3


def _load_all_csvs():
    """
    Load and concatenate all event CSVs from outputs/.
    Skips reliable_accounts.csv and other non-event files.
    Returns a DataFrame with at minimum columns:
      handle, event_name, display_name, source_file
    """
    csv_files = sorted(
        f for f in glob.glob(str(OUTPUTS_DIR / "*.csv"))
        if "reliable_accounts" not in os.path.basename(f)
    )

    if not csv_files:
        print("❌ No CSV files found in outputs/")
        return pd.DataFrame()

    frames = []
    for path in csv_files:
        try:
            df = pd.read_csv(path, low_memory=False)

            handle_col = next(
                (c for c in df.columns
                 if c.lower().replace(' ', '_') == 'instagram_handle'), None
            )
            name_col = next(
                (c for c in df.columns
                 if c.lower().replace(' ', '_') == 'event_name'), None
            )

            if not handle_col or not name_col:
                continue

            display_col = next(
                (c for c in df.columns
                 if c.lower().replace(' ', '_') == 'account_name'), None
            )

            slim = pd.DataFrame({
                'handle': df[handle_col].fillna('').astype(str).str.strip().str.lower(),
                'event_name': df[name_col].fillna('').astype(str).str.strip(),
                'display_name': df[display_col].fillna('').astype(str).str.strip()
                    if display_col else '',
                'source_file': os.path.basename(path),
            })
            frames.append(slim)

        except Exception as e:
            print(f"  ⚠ Skipping {path}: {e}")

    if not frames:
        print("❌ No valid event CSV files found (missing required columns).")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined[(combined['handle'].str.len() > 0) &
                        (combined['event_name'].str.len() > 0)]
    print(f"✓ Loaded {len(csv_files)} CSV files → {len(combined):,} rows")
    return combined


def detect_recurring_accounts(df):
    """
    Apply the three recurrence patterns and return a results DataFrame
    with columns: Account, Display Name, Example Series Names,
    Pattern Types Matched, Occurrences — sorted by Occurrences desc.
    """
    if df.empty:
        return pd.DataFrame(columns=TAB_HEADERS)

    rows = []

    for handle, group in df.groupby('handle', sort=False):
        display_name = ''
        mode_vals = group['display_name'][group['display_name'].str.len() > 0]
        if not mode_vals.empty:
            display_name = mode_vals.mode().iloc[0]

        matched_patterns = set()
        matched_names = []

        for event_name in group['event_name'].unique():
            name_patterns = set()

            if DAY_PLURAL_RE.search(event_name):
                name_patterns.add('day-of-week plural')

            if RECURRENCE_KW_RE.search(event_name):
                name_patterns.add('recurrence keyword')

            files_with_name = group.loc[
                group['event_name'] == event_name, 'source_file'
            ].nunique()
            if files_with_name >= BRANDED_REPEAT_THRESHOLD:
                name_patterns.add('repeated branded name')

            if name_patterns:
                matched_patterns |= name_patterns
                matched_names.append(event_name)

        if not matched_patterns:
            continue

        rows.append({
            'Account': handle,
            'Display Name': display_name,
            'Example Series Names': '; '.join(matched_names[:5]),
            'Pattern Types Matched': ', '.join(sorted(matched_patterns)),
            'Occurrences': len(group),
        })

    if not rows:
        return pd.DataFrame(columns=TAB_HEADERS)

    result = (
        pd.DataFrame(rows, columns=TAB_HEADERS)
        .sort_values('Occurrences', ascending=False)
        .reset_index(drop=True)
    )
    return result


def _write_to_sheets(main_sheet, result_df):
    """Create or overwrite the Reliable_Accounts tab."""
    try:
        ws = main_sheet.worksheet("Reliable_Accounts")
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = main_sheet.add_worksheet(
            title="Reliable_Accounts",
            rows=max(1000, len(result_df) + 10),
            cols=len(TAB_HEADERS)
        )

    all_rows = [TAB_HEADERS] + [
        [str(v) for v in row] for row in result_df.values.tolist()
    ]
    ws.update(all_rows, value_input_option='RAW')
    print(f"✓ Reliable_Accounts tab updated ({len(result_df)} accounts)")


def _connect_sheets():
    """Authenticate and return the main Google Sheet, or None on failure."""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print("⚠ No service account file — skipping Sheets sync.")
        return None
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client = gspread.authorize(creds)

        sheet_name = "Instagram_Events_Master"
        if os.path.exists("config.json"):
            with open("config.json") as f:
                sheet_name = json.load(f).get("sheet_name", sheet_name)

        main_sheet = client.open(sheet_name)
        print(f"✓ Connected to Sheet: {sheet_name}")
        return main_sheet
    except Exception as e:
        print(f"⚠ Sheets connection failed: {e}")
        return None


def refresh(main_sheet=None):
    """
    Run detection, save local CSV, and update Google Sheets.
    Called from main.py save_data() after each pipeline run.
    If main_sheet is provided (gspread Spreadsheet object), re-auth is skipped.
    """
    print("\n" + "=" * 60)
    print("RELIABLE ACCOUNTS — RECURRING SERIES DETECTION")
    print("=" * 60)

    df = _load_all_csvs()
    if df.empty:
        print("No data to analyse.")
        return pd.DataFrame(columns=TAB_HEADERS)

    result = detect_recurring_accounts(df)
    print(f"✓ {len(result)} recurring-series accounts detected")

    if not result.empty:
        print("\nTop accounts:")
        print(result[['Account', 'Pattern Types Matched', 'Occurrences']].head(10).to_string(index=False))

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(LOCAL_CSV, index=False)
    print(f"\n✓ Saved {LOCAL_CSV}")

    if main_sheet is None:
        main_sheet = _connect_sheets()

    if main_sheet is not None:
        try:
            _write_to_sheets(main_sheet, result)
        except Exception as e:
            print(f"⚠ Sheets write error: {e}")

    print("=" * 60)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reliable Accounts — Recurring Series Detection")
    parser.add_argument("--local", action="store_true",
                        help="Skip Google Sheets sync, save local CSV only")
    args = parser.parse_args()

    if args.local:
        df = _load_all_csvs()
        if not df.empty:
            result = detect_recurring_accounts(df)
            print(f"\n✓ {len(result)} recurring-series accounts found")
            if not result.empty:
                print(result.to_string(index=False))
            OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
            result.to_csv(LOCAL_CSV, index=False)
            print(f"\n✓ Saved {LOCAL_CSV}")
    else:
        refresh()
