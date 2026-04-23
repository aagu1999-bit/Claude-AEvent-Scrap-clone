#!/usr/bin/env python3
"""
Reliable Accounts Detection
Scans all historical event CSVs to identify Instagram accounts that
consistently post recurring events (weekly nights, branded series, etc.)
and writes results to the Reliable_Accounts tab in Google Sheets.

Usage:
    python recurring_accounts.py          # full run + sheets sync
    python recurring_accounts.py --local  # local CSV only, no sheets
"""

import os
import re
import sys
import glob
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
OUTPUTS_DIR = Path("outputs")

DAY_PLURAL_PATTERN = re.compile(
    r'\b(?:mondays|tuesdays|wednesdays|thursdays|fridays|saturdays|sundays)\b',
    re.IGNORECASE
)
MIN_SCORE = 2
REPEAT_THRESHOLD = 3


def load_all_csvs():
    """Load and concatenate all event CSVs from outputs/."""
    csv_files = sorted(glob.glob(str(OUTPUTS_DIR / "*.csv")))
    if not csv_files:
        print("❌ No CSV files found in outputs/")
        return pd.DataFrame()

    frames = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, low_memory=False)
            if 'event_name' in df.columns and 'instagram_handle' in df.columns:
                frames.append(df)
        except Exception as e:
            print(f"  ⚠ Skipping {f}: {e}")

    if not frames:
        print("❌ No valid CSV files found.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    print(f"✓ Loaded {len(csv_files)} CSV files → {len(combined):,} total rows")
    return combined


def score_accounts(df):
    """Score each account on three recurring-event signals."""
    df = df.copy()
    df['event_name'] = df['event_name'].fillna('').astype(str)
    df['instagram_handle'] = df['instagram_handle'].fillna('').astype(str)
    df['event_name_clean'] = df['event_name'].str.strip().str.lower()
    df['is_recurring_flag'] = df.get('is_recurring', pd.Series(dtype=str)) \
        .astype(str).str.lower().isin(['true', '1', 'yes'])
    df['has_day_plural'] = df['event_name'].str.contains(DAY_PLURAL_PATTERN, na=False)

    name_counts = (
        df.groupby(['instagram_handle', 'event_name_clean'])
        .size()
        .reset_index(name='cnt')
    )
    repeat_accounts = set(
        name_counts[name_counts['cnt'] >= REPEAT_THRESHOLD]['instagram_handle']
    )

    rows = []
    for acct, sub in df.groupby('instagram_handle'):
        if not acct:
            continue

        has_plural = bool(sub['has_day_plural'].any())
        has_recurring = bool(sub['is_recurring_flag'].any())
        has_repeat = acct in repeat_accounts
        score = int(has_plural) + int(has_recurring) + int(has_repeat)

        if score < MIN_SCORE:
            continue

        examples = []
        if has_plural:
            examples += list(sub[sub['has_day_plural']]['event_name'].unique()[:2])
        if has_repeat:
            acct_counts = name_counts[
                (name_counts['instagram_handle'] == acct) &
                (name_counts['cnt'] >= REPEAT_THRESHOLD)
            ].sort_values('cnt', ascending=False)
            examples += list(acct_counts['event_name_clean'].head(2))

        examples = list(dict.fromkeys(e.strip() for e in examples if e.strip()))[:3]

        last_seen = ''
        if 'date' in sub.columns:
            dates = pd.to_datetime(sub['date'], errors='coerce').dropna()
            if not dates.empty:
                last_seen = str(dates.max().date())

        rows.append({
            'Account': acct,
            'Score': score,
            'Example Events': ' | '.join(examples),
            'Last Seen': last_seen,
            'Day Plural': 'Yes' if has_plural else 'No',
            'Gemini Recurring': 'Yes' if has_recurring else 'No',
            'Name Repeat': 'Yes' if has_repeat else 'No',
        })

    result = pd.DataFrame(rows).sort_values('Score', ascending=False).reset_index(drop=True)
    return result


def connect_sheets():
    """Authenticate and return the main Google Sheet."""
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

        import json
        with open("config.json") as f:
            conf = json.load(f)
        sheet_name = conf.get("sheet_name", "Instagram_Events_Master")

        main_sheet = client.open(sheet_name)
        print(f"✓ Connected to Sheet: {sheet_name}")
        return main_sheet
    except Exception as e:
        print(f"⚠ Sheets connection failed: {e}")
        return None


def write_to_sheets(main_sheet, result_df):
    """Write/overwrite the Reliable_Accounts tab."""
    if main_sheet is None or result_df.empty:
        return

    try:
        try:
            ws = main_sheet.worksheet("Reliable_Accounts")
            ws.clear()
            print("✓ Cleared existing Reliable_Accounts tab")
        except gspread.WorksheetNotFound:
            ws = main_sheet.add_worksheet("Reliable_Accounts", 1000, 10)
            print("✓ Created Reliable_Accounts tab")

        header = result_df.columns.tolist()
        rows = [header] + result_df.values.tolist()
        ws.update(rows, value_input_option='RAW')
        print(f"✓ Written {len(result_df)} accounts to Reliable_Accounts tab")
    except Exception as e:
        print(f"⚠ Sheets write error: {e}")


def refresh(main_sheet=None):
    """
    Main entry point — callable from main.py's save_data() or standalone.
    If main_sheet is provided (gspread Spreadsheet object), skips re-auth.
    """
    print("\n" + "="*60)
    print("RELIABLE ACCOUNTS ANALYSIS")
    print("="*60)

    df = load_all_csvs()
    if df.empty:
        print("No data to analyse.")
        return

    result = score_accounts(df)
    print(f"✓ {len(result)} accounts scored {MIN_SCORE}+ signals")

    if not result.empty:
        print("\nTop 10 Most Reliable Accounts:")
        print(result[['Account', 'Score', 'Example Events']].head(10).to_string(index=False))

    out_path = OUTPUTS_DIR / "reliable_accounts.csv"
    result.to_csv(out_path, index=False)
    print(f"\n✓ Saved {out_path}")

    if main_sheet is None:
        main_sheet = connect_sheets()

    write_to_sheets(main_sheet, result)
    print("="*60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reliable Accounts Detection")
    parser.add_argument("--local", action="store_true",
                        help="Skip Google Sheets sync, local CSV only")
    args = parser.parse_args()

    if args.local:
        df = load_all_csvs()
        if not df.empty:
            result = score_accounts(df)
            print(f"\n✓ {len(result)} reliable accounts found")
            print(result[['Account', 'Score', 'Example Events']].to_string(index=False))
            out_path = OUTPUTS_DIR / "reliable_accounts.csv"
            result.to_csv(out_path, index=False)
            print(f"\n✓ Saved {out_path}")
    else:
        refresh()
