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

Reliable accounts that meet the promotion bar are also appended to the live
Accounts tab so they get scraped on subsequent runs — see
docs/decisions/0001-reliable-to-active-promotion.md.

Usage:
    python recurring_accounts.py                    # full run + sheets sync + promotion
    python recurring_accounts.py --local            # local CSV only, no sheets
    python recurring_accounts.py --dry-run          # preview promotions, no writes
    python recurring_accounts.py --threshold 5     # require ≥5 occurrences
    python recurring_accounts.py --recency-days 90 # require last seen ≤90 days ago
    python recurring_accounts.py --no-promote       # skip the Accounts-tab promotion step
"""

import os
import re
import sys
import glob
import json
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("SERVICE_ACCOUNT_FILE") or "apt-mark-468506-u9-ec44cabc7335 copy.json"
OUTPUTS_DIR = Path("outputs")
LOCAL_CSV = OUTPUTS_DIR / "reliable_accounts.csv"
BLOCKLIST_FILE = Path("promotion_blocklist.json")


def _load_promotion_blocklist():
    """Read the persistent blocklist of handles that should never be proposed
    for promotion. Survives across runs so you only have to reject a handle
    once. Returns a lowercase set of handles (empty if file missing/malformed).
    """
    if not BLOCKLIST_FILE.exists():
        return set()
    try:
        with open(BLOCKLIST_FILE) as f:
            data = json.load(f)
        return {h.strip().lower() for h in data.get("handles", []) if h.strip()}
    except Exception as e:
        print(f"⚠ Could not read {BLOCKLIST_FILE}: {e}")
        return set()

TAB_HEADERS = [
    "Account", "Display Name", "Example Series Names",
    "Pattern Types Matched", "Occurrences", "Distinct Posts"
]

POST_URL_CANDIDATES = ("instagram_post_url", "post_url", "INSTAGRAM POST URL", "POST URL")
POST_ID_CANDIDATES = ("post_id", "POST ID")

DAY_PLURAL_RE = re.compile(
    r'\b(Mondays|Tuesdays|Wednesdays|Thursdays|Fridays|Saturdays|Sundays)\b',
    re.IGNORECASE
)

RECURRENCE_KW_RE = re.compile(
    r'\b(Weekly|Every|Nightly|Nights|Series|Season)\b',
    re.IGNORECASE
)

BRANDED_REPEAT_THRESHOLD = 3

DEFAULT_PROMOTION_THRESHOLD = 3
DEFAULT_RECENCY_DAYS = 180

DATE_COL_CANDIDATES = (
    "post_date",
    "extraction_timestamp",
    "PROCESSED TIMESTAMP",
)


def _parse_iso_to_date(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "nat", "none"):
        return None
    s_clean = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s_clean).date()
    except (ValueError, TypeError):
        pass
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s.split("+")[0].strip(), fmt).date()
        except ValueError:
            continue
    return None


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

            date_col = next(
                (c for c in df.columns if c in DATE_COL_CANDIDATES), None
            )
            url_col = next((c for c in df.columns if c in POST_URL_CANDIDATES), None)
            id_col = next((c for c in df.columns if c in POST_ID_CANDIDATES), None)

            post_key = None
            if url_col:
                post_key = df[url_col].fillna('').astype(str).str.strip()
            elif id_col:
                post_key = df[id_col].fillna('').astype(str).str.strip()

            slim = pd.DataFrame({
                'handle': df[handle_col].fillna('').astype(str).str.strip().str.lower(),
                'event_name': df[name_col].fillna('').astype(str).str.strip(),
                'display_name': df[display_col].fillna('').astype(str).str.strip()
                    if display_col else '',
                'source_file': os.path.basename(path),
                'seen_at': df[date_col].apply(_parse_iso_to_date)
                    if date_col else pd.Series([None] * len(df)),
                'post_key': post_key if post_key is not None else pd.Series([''] * len(df)),
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

        last_seen = None
        if 'seen_at' in group.columns:
            valid_dates = [d for d in group['seen_at'] if d is not None]
            if valid_dates:
                last_seen = max(valid_dates)

        distinct_posts = 0
        if 'post_key' in group.columns:
            keys = group['post_key'].dropna()
            keys = keys[keys.astype(str).str.len() > 0]
            distinct_posts = keys.nunique()

        rows.append({
            'Account': handle,
            'Display Name': display_name,
            'Example Series Names': '; '.join(matched_names[:5]),
            'Pattern Types Matched': ', '.join(sorted(matched_patterns)),
            'Occurrences': len(group),
            'Distinct Posts': int(distinct_posts),
            '_last_seen': last_seen,
        })

    if not rows:
        return pd.DataFrame(columns=TAB_HEADERS + ['_last_seen'])

    result = (
        pd.DataFrame(rows, columns=TAB_HEADERS + ['_last_seen'])
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


def _read_active_accounts(main_sheet):
    """Return (worksheet, [usernames]) for the live Accounts tab, or (None, [])."""
    try:
        ws = main_sheet.worksheet("Accounts")
    except gspread.WorksheetNotFound:
        print("⚠ Accounts tab not found — skipping promotion step")
        return None, []
    try:
        col = ws.col_values(1)
    except Exception as e:
        print(f"⚠ Could not read Accounts tab: {e}")
        return ws, []
    if col and col[0].strip().lower() == "username":
        col = col[1:]
    return ws, [u.strip() for u in col if u.strip()]


def _levenshtein(a, b):
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            curr.append(min(
                curr[j - 1] + 1,
                prev[j] + 1,
                prev[j - 1] + (ca != cb),
            ))
        prev = curr
    return prev[-1]


def _detect_typos(active, reliable_handles, max_dist=2):
    """
    Find handles in `active` that look like near-misspellings of a handle in
    `reliable_handles` (Levenshtein ≤ max_dist, but not equal). These are
    candidates for manual correction — silent fetch failures from typo'd
    handles are how the interludseries→interludeseries bug went unnoticed.

    Returns list of (active_handle, suggested_handle, distance) tuples.
    """
    active_lc = {h.lower(): h for h in active}
    reliable_set = {h.lower() for h in reliable_handles}
    suspects = []
    for a_lc, a_orig in active_lc.items():
        if a_lc in reliable_set:
            continue
        best = None
        for r in reliable_set:
            if abs(len(r) - len(a_lc)) > max_dist:
                continue
            d = _levenshtein(a_lc, r)
            if d <= max_dist and (best is None or d < best[1]):
                best = (r, d)
        if best:
            suspects.append((a_orig, best[0], best[1]))
    return sorted(suspects, key=lambda t: (t[2], t[0]))


def _promote_to_active_accounts(
    main_sheet,
    result_df,
    threshold=DEFAULT_PROMOTION_THRESHOLD,
    recency_days=DEFAULT_RECENCY_DAYS,
    dry_run=False,
):
    """
    Append qualifying reliable accounts into the live Accounts tab.

    An account qualifies when ALL of:
      - Pattern-matched as recurring (already filtered into result_df).
      - Distinct Posts >= threshold. We gate on distinct posts (deduped
        on post URL/ID), NOT raw row count, because cumulative weekly
        re-exports inflate Occurrences. An account that produced 1 post
        repeated across 30 weekly exports has Occurrences=30 but
        Distinct Posts=1 — and is not strong promotion evidence.
      - _last_seen within `recency_days` (or no date available — kept,
        not penalized for stale data shape).
      - Not already present in the Accounts tab (case-insensitive).

    See docs/decisions/0001-reliable-to-active-promotion.md for context.
    """
    if result_df.empty:
        return [], []

    ws, active = _read_active_accounts(main_sheet)
    active_lc = {h.lower() for h in active}
    blocklist = _load_promotion_blocklist()

    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=recency_days)
              if recency_days else None)

    promoted = []
    skipped = {'already_active': 0, 'below_threshold': [], 'dormant': [], 'blocked': []}

    for _, row in result_df.iterrows():
        handle = str(row['Account']).strip().lower()
        if not handle:
            continue
        if handle in blocklist:
            skipped['blocked'].append(handle)
            continue
        if handle in active_lc:
            skipped['already_active'] += 1
            continue
        distinct = int(row.get('Distinct Posts', 0) or 0)
        if distinct < threshold:
            skipped['below_threshold'].append((handle, distinct))
            continue
        last_seen = row.get('_last_seen')
        if cutoff and last_seen and last_seen < cutoff:
            skipped['dormant'].append((handle, last_seen))
            continue
        promoted.append({
            'handle': handle,
            'display': str(row.get('Display Name', '')),
            'distinct': distinct,
            'occ': int(row.get('Occurrences', 0) or 0),
            'last_seen': last_seen.isoformat() if last_seen else 'unknown',
            'sample': str(row.get('Example Series Names', ''))[:60],
        })

    print("\n" + "─" * 60)
    print(f"PROMOTION{' (DRY RUN)' if dry_run else ''} — "
          f"distinct posts ≥ {threshold}, recency ≤ {recency_days}d")
    print("─" * 60)
    if promoted:
        print(f"  Would promote {len(promoted)} accounts:" if dry_run
              else f"  Promoting {len(promoted)} accounts:")
        for p in promoted:
            print(f"    + {p['handle']:30s} distinct={p['distinct']:<4} "
                  f"last={p['last_seen']:<12} {p['sample']}")
    else:
        print("  No accounts qualify for promotion.")
    print(f"  Skipped — already active:    {skipped['already_active']}")
    print(f"  Skipped — blocklisted:       {len(skipped['blocked'])}")
    print(f"  Skipped — below threshold:   {len(skipped['below_threshold'])}")
    print(f"  Skipped — dormant >{recency_days}d:    {len(skipped['dormant'])}")
    if skipped['dormant'][:5]:
        for h, d in skipped['dormant'][:5]:
            print(f"    - {h} (last seen {d})")
        if len(skipped['dormant']) > 5:
            print(f"    ... and {len(skipped['dormant']) - 5} more")

    if not dry_run and ws is not None and promoted:
        try:
            new_rows = [[p['handle']] for p in promoted]
            ws.append_rows(new_rows, value_input_option='RAW')
            print(f"  ✓ Appended {len(promoted)} rows to Accounts tab")
        except Exception as e:
            print(f"  ⚠ Append failed: {e}")
    elif dry_run and promoted:
        print("  ℹ️  Dry run — no rows written to the Accounts tab.")

    return promoted, skipped


def _report_typos(active, reliable_handles):
    """Print suspected typos in the active Accounts list."""
    suspects = _detect_typos(active, reliable_handles)
    if not suspects:
        return suspects
    print("\n" + "─" * 60)
    print(f"TYPO SUSPECTS — {len(suspects)} active handle(s) look like a "
          "near-miss of a known reliable handle")
    print("─" * 60)
    print("  Manual review recommended. Each line shows the active handle, "
          "the closest reliable\n  handle, and the edit distance. "
          "Distance ≤ 2 is usually a typo (e.g. "
          "interludeseries→interludseries, dist=1).")
    for active_h, suggested_h, dist in suspects:
        print(f"    {active_h:30s} → {suggested_h:30s} (dist={dist})")
    return suspects


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


def refresh(
    main_sheet=None,
    promote=True,
    dry_run=False,
    threshold=DEFAULT_PROMOTION_THRESHOLD,
    recency_days=DEFAULT_RECENCY_DAYS,
):
    """
    Run detection, save local CSV, update Google Sheets, and (optionally)
    promote qualifying reliable accounts into the live Accounts tab.

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
    result.drop(columns=['_last_seen'], errors='ignore').to_csv(LOCAL_CSV, index=False)
    print(f"\n✓ Saved {LOCAL_CSV}")

    if main_sheet is None:
        main_sheet = _connect_sheets()

    if main_sheet is not None:
        try:
            _write_to_sheets(
                main_sheet,
                result.drop(columns=['_last_seen'], errors='ignore'),
            )
        except Exception as e:
            print(f"⚠ Sheets write error: {e}")

        if promote and not result.empty:
            try:
                _, active = _read_active_accounts(main_sheet)
                if active:
                    _report_typos(active, result['Account'].tolist())
                _promote_to_active_accounts(
                    main_sheet, result,
                    threshold=threshold,
                    recency_days=recency_days,
                    dry_run=dry_run,
                )
            except Exception as e:
                print(f"⚠ Promotion step failed: {e}")

    print("=" * 60)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reliable Accounts — Recurring Series Detection")
    parser.add_argument("--local", action="store_true",
                        help="Skip Google Sheets sync, save local CSV only")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run promotion in preview mode — print what would be added "
                             "to the Accounts tab without writing.")
    parser.add_argument("--no-promote", action="store_true",
                        help="Skip the Accounts-tab promotion step entirely.")
    parser.add_argument("--threshold", type=int, default=DEFAULT_PROMOTION_THRESHOLD,
                        help=f"Min Occurrences to qualify (default {DEFAULT_PROMOTION_THRESHOLD}).")
    parser.add_argument("--recency-days", type=int, default=DEFAULT_RECENCY_DAYS,
                        help=f"Skip accounts last seen more than N days ago "
                             f"(default {DEFAULT_RECENCY_DAYS}; 0 disables the filter).")
    args = parser.parse_args()

    if args.local:
        df = _load_all_csvs()
        if not df.empty:
            result = detect_recurring_accounts(df)
            print(f"\n✓ {len(result)} recurring-series accounts found")
            if not result.empty:
                print(result.drop(columns=['_last_seen'], errors='ignore').to_string(index=False))
            OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
            result.drop(columns=['_last_seen'], errors='ignore').to_csv(LOCAL_CSV, index=False)
            print(f"\n✓ Saved {LOCAL_CSV}")
    else:
        refresh(
            promote=not args.no_promote,
            dry_run=args.dry_run,
            threshold=args.threshold,
            recency_days=args.recency_days,
        )
