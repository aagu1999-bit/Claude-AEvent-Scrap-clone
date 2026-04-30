#!/usr/bin/env python3
"""
account_hygiene.py — Periodic account health check.

Walks the `Accounts` tab of your Google Sheet, hits IG's public profile
endpoint for each handle, classifies it (real/missing/private/0_posts),
and writes a snapshot to a new `Account_Hygiene` tab. Flags handles whose
status changed since the last check (so you spot newly-broken accounts
without scanning the whole tab).

USAGE
─────
  python account_hygiene.py                   # check stale handles only (default)
  python account_hygiene.py --all             # force-check every handle this run
  python account_hygiene.py --limit 200       # cap this run at N checks
  python account_hygiene.py --days-stale 14   # only check handles not seen in N days
  python account_hygiene.py --dry-run         # don't write to sheet, just print summary

WHY STAGGER (default behavior)
──────────────────────────────
With ~850 handles and ~2 sec per IG call, a full sweep takes ~30 min and
risks IG rate-limiting our IP. Staggering — only checking handles whose
last_checked is older than N days — spreads the load and keeps the sheet
fresh without blowing through quotas. Default: check up to 100 stalest
handles per invocation. Run daily to fully cycle every 8-9 days.

OUTPUT TAB: `Account_Hygiene`
────────────────────────────
Always reflects the FULL account list. Each row:
  handle | status | followers | post_count | last_checked
  | previous_status | newly_broken | notes
- "newly_broken" = "yes" if status flipped from real → (missing|private|error|0_posts)
- "notes" carries any quick context (e.g. "skipped — not stale yet")
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from ig_lookup import normalize_handle, check_handle


# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ACCOUNTS_TAB = "Accounts"
HYGIENE_TAB = "Account_Hygiene"

HEADER = [
    "handle",
    "status",                  # real | missing | private | unknown | error | not_yet_checked
    "followers",
    "post_count",
    "last_checked",            # YYYY-MM-DD HH:MM:SS or empty
    "previous_status",         # what it was before this run (empty if first time)
    "newly_broken",            # "yes" if real -> bad this run, else ""
    "notes",
]

DEFAULT_LIMIT = 100            # max IG calls per invocation (when not --all)
DEFAULT_STALE_DAYS = 7         # re-check a handle if last_checked older than this
DEFAULT_DELAY_SEC = 4.0        # sleep between IG calls (4s = friendlier to cloud IPs like Replit)
BACKOFF_THRESHOLD = 3          # consecutive 401/error before extended sleep
BACKOFF_SLEEP_SEC = 60


# ─────────────────────────────────────────────────────────────
# Sheets connection
# ─────────────────────────────────────────────────────────────


def connect_sheets():
    """Open the master spreadsheet. Exits on failure (this script needs sheets)."""
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account file not found: {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    return client.open(SHEET_NAME)


def read_accounts(spreadsheet):
    """Return list of normalized handles from the Accounts tab."""
    ws = spreadsheet.worksheet(ACCOUNTS_TAB)
    col = ws.col_values(1)
    if col and col[0].strip().lower() in ("username", "handle", "account"):
        col = col[1:]
    return [normalize_handle(h) for h in col if h.strip()]


def read_hygiene_snapshot(spreadsheet):
    """
    Read existing Account_Hygiene tab. Returns dict: handle -> row dict.
    Empty dict if tab missing.
    """
    try:
        ws = spreadsheet.worksheet(HYGIENE_TAB)
    except gspread.WorksheetNotFound:
        return {}
    rows = ws.get_all_values()
    if not rows or len(rows) < 2:
        return {}
    header, *data = rows
    out = {}
    for r in data:
        if not r or not r[0].strip():
            continue
        rec = {header[i]: r[i] if i < len(r) else "" for i in range(len(header))}
        out[normalize_handle(rec.get("handle", ""))] = rec
    return out


def get_or_create_hygiene_tab(spreadsheet, rows_needed: int):
    """Get HYGIENE_TAB worksheet; create with header if missing."""
    try:
        return spreadsheet.worksheet(HYGIENE_TAB), False
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=HYGIENE_TAB,
            rows=max(rows_needed + 50, 100),
            cols=len(HEADER),
        )
        ws.append_row(HEADER, value_input_option="USER_ENTERED")
        return ws, True


# ─────────────────────────────────────────────────────────────
# Stale picker
# ─────────────────────────────────────────────────────────────


def parse_last_checked(s: str):
    """Parse 'YYYY-MM-DD HH:MM:SS' → datetime, else None."""
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    return None


def pick_handles_to_check(all_handles: list, prior: dict, stale_days: int, limit: int, force_all: bool):
    """
    Decide which handles to probe this run.

    Priority order (when not --all):
      1. Handles never checked before (no row in prior snapshot)
      2. Handles whose status was bad last time (real → reverify, broken → maybe-fixed?)
      3. Handles whose last_checked is older than stale_days, oldest first

    Returns list of handles up to `limit`.
    """
    if force_all:
        return all_handles[:limit] if limit else all_handles

    cutoff = datetime.now() - timedelta(days=stale_days)
    never_checked, broken_recheck, stale = [], [], []

    for h in all_handles:
        rec = prior.get(h)
        if not rec:
            never_checked.append(h)
            continue
        last = parse_last_checked(rec.get("last_checked", ""))
        status = rec.get("status", "")
        if status in ("missing", "error", "unknown"):
            # Re-check broken ones at higher cadence (in case of typo fixes or transient errors)
            if not last or last < cutoff:
                broken_recheck.append((last or datetime.min, h))
        elif not last or last < cutoff:
            stale.append((last or datetime.min, h))

    # Oldest first within each bucket
    broken_recheck.sort(key=lambda x: x[0])
    stale.sort(key=lambda x: x[0])

    queue = never_checked + [h for _, h in broken_recheck] + [h for _, h in stale]
    return queue[:limit] if limit else queue


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────


def parse_args():
    p = argparse.ArgumentParser(description="Account hygiene loop")
    p.add_argument("--all", action="store_true", help="Check every handle this run (no stagger)")
    p.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help=f"Max checks this run (default {DEFAULT_LIMIT}); 0 = unlimited")
    p.add_argument("--days-stale", type=int, default=DEFAULT_STALE_DAYS, help=f"Treat as stale after N days (default {DEFAULT_STALE_DAYS})")
    p.add_argument("--delay", type=float, default=DEFAULT_DELAY_SEC, help=f"Sleep seconds between IG calls (default {DEFAULT_DELAY_SEC})")
    p.add_argument("--dry-run", action="store_true", help="Don't write to sheet")
    return p.parse_args()


def main():
    args = parse_args()
    started_at = datetime.now()

    print("=" * 60)
    print("🩺 ACCOUNT HYGIENE")
    print(f"   started: {started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    sh = connect_sheets()
    accounts = read_accounts(sh)
    prior = read_hygiene_snapshot(sh)
    print(f"  Accounts in tab:        {len(accounts)}")
    print(f"  Prior hygiene records:  {len(prior)}")

    # --all overrides --limit (unlimited). Otherwise use --limit (0 = unlimited).
    if args.all:
        effective_limit = len(accounts)
    elif args.limit <= 0:
        effective_limit = len(accounts)
    else:
        effective_limit = args.limit

    to_check = pick_handles_to_check(
        accounts, prior, args.days_stale,
        effective_limit,
        force_all=args.all,
    )
    print(f"  Will check this run:    {len(to_check)}  (mode: {'ALL' if args.all else f'stale > {args.days_stale}d, cap={args.limit}'})")

    if not to_check:
        print("\n  Nothing stale — exiting clean.")
        return

    # Probe each handle, with backoff if we get hammered
    results = {}
    consecutive_fails = 0
    for i, h in enumerate(to_check, 1):
        info = check_handle(h)
        results[h] = info
        if info["status"] in ("error", "unknown"):
            consecutive_fails += 1
            if consecutive_fails >= BACKOFF_THRESHOLD:
                print(f"  ⚠ {consecutive_fails} consecutive errors — backing off {BACKOFF_SLEEP_SEC}s")
                time.sleep(BACKOFF_SLEEP_SEC)
                consecutive_fails = 0
        else:
            consecutive_fails = 0
        time.sleep(args.delay)
        if i % 25 == 0:
            ok = sum(1 for r in results.values() if r["status"] in ("real", "missing", "private"))
            print(f"  {i}/{len(to_check)}  resolved={ok}")

    # Build new full snapshot — every handle gets a row, even if not checked this run
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    snapshot = []
    newly_broken_count = 0
    status_changed_count = 0
    for h in accounts:
        prior_rec = prior.get(h, {})
        prior_status = prior_rec.get("status", "")
        if h in results:
            new = results[h]
            row_status = new["status"]
            row_followers = "" if new["followers"] is None else new["followers"]
            row_posts = "" if new["posts"] is None else new["posts"]
            last_checked = now_str
            previous_status = prior_status
            newly_broken = "yes" if (prior_status == "real" and row_status in ("missing", "private", "error")) else ""
            if newly_broken == "yes":
                newly_broken_count += 1
            if prior_status and prior_status != row_status:
                status_changed_count += 1
            notes = ""
        else:
            # Carry forward existing record unchanged; mark not_yet_checked if no prior
            row_status = prior_status or "not_yet_checked"
            row_followers = prior_rec.get("followers", "")
            row_posts = prior_rec.get("post_count", "")
            last_checked = prior_rec.get("last_checked", "")
            previous_status = prior_rec.get("previous_status", "")
            newly_broken = prior_rec.get("newly_broken", "")
            notes = "skipped — not stale yet" if prior_rec else "never checked yet"

        snapshot.append([h, row_status, row_followers, row_posts,
                         last_checked, previous_status, newly_broken, notes])

    # Console summary
    by_status = {}
    for row in snapshot:
        by_status[row[1]] = by_status.get(row[1], 0) + 1
    print("\n  Snapshot status breakdown:")
    for status, n in sorted(by_status.items(), key=lambda kv: -kv[1]):
        print(f"    {status:<20} {n}")
    print(f"\n  Status changes this run:  {status_changed_count}")
    print(f"  NEWLY BROKEN this run:    {newly_broken_count}")

    if args.dry_run:
        print("\n  --dry-run: skipping sheet write")
        return

    # Write
    ws, created = get_or_create_hygiene_tab(sh, len(snapshot))
    if created:
        print(f"\n  ✓ Created '{HYGIENE_TAB}' tab")

    values = [HEADER] + snapshot
    # Pad to overwrite any leftover trailing rows from a previous larger snapshot
    existing_rows = ws.row_count
    while len(values) < existing_rows:
        values.append([""] * len(HEADER))
    ws.update("A1", values, value_input_option="USER_ENTERED")
    print(f"  ✓ Wrote {len(snapshot)} rows to '{HYGIENE_TAB}'")
    print("=" * 60)


if __name__ == "__main__":
    main()
