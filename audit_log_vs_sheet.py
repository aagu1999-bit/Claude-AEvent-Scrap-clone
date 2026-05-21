#!/usr/bin/env python3
"""
audit_log_vs_sheet.py — read-only census of log vs sheet drift.

Answers two questions across one or more run logs:

  1. Processed_Log coverage
     How many post_ids did the logs see that AREN'T in Processed_Log?
     How many post_ids are in Processed_Log but appear in NO log?

  2. All_Events coverage
     How many events did the logs emit that have NO matching All_Events row?
     How many All_Events rows have no matching log event?
     (Match key: POST ID + EVENT NAME + DATE, same as repair_flags_from_log.)

Does NOT modify the sheet. No --apply flag. Always read-only.
Pure census tool — no derivation, no formatting, no flag logic.

USAGE
─────
  # All logs in outputs/ (typical)
  python audit_log_vs_sheet.py

  # Specific logs
  python audit_log_vs_sheet.py --log outputs/run_*.log

  # Sample the mismatches (default 10 rows; --samples 0 to skip)
  python audit_log_vs_sheet.py --samples 25

OUTPUT
──────
- Console: aligned counts table for both comparisons
- CSV samples: outputs/audit_log_vs_sheet_<ts>.csv with up to N mismatches
  from each bucket, so you can spot-check WHY they don't match (purged?
  hand-edited? name drift?).
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Reuse the parser + helpers from repair_flags_from_log so log handling
# stays identical (auto-detects new vs old format, etc.).
from repair_flags_from_log import (
    parse_log, detect_log_format, normalize_date_for_key,
    SERVICE_ACCOUNT_FILE, SHEET_NAME, ALL_EVENTS_TAB,
)

PROCESSED_LOG_TAB = "Processed_Log"

# Need ALL post_ids that were attempted in a log — not just ones that
# produced events. parse_log() only records posts with events or flags,
# so we make a second pass with this regex to catch every Processing line.
RE_PROCESSING_ANY = re.compile(r'\[\d+/\d+\]\s+Processing post:\s+(\S+)')


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


def collect_post_ids_from_log(log_path: Path) -> set:
    """Every post_id mentioned in any Processing line in the log, regardless
    of whether it produced events. Used for Processed_Log comparison."""
    ids = set()
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                m = RE_PROCESSING_ANY.search(line)
                if m:
                    ids.add(m.group(1).strip())
    except OSError as e:
        print(f"  ⚠ Could not read {log_path}: {e}", file=sys.stderr)
    return ids


def main():
    p = argparse.ArgumentParser(
        description="Read-only audit of log coverage vs sheet rows.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--log", action="append", default=[],
                   help="Path to a run_*.log file. Repeatable.")
    p.add_argument("--logs-dir", default="outputs",
                   help="Directory containing run_*.log files (default: outputs/).")
    p.add_argument("--samples", type=int, default=10,
                   help="Sample size for mismatch CSV per bucket (default: 10).")
    args = p.parse_args()

    # Resolve log paths
    log_paths = [Path(x) for x in args.log]
    if args.logs_dir:
        log_paths.extend(sorted(Path(args.logs_dir).glob("run_*.log")))
    log_paths = [pp for pp in log_paths if pp.exists()]
    log_paths = sorted(set(log_paths))  # dedup + stable order

    if not log_paths:
        print("❌ No log files found.", file=sys.stderr)
        sys.exit(1)

    print(f"━━━ audit_log_vs_sheet.py ━━━")
    print(f"  Logs ({len(log_paths)}):")
    for lp in log_paths:
        fmt = detect_log_format(lp)
        marker = '✓' if fmt == 'new' else '⚠'
        print(f"    {marker} [{fmt:3s}] {lp.name}")
    print()

    # ── COLLECT FROM LOGS ─────────────────────────────────────────────
    print("  Parsing logs...")
    log_post_ids = set()         # every post_id seen in any log (Processing lines)
    log_event_keys = set()       # (post_id, event_name_upper, date_iso) for events emitted
    sample_event_by_key = {}     # for CSV sampling

    for lp in log_paths:
        # All post_ids (incl. ones with no events)
        log_post_ids |= collect_post_ids_from_log(lp)
        # Events parsed by the format-aware parser
        for rec in parse_log(lp):
            for e in rec['events']:
                name_upper = e['name'].strip().upper()
                date_iso = normalize_date_for_key(e.get('date'))
                key = (rec['post_id'], name_upper, date_iso)
                log_event_keys.add(key)
                sample_event_by_key.setdefault(key, {
                    'post_id':  rec['post_id'],
                    'event':    e['name'],
                    'date':     e.get('date') or '',
                    'log_file': lp.name,
                })

    print(f"    posts attempted (any Processing line): {len(log_post_ids):>7,}")
    print(f"    events emitted (canonical key):        {len(log_event_keys):>7,}")
    print()

    # ── COLLECT FROM SHEET ────────────────────────────────────────────
    print("  Connecting to sheet...")
    sh = setup_sheet()

    # Processed_Log
    print(f"  Reading {PROCESSED_LOG_TAB}...")
    try:
        pl = sh.worksheet(PROCESSED_LOG_TAB)
        pl_values = pl.get_all_values()
    except gspread.WorksheetNotFound:
        print(f"  ⚠ {PROCESSED_LOG_TAB} not found — skipping that comparison.")
        pl_values = []

    pl_post_ids = set()
    if pl_values:
        pl_header = pl_values[0]
        pid_idx = col_index(pl_header, 'POST ID') or 0
        for row in pl_values[1:]:
            if len(row) > pid_idx:
                pid = row[pid_idx].strip()
                if pid:
                    pl_post_ids.add(pid)
    print(f"    Processed_Log post_ids: {len(pl_post_ids):>7,}")

    # All_Events
    print(f"  Reading {ALL_EVENTS_TAB}...")
    ae = sh.worksheet(ALL_EVENTS_TAB)
    ae_values = ae.get_all_values()
    ae_header = ae_values[0] if ae_values else []
    ae_data = ae_values[1:] if ae_values else []

    cols = {
        'POST ID':    col_index(ae_header, 'POST ID'),
        'EVENT NAME': col_index(ae_header, 'EVENT NAME'),
        'DATE':       col_index(ae_header, 'DATE'),
    }
    missing_cols = [k for k, v in cols.items() if v is None]
    if missing_cols:
        print(f"❌ Missing required All_Events columns: {missing_cols}", file=sys.stderr)
        sys.exit(1)

    sheet_event_keys = set()
    sheet_sample_by_key = {}
    for i, row in enumerate(ae_data, start=2):
        if len(row) <= max(cols.values()):
            continue
        pid  = (row[cols['POST ID']] or '').strip()
        name = (row[cols['EVENT NAME']] or '').strip().upper()
        date_iso = normalize_date_for_key(row[cols['DATE']])
        if pid and name:
            key = (pid, name, date_iso)
            sheet_event_keys.add(key)
            sheet_sample_by_key.setdefault(key, {
                'post_id':   pid,
                'event':     (row[cols['EVENT NAME']] or '').strip(),
                'date':      (row[cols['DATE']] or '').strip(),
                'sheet_row': i,
            })

    print(f"    All_Events rows (unique by key): {len(sheet_event_keys):>7,}")
    print(f"    All_Events total data rows:      {len(ae_data):>7,}")
    print()

    # ── COMPARE: Processed_Log vs logs ───────────────────────────────
    in_log_not_in_pl = log_post_ids - pl_post_ids
    in_pl_not_in_log = pl_post_ids - log_post_ids
    in_both_pl       = log_post_ids & pl_post_ids

    # ── COMPARE: All_Events vs logs ──────────────────────────────────
    in_log_not_in_ae = log_event_keys - sheet_event_keys
    in_ae_not_in_log = sheet_event_keys - log_event_keys
    in_both_ae       = log_event_keys & sheet_event_keys

    # ── REPORT ───────────────────────────────────────────────────────
    print(f"━━━ PROCESSED_LOG (post_id sets) ━━━")
    print(f"  In log, NOT in Processed_Log: {len(in_log_not_in_pl):>7,}")
    print(f"  In Processed_Log, NOT in log: {len(in_pl_not_in_log):>7,}")
    print(f"  In both:                      {len(in_both_pl):>7,}")
    print()
    print(f"━━━ ALL_EVENTS (POST ID + EVENT NAME + DATE) ━━━")
    print(f"  In log, NOT in All_Events:    {len(in_log_not_in_ae):>7,}")
    print(f"  In All_Events, NOT in log:    {len(in_ae_not_in_log):>7,}")
    print(f"  In both:                      {len(in_both_ae):>7,}")
    print()

    # Interpretation hints (heuristic, not authoritative)
    print(f"━━━ READ ━━━")
    if len(in_log_not_in_pl) > 0:
        print(f"  • {len(in_log_not_in_pl)} log post_ids missing from Processed_Log")
        print(f"    → posts that were attempted but never claimed in dedup. Possible")
        print(f"      causes: pipeline crashed before flush, or older runs whose logs")
        print(f"      pre-date the Processed_Log sheet entry. Check the audit CSV.")
    if len(in_pl_not_in_log) > 0:
        print(f"  • {len(in_pl_not_in_log)} Processed_Log post_ids missing from any log")
        print(f"    → either pre-archived logs you don't have, or posts processed in")
        print(f"      runs whose logs were never saved. Older claims, mostly safe.")
    if len(in_log_not_in_ae) > 0:
        print(f"  • {len(in_log_not_in_ae)} log events have no matching All_Events row")
        print(f"    → most common cause: purge_events_by_date.py removed them. Other")
        print(f"      causes: event name was hand-edited in the sheet; date format")
        print(f"      mismatch; events that failed to write at extract time.")
    if len(in_ae_not_in_log) > 0:
        ratio = len(in_ae_not_in_log) / max(1, len(sheet_event_keys))
        print(f"  • {len(in_ae_not_in_log)} All_Events rows ({ratio:.0%}) have no log match")
        print(f"    → expected: these are from runs whose logs you don't have in this")
        print(f"      sample. Add more logs to --logs-dir to shrink this number.")
    print()

    # ── CSV samples ──────────────────────────────────────────────────
    if args.samples > 0:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = Path("outputs") / f"audit_log_vs_sheet_{ts}.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, 'w', newline='') as f:
            w = csv.writer(f)
            w.writerow(['bucket', 'post_id', 'event_name', 'date', 'sheet_row', 'log_file'])

            for pid in list(in_log_not_in_pl)[:args.samples]:
                w.writerow(['log_post_not_in_processed_log', pid, '', '', '', ''])
            for pid in list(in_pl_not_in_log)[:args.samples]:
                w.writerow(['processed_log_not_in_any_log', pid, '', '', '', ''])
            for key in list(in_log_not_in_ae)[:args.samples]:
                s = sample_event_by_key.get(key, {})
                w.writerow(['log_event_not_in_all_events', s.get('post_id',''),
                            s.get('event',''), s.get('date',''), '', s.get('log_file','')])
            for key in list(in_ae_not_in_log)[:args.samples]:
                s = sheet_sample_by_key.get(key, {})
                w.writerow(['all_events_not_in_any_log', s.get('post_id',''),
                            s.get('event',''), s.get('date',''), s.get('sheet_row',''), ''])
        print(f"  Sample CSV ({args.samples} per bucket): {csv_path}")


if __name__ == "__main__":
    main()
