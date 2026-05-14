#!/usr/bin/env python3
"""
repair_flags_from_log.py -- re-derive QUALITY_FLAGS column + cell highlighting
in All_Events from one or more main.py run logs.

USE CASE
--------
The QUALITY_FLAGS column should reflect what the extraction sanity checks
said at extract time. Over time it drifts because:
  - Cell highlighting is applied at write time and never re-applied
  - QUALITY_FLAGS column gets manually edited (or never written for old rows)
  - FLAG_TO_COLUMNS mapping changes in code

The run log (outputs/run_*.log) is the canonical record of what main.py
actually saw. This tool parses run logs, derives the correct per-event
flags, matches to All_Events rows by (POST ID, EVENT NAME, DATE), and:
  - Updates QUALITY_FLAGS cell text where it differs
  - Re-applies cell highlighting per current FLAG_TO_COLUMNS

LOG FORMATS (mixed support)
---------------------------
The tool handles a MIX of two log formats produced by main.py over time.
Both can appear in the same logs directory; per-post, the format used by
that post's most-recent log entry wins.

(1) EVENT-LEVEL  (new, preferred)
    Per event, main.py emits a marker line plus bullet fields:

        [Tier 1: Flash-Lite caption-only (no Vision)]
        ✦ [flash_caption] 'Bad Bunny Night' | 2026-05-22
            • Venue: Birch Hoboken
            • Confidence: 1.0
            • Flags: none

        ✦ [flash_caption] 'Seafood Thursday' | 2026-04-30
            • Venue: First Republic Lounge
            • Confidence: 1.0
            • Flags: DATE_DAY_MISMATCH

    The 'Flags:' bullet is authoritative for that event. 'Flags: none'
    means "evaluated cleanly at event level" -- the repair tool trusts
    it directly and does NOT fall back to conservative post-level
    spreading for this post.

(2) POST-LEVEL  (legacy)
    Older logs use:

        ✓ EVENT FOUND: Watermelon & Feta Salad
          • Date: 2026-05-01
          • Venue: Raza's at Hamilton
          • Confidence: 1.0

    or for multi-event posts:

        🎉 MULTIPLE EVENTS FOUND: 5 events extracted!
          1. The Dirty German 50K
             Date: 2026-05-09
             Confidence: 0.8
          2. Cooper Loops
             Date: 2026-05-09
             Confidence: 0.9
          ...

    plus a separate post-level flags line:

        ↳ informational flags: ['CITY_NOT_IN_NJ_LOOKUP']

    For these the tool falls back to the conservative derivation below.

CONSERVATIVE DERIVATION (post-level fallback only)
--------------------------------------------------
For each event extracted from a POST-LEVEL log, determine its flag set:

(A) Always-apply post-level flags (apply to ALL events from that post):
    CALENDAR_LOW_EVENTS, CAROUSEL_LOW_EVENTS, OCR_RICH_LOW_EVENTS,
    ACCOUNT_PATTERN_DROP, NO_GROUNDING

(B) Per-event derivation from log fields:
    MISSING_DATE     <- event Date == None
    LOW_CONFIDENCE   <- event Confidence < 0.5

(C) Per-event conditional via sheet field check (only if log fired):
    CITY_NOT_IN_NJ_LOOKUP  <- log fired AND sheet CITY non-empty
    REGION_AUTOFIXED       <- log fired AND sheet SECTION OF NJ non-empty
    VENUE_CITY_MISMATCH    <- log fired AND sheet VENUE NAME + CITY both set
    PAST_DATE              <- log fired AND event has earliest date in post
    FAR_FUTURE_DATE        <- log fired AND event has latest date in post

(D) Skipped by default:
    DATE_DAY_MISMATCH -- needs original caption text to verify; cannot
    be cleanly derived from log + sheet alone.

USAGE
-----
  # Dry-run against one log
  python repair_flags_from_log.py --log outputs/run_20260514_160556.log

  # All logs in outputs/, dry-run
  python repair_flags_from_log.py --logs-dir outputs/

  # Apply
  python repair_flags_from_log.py --log outputs/run_20260514_160556.log --apply

  # Limit scope to recent rows
  python repair_flags_from_log.py --log outputs/run_*.log --after-row 18000

OUTPUT
------
  - Console: per-status counts (matched/conflict/log-only/sheet-only) and
    per-source counts (event-level vs post-level conservative)
  - CSV audit: outputs/repair_flags_<ts>.csv with every row + before/after
              plus a 'source' column ('event-level' or 'post-level')
  - (with --apply) Sheet writes: QUALITY_FLAGS text + cell formatting

SAFETY
------
  - Dry-run by default; --apply required to write
  - Only TOUCHES rows present in the log being parsed (won't blank out rows
    from other runs unless --strip-extra is set)
  - Preserves manual additions in QUALITY_FLAGS unless --strip-extra
"""

import argparse
import csv
import glob
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from extraction_core import FLAG_TO_COLUMNS, FLAG_BG_COLOR, load_nj_municipalities

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"

WHITE_BG = {"red": 1.0, "green": 1.0, "blue": 1.0}
FORMAT_BATCH_SIZE = 100
TEXT_BATCH_SIZE = 200
BATCH_DELAY_SEC = 1.0

DATE_FMTS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y")

# Flag categories (used only when falling back to conservative derivation)
ALWAYS_APPLY_POST_FLAGS = {
    'CALENDAR_LOW_EVENTS', 'CAROUSEL_LOW_EVENTS', 'OCR_RICH_LOW_EVENTS',
    'ACCOUNT_PATTERN_DROP', 'NO_GROUNDING',
}
SHEET_CONDITIONAL_FLAGS = {
    'CITY_NOT_IN_NJ_LOOKUP', 'REGION_AUTOFIXED', 'VENUE_CITY_MISMATCH',
    'PAST_DATE', 'FAR_FUTURE_DATE',
}
SKIP_BY_DEFAULT = {'DATE_DAY_MISMATCH'}


# -----------------------------------------------------------------
# Log parser -- handles BOTH old post-level and new event-level formats
# -----------------------------------------------------------------

# Post boundary -- same line in both formats
RE_PROCESSING = re.compile(r'\[\d+/\d+\]\s+Processing post:\s+(\S+)')

# OLD format: post-level flag emission
RE_INFO_FLAGS = re.compile(r"informational flags:\s*\[([^\]]*)\]")

# OLD format: event markers
RE_EVENT_FOUND  = re.compile(r'EVENT FOUND:\s+(.+?)\s*$')
RE_MULTI_HEADER = re.compile(r'MULTIPLE EVENTS FOUND:\s+(\d+)\s+events extracted')
RE_MULTI_ITEM   = re.compile(r'^\s+(\d+)\.\s+(.+?)\s*$')

# NEW format: event marker like  ✦ [flash_caption] 'Bad Bunny Night' | 2026-05-22
#   - leading ✦ (or anything) is not required -- we match on the [tier] 'name' | date shape
#   - tier name is lower-snake (flash_caption, flash_image, flash_text, pro_text, ...)
#   - event name may be in ASCII or curly quotes
#   - date may be YYYY-MM-DD or "None"
# Anchored to end-of-line with \s*$ so the lazy .+? expands to cover names
# containing apostrophes (e.g. "St. Patrick's Day").
RE_NEW_EVENT = re.compile(
    r"\[([a-z][a-z0-9_]*)\]\s+[\'\"\u2018\u201c](.+?)[\'\"\u2019\u201d]\s+\|\s+(\S+)\s*$"
)

# Field lines: old format used plain (no bullet) form; new format uses '•'.
# Match either by making the bullet optional.
RE_DATE_LINE  = re.compile(r'(?:•\s*)?Date:\s+(\S+)')
RE_VENUE_LINE = re.compile(r'(?:•\s*)?Venue:\s+(.+?)\s*$')
RE_CONF_LINE  = re.compile(r'(?:•\s*)?Confidence:\s+([\d.]+)')

# NEW format event-level flags line:
#   • Flags: none
#   • Flags: DATE_DAY_MISMATCH
#   • Flags: LOW_CONFIDENCE,NO_GROUNDING
# Always lowercase-bulleted in new format; we tolerate missing bullet too.
RE_NEW_FLAGS = re.compile(r'(?:•\s*)?Flags:\s*(.+?)\s*$')


def _parse_new_flags_value(raw):
    """
    Parse the value side of '• Flags: ...'.
    Returns a set of flag strings. 'none' / empty -> empty set.
    """
    raw = (raw or '').strip()
    if not raw or raw.lower() == 'none':
        return set()
    # Comma-separated, tolerant of spaces
    return {tok.strip() for tok in raw.split(',') if tok.strip()}


def parse_log(log_path):
    """
    Parse a single run log into a list of post records.

    Each post record:
      {
        'post_id': str,
        'log_mtime': datetime,
        'post_flags': set[str],            # legacy 'informational flags:' line
        'has_event_level': bool,           # any new-format event seen with Flags line?
        'events': [
          {'name': str,
           'date': str|None,
           'venue': str|None,
           'confidence': float|None,
           'event_flags': set[str]|None,   # None = no event-level info; set() = clean
           'tier': str|None}               # new-format tier label, informational
        ]
      }
    """
    text = log_path.read_text(encoding='utf-8', errors='replace')
    log_mtime = datetime.fromtimestamp(log_path.stat().st_mtime)
    posts = []
    current = None       # in-progress post dict
    cur_event = None     # most-recently-added event in the current post

    def flush_current():
        if current and (current['events'] or current['post_flags']):
            posts.append(current)

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        # Post boundary
        m = RE_PROCESSING.search(line)
        if m:
            flush_current()
            current = {
                'post_id': m.group(1),
                'log_mtime': log_mtime,
                'post_flags': set(),
                'has_event_level': False,
                'events': [],
            }
            cur_event = None
            continue

        if current is None:
            continue

        # --- NEW format event marker (check first; most specific) ---
        m = RE_NEW_EVENT.search(line)
        if m:
            tier = m.group(1)
            name = m.group(2).strip()
            date = m.group(3).strip()
            cur_event = {
                'name': name,
                'date': None if date == 'None' else date,
                'venue': None,
                'confidence': None,
                'event_flags': None,    # will be filled by RE_NEW_FLAGS below
                'tier': tier,
            }
            current['events'].append(cur_event)
            continue

        # --- NEW format Flags bullet (event-level) ---
        # Must come BEFORE general field lines so we don't misclassify.
        # Use \bFlags\b boundary check via the regex itself.
        m = RE_NEW_FLAGS.search(line)
        if m and cur_event is not None:
            cur_event['event_flags'] = _parse_new_flags_value(m.group(1))
            current['has_event_level'] = True
            continue

        # --- OLD format post-level flags line ---
        m = RE_INFO_FLAGS.search(line)
        if m:
            raw = m.group(1).strip()
            for tok in re.findall(r"'([^']+)'", raw):
                current['post_flags'].add(tok)
            continue

        # --- OLD format single-event marker ---
        m = RE_EVENT_FOUND.search(line)
        if m:
            cur_event = {
                'name': m.group(1).strip(),
                'date': None, 'venue': None, 'confidence': None,
                'event_flags': None, 'tier': None,
            }
            current['events'].append(cur_event)
            continue

        # --- OLD format multi-event list item: "  N. <name>" ---
        m = RE_MULTI_ITEM.match(line)
        if m and RE_MULTI_HEADER.search(line) is None:
            cur_event = {
                'name': m.group(2).strip(),
                'date': None, 'venue': None, 'confidence': None,
                'event_flags': None, 'tier': None,
            }
            current['events'].append(cur_event)
            continue

        # --- Field lines (apply to current event) ---
        if cur_event is not None:
            m = RE_DATE_LINE.search(line)
            if m:
                val = m.group(1).strip()
                cur_event['date'] = None if val == 'None' else val
                continue

            m = RE_VENUE_LINE.search(line)
            if m:
                val = m.group(1).strip()
                cur_event['venue'] = None if val == 'None' else val
                continue

            m = RE_CONF_LINE.search(line)
            if m:
                try:
                    cur_event['confidence'] = float(m.group(1))
                except ValueError:
                    pass
                continue

    flush_current()
    return posts


def parse_logs(log_paths):
    """
    Parse multiple logs. Returns dict keyed by post_id; for posts that appear
    in multiple logs, the LATEST log (by mtime) wins.
    """
    by_post = {}
    for p in log_paths:
        for rec in parse_log(p):
            existing = by_post.get(rec['post_id'])
            if existing is None or rec['log_mtime'] > existing['log_mtime']:
                by_post[rec['post_id']] = rec
    return by_post


# -----------------------------------------------------------------
# Date helpers
# -----------------------------------------------------------------

def parse_date(s):
    if not s or s == 'None':
        return None
    s = s.strip()
    for fmt in DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def normalize_date_for_key(s):
    """Normalize a date string for matching. Returns canonical string or original."""
    d = parse_date(s)
    return d.isoformat() if d else (s.strip() if s else '')


# -----------------------------------------------------------------
# Sheet helpers
# -----------------------------------------------------------------

def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"ERROR: Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def col_index(header, *names):
    for name in names:
        for i, h in enumerate(header):
            if h.strip().upper() == name.upper():
                return i
    return None


def col_letter(idx_zero_based):
    """Convert 0-based column index to A1 letter (A, B, ..., Z, AA, ...)."""
    n = idx_zero_based + 1
    s = ''
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


# -----------------------------------------------------------------
# Flag derivation
# -----------------------------------------------------------------

def derive_flags(post_record, event_log, sheet_row, col_indices,
                 nj_lookup, all_event_dates_in_post, skip_day_mismatch=True):
    """
    Compute the canonical set of flags for one event.

    If the event has event-level flags from the log (event_log['event_flags']
    is not None), those are AUTHORITATIVE -- they reflect what main.py's
    sanity checks decided at extract time, on a per-event basis. We trust
    them directly and skip the conservative post-level derivation entirely.

    Otherwise (legacy post-level log only), apply the conservative derivation:
      (A) ALWAYS_APPLY_POST_FLAGS  -- applied to all events in the post
      (B) Per-event from log fields (MISSING_DATE, LOW_CONFIDENCE)
      (C) SHEET_CONDITIONAL_FLAGS  -- only if sheet cells warrant it
      (D) DATE_DAY_MISMATCH        -- skipped unless explicitly asked

    Returns: (flags_set, source_str) where source_str is 'event-level' or
             'post-level'.
    """
    # --- Event-level path: trust the log directly ---
    # No DATE_DAY_MISMATCH discard here -- the old skip rule existed because
    # post-level logs couldn't tell us WHICH event in a multi-event post had
    # the mismatch. Event-level logs name the specific event, so we honour it.
    if event_log.get('event_flags') is not None:
        return set(event_log['event_flags']), 'event-level'

    # --- Post-level fallback (legacy conservative derivation) ---
    flags = set()
    log_post_flags = post_record['post_flags']

    # (A) Always-apply post-level flags
    for f in ALWAYS_APPLY_POST_FLAGS:
        if f in log_post_flags:
            flags.add(f)

    # (B) Per-event derivation from log fields
    if event_log.get('date') is None:
        flags.add('MISSING_DATE')

    conf = event_log.get('confidence')
    if conf is not None and conf < 0.5:
        flags.add('LOW_CONFIDENCE')

    # (C) Per-event conditional via sheet field check
    def sheet_val(col_name):
        idx = col_indices.get(col_name)
        if idx is None or idx >= len(sheet_row):
            return ''
        return (sheet_row[idx] or '').strip()

    sheet_city    = sheet_val('CITY')
    sheet_venue   = sheet_val('VENUE NAME')
    sheet_section = sheet_val('SECTION OF NJ')

    if 'CITY_NOT_IN_NJ_LOOKUP' in log_post_flags:
        if sheet_city:
            if not nj_lookup or sheet_city.lower() not in {k.lower() for k in nj_lookup.keys()}:
                flags.add('CITY_NOT_IN_NJ_LOOKUP')

    if 'REGION_AUTOFIXED' in log_post_flags:
        if sheet_section:
            flags.add('REGION_AUTOFIXED')

    if 'VENUE_CITY_MISMATCH' in log_post_flags:
        if sheet_venue and sheet_city:
            flags.add('VENUE_CITY_MISMATCH')

    # PAST_DATE / FAR_FUTURE_DATE: apply to event(s) with earliest/latest date
    event_date = parse_date(event_log.get('date'))
    if 'PAST_DATE' in log_post_flags and event_date and all_event_dates_in_post:
        earliest = min(d for d in all_event_dates_in_post if d)
        if event_date == earliest:
            flags.add('PAST_DATE')

    if 'FAR_FUTURE_DATE' in log_post_flags and event_date and all_event_dates_in_post:
        latest = max(d for d in all_event_dates_in_post if d)
        if event_date == latest:
            flags.add('FAR_FUTURE_DATE')

    # (D) DATE_DAY_MISMATCH -- skip by default
    if not skip_day_mismatch and 'DATE_DAY_MISMATCH' in log_post_flags:
        flags.add('DATE_DAY_MISMATCH')

    return flags, 'post-level'


# -----------------------------------------------------------------
# Sheet update batchers
# -----------------------------------------------------------------

def batch_update_text_cells(spreadsheet, ws, sheet_id, updates):
    """updates: list of (sheet_row, col_idx, new_value)"""
    if not updates:
        return
    requests = []
    for row, col, val in updates:
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row - 1,
                    "endRowIndex": row,
                    "startColumnIndex": col,
                    "endColumnIndex": col + 1,
                },
                "rows": [{"values": [{
                    "userEnteredValue": {"stringValue": val}
                }]}],
                "fields": "userEnteredValue",
            }
        })
    for start in range(0, len(requests), TEXT_BATCH_SIZE):
        spreadsheet.batch_update({"requests": requests[start:start + TEXT_BATCH_SIZE]})
        if start + TEXT_BATCH_SIZE < len(requests):
            time.sleep(BATCH_DELAY_SEC)


def batch_update_format_cells(spreadsheet, ws, sheet_id, paint_ops, wipe_ops):
    """
    paint_ops: list of (sheet_row, col_idx, color_dict)
    wipe_ops:  list of (sheet_row, col_idx) -- set to white
    """
    requests = []
    for row, col in wipe_ops:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row - 1,
                    "endRowIndex": row,
                    "startColumnIndex": col,
                    "endColumnIndex": col + 1,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": WHITE_BG}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })
    for row, col, color in paint_ops:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row - 1,
                    "endRowIndex": row,
                    "startColumnIndex": col,
                    "endColumnIndex": col + 1,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": color}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })
    for start in range(0, len(requests), FORMAT_BATCH_SIZE):
        spreadsheet.batch_update({"requests": requests[start:start + FORMAT_BATCH_SIZE]})
        if start + FORMAT_BATCH_SIZE < len(requests):
            time.sleep(BATCH_DELAY_SEC)


# -----------------------------------------------------------------
# Main
# -----------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Re-derive QUALITY_FLAGS + cell formatting from main.py run logs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--log", action="append", default=[],
                   help="Path to a run_*.log file. Repeatable.")
    p.add_argument("--logs-dir", default=None,
                   help="Directory containing run_*.log files (uses all of them).")
    p.add_argument("--apply", action="store_true",
                   help="Commit changes to the sheet (default: dry-run).")
    p.add_argument("--after-row", type=int, default=None,
                   help="Only consider sheet rows >= this row number.")
    p.add_argument("--strip-extra", action="store_true",
                   help="Also remove flags present in sheet but not derived from log.")
    p.add_argument("--aggressive-day-mismatch", action="store_true",
                   help="Apply DATE_DAY_MISMATCH to all events from posts that had it.")
    args = p.parse_args()

    # Resolve log paths
    log_paths = [Path(x) for x in args.log]
    if args.logs_dir:
        log_paths.extend(sorted(Path(args.logs_dir).glob("run_*.log")))
    log_paths = [p for p in log_paths if p.exists()]

    if not log_paths:
        print("ERROR: No log files found. Use --log <path> or --logs-dir <dir>.", file=sys.stderr)
        sys.exit(1)

    print(f"=== repair_flags_from_log.py ===")
    print(f"  Apply mode:          {args.apply}")
    print(f"  Strip extras:        {args.strip_extra}")
    print(f"  Aggressive day-flag: {args.aggressive_day_mismatch}")
    print(f"  Logs ({len(log_paths)}):")
    for lp in log_paths:
        print(f"    - {lp}")
    print()

    # Parse logs
    print("  Parsing logs...")
    log_by_post = parse_logs(log_paths)
    n_events           = sum(len(p['events']) for p in log_by_post.values())
    n_event_level      = sum(1 for p in log_by_post.values() if p['has_event_level'])
    n_post_level_only  = sum(1 for p in log_by_post.values()
                             if not p['has_event_level'] and p['post_flags'])
    n_flagged_posts    = sum(1 for p in log_by_post.values() if p['post_flags'])
    print(f"  Parsed {len(log_by_post)} posts with {n_events} events")
    print(f"    - event-level posts:        {n_event_level}")
    print(f"    - post-level-only w/ flags: {n_post_level_only}")
    print(f"    - total posts with flags:   {n_flagged_posts}")

    # Connect + read sheet
    print("\n  Connecting to sheet...")
    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)
    sheet_id = ws.id

    print("  Reading All_Events...")
    all_values = ws.get_all_values()
    if not all_values:
        print("WARNING: Sheet is empty.")
        return
    header = all_values[0]
    data_rows = all_values[1:]
    print(f"    {len(data_rows)} data rows")

    # Resolve column indices
    cols = {
        'POST ID':       col_index(header, 'POST ID'),
        'EVENT NAME':    col_index(header, 'EVENT NAME'),
        'DATE':          col_index(header, 'DATE'),
        'VENUE NAME':    col_index(header, 'VENUE NAME'),
        'CITY':          col_index(header, 'CITY'),
        'SECTION OF NJ': col_index(header, 'SECTION OF NJ'),
        'CONFIDENCE':    col_index(header, 'CONFIDENCE'),
        'DESCRIPTION':   col_index(header, 'DESCRIPTION'),
        'QUALITY_FLAGS': col_index(header, 'QUALITY_FLAGS', 'QUALITY FLAGS'),
    }
    missing = [k for k, v in cols.items() if v is None]
    if missing:
        print(f"ERROR: Missing required columns in sheet header: {missing}", file=sys.stderr)
        sys.exit(1)

    # Build sheet index by composite key
    print("  Indexing sheet rows...")
    sheet_index = defaultdict(list)  # (post_id, name_upper, date_iso) -> [(row_num, row_data)]
    for i, row in enumerate(data_rows, start=2):
        if args.after_row and i < args.after_row:
            continue
        if len(row) <= max(cols['POST ID'], cols['EVENT NAME'], cols['DATE']):
            continue
        pid = (row[cols['POST ID']] or '').strip()
        name = (row[cols['EVENT NAME']] or '').strip().upper()
        date_iso = normalize_date_for_key(row[cols['DATE']])
        if pid and name:
            sheet_index[(pid, name, date_iso)].append((i, row))

    # Derive + diff
    print("\n  Deriving flags + diffing...")
    nj_lookup = load_nj_municipalities()

    text_updates = []   # (row, col, new_value)
    paint_ops    = []   # (row, col, color)
    wipe_ops     = []   # (row, col)
    audit_rows   = []   # for CSV

    counts = {'matched': 0, 'updated': 0, 'log_only_no_match': 0,
              'sheet_only_not_in_log': 0, 'multi_match_skipped': 0}
    source_counts = {'event-level': 0, 'post-level': 0}
    matched_keys = set()

    for post_id, post_rec in log_by_post.items():
        all_dates = [parse_date(e.get('date')) for e in post_rec['events']]
        all_dates = [d for d in all_dates if d is not None]

        for event_log in post_rec['events']:
            ev_name_upper = event_log['name'].strip().upper()
            ev_date_iso   = normalize_date_for_key(event_log.get('date'))
            key = (post_id, ev_name_upper, ev_date_iso)

            sheet_matches = sheet_index.get(key, [])
            ev_source_hint = 'event-level' if event_log.get('event_flags') is not None else 'post-level'

            if not sheet_matches:
                counts['log_only_no_match'] += 1
                audit_rows.append({
                    'status':     'log_only_no_match',
                    'source':     ev_source_hint,
                    'post_id':    post_id,
                    'event_name': event_log['name'],
                    'date':       event_log.get('date') or '',
                    'sheet_row':  '',
                    'old_flags':  '',
                    'new_flags':  '',
                })
                continue

            if len(sheet_matches) > 1:
                counts['multi_match_skipped'] += 1
                audit_rows.append({
                    'status':     'multi_match_skipped',
                    'source':     ev_source_hint,
                    'post_id':    post_id,
                    'event_name': event_log['name'],
                    'date':       event_log.get('date') or '',
                    'sheet_row':  ';'.join(str(r) for r, _ in sheet_matches),
                    'old_flags':  '',
                    'new_flags':  '',
                })
                continue

            sheet_row_num, sheet_row = sheet_matches[0]
            matched_keys.add(key)

            # Compute derived flags + source
            derived, source = derive_flags(
                post_rec, event_log, sheet_row, cols, nj_lookup, all_dates,
                skip_day_mismatch=not args.aggressive_day_mismatch,
            )
            source_counts[source] = source_counts.get(source, 0) + 1

            # Read current flags from sheet
            current_text = (sheet_row[cols['QUALITY_FLAGS']]
                            if cols['QUALITY_FLAGS'] < len(sheet_row) else '') or ''
            current = {f.strip() for f in current_text.split(',') if f.strip()}

            # Build new flag set:
            #   --strip-extra        : take derived exactly
            #   event-level source   : trust derived; keep only unknown manual extras
            #   post-level source    : preserve all sheet extras (legacy behaviour)
            if args.strip_extra:
                new_flags = derived
            elif source == 'event-level':
                known = set(FLAG_TO_COLUMNS.keys())
                manual_extras = current - known
                new_flags = derived | manual_extras
            else:
                new_flags = current | derived

            if new_flags == current:
                counts['matched'] += 1
                audit_rows.append({
                    'status':     'matched',
                    'source':     source,
                    'post_id':    post_id,
                    'event_name': event_log['name'],
                    'date':       event_log.get('date') or '',
                    'sheet_row':  str(sheet_row_num),
                    'old_flags':  ','.join(sorted(current)),
                    'new_flags':  ','.join(sorted(new_flags)),
                })
                continue

            # Diff: queue text update + repaint
            counts['updated'] += 1
            new_text = ','.join(sorted(new_flags))
            text_updates.append((sheet_row_num, cols['QUALITY_FLAGS'], new_text))

            # Determine all columns that need painting/wiping for this row
            cols_to_paint = set()
            for f in new_flags:
                for col_name in FLAG_TO_COLUMNS.get(f, []):
                    ci = cols.get(col_name)
                    if ci is not None:
                        cols_to_paint.add(ci)

            cols_should_be_white = set()
            for f in current - new_flags:
                for col_name in FLAG_TO_COLUMNS.get(f, []):
                    ci = cols.get(col_name)
                    if ci is not None and ci not in cols_to_paint:
                        cols_should_be_white.add(ci)

            for ci in cols_to_paint:
                paint_ops.append((sheet_row_num, ci, FLAG_BG_COLOR))
            for ci in cols_should_be_white:
                wipe_ops.append((sheet_row_num, ci))

            audit_rows.append({
                'status':     'updated',
                'source':     source,
                'post_id':    post_id,
                'event_name': event_log['name'],
                'date':       event_log.get('date') or '',
                'sheet_row':  str(sheet_row_num),
                'old_flags':  ','.join(sorted(current)),
                'new_flags':  ','.join(sorted(new_flags)),
            })

    # Sheet-only rows (in sheet, not in any parsed log)
    for key, matches in sheet_index.items():
        if key in matched_keys:
            continue
        for (row_num, row_data) in matches:
            counts['sheet_only_not_in_log'] += 1
            cur = (row_data[cols['QUALITY_FLAGS']] if cols['QUALITY_FLAGS'] < len(row_data) else '') or ''
            audit_rows.append({
                'status':     'sheet_only_not_in_log',
                'source':     '',
                'post_id':    key[0],
                'event_name': key[1],
                'date':       key[2],
                'sheet_row':  str(row_num),
                'old_flags':  cur,
                'new_flags':  cur,
            })

    # --- REPORT ---
    print(f"\n=== FINDINGS ===")
    print(f"  Matched (no change needed):  {counts['matched']}")
    print(f"  Updates queued:              {counts['updated']}")
    print(f"  In log, not in sheet:        {counts['log_only_no_match']}")
    print(f"  In sheet, not in log:        {counts['sheet_only_not_in_log']}")
    print(f"  Ambiguous (multi-match):     {counts['multi_match_skipped']}")
    print(f"  Format ops queued: {len(paint_ops)} paint, {len(wipe_ops)} wipe")
    print(f"\n  Derivation source breakdown:")
    print(f"    - event-level (trusted from log):       {source_counts.get('event-level', 0)}")
    print(f"    - post-level (conservative fallback):   {source_counts.get('post-level', 0)}")

    # CSV audit
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("outputs") / f"repair_flags_{ts}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=[
            'status', 'source', 'post_id', 'event_name', 'date',
            'sheet_row', 'old_flags', 'new_flags'
        ])
        w.writeheader()
        for r in audit_rows:
            w.writerow(r)
    print(f"\n  Audit CSV: {csv_path}")

    if not args.apply:
        print(f"\n  [dry-run] No sheet changes made. Re-run with --apply to commit.")
        return

    if not text_updates and not paint_ops and not wipe_ops:
        print(f"\n  OK: Nothing to apply. Sheet already in sync.")
        return

    # --- APPLY ---
    print(f"\n=== APPLYING ===")
    print(f"  Updating {len(text_updates)} QUALITY_FLAGS cells...")
    batch_update_text_cells(sh, ws, sheet_id, text_updates)

    print(f"  Updating {len(paint_ops) + len(wipe_ops)} cell formats...")
    batch_update_format_cells(sh, ws, sheet_id, paint_ops, wipe_ops)

    print(f"\nOK: Done. Audit trail: {csv_path}")


if __name__ == "__main__":
    main()
