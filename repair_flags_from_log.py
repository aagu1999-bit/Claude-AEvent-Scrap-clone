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
Three log formats are recognized; the same logs directory may contain a mix.

(0) [post_id] PREFIX  (newest; safest under parallel workers)
    Every line in main.py output is prefixed by worker_log.wprint() with
    "[<post_id>] " when emitted inside a worker context. The parser uses
    the prefix to route each line independently, so interleaved output
    from multiple workers can no longer cause misattribution.

        [3894171512534099386]   ✦ [flash_caption] 'Bad Bunny Night' | 2026-05-22
        [3894171512534099386]       • Flags: none

    Lines without a prefix fall back to the legacy "most recent
    Processing post:" rule below.

(1) EVENT-LEVEL  (new -- unprefixed)
    Per-event marker + bullet fields:

        ✦ [flash_caption] 'Bad Bunny Night' | 2026-05-22
            • Venue: Birch Hoboken
            • Confidence: 1.0
            • Flags: none

    "Flags:" is authoritative for that event. "Flags: none" means
    "evaluated cleanly at event level" -- the repair tool trusts it
    directly and does NOT fall back to conservative post-level spreading
    for this post.

(2) POST-LEVEL  (legacy)
    Older logs use:

        ✓ EVENT FOUND: Watermelon & Feta Salad
          • Date: 2026-05-01

    or for multi-event posts:

        🎉 MULTIPLE EVENTS FOUND: 5 events extracted!
          1. The Dirty German 50K
             Date: 2026-05-09

    plus a separate post-level flags line:

        ↳ informational flags: ['CITY_NOT_IN_NJ_LOOKUP']

    For these the tool falls back to the conservative derivation below.

CONSERVATIVE DERIVATION (post-level fallback only)
--------------------------------------------------
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
    DATE_DAY_MISMATCH -- needs original caption text to verify.

USAGE
-----
  python repair_flags_from_log.py --log outputs/run_20260514_160556.log
  python repair_flags_from_log.py --logs-dir outputs/
  python repair_flags_from_log.py --log <path> --apply
  python repair_flags_from_log.py --log <path> --after-row 18000
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
# Log parser -- handles [post_id] prefix + new event-level + legacy post-level
# -----------------------------------------------------------------

# Optional [post_id] prefix at start of line. Requires 6+ chars of
# [A-Za-z0-9_-] inside the brackets so it doesn't collide with [N/M] (slash),
# [Tier X: ...] (space/colon), or [flash_caption] (mid-line, not at start).
RE_PREFIX = re.compile(r'^\s*\[([A-Za-z0-9_-]{6,})\]\s+')

RE_PROCESSING  = re.compile(r'\[\d+/\d+\]\s+Processing post:\s+(\S+)')
RE_INFO_FLAGS  = re.compile(r"informational flags:\s*\[([^\]]*)\]")

RE_EVENT_FOUND  = re.compile(r'EVENT FOUND:\s+(.+?)\s*$')
RE_MULTI_HEADER = re.compile(r'MULTIPLE EVENTS FOUND:\s+(\d+)\s+events extracted')
RE_MULTI_ITEM   = re.compile(r'^\s+(\d+)\.\s+(.+?)\s*$')

RE_NEW_EVENT = re.compile(
    r"\[([a-z][a-z0-9_]*)\]\s+[\'\"\u2018\u201c](.+?)[\'\"\u2019\u201d]\s+\|\s+(\S+)\s*$"
)

RE_DATE_LINE  = re.compile(r'(?:•\s*)?Date:\s+(\S+)')
RE_VENUE_LINE = re.compile(r'(?:•\s*)?Venue:\s+(.+?)\s*$')
RE_CONF_LINE  = re.compile(r'(?:•\s*)?Confidence:\s+([\d.]+)')
RE_NEW_FLAGS  = re.compile(r'(?:•\s*)?Flags:\s*(.+?)\s*$')


def _parse_new_flags_value(raw):
    raw = (raw or '').strip()
    if not raw or raw.lower() == 'none':
        return set()
    return {tok.strip() for tok in raw.split(',') if tok.strip()}


def parse_log(log_path):
    """
    Parse a single run log into a list of post records.

    Per post record:
      {
        'post_id', 'log_mtime',
        'post_flags': set[str],         # legacy 'informational flags:' line
        'has_event_level': bool,        # any '• Flags:' line seen?
        'events': [{'name','date','venue','confidence','event_flags','tier'}],
      }

    Lines with [post_id] prefix are routed directly to that post's record.
    Lines without prefix fall back to the most-recent Processing post: ID.
    """
    text = log_path.read_text(encoding='utf-8', errors='replace')
    log_mtime = datetime.fromtimestamp(log_path.stat().st_mtime)

    posts_by_id = {}
    legacy_current_id = None

    def ensure_post(post_id):
        if post_id not in posts_by_id:
            posts_by_id[post_id] = {
                'post_id': post_id,
                'log_mtime': log_mtime,
                'post_flags': set(),
                'has_event_level': False,
                'events': [],
                '_cur_event': None,
            }
        return posts_by_id[post_id]

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        # Peel optional [post_id] prefix
        m_pre = RE_PREFIX.match(line)
        if m_pre:
            ctx = ensure_post(m_pre.group(1))
            body = line[m_pre.end():]
        else:
            body = line
            ctx = posts_by_id.get(legacy_current_id) if legacy_current_id else None

        # Processing post: marker -- always update legacy anchor
        m = RE_PROCESSING.search(body)
        if m:
            inner_id = m.group(1)
            ensure_post(inner_id)
            legacy_current_id = inner_id
            continue

        if ctx is None:
            continue

        # NEW format event marker (specific, check first)
        m = RE_NEW_EVENT.search(body)
        if m:
            ev = {
                'name': m.group(2).strip(),
                'date': None if m.group(3).strip() == 'None' else m.group(3).strip(),
                'venue': None, 'confidence': None,
                'event_flags': None, 'tier': m.group(1),
            }
            ctx['events'].append(ev)
            ctx['_cur_event'] = ev
            continue

        # NEW format Flags bullet (must come before generic field lines)
        m = RE_NEW_FLAGS.search(body)
        if m and ctx['_cur_event'] is not None:
            ctx['_cur_event']['event_flags'] = _parse_new_flags_value(m.group(1))
            ctx['has_event_level'] = True
            continue

        # OLD format post-level flags
        m = RE_INFO_FLAGS.search(body)
        if m:
            for tok in re.findall(r"'([^']+)'", m.group(1).strip()):
                ctx['post_flags'].add(tok)
            continue

        # OLD format single-event marker
        m = RE_EVENT_FOUND.search(body)
        if m:
            ev = {'name': m.group(1).strip(), 'date': None, 'venue': None,
                  'confidence': None, 'event_flags': None, 'tier': None}
            ctx['events'].append(ev)
            ctx['_cur_event'] = ev
            continue

        # OLD format multi-event list item: "  N. <name>"
        m = RE_MULTI_ITEM.match(body)
        if m and RE_MULTI_HEADER.search(body) is None:
            ev = {'name': m.group(2).strip(), 'date': None, 'venue': None,
                  'confidence': None, 'event_flags': None, 'tier': None}
            ctx['events'].append(ev)
            ctx['_cur_event'] = ev
            continue

        # Field lines apply to ctx's current event
        if ctx['_cur_event'] is not None:
            m = RE_DATE_LINE.search(body)
            if m:
                val = m.group(1).strip()
                ctx['_cur_event']['date'] = None if val == 'None' else val
                continue
            m = RE_VENUE_LINE.search(body)
            if m:
                val = m.group(1).strip()
                ctx['_cur_event']['venue'] = None if val == 'None' else val
                continue
            m = RE_CONF_LINE.search(body)
            if m:
                try:
                    ctx['_cur_event']['confidence'] = float(m.group(1))
                except ValueError:
                    pass
                continue

    posts = []
    for post in posts_by_id.values():
        post.pop('_cur_event', None)
        if post['events'] or post['post_flags']:
            posts.append(post)
    return posts


def parse_logs(log_paths):
    """Latest log by mtime wins per post_id."""
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
    if event_log.get('event_flags') is not None:
        return set(event_log['event_flags']), 'event-level'

    flags = set()
    log_post_flags = post_record['post_flags']

    for f in ALWAYS_APPLY_POST_FLAGS:
        if f in log_post_flags:
            flags.add(f)

    if event_log.get('date') is None:
        flags.add('MISSING_DATE')
    conf = event_log.get('confidence')
    if conf is not None and conf < 0.5:
        flags.add('LOW_CONFIDENCE')

    def sheet_val(col_name):
        idx = col_indices.get(col_name)
        if idx is None or idx >= len(sheet_row):
            return ''
        return (sheet_row[idx] or '').strip()

    sheet_city    = sheet_val('CITY')
    sheet_venue   = sheet_val('VENUE NAME')
    sheet_section = sheet_val('SECTION OF NJ')

    if 'CITY_NOT_IN_NJ_LOOKUP' in log_post_flags and sheet_city:
        if not nj_lookup or sheet_city.lower() not in {k.lower() for k in nj_lookup.keys()}:
            flags.add('CITY_NOT_IN_NJ_LOOKUP')

    if 'REGION_AUTOFIXED' in log_post_flags and sheet_section:
        flags.add('REGION_AUTOFIXED')

    if 'VENUE_CITY_MISMATCH' in log_post_flags and sheet_venue and sheet_city:
        flags.add('VENUE_CITY_MISMATCH')

    event_date = parse_date(event_log.get('date'))
    if 'PAST_DATE' in log_post_flags and event_date and all_event_dates_in_post:
        if event_date == min(d for d in all_event_dates_in_post if d):
            flags.add('PAST_DATE')
    if 'FAR_FUTURE_DATE' in log_post_flags and event_date and all_event_dates_in_post:
        if event_date == max(d for d in all_event_dates_in_post if d):
            flags.add('FAR_FUTURE_DATE')

    if not skip_day_mismatch and 'DATE_DAY_MISMATCH' in log_post_flags:
        flags.add('DATE_DAY_MISMATCH')

    return flags, 'post-level'


# -----------------------------------------------------------------
# Sheet update batchers
# -----------------------------------------------------------------

def batch_update_text_cells(spreadsheet, ws, sheet_id, updates):
    if not updates:
        return
    requests = []
    for row, col, val in updates:
        requests.append({
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row - 1, "endRowIndex": row,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
                },
                "rows": [{"values": [{"userEnteredValue": {"stringValue": val}}]}],
                "fields": "userEnteredValue",
            }
        })
    for start in range(0, len(requests), TEXT_BATCH_SIZE):
        spreadsheet.batch_update({"requests": requests[start:start + TEXT_BATCH_SIZE]})
        if start + TEXT_BATCH_SIZE < len(requests):
            time.sleep(BATCH_DELAY_SEC)


def batch_update_format_cells(spreadsheet, ws, sheet_id, paint_ops, wipe_ops):
    requests = []
    for row, col in wipe_ops:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row - 1, "endRowIndex": row,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
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
                    "startRowIndex": row - 1, "endRowIndex": row,
                    "startColumnIndex": col, "endColumnIndex": col + 1,
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

    log_paths = [Path(x) for x in args.log]
    if args.logs_dir:
        log_paths.extend(sorted(Path(args.logs_dir).glob("run_*.log")))
    log_paths = [p for p in log_paths if p.exists()]
    if not log_paths:
        print("ERROR: No log files found.", file=sys.stderr)
        sys.exit(1)

    print(f"=== repair_flags_from_log.py ===")
    print(f"  Apply mode:          {args.apply}")
    print(f"  Strip extras:        {args.strip_extra}")
    print(f"  Aggressive day-flag: {args.aggressive_day_mismatch}")
    print(f"  Logs ({len(log_paths)}):")
    for lp in log_paths:
        print(f"    - {lp}")

    print("\n  Parsing logs...")
    log_by_post = parse_logs(log_paths)
    n_events          = sum(len(p['events']) for p in log_by_post.values())
    n_event_level     = sum(1 for p in log_by_post.values() if p['has_event_level'])
    n_post_level_only = sum(1 for p in log_by_post.values()
                            if not p['has_event_level'] and p['post_flags'])
    n_flagged_posts   = sum(1 for p in log_by_post.values() if p['post_flags'])
    print(f"  Parsed {len(log_by_post)} posts with {n_events} events")
    print(f"    - event-level posts:        {n_event_level}")
    print(f"    - post-level-only w/ flags: {n_post_level_only}")
    print(f"    - total posts with flags:   {n_flagged_posts}")

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
        print(f"ERROR: Missing required columns: {missing}", file=sys.stderr)
        sys.exit(1)

    print("  Indexing sheet rows...")
    sheet_index = defaultdict(list)
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

    print("\n  Deriving flags + diffing...")
    nj_lookup = load_nj_municipalities()
    text_updates, paint_ops, wipe_ops, audit_rows = [], [], [], []
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
            ev_src_hint = 'event-level' if event_log.get('event_flags') is not None else 'post-level'

            if not sheet_matches:
                counts['log_only_no_match'] += 1
                audit_rows.append({'status': 'log_only_no_match', 'source': ev_src_hint,
                                   'post_id': post_id, 'event_name': event_log['name'],
                                   'date': event_log.get('date') or '', 'sheet_row': '',
                                   'old_flags': '', 'new_flags': ''})
                continue

            if len(sheet_matches) > 1:
                counts['multi_match_skipped'] += 1
                audit_rows.append({'status': 'multi_match_skipped', 'source': ev_src_hint,
                                   'post_id': post_id, 'event_name': event_log['name'],
                                   'date': event_log.get('date') or '',
                                   'sheet_row': ';'.join(str(r) for r, _ in sheet_matches),
                                   'old_flags': '', 'new_flags': ''})
                continue

            sheet_row_num, sheet_row = sheet_matches[0]
            matched_keys.add(key)

            derived, source = derive_flags(
                post_rec, event_log, sheet_row, cols, nj_lookup, all_dates,
                skip_day_mismatch=not args.aggressive_day_mismatch,
            )
            source_counts[source] = source_counts.get(source, 0) + 1

            current_text = (sheet_row[cols['QUALITY_FLAGS']]
                            if cols['QUALITY_FLAGS'] < len(sheet_row) else '') or ''
            current = {f.strip() for f in current_text.split(',') if f.strip()}

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
                audit_rows.append({'status': 'matched', 'source': source,
                                   'post_id': post_id, 'event_name': event_log['name'],
                                   'date': event_log.get('date') or '',
                                   'sheet_row': str(sheet_row_num),
                                   'old_flags': ','.join(sorted(current)),
                                   'new_flags': ','.join(sorted(new_flags))})
                continue

            counts['updated'] += 1
            text_updates.append((sheet_row_num, cols['QUALITY_FLAGS'], ','.join(sorted(new_flags))))

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

            audit_rows.append({'status': 'updated', 'source': source,
                               'post_id': post_id, 'event_name': event_log['name'],
                               'date': event_log.get('date') or '',
                               'sheet_row': str(sheet_row_num),
                               'old_flags': ','.join(sorted(current)),
                               'new_flags': ','.join(sorted(new_flags))})

    for key, matches in sheet_index.items():
        if key in matched_keys:
            continue
        for (row_num, row_data) in matches:
            counts['sheet_only_not_in_log'] += 1
            cur = (row_data[cols['QUALITY_FLAGS']] if cols['QUALITY_FLAGS'] < len(row_data) else '') or ''
            audit_rows.append({'status': 'sheet_only_not_in_log', 'source': '',
                               'post_id': key[0], 'event_name': key[1],
                               'date': key[2], 'sheet_row': str(row_num),
                               'old_flags': cur, 'new_flags': cur})

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

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("outputs") / f"repair_flags_{ts}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=[
            'status', 'source', 'post_id', 'event_name', 'date',
            'sheet_row', 'old_flags', 'new_flags'])
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

    print(f"\n=== APPLYING ===")
    print(f"  Updating {len(text_updates)} QUALITY_FLAGS cells...")
    batch_update_text_cells(sh, ws, sheet_id, text_updates)
    print(f"  Updating {len(paint_ops) + len(wipe_ops)} cell formats...")
    batch_update_format_cells(sh, ws, sheet_id, paint_ops, wipe_ops)
    print(f"\nOK: Done. Audit trail: {csv_path}")


if __name__ == "__main__":
    main()
