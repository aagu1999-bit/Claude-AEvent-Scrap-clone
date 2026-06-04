#!/usr/bin/env python3
"""
repair_flags_from_log.py — re-derive QUALITY_FLAGS column + cell highlighting
in All_Events from one or more main.py run logs.

USE CASE
────────
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

LOG FORMAT SUPPORT
──────────────────
Auto-detects two log formats per file:

  NEW (post-cutover, with worker_log.py [W<n>] prefixes):
    Every event has a ✦ line with authoritative per-event Flags. The
    pipeline wrote these exact flags to the sheet at extract time, so
    no derivation is needed — the rules below are bypassed entirely.
    Tier-escalation gotcha: ✦ lines fire per tier. We dedup by event
    name and keep the last occurrence (matches the final tier's output).

  OLD (pre-cutover, no per-line tags):
    Only post-level informational flags are emitted. Per-event attribution
    is inferred via the rules below. ~85-95% accurate due to thread
    interleaving on carousel-heavy multi-event posts. For high-confidence
    repair, re-extract with events_from_ids.py to generate a NEW-format
    log, then re-run this tool against that log.

DERIVATION RULES (old-format only)
──────────────────────────────────
For each event extracted in an old-format log, determine its flag set:

  (A) Always-apply post-level flags (apply to ALL events from that post):
        CALENDAR_LOW_EVENTS, CAROUSEL_LOW_EVENTS, OCR_RICH_LOW_EVENTS,
        ACCOUNT_PATTERN_DROP, NO_GROUNDING

  (B) Per-event derivation from log fields:
        MISSING_DATE     ← event Date == None
        LOW_CONFIDENCE   ← event Confidence < 0.5

  (C) Per-event conditional via sheet field check (only if log fired):
        CITY_NOT_IN_NJ_LOOKUP   ← log fired AND sheet CITY non-empty
        REGION_AUTOFIXED        ← log fired AND sheet SECTION OF NJ non-empty
        VENUE_CITY_MISMATCH     ← log fired AND sheet VENUE NAME + CITY both set
        PAST_DATE               ← log fired AND event has earliest date in post
        FAR_FUTURE_DATE         ← log fired AND event has latest date in post

  (D) Skipped by default:
        DATE_DAY_MISMATCH — needs original caption text to verify; cannot
        be cleanly derived from log + sheet alone.

USAGE
─────
  # Dry-run against one log
  python repair_flags_from_log.py --log outputs/run_20260514_160556.log

  # All logs in outputs/, dry-run
  python repair_flags_from_log.py --logs-dir outputs/

  # Apply
  python repair_flags_from_log.py --log outputs/run_20260514_160556.log --apply

  # Limit scope to recent rows
  python repair_flags_from_log.py --log outputs/run_*.log --after-row 18000

OUTPUT
──────
- Console: per-status counts (matched/conflict/log-only/sheet-only)
- CSV audit: outputs/repair_flags_<ts>.csv with every row + before/after
- (with --apply) Sheet writes: QUALITY_FLAGS text + cell formatting

SAFETY
──────
- Dry-run by default; --apply required to write
- Only TOUCHES rows present in the log being parsed (won't blank out rows
  from other runs unless --strip-extra is set)
- Preserves manual additions in QUALITY_FLAGS unless --strip-extra
"""

import os
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

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("SERVICE_ACCOUNT_FILE") or "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"

WHITE_BG = {"red": 1.0, "green": 1.0, "blue": 1.0}
FORMAT_BATCH_SIZE = 100
TEXT_BATCH_SIZE = 200
BATCH_DELAY_SEC = 1.0

DATE_FMTS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y")

# Flag categories
ALWAYS_APPLY_POST_FLAGS = {
    'CALENDAR_LOW_EVENTS', 'CAROUSEL_LOW_EVENTS', 'OCR_RICH_LOW_EVENTS',
    'ACCOUNT_PATTERN_DROP', 'NO_GROUNDING',
}
SHEET_CONDITIONAL_FLAGS = {
    'CITY_NOT_IN_NJ_LOOKUP', 'REGION_AUTOFIXED', 'VENUE_CITY_MISMATCH',
    'PAST_DATE', 'FAR_FUTURE_DATE',
}
SKIP_BY_DEFAULT = {'DATE_DAY_MISMATCH'}


# ─────────────────────────────────────────────────────────────────
# Log parser
# ─────────────────────────────────────────────────────────────────

# Regexes for the patterns we care about. Tolerates emoji prefixes (or absence)
# since terminal mojibake can vary.
RE_PROCESSING = re.compile(r'\[\d+/\d+\]\s+Processing post:\s+(\S+)')
RE_INFO_FLAGS = re.compile(r"informational flags:\s*\[([^\]]*)\]")
RE_EVENT_FOUND = re.compile(r'EVENT FOUND:\s+(.+?)\s*$')
RE_MULTI_HEADER = re.compile(r'MULTIPLE EVENTS FOUND:\s+(\d+)\s+events extracted')
RE_MULTI_ITEM   = re.compile(r'^\s+(\d+)\.\s+(.+?)\s*$')
RE_DATE_LINE    = re.compile(r'Date:\s+(\S+)')
RE_VENUE_LINE   = re.compile(r'Venue:\s+(.+?)\s*$')
RE_CONF_LINE    = re.compile(r'Confidence:\s+([\d.]+)')

# ─── New format markers (feat/worker-log-prefixing branch + later) ───
# Every line emitted inside a worker context is prefixed with [W<n>]
# (worker tag) and [<post_id>] (post tag). The new ✦ line emits the
# authoritative per-event flag set directly, so derivation rules can be
# skipped when present.

# Format detector: any [W<digits>] prefix anywhere → new format
RE_FORMAT_NEW_MARKER = re.compile(r'\[W\d+\]')

# Single ✦ event line — emitted per tier inside _run_tier. The tier may
# escalate after this, in which case more ✦ lines will follow for the same
# post. We dedup by event name and keep the last seen entry per post (which
# corresponds to the final tier and matches what's written to the sheet).
# Format: "    ✦ [<tier_name>] '<event_name>' | <date>"
RE_EVENT_DOT = re.compile(
    r"✦\s+\[(\w+)\]\s+['\"](.+?)['\"]\s+\|\s+(\S+)"
)

# Sub-field lines that follow a ✦ event. They're not [post_id]-prefixed
# (they're continuation lines from a single multi-line print) so we
# attribute them to the most recently seen ✦ event in the same post.
RE_NEW_VENUE = re.compile(r'•\s+Venue:\s+(.+?)\s*$')
RE_NEW_CONF  = re.compile(r'•\s+Confidence:\s+([\d.]+)')
RE_NEW_FLAGS = re.compile(r'•\s+Flags:\s+(.+?)\s*$')

# New Done boundary — marks end of post processing. Useful as a "close
# this post record" signal even though Processing post: of the NEXT post
# would also flush.
RE_NEW_DONE = re.compile(r'\[\d+/\d+\]\s+Done in\s+[\d.]+s')


def detect_log_format(log_path: Path) -> str:
    """Sniff a log file to determine whether it's the old format (no per-line
    worker tag) or the new format (every worker line prefixed with [W<n>]).
    Reads until first match — early-exit cheap for new-format files."""
    try:
        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                if RE_FORMAT_NEW_MARKER.search(line):
                    return 'new'
    except OSError:
        pass
    return 'old'


def parse_log(log_path: Path) -> list:
    """
    Parse a single run log into a list of post records. Auto-detects format.

    Returns:
        [
          {
            'post_id': str,
            'log_mtime': datetime,
            'format': 'old' | 'new',
            'post_flags': set[str],           # post-level only (informational flags)
            'events': [
              {
                'name': str,
                'date': str|None,
                'venue': str|None,
                'confidence': float|None,
                'authoritative_flags': set[str] | None,  # set in new format from ✦ Flags:
                'tier': str | None,                       # set in new format from ✦ [tier]
              }
            ]
          },
          ...
        ]

    For old-format records, authoritative_flags is None and derive_flags()
    falls back to the heuristic rules in its (A)/(B)/(C)/(D) sections.
    For new-format records, authoritative_flags is the canonical per-event
    flag set the pipeline assigned at write time — derive_flags() returns
    it as-is, no derivation needed.
    """
    fmt = detect_log_format(log_path)
    if fmt == 'new':
        return _parse_new_format(log_path)
    return _parse_old_format(log_path)


def _parse_old_format(log_path: Path) -> list:
    """Original parser — pre-worker_log.py logs without [W<n>] prefix.
    Uses informational flags + EVENT FOUND / MULTIPLE EVENTS FOUND lines.
    derive_flags() will apply heuristic rules to fill in per-event flags."""
    text = log_path.read_text(encoding='utf-8', errors='replace')
    log_mtime = datetime.fromtimestamp(log_path.stat().st_mtime)

    posts = []
    current = None  # in-progress post dict
    multi_event = None  # in-progress event dict (for MULTIPLE EVENTS FOUND items)

    def flush_current():
        if current and (current['events'] or current['post_flags']):
            posts.append(current)

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        # New post boundary
        m = RE_PROCESSING.search(line)
        if m:
            flush_current()
            current = {
                'post_id': m.group(1),
                'log_mtime': log_mtime,
                'format': 'old',
                'post_flags': set(),
                'events': [],
            }
            multi_event = None
            continue

        if current is None:
            continue

        # Post-level informational flags
        m = RE_INFO_FLAGS.search(line)
        if m:
            raw = m.group(1).strip()
            for tok in re.findall(r"'([^']+)'", raw):
                current['post_flags'].add(tok)
            continue

        # Single-event marker
        m = RE_EVENT_FOUND.search(line)
        if m:
            multi_event = {'name': m.group(1).strip(), 'date': None,
                           'venue': None, 'confidence': None,
                           'authoritative_flags': None, 'tier': None}
            current['events'].append(multi_event)
            continue

        # Multiple-events list item: "  N. <name>"
        m = RE_MULTI_ITEM.match(line)
        if m and RE_MULTI_HEADER.search(line) is None:
            # Plausibly a multi-event list item; verify we're inside one
            # (lookback approach: any recent MULTIPLE EVENTS marker for this post)
            multi_event = {'name': m.group(2).strip(), 'date': None,
                           'venue': None, 'confidence': None,
                           'authoritative_flags': None, 'tier': None}
            current['events'].append(multi_event)
            continue

        # Multi-event header just informs that the next N items are events;
        # we don't need to act on it specifically — the items are caught above.

        # Field lines (apply to most-recent event in this post)
        if multi_event is not None:
            m = RE_DATE_LINE.search(line)
            if m:
                val = m.group(1).strip()
                multi_event['date'] = None if val == 'None' else val
                continue
            m = RE_VENUE_LINE.search(line)
            if m:
                val = m.group(1).strip()
                multi_event['venue'] = None if val == 'None' else val
                continue
            m = RE_CONF_LINE.search(line)
            if m:
                try:
                    multi_event['confidence'] = float(m.group(1))
                except ValueError:
                    pass
                continue

    flush_current()
    return posts


def _parse_new_format(log_path: Path) -> list:
    """Parser for the new log format (feat/worker-log-prefixing branch and
    later). Authoritative per-event flags come from the ✦ line's
    `• Flags: <comma flags>` sub-field. derive_flags() returns them as-is.

    Tier-escalation gotcha: ✦ lines emit once per tier execution. A post
    that escalates Tier1 → Tier2 → Tier3 produces 3 sets of ✦ lines. Only
    the FINAL tier's events are written to the sheet. We handle this by
    keeping ✦ events in a dict keyed by event name — later occurrences
    overwrite earlier ones, so the surviving set matches the final tier.
    """
    text = log_path.read_text(encoding='utf-8', errors='replace')
    log_mtime = datetime.fromtimestamp(log_path.stat().st_mtime)

    posts = []
    current = None       # in-progress post dict (events as dict keyed by name)
    current_event = None # ref to event dict that the next sub-field lines describe

    def flush_current():
        if current is None:
            return
        # Convert events dict back to list (insertion order preserved in py3.7+)
        rec = {
            'post_id': current['post_id'],
            'log_mtime': current['log_mtime'],
            'format': 'new',
            'post_flags': current['post_flags'],
            'events': list(current['events'].values()),
        }
        if rec['events'] or rec['post_flags']:
            posts.append(rec)

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        # New post boundary (search, so it works whether prefixed or not)
        m = RE_PROCESSING.search(line)
        if m:
            flush_current()
            current = {
                'post_id': m.group(1),
                'log_mtime': log_mtime,
                'post_flags': set(),
                'events': {},  # keyed by event name; later writes overwrite earlier
            }
            current_event = None
            continue

        if current is None:
            continue

        # Done boundary — close the post (next Processing also would, but
        # being explicit avoids a stale current_event lingering for the
        # space between posts).
        if RE_NEW_DONE.search(line):
            flush_current()
            current = None
            current_event = None
            continue

        # ✦ event line — starts (or replaces) an event in the current post
        m = RE_EVENT_DOT.search(line)
        if m:
            tier_name, ev_name, ev_date = m.group(1), m.group(2).strip(), m.group(3).strip()
            current_event = {
                'name': ev_name,
                'date': None if ev_date in ('None', '?', '') else ev_date,
                'venue': None,
                'confidence': None,
                'authoritative_flags': set(),
                'tier': tier_name,
            }
            # Dedup by name — later occurrence (later tier) wins.
            current['events'][ev_name] = current_event
            continue

        # Post-level informational flags — recorded for diagnostics / fallback,
        # but the per-event ✦ Flags lines are the canonical source.
        m = RE_INFO_FLAGS.search(line)
        if m:
            raw = m.group(1).strip()
            for tok in re.findall(r"'([^']+)'", raw):
                current['post_flags'].add(tok)
            continue

        # Sub-field lines attach to the most recent ✦ event in this post.
        # These continuation lines are NOT prefixed because they share a
        # single print() with the ✦ line, but they ARE physically contiguous
        # in the log (atomic write), so attaching to current_event is safe.
        if current_event is not None:
            m = RE_NEW_VENUE.search(line)
            if m:
                val = m.group(1).strip()
                current_event['venue'] = None if val in ('None', '') else val
                continue
            m = RE_NEW_CONF.search(line)
            if m:
                try:
                    current_event['confidence'] = float(m.group(1))
                except ValueError:
                    pass
                continue
            m = RE_NEW_FLAGS.search(line)
            if m:
                raw = m.group(1).strip()
                if raw and raw.lower() != 'none':
                    for tok in raw.split(','):
                        tok = tok.strip()
                        if tok:
                            current_event['authoritative_flags'].add(tok)
                continue

    flush_current()
    return posts


def parse_logs(log_paths: list) -> dict:
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


# ─────────────────────────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────
# Sheet helpers
# ─────────────────────────────────────────────────────────────────

def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
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


# ─────────────────────────────────────────────────────────────────
# Flag derivation
# ─────────────────────────────────────────────────────────────────

def derive_flags(post_record, event_log, sheet_row, col_indices,
                 nj_lookup, all_event_dates_in_post, skip_day_mismatch=True):
    """
    Compute the canonical set of flags for one event.

    New-format short-circuit: if event_log['authoritative_flags'] is set
    (not None), it's the actual flag set the pipeline wrote at extract
    time. Return it directly — no derivation needed.

    Old-format fallback: apply the heuristic derivation rules (A)/(B)/(C)/(D)
    against post-level informational flags + event fields + sheet fields.

    post_record: dict from log parser (has post_flags)
    event_log: dict for this specific event from log
    sheet_row: list of cell values for this row in the sheet
    col_indices: dict of column-name -> index
    nj_lookup: dict of city -> region (from data/nj_municipalities.json)
    all_event_dates_in_post: list of parsed date objects for all events in this post
    skip_day_mismatch: if True, never apply DATE_DAY_MISMATCH (old format only)
    """
    # New format: per-event Flags from ✦ line is authoritative. Use as-is.
    auth = event_log.get('authoritative_flags')
    if auth is not None:
        return set(auth)

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

    sheet_city = sheet_val('CITY')
    sheet_venue = sheet_val('VENUE NAME')
    sheet_section = sheet_val('SECTION OF NJ')

    if 'CITY_NOT_IN_NJ_LOOKUP' in log_post_flags:
        # Apply to events whose sheet CITY is non-empty (not just "")
        if sheet_city:
            # Stricter: only flag if city actually isn't in NJ lookup
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

    # (D) DATE_DAY_MISMATCH — skip by default
    if not skip_day_mismatch and 'DATE_DAY_MISMATCH' in log_post_flags:
        flags.add('DATE_DAY_MISMATCH')

    return flags


# ─────────────────────────────────────────────────────────────────
# Sheet update batchers
# ─────────────────────────────────────────────────────────────────

def batch_update_text_cells(spreadsheet, ws, sheet_id, updates):
    """
    updates: list of (sheet_row, col_idx, new_value)
    """
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
    wipe_ops:  list of (sheet_row, col_idx) — set to white
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


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────

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
        print("❌ No log files found. Use --log <path> or --logs-dir <dir>.", file=sys.stderr)
        sys.exit(1)

    print(f"━━━ repair_flags_from_log.py ━━━")
    print(f"  Apply mode:           {args.apply}")
    print(f"  Strip extras:         {args.strip_extra}")
    print(f"  Aggressive day-flag:  {args.aggressive_day_mismatch}")
    print(f"  Logs ({len(log_paths)}):")
    for lp in log_paths:
        fmt = detect_log_format(lp)
        marker = '✓' if fmt == 'new' else '⚠'
        print(f"    {marker} [{fmt:3s}] {lp}")
    print()
    print("  Format notes:")
    print("    'new' = post-cutover logs with [W<n>] [<post_id>] line tags")
    print("            and per-event ✦ Flags — flags are AUTHORITATIVE,")
    print("            no derivation needed.")
    print("    'old' = pre-cutover logs without per-line tags. Heuristic")
    print("            derivation applied; ~85-95% accurate due to thread")
    print("            interleaving in carousel-heavy multi-event posts.")
    print()

    # Parse logs
    print("  Parsing logs...")
    log_by_post = parse_logs(log_paths)
    n_events = sum(len(p['events']) for p in log_by_post.values())
    n_flagged_posts = sum(1 for p in log_by_post.values() if p['post_flags'])
    n_new_format = sum(1 for p in log_by_post.values() if p.get('format') == 'new')
    n_authoritative = sum(
        1 for p in log_by_post.values() for e in p['events']
        if e.get('authoritative_flags') is not None
    )
    print(f"  Parsed {len(log_by_post)} posts with {n_events} events")
    print(f"    • {n_new_format} posts from new-format logs (authoritative per-event flags)")
    print(f"    • {n_authoritative}/{n_events} events have authoritative flags (vs derived)")
    print(f"    • {n_flagged_posts} posts had any post-level flags recorded")

    # Connect + read sheet
    print("\n  Connecting to sheet...")
    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)
    sheet_id = ws.id

    print("  Reading All_Events...")
    all_values = ws.get_all_values()
    if not all_values:
        print("⚠ Sheet is empty.")
        return
    header = all_values[0]
    data_rows = all_values[1:]
    print(f"  {len(data_rows)} data rows")

    # Resolve column indices
    cols = {
        'POST ID':         col_index(header, 'POST ID'),
        'EVENT NAME':      col_index(header, 'EVENT NAME'),
        'DATE':            col_index(header, 'DATE'),
        'VENUE NAME':      col_index(header, 'VENUE NAME'),
        'CITY':            col_index(header, 'CITY'),
        'SECTION OF NJ':   col_index(header, 'SECTION OF NJ'),
        'CONFIDENCE':      col_index(header, 'CONFIDENCE'),
        'DESCRIPTION':     col_index(header, 'DESCRIPTION'),
        'QUALITY_FLAGS':   col_index(header, 'QUALITY_FLAGS', 'QUALITY FLAGS'),
    }
    missing = [k for k, v in cols.items() if v is None]
    if missing:
        print(f"❌ Missing required columns in sheet header: {missing}", file=sys.stderr)
        sys.exit(1)
    # 2026-06-04: alias the QUALITY FLAGS column index under both header
    # forms. Round-2 FLAG_TO_COLUMNS now uses 'QUALITY FLAGS' (with space)
    # as the canonical column name (e.g. CALENDAR_LOW_EVENTS,
    # CAROUSEL_LOW_EVENTS, etc.). Without the alias, the paint/wipe loop
    # below silently skips those post-level flags because cols.get(col_name)
    # returns None.
    cols['QUALITY FLAGS'] = cols['QUALITY_FLAGS']

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
    text_updates = []     # (row, col, new_value)
    paint_ops = []        # (row, col, color)
    wipe_ops = []         # (row, col)
    audit_rows = []       # for CSV

    counts = {'matched': 0, 'updated': 0, 'log_only_no_match': 0,
              'sheet_only_not_in_log': 0, 'multi_match_skipped': 0}

    matched_keys = set()

    for post_id, post_rec in log_by_post.items():
        all_dates = [parse_date(e.get('date')) for e in post_rec['events']]
        all_dates = [d for d in all_dates if d is not None]

        for event_log in post_rec['events']:
            ev_name_upper = event_log['name'].strip().upper()
            ev_date_iso = normalize_date_for_key(event_log.get('date'))
            key = (post_id, ev_name_upper, ev_date_iso)

            sheet_matches = sheet_index.get(key, [])
            if not sheet_matches:
                counts['log_only_no_match'] += 1
                audit_rows.append({
                    'status': 'log_only_no_match',
                    'post_id': post_id,
                    'event_name': event_log['name'],
                    'date': event_log.get('date') or '',
                    'sheet_row': '',
                    'old_flags': '',
                    'new_flags': '',
                })
                continue
            if len(sheet_matches) > 1:
                counts['multi_match_skipped'] += 1
                audit_rows.append({
                    'status': 'multi_match_skipped',
                    'post_id': post_id,
                    'event_name': event_log['name'],
                    'date': event_log.get('date') or '',
                    'sheet_row': ';'.join(str(r) for r, _ in sheet_matches),
                    'old_flags': '',
                    'new_flags': '',
                })
                continue

            sheet_row_num, sheet_row = sheet_matches[0]
            matched_keys.add(key)

            # Compute derived flags
            derived = derive_flags(
                post_rec, event_log, sheet_row, cols, nj_lookup, all_dates,
                skip_day_mismatch=not args.aggressive_day_mismatch,
            )

            # Read current flags from sheet
            current_text = (sheet_row[cols['QUALITY_FLAGS']]
                            if cols['QUALITY_FLAGS'] < len(sheet_row) else '') or ''
            current = {f.strip() for f in current_text.split(',') if f.strip()}

            # Build new flag set
            if args.strip_extra:
                new_flags = derived
            else:
                # Preserve sheet extras, but always include all derived
                new_flags = current | derived
                # Remove DATE_DAY_MISMATCH only if not in derived AND not in sheet originally
                # (we leave existing ones alone since we can't verify them)

            if new_flags == current:
                counts['matched'] += 1
                audit_rows.append({
                    'status': 'matched',
                    'post_id': post_id,
                    'event_name': event_log['name'],
                    'date': event_log.get('date') or '',
                    'sheet_row': str(sheet_row_num),
                    'old_flags': ','.join(sorted(current)),
                    'new_flags': ','.join(sorted(new_flags)),
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
                'status': 'updated',
                'post_id': post_id,
                'event_name': event_log['name'],
                'date': event_log.get('date') or '',
                'sheet_row': str(sheet_row_num),
                'old_flags': ','.join(sorted(current)),
                'new_flags': ','.join(sorted(new_flags)),
            })

    # Sheet-only rows (in sheet, not in any parsed log)
    for key, matches in sheet_index.items():
        if key in matched_keys:
            continue
        for (row_num, row_data) in matches:
            counts['sheet_only_not_in_log'] += 1
            cur = (row_data[cols['QUALITY_FLAGS']] if cols['QUALITY_FLAGS'] < len(row_data) else '') or ''
            audit_rows.append({
                'status': 'sheet_only_not_in_log',
                'post_id': key[0],
                'event_name': key[1],
                'date': key[2],
                'sheet_row': str(row_num),
                'old_flags': cur,
                'new_flags': cur,
            })

    # ── REPORT ─────────────────────────────────────────────────────────
    print(f"\n━━━ FINDINGS ━━━")
    print(f"  Matched (no change needed):  {counts['matched']}")
    print(f"  Updates queued:              {counts['updated']}")
    print(f"  In log, not in sheet:        {counts['log_only_no_match']}")
    print(f"  In sheet, not in log:        {counts['sheet_only_not_in_log']}")
    print(f"  Ambiguous (multi-match):     {counts['multi_match_skipped']}")
    print(f"  Format ops queued:           {len(paint_ops)} paint, {len(wipe_ops)} wipe")

    # CSV audit
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path("outputs") / f"repair_flags_{ts}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=[
            'status', 'post_id', 'event_name', 'date',
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
        print(f"\n  ✓ Nothing to apply. Sheet already in sync.")
        return

    # ── APPLY ─────────────────────────────────────────────────────────
    print(f"\n━━━ APPLYING ━━━")
    print(f"  Updating {len(text_updates)} QUALITY_FLAGS cells...")
    batch_update_text_cells(sh, ws, sheet_id, text_updates)

    print(f"  Updating {len(paint_ops) + len(wipe_ops)} cell formats...")
    batch_update_format_cells(sh, ws, sheet_id, paint_ops, wipe_ops)

    print(f"\n✓ Done. Audit trail: {csv_path}")


if __name__ == "__main__":
    main()
