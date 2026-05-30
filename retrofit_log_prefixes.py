#!/usr/bin/env python3
"""
retrofit_log_prefixes.py -- add [post_id] prefixes to legacy main.py logs.

PURPOSE
-------
Logs written before worker_log.py was integrated have interleaved output from
parallel workers, with no per-line post_id. This script reads those logs and
writes new files with every line prefixed by a post_id, so downstream tools
(repair_flags_from_log.py) can route lines correctly even when the original
log had jumbled output.

TWO MODES
---------
--mode naive   (default; offline, no auth needed)
   Prefix every line with the most-recent "Processing post: XXX" ID. Makes
   the log mechanically uniform; does NOT correct misattribution caused by
   interleaving. Useful if you just want one consistent format for all logs.

--mode smart   (requires gspread auth + All_Events sheet access)
   Same as naive for most lines, BUT for event-marker lines (✦ tier, ✓ EVENT
   FOUND, multi-event list items) it cross-references the event's (name, date)
   against All_Events. If the sheet says this event belongs to a different
   post than the current candidate, the line (and its trailing field/Flags
   lines) get re-attributed to the correct post. This actually corrects
   misattribution from interleaved output.

USAGE
-----
  # Naive retrofit, all logs in outputs/, write to outputs_retrofit/
  python retrofit_log_prefixes.py --logs-dir outputs/ --out-dir outputs_retrofit/

  # Smart retrofit using sheet
  python retrofit_log_prefixes.py --logs-dir outputs/ --out-dir outputs_retrofit/ --mode smart

  # Single file
  python retrofit_log_prefixes.py --log outputs/run_X.log --out-dir outputs_retrofit/

OUTPUT
------
For each input run_X.log, writes outputs_retrofit/run_X.log (same basename).
Prints a per-file summary: lines processed, lines prefixed, lines corrected
(smart mode only), lines left unattributed.

SAFETY
------
Never modifies input files. Output dir created if missing. Output files
overwrite within the output dir (idempotent: re-running produces the same
result).
"""

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

RE_PREFIX = re.compile(r'^\s*\[([A-Za-z0-9_-]{6,})\]\s+')
RE_PROCESSING = re.compile(r'\[\d+/\d+\]\s+Processing post:\s+(\S+)')

RE_NEW_EVENT = re.compile(
    r"\[([a-z][a-z0-9_]*)\]\s+[\'\"\u2018\u201c](.+?)[\'\"\u2019\u201d]\s+\|\s+(\S+)\s*$"
)
RE_EVENT_FOUND  = re.compile(r'EVENT FOUND:\s+(.+?)\s*$')
RE_MULTI_HEADER = re.compile(r'MULTIPLE EVENTS FOUND:\s+(\d+)\s+events extracted')
RE_MULTI_ITEM   = re.compile(r'^\s+(\d+)\.\s+(.+?)\s*$')


def _normalize_name(s):
    return (s or '').strip().upper()


def build_sheet_event_index():
    """
    Smart-mode helper: connect to the sheet and return:
      { (event_name_upper, date_iso): [post_id, ...] }
    Post IDs are listed in the order they appear in the sheet.
    """
    import gspread
    import os as _os
    from oauth2client.service_account import ServiceAccountCredentials
    SERVICE_ACCOUNT_FILE = _os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or _os.environ.get("SERVICE_ACCOUNT_FILE") or "apt-mark-468506-u9-ec44cabc7335 copy.json"
    SHEET_NAME = "Instagram_Events_Master"
    ALL_EVENTS_TAB = "All_Events"

    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    sh = gspread.authorize(creds).open(SHEET_NAME)
    ws = sh.worksheet(ALL_EVENTS_TAB)
    rows = ws.get_all_values()
    if not rows:
        return {}

    header = rows[0]

    def col_idx(*names):
        for name in names:
            for i, h in enumerate(header):
                if h.strip().upper() == name.upper():
                    return i
        return None

    pid_col, name_col, date_col = col_idx('POST ID'), col_idx('EVENT NAME'), col_idx('DATE')
    if None in (pid_col, name_col, date_col):
        raise RuntimeError("Sheet missing POST ID / EVENT NAME / DATE columns")

    from datetime import datetime
    DATE_FMTS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y")

    def norm_date(s):
        s = (s or '').strip()
        if not s:
            return ''
        for fmt in DATE_FMTS:
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except ValueError:
                continue
        return s

    index = defaultdict(list)
    for row in rows[1:]:
        if len(row) <= max(pid_col, name_col, date_col):
            continue
        pid = (row[pid_col] or '').strip()
        name = _normalize_name(row[name_col])
        date = norm_date(row[date_col])
        if pid and name:
            index[(name, date)].append(pid)
    return dict(index)


def _maybe_extract_event_marker(body):
    """
    Return (event_name, date_iso_or_raw) if this line is an event marker,
    else None.
    """
    m = RE_NEW_EVENT.search(body)
    if m:
        date = m.group(3).strip()
        return (m.group(2).strip(), date)
    m = RE_EVENT_FOUND.search(body)
    if m:
        return (m.group(1).strip(), '')
    m = RE_MULTI_ITEM.match(body)
    if m and RE_MULTI_HEADER.search(body) is None:
        return (m.group(2).strip(), '')
    return None


def retrofit_one_log(in_path, out_path, mode, sheet_index):
    """
    Walk a single log file, emit a retrofitted version with [post_id] prefixes.
    Returns a stats dict.
    """
    text = in_path.read_text(encoding='utf-8', errors='replace')
    out_lines = []

    candidate = None
    last_event_post = None

    stats = {'total': 0, 'prefixed': 0, 'pre_prefixed': 0,
             'corrected': 0, 'unattributed': 0}

    for raw_line in text.splitlines():
        stats['total'] += 1
        line = raw_line.rstrip('\n')

        m_pre = RE_PREFIX.match(line)
        if m_pre:
            stats['pre_prefixed'] += 1
            body_pre = line[m_pre.end():]
            m_proc = RE_PROCESSING.search(body_pre)
            if m_proc:
                candidate = m_proc.group(1)
            last_event_post = m_pre.group(1)
            out_lines.append(line)
            continue

        m_proc = RE_PROCESSING.search(line)
        if m_proc:
            candidate = m_proc.group(1)
            last_event_post = candidate
            out_lines.append(f"[{candidate}] {line}" if candidate else line)
            if candidate:
                stats['prefixed'] += 1
            else:
                stats['unattributed'] += 1
            continue

        assigned = candidate
        was_corrected = False

        if mode == 'smart' and sheet_index:
            ev = _maybe_extract_event_marker(line)
            if ev is not None:
                ev_name, ev_date = ev
                key = (_normalize_name(ev_name), ev_date)
                pids = sheet_index.get(key)
                if not pids and not ev_date:
                    matches = [pid for (n, d), pids2 in sheet_index.items()
                               for pid in pids2 if n == _normalize_name(ev_name)]
                    pids = list(dict.fromkeys(matches))
                if pids:
                    if candidate in pids:
                        assigned = candidate
                    elif len(pids) == 1:
                        assigned = pids[0]
                        was_corrected = True
                    else:
                        assigned = candidate
                last_event_post = assigned

        if mode == 'smart' and assigned == candidate and last_event_post and last_event_post != candidate:
            assigned = last_event_post

        if assigned:
            out_lines.append(f"[{assigned}] {line}")
            stats['prefixed'] += 1
            if was_corrected:
                stats['corrected'] += 1
        else:
            out_lines.append(line)
            stats['unattributed'] += 1

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text('\n'.join(out_lines) + ('\n' if text.endswith('\n') else ''),
                        encoding='utf-8')
    return stats


def main():
    p = argparse.ArgumentParser(
        description="Add [post_id] prefixes to legacy main.py run logs.",
        formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__)
    p.add_argument("--log", action="append", default=[],
                   help="Path to a single run_*.log file. Repeatable.")
    p.add_argument("--logs-dir", default=None,
                   help="Directory of run_*.log files (process all).")
    p.add_argument("--out-dir", required=True,
                   help="Output directory for retrofitted logs.")
    p.add_argument("--mode", choices=["naive", "smart"], default="naive",
                   help="naive = candidate-only; smart = use sheet to correct.")
    args = p.parse_args()

    paths = [Path(x) for x in args.log]
    if args.logs_dir:
        paths.extend(sorted(Path(args.logs_dir).glob("run_*.log")))
    paths = [p for p in paths if p.exists()]
    if not paths:
        print("ERROR: No log files found.", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.out_dir)

    sheet_index = None
    if args.mode == "smart":
        print("  Connecting to sheet for smart-mode lookup...")
        sheet_index = build_sheet_event_index()
        print(f"  Indexed {sum(len(v) for v in sheet_index.values())} sheet event rows "
              f"({len(sheet_index)} unique name+date keys).")

    print(f"\n=== retrofit_log_prefixes.py (mode={args.mode}) ===")
    print(f"  Input logs:  {len(paths)}")
    print(f"  Output dir:  {out_dir}")
    print()

    totals = defaultdict(int)
    for in_path in paths:
        out_path = out_dir / in_path.name
        stats = retrofit_one_log(in_path, out_path, args.mode, sheet_index)
        print(f"  {in_path.name}:")
        print(f"    lines={stats['total']:>7}  "
              f"prefixed={stats['prefixed']:>7}  "
              f"pre-prefixed={stats['pre_prefixed']:>5}  "
              f"corrected={stats['corrected']:>5}  "
              f"unattributed={stats['unattributed']:>5}")
        for k, v in stats.items():
            totals[k] += v

    print(f"\n=== TOTALS ===")
    for k, v in totals.items():
        print(f"  {k:>14}: {v}")
    print(f"\nOK: Retrofitted logs written to {out_dir}/")


if __name__ == "__main__":
    main()
