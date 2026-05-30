#!/usr/bin/env python3
"""
quality_metrics.py — snapshot the current quality state of All_Events.

Captures a baseline of measurable quality signals so you can detect
drift after a PR merge. Run before and after each PR; eyeball the
deltas. Big unexpected shift = investigate before relaxing monitoring.

WHAT IT MEASURES
────────────────
1. Flag distribution
   - Count of rows for each QUALITY_FLAGS value
   - Rows with multiple flags
   - Rows with NO flags
2. Confidence statistics
   - Mean / median / min / max
   - Count of out-of-range values (i.e. not in 0.0–1.0 — these are bugs)
   - Histogram by 0.1 bucket
3. Hallucination indicators
   - "New Jersey" / "NJ" / "NJ area" appearing as CITY (should be 0)
   - Empty CITY rows (per the new prompt, empty is preferred over guessing)
   - Empty DATE rows
4. Event-rate per post
   - Avg events per POST ID
   - Posts producing >5 events (suspicious — often calendar-style)

WHEN TO RUN
───────────
- Before merging any PR — capture baseline
- After merging the same PR — compare
- Periodically (monthly?) to detect prompt drift over time

USAGE
─────
  python quality_metrics.py                       # full scan, prints summary + JSON
  python quality_metrics.py --quiet               # JSON-only (machine-readable)
  python quality_metrics.py --since 18400         # scan only rows >= N (e.g. recent only)
  python quality_metrics.py --compare <json>      # compare current to a prior snapshot

OUTPUT
──────
- Console: human-readable summary
- File: outputs/quality_metrics_<ts>.json  (canonical snapshot for later --compare)

This is READ-ONLY. Never modifies your sheets.
"""

import os
import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("SERVICE_ACCOUNT_FILE") or "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"
OUTPUT_DIR = Path("outputs")

# Strings that should NOT appear as a city — they're hallucinated
# generics. Compared case-insensitively, exact match.
HALLUCINATED_CITY_PATTERNS = [
    "NEW JERSEY", "NJ", "NJ AREA", "NJ.", "N.J.",
    "JERSEY", "USA", "UNITED STATES",
]


def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found: {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def safe_float(v, default=None):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def compute_metrics(rows, header, since=None):
    """Return a dict of metrics. `rows` is the raw list-of-lists from
    get_all_values() (header already excluded). `header` is the header row."""
    # Resolve column indices once
    col = {name: header.index(name) for name in header}
    pid_idx     = col.get('POST ID')
    flags_idx   = col.get('QUALITY_FLAGS')
    conf_idx    = col.get('CONFIDENCE')
    city_idx    = col.get('CITY')
    date_idx    = col.get('DATE')
    section_idx = col.get('SECTION OF NJ')

    flag_counts = Counter()
    multi_flag_rows = 0
    no_flag_rows = 0
    confidence_values = []
    confidence_out_of_range = 0
    city_blank = 0
    date_blank = 0
    section_blank = 0
    hallucinated_city_rows = []  # list of (row_num, city)
    events_per_pid = Counter()
    rows_considered = 0

    for i, row in enumerate(rows, start=2):
        if since and i < since:
            continue
        if not row:
            continue
        rows_considered += 1

        flags_str = (row[flags_idx] if flags_idx is not None and len(row) > flags_idx else '').strip()
        flags = [f.strip() for f in flags_str.split(',') if f.strip()] if flags_str else []
        if not flags:
            no_flag_rows += 1
        else:
            for f in flags:
                flag_counts[f] += 1
            if len(flags) > 1:
                multi_flag_rows += 1

        conf_raw = row[conf_idx] if conf_idx is not None and len(row) > conf_idx else ''
        conf = safe_float(conf_raw)
        if conf is not None:
            confidence_values.append(conf)
            if conf < 0 or conf > 1:
                confidence_out_of_range += 1

        city = (row[city_idx] if city_idx is not None and len(row) > city_idx else '').strip()
        if not city:
            city_blank += 1
        elif city.upper() in HALLUCINATED_CITY_PATTERNS:
            hallucinated_city_rows.append((i, city))

        date_val = (row[date_idx] if date_idx is not None and len(row) > date_idx else '').strip()
        if not date_val:
            date_blank += 1

        section_val = (row[section_idx] if section_idx is not None and len(row) > section_idx else '').strip()
        if not section_val:
            section_blank += 1

        pid = (row[pid_idx] if pid_idx is not None and len(row) > pid_idx else '').strip()
        if pid:
            events_per_pid[pid] += 1

    # Confidence statistics
    conf_stats = {}
    if confidence_values:
        s = sorted(confidence_values)
        conf_stats = {
            'count':           len(s),
            'mean':            sum(s) / len(s),
            'median':          s[len(s) // 2],
            'min':             s[0],
            'max':             s[-1],
            'out_of_range':    confidence_out_of_range,
            'below_0.6':       sum(1 for v in s if v < 0.6),
        }
        # Histogram by 0.1
        hist = Counter()
        for v in s:
            bucket = round(v, 1)
            hist[bucket] += 1
        conf_stats['histogram'] = dict(sorted(hist.items()))

    # Events-per-post
    epp_stats = {}
    if events_per_pid:
        counts = sorted(events_per_pid.values(), reverse=True)
        epp_stats = {
            'distinct_pids':           len(events_per_pid),
            'total_events':            sum(counts),
            'avg_events_per_post':     sum(counts) / len(counts),
            'pids_with_1_event':       sum(1 for c in counts if c == 1),
            'pids_with_2to5_events':   sum(1 for c in counts if 2 <= c <= 5),
            'pids_with_over_5_events': sum(1 for c in counts if c > 5),
            'max_events_in_one_post':  counts[0],
        }
        # Top 5 highest-event posts (often calendar-style — useful for spot-check)
        epp_stats['top_5_high_event_pids'] = [
            {'pid': pid, 'events': n}
            for pid, n in events_per_pid.most_common(5)
        ]

    return {
        'rows_considered': rows_considered,
        'flag_distribution':     dict(flag_counts.most_common()),
        'multi_flag_rows':       multi_flag_rows,
        'no_flag_rows':          no_flag_rows,
        'confidence':            conf_stats,
        'city_blank':            city_blank,
        'date_blank':            date_blank,
        'section_blank':         section_blank,
        'hallucinated_city_count': len(hallucinated_city_rows),
        'hallucinated_city_samples': hallucinated_city_rows[:10],
        'events_per_post':       epp_stats,
    }


def print_summary(m, since=None):
    print(f"━━━ QUALITY METRICS ━━━")
    print(f"  Rows considered:  {m['rows_considered']:>7}"
          + (f"   (from row {since} onward)" if since else ""))
    print()
    print(f"  Flag distribution:")
    if m['flag_distribution']:
        for flag, n in m['flag_distribution'].items():
            print(f"    {flag:<28} {n:>6}")
    else:
        print(f"    (none)")
    print(f"    {'(multi-flag rows)':<28} {m['multi_flag_rows']:>6}")
    print(f"    {'(no-flag rows)':<28} {m['no_flag_rows']:>6}")
    print()

    if m['confidence']:
        c = m['confidence']
        print(f"  Confidence:")
        print(f"    count={c['count']}  mean={c['mean']:.3f}  median={c['median']:.3f}"
              f"  min={c['min']:.2f}  max={c['max']:.2f}")
        print(f"    out-of-range (NOT in 0..1): {c['out_of_range']}"
              + ("  ⚠ THESE ARE BUGS" if c['out_of_range'] > 0 else ""))
        print(f"    below 0.6 (would trigger LOW_CONFIDENCE flag): {c['below_0.6']}")
        print()

    print(f"  Missing fields:")
    print(f"    {'CITY blank':<28} {m['city_blank']:>6}")
    print(f"    {'DATE blank':<28} {m['date_blank']:>6}")
    print(f"    {'SECTION OF NJ blank':<28} {m['section_blank']:>6}")
    print()

    print(f"  Hallucinated city rows: {m['hallucinated_city_count']}"
          + ("  ⚠ SHOULD BE 0" if m['hallucinated_city_count'] > 0 else ""))
    if m['hallucinated_city_samples']:
        print(f"    Samples:")
        for row_num, city in m['hallucinated_city_samples']:
            print(f"      row {row_num}: CITY={city!r}")
    print()

    if m['events_per_post']:
        e = m['events_per_post']
        print(f"  Events-per-post:")
        print(f"    distinct posts:    {e['distinct_pids']:>6}")
        print(f"    total events:      {e['total_events']:>6}")
        print(f"    avg events/post:   {e['avg_events_per_post']:.2f}")
        print(f"    1 event:           {e['pids_with_1_event']:>6}")
        print(f"    2-5 events:        {e['pids_with_2to5_events']:>6}")
        print(f"    > 5 events:        {e['pids_with_over_5_events']:>6}"
              + "  ← spot-check; often calendar posts")
        print(f"    max in one post:   {e['max_events_in_one_post']:>6}")


def print_compare(curr, prior):
    """Side-by-side delta. `prior` is a previously saved metrics dict."""
    def delta(label, curr_v, prior_v, fmt='{:>6}'):
        c = curr_v if curr_v is not None else 0
        p = prior_v if prior_v is not None else 0
        d = c - p
        arrow = "→" if d == 0 else ("↑" if d > 0 else "↓")
        print(f"  {label:<28} {fmt.format(p)} {arrow} {fmt.format(c)}"
              f"  (Δ {d:+})")

    print(f"━━━ COMPARISON ━━━")
    print(f"  {'metric':<28} {'before':>6}   {'after':>6}   delta")
    delta('rows_considered',     curr.get('rows_considered'),     prior.get('rows_considered'))
    delta('multi_flag_rows',     curr.get('multi_flag_rows'),     prior.get('multi_flag_rows'))
    delta('no_flag_rows',        curr.get('no_flag_rows'),        prior.get('no_flag_rows'))
    delta('city_blank',          curr.get('city_blank'),          prior.get('city_blank'))
    delta('date_blank',          curr.get('date_blank'),          prior.get('date_blank'))
    delta('hallucinated_city',   curr.get('hallucinated_city_count'),
                                  prior.get('hallucinated_city_count'))
    if curr.get('confidence') and prior.get('confidence'):
        cc, pp = curr['confidence'], prior['confidence']
        delta('conf out-of-range', cc.get('out_of_range'), pp.get('out_of_range'))
        delta('conf below 0.6',    cc.get('below_0.6'),    pp.get('below_0.6'))
        cm = cc.get('mean'); pm = pp.get('mean')
        if cm is not None and pm is not None:
            print(f"  {'conf mean':<28} {pm:>6.3f}   {cm:>6.3f}   (Δ {cm - pm:+.3f})")

    print()
    print(f"  Flag distribution deltas:")
    all_flags = set(curr.get('flag_distribution', {}).keys()) | set(prior.get('flag_distribution', {}).keys())
    for flag in sorted(all_flags):
        c = curr.get('flag_distribution', {}).get(flag, 0)
        p = prior.get('flag_distribution', {}).get(flag, 0)
        d = c - p
        arrow = "→" if d == 0 else ("↑" if d > 0 else "↓")
        print(f"    {flag:<28} {p:>6} {arrow} {c:>6}  (Δ {d:+})")


def main():
    p = argparse.ArgumentParser(
        description="Snapshot All_Events quality state.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--quiet", action="store_true",
                   help="JSON-only output (suppress human-readable summary)")
    p.add_argument("--since", type=int, default=None,
                   help="Only consider rows >= this row number (1-based, header is row 1)")
    p.add_argument("--compare", type=str, default=None,
                   help="Path to a prior quality_metrics_*.json — print delta")
    args = p.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    if not args.quiet:
        print(f"━━━ quality_metrics.py ━━━")
        print(f"  Since:    {args.since or '(all)'}")
        print(f"  Compare:  {args.compare or '(no)'}")
        print()

    sh = setup_sheet()

    t0 = time.time()
    try:
        ae = sh.worksheet(ALL_EVENTS_TAB)
    except gspread.WorksheetNotFound:
        print(f"❌ '{ALL_EVENTS_TAB}' tab not found", file=sys.stderr)
        sys.exit(1)

    ae_data = ae.get_all_values()
    if not ae_data:
        print(f"❌ '{ALL_EVENTS_TAB}' is empty", file=sys.stderr)
        sys.exit(1)

    header = ae_data[0]
    if not args.quiet:
        print(f"  Read {len(ae_data)-1} All_Events rows in {time.time()-t0:.1f}s")
        print()

    metrics = compute_metrics(ae_data[1:], header, since=args.since)
    metrics['_meta'] = {
        'timestamp':       ts,
        'sheet':           SHEET_NAME,
        'tab':             ALL_EVENTS_TAB,
        'since_row':       args.since,
        'total_data_rows': len(ae_data) - 1,
    }

    json_path = OUTPUT_DIR / f'quality_metrics_{ts}.json'
    with open(json_path, 'w') as f:
        json.dump(metrics, f, indent=2, default=str)
    if not args.quiet:
        print(f"✓ Wrote {json_path}")
        print()
        print_summary(metrics, since=args.since)

    if args.compare:
        try:
            with open(args.compare) as f:
                prior = json.load(f)
            print()
            print_compare(metrics, prior)
        except Exception as e:
            print(f"\n⚠ Could not read --compare file: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
