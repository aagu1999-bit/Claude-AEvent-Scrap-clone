#!/usr/bin/env python3
"""
merge_extract_runs.py — combine multiple extract_runs/*.json summaries into one.

PURPOSE
───────
When a run is interrupted (Ctrl+C, network blip, container restart) and
resumed later under a different --tag, you end up with two or more partial
JSON summaries — none of which alone covers the full work. This tool
combines them into a single unified summary.

Use cases:
  - Combining an interrupted run's partial summary with its resume run's summary
  - Aggregating multiple smoke-test runs for a higher-level view
  - Cross-run cost/quality analysis at a campaign level

USAGE
─────
  # Two specific files
  python merge_extract_runs.py outputs/extract_runs/run1.json outputs/extract_runs/run2.json

  # Glob pattern (shell-expanded)
  python merge_extract_runs.py outputs/extract_runs/full_recovery_*.json

  # Save merged JSON instead of printing
  python merge_extract_runs.py outputs/extract_runs/*.json -o combined.json

HOW MERGING WORKS
─────────────────
For each field type:
  - wall_time_seconds       → sum across runs
  - phase_timings_seconds   → sum per phase (apify_load, ocr, gemini, sheet_write)
  - counts                  → element-wise sum
  - tiers_used              → element-wise sum
  - flag_distribution       → element-wise sum
  - skip_reasons            → element-wise sum
  - per_account_events      → element-wise sum (top 100 in output)
  - api_call_counts         → element-wise sum
  - region_lookup           → element-wise sum (all sub-counters)
  - estimated_cost_usd      → sum
  - completion_status       → "completed" only if ALL inputs completed; else "interrupted"
  - merged_from             → list of source run_ids for traceability

Constants from the inputs (tag, mode, queue_csv, source_datasets, max_tier)
are taken from the FIRST input and a warning printed if they differ across
inputs — merging fundamentally-different runs is allowed but flagged.
"""

import argparse
import glob
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path


def merge_summaries(paths: list, output_path: str = None) -> dict:
    summaries = []
    for p in paths:
        try:
            summaries.append(json.loads(Path(p).read_text()))
        except Exception as e:
            print(f"⚠ Failed to read {p}: {e}", file=sys.stderr)
    if not summaries:
        print("❌ No valid summaries to merge", file=sys.stderr)
        sys.exit(1)

    # Flag config drift (taking first run's values as canonical)
    first = summaries[0]
    drift_warnings = []
    for s in summaries[1:]:
        for k in ('tag', 'mode', 'queue_csv', 'source_datasets', 'max_tier'):
            if s.get(k) != first.get(k):
                drift_warnings.append(
                    f"  {k}: first={first.get(k)!r} vs {s.get('run_id', '?')}={s.get(k)!r}"
                )
    if drift_warnings:
        print("⚠ Inputs have differing config — merging anyway:", file=sys.stderr)
        for w in drift_warnings:
            print(w, file=sys.stderr)

    merged = {
        'merged_from':         [s.get('run_id', '?') for s in summaries],
        'merged_count':        len(summaries),
        'merge_timestamp':     datetime.now().isoformat(),
        'tag':                 first.get('tag', ''),
        'mode':                first.get('mode', 'extract'),
        'completion_status':   ('completed' if all(s.get('completion_status') == 'completed'
                                                   for s in summaries)
                                else 'interrupted'),
        'any_interrupted':     any(s.get('interrupted') for s in summaries),
        'queue_csv':           first.get('queue_csv', ''),
        'source_datasets':     first.get('source_datasets', []),
        'max_tier':            first.get('max_tier', ''),
        'wall_time_seconds':   round(sum(s.get('wall_time_seconds', 0) for s in summaries), 2),
        'phase_timings_seconds': {},
        'counts':              Counter(),
        'tiers_used':          Counter(),
        'flag_distribution':   Counter(),
        'skip_reasons':        Counter(),
        'per_account_events':  Counter(),
        'api_call_counts':     Counter(),
        'region_lookup':       Counter(),
        'estimated_cost_usd':  0.0,
    }

    for s in summaries:
        # Phase timings: sum per key
        for k, v in (s.get('phase_timings_seconds') or {}).items():
            merged['phase_timings_seconds'][k] = round(
                merged['phase_timings_seconds'].get(k, 0.0) + v, 2
            )
        # Counter-typed dicts: element-wise sum
        for key in ('counts', 'tiers_used', 'flag_distribution', 'skip_reasons',
                    'per_account_events', 'api_call_counts', 'region_lookup'):
            merged[key].update(s.get(key) or {})
        merged['estimated_cost_usd'] += s.get('estimated_cost_usd', 0) or 0

    # Convert Counters back to plain dicts. Cap per_account_events at top-100
    # to keep the merged output a reasonable size.
    merged['counts'] = dict(merged['counts'])
    merged['tiers_used'] = dict(merged['tiers_used'])
    merged['flag_distribution'] = dict(merged['flag_distribution'])
    merged['skip_reasons'] = dict(merged['skip_reasons'])
    merged['per_account_events'] = dict(merged['per_account_events'].most_common(100))
    merged['api_call_counts'] = dict(merged['api_call_counts'])
    merged['region_lookup'] = dict(merged['region_lookup'])
    merged['estimated_cost_usd'] = round(merged['estimated_cost_usd'], 4)

    if output_path:
        Path(output_path).write_text(json.dumps(merged, indent=2, sort_keys=True))
        print(f"✓ Merged {len(summaries)} summaries → {output_path}")
    else:
        print(json.dumps(merged, indent=2, sort_keys=True))

    return merged


def print_human_summary(merged: dict):
    """Quick human-readable summary printed to stderr (so stdout can be piped)."""
    c = merged['counts']
    print(f"\n━━━ MERGED SUMMARY ({merged['merged_count']} runs) ━━━", file=sys.stderr)
    print(f"  Status:                 {merged['completion_status']}", file=sys.stderr)
    print(f"  Wall time:              {merged['wall_time_seconds']:.1f}s "
          f"(combined across runs)", file=sys.stderr)
    print(f"  Queue rows:             {c.get('queue_size', 0)} "
          f"(may double-count if same queue ran multiple times)", file=sys.stderr)
    print(f"  Posts processed:        {c.get('processed', 0)}", file=sys.stderr)
    print(f"  Events extracted:       {c.get('extracted_events', 0)}", file=sys.stderr)
    print(f"  Events with flags:      {c.get('events_with_flags', 0)}", file=sys.stderr)
    print(f"  Estimated cost:         ${merged['estimated_cost_usd']:.4f}", file=sys.stderr)
    if merged.get('any_interrupted'):
        print(f"  ⚠ One or more source runs were interrupted", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(
        description="Combine multiple extract_runs/*.json summaries into one.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument('inputs', nargs='+',
                   help='JSON files to merge (shell glob patterns supported)')
    p.add_argument('--output', '-o', help='Write merged JSON to file (default: stdout)')
    p.add_argument('--quiet', '-q', action='store_true',
                   help='Suppress human-readable summary on stderr')
    args = p.parse_args()

    # Resolve any glob patterns the shell didn't expand
    paths = []
    for inp in args.inputs:
        matched = glob.glob(inp)
        if matched:
            paths.extend(matched)
        else:
            paths.append(inp)  # treat as literal path; will fail loudly if missing
    paths = sorted(set(paths))

    if not paths:
        print(f"❌ No files matched: {args.inputs}", file=sys.stderr)
        sys.exit(1)

    print(f"Merging {len(paths)} file(s):", file=sys.stderr)
    for f in paths:
        print(f"  {f}", file=sys.stderr)

    merged = merge_summaries(paths, args.output)
    if not args.quiet:
        print_human_summary(merged)


if __name__ == '__main__':
    main()
