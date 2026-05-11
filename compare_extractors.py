#!/usr/bin/env python3
"""
compare_extractors.py — diff main.py vs events_from_ids.py extractions.

WHAT IT DOES
────────────
For N cached Apify posts, runs BOTH extraction prompts (main.py's
current prompt + events_from_ids.py's prompt) against Gemini, then
diffs the resulting events field-by-field.

Both tools use the SAME extraction_core sanity checks, so any output
difference is attributable to PROMPT differences (which is what we
want to track for PR B validation).

USAGE
─────
  python compare_extractors.py                  # 5 cached posts, most recent dataset
  python compare_extractors.py --n 10           # 10 posts
  python compare_extractors.py --post-ids file.csv  # specific posts (CSV: post_id column)
  python compare_extractors.py --cache <path>   # specific apify_cache file

WHEN TO RUN
───────────
- BEFORE PR B (prompt backport) — captures baseline divergence
- AFTER PR B — should drop to ~0 differences (both prompts identical)
- Monthly thereafter — detect Gemini-side drift between identical prompts

OUTPUT
──────
- Console: per-post summary (M events vs E events, X fields differ)
- File: outputs/compare_<ts>.csv (side-by-side, one row per event field)
- File: outputs/compare_<ts>.json (machine-readable for trend tracking)

COSTS
─────
~2 Gemini API calls per post (one per prompt). At ~$0.001 each on
Flash-Lite, ~$0.01 for 5 posts. Not a budget concern but worth
noting if you run this in a loop.

This is READ-ONLY. Never modifies your sheets.
"""

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path("outputs")
CACHE_DIR  = Path("outputs/apify_cache")

# Fields we compare per event. Listed here so the diff is deterministic
# regardless of dict ordering or extra keys Gemini might return.
COMPARED_FIELDS = [
    'event_name', 'date', 'start_time', 'venue_name', 'city',
    'section_of_nj', 'event_type', 'confidence', 'is_recurring',
]


# ────────────────────────────────────────────────────────────────
# Prompt builders
# ────────────────────────────────────────────────────────────────
#
# IMPORTANT: main.py's prompt is hardcoded as an f-string inside
# process_post() and isn't easily importable. We mirror it here.
# If main.py's prompt changes, update this function to match.
# After PR B, main.py should call extraction_core.build_prompt
# directly and this duplication can be removed.

def build_prompt_main(post: dict, ocr_text: str, post_date_str: str) -> str:
    """Mirrors main.py's current prompt (lines 913-972 in main.py).
    Update this if main.py's prompt changes BEFORE you delete it post-PR B."""
    user = post.get('ownerUsername', '')
    owner_full_name = post.get('ownerFullName', '')
    location_name = post.get('locationName', '') or post.get('location', '')
    caption = post.get('caption', '') or post.get('text', '')

    return f"""
        Extract ALL events from this Instagram post. A post may contain MULTIPLE events.

        POST DATE: {post_date_str} (use this to resolve relative and recurring dates)
        ACCOUNT: @{user} ({owner_full_name})
        LOCATION TAG: {location_name}

        CAPTION: {caption[:2000]}
        OCR TEXT FROM IMAGE(S): {ocr_text[:5000]}
        NOTE: If OCR text contains [SLIDE N of M] markers, this is a carousel post.
        Each slide may show different events (e.g. a weekly calendar spread across slides).
        Extract events from ALL slides.

        EXTRACTION INSTRUCTIONS:
        1. Look for MULTIPLE events - calendars, weekly lineups, event series
        2. Common patterns: "Monday: Jazz Night, Tuesday: Open Mic"
        3. Monthly calendars: "Dec 15 - Band Name, Dec 22 - Holiday Party"
        4. Each date/event combination should be a separate event
        5. If location_name exists, use it as venue for ALL events

        DATE PARSING — handle ALL of these formats and convert to YYYY-MM-DD:
        - Shorthand with dots: "3.13.26" or "3.13" → use POST DATE year if year omitted
        - Shorthand with slashes: "3/13", "3/13/26", "03/13/2026"
        - Written out: "March 13th", "March 13", "Mar 13"
        - Day references: "this Saturday", "next Friday", "this weekend" → calculate from POST DATE
        - Year shorthand: "26" means 2026, "25" means 2025

        REQUIREMENTS:
        1. "event_name": Maximum 40 characters.
        2. "section_of_nj": North/Central/South based on city/county
        3. TIME: Strict 12-hour format (e.g. 2:00 PM).

        Return JSON with "events" list containing:
        event_name, date (YYYY-MM-DD), start_time, venue_name, city, section_of_nj,
        newsletter_description, event_type, description, performer, price, confidence, is_recurring

        Also include "total_events_found": number, "is_calendar_post": true/false
        If no events found, return: {{"events": [], "total_events_found": 0}}
    """


def build_prompt_eft(post: dict, ocr_text: str, post_date) -> str:
    """Use the actual events_from_ids.py build_prompt — single source
    of truth for the 'good' prompt. Import lazily to keep startup fast."""
    from events_from_ids import build_prompt
    return build_prompt(post, ocr_text, post_date)


# ────────────────────────────────────────────────────────────────
# Gemini wrapper
# ────────────────────────────────────────────────────────────────

def call_gemini(model, prompt):
    """Call Gemini, parse JSON, return list of events or empty list.
    Catches errors and returns None to signal failure (caller decides
    how to record it)."""
    try:
        resp = model.generate_content(prompt)
        text = resp.text.strip()
        # Strip code fences if present
        text = re.sub(r'^```json\s*|```\s*$', '', text, flags=re.MULTILINE).strip()
        data = json.loads(text)
        return data.get('events', []), None
    except json.JSONDecodeError as e:
        return None, f'JSON parse failed: {e}'
    except Exception as e:
        return None, f'Gemini call failed: {type(e).__name__}: {str(e)[:120]}'


# ────────────────────────────────────────────────────────────────
# Event comparison
# ────────────────────────────────────────────────────────────────

def normalize_value(v):
    """Normalize a field value for comparison. Lowercases strings,
    coerces None/missing to '', floats to fixed precision."""
    if v is None:
        return ''
    if isinstance(v, str):
        return v.strip().upper()
    if isinstance(v, float):
        return f'{v:.2f}'
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    return str(v).upper()


def diff_events(events_main, events_eft):
    """Compare two lists of events. Best-effort matching by date+event_name
    (since order may differ). Returns a list of per-event diff records."""
    # Build a key for each event for matching
    def key(ev):
        return (normalize_value(ev.get('date')), normalize_value(ev.get('event_name'))[:30])

    main_by_key = {key(e): e for e in events_main}
    eft_by_key = {key(e): e for e in events_eft}

    all_keys = set(main_by_key) | set(eft_by_key)
    diffs = []
    for k in sorted(all_keys, key=lambda kk: (kk[0] or '', kk[1] or '')):
        m_ev = main_by_key.get(k)
        e_ev = eft_by_key.get(k)
        rec = {
            'key': k,
            'in_main': m_ev is not None,
            'in_eft':  e_ev is not None,
            'fields':  {},
        }
        for f in COMPARED_FIELDS:
            mv = normalize_value(m_ev.get(f) if m_ev else None)
            ev = normalize_value(e_ev.get(f) if e_ev else None)
            rec['fields'][f] = {
                'main':  mv,
                'eft':   ev,
                'match': mv == ev,
            }
        rec['total_field_mismatches'] = sum(
            1 for fv in rec['fields'].values() if not fv['match']
        )
        diffs.append(rec)
    return diffs


# ────────────────────────────────────────────────────────────────
# Cache loading
# ────────────────────────────────────────────────────────────────

def load_cache_posts(cache_path: Path):
    """Load posts from an apify_cache JSON file. Handles both bare list
    and {posts: [...]} schemas."""
    with open(cache_path) as f:
        raw = json.load(f)
    if isinstance(raw, list):
        return raw
    return raw.get('posts', raw.get('items', []))


def pick_posts(args):
    """Return list of (post, dataset_path) tuples to compare against."""
    if args.post_ids:
        # Specific post IDs requested; need to scan all cache files to find them.
        wanted = set()
        with open(args.post_ids) as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                pid = (row.get('post_id') or row.get('id') or row.get('shortCode') or '').strip()
                if pid:
                    wanted.add(pid)
        if not wanted:
            print(f"❌ No post_ids found in {args.post_ids}", file=sys.stderr)
            sys.exit(1)
        found = []
        for cf in sorted(CACHE_DIR.glob('*.json')):
            try:
                posts = load_cache_posts(cf)
            except Exception:
                continue
            for post in posts:
                pid = post.get('shortCode') or post.get('id', '')
                if pid in wanted:
                    found.append((post, cf))
                    wanted.discard(pid)
                if not wanted:
                    break
            if not wanted:
                break
        if wanted:
            print(f"⚠ Could not find these post_ids in cache: {sorted(wanted)}")
        return found

    # Otherwise: take N most-recently-cached posts
    cache_files = sorted(CACHE_DIR.glob('*.json'),
                         key=lambda p: p.stat().st_mtime, reverse=True)
    if args.cache:
        cache_files = [Path(args.cache)]
    if not cache_files:
        print(f"❌ No cached datasets in {CACHE_DIR}", file=sys.stderr)
        sys.exit(1)

    for cf in cache_files:
        try:
            posts = load_cache_posts(cf)
        except Exception as e:
            print(f"⚠ Could not load {cf}: {e}")
            continue
        if posts:
            sample = [p for p in posts if (p.get('caption') or '').strip()][:args.n]
            if sample:
                return [(p, cf) for p in sample]
    print(f"❌ No cached posts with captions found", file=sys.stderr)
    sys.exit(1)


# ────────────────────────────────────────────────────────────────
# Driver
# ────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Diff main.py vs events_from_ids.py extraction outputs.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--n", type=int, default=5,
                   help="Number of posts to compare (default 5)")
    p.add_argument("--post-ids", type=str, default=None,
                   help="CSV file with a 'post_id' column listing specific posts")
    p.add_argument("--cache", type=str, default=None,
                   help="Specific apify_cache file (default: most-recently-modified)")
    p.add_argument("--model", type=str, default="gemini-2.0-flash-lite",
                   help="Gemini model (default: gemini-2.0-flash-lite, matches main.py)")
    args = p.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    print(f"━━━ compare_extractors.py ━━━")
    print(f"  Posts:    {args.n}")
    print(f"  Cache:    {args.cache or '(latest)'}")
    print(f"  Post IDs: {args.post_ids or '(none — sampling latest cache)'}")
    print(f"  Model:    {args.model}")
    print()

    # Gemini setup
    api_key = os.environ.get('GEMINI_API_KEY', '').strip()
    if not api_key:
        print(f"❌ GEMINI_API_KEY not set", file=sys.stderr)
        sys.exit(2)
    import google.generativeai as genai
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(args.model)

    # Pick posts
    selected = pick_posts(args)
    if not selected:
        print(f"❌ No posts to compare", file=sys.stderr)
        sys.exit(1)
    print(f"  Selected {len(selected)} post(s) for comparison")
    print()

    all_results = []
    field_mismatches_total = {f: 0 for f in COMPARED_FIELDS}
    n_posts_main_failed = 0
    n_posts_eft_failed  = 0

    for i, (post, src) in enumerate(selected, start=1):
        pid = post.get('shortCode') or post.get('id', f'?_{i}')
        timestamp = post.get('timestamp', '')
        try:
            post_date = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).date()
        except Exception:
            post_date = datetime.now().date()
        post_date_str = post_date.strftime('%Y-%m-%d')

        print(f"  [{i}/{len(selected)}] {pid}  ({post_date_str}, @{post.get('ownerUsername','?')})")

        # OCR text is hard to reproduce here without running Vision; for a
        # prompt-only comparison this is acceptable. Real extraction in
        # main.py / events_from_ids.py also has OCR — the diff captured
        # by this script is the prompt-attributable component only.
        ocr_text = ''

        t0 = time.time()
        prompt_main = build_prompt_main(post, ocr_text, post_date_str)
        events_main, err_main = call_gemini(model, prompt_main)
        t_main = time.time() - t0

        t0 = time.time()
        prompt_eft = build_prompt_eft(post, ocr_text, post_date)
        events_eft, err_eft = call_gemini(model, prompt_eft)
        t_eft = time.time() - t0

        if events_main is None:
            n_posts_main_failed += 1
            print(f"      main.py prompt FAILED: {err_main}")
            events_main = []
        if events_eft is None:
            n_posts_eft_failed += 1
            print(f"      events_from_ids.py prompt FAILED: {err_eft}")
            events_eft = []

        diffs = diff_events(events_main, events_eft)
        n_only_main = sum(1 for d in diffs if d['in_main'] and not d['in_eft'])
        n_only_eft  = sum(1 for d in diffs if d['in_eft'] and not d['in_main'])
        n_both      = sum(1 for d in diffs if d['in_eft'] and d['in_main'])
        n_field_mismatches = sum(d['total_field_mismatches'] for d in diffs if d['in_main'] and d['in_eft'])

        # Per-field accounting (only for events that exist in BOTH outputs)
        for d in diffs:
            if not (d['in_main'] and d['in_eft']):
                continue
            for f, fv in d['fields'].items():
                if not fv['match']:
                    field_mismatches_total[f] += 1

        print(f"      main: {len(events_main)} events ({t_main:.1f}s)  "
              f"eft: {len(events_eft)} events ({t_eft:.1f}s)")
        print(f"      both: {n_both}, main-only: {n_only_main}, eft-only: {n_only_eft}, "
              f"field mismatches: {n_field_mismatches}")

        all_results.append({
            'post_id':  pid,
            'post_date': post_date_str,
            'account':  post.get('ownerUsername', ''),
            'source':   str(src),
            'main_event_count': len(events_main),
            'eft_event_count':  len(events_eft),
            'main_only':  n_only_main,
            'eft_only':   n_only_eft,
            'both':       n_both,
            'field_mismatches': n_field_mismatches,
            'main_error': err_main,
            'eft_error':  err_eft,
            'diffs':      diffs,
        })

    # ─── Per-field totals ─────────────────────────────────────────
    print(f"\n━━━ FIELD-LEVEL MISMATCH TOTALS (events present in BOTH outputs) ━━━")
    for f in COMPARED_FIELDS:
        n = field_mismatches_total[f]
        marker = "  ← " if n > 0 else ""
        print(f"  {f:<28} {n:>4} mismatches{marker}")

    print(f"\n━━━ SUMMARY ━━━")
    total_main = sum(r['main_event_count'] for r in all_results)
    total_eft = sum(r['eft_event_count'] for r in all_results)
    print(f"  Events from main.py prompt:  {total_main}")
    print(f"  Events from eft     prompt:  {total_eft}")
    print(f"  Posts where main.py FAILED:  {n_posts_main_failed}")
    print(f"  Posts where eft     FAILED:  {n_posts_eft_failed}")
    print(f"  Total field mismatches:      {sum(field_mismatches_total.values())}")

    # ─── Output files ─────────────────────────────────────────────
    json_path = OUTPUT_DIR / f'compare_{ts}.json'
    with open(json_path, 'w') as f:
        json.dump({
            'timestamp':              ts,
            'model':                  args.model,
            'n_posts':                len(selected),
            'field_mismatches_total': field_mismatches_total,
            'results':                all_results,
        }, f, indent=2, default=str)
    print(f"\n  Detailed JSON: {json_path}")

    csv_path = OUTPUT_DIR / f'compare_{ts}.csv'
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['post_id', 'event_key', 'in_main', 'in_eft',
                    'field', 'main_value', 'eft_value', 'match'])
        for r in all_results:
            for d in r['diffs']:
                k = ' / '.join(d['key'])
                for fname, fv in d['fields'].items():
                    w.writerow([
                        r['post_id'], k, d['in_main'], d['in_eft'],
                        fname, fv['main'], fv['eft'], fv['match'],
                    ])
    print(f"  Side-by-side CSV: {csv_path}")

    # Exit code: 0 if no field mismatches (post-PR B expected state),
    # 1 if there are mismatches (pre-PR B baseline).
    total_mismatches = sum(field_mismatches_total.values())
    if total_mismatches == 0 and n_posts_main_failed == 0 and n_posts_eft_failed == 0:
        print(f"\n✓ Outputs match. PR B alignment achieved (or no real differences).")
        sys.exit(0)
    else:
        print(f"\n⚠ {total_mismatches} field mismatches detected — review CSV above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
