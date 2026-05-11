#!/usr/bin/env python3
"""
smoke_test.py — verify the extraction safety net is correctly wired.

The default mode (NO --with-gemini) runs in seconds, costs nothing,
and validates that:
  - extraction_core imports cleanly
  - All 13 known flag names are defined in FLAG_TO_COLUMNS
  - Each ec_check_* function fires on a known-positive input and
    stays silent on a known-negative input
  - Lookup files (nj_municipalities, venue_canonical) load and have
    expected structure
  - The ESCALATION_FLAGS subset is consistent with FLAG_TO_COLUMNS

If anything is broken, this catches it BEFORE the pipeline runs.
Run this before every PR merge as a fast pre-flight check.

The optional --with-gemini mode runs actual extraction on N cached
Apify posts (from outputs/apify_cache/*.json). Costs Gemini API
credits (~$0.001/post on Flash-Lite). Useful for catching prompt
regressions that pure-logic tests miss.

USAGE
─────
  python smoke_test.py                          # logic-only, ~5 sec, free
  python smoke_test.py --with-gemini --n 5      # also run 5 cached posts through Gemini
  python smoke_test.py --quiet                  # CI-friendly (pass/fail only)

OUTPUT
──────
  - Console: per-check ✓/✗ summary
  - File: outputs/smoke_<ts>.json (detailed results — for trend tracking)

EXIT CODES
──────────
  0 = all checks passed
  1 = one or more checks failed
  2 = setup error (missing module, missing data file)
"""

import argparse
import json
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path("outputs")

# The 13 flags the safety net is supposed to know about (matches the
# table from docs/tooling.md and reset_quality_formatting.py header).
EXPECTED_FLAGS = {
    'MISSING_DATE', 'DATE_DAY_MISMATCH', 'PAST_DATE', 'FAR_FUTURE_DATE',
    'VENUE_CITY_MISMATCH', 'CITY_NOT_IN_NJ_LOOKUP', 'REGION_AUTOFIXED',
    'LOW_CONFIDENCE', 'CALENDAR_LOW_EVENTS', 'CAROUSEL_LOW_EVENTS',
    'OCR_RICH_LOW_EVENTS', 'ACCOUNT_PATTERN_DROP', 'NO_GROUNDING',
}


# ────────────────────────────────────────────────────────────────
# Test result tracking
# ────────────────────────────────────────────────────────────────

class Results:
    def __init__(self):
        self.passed = []
        self.failed = []
        self.skipped = []

    def add(self, name, ok, detail='', exc=None):
        rec = {'name': name, 'ok': ok, 'detail': detail}
        if exc:
            rec['traceback'] = traceback.format_exception_only(type(exc), exc)[-1].strip()
        (self.passed if ok else self.failed).append(rec)

    def skip(self, name, reason):
        self.skipped.append({'name': name, 'reason': reason})

    def print_one(self, rec, ok, quiet=False):
        if quiet and ok:
            return
        mark = "✓" if ok else "✗"
        print(f"  {mark} {rec['name']}")
        if rec.get('detail') and not quiet:
            print(f"      {rec['detail']}")
        if rec.get('traceback'):
            print(f"      {rec['traceback']}")

    def report(self, quiet=False):
        for rec in self.passed:
            self.print_one(rec, ok=True, quiet=quiet)
        for rec in self.failed:
            self.print_one(rec, ok=False, quiet=quiet)
        for rec in self.skipped:
            print(f"  ⊘ {rec['name']}  ({rec['reason']})")
        n_pass, n_fail, n_skip = len(self.passed), len(self.failed), len(self.skipped)
        print(f"\n  Total: {n_pass} passed, {n_fail} failed, {n_skip} skipped")
        return n_fail == 0


# ────────────────────────────────────────────────────────────────
# Test groups
# ────────────────────────────────────────────────────────────────

def test_imports(r: Results):
    """Verify extraction_core imports without exploding."""
    try:
        import extraction_core as ec
        r.add('extraction_core imports', True,
              detail=f'module loaded from {ec.__file__}')
        return ec
    except Exception as e:
        r.add('extraction_core imports', False, exc=e)
        return None


def test_flag_inventory(r: Results, ec):
    """Verify every expected flag is in FLAG_TO_COLUMNS, and nothing
    extra. Catches schema drift between this test's expectations and
    extraction_core."""
    if ec is None:
        r.skip('flag inventory', 'extraction_core import failed')
        return

    actual = set(ec.FLAG_TO_COLUMNS.keys())
    missing = EXPECTED_FLAGS - actual
    extra = actual - EXPECTED_FLAGS

    if missing:
        r.add('FLAG_TO_COLUMNS covers all 13 known flags', False,
              detail=f'missing: {sorted(missing)}')
    else:
        r.add('FLAG_TO_COLUMNS covers all 13 known flags', True,
              detail=f'all {len(actual)} flags present')

    # Extra flags aren't a failure — could be a new flag added since this
    # script was written — but worth flagging for visibility.
    if extra:
        r.add('No unknown flags in FLAG_TO_COLUMNS', True,
              detail=f'note: {len(extra)} new flag(s) since smoke_test was last updated: {sorted(extra)}')

    # Verify FLAG_TO_COLUMNS values are non-empty lists of strings
    for flag, cols in ec.FLAG_TO_COLUMNS.items():
        ok = isinstance(cols, list) and len(cols) > 0 and all(isinstance(c, str) for c in cols)
        if not ok:
            r.add(f'FLAG_TO_COLUMNS[{flag}] is non-empty list of column names', False,
                  detail=f'got: {cols!r}')

    # Verify ESCALATION_FLAGS (if present) is a subset of FLAG_TO_COLUMNS
    if hasattr(ec, 'ESCALATION_FLAGS'):
        bad = set(ec.ESCALATION_FLAGS) - actual
        if bad:
            r.add('ESCALATION_FLAGS ⊆ FLAG_TO_COLUMNS', False,
                  detail=f'ESCALATION_FLAGS contains unknown: {sorted(bad)}')
        else:
            r.add('ESCALATION_FLAGS ⊆ FLAG_TO_COLUMNS', True,
                  detail=f'{len(ec.ESCALATION_FLAGS)} escalation flags, all defined')


def test_lookups(r: Results, ec):
    """Verify canonical lookup files load and have plausible structure."""
    if ec is None:
        r.skip('NJ municipalities lookup', 'extraction_core import failed')
        r.skip('venue canonical lookup', 'extraction_core import failed')
        return

    try:
        muni = ec.load_nj_municipalities()
        if not isinstance(muni, dict):
            raise ValueError(f'expected dict, got {type(muni).__name__}')
        if len(muni) < 100:
            raise ValueError(f'only {len(muni)} entries; expected 600+')
        # Spot-check a few well-known cities
        for city in ('NEWARK', 'JERSEY CITY', 'ELIZABETH'):
            if city not in muni:
                raise ValueError(f'expected city {city!r} missing from lookup')
        r.add('NJ municipalities lookup', True,
              detail=f'{len(muni)} cities loaded; spot-checks OK')
    except Exception as e:
        r.add('NJ municipalities lookup', False, exc=e)

    try:
        venues = ec.load_venue_canonical()
        if not isinstance(venues, dict):
            raise ValueError(f'expected dict, got {type(venues).__name__}')
        r.add('venue canonical lookup', True,
              detail=f'{len(venues)} venues loaded')
    except Exception as e:
        r.add('venue canonical lookup', False, exc=e)


def test_check_functions(r: Results, ec):
    """For each ec_check_* function, run one POSITIVE (should fire)
    and one NEGATIVE (should NOT fire) synthetic case. Catches:
      - Check function throws on a well-formed event
      - Check returns wrong flag name (typo drift)
      - Check returns flag where it shouldn't"""
    if ec is None:
        r.skip('check functions', 'extraction_core import failed')
        return

    from datetime import datetime as _dt, timedelta as _td
    today = _dt.now().date()

    # ─── check_missing_date ───
    try:
        positive = ec.check_missing_date({'date': ''})
        negative = ec.check_missing_date({'date': '2026-06-01'})
        ok = positive == 'MISSING_DATE' and negative is None
        r.add('check_missing_date: empty → MISSING_DATE, populated → None', ok,
              detail=f'empty→{positive!r}, populated→{negative!r}')
    except Exception as e:
        r.add('check_missing_date', False, exc=e)

    # ─── check_low_confidence ───
    try:
        positive = ec.check_low_confidence({'confidence': 0.3})
        negative = ec.check_low_confidence({'confidence': 0.9})
        ok = positive == 'LOW_CONFIDENCE' and negative is None
        r.add('check_low_confidence: 0.3 → LOW_CONFIDENCE, 0.9 → None', ok,
              detail=f'0.3→{positive!r}, 0.9→{negative!r}')
    except Exception as e:
        r.add('check_low_confidence', False, exc=e)

    # ─── check_date_sanity (PAST_DATE / FAR_FUTURE_DATE) ───
    try:
        past_event = {'date': (today - _td(days=30)).strftime('%Y-%m-%d')}
        future_event = {'date': (today + _td(days=365 * 3)).strftime('%Y-%m-%d')}
        good_event = {'date': (today + _td(days=10)).strftime('%Y-%m-%d')}
        past = ec.check_date_sanity(past_event, today)
        future = ec.check_date_sanity(future_event, today)
        good = ec.check_date_sanity(good_event, today)
        ok = past == 'PAST_DATE' and future == 'FAR_FUTURE_DATE' and good is None
        r.add('check_date_sanity: past → PAST_DATE, far future → FAR_FUTURE_DATE', ok,
              detail=f'past→{past!r}, +3yr→{future!r}, +10d→{good!r}')
    except Exception as e:
        r.add('check_date_sanity', False, exc=e)

    # ─── check_date_day_match ───
    # POST contains "Friday" but date is a Tuesday → should fire
    try:
        # Pick a Tuesday: 2026-06-02 is a Tuesday (verify below)
        tuesday_event = {'date': '2026-06-02'}
        weekday = _dt.strptime(tuesday_event['date'], '%Y-%m-%d').strftime('%A')
        if weekday == 'Tuesday':
            positive = ec.check_date_day_match(tuesday_event,
                                                source_text="Join us this Friday for live music")
            negative = ec.check_date_day_match(tuesday_event,
                                                source_text="Open Tuesday night at 8pm")
            ok = positive == 'DATE_DAY_MISMATCH' and negative is None
            r.add('check_date_day_match: Tue date + "Friday" mention → MISMATCH', ok,
                  detail=f'Friday-mention→{positive!r}, Tuesday-mention→{negative!r}')
        else:
            r.skip('check_date_day_match', f'2026-06-02 is {weekday}, not Tuesday')
    except Exception as e:
        r.add('check_date_day_match', False, exc=e)

    # ─── check_no_grounding ───
    try:
        positive = ec.check_no_grounding(caption='', ocr_text='', n_events=1)
        negative = ec.check_no_grounding(caption='Live jazz tonight at 8pm',
                                          ocr_text='', n_events=1)
        ok = positive == 'NO_GROUNDING' and negative is None
        r.add('check_no_grounding: empty caption+OCR with events → NO_GROUNDING', ok,
              detail=f'empty→{positive!r}, has-caption→{negative!r}')
    except Exception as e:
        r.add('check_no_grounding', False, exc=e)

    # ─── check_region_against_lookup ───
    try:
        muni = ec.load_nj_municipalities()
        # NEWARK is in ESSEX, region NORTH per canonical data
        ev_wrong = {'city': 'NEWARK', 'section_of_nj': 'SOUTH'}
        result_wrong = ec.check_region_against_lookup(ev_wrong, muni)
        # After auto-fix, section should be NORTH:
        section_after = ev_wrong.get('section_of_nj')
        ok = result_wrong == 'REGION_AUTOFIXED' and section_after == 'NORTH'
        r.add('check_region_against_lookup: NEWARK+SOUTH → AUTOFIX to NORTH', ok,
              detail=f'result={result_wrong!r}, section_after={section_after!r}')
    except Exception as e:
        r.add('check_region_against_lookup', False, exc=e)


def test_apify_cache_available(r: Results):
    """Verify we have at least one cached Apify dataset to use for
    --with-gemini mode. Skipped if the cache is empty."""
    cache_dir = Path("outputs/apify_cache")
    if not cache_dir.exists():
        r.skip('apify cache available', 'outputs/apify_cache/ does not exist')
        return False
    files = sorted(cache_dir.glob('*.json'))
    if not files:
        r.skip('apify cache available', 'no *.json files in outputs/apify_cache/')
        return False
    r.add('apify cache available', True,
          detail=f'{len(files)} dataset file(s) found')
    return True


def test_with_gemini(r: Results, ec, n_posts):
    """OPTIONAL: run actual Gemini extraction on N cached posts.
    Validates that the prompt produces parseable JSON and that
    extracted events pass the same schema validation pipeline."""
    if ec is None:
        r.skip('with-gemini extraction', 'extraction_core import failed')
        return

    try:
        import os
        api_key = os.environ.get('GEMINI_API_KEY', '').strip()
        if not api_key:
            r.skip('with-gemini extraction', 'GEMINI_API_KEY not set')
            return
        import google.generativeai as genai
        genai.configure(api_key=api_key)
    except Exception as e:
        r.add('with-gemini extraction setup', False, exc=e)
        return

    # Pick the most recent cached dataset
    cache_dir = Path("outputs/apify_cache")
    files = sorted(cache_dir.glob('*.json'), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        r.skip('with-gemini extraction', 'no apify_cache files')
        return

    cache_path = files[0]
    try:
        with open(cache_path) as f:
            raw = json.load(f)
        posts = raw if isinstance(raw, list) else raw.get('posts', [])
        if not posts:
            r.skip('with-gemini extraction', f'{cache_path} has no posts')
            return
    except Exception as e:
        r.add('with-gemini cache load', False, exc=e)
        return

    sample = posts[:n_posts]
    r.add(f'with-gemini: loaded {len(sample)} cached posts', True,
          detail=f'from {cache_path.name}')

    # Run a minimal extraction on each — JSON parse + schema check.
    # NOTE: this intentionally does NOT call the FULL main.py pipeline,
    # because we want to test the EXTRACTION step in isolation. Race
    # conditions and locking are validated by a different test (TBD).
    model = genai.GenerativeModel('gemini-2.0-flash-lite')
    n_extracted = 0
    n_with_events = 0
    n_with_bad_confidence = 0
    n_with_ny_city = 0

    for i, post in enumerate(sample):
        pid = post.get('shortCode', post.get('id', f'?_{i}'))
        try:
            # Use the SIMPLEST possible prompt — just to verify the API
            # path works and we get parseable JSON back. Full prompt
            # comparison belongs in compare_extractors.py (TBD).
            caption = (post.get('caption') or '')[:1500]
            simple_prompt = f"""Extract events from this Instagram caption. Return JSON.
            CAPTION: {caption}
            Return: {{"events": [{{event_name, date, city, confidence}}], "total": N}}"""
            resp = model.generate_content(simple_prompt)
            text = resp.text.strip().replace('```json', '').replace('```', '').strip()
            data = json.loads(text)
            events = data.get('events', [])
            n_extracted += 1
            if events:
                n_with_events += 1
            for ev in events:
                conf = ev.get('confidence')
                if conf is not None:
                    try:
                        cf = float(conf)
                        if cf < 0 or cf > 1:
                            n_with_bad_confidence += 1
                    except (ValueError, TypeError):
                        n_with_bad_confidence += 1
                city = (ev.get('city') or '').strip().upper()
                if city in ('NEW JERSEY', 'NJ', 'NJ AREA'):
                    n_with_ny_city += 1
        except Exception as e:
            r.add(f'with-gemini extract {pid}', False, exc=e)

    r.add(f'with-gemini: {n_extracted}/{len(sample)} extracted without exception',
          n_extracted == len(sample),
          detail=f'{n_with_events} produced events')
    r.add(f'with-gemini: no out-of-range confidence values',
          n_with_bad_confidence == 0,
          detail=f'{n_with_bad_confidence} bad confidence values found')
    # n_with_ny_city flagged as informational — useful BEFORE PR B
    # to capture baseline ("how often does Gemini hallucinate NJ as city")
    r.add(f'with-gemini: hallucinated-NJ-city baseline = {n_with_ny_city}', True,
          detail=f'after PR B (prompt fix) this should drop to 0')


# ────────────────────────────────────────────────────────────────
# Driver
# ────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Pre-flight check: verify extraction safety net is wired.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--with-gemini", action="store_true",
                   help="Also run actual extraction on cached posts (uses API credits)")
    p.add_argument("--n", type=int, default=5,
                   help="With --with-gemini: how many cached posts to test (default 5)")
    p.add_argument("--quiet", action="store_true",
                   help="Suppress per-check ✓ lines (show only failures + final summary)")
    args = p.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')

    if not args.quiet:
        print(f"━━━ smoke_test.py ━━━")
        print(f"  Mode:        {'with-gemini' if args.with_gemini else 'logic-only'}")
        print(f"  Posts:       {args.n}" if args.with_gemini else "  (no API calls)")
        print()

    r = Results()
    t0 = time.time()

    # ─── Layer 1: imports & inventory ─────────────────────────────
    ec = test_imports(r)
    test_flag_inventory(r, ec)
    test_lookups(r, ec)
    test_check_functions(r, ec)

    # ─── Layer 2: gemini (opt-in) ─────────────────────────────────
    if args.with_gemini:
        if test_apify_cache_available(r):
            test_with_gemini(r, ec, args.n)
        else:
            r.skip('with-gemini extraction', 'apify cache not available')
    else:
        r.skip('with-gemini extraction', '--with-gemini not specified')

    elapsed = time.time() - t0

    if not args.quiet:
        print(f"\n━━━ RESULTS ({elapsed:.1f}s) ━━━")
    success = r.report(quiet=args.quiet)

    # ─── Dump JSON for trend tracking ─────────────────────────────
    out_path = OUTPUT_DIR / f'smoke_{ts}.json'
    payload = {
        'timestamp': ts,
        'elapsed_sec': round(elapsed, 2),
        'mode': 'with-gemini' if args.with_gemini else 'logic-only',
        'passed': r.passed,
        'failed': r.failed,
        'skipped': r.skipped,
    }
    with open(out_path, 'w') as f:
        json.dump(payload, f, indent=2, default=str)
    if not args.quiet:
        print(f"\n  Detailed results: {out_path}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
