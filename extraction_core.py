"""
extraction_core.py — shared extraction logic for main.py and events_from_ids.py.

PURPOSE
───────
Both extraction tools historically had their OWN copies of OCR + Gemini call
logic, sanity checks, region lookup, prompt construction. Today's investigation
surfaced the cost of that duplication: events_from_ids.py grew a safety net
(region auto-fix, sanity flags, prompt audit, tier ladder) while main.py
kept producing rows without those features. Result: main.py's weekly cron
keeps creating new "bad" data that we then have to fix via events_from_ids.

This module consolidates the LOGIC pieces — sanity checks, lookups,
constants, helpers — into one place. The actual run-loop / pipeline-driver
code stays in each consumer (different jobs, different input sources).

STATUS
──────
FOUNDATION ONLY. This is PR 1 of an N-part refactor:

  ✓ PR 1 (this PR): create extraction_core with shared lookups, sanity
    checks, constants. Both tools can import from it; nothing breaks.

  ☐ PR 2 (future): migrate events_from_ids.py to import from
    extraction_core. Pure import-swap, no behavior change.

  ☐ PR 3 (future): migrate main.py to import from extraction_core.
    BIGGER lift — main.py has its own ways of doing things; needs
    careful step-by-step migration.

  ☐ PR 4 (future): main.py also gets the tier ladder + QUALITY_FLAGS
    + RECURRENCE_PATTERN column. Production cron starts producing
    safety-net'd data.

Until then, this module is a stub for future imports. Adding it on its
own does NOT change any current behavior.
"""

import re
import json
from datetime import datetime
from pathlib import Path
from typing import Optional


# ─────────────────────────────────────────────────────────────────
# Constants — single source of truth for both tools
# ─────────────────────────────────────────────────────────────────

# Gemini model identifiers
MODEL_FLASH_LITE = "gemini-2.5-flash-lite"
MODEL_PRO        = "gemini-2.5-pro"

# Rough per-call cost estimates (USD). Vendor pricing changes — these are
# back-of-envelope figures for run-summary visibility, not billing-grade
# accuracy. Update as pricing evolves.
COST_PER_CALL = {
    'vision':         0.0015,
    'flash_caption':  0.0005,
    'flash_text':     0.0005,
    'flash_image':    0.0010,
    'pro_image':      0.0080,
}

# Tier ladder (events_from_ids.py uses this; main.py future-uses this)
TIER_FLASH_CAPTION = "flash_caption"
TIER_FLASH_TEXT    = "flash_text"
TIER_FLASH_IMAGE   = "flash_image"
TIER_PRO_IMAGE     = "pro_image"
TIER_LADDER        = [TIER_FLASH_CAPTION, TIER_FLASH_IMAGE, TIER_FLASH_TEXT, TIER_PRO_IMAGE]

# Lookups (relative paths from project root)
NJ_MUNICIPALITIES_JSON     = "data/nj_municipalities.json"
VENUE_CITY_CANONICAL_JSON  = "data/venue_city_canonical.json"

# Sanity check thresholds
LOW_CONFIDENCE_THRESHOLD = 0.5
CAROUSEL_LOW_EVENTS_MIN_SLIDES = 3
CAROUSEL_LOW_EVENTS_MIN_TEXT_SLIDES = 3
OCR_RICH_LOW_EVENTS_MIN_CHARS = 1500
OCR_RICH_LOW_EVENTS_MIN_DATES = 2
PAST_DATE_DAYS = 7         # extracted date >7 days before post = suspect
FAR_FUTURE_DATE_DAYS = 365 # extracted date >1 year after post = suspect

# Calendar/range tokens
CALENDAR_KEYWORDS = ("calendar", "lineup", "schedule", "weekly", "monthly",
                     "weekend", "series", "every")

# Day-name → weekday number (Mon=0)
DAY_NAMES = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
    "mon": 0, "tue": 1, "tues": 1, "wed": 2, "thu": 3, "thur": 3, "thurs": 3,
    "fri": 4, "sat": 5, "sun": 6,
}

# Per-cell formatting: which sheet columns get highlighted for each flag.
# Post-level flags (CALENDAR/CAROUSEL/OCR_RICH/ACCOUNT/NO_GROUNDING) map to
# QUALITY_FLAGS — they're not about a specific field.
FLAG_TO_COLUMNS = {
    'DATE_DAY_MISMATCH':     ['DATE'],
    'MISSING_DATE':          ['DATE'],
    'PAST_DATE':             ['DATE'],
    'FAR_FUTURE_DATE':       ['DATE'],
    'VENUE_CITY_MISMATCH':   ['VENUE NAME', 'CITY'],
    'CITY_NOT_IN_NJ_LOOKUP': ['CITY', 'SECTION OF NJ'],
    'REGION_AUTOFIXED':      ['SECTION OF NJ'],
    'LOW_CONFIDENCE':        ['CONFIDENCE'],
    'CALENDAR_LOW_EVENTS':   ['QUALITY_FLAGS'],
    'CAROUSEL_LOW_EVENTS':   ['QUALITY_FLAGS'],
    'OCR_RICH_LOW_EVENTS':   ['QUALITY_FLAGS'],
    'ACCOUNT_PATTERN_DROP':  ['QUALITY_FLAGS'],
    'NO_GROUNDING':          ['QUALITY_FLAGS'],
}

# Flags that should escalate to the next tier (post-level "low events"
# flags + per-row sanity-failure flags). Auto-fix flags (REGION_AUTOFIXED,
# CITY_NOT_IN_NJ_LOOKUP, MISSING_DATE) are informational and don't escalate.
ESCALATION_FLAGS = {
    "DATE_DAY_MISMATCH", "VENUE_CITY_MISMATCH", "CALENDAR_LOW_EVENTS",
    "CAROUSEL_LOW_EVENTS", "OCR_RICH_LOW_EVENTS", "LOW_CONFIDENCE",
    "ACCOUNT_PATTERN_DROP",
}

# Conditional formatting: light yellow for flagged cells
FLAG_BG_COLOR = {"red": 1.0, "green": 0.97, "blue": 0.78}


# ─────────────────────────────────────────────────────────────────
# Lookup loaders
# ─────────────────────────────────────────────────────────────────

def load_nj_municipalities() -> dict:
    """Load city → region map. Returns empty dict if file missing."""
    p = Path(NJ_MUNICIPALITIES_JSON)
    if not p.exists():
        return {}
    return json.loads(p.read_text())


def load_venue_canonical() -> dict:
    """Load venue → canonical city map. Empty if not yet built."""
    p = Path(VENUE_CITY_CANONICAL_JSON)
    if not p.exists():
        return {}
    return json.loads(p.read_text())


# ─────────────────────────────────────────────────────────────────
# Helpers (used by multiple sanity checks)
# ─────────────────────────────────────────────────────────────────

def expand_day_ranges(text: str) -> set:
    """Detect day ranges in text and return the set of weekday numbers
    they cover. Without this, 'Mon-Fri' would be parsed as just
    {Mon, Fri} instead of {Mon, Tue, Wed, Thu, Fri}."""
    days = set()
    src = (text or '').lower()

    # Hyphen patterns: "mon-fri", "wednesday - saturday", "mon — sat"
    for m in re.finditer(r'\b([a-z]+)\s*[-–—]\s*([a-z]+)\b', src):
        s = DAY_NAMES.get(m.group(1))
        e = DAY_NAMES.get(m.group(2))
        if s is not None and e is not None:
            i = s
            for _ in range(7):
                days.add(i)
                if i == e:
                    break
                i = (i + 1) % 7

    # "X to Y" / "X through Y" / "X thru Y"
    for m in re.finditer(r'\b([a-z]+)\s+(?:to|through|thru)\s+([a-z]+)\b', src):
        s = DAY_NAMES.get(m.group(1))
        e = DAY_NAMES.get(m.group(2))
        if s is not None and e is not None:
            i = s
            for _ in range(7):
                days.add(i)
                if i == e:
                    break
                i = (i + 1) % 7

    if re.search(r'\bweekend\b', src):
        days.update({5, 6})
    if re.search(r'\bweek(?:day|night)s?\b', src):
        days.update({0, 1, 2, 3, 4})
    if re.search(r'\bdaily\b', src):
        days.update({0, 1, 2, 3, 4, 5, 6})

    return days


def count_distinct_dates(text: str) -> int:
    """Count distinct date-shaped tokens in text. Used to distinguish real
    calendar posts (multiple dates) from single-event posts that happen to
    mention 'schedule'/'every'/etc."""
    if not text:
        return 0
    found = set()
    # M/D, MM/DD, M/D/YY, MM/DD/YYYY
    for m in re.finditer(r'\b(\d{1,2})/(\d{1,2})(?:/\d{2,4})?\b', text):
        found.add((m.group(1).zfill(2), m.group(2).zfill(2)))
    # M.D with strict bounds (avoid catching "$5.99" decimals)
    for m in re.finditer(r'\b(0?[1-9]|1[0-2])\.(0?[1-9]|[12]\d|3[01])\b', text):
        found.add((m.group(1).zfill(2), m.group(2).zfill(2)))
    # Month name + day: "May 8", "MAY 8TH", "Jun 12"
    months = r'(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)'
    for m in re.finditer(rf'\b{months}\w*\s+(\d{{1,2}})(?:st|nd|rd|th)?\b', text, re.IGNORECASE):
        found.add(("month", m.group(1)))
    return len(found)


# ─────────────────────────────────────────────────────────────────
# Sanity checks
# ─────────────────────────────────────────────────────────────────
# Each returns the flag string (e.g. "DATE_DAY_MISMATCH") when it fires,
# or None. Designed to be composable — consumers stitch them together
# in whatever order makes sense for their pipeline.

def check_date_day_match(event: dict, source_text: str) -> Optional[str]:
    """Flag if extracted date doesn't fall on a day that the source text
    indicates. Handles day ranges (mon-fri, weekend, daily) so it doesn't
    fire false positives on multi-day recurring deals."""
    date_str = event.get('date', '')
    if not date_str or not source_text:
        return None
    try:
        d = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
    except ValueError:
        return None
    actual_day = d.weekday()

    src_lower = (source_text or '').lower()
    mentioned_days = set()
    for name, num in DAY_NAMES.items():
        if re.search(rf'\b{name}\b', src_lower):
            mentioned_days.add(num)
    mentioned_days.update(expand_day_ranges(src_lower))

    if not mentioned_days:
        return None
    if actual_day not in mentioned_days:
        return "DATE_DAY_MISMATCH"
    return None


def check_venue_city(event: dict, venue_lookup: dict) -> Optional[str]:
    """Fire if extracted venue's canonical city in the lookup differs
    from the extracted city. Skips MULTI_LOCATION and NOT_A_VENUE entries."""
    venue = (event.get('venue_name') or '').strip().upper()
    city = (event.get('city') or '').strip().upper()
    if not venue or not city:
        return None
    canonical = venue_lookup.get(venue)
    if not canonical:
        return None
    if canonical in ("MULTI_LOCATION", "NOT_A_VENUE"):
        return None
    if canonical.upper() != city:
        return "VENUE_CITY_MISMATCH"
    return None


def check_region_against_lookup(event: dict, region_lookup: dict, stats: dict = None) -> Optional[str]:
    """Auto-fix region using canonical lookup. Mutates event in-place.
    Returns flag indicating what happened (or None if nothing). Updates
    stats['region_lookup'] counters when stats is provided."""
    rl = stats['region_lookup'] if stats else None
    city = (event.get('city') or '').strip().upper()
    if not city:
        return None
    canonical = region_lookup.get(city)
    if canonical is None:
        if rl is not None:
            rl['unknown_city'] = rl.get('unknown_city', 0) + 1
        return "CITY_NOT_IN_NJ_LOOKUP"
    if canonical == "NON_NJ":
        if event.get('section_of_nj'):
            event['section_of_nj'] = ""
        if rl is not None:
            rl['non_nj_cleared'] = rl.get('non_nj_cleared', 0) + 1
        return "CITY_NOT_IN_NJ_LOOKUP"
    if rl is not None:
        rl['hits'] = rl.get('hits', 0) + 1
    current = (event.get('section_of_nj') or '').strip().upper()
    if current != canonical:
        event['section_of_nj'] = canonical
        if rl is not None:
            rl['autofixed'] = rl.get('autofixed', 0) + 1
        return "REGION_AUTOFIXED"
    if rl is not None:
        rl['already_correct'] = rl.get('already_correct', 0) + 1
    return None


def check_low_confidence(event: dict) -> Optional[str]:
    try:
        conf = float(event.get('confidence', 1.0))
    except (TypeError, ValueError):
        return None
    return "LOW_CONFIDENCE" if conf < LOW_CONFIDENCE_THRESHOLD else None


def check_missing_date(event: dict) -> Optional[str]:
    """Fire when an event was extracted without a date. Date is essential
    for an event row to be actionable — flagging surfaces these for human
    review without preventing the row from being saved."""
    date = event.get('date')
    if date is None:
        return "MISSING_DATE"
    s = str(date).strip().lower()
    if s == '' or s in ('none', 'null', 'tbd', 'tba'):
        return "MISSING_DATE"
    return None


def check_date_sanity(event: dict, post_date: datetime) -> Optional[str]:
    """Fire when extracted date is implausibly far from POST DATE.
    PAST_DATE: extracted date >7 days BEFORE post date.
    FAR_FUTURE_DATE: extracted date >365 days AFTER post date."""
    date_str = event.get('date')
    if not date_str:
        return None
    try:
        d = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
    except (ValueError, TypeError):
        return None
    delta = (d - post_date).days
    if delta < -PAST_DATE_DAYS:
        return "PAST_DATE"
    if delta > FAR_FUTURE_DATE_DAYS:
        return "FAR_FUTURE_DATE"
    return None


def check_calendar_low_events(caption: str, ocr_text: str, num_events: int) -> Optional[str]:
    """Fire only when (caption keyword OR OCR keyword) AND ≥2 distinct
    date tokens appear across caption+OCR. Single-event posts that mention
    'schedule' once shouldn't trip this — they need multi-date evidence."""
    if num_events > 1:
        return None
    combined = ((caption or '') + ' ' + (ocr_text or '')).lower()
    if not any(kw in combined for kw in CALENDAR_KEYWORDS):
        return None
    if count_distinct_dates(combined) < OCR_RICH_LOW_EVENTS_MIN_DATES:
        return None
    return "CALENDAR_LOW_EVENTS"


def check_carousel_low_events(slide_count: int, slides_with_text: int, num_events: int) -> Optional[str]:
    if (slide_count >= CAROUSEL_LOW_EVENTS_MIN_SLIDES
            and slides_with_text >= CAROUSEL_LOW_EVENTS_MIN_TEXT_SLIDES
            and num_events <= 1):
        return "CAROUSEL_LOW_EVENTS"
    return None


def check_ocr_rich_low_events(ocr_text: str, num_events: int) -> Optional[str]:
    if num_events > 1:
        return None
    if not ocr_text or len(ocr_text) < OCR_RICH_LOW_EVENTS_MIN_CHARS:
        return None
    if count_distinct_dates(ocr_text) < OCR_RICH_LOW_EVENTS_MIN_DATES:
        return None
    return "OCR_RICH_LOW_EVENTS"


def check_account_pattern(account: str, num_events: int, account_avg: dict) -> Optional[str]:
    avg = account_avg.get(account)
    if avg is not None and avg >= 3 and num_events <= 1:
        return "ACCOUNT_PATTERN_DROP"
    return None


def check_no_grounding(caption: str, ocr_text: str, num_events: int) -> Optional[str]:
    """Fire when events were extracted but neither caption nor OCR text
    contained meaningful content. Catches Gemini hallucinating from
    metadata-only context (account name + Instagram location tag)."""
    if num_events == 0:
        return None
    if (caption or '').strip():
        return None
    if (ocr_text or '').strip():
        return None
    return "NO_GROUNDING"


# ─────────────────────────────────────────────────────────────────
# Estimated cost helper
# ─────────────────────────────────────────────────────────────────

def estimate_cost(api_call_counts: dict) -> float:
    """Rough USD estimate from per-call counts. See COST_PER_CALL caveat."""
    return sum(api_call_counts.get(k, 0) * COST_PER_CALL.get(k, 0)
               for k in COST_PER_CALL)
