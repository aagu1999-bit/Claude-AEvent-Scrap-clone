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
    # Post-level flags: only the QUALITY FLAGS cell itself is highlighted,
    # not specific data fields (since these concerns are about the post
    # overall). The column key MUST be 'QUALITY FLAGS' (with a space) — the
    # consumer's column-letter map is built from df.columns AFTER the
    # underscore-to-space replacement in main.py's save_data, so the
    # underscore form 'QUALITY_FLAGS' silently failed to look up a letter
    # and the cell never got highlighted. Round 1 bug, found 2026-06-04.
    'CALENDAR_LOW_EVENTS':   ['QUALITY FLAGS'],
    'CAROUSEL_LOW_EVENTS':   ['QUALITY FLAGS'],
    'OCR_RICH_LOW_EVENTS':   ['QUALITY FLAGS'],
    'ACCOUNT_PATTERN_DROP':  ['QUALITY FLAGS'],
    # NO_GROUNDING fires when the source post had no caption + no OCR text,
    # yet events were extracted. The DESCRIPTION field is the most suspect —
    # it's the field Gemini fabricates most aggressively when there's nothing
    # to ground on (vs. structural fields like date/venue which need explicit
    # source mentions). Highlighting DESCRIPTION points the reviewer at the
    # field that's most likely hallucinated.
    'NO_GROUNDING':          ['DESCRIPTION'],
    # ── Round 2 (2026-06) "wrong field" flags — PINK data-cell highlight.
    # Also styled bold + pink inside the QUALITY FLAGS cell via the
    # textFormatRuns path (see FLAG_TO_TEXT_COLOR below + main.py's
    # _apply_quality_flag_text_runs). Pink keeps the data-cell highlight
    # because the whole point of these flags is "this specific field is
    # wrong" — coloring the field cell tells you which one to look at.
    'VENUE_IS_HANDLE':         ['VENUE NAME'],
    'VENUE_EQUALS_EVENT_NAME': ['VENUE NAME', 'EVENT NAME'],
    'VENUE_EQUALS_ACCOUNT':    ['VENUE NAME'],
    'VENUE_TRUNCATED':         ['VENUE NAME'],
    'VENUE_OUT_OF_NJ_REGION':  ['CITY', 'SECTION OF NJ'],
    # ── Round 2 "probably not an event" flags — ORANGE rich-text only.
    # These intentionally do NOT have data-cell entries. Per the user's
    # 2026-06-04 review: orange-tier flags should style the flag *token*
    # inside the QUALITY FLAGS cell (bold + orange text) rather than
    # painting an unrelated data cell (e.g., EVENT_NAME_GENERIC used to
    # paint the EVENT NAME cell orange, which was confusing — the issue
    # is the overall post quality, not the event-name field per se).
    # Wiring lives in FLAG_TO_TEXT_COLOR below.
    #
    # 'PERSONAL_POST_SIGNALS': (rich text only)
    # 'NO_EVENT_SIGNALS':      (rich text only)
    # 'EVENT_NAME_GENERIC':    (rich text only)
    # 'CAPTION_ONLY_NO_OCR':   (rich text only)

    # ── Round 2 "missing field" flags — yellow (default) ──
    'MISSING_VENUE':           ['VENUE NAME'],
    'MISSING_CITY':            ['CITY'],
    'MISSING_TIME':            ['START TIME'],
}

# Flags that should escalate to the next tier (post-level "low events"
# flags + per-row sanity-failure flags). Auto-fix flags (REGION_AUTOFIXED,
# CITY_NOT_IN_NJ_LOOKUP, MISSING_DATE) are informational and don't escalate.
ESCALATION_FLAGS = {
    "DATE_DAY_MISMATCH", "VENUE_CITY_MISMATCH", "CALENDAR_LOW_EVENTS",
    "CAROUSEL_LOW_EVENTS", "OCR_RICH_LOW_EVENTS", "LOW_CONFIDENCE",
    "ACCOUNT_PATTERN_DROP",
}

# Conditional formatting: legacy + round 2 colors. FLAG_BG_COLOR (yellow)
# stays as the default; callers that already look it up by name continue
# to work. Round 2 introduces two new categories — see FLAG_TO_COLOR for
# per-flag dispatch.
FLAG_BG_COLOR        = {"red": 1.0,  "green": 0.97, "blue": 0.78}  # YELLOW  — legacy / missing
FLAG_BG_COLOR_PINK   = {"red": 0.99, "green": 0.83, "blue": 0.87}  # PINK    — wrong field
FLAG_BG_COLOR_ORANGE = {"red": 1.0,  "green": 0.88, "blue": 0.72}  # ORANGE  — probably not event

# Per-flag DATA-CELL color dispatch. Flags not listed fall back to
# FLAG_BG_COLOR (yellow). "Wrong field" flags get pink — they signal
# the model named a specific field incorrectly, so the data cell
# (VENUE NAME, CITY, etc.) gets the highlight.
#
# Orange-tier "probably not event" flags are NOT in this dict — they
# style the flag *token* inside the QUALITY FLAGS cell via the rich-text
# path (FLAG_TO_TEXT_COLOR + _apply_quality_flag_text_runs in main.py).
# Per the user's 2026-06-04 review: highlighting EVENT NAME orange when
# EVENT_NAME_GENERIC fired was confusing — the issue is the overall
# post quality, not a specific field being wrong.
FLAG_TO_COLOR = {
    # Pink: wrong-field signals — paints the offending data cell pink
    'VENUE_IS_HANDLE':         FLAG_BG_COLOR_PINK,
    'VENUE_EQUALS_EVENT_NAME': FLAG_BG_COLOR_PINK,
    'VENUE_EQUALS_ACCOUNT':    FLAG_BG_COLOR_PINK,
    'VENUE_TRUNCATED':         FLAG_BG_COLOR_PINK,
    'VENUE_OUT_OF_NJ_REGION':  FLAG_BG_COLOR_PINK,
}

# ─────────────────────────────────────────────────────────────────
# Rich-text styling for flag tokens inside the QUALITY FLAGS cell.
# ─────────────────────────────────────────────────────────────────
# When a flag in this dict appears in the QUALITY FLAGS comma-separated
# string, the *token* (the literal flag name characters) renders bold +
# colored — pink for "wrong field" flags, orange for "probably not
# event" flags. Yellow-tier flags (legacy + missing) are NOT styled —
# they render in default unbolded text.
#
# This is independent of FLAG_TO_COLOR (which controls data-cell
# background). A row with VENUE_EQUALS_EVENT_NAME gets BOTH:
#   · VENUE NAME + EVENT NAME cell backgrounds painted pink
#     (via FLAG_TO_COLOR + FLAG_TO_COLUMNS)
#   · the "VENUE_EQUALS_EVENT_NAME" token inside QUALITY FLAGS rendered
#     bold + pink (via FLAG_TO_TEXT_COLOR)
# A row with PERSONAL_POST_SIGNALS gets ONLY:
#   · the token rendered bold + orange in QUALITY FLAGS
#   · no data-cell background highlight (orange-tier flags don't
#     attribute to a specific field)
#
# Per-character formatting in Sheets cells is the textFormatRuns feature
# of spreadsheets.values.batchUpdateByDataFilter / spreadsheets.batchUpdate
# updateCells request. main.py builds the runs and applies them per row.
FLAG_TO_TEXT_COLOR = {
    # Pink-tier — bold + pink token text
    'VENUE_IS_HANDLE':         FLAG_BG_COLOR_PINK,
    'VENUE_EQUALS_EVENT_NAME': FLAG_BG_COLOR_PINK,
    'VENUE_EQUALS_ACCOUNT':    FLAG_BG_COLOR_PINK,
    'VENUE_TRUNCATED':         FLAG_BG_COLOR_PINK,
    'VENUE_OUT_OF_NJ_REGION':  FLAG_BG_COLOR_PINK,
    # Orange-tier — bold + orange token text (no data-cell highlight)
    'PERSONAL_POST_SIGNALS':   FLAG_BG_COLOR_ORANGE,
    'NO_EVENT_SIGNALS':        FLAG_BG_COLOR_ORANGE,
    'EVENT_NAME_GENERIC':      FLAG_BG_COLOR_ORANGE,
    'CAPTION_ONLY_NO_OCR':     FLAG_BG_COLOR_ORANGE,
}


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
    fire false positives on multi-day recurring deals.

    2026-06 tightening: only fire when source mentions a SINGLE distinct
    day. Previously, captions like "After Friday's launch, join us
    Saturday" produced mentioned_days={Fri, Sat}; if extracted date was a
    Sunday this fired DATE_DAY_MISMATCH even when "Sunday" was the model's
    correct read of the actual event date. The false positives showed up
    in All_Events as DATE-column highlights on rows where the date was
    clearly right (per the user's 2026-06 screenshot review).

    The cost of tightening: we miss real mismatches where the caption
    mentions multiple days (e.g. multi-event calendar posts). For those,
    CALENDAR_LOW_EVENTS and the multi-event extraction path are the
    better backstop."""
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
    range_days = expand_day_ranges(src_lower)
    mentioned_days.update(range_days)

    # No days mentioned at all → can't verify, don't fire.
    if not mentioned_days:
        return None
    # Day ranges in source ("Mon-Fri", "weekend", "daily") legitimately
    # cover multiple days — the actual event day belongs to one of them.
    # If the actual day falls inside the range, we're fine; otherwise fire.
    if range_days:
        if actual_day in mentioned_days:
            return None
        return "DATE_DAY_MISMATCH"
    # Single-day case: caption mentions exactly one specific day, and the
    # extracted date doesn't match → high-confidence mismatch.
    if len(mentioned_days) == 1 and actual_day not in mentioned_days:
        return "DATE_DAY_MISMATCH"
    # Multiple distinct days in source without an explicit range — model
    # had ambiguous input. Don't punish a plausible pick. The earlier
    # behavior (fire on any miss) created false positives on correct dates
    # whenever a caption cross-referenced another event's day.
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
    FAR_FUTURE_DATE: extracted date >365 days AFTER post date.

    Bug fix (2026-05-12): main.py's PR F made datetime.now() timezone-aware,
    so post_date may now be tz-aware while `d` (from strptime) is naive.
    Subtracting one from the other raises TypeError. Strip tzinfo from
    post_date if present so we always compare naive↔naive. This is safe
    because we only use .days (not actual wall-clock arithmetic)."""
    date_str = event.get('date')
    if not date_str:
        return None
    try:
        d = datetime.strptime(str(date_str)[:10], '%Y-%m-%d')
    except (ValueError, TypeError):
        return None
    # Normalize post_date to naive — handle both tz-aware (PR F'd main.py)
    # and tz-naive (events_from_ids.py) callers.
    if post_date is not None and post_date.tzinfo is not None:
        post_date = post_date.replace(tzinfo=None)
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
# Round 2 (2026-06): bad-extraction signal flags
# ─────────────────────────────────────────────────────────────────
# These were added after running lookup_post.py against ~5 misextracted
# posts (Whitney Museum hallucination, 4loversonly/Consigliere mis-venue,
# JAM N SKATE EVENT@JAM N SKATE padding, etc.) and finding the existing
# flag set never caught the patterns that actually pollute All_Events.
#
# Two semantic categories:
#   • "PROBABLY NOT AN EVENT" (PERSONAL_POST_SIGNALS, NO_EVENT_SIGNALS,
#     EVENT_NAME_GENERIC, CAPTION_ONLY_NO_OCR) — caller should treat as
#     candidates for bulk review/delete
#   • "WRONG FIELD" (VENUE_IS_HANDLE, VENUE_EQUALS_EVENT_NAME,
#     VENUE_EQUALS_ACCOUNT, VENUE_TRUNCATED) — the extraction names a
#     field that's obviously wrong, even if event-shape is plausible
#
# The color-classification logic lives in FLAG_TO_COLOR below so callers
# can pick the right highlight without re-encoding the semantics.

_NJ_REGION_HINTS_OUT = {
    # Common cities that come up in scrapes but are clearly out of NJ.
    "NEW YORK", "NYC", "MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND",
    "PHILADELPHIA", "PHILLY",
    "BEVERLY HILLS", "LOS ANGELES", "LA",
    "MIAMI", "ATLANTA", "CHICAGO", "BOSTON",
}

_NON_NJ_STATE_SUFFIXES = (", CA", ", NY", ", PA", ", FL", ", TX", ", IL", ", MA", ", GA")

_EVENT_NAME_GENERIC_SET = {
    "EVENT", "PARTY", "NIGHT", "SHOW", "MEETUP", "GATHERING",
    "POPUP", "POP UP", "POP-UP", "FUNCTION", "AFFAIR",
}

_PERSONAL_HASHTAGS = (
    "#fyp", "#fypシ", "#ootd", "#selfie", "#me", "#stylish", "#streetwear",
    "#instagood", "#mood", "#vibes", "#aesthetic", "#sundayfunday",
    "#photodump", "#dump", "#photooftheday",
)

# Signals that a caption/OCR is event-shaped: tickets, RSVP, doors,
# time-of-day, price markers. If NONE of these appear in caption+OCR,
# whatever Gemini "extracted" is unmoored from the source.
_EVENT_GROUNDING_RX = re.compile(
    r"\b(?:"
    r"tickets?|rsvp|join us|hosted by|presents?|doors\s+open|showtime|"
    r"happy\s+hour|cover|free\s+entry|cash\s+bar|early\s+bird|"
    r"link\s+in\s+bio|swipe\s+up|"
    r"\d{1,2}\s*[ap]\.?m\.?|\d{1,2}:\d{2}\s*(?:[ap]\.?m\.?)?|"
    r"\$\d+|\d+\.\d{2}"
    r")\b",
    re.IGNORECASE,
)

_DATE_PATTERN_RX = re.compile(
    r"\b(?:"
    r"\d{1,2}/\d{1,2}(?:/\d{2,4})?|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{1,2}|"
    r"(?:mon|tue|wed|thu|fri|sat|sun)\w*\s+\d{1,2}"
    r")\b",
    re.IGNORECASE,
)

# Loose IG-handle shape: 3-30 chars, [a-z0-9._], no spaces.
_IG_HANDLE_RX = re.compile(r"^@?[a-z0-9._]{3,30}$", re.IGNORECASE)


def _venue_norm(s: str) -> str:
    """Lowercase, strip non-alphanumerics. For comparison only."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def check_venue_is_handle(event: dict) -> Optional[str]:
    """Fire when the venue field is an Instagram handle (@-mention or
    handle-shaped string with no spaces). Catches BRICKCITYJAM rows where
    venue ended up as `@worldcupcorner` because the caption used the
    @-mention as the only "venue" reference."""
    venue = (event.get('venue_name') or '').strip()
    if not venue:
        return None
    if venue.startswith('@'):
        return "VENUE_IS_HANDLE"
    if (' ' not in venue
            and venue == venue.lower()
            and _IG_HANDLE_RX.match(venue)
            and ('.' in venue or '_' in venue)):
        # Containing . or _ tightens the heuristic so we don't false-fire
        # on single-word venue names like "Stateside" or "Westlight".
        return "VENUE_IS_HANDLE"
    return None


def check_venue_equals_event_name(event: dict) -> Optional[str]:
    """Fire when venue is just a restatement of the event name. The model
    padded missing data — used the event name as the venue too. Catches
    'JAM N SKATE EVENT' at venue 'JAM N SKATE'."""
    v = _venue_norm(event.get('venue_name'))
    n = _venue_norm(event.get('event_name'))
    if not v or not n:
        return None
    if v == n:
        return "VENUE_EQUALS_EVENT_NAME"
    short, long = sorted((v, n), key=len)
    # Subset only counts as a hit when the shorter side is substantial,
    # so brief generic words ("party"/"the") don't trigger this.
    if len(short) >= 6 and short in long:
        return "VENUE_EQUALS_EVENT_NAME"
    return None


def check_venue_equals_account(event: dict) -> Optional[str]:
    """Fire when the venue is just the account's own handle or display
    name. The model lifted the account context as the venue rather than
    finding the real venue from caption/OCR. Catches the 4loversonly /
    Consigliere case (real venue was Consigliere on the flyer, but model
    used the posting account as the venue)."""
    v = _venue_norm(event.get('venue_name'))
    if not v:
        return None
    h = _venue_norm((event.get('instagram_handle') or '').lstrip('@'))
    a = _venue_norm(event.get('account_name'))
    if h and v == h:
        return "VENUE_EQUALS_ACCOUNT"
    if a and v == a:
        return "VENUE_EQUALS_ACCOUNT"
    return None


def check_personal_post_signals(caption: str, num_events: int) -> Optional[str]:
    """Fire when caption has multiple personal-post markers (selfie/style
    hashtags, no event keywords) yet events were extracted. Catches the
    Whitney Museum hallucination pattern: personal selfie post → model
    invented an event from a venue hashtag."""
    if num_events == 0:
        return None
    c = (caption or '').lower()
    if not c:
        return None
    n_personal = sum(1 for tag in _PERSONAL_HASHTAGS if tag in c)
    has_event_signal = bool(_EVENT_GROUNDING_RX.search(c)) or bool(_DATE_PATTERN_RX.search(c))
    if n_personal >= 2 and not has_event_signal:
        return "PERSONAL_POST_SIGNALS"
    return None


def check_no_event_signals(caption: str, ocr_text: str, num_events: int) -> Optional[str]:
    """Fire when caption+OCR have content but NO event-shape signals (no
    date pattern, no time pattern, no ticket/RSVP/$ keywords). Distinct
    from NO_GROUNDING — that one only fires when both caption AND OCR
    are empty. This one catches the case where there IS source text but
    none of it looks event-related."""
    if num_events == 0:
        return None
    combined = ((caption or '') + ' ' + (ocr_text or '')).strip()
    if not combined:
        return None
    if _EVENT_GROUNDING_RX.search(combined) or _DATE_PATTERN_RX.search(combined):
        return None
    return "NO_EVENT_SIGNALS"


def check_venue_out_of_nj_region(event: dict) -> Optional[str]:
    """Fire when city is clearly outside NJ + NJ-adjacent metros. Distinct
    from CITY_NOT_IN_NJ_LOOKUP, which only checks against the canonical
    NJ municipality list. This one catches Brooklyn / Philadelphia /
    Beverly Hills explicitly so they don't fall into the silent gap of
    'not in NJ list but also not flagged.'"""
    city = (event.get('city') or '').strip().upper()
    if not city:
        return None
    if city in _NJ_REGION_HINTS_OUT:
        return "VENUE_OUT_OF_NJ_REGION"
    if any(suffix in city for suffix in _NON_NJ_STATE_SUFFIXES):
        return "VENUE_OUT_OF_NJ_REGION"
    return None


def check_venue_truncated(event: dict) -> Optional[str]:
    """Fire when venue ends with truncation markers (…, ...) or exceeds
    a length the model usually doesn't produce naturally (>60 chars
    typically means the model returned a sentence as the venue)."""
    venue = (event.get('venue_name') or '').strip()
    if not venue:
        return None
    if venue.endswith(('…', '...')):
        return "VENUE_TRUNCATED"
    if len(venue) > 60:
        return "VENUE_TRUNCATED"
    return None


def check_event_name_generic(event: dict) -> Optional[str]:
    """Fire when the event name is just a placeholder word (EVENT, PARTY,
    NIGHT, etc.). Almost always means the model couldn't read a real name
    off the flyer."""
    name = (event.get('event_name') or '').strip().upper()
    if not name:
        return None
    if name in _EVENT_NAME_GENERIC_SET:
        return "EVENT_NAME_GENERIC"
    return None


def check_caption_only_no_ocr(event: dict) -> Optional[str]:
    """Fire when this event was extracted with no OCR signal. Derivable
    from the HAD OCR column directly, but as a quality flag it becomes
    sortable + composable with other flags in QUALITY_FLAGS without
    needing a separate column filter."""
    had_ocr = event.get('had_ocr')
    if had_ocr is False or str(had_ocr).strip().lower() in ('false', 'no', '0', ''):
        # 'False'/'no'/'0' all map to "no OCR" — covers the bool, the
        # sheet-serialized string form, and the post-CSV roundtrip form.
        return "CAPTION_ONLY_NO_OCR"
    return None


def check_missing_venue(event: dict) -> Optional[str]:
    v = (event.get('venue_name') or '').strip()
    if not v or v.lower() in ('none', 'null', 'tbd', 'tba', 'n/a'):
        return "MISSING_VENUE"
    return None


def check_missing_city(event: dict) -> Optional[str]:
    c = (event.get('city') or '').strip()
    if not c or c.lower() in ('none', 'null', 'tbd', 'tba', 'n/a'):
        return "MISSING_CITY"
    return None


def check_missing_time(event: dict) -> Optional[str]:
    t = (event.get('start_time') or '').strip()
    if not t or t.lower() in ('none', 'null', 'tbd', 'tba', 'n/a'):
        return "MISSING_TIME"
    return None


# ─────────────────────────────────────────────────────────────────
# Estimated cost helper
# ─────────────────────────────────────────────────────────────────

def estimate_cost(api_call_counts: dict) -> float:
    """Rough USD estimate from per-call counts. See COST_PER_CALL caveat."""
    return sum(api_call_counts.get(k, 0) * COST_PER_CALL.get(k, 0)
               for k in COST_PER_CALL)


# ─────────────────────────────────────────────────────────────────
# Gemini extraction prompt — single source of truth
# ─────────────────────────────────────────────────────────────────
# Lives here so main.py and events_from_ids.py call the same builder.
# Previously main.py had a weaker inline prompt and events_from_ids.py
# had this stronger one — measurable drift in production (60 hallucinated
# "New Jersey" cities + 384 out-of-range confidence values, per quality
# baseline captured 2026-05-11). PR B consolidates both tools onto this
# version. Six categories of guidance: anti-hallucination, spatial layout,
# explicit relative-date resolution, recurring/multi-day handling, city
# specificity, and confidence scale documentation.

def build_prompt(post: dict, ocr_text: str, post_date: datetime) -> str:
    """Construct the extraction prompt sent to Gemini. Single source of
    truth — both main.py (production cron) and events_from_ids.py
    (recovery + re-extraction) import and call this function. Any prompt
    change goes here so the two tools never drift again."""
    user = post.get('ownerUsername', '')
    owner_full_name = post.get('ownerFullName', '')
    location_name = post.get('locationName', '') or post.get('location', '')
    caption = post.get('caption', '') or post.get('text', '')
    post_date_str = post_date.strftime('%Y-%m-%d')
    post_day_name = post_date.strftime('%A')

    return f"""
        Extract ALL events from this Instagram post. A post may contain MULTIPLE events.

        POST DATE: {post_date_str} ({post_day_name}) — your anchor for relative dates
        ACCOUNT: @{user} ({owner_full_name})
        LOCATION TAG: {location_name}

        CAPTION: {caption[:2000]}
        OCR TEXT FROM IMAGE(S): {ocr_text[:5000]}
        NOTE: If OCR text contains [SLIDE N of M] markers, this is a carousel post.
        Each slide may show different events (e.g. a weekly calendar spread across slides).
        Extract events from ALL slides.

        ════════════════════════════════════════════════════
        EXTRACTION GUIDANCE
        ════════════════════════════════════════════════════

        ANTI-HALLUCINATION (critical):
        Each event's name, date, venue, and city MUST appear in (or be a clear
        paraphrase of) the caption or OCR text. Do NOT invent details. If a
        required field cannot be found in the source, leave it BLANK rather
        than guessing.

        SPATIAL LAYOUT (multi-column flyers):
        If the post has a grid or table layout (e.g., 2-column event list),
        preserve each event's pairing with its corresponding venue and time.
        Do NOT shuffle these fields across rows of the grid. Read each
        cell/box as a self-contained unit.

        MULTIPLE EVENTS:
        - Calendars, weekly lineups, event series → extract each
        - "Monday: Jazz Night, Tuesday: Open Mic" → 2 events
        - "Dec 15 - Band, Dec 22 - Party" → 2 events
        - If a single recurring deal applies to a day RANGE (e.g.
          "Mon-Fri Happy Hour"), extract as ONE event (see RECURRING below),
          NOT one event per day.

        ════════════════════════════════════════════════════
        DATE HANDLING
        ════════════════════════════════════════════════════

        DATE PARSING — handle these formats and convert to YYYY-MM-DD:
        - Shorthand: "3.13.26", "3.13", "3/13", "March 13th", "Mar 13"
        - Year shorthand: "26" means 2026, "25" means 2025
        - "<day> <date>": e.g., "Friday May 9" — use the explicit date.
          (If day-of-week conflicts with date, prefer the date.)

        RELATIVE DATE RESOLUTION (POST DATE = {post_date_str}, {post_day_name}):
        - "today" / "tonight" → POST DATE
        - "tomorrow" → POST DATE + 1 day
        - "this <day>":
            • If POST DATE falls on that day → use POST DATE itself
            • Otherwise → next occurrence within 6 days
        - "next <day>" → strictly AFTER this <day> (typically following week)
        - "this weekend":
            • If POST DATE is Fri/Sat/Sun → that weekend (Sat or Sun)
            • Otherwise → upcoming Saturday/Sunday
        - "next weekend" → the weekend AFTER this weekend

        ════════════════════════════════════════════════════
        RECURRING / MULTI-DAY EVENTS
        ════════════════════════════════════════════════════

        Extract as ONE event (NOT multiple) with is_recurring=true when:
        - "Every <day>" — "Every Saturday", "Every Friday night"
        - Day RANGE — "Mon-Fri", "Wednesday-Saturday", "Mon through Fri"
        - Shorthand — "weekday", "weeknight" (= Mon-Fri), "weekend" (= Sat-Sun)
        - Ongoing offers — "Daily happy hour", "Trivia Tuesdays", "$5 marg mon-fri"

        For these:
        - date: NEXT occurrence on or after POST DATE
        - is_recurring: true
        - recurrence_pattern: short pattern like "Mon-Fri", "Every Saturday",
          "Daily", "Weekend", "Mon/Wed/Fri"

        DATE RANGES (one-time events spanning multiple days):
        - "Sale May 5-12" → ONE event, date=2026-05-05, is_recurring=false
        - Include the date range in the description.

        ════════════════════════════════════════════════════
        FIELD REQUIREMENTS
        ════════════════════════════════════════════════════

        1. event_name: Max 40 characters. If natural name is longer, create a
           shorter marketable version that captures the essence.
        2. newsletter_description: one-sentence punchy teaser for a newsletter.
        3. city: SPECIFIC NJ municipality (e.g., "Newark", "Jersey City",
           "Madison", "Asbury Park"). NEVER use the state name "New Jersey",
           "NJ", or generic "NJ area". If the city cannot be determined,
           leave it BLANK.
        4. section_of_nj: North / Central / South based on the city's county:
           - North = Bergen, Essex, Hudson, Morris, Passaic, Sussex, Warren
           - Central = Hunterdon, Mercer, Middlesex, Monmouth, Somerset, Union
           - South = Atlantic, Burlington, Camden, Cape May, Cumberland,
                    Gloucester, Ocean, Salem
           If city is empty or out-of-state, leave section_of_nj BLANK.
        5. start_time: Strict 12-hour format (e.g., "2:00 PM"). Blank if not stated.
           If source gives a TIME RANGE ("4-7 PM", "11AM-3PM", "6 to 10pm"),
           use the START of the range here. Put the full range in description.
        6. confidence: Float between 0.0 and 1.0 (NOT a percentage):
           • 0.9-1.0 = explicit info (date, venue, time clearly stated)
           • 0.7-0.9 = mostly stated; minor inference required
           • 0.5-0.7 = significant inference; some fields ambiguous
           • < 0.5 = guesswork; consider whether to extract at all

        ════════════════════════════════════════════════════
        OUTPUT FORMAT
        ════════════════════════════════════════════════════

        Return JSON with "events" list. Each event has these fields:
        event_name, date (YYYY-MM-DD), start_time, venue_name, city,
        section_of_nj, newsletter_description, event_type, description,
        performer, price, confidence, is_recurring, recurrence_pattern

        Also include:
        "total_events_found": number,
        "is_calendar_post": true/false

        If no events found, return: {{"events": [], "total_events_found": 0}}
        """
