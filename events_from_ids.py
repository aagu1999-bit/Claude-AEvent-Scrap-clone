#!/usr/bin/env python3
"""
events_from_ids.py — Re-extract events for a known list of Instagram post IDs,
or upload an existing Events_*.csv directly. Writes to All_Events with
per-row quality flags and conditional formatting.

PURPOSE
───────
Two modes, one tool, same output:

  1. EXTRACT MODE  (--from-ids <list.csv>)
     For each post_id: look up in cached Apify dataset(s), run OCR + Gemini,
     apply sanity checks, escalate to image/Pro tiers as needed, append to
     All_Events. Used for orphan recovery, manual re-runs, A/B testing.

  2. UPLOAD MODE  (--from-events <events.csv>)
     Take a pre-extracted Events_*.csv, dedup against All_Events, append.
     Used when save_data() wrote the CSV but the Sheets push failed.
     (Replaces recover_events.py.)

DESIGN
──────
- Bypasses Processed_Log entirely (target posts are already marked done)
- Incremental writes (every BATCH_SIZE), kill-resilient
- 3-tier extraction ladder: Flash-Lite text → Flash-Lite image → Pro image
- Per-row sanity checks drive escalation; surviving flags hit QUALITY_FLAGS
- Authoritative city → region lookup (data/nj_municipalities.json)
- Light-yellow conditional formatting on flagged rows

USAGE
─────
  python events_from_ids.py --from-ids outputs/orphan_recovery_queue.csv \\
      --source-datasets UwYunGBIyTDHqPE1Y,WqdAa2sd2LTESaEVc \\
      --tag recovery_2026_05_08 \\
      --dry-run

  python events_from_ids.py --from-events outputs/Events_20260508_174931.csv

See docs/decisions/0011-events-from-ids.md for design rationale.
"""

import argparse
import csv
import json
import math
import os
import re
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from google.cloud import vision
import google.generativeai as genai

# ─────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"

NJ_MUNICIPALITIES_JSON = "data/nj_municipalities.json"
VENUE_CITY_CANONICAL_JSON = "data/venue_city_canonical.json"

# Sheet schema — matches save_data() in main.py + new QUALITY_FLAGS column
ALL_EVENTS_HEADER = [
    "INSTAGRAM HANDLE", "EVENT NAME", "DATE", "START TIME",
    "VENUE NAME", "CITY", "SECTION OF NJ", "NEWSLETTER DESCRIPTION",
    "INSTAGRAM POST URL", "DISPLAY URL", "POST URL", "INSTAGRAM PROFILE URL",
    "EVENT TYPE", "ACCOUNT NAME", "DESCRIPTION", "PERFORMER", "PRICE",
    "CONFIDENCE", "POST ID", "HAD OCR", "FROM CALENDAR",
    "IS RECURRING", "PROCESSED TIMESTAMP", "QUALITY_FLAGS",
]
QUALITY_FLAGS_COL_IDX = ALL_EVENTS_HEADER.index("QUALITY_FLAGS")

BATCH_SIZE = 25
BATCH_DELAY_SEC = 1.5

TIER_FLASH_TEXT = "flash_text"
TIER_FLASH_IMAGE = "flash_image"
TIER_PRO_IMAGE = "pro_image"

LOW_CONFIDENCE_THRESHOLD = 0.5
CALENDAR_KEYWORDS = ("calendar", "lineup", "schedule", "weekly", "monthly",
                     "weekend", "series", "every")

FLAG_BG_COLOR = {"red": 1.0, "green": 0.97, "blue": 0.78}  # light yellow

# Day-name → weekday number (Mon=0)
DAY_NAMES = {
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
    "mon": 0, "tue": 1, "tues": 1, "wed": 2, "thu": 3, "thur": 3, "thurs": 3,
    "fri": 4, "sat": 5, "sun": 6,
}

ESCALATION_FLAGS = {
    "DATE_DAY_MISMATCH", "VENUE_CITY_MISMATCH", "CALENDAR_LOW_EVENTS",
    "CAROUSEL_LOW_EVENTS", "OCR_RICH_LOW_EVENTS", "LOW_CONFIDENCE",
    "ACCOUNT_PATTERN_DROP",
}

# Apify dataset URL template
APIFY_DATASET_URL = (
    "https://api.apify.com/v2/datasets/{ds_id}/items?format=json"
    "&omit=audioUrl%2CvideoViewCount%2CvideoPlayCount%2CvideoDuration"
    "%2Csponsors%2CrequestErrorMessages%2CproductType%2CpaidPartnership"
    "%2CmusicInfo%2Cmentions%2ClikesCount%2ClatestComments"
    "%2CisCommentsDisabled%2CerrorDescription%2Chashtags"
    "%2CcoauthorProducers%2CcommentsCount%2CdimensionsHeight"
    "%2CdimensionsWidth%2Cerror&clean=false"
)

# Gemini model identifiers
MODEL_FLASH_LITE = "gemini-2.5-flash-lite"
MODEL_PRO = "gemini-2.5-pro"

# Rough per-call cost estimates (USD). Vendor pricing changes — these are
# back-of-envelope figures for run-summary visibility, not billing-grade
# accuracy. Update as pricing evolves.
COST_PER_CALL = {
    'vision':       0.0015,   # Cloud Vision text_detection per image
    'flash_text':   0.0005,   # Gemini 2.5 Flash-Lite (text input only)
    'flash_image':  0.0010,   # Gemini 2.5 Flash-Lite (image input)
    'pro_image':    0.0080,   # Gemini 2.5 Pro (image input)
}

EXTRACT_RUNS_DIR = Path("outputs/extract_runs")


# ─────────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────────

def setup_sheet():
    if not Path(SERVICE_ACCOUNT_FILE).exists():
        print(f"❌ Service account not found at {SERVICE_ACCOUNT_FILE}", file=sys.stderr)
        sys.exit(1)
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    return gspread.authorize(creds).open(SHEET_NAME)


def setup_vision():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/cloud-vision']
    )
    return vision.ImageAnnotatorClient(credentials=creds)


def setup_gemini(model_name: str, api_key: Optional[str] = None):
    api_key = api_key or os.environ.get('GEMINI_API_KEY') or os.environ.get('GOOGLE_API_KEY')
    if not api_key:
        # Try config.json
        try:
            with open('config.json') as f:
                api_key = json.load(f).get('gemini_api_key')
        except Exception:
            pass
    if not api_key:
        print("❌ No Gemini API key found (env GEMINI_API_KEY or config.json)", file=sys.stderr)
        sys.exit(1)
    genai.configure(api_key=api_key)
    return genai.GenerativeModel(model_name)


def ensure_schema(ws, dry_run=False):
    current = ws.row_values(1)
    if current == ALL_EVENTS_HEADER:
        return
    if len(current) == len(ALL_EVENTS_HEADER) - 1:
        if dry_run:
            print("  [dry-run] would add QUALITY_FLAGS column to All_Events header")
            return
        print("  Adding QUALITY_FLAGS column to All_Events header...")
        col_letter = chr(ord('A') + len(ALL_EVENTS_HEADER) - 1)
        # gspread 6.x argument order: values first, range_name second
        ws.update(values=[["QUALITY_FLAGS"]], range_name=f"{col_letter}1")
        return
    if dry_run:
        print(f"  [dry-run] header mismatch (have {len(current)} cols, expected {len(ALL_EVENTS_HEADER)}) — would rewrite")
        return
    print("  Rewriting All_Events header to canonical schema...")
    ws.update(values=[ALL_EVENTS_HEADER], range_name="A1", value_input_option="USER_ENTERED")


def init_stats() -> dict:
    """Bundle of per-run telemetry. Mutated throughout the run; serialized at end."""
    return {
        'phase_timings': {
            'apify_load':  0.0,
            'ocr':         0.0,
            'gemini':      0.0,
            'sheet_write': 0.0,
        },
        'counts': {
            'queue_size':           0,
            'processed':            0,
            'extracted_events':     0,
            'events_with_flags':    0,
            'no_events':            0,
            'missing_in_apify':     0,
            'already_in_all_events': 0,
        },
        'tiers_used':         Counter(),  # final tier per post
        'flag_distribution':  Counter(),  # per-flag occurrence count
        'skip_reasons':       Counter(),
        'per_account_events': Counter(),  # handle -> events extracted
        'api_call_counts': {
            'vision':       0,
            'flash_text':   0,
            'flash_image':  0,
            'pro_image':    0,
        },
        # Region-lookup visibility — distinguishes "Gemini got it right"
        # from "we corrected it" so we can see if the safety net is working
        # vs Gemini happening to be lucky.
        'region_lookup': {
            'hits':            0,  # event city was in NJ lookup
            'autofixed':       0,    # we corrected the region
            'already_correct': 0,    # Gemini agreed with lookup
            'non_nj_cleared':  0,    # city tagged NON_NJ; region cleared
            'unknown_city':    0,    # city not in lookup at all
        },
    }


def estimate_cost(api_call_counts: dict) -> float:
    """Rough USD estimate from per-call counts. See COST_PER_CALL caveat."""
    return sum(api_call_counts.get(k, 0) * COST_PER_CALL.get(k, 0)
               for k in COST_PER_CALL)


def write_run_summary(stats: dict, args, wall_time: float, run_id: str):
    """Persist run summary as JSON for future audit / cross-run analysis."""
    EXTRACT_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        'run_id':              run_id,
        'tag':                 args.tag,
        'run_timestamp':       datetime.now().isoformat(),
        'wall_time_seconds':   round(wall_time, 2),
        'mode':                'extract',
        'queue_csv':           args.from_ids,
        'source_datasets':     args.source_datasets.split(',') if args.source_datasets else [],
        'max_tier':            args.max_tier,
        'limit':               args.limit,
        'dry_run':             args.dry_run,
        'phase_timings_seconds': {k: round(v, 2) for k, v in stats['phase_timings'].items()},
        'counts':              dict(stats['counts']),
        'tiers_used':          dict(stats['tiers_used']),
        'flag_distribution':   dict(stats['flag_distribution']),
        'skip_reasons':        dict(stats['skip_reasons']),
        'per_account_events':  dict(stats['per_account_events'].most_common(50)),
        'api_call_counts':     dict(stats['api_call_counts']),
        'region_lookup':       dict(stats['region_lookup']),
        'estimated_cost_usd':  round(estimate_cost(stats['api_call_counts']), 4),
    }
    out_path = EXTRACT_RUNS_DIR / f"{run_id}.json"
    with open(out_path, 'w') as f:
        json.dump(out, f, indent=2, sort_keys=True)
    return out_path


# ─────────────────────────────────────────────────────────────────
# Lookups
# ─────────────────────────────────────────────────────────────────

def load_nj_municipalities() -> dict:
    p = Path(NJ_MUNICIPALITIES_JSON)
    if not p.exists():
        print(f"⚠ {p} missing — region check disabled")
        return {}
    return json.loads(p.read_text())


def load_venue_canonical() -> dict:
    p = Path(VENUE_CITY_CANONICAL_JSON)
    if not p.exists():
        print(f"⚠ {p} missing — venue check disabled")
        return {}
    return json.loads(p.read_text())


def load_apify_datasets(dataset_ids: list) -> dict:
    """Load multiple Apify datasets, return dict of post_id -> post."""
    lookup = {}
    for ds_id in dataset_ids:
        url = APIFY_DATASET_URL.format(ds_id=ds_id)
        print(f"  Fetching {ds_id}...")
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                data = json.load(r)
            print(f"    ✓ {len(data)} posts")
        except Exception as e:
            print(f"    ✗ Failed: {e}")
            continue
        for p in data:
            pid = p.get('id')
            if pid and pid not in lookup:
                lookup[pid] = p
    print(f"  Total unique posts in lookup: {len(lookup)}")
    return lookup


# ─────────────────────────────────────────────────────────────────
# Image / OCR helpers (ported from main.py)
# ─────────────────────────────────────────────────────────────────

def collect_carousel_urls(post: dict) -> list:
    """Return all image URLs for a post (carousel-aware)."""
    urls = []
    seen = set()

    def add(u):
        if u and u not in seen:
            seen.add(u)
            urls.append(u)

    if post.get('images'):
        for img in post['images']:
            if isinstance(img, str):
                add(img)
            elif isinstance(img, dict):
                add(img.get('url') or img.get('displayUrl', ''))
    if post.get('childPosts'):
        for child in post['childPosts']:
            if isinstance(child, dict):
                add(child.get('displayUrl', ''))
                if child.get('images'):
                    for img in child['images']:
                        add(img if isinstance(img, str) else img.get('url', ''))
    if not urls and post.get('displayUrl'):
        add(post['displayUrl'])

    return urls


def ocr_one_image(vision_client, image_url: str, stats: dict = None, timeout=15) -> str:
    """Download image and run OCR. Returns text or '' on any failure.
    Increments stats['api_call_counts']['vision'] on each text_detection call."""
    if not image_url:
        return ""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'image/webp,image/*,*/*;q=0.8',
        }
        r = requests.get(image_url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return ""
        image = vision.Image(content=r.content)
        if stats is not None:
            stats['api_call_counts']['vision'] += 1
        resp = vision_client.text_detection(image=image)
        if resp.error.message:
            return ""
        if resp.text_annotations:
            return resp.text_annotations[0].description
        return ""
    except Exception as e:
        print(f"    ⚠ OCR error: {str(e)[:80]}")
        return ""


def ocr_post(vision_client, post: dict, stats: dict = None, verbose_slides: bool = False) -> tuple:
    """Run OCR on all images for a post. Returns (combined_ocr_text, num_slides).
    Times itself into stats['phase_timings']['ocr'] when stats provided.

    For multi-slide posts (>=3 slides) prints a per-slide breakdown so we
    can see whether the post genuinely has multi-event data spread across
    slides vs. a single flyer surrounded by promo/decoration images.
    Critical for diagnosing CAROUSEL_LOW_EVENTS without guessing."""
    urls = collect_carousel_urls(post)
    if not urls:
        return "", 0
    t0 = time.perf_counter()
    parts = []
    show_breakdown = verbose_slides or len(urls) >= 3
    for i, url in enumerate(urls, 1):
        text = ocr_one_image(vision_client, url, stats=stats)
        if show_breakdown:
            n = len(text or '')
            if n == 0:
                print(f"      Slide {i}/{len(urls)}: 0 chars (no text)")
            else:
                preview = (text or '').replace('\n', ' ')[:60]
                print(f"      Slide {i}/{len(urls)}: {n} chars  | {preview}")
        if text:
            if len(urls) > 1:
                parts.append(f"[SLIDE {i} of {len(urls)}]\n{text}")
            else:
                parts.append(text)
    if stats is not None:
        stats['phase_timings']['ocr'] += time.perf_counter() - t0
    return "\n\n".join(parts), len(urls)


# ─────────────────────────────────────────────────────────────────
# Gemini extraction
# ─────────────────────────────────────────────────────────────────

def build_prompt(post: dict, ocr_text: str, post_date: datetime) -> str:
    """Same prompt as main.py:784. One source of truth via copy for now;
    when prompt audit (A6) lands, both sites will update together."""
    user = post.get('ownerUsername', '')
    owner_full_name = post.get('ownerFullName', '')
    location_name = post.get('locationName', '') or post.get('location', '')
    caption = post.get('caption', '') or post.get('text', '')

    return f"""
        Extract ALL events from this Instagram post. A post may contain MULTIPLE events.

        POST DATE: {post_date.strftime('%Y-%m-%d')} (use this to resolve relative and recurring dates)
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

        DATE PARSING — handle ALL formats and convert to YYYY-MM-DD:
        - Shorthand: "3.13.26", "3.13", "3/13", "March 13th", "Mar 13"
        - Day refs: "this Saturday", "next Friday", "tonight" → calculate from POST DATE
        - Year shorthand: "26" means 2026, "25" means 2025

        RECURRING EVENTS — if no specific one-time date:
        - "Every Saturday", "Weekly Thursdays", "EVERY SATURDAY & SUNDAY"
        → Calculate NEXT occurrence ON OR AFTER POST DATE, set "is_recurring": true

        REQUIREMENTS:
        1. event_name: Max 40 characters
        2. newsletter_description: one-sentence punchy teaser
        3. section_of_nj: North/Central/South based on city/county
        4. start_time: Strict 12-hour format (e.g. 2:00 PM)

        Return JSON with "events" list containing:
        event_name, date (YYYY-MM-DD), start_time, venue_name, city, section_of_nj,
        newsletter_description, event_type, description, performer, price, confidence, is_recurring

        Also include:
        "total_events_found": number,
        "is_calendar_post": true/false

        If no events found, return: {{"events": [], "total_events_found": 0}}
        """


def call_gemini(model, prompt: str, stats: dict = None, tier: str = 'flash_text') -> Optional[dict]:
    """Call Gemini and parse JSON response. Returns dict or None on failure.
    Tracks api_call_counts[tier] and phase_timings['gemini'] when stats given."""
    if stats is not None:
        stats['api_call_counts'][tier] = stats['api_call_counts'].get(tier, 0) + 1
    t0 = time.perf_counter()
    try:
        resp = model.generate_content(prompt)
        if not resp:
            return None
        text = resp.text.strip()
    except Exception as e:
        print(f"    ✗ Gemini error: {str(e)[:120]}")
        return None
    finally:
        if stats is not None:
            stats['phase_timings']['gemini'] += time.perf_counter() - t0
    clean = re.sub(r'```json\s*|```', '', text).strip()
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        m = re.search(r'\{.*\}', clean, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except Exception:
                return None
        return None


def enrich_events(events: list, post: dict, post_date_str: str, has_ocr: bool, is_calendar: bool) -> list:
    """Add per-event metadata fields the sheet expects. Same as main.py."""
    user = post.get('ownerUsername', '')
    owner_full_name = post.get('ownerFullName', '')
    shortcode = post.get('shortCode', '') or post.get('shortcode', '')
    display_url = post.get('displayUrl', '') or post.get('display_url', '')
    pid = post.get('id') or post.get('shortCode')

    out = []
    for e in events:
        if not e.get('event_name') and not e.get('date'):
            continue
        if e.get('event_name') and len(e['event_name']) > 40:
            e['event_name'] = e['event_name'][:40].rsplit(' ', 1)[0]
        e['instagram_handle'] = user
        e['instagram_post_url'] = f"https://www.instagram.com/p/{shortcode}/" if shortcode else ''
        e['display_url'] = display_url
        e['post_url'] = post.get('url', '')
        e['instagram_profile_url'] = f"https://www.instagram.com/{user}/" if user else ''
        e['account_name'] = owner_full_name
        e['post_id'] = pid
        e['had_ocr'] = has_ocr
        e['from_calendar'] = is_calendar
        e['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # start_time normalization (lightweight — main.py has clean_time helper)
        st = e.get('start_time', '')
        if st and isinstance(st, str):
            e['start_time'] = st.strip().upper().replace('.', '')
        out.append(e)
    return out


# ─────────────────────────────────────────────────────────────────
# Tier extractors
# ─────────────────────────────────────────────────────────────────

def extract_tier_flash_text(ctx: dict, post: dict) -> tuple:
    """Tier 1: Flash-Lite + OCR text. Returns (events, slide_count, ocr_len, has_ocr).
    Stores OCR text back on post['_ocr_text'] so downstream sanity checks can
    use it (date↔day, calendar-low-events both need source text)."""
    print(f"  [Tier 1: Flash-Lite + OCR text]")

    stats = ctx.get('stats')

    # OCR
    ocr_text, num_slides = ocr_post(ctx['vision_client'], post, stats=stats)
    post['_ocr_text'] = ocr_text  # surface for sanity checks
    has_ocr = bool(ocr_text)
    if has_ocr:
        print(f"    ✓ OCR: {len(ocr_text)} chars across {num_slides} slide(s)")
    else:
        print(f"    ⚠ No OCR text")

    # Post date
    post_date = parse_post_date(post)

    # Build prompt + call Flash-Lite
    prompt = build_prompt(post, ocr_text, post_date)
    data = call_gemini(ctx['model_flash'], prompt, stats=stats, tier='flash_text')
    if not data:
        return [], num_slides, len(ocr_text), has_ocr

    events = data.get('events', [])
    is_calendar = data.get('is_calendar_post', False)
    enriched = enrich_events(events, post, post_date.strftime('%Y-%m-%d'), has_ocr, is_calendar)
    return enriched, num_slides, len(ocr_text), has_ocr


def extract_tier_flash_image(ctx: dict, post: dict) -> tuple:
    """Tier 2: Flash-Lite multimodal. TODO — implement after Tier 1 verified."""
    print(f"  [Tier 2: Flash-Lite + image — NOT YET IMPLEMENTED, falling back to Tier 1]")
    return extract_tier_flash_text(ctx, post)


def extract_tier_pro_image(ctx: dict, post: dict) -> tuple:
    """Tier 3: Pro multimodal. TODO — implement after Tier 1 verified."""
    print(f"  [Tier 3: Pro + image — NOT YET IMPLEMENTED, falling back to Tier 1]")
    return extract_tier_flash_text(ctx, post)


TIER_HANDLERS = {
    TIER_FLASH_TEXT: extract_tier_flash_text,
    TIER_FLASH_IMAGE: extract_tier_flash_image,
    TIER_PRO_IMAGE: extract_tier_pro_image,
}


# ─────────────────────────────────────────────────────────────────
# Sanity checks
# ─────────────────────────────────────────────────────────────────

def check_date_day_match(event: dict, source_text: str) -> Optional[str]:
    """If source mentions a day name and extracted date doesn't fall on
    that day, flag it."""
    date_str = event.get('date', '')
    if not date_str or not source_text:
        return None
    try:
        d = datetime.strptime(date_str[:10], '%Y-%m-%d')
    except ValueError:
        return None
    actual_day = d.weekday()  # 0=Mon

    src_lower = source_text.lower()
    mentioned_days = set()
    for name, num in DAY_NAMES.items():
        # word-boundary match
        if re.search(rf'\b{name}\b', src_lower):
            mentioned_days.add(num)

    if not mentioned_days:
        return None
    if actual_day not in mentioned_days:
        return "DATE_DAY_MISMATCH"
    return None


def check_venue_city(event: dict, venue_lookup: dict) -> Optional[str]:
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
    Updates stats['region_lookup'] counters when stats is provided so we can
    see whether Gemini's getting it right on its own vs being corrected."""
    rl = stats['region_lookup'] if stats else None
    city = (event.get('city') or '').strip().upper()
    if not city:
        return None
    canonical = region_lookup.get(city)
    if canonical is None:
        if rl is not None:
            rl['unknown_city'] += 1
        return "CITY_NOT_IN_NJ_LOOKUP"
    if canonical == "NON_NJ":
        if event.get('section_of_nj'):
            event['section_of_nj'] = ""
        if rl is not None:
            rl['non_nj_cleared'] += 1
        return "CITY_NOT_IN_NJ_LOOKUP"
    # City IS in NJ lookup — register the hit
    if rl is not None:
        rl['hits'] += 1
    current = (event.get('section_of_nj') or '').strip().upper()
    if current != canonical:
        event['section_of_nj'] = canonical
        if rl is not None:
            rl['autofixed'] += 1
        return "REGION_AUTOFIXED"
    if rl is not None:
        rl['already_correct'] += 1
    return None


def check_low_confidence(event: dict) -> Optional[str]:
    try:
        conf = float(event.get('confidence', 1.0))
    except (TypeError, ValueError):
        return None
    return "LOW_CONFIDENCE" if conf < LOW_CONFIDENCE_THRESHOLD else None


def _count_distinct_dates(text: str) -> int:
    """Count distinct date-shaped tokens in text. Used to distinguish
    real calendar posts (multiple dates) from single-event posts that
    happen to mention 'schedule'/'every'/etc."""
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


def check_calendar_low_events(caption: str, ocr_text: str, num_events: int) -> Optional[str]:
    """Fire only when (caption keyword OR OCR keyword) AND ≥2 distinct
    date tokens appear across caption+OCR. Single-event posts that mention
    'schedule' once shouldn't trip this — they need multi-date evidence."""
    if num_events > 1:
        return None
    combined = ((caption or '') + ' ' + (ocr_text or '')).lower()
    if not any(kw in combined for kw in CALENDAR_KEYWORDS):
        return None
    # Require ≥2 distinct dates in source — that's what makes it a calendar
    if _count_distinct_dates(combined) < 2:
        return None
    return "CALENDAR_LOW_EVENTS"


def check_carousel_low_events(slide_count: int, num_events: int) -> Optional[str]:
    if slide_count >= 3 and num_events <= 1:
        return "CAROUSEL_LOW_EVENTS"
    return None


def check_ocr_rich_low_events(ocr_len: int, num_events: int) -> Optional[str]:
    if ocr_len >= 1500 and num_events <= 1:
        return "OCR_RICH_LOW_EVENTS"
    return None


def check_account_pattern(account: str, num_events: int, account_avg: dict) -> Optional[str]:
    avg = account_avg.get(account)
    if avg is not None and avg >= 3 and num_events <= 1:
        return "ACCOUNT_PATTERN_DROP"
    return None


def should_escalate(flags: list) -> bool:
    return any(f in ESCALATION_FLAGS for f in flags)


def next_tier(current: str) -> Optional[str]:
    if current == TIER_FLASH_TEXT:
        return TIER_FLASH_IMAGE
    if current == TIER_FLASH_IMAGE:
        return TIER_PRO_IMAGE
    return None


# ─────────────────────────────────────────────────────────────────
# Per-post orchestration
# ─────────────────────────────────────────────────────────────────

def parse_post_date(post: dict) -> datetime:
    ts = post.get('timestamp', '')
    if not ts:
        return datetime.now()
    try:
        if isinstance(ts, (int, float)):
            return datetime.fromtimestamp(ts)
        return datetime.fromisoformat(str(ts).replace('Z', '+00:00')).replace(tzinfo=None)
    except Exception:
        return datetime.now()


def process_one_post(ctx: dict, post: dict, max_tier: str = TIER_PRO_IMAGE) -> tuple:
    """Run a post through the tier ladder. Returns (events_with_flags, tier_used)."""
    current_tier = TIER_FLASH_TEXT
    final_events = []

    while current_tier is not None:
        handler = TIER_HANDLERS[current_tier]
        events, slide_count, ocr_len, has_ocr = handler(ctx, post)

        # Per-event flags
        source_text = (post.get('caption') or '') + ' ' + (post.get('_ocr_text') or '')
        flags_per_event = []
        for ev in events:
            row_flags = []
            f = check_region_against_lookup(ev, ctx['region_lookup'], stats=ctx.get('stats'))
            if f: row_flags.append(f)
            f = check_date_day_match(ev, source_text)
            if f: row_flags.append(f)
            f = check_venue_city(ev, ctx['venue_lookup'])
            if f: row_flags.append(f)
            f = check_low_confidence(ev)
            if f: row_flags.append(f)
            flags_per_event.append(row_flags)

        # Post-level flags (apply to all events)
        post_flags = []
        for f in [
            check_calendar_low_events(post.get('caption', ''),
                                      post.get('_ocr_text', ''),
                                      len(events)),
            check_carousel_low_events(slide_count, len(events)),
            check_ocr_rich_low_events(ocr_len, len(events)),
            check_account_pattern(post.get('ownerUsername', ''), len(events), ctx['account_avg']),
        ]:
            if f: post_flags.append(f)

        for ev_flags in flags_per_event:
            ev_flags.extend(post_flags)

        # Decide escalation
        all_flags = set()
        for fl in flags_per_event:
            all_flags.update(fl)
        nt = next_tier(current_tier)
        if should_escalate(list(all_flags)) and nt and current_tier != max_tier:
            print(f"  ↳ flags fired ({sorted(all_flags)}); escalating to {nt}")
            current_tier = nt
        else:
            # Final tier reached (or no escalation needed). Surface
            # informational flags before settling so user sees auto-fixes.
            non_escalating = sorted(f for f in all_flags if f not in ESCALATION_FLAGS)
            if non_escalating:
                print(f"  ↳ informational flags: {non_escalating}")
            # Final result
            for ev, fl in zip(events, flags_per_event):
                ev['quality_flags'] = ",".join(sorted(set(fl)))
            final_events = events
            break

    return final_events, current_tier


# ─────────────────────────────────────────────────────────────────
# Sheet writing
# ─────────────────────────────────────────────────────────────────

def event_to_row(event: dict, run_tag: str = "") -> list:
    """Map event dict to row matching ALL_EVENTS_HEADER order, all-uppercase strings."""
    def up(v):
        if v is None: return ""
        if isinstance(v, bool): return str(v)
        s = str(v)
        return s.upper() if not s.startswith(('http://', 'https://')) else s

    fields = {
        "INSTAGRAM HANDLE": up(event.get('instagram_handle', '')),
        "EVENT NAME": up(event.get('event_name', '')),
        "DATE": up(event.get('date', '')),
        "START TIME": up(event.get('start_time', '')),
        "VENUE NAME": up(event.get('venue_name', '')),
        "CITY": up(event.get('city', '')),
        "SECTION OF NJ": up(event.get('section_of_nj', '')),
        "NEWSLETTER DESCRIPTION": up(event.get('newsletter_description', '')),
        "INSTAGRAM POST URL": event.get('instagram_post_url', ''),
        "DISPLAY URL": event.get('display_url', ''),
        "POST URL": event.get('post_url', ''),
        "INSTAGRAM PROFILE URL": event.get('instagram_profile_url', ''),
        "EVENT TYPE": up(event.get('event_type', '')),
        "ACCOUNT NAME": up(event.get('account_name', '')),
        "DESCRIPTION": up(event.get('description', '')),
        "PERFORMER": up(event.get('performer', '')),
        "PRICE": up(event.get('price', '')),
        "CONFIDENCE": up(event.get('confidence', '')),
        "POST ID": str(event.get('post_id', '')),
        "HAD OCR": up(event.get('had_ocr', '')),
        "FROM CALENDAR": up(event.get('from_calendar', '')),
        "IS RECURRING": up(event.get('is_recurring', '')),
        "PROCESSED TIMESTAMP": event.get('processed_timestamp', ''),
        "QUALITY_FLAGS": event.get('quality_flags', ''),
    }
    return [fields[col] for col in ALL_EVENTS_HEADER]


def append_with_formatting(ws, rows: list, start_row: int, dry_run: bool = False, no_format: bool = False):
    """Append rows + apply light yellow to flagged ones."""
    if not rows:
        return
    if dry_run:
        print(f"  [dry-run] would append {len(rows)} rows starting at row {start_row}")
        flagged = [i for i, r in enumerate(rows) if r[QUALITY_FLAGS_COL_IDX]]
        if flagged:
            print(f"  [dry-run] would format {len(flagged)} flagged row(s) light yellow")
        return
    ws.append_rows(rows, value_input_option="USER_ENTERED")
    if no_format:
        return
    flagged = [i for i, r in enumerate(rows) if r[QUALITY_FLAGS_COL_IDX]]
    last_col = chr(ord('A') + len(ALL_EVENTS_HEADER) - 1)
    for offset in flagged:
        row_num = start_row + offset
        a1 = f"A{row_num}:{last_col}{row_num}"
        try:
            ws.format(a1, {"backgroundColor": FLAG_BG_COLOR})
            time.sleep(0.3)
        except Exception as e:
            print(f"    ⚠ Format failed for row {row_num}: {str(e)[:80]}")


# ─────────────────────────────────────────────────────────────────
# Mode: extract from post-IDs
# ─────────────────────────────────────────────────────────────────

def run_extract_mode(args):
    run_start = time.perf_counter()
    run_id = args.tag or datetime.now().strftime("extract_%Y%m%d_%H%M%S")

    print(f"━━━ EXTRACT MODE ━━━")
    print(f"  Run ID:           {run_id}")
    print(f"  Input CSV:        {args.from_ids}")
    print(f"  Source datasets:  {args.source_datasets}")
    print(f"  Max tier:         {args.max_tier}")
    print(f"  Tag:              {args.tag or '(none)'}")
    print(f"  Dry run:          {args.dry_run}")
    print(f"  Workers:          {args.workers}")
    print()

    if not args.source_datasets:
        print("❌ --source-datasets is required for extract mode", file=sys.stderr)
        sys.exit(1)

    stats = init_stats()

    # Load orphan queue
    queue_path = Path(args.from_ids)
    if not queue_path.exists():
        print(f"❌ Queue not found: {queue_path}", file=sys.stderr)
        sys.exit(1)
    with open(queue_path) as f:
        queue = list(csv.DictReader(f))
    if args.limit:
        queue = queue[:args.limit]
    stats['counts']['queue_size'] = len(queue)
    print(f"  Queue rows: {len(queue)}")

    # Load datasets (timed)
    print(f"\n  Loading Apify datasets...")
    t0 = time.perf_counter()
    ds_ids = [s.strip() for s in args.source_datasets.split(',') if s.strip()]
    apify_lookup = load_apify_datasets(ds_ids)
    stats['phase_timings']['apify_load'] = time.perf_counter() - t0

    # Load lookups
    region_lookup = load_nj_municipalities()
    venue_lookup = load_venue_canonical()
    print(f"  NJ municipalities: {len(region_lookup)}")
    print(f"  Venue canonical:   {len(venue_lookup)}")

    # Setup APIs
    print(f"\n  Setting up Vision + Gemini...")
    vision_client = setup_vision()
    model_flash = setup_gemini(MODEL_FLASH_LITE)

    ctx = {
        'vision_client': vision_client,
        'model_flash':   model_flash,
        'model_pro':     None,  # lazy-init when first Tier 3 call happens
        'region_lookup': region_lookup,
        'venue_lookup':  venue_lookup,
        'account_avg':   {},
        'stats':         stats,  # mutated by tier handlers
    }

    # Setup sheet
    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)
    ensure_schema(ws, dry_run=args.dry_run)
    # Find true last data row (row_count is sheet capacity, not data extent)
    next_row = len(ws.col_values(1)) + 1 if not args.dry_run else 99999

    # Dedup against existing All_Events: skip queue posts that already have
    # rows in the sheet. Belt-and-suspenders against stale orphan queues
    # (the queue is a snapshot in time; rerunning the tool against the same
    # queue should be safe). Add --force-reextract later if the workflow
    # needs to override this.
    print(f"\n  Loading existing All_Events post IDs for dedup...")
    existing_data = ws.get_all_values()
    existing_pids = set()
    if existing_data:
        post_id_idx = ALL_EVENTS_HEADER.index("POST ID")
        for row in existing_data[1:]:
            if len(row) > post_id_idx and row[post_id_idx].strip():
                existing_pids.add(row[post_id_idx].strip())
    print(f"    {len(existing_pids)} unique post IDs already in All_Events")

    pending_rows = []

    for i, queue_row in enumerate(queue, 1):
        pid = queue_row.get('post_id', '').strip()
        if not pid:
            stats['skip_reasons']['empty_post_id_in_queue'] += 1
            continue

        if pid in existing_pids:
            print(f"\n[{i}/{len(queue)}] {pid} — already in All_Events, skipping")
            stats['counts']['already_in_all_events'] += 1
            stats['skip_reasons']['already_in_all_events'] += 1
            continue

        post = apify_lookup.get(pid)
        if not post:
            print(f"\n[{i}/{len(queue)}] {pid} — NOT in any source dataset, skipping")
            stats['counts']['missing_in_apify'] += 1
            stats['skip_reasons']['missing_in_apify'] += 1
            continue

        account = post.get('ownerUsername', '?')
        print(f"\n[{i}/{len(queue)}] {pid}  @{account}")
        events, tier_used = process_one_post(ctx, post, max_tier=args.max_tier)
        stats['counts']['processed'] += 1
        stats['tiers_used'][tier_used] += 1

        if not events:
            print(f"  ↳ No events extracted (tier={tier_used})")
            stats['counts']['no_events'] += 1
            stats['skip_reasons']['gemini_returned_no_events'] += 1
            continue

        print(f"  ✓ {len(events)} event(s) (tier={tier_used})")
        stats['counts']['extracted_events'] += len(events)
        stats['per_account_events'][account] += len(events)

        for idx, ev in enumerate(events, 1):
            name = ev.get('event_name', '(unnamed)')
            date = ev.get('date', '?')
            venue = ev.get('venue_name', '?') or '—'
            city = ev.get('city', '?') or '—'
            region = ev.get('section_of_nj', '') or '—'
            conf = ev.get('confidence', '?')
            flags = ev.get('quality_flags', '')
            print(f"    {idx}. {name}")
            print(f"       date={date}  time={ev.get('start_time','—') or '—'}  venue={venue}  city={city}  region={region}  conf={conf}")
            if flags:
                print(f"       🚩 FLAGS: {flags}")
                stats['counts']['events_with_flags'] += 1
                for flag in flags.split(','):
                    f = flag.strip()
                    if f:
                        stats['flag_distribution'][f] += 1
            else:
                print(f"       ✓ clean (no flags)")
            pending_rows.append(event_to_row(ev, run_tag=args.tag))

        # Incremental flush — kill-resilient batches
        if len(pending_rows) >= BATCH_SIZE:
            tw = time.perf_counter()
            append_with_formatting(ws, pending_rows, next_row,
                                   dry_run=args.dry_run, no_format=args.no_formatting)
            stats['phase_timings']['sheet_write'] += time.perf_counter() - tw
            if not args.dry_run:
                next_row += len(pending_rows)
            pending_rows = []
            time.sleep(BATCH_DELAY_SEC)

    # Final flush
    if pending_rows:
        tw = time.perf_counter()
        append_with_formatting(ws, pending_rows, next_row,
                               dry_run=args.dry_run, no_format=args.no_formatting)
        stats['phase_timings']['sheet_write'] += time.perf_counter() - tw

    # ──────────────────────────────────────────────────────────
    # Summary report
    # ──────────────────────────────────────────────────────────
    wall = time.perf_counter() - run_start
    print(f"\n━━━ SUMMARY ━━━")

    c = stats['counts']
    print(f"  Wall time:              {wall:.1f}s")
    print(f"  Queue rows:             {c['queue_size']}")
    print(f"  Posts processed:        {c['processed']}")
    print(f"  Events extracted:       {c['extracted_events']}")
    print(f"  Events with flags:      {c['events_with_flags']}")
    print(f"  Posts no events:        {c['no_events']}")
    print(f"  Posts missing in Apify: {c['missing_in_apify']}")
    print(f"  Posts already in sheet: {c['already_in_all_events']}")

    if stats['tiers_used']:
        print(f"\n  Tier usage:")
        for tier in (TIER_FLASH_TEXT, TIER_FLASH_IMAGE, TIER_PRO_IMAGE):
            n = stats['tiers_used'].get(tier, 0)
            pct = 100 * n / max(c['processed'], 1)
            print(f"    {tier:<14} {n:>4}  ({pct:.0f}%)")

    if stats['flag_distribution']:
        print(f"\n  Flag distribution:")
        for flag, n in stats['flag_distribution'].most_common():
            print(f"    {flag:<28} {n:>4}")

    # Region lookup — distinguishes "Gemini got it right" from "we corrected it"
    rl = stats['region_lookup']
    if rl['hits'] or rl['unknown_city'] or rl['non_nj_cleared']:
        autofix_pct = 100 * rl['autofixed'] / max(rl['hits'], 1)
        print(f"\n  Region lookup activity:")
        print(f"    NJ cities matched lookup: {rl['hits']}")
        print(f"      ↳ Gemini already correct: {rl['already_correct']}")
        print(f"      ↳ region auto-fixed:      {rl['autofixed']}  ({autofix_pct:.0f}% of NJ hits)")
        print(f"    NON_NJ cleared:           {rl['non_nj_cleared']}")
        print(f"    Unknown to lookup:        {rl['unknown_city']}")

    if stats['skip_reasons']:
        print(f"\n  Skip reasons:")
        for reason, n in stats['skip_reasons'].most_common():
            print(f"    {reason:<32} {n:>4}")

    if stats['per_account_events']:
        print(f"\n  Top accounts by events extracted:")
        for handle, n in stats['per_account_events'].most_common(10):
            print(f"    @{handle:<28} {n:>4}")

    print(f"\n  Phase timings (s):")
    for phase, t in stats['phase_timings'].items():
        print(f"    {phase:<14} {t:>7.1f}")

    print(f"\n  API calls + estimated cost:")
    for kind, n in stats['api_call_counts'].items():
        cost = n * COST_PER_CALL.get(kind, 0)
        print(f"    {kind:<14} {n:>5}  ≈ ${cost:.4f}")
    total_cost = estimate_cost(stats['api_call_counts'])
    print(f"    {'TOTAL':<14} {'':>5}  ≈ ${total_cost:.4f}")

    # Write JSON summary (always, even on dry-run, for repeatable audits)
    summary_path = write_run_summary(stats, args, wall, run_id)
    print(f"\n✓ Run summary saved: {summary_path}")


# ─────────────────────────────────────────────────────────────────
# Mode: upload existing events CSV
# ─────────────────────────────────────────────────────────────────

def run_upload_mode(args):
    print(f"━━━ UPLOAD MODE ━━━")
    print(f"  Input CSV: {args.from_events}")
    print(f"  Dry run:   {args.dry_run}")
    print()

    csv_path = Path(args.from_events)
    if not csv_path.exists():
        print(f"❌ Not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    import pandas as pd
    df = pd.read_csv(csv_path, dtype=str).fillna("")
    print(f"  Loaded {len(df)} rows, {len(df.columns)} cols")

    sh = setup_sheet()
    ws = sh.worksheet(ALL_EVENTS_TAB)
    ensure_schema(ws, dry_run=args.dry_run)

    # Dedup
    print(f"\n  Reading All_Events for dedup...")
    existing = ws.get_all_values()
    if existing:
        header = existing[0]
        pid_idx = header.index("POST ID") if "POST ID" in header else None
        name_idx = header.index("EVENT NAME") if "EVENT NAME" in header else None
        date_idx = header.index("DATE") if "DATE" in header else None
        existing_keys = set()
        for r in existing[1:]:
            pid = r[pid_idx] if pid_idx is not None and pid_idx < len(r) else ""
            n = r[name_idx] if name_idx is not None and name_idx < len(r) else ""
            d = r[date_idx] if date_idx is not None and date_idx < len(r) else ""
            existing_keys.add((str(pid).strip(), str(n).strip().upper(), str(d).strip()))
        print(f"    {len(existing)-1} existing rows, {len(existing_keys)} unique composite keys")
    else:
        existing_keys = set()

    if "POST ID" in df.columns:
        df["_key"] = df.apply(
            lambda r: (str(r.get("POST ID", "")).strip(),
                       str(r.get("EVENT NAME", "")).strip().upper(),
                       str(r.get("DATE", "")).strip()),
            axis=1)
        new = df[~df["_key"].isin(existing_keys)].drop(columns=["_key"])
    else:
        new = df

    skipped = len(df) - len(new)
    print(f"  Net-new rows: {len(new)} (skipping {skipped} dupes)")

    if len(new) == 0:
        print("\n✅ Nothing to append")
        return

    if args.dry_run:
        print(f"\n[dry-run] would append {len(new)} rows. Sample:")
        print(new.head(3).to_string())
        return

    print(f"\n  Appending {len(new)} rows in batches of {BATCH_SIZE}...")
    rows = new.fillna("").astype(str).values.tolist()
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        n = i // BATCH_SIZE + 1
        total = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"    ✓ batch {n}/{total} ({len(batch)} rows)")
        if i + BATCH_SIZE < len(rows):
            time.sleep(BATCH_DELAY_SEC)

    print(f"\n✅ Uploaded {len(new)} events")


# ─────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Re-extract events for post-IDs, or upload existing events CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--from-ids", metavar="CSV",
                      help="Input CSV with post_id column (extract mode)")
    mode.add_argument("--from-events", metavar="CSV",
                      help="Input Events_*.csv to upload (upload mode)")

    p.add_argument("--source-datasets", default="",
                   help="Comma-separated Apify dataset IDs (extract mode)")
    p.add_argument("--max-tier", choices=[TIER_FLASH_TEXT, TIER_FLASH_IMAGE, TIER_PRO_IMAGE],
                   default=TIER_PRO_IMAGE, help="Cap tier escalation (default: pro_image)")
    p.add_argument("--workers", type=int, default=1, help="Parallel workers (default: 1)")
    p.add_argument("--limit", type=int, default=None,
                   help="Process only first N rows (smoke testing)")
    p.add_argument("--tag", default="", help="Tag for traceability")
    p.add_argument("--dry-run", action="store_true", help="Don't write to sheet")
    p.add_argument("--no-formatting", action="store_true",
                   help="Skip conditional formatting (faster)")
    return p.parse_args()


def main():
    args = parse_args()
    if args.from_ids:
        run_extract_mode(args)
    else:
        run_upload_mode(args)


if __name__ == "__main__":
    main()
