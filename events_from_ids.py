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
import threading
import time
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

# Shared extraction logic. See docs/decisions/0012-extraction-core-foundation.md.
# Pulls the canonical constants, lookup loaders, helpers, and sanity checks
# from a single source of truth so main.py can adopt the same safety net
# via a parallel migration (B5 step 3).
from extraction_core import (
    # Constants
    MODEL_FLASH_LITE, MODEL_PRO,
    COST_PER_CALL,
    TIER_FLASH_CAPTION, TIER_FLASH_TEXT, TIER_FLASH_IMAGE, TIER_PRO_IMAGE,
    TIER_LADDER,
    NJ_MUNICIPALITIES_JSON, VENUE_CITY_CANONICAL_JSON,
    LOW_CONFIDENCE_THRESHOLD,
    CALENDAR_KEYWORDS,
    DAY_NAMES,
    FLAG_TO_COLUMNS,
    ESCALATION_FLAGS,
    FLAG_BG_COLOR,
    # Lookup loaders
    load_nj_municipalities, load_venue_canonical,
    # Cost
    estimate_cost,
    # Sanity checks
    check_date_day_match, check_venue_city, check_region_against_lookup,
    check_low_confidence, check_missing_date, check_date_sanity,
    check_calendar_low_events, check_carousel_low_events,
    check_ocr_rich_low_events, check_account_pattern, check_no_grounding,
)

import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
from google.cloud import vision
import google.generativeai as genai

# ─────────────────────────────────────────────────────────────────
# Tool-local constants (sheet schema, batching, paths)
# ─────────────────────────────────────────────────────────────────
# Most cross-tool constants (model IDs, tier ladder, cost-per-call,
# DAY_NAMES, FLAG_TO_COLUMNS, ESCALATION_FLAGS, FLAG_BG_COLOR, lookup
# paths, etc.) now come from extraction_core via the top imports.
# Items here are events_from_ids-specific.

SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"
SHEET_NAME = "Instagram_Events_Master"
ALL_EVENTS_TAB = "All_Events"

# Sheet schema — extends what save_data() in main.py produces with two new
# columns at the end: QUALITY_FLAGS and RECURRENCE_PATTERN. Appending at end
# (vs inserting in the middle) makes the migration trivial — append columns
# to existing sheets without shifting any data.
ALL_EVENTS_HEADER = [
    "INSTAGRAM HANDLE", "EVENT NAME", "DATE", "START TIME",
    "VENUE NAME", "CITY", "SECTION OF NJ", "NEWSLETTER DESCRIPTION",
    "INSTAGRAM POST URL", "DISPLAY URL", "POST URL", "INSTAGRAM PROFILE URL",
    "EVENT TYPE", "ACCOUNT NAME", "DESCRIPTION", "PERFORMER", "PRICE",
    "CONFIDENCE", "POST ID", "HAD OCR", "FROM CALENDAR",
    "IS RECURRING", "PROCESSED TIMESTAMP",
    "QUALITY_FLAGS",        # added today: per-row sanity-check fingerprint
    "RECURRENCE_PATTERN",   # added today: e.g., "Mon-Fri", "Every Saturday", "Daily"
]
QUALITY_FLAGS_COL_IDX = ALL_EVENTS_HEADER.index("QUALITY_FLAGS")
RECURRENCE_PATTERN_COL_IDX = ALL_EVENTS_HEADER.index("RECURRENCE_PATTERN")

BATCH_SIZE = 25
BATCH_DELAY_SEC = 1.5

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


def _col_letter(header_label: str) -> str:
    """Convert ALL_EVENTS_HEADER label -> A1 column letter (A, B, ..., AA, AB...)."""
    idx = ALL_EVENTS_HEADER.index(header_label)
    if idx < 26:
        return chr(ord('A') + idx)
    return chr(ord('A') + (idx // 26) - 1) + chr(ord('A') + (idx % 26))

EXTRACT_RUNS_DIR = Path("outputs/extract_runs")

# Apify datasets are immutable once published, so caching them locally is
# always safe — and protects us if Apify wipes a dataset later (which DID
# happen during today's investigation: the original 4,441-entry dataset
# at 3YcD7sSCliQhCw87m is now down to 3 entries). Keep a permanent local
# copy so future audits, re-runs, and recovery work always have access
# to the exact source data the run consumed.
APIFY_CACHE_DIR = Path("outputs/apify_cache")


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


def _idx_to_col_letter(idx: int) -> str:
    """0-indexed column number -> A1 letter ('A', 'B', ..., 'Z', 'AA', 'AB', ...)."""
    if idx < 26:
        return chr(ord('A') + idx)
    return chr(ord('A') + (idx // 26) - 1) + chr(ord('A') + (idx % 26))


def ensure_schema(ws, dry_run=False):
    """Bring All_Events header to ALL_EVENTS_HEADER. Handles three cases:
    1. Already canonical → no-op
    2. Current is a prefix (missing trailing columns) → append the missing ones
       (this is the safe migration path — preserves existing row data)
    3. Mid-row mismatch (column shifted/renamed) → full rewrite (loud)
    """
    current = [c.strip() for c in ws.row_values(1)]
    if current == ALL_EVENTS_HEADER:
        return

    # Prefix-match: current header matches the start of the canonical, just
    # missing some columns at the end. Safe to append.
    if (len(current) <= len(ALL_EVENTS_HEADER)
            and ALL_EVENTS_HEADER[:len(current)] == current):
        missing = ALL_EVENTS_HEADER[len(current):]
        if not missing:
            return
        if dry_run:
            print(f"  [dry-run] would append {len(missing)} column(s): {missing}")
            return
        print(f"  Appending {len(missing)} column(s) to All_Events: {missing}")
        start_col = _idx_to_col_letter(len(current))
        ws.update(values=[missing], range_name=f"{start_col}1")
        return

    # Otherwise: header has unexpected drift — rewrite (preserves data, may
    # cause column-meaning mismatch on existing rows; warn loudly)
    if dry_run:
        print(f"  [dry-run] header has drift: have {len(current)} cols, expected {len(ALL_EVENTS_HEADER)} — would rewrite (CAUTION: existing rows may misalign)")
        return
    print(f"  ⚠ Rewriting All_Events header (drift from canonical) — {len(current)} → {len(ALL_EVENTS_HEADER)} cols")
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
            'vision':         0,
            'flash_caption':  0,
            'flash_text':     0,
            'flash_image':    0,
            'pro_image':      0,
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


def write_run_summary(stats: dict, args, wall_time: float, run_id: str,
                      interrupted: bool = False):
    """Persist run summary as JSON for future audit / cross-run analysis.

    Set interrupted=True when called from the SIGINT path — adds an
    'interrupted' flag and 'completion_status' field to the JSON so
    downstream tools (merge_extract_runs.py, audits) can distinguish
    partial summaries from complete ones."""
    EXTRACT_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    out = {
        'run_id':              run_id,
        'tag':                 args.tag,
        'run_timestamp':       datetime.now().isoformat(),
        'wall_time_seconds':   round(wall_time, 2),
        'mode':                'extract',
        'completion_status':   'interrupted' if interrupted else 'completed',
        'interrupted':         interrupted,
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
# load_nj_municipalities() and load_venue_canonical() now live in
# extraction_core (imported at top of file). load_apify_datasets stays
# here because it's events_from_ids-specific (Apify-aware caching).


def load_apify_datasets(dataset_ids: list) -> dict:
    """Load multiple Apify datasets, return dict of post_id -> post.

    Uses a local file cache (outputs/apify_cache/<dataset_id>.json) to
    avoid re-fetching. Apify datasets are immutable once published, so
    cache hit is always safe. Cache also serves as a permanent local
    archive — we discovered today that Apify can wipe datasets
    (3YcD7sSCliQhCw87m went from 4,441 entries to 3 entries between
    morning and afternoon). The cache protects us from that."""
    APIFY_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    lookup = {}
    for ds_id in dataset_ids:
        cache_path = APIFY_CACHE_DIR / f"{ds_id}.json"
        data = None

        # Try cache first
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    data = json.load(f)
                print(f"  Loaded {ds_id} from cache  ✓ {len(data)} posts")
            except Exception as e:
                print(f"  Cache read failed for {ds_id} ({e}); fetching fresh")
                data = None

        # Fetch + cache if no valid cache
        if data is None:
            url = APIFY_DATASET_URL.format(ds_id=ds_id)
            print(f"  Fetching {ds_id}...")
            try:
                with urllib.request.urlopen(url, timeout=60) as r:
                    data = json.load(r)
                print(f"    ✓ {len(data)} posts")
                # Persist to cache
                try:
                    with open(cache_path, 'w') as f:
                        json.dump(data, f)
                    print(f"    ✓ Cached to {cache_path} for future runs")
                except Exception as e:
                    print(f"    ⚠ Cache write failed: {e}")
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


def download_post_images(post: dict, max_images: int = 20, timeout: int = 15) -> list:
    """Download images for a post — returns list of (bytes, mime_type) tuples.
    Used by flash_image / pro_image tiers (multimodal Gemini takes bytes
    directly; no Vision API roundtrip needed). Skips images that fail.
    Caps at max_images to avoid token blowup on huge carousels."""
    urls = collect_carousel_urls(post)[:max_images]
    images = []
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'image/webp,image/*,*/*;q=0.8',
    }
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code != 200:
                continue
            mime = r.headers.get('Content-Type', 'image/jpeg').split(';')[0].strip()
            # Gemini accepts image/jpeg, image/png, image/webp, image/heic, image/heif
            if mime not in ('image/jpeg', 'image/png', 'image/webp', 'image/heic', 'image/heif'):
                mime = 'image/jpeg'  # best-effort default
            images.append((r.content, mime))
        except Exception:
            continue
    return images


def ocr_post(vision_client, post: dict, stats: dict = None, verbose_slides: bool = False) -> tuple:
    """Run OCR on all images for a post. Returns (combined_ocr_text, num_slides, slides_with_text).
    Times itself into stats['phase_timings']['ocr'] when stats provided.

    Returns three values:
      - combined_ocr_text: concatenated OCR output, with [SLIDE N of M] markers
      - num_slides: total slides processed
      - slides_with_text: count of slides that produced non-empty OCR

    The slides_with_text count is used by CAROUSEL_LOW_EVENTS to distinguish
    "carousel of decorative photos + 1 flyer" (where 1-event extraction is
    correct) from "multiple slides each with event content" (where 1-event
    extraction probably missed events)."""
    urls = collect_carousel_urls(post)
    if not urls:
        return "", 0, 0
    t0 = time.perf_counter()
    parts = []
    slides_with_text = 0
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
            slides_with_text += 1
            if len(urls) > 1:
                parts.append(f"[SLIDE {i} of {len(urls)}]\n{text}")
            else:
                parts.append(text)
    if stats is not None:
        stats['phase_timings']['ocr'] += time.perf_counter() - t0
    return "\n\n".join(parts), len(urls), slides_with_text


# ─────────────────────────────────────────────────────────────────
# Gemini extraction
# ─────────────────────────────────────────────────────────────────

def build_prompt(post: dict, ocr_text: str, post_date: datetime) -> str:
    """Construct the extraction prompt sent to Gemini. A6 prompt audit
    introduced 6 categories of guidance: anti-hallucination, spatial layout,
    explicit relative-date resolution, recurring/multi-day handling,
    city specificity, and confidence scale documentation."""
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


def call_gemini(model, prompt, stats: dict = None, tier: str = 'flash_text') -> Optional[dict]:
    """Call Gemini and parse JSON response. Returns dict or None on failure.
    Accepts either a string prompt (text-only) or a list mixing text and
    image dicts for multimodal calls — generate_content() handles both.
    Tracks api_call_counts[tier] and phase_timings['gemini'] when stats given."""
    if stats is not None:
        stats['api_call_counts'][tier] = stats['api_call_counts'].get(tier, 0) + 1
    t0 = time.perf_counter()
    text = None
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
    if text is None:
        return None
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

def extract_tier_flash_caption_only(ctx: dict, post: dict) -> tuple:
    """Tier 1: cheapest probe. Flash-Lite on caption only, no OCR/Vision.

    Returns (events, slide_count, ocr_len, has_ocr).

    Why this exists: many posts have all the event info in the caption
    text. For those, we don't need to spend Vision API calls per slide.
    If this tier produces events with no flags fired, we save real money.
    Otherwise the caller escalates to Tier 2 which adds OCR."""
    print(f"  [Tier 1: Flash-Lite caption-only (no Vision)]")
    stats = ctx.get('stats')

    # Surface (empty) OCR text on the post so downstream checks have a
    # consistent interface — caption-only treats ocr_text as ""
    post['_ocr_text'] = ""
    num_slides = len(collect_carousel_urls(post))

    post_date = parse_post_date(post)
    prompt = build_prompt(post, "", post_date)
    data = call_gemini(ctx['model_flash'], prompt, stats=stats, tier='flash_caption')
    if not data:
        return [], num_slides, 0, False

    events = data.get('events', [])
    is_calendar = data.get('is_calendar_post', False)
    enriched = enrich_events(events, post, post_date.strftime('%Y-%m-%d'),
                             has_ocr=False, is_calendar=is_calendar)
    return enriched, num_slides, 0, False


def extract_tier_flash_text(ctx: dict, post: dict) -> tuple:
    """Tier 2: Flash-Lite + OCR text. Returns (events, slide_count, ocr_len, has_ocr).
    Stores OCR text back on post['_ocr_text'] so downstream sanity checks can
    use it (date↔day, calendar-low-events both need source text)."""
    print(f"  [Tier 3: Flash-Lite + OCR text]")

    stats = ctx.get('stats')

    # OCR
    ocr_text, num_slides, slides_with_text = ocr_post(ctx['vision_client'], post, stats=stats)
    post['_ocr_text'] = ocr_text  # surface for sanity checks
    post['_slides_with_text'] = slides_with_text  # surface for CAROUSEL check
    has_ocr = bool(ocr_text)
    if has_ocr:
        print(f"    ✓ OCR: {len(ocr_text)} chars across {num_slides} slide(s) ({slides_with_text} with text)")
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
    """Tier 2: Flash-Lite multimodal. Sends caption + post images directly to
    Gemini (no Vision OCR) — single API call regardless of slide count, and
    preserves spatial layout (addresses kinnectnj-class column-grid bugs).

    Returns (events, slide_count, ocr_len, has_ocr).
    Note: ocr_len=0, has_ocr=False because this tier doesn't do OCR. Subsequent
    sanity checks that depend on OCR text won't fire (intentional — escalation
    to flash_text would re-introduce them if needed)."""
    print(f"  [Tier 2: Flash-Lite multimodal (image)]")
    stats = ctx.get('stats')

    images = download_post_images(post)
    num_slides = len(collect_carousel_urls(post))
    if not images:
        print(f"    ⚠ No images downloadable — falling back to caption-only handling")
        post['_ocr_text'] = ""
        post_date = parse_post_date(post)
        prompt = build_prompt(post, "", post_date)
        data = call_gemini(ctx['model_flash'], prompt, stats=stats, tier='flash_caption')
        if not data:
            return [], num_slides, 0, False
        events = data.get('events', [])
        is_calendar = data.get('is_calendar_post', False)
        return enrich_events(events, post, post_date.strftime('%Y-%m-%d'),
                             has_ocr=False, is_calendar=is_calendar), num_slides, 0, False

    print(f"    ✓ {len(images)} image(s) prepared for multimodal")
    post['_ocr_text'] = ""  # consistent interface; this tier doesn't have OCR text
    post_date = parse_post_date(post)
    prompt = build_prompt(post, "", post_date)

    # Multimodal payload: prompt text + each image as a content part
    content = [prompt] + [{'mime_type': mime, 'data': data} for data, mime in images]
    resp_data = call_gemini(ctx['model_flash'], content, stats=stats, tier='flash_image')
    if not resp_data:
        return [], num_slides, 0, False

    events = resp_data.get('events', [])
    is_calendar = resp_data.get('is_calendar_post', False)
    enriched = enrich_events(events, post, post_date.strftime('%Y-%m-%d'),
                             has_ocr=False, is_calendar=is_calendar)
    return enriched, num_slides, 0, False


def extract_tier_pro_image(ctx: dict, post: dict) -> tuple:
    """Tier 4: Pro multimodal. Last resort for cases that flash_image still
    flags. Same input shape as flash_image (caption + all images, single
    multimodal call) but uses Gemini 2.5 Pro for higher-quality reasoning.

    Pro is dramatically more expensive than Flash-Lite (~8× per call), so
    this should only fire on a small minority of posts that escalate
    through Tiers 1-3 with persistent flags. The cost discipline is
    enforced by the tier ladder; the model itself does not gate."""
    print(f"  [Tier 4: Pro multimodal (image)]")
    stats = ctx.get('stats')

    # Lazy-init the Pro model — most runs never hit this tier, so paying
    # the import/setup cost only when needed
    if ctx.get('model_pro') is None:
        ctx['model_pro'] = setup_gemini(MODEL_PRO)

    images = download_post_images(post)
    num_slides = len(collect_carousel_urls(post))
    if not images:
        print(f"    ⚠ No images downloadable — falling back to caption-only on Pro")
        post['_ocr_text'] = ""
        post_date = parse_post_date(post)
        prompt = build_prompt(post, "", post_date)
        # Caption-only on Pro is still tracked under pro_image (it's the same
        # tier-4 attempt, just without images available)
        data = call_gemini(ctx['model_pro'], prompt, stats=stats, tier='pro_image')
        if not data:
            return [], num_slides, 0, False
        events = data.get('events', [])
        is_calendar = data.get('is_calendar_post', False)
        return enrich_events(events, post, post_date.strftime('%Y-%m-%d'),
                             has_ocr=False, is_calendar=is_calendar), num_slides, 0, False

    print(f"    ✓ {len(images)} image(s) prepared for Pro multimodal")
    post['_ocr_text'] = ""
    post_date = parse_post_date(post)
    prompt = build_prompt(post, "", post_date)

    content = [prompt] + [{'mime_type': mime, 'data': data} for data, mime in images]
    resp_data = call_gemini(ctx['model_pro'], content, stats=stats, tier='pro_image')
    if not resp_data:
        return [], num_slides, 0, False

    events = resp_data.get('events', [])
    is_calendar = resp_data.get('is_calendar_post', False)
    enriched = enrich_events(events, post, post_date.strftime('%Y-%m-%d'),
                             has_ocr=False, is_calendar=is_calendar)
    return enriched, num_slides, 0, False


TIER_HANDLERS = {
    TIER_FLASH_CAPTION: extract_tier_flash_caption_only,
    TIER_FLASH_TEXT:    extract_tier_flash_text,
    TIER_FLASH_IMAGE:   extract_tier_flash_image,
    TIER_PRO_IMAGE:     extract_tier_pro_image,
}


# ─────────────────────────────────────────────────────────────────
# Sanity checks now live in extraction_core (imported at top of file).
# expand_day_ranges, count_distinct_dates, and the eleven check_* functions
# are all canonical there; see docs/decisions/0012-extraction-core-foundation.md.
# ─────────────────────────────────────────────────────────────────

def should_escalate(flags: list) -> bool:
    return any(f in ESCALATION_FLAGS for f in flags)


def next_tier(current: str) -> Optional[str]:
    """Return the next tier in the ladder, or None if already at the top."""
    try:
        idx = TIER_LADDER.index(current)
    except ValueError:
        return None
    if idx + 1 < len(TIER_LADDER):
        return TIER_LADDER[idx + 1]
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
    """Run a post through the tier ladder. Returns (events_with_flags, tier_used).

    Starts at TIER_FLASH_CAPTION (cheapest probe — caption only, no Vision)
    and escalates only when needed.

    Smart skip: if the post has no caption text, Tier 1 (caption-only) has
    nothing to work with — skip directly to TIER_FLASH_IMAGE so we don't
    waste an API call on guaranteed-empty input."""
    if not (post.get('caption') or '').strip():
        print(f"  ↳ no caption — starting at flash_image (skipping Tier 1)")
        current_tier = TIER_FLASH_IMAGE
    else:
        current_tier = TIER_FLASH_CAPTION
    final_events = []

    while current_tier is not None:
        handler = TIER_HANDLERS[current_tier]
        events, slide_count, ocr_len, has_ocr = handler(ctx, post)

        # Per-event flags
        source_text = (post.get('caption') or '') + ' ' + (post.get('_ocr_text') or '')
        post_date = parse_post_date(post)
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
            f = check_missing_date(ev)
            if f: row_flags.append(f)
            f = check_date_sanity(ev, post_date)
            if f: row_flags.append(f)
            flags_per_event.append(row_flags)

        # Post-level flags (apply to all events)
        slides_with_text = post.get('_slides_with_text', 0)
        post_flags = []
        for f in [
            check_calendar_low_events(post.get('caption', ''),
                                      post.get('_ocr_text', ''),
                                      len(events)),
            check_carousel_low_events(slide_count, slides_with_text, len(events)),
            check_ocr_rich_low_events(post.get('_ocr_text', ''), len(events)),
            check_account_pattern(post.get('ownerUsername', ''), len(events), ctx['account_avg']),
            check_no_grounding(post.get('caption', ''), post.get('_ocr_text', ''), len(events)),
        ]:
            if f: post_flags.append(f)

        for ev_flags in flags_per_event:
            ev_flags.extend(post_flags)

        # Decide escalation
        all_flags = set()
        for fl in flags_per_event:
            all_flags.update(fl)
        nt = next_tier(current_tier)

        # Escalate if either:
        #  (a) any escalation flag fired (ALWAYS escalates, including to Pro), OR
        #  (b) we got 0 events at a non-final, non-Pro tier (the cheap probe
        #      missed — worth trying a more expensive non-Pro tier once)
        #
        # Critical: we DO NOT escalate to Tier 4 (Pro) just because earlier
        # tiers returned 0 events. If Tier 1+2+3 all returned 0 events with
        # no quality flags, the post likely has no event content (Reels with
        # empty captions, recap posts, etc.) — Pro can't manufacture events
        # from nothing, and Pro is ~8× more expensive than Flash-Lite. Only
        # specific quality concerns (a flag firing) earn Pro budget.
        # Discovered via tier4_test (PR #8): 5 of 6 captionless-Reel posts
        # escalated all the way to Pro on zero_events with no flags, all
        # produced 0 events at Pro too. Wasted ~$0.04 on a 10-post test.
        flag_escalate = should_escalate(list(all_flags))
        zero_event_escalate = (len(events) == 0 and nt != TIER_PRO_IMAGE)
        can_escalate = nt is not None and current_tier != max_tier

        if can_escalate and (flag_escalate or zero_event_escalate):
            reason = []
            if flag_escalate: reason.append(f"flags={sorted(all_flags)}")
            if zero_event_escalate: reason.append("zero_events")
            print(f"  ↳ escalating to {nt}  ({', '.join(reason)})")
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
        "RECURRENCE_PATTERN": up(event.get('recurrence_pattern', '')),
    }
    return [fields[col] for col in ALL_EVENTS_HEADER]


def append_with_formatting(ws, rows: list, start_row: int, dry_run: bool = False, no_format: bool = False):
    """Append rows + apply per-cell light yellow based on which flag fired.

    Per-cell coloring (vs. whole-row) lets the user visually identify exactly
    which field is uncertain — date cell yellow = date is suspect, city cell
    yellow = city is uncertain, etc. Post-level flags color the QUALITY_FLAGS
    cell itself, since they're not about a specific field.

    Uses gspread's batch_format to apply all formatting in one API call (vs.
    one ws.format() per row, which used to hit rate limits)."""
    if not rows:
        return

    # Compute per-row cells to highlight up front (used by both dry-run and live)
    per_row_cells = []
    for r in rows:
        flags_str = r[QUALITY_FLAGS_COL_IDX]
        if not flags_str:
            per_row_cells.append(set())
            continue
        cells = set()
        for f in flags_str.split(','):
            f = f.strip()
            if f in FLAG_TO_COLUMNS:
                cells.update(FLAG_TO_COLUMNS[f])
        per_row_cells.append(cells)

    if dry_run:
        print(f"  [dry-run] would append {len(rows)} rows starting at row {start_row}")
        cell_count = sum(len(c) for c in per_row_cells)
        if cell_count:
            print(f"  [dry-run] would format {cell_count} cells light yellow (per-flag)")
        return

    ws.append_rows(rows, value_input_option="USER_ENTERED")
    if no_format:
        return

    # Build batch_format request: one entry per (row, cell) flagged
    format_requests = []
    for i, cells in enumerate(per_row_cells):
        if not cells:
            continue
        row_num = start_row + i
        for col_label in cells:
            letter = _col_letter(col_label)
            format_requests.append({
                'range': f'{letter}{row_num}',
                'format': {'backgroundColor': FLAG_BG_COLOR},
            })

    if not format_requests:
        return
    try:
        ws.batch_format(format_requests)
    except AttributeError:
        # Older gspread without batch_format — fall back to per-cell calls
        for fr in format_requests:
            try:
                ws.format(fr['range'], fr['format'])
                time.sleep(0.3)
            except Exception as e2:
                print(f"    ⚠ Format failed for {fr['range']}: {str(e2)[:80]}")
    except Exception as e:
        print(f"    ⚠ batch_format failed: {str(e)[:120]}")


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

    # Shared state + locks for parallel processing.
    # - stats_lock: serializes all stats dict mutations (Counters, nested dicts)
    # - pending_lock: serializes pending_rows list mutation + sheet flush
    #   (using one lock for both ensures the flush snapshot is consistent)
    # - print_lock: serializes per-post output blocks so each post's logs
    #   stay roughly contiguous (some interleaving still possible at the
    #   start of a worker, but the bulk of each post's output prints atomically)
    pending_rows = []
    state = {'next_row': next_row}  # mutable container so workers can update
    stats_lock = threading.Lock()
    pending_lock = threading.Lock()
    print_lock = threading.Lock()

    def process_one_queue_row(idx_and_row):
        """Worker function. Each call processes one queue row independently.
        Buffers per-post output and prints it atomically at end."""
        i, queue_row = idx_and_row
        pid = queue_row.get('post_id', '').strip()
        if not pid:
            with stats_lock:
                stats['skip_reasons']['empty_post_id_in_queue'] += 1
            return

        if pid in existing_pids:
            with print_lock:
                print(f"\n[{i}/{len(queue)}] {pid} — already in All_Events, skipping")
            with stats_lock:
                stats['counts']['already_in_all_events'] += 1
                stats['skip_reasons']['already_in_all_events'] += 1
            return

        post = apify_lookup.get(pid)
        if not post:
            with print_lock:
                print(f"\n[{i}/{len(queue)}] {pid} — NOT in any source dataset, skipping")
            with stats_lock:
                stats['counts']['missing_in_apify'] += 1
                stats['skip_reasons']['missing_in_apify'] += 1
            return

        account = post.get('ownerUsername', '?')
        shortcode = post.get('shortCode', '') or post.get('shortcode', '')
        ig_url = f"https://instagram.com/p/{shortcode}/" if shortcode else "(no shortcode)"

        # Buffer per-post output so all of it prints under one print_lock
        # acquisition at the end. process_one_post + tier handlers print
        # internally to stdout — at 2 workers, those interleave somewhat, but
        # the JSON summary is the authoritative record.
        with print_lock:
            print(f"\n[{i}/{len(queue)}] {pid}  @{account}  {ig_url}")

        events, tier_used = process_one_post(ctx, post, max_tier=args.max_tier)

        with stats_lock:
            stats['counts']['processed'] += 1
            stats['tiers_used'][tier_used] += 1

        if not events:
            with print_lock:
                print(f"  ↳ No events extracted for {pid} (tier={tier_used})")
            with stats_lock:
                stats['counts']['no_events'] += 1
                stats['skip_reasons']['gemini_returned_no_events'] += 1
            return

        # Build event-summary lines + new pending rows in local buffers
        out_lines = [f"  ✓ {len(events)} event(s) for {pid} (tier={tier_used})"]
        new_rows = []
        flagged_count = 0
        flag_counts = Counter()
        for idx2, ev in enumerate(events, 1):
            name = ev.get('event_name', '(unnamed)')
            date = ev.get('date', '?')
            venue = ev.get('venue_name', '?') or '—'
            city = ev.get('city', '?') or '—'
            region = ev.get('section_of_nj', '') or '—'
            conf = ev.get('confidence', '?')
            flags = ev.get('quality_flags', '')
            out_lines.append(f"    {idx2}. {name}")
            out_lines.append(f"       date={date}  time={ev.get('start_time','—') or '—'}  venue={venue}  city={city}  region={region}  conf={conf}")
            if flags:
                out_lines.append(f"       🚩 FLAGS: {flags}")
                flagged_count += 1
                for flag in flags.split(','):
                    f = flag.strip()
                    if f:
                        flag_counts[f] += 1
            else:
                out_lines.append(f"       ✓ clean (no flags)")
            new_rows.append(event_to_row(ev, run_tag=args.tag))

        # Print all summary lines together (atomic block)
        with print_lock:
            print('\n'.join(out_lines))

        # Update stats + pending_rows together (under their respective locks)
        with stats_lock:
            stats['counts']['extracted_events'] += len(events)
            stats['counts']['events_with_flags'] += flagged_count
            stats['per_account_events'][account] += len(events)
            stats['flag_distribution'].update(flag_counts)

        # Append new rows + maybe flush. Single lock for both append + flush
        # ensures the next_row snapshot is consistent with what we wrote.
        with pending_lock:
            pending_rows.extend(new_rows)
            if len(pending_rows) >= BATCH_SIZE:
                tw = time.perf_counter()
                batch = list(pending_rows)
                pending_rows.clear()
                start_row = state['next_row']
                append_with_formatting(ws, batch, start_row,
                                       dry_run=args.dry_run, no_format=args.no_formatting)
                with stats_lock:
                    stats['phase_timings']['sheet_write'] += time.perf_counter() - tw
                if not args.dry_run:
                    state['next_row'] = start_row + len(batch)
                time.sleep(BATCH_DELAY_SEC)

    # Drive the work pool. ThreadPoolExecutor handles 1+ workers cleanly
    # — at workers=1, behavior is effectively serial (slight overhead).
    # KeyboardInterrupt is caught so Ctrl+C still flushes pending rows AND
    # writes a (partial) JSON summary marked interrupted=true. Otherwise a
    # killed run loses ALL telemetry and up to BATCH_SIZE in-memory events.
    interrupted = False
    try:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            # Submit all and wait. We use list(map) because we don't need
            # incremental result handling — workers update shared state directly.
            list(pool.map(process_one_queue_row, list(enumerate(queue, 1))))
    except KeyboardInterrupt:
        interrupted = True
        print("\n\n⚠ INTERRUPTED (Ctrl+C) — flushing pending rows and writing partial summary...")

    # Final flush of any leftover rows — runs regardless of interrupt, so
    # the in-memory batch lands in the sheet before we exit
    with pending_lock:
        if pending_rows:
            try:
                tw = time.perf_counter()
                batch = list(pending_rows)
                pending_rows.clear()
                start_row = state['next_row']
                append_with_formatting(ws, batch, start_row,
                                       dry_run=args.dry_run, no_format=args.no_formatting)
                stats['phase_timings']['sheet_write'] += time.perf_counter() - tw
                if interrupted:
                    print(f"  ✓ Flushed {len(batch)} pending events to All_Events before exit")
            except Exception as e:
                print(f"  ⚠ Final flush failed: {e}")

    # ──────────────────────────────────────────────────────────
    # Summary report
    # ──────────────────────────────────────────────────────────
    wall = time.perf_counter() - run_start
    status_banner = "━━━ SUMMARY (INTERRUPTED — PARTIAL) ━━━" if interrupted else "━━━ SUMMARY ━━━"
    print(f"\n{status_banner}")

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
        for tier in TIER_LADDER:
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
    summary_path = write_run_summary(stats, args, wall, run_id, interrupted=interrupted)
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
    p.add_argument("--max-tier",
                   choices=[TIER_FLASH_CAPTION, TIER_FLASH_TEXT, TIER_FLASH_IMAGE, TIER_PRO_IMAGE],
                   default=TIER_PRO_IMAGE,
                   help="Cap tier escalation (default: pro_image — full ladder)")
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
