#!/usr/bin/env python3
"""
Instagram Event Extraction Pipeline
Features: Parallel Processing (10 workers), Config File, Gemini 2.0 Flash Lite,
          Google Sheets Sync, Scheduler, OCR Support, Verbose Logging
"""

import os
import sys
import json
import pandas as pd
import requests
import time
import re
import pickle
import threading
import signal
import atexit
import base64
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# PR F — all timestamps written to Sheets, run logs, and filenames
# use America/New_York wall clock. Previously bare `datetime.now()` ran
# in the Replit container's TZ (UTC), so PROCESSED TIMESTAMP / run_id
# / emergency-save filenames showed UTC times — confusing when the
# rest of the operation (cron schedule, NJ events, user) is on ET.
EAST_TZ = ZoneInfo("America/New_York")

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import recurring_accounts
import audit
# B5: shared extraction logic — sanity checks, region/venue lookups,
# flag→column mapping. Imported under ec_ prefix to avoid collision with
# any same-named locals (e.g., the existing check_* names in legacy code).
# See docs/decisions/0012-extraction-core-foundation.md.
from extraction_core import (
    load_nj_municipalities    as ec_load_nj_municipalities,
    load_venue_canonical      as ec_load_venue_canonical,
    check_date_day_match      as ec_check_date_day_match,
    check_venue_city          as ec_check_venue_city,
    check_region_against_lookup as ec_check_region_against_lookup,
    check_low_confidence      as ec_check_low_confidence,
    check_missing_date        as ec_check_missing_date,
    check_date_sanity         as ec_check_date_sanity,
    check_calendar_low_events as ec_check_calendar_low_events,
    check_carousel_low_events as ec_check_carousel_low_events,
    check_ocr_rich_low_events as ec_check_ocr_rich_low_events,
    check_no_grounding        as ec_check_no_grounding,
    # Round 2 (2026-06) flag detectors. See extraction_core for rationale.
    check_venue_is_handle         as ec_check_venue_is_handle,
    check_venue_equals_event_name as ec_check_venue_equals_event_name,
    check_venue_equals_account    as ec_check_venue_equals_account,
    check_personal_post_signals   as ec_check_personal_post_signals,
    check_no_event_signals        as ec_check_no_event_signals,
    check_venue_out_of_nj_region  as ec_check_venue_out_of_nj_region,
    check_venue_truncated         as ec_check_venue_truncated,
    check_event_name_generic      as ec_check_event_name_generic,
    check_caption_only_no_ocr     as ec_check_caption_only_no_ocr,
    check_missing_venue           as ec_check_missing_venue,
    check_missing_city            as ec_check_missing_city,
    check_missing_time            as ec_check_missing_time,
    FLAG_TO_COLUMNS           as ec_FLAG_TO_COLUMNS,
    FLAG_BG_COLOR             as ec_FLAG_BG_COLOR,
    FLAG_TO_COLOR             as ec_FLAG_TO_COLOR,
    FLAG_TO_TEXT_COLOR        as ec_FLAG_TO_TEXT_COLOR,
    # PR B — shared prompt builder; replaces the inline f-string in
    # process_post. Both main.py and events_from_ids.py call this so
    # extractions stay in lockstep.
    build_prompt              as ec_build_prompt,
    # PR I — tier escalation constants. main.py mirrors the same
    # 4-tier ladder events_from_ids.py uses so production cron and
    # recovery tool produce comparable extractions.
    MODEL_FLASH_LITE          as ec_MODEL_FLASH_LITE,
    MODEL_PRO                 as ec_MODEL_PRO,
    TIER_FLASH_CAPTION        as ec_TIER_FLASH_CAPTION,
    TIER_FLASH_IMAGE          as ec_TIER_FLASH_IMAGE,
    TIER_FLASH_TEXT           as ec_TIER_FLASH_TEXT,
    TIER_PRO_IMAGE            as ec_TIER_PRO_IMAGE,
    TIER_LADDER               as ec_TIER_LADDER,
    ESCALATION_FLAGS          as ec_ESCALATION_FLAGS,
)

from worker_log import (
    wprint, log_post_context, get_logger,
    get_worker_id, set_buffered_mode, is_buffered_mode,
    post_buffer, print_unbuffered,
)

import outage_watchdog

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%I:%M:%S %p')
logger = get_logger(__name__)

try:
    import google.generativeai as genai
    from google.cloud import vision
    from google.oauth2 import service_account
except ImportError:
    print("Installing dependencies...")
    os.system(f"{sys.executable} -m pip install -q google-generativeai google-cloud-vision pandas gspread oauth2client openpyxl requests")
    import google.generativeai as genai
    from google.cloud import vision
    from google.oauth2 import service_account


def load_configuration():
    config = {
        "schedule_day": "Thursday",
        "schedule_time": "14:00",
        "sheet_name": "Instagram_Events_Master",
        "gemini_model": "gemini-2.0-flash-lite",
        "instagram_data_url": "",
        "max_workers": 10,
        "rate_limit_delay": 0.5,
        "history_max_age_days": 30,
        "apify_enabled": True,
        "apify_posts_per_profile": 25,
        "apify_newer_than_days": 21,
    }

    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                file_config = json.load(f)
                config.update(file_config)
            print("✓ Loaded settings from config.json")
        except Exception as e:
            print(f"⚠ Error loading config.json: {e}")

    env_overrides = {
        "SCHEDULE_DAY": "schedule_day",
        "SCHEDULE_TIME": "schedule_time",
        "DATA_URL": "instagram_data_url",
        "MAX_WORKERS": "max_workers",
        "RATE_LIMIT_DELAY": "rate_limit_delay",
    }
    for env_key, conf_key in env_overrides.items():
        val = os.environ.get(env_key)
        if val:
            config[conf_key] = val

    config["max_workers"] = min(20, max(1, int(config.get("max_workers", 10))))
    config["rate_limit_delay"] = max(0.1, float(config.get("rate_limit_delay", 0.5)))

    return config

CONF = load_configuration()

# ─────────────────────────────────────────────────────────────
# PR H — Scratch-sheet support.
# When --scratch is passed on the CLI (or SCRATCH_MODE=1 in env,
# or scratch_mode: true in config.json), main.py redirects ALL
# sheet reads/writes to an alternate spreadsheet whose name is
# CONF["scratch_sheet_name"] (default: "Instagram_Events_Scratch").
#
# Purpose: validate risky changes (tier escalation backport,
# prompt experiments, schema migrations) against a sandbox sheet
# before flipping production. The scratch sheet must exist and
# be shared with the service account — main.py will not create
# spreadsheets, only worksheet TABS inside an existing sheet.
#
# Loud banner is intentional: confusing your sandbox with prod
# is the failure mode this is designed to prevent.
# ─────────────────────────────────────────────────────────────
SCRATCH_MODE = (
    "--scratch" in sys.argv
    or os.environ.get("SCRATCH_MODE", "").strip() == "1"
    or bool(CONF.get("scratch_mode", False))
)
if SCRATCH_MODE:
    scratch_name = CONF.get("scratch_sheet_name", "Instagram_Events_Scratch")
    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  ⚠  SCRATCH MODE ENABLED  ⚠                                  ║")
    print("║                                                              ║")
    print(f"║  All sheet writes redirected to: {scratch_name:<24}     ║")
    print("║  Production 'Instagram_Events_Master' will NOT be touched.   ║")
    print("║                                                              ║")
    print("║  To disable, remove --scratch flag and SCRATCH_MODE env var. ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()
    CONF["sheet_name"] = scratch_name

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") or os.environ.get("SERVICE_ACCOUNT_FILE") or "apt-mark-468506-u9-ec44cabc7335 copy.json"

# ─────────────────────────────────────────────────────────────
# Active retry queue (added 2026-05-29).
# Posts marked ocr_failed or gemini_error in Processed_Log were previously
# only re-attempted if Apify happened to re-scrape them — and with
# apify_newer_than_days=14, posts age out of the scrape window before
# they get retried. The retry queue closes this gap: at startup we pull
# every retry-eligible pid from Processed_Log, look up its raw payload
# from outputs/apify_raw_*.json (or apify_cache/), and merge those posts
# back into the work list alongside the new scrape.
#
# RETRY_MAX_ATTEMPTS — after this many failed attempts, the post gets
#   tagged 'permanently_failed' and drops out of the queue.
# RETRY_ELIGIBLE_TAGS — only these result tags trigger re-processing.
#   Matches the existing retry_tags set in setup_sheets.
# STALE_RAW_DAYS — apify_raw_*.json files older than this almost certainly
#   have dead CDN image URLs (IG CDN URLs expire within ~2wk). When a
#   retry post's only available payload is stale, we set the
#   __caption_only_retry__ flag and skip OCR — caption-only Gemini still
#   catches text-described events.
# ─────────────────────────────────────────────────────────────
RETRY_MAX_ATTEMPTS  = 3
RETRY_ELIGIBLE_TAGS = {'ocr_failed', 'gemini_error'}
STALE_RAW_DAYS      = 10


class _TeeOutput:
    """Forward writes to multiple streams. Used to mirror stdout/stderr to a
    per-run log file so post-mortem forensics can read what the run actually
    printed without needing access to Replit's live console buffer."""
    def __init__(self, *streams):
        self._streams = streams

    def write(self, msg):
        for s in self._streams:
            try:
                s.write(msg)
                s.flush()
            except Exception:
                pass

    def flush(self):
        for s in self._streams:
            try:
                s.flush()
            except Exception:
                pass

    def isatty(self):
        try:
            return self._streams[0].isatty()
        except Exception:
            return False


class InstagramEventPipeline:
    def __init__(self):
        self.processed_posts = set()
        self.post_results = {}
        self.results = []
        self.failed_ocr = []
        self.successful_ocr = []
        # Active retry queue state (see RETRY_* constants at module top).
        # retry_attempts: pid -> prior retry_count read from Processed_Log
        #   (0 if no Retry Count column yet). Incremented at outcome time.
        # retry_caption_only: pids whose only available payload is from a
        #   stale apify_raw dump → skip OCR, send caption directly to Gemini.
        # retry_metadata: pid -> {prior_result, post_date} so the stale
        #   detector can decide retry strategy without re-reading the sheet.
        self.retry_attempts = {}
        self.retry_caption_only = set()
        self.retry_metadata = {}

        # Per-worker telemetry. Keyed by worker_id (W1, W2, ...) → Counter
        # of result tags. Lets the final report surface "W3 had 40% gemini
        # errors while everyone else had 2%" — invisible in aggregate stats
        # but trivial here. Always-on; the overhead is one dict lookup +
        # one counter increment per processed post.
        from collections import Counter, defaultdict
        self.worker_stats = defaultdict(Counter)
        self.sheets_client = None
        self.main_sheet = None
        self.log_worksheet = None
        self.output_dir = Path("outputs")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_file = self.output_dir / 'pipeline_checkpoint.pkl'
        self.lock = threading.Lock()
        self.vision_client = None
        self.vision_enabled = False
        self.max_workers = CONF["max_workers"]
        self.rate_limit_delay = CONF["rate_limit_delay"]

        self.run_id = datetime.now(EAST_TZ).strftime('%Y%m%d_%H%M%S')
        self._setup_run_log()

        # Incremental Processed_Log flushing — see ADR 0010.
        # post_results entries get flushed to the Sheet's Processed_Log tab
        # periodically during the run (every PL_FLUSH_THRESHOLD new entries)
        # so that if the run is killed before save_data() completes, the
        # dedup work that DID happen is preserved. Without this, every
        # killed run loses 100% of its dedup state and re-processes all
        # those posts on the next run.
        self._flushed_pids = set()
        self._flush_lock = threading.Lock()
        self.PL_FLUSH_THRESHOLD = 50

        # Index of the next entry in self.results that hasn't yet been
        # flushed to the All_Events sheet. Mirrors _flushed_pids for the
        # Processed_Log → keeps event writes in lockstep with dedup-tag
        # writes so a kill-mid-run never produces "marked done but not
        # actually saved" orphans (the failure mode we recovered today).
        self._events_flushed_count = 0
        self.EVENTS_FLUSH_THRESHOLD = 25

        self.stats = {
            'total_posts': 0,
            'processed': 0,
            'skipped_history': 0,
            'skipped_no_data': 0,
            'events_found': 0,
            'posts_with_events': 0,
            'posts_no_events': 0,
            'multi_event_posts': 0,
            'max_events_in_post': 0,
            'calendar_posts': 0,
            'gemini_errors': 0,
            'ocr_success': 0,
            'ocr_failed': 0,
            'download_errors': 0,
            'vision_errors': {},
            'carousel_posts': 0,
            'total_slides_ocrd': 0,
            'slides_with_text': 0,
            # PR I — per-tier usage counters. Each post processed via
            # the tier ladder increments the tier it FINISHED at (after
            # any escalations). Useful for cost forecasting + tracking
            # how often escalation actually kicks in.
            'tiers_used': {
                ec_TIER_FLASH_CAPTION: 0,
                ec_TIER_FLASH_IMAGE: 0,
                ec_TIER_FLASH_TEXT: 0,
                ec_TIER_PRO_IMAGE: 0,
            },
        }

        if not GEMINI_API_KEY:
            print("❌ ERROR: GEMINI_API_KEY not found in Secrets!")
        else:
            genai.configure(api_key=GEMINI_API_KEY)

        # PR I — Tier escalation in main.py.
        # Default ON (matches events_from_ids.py behavior). Set
        # tier_escalation_enabled: false in config.json to revert to
        # single-model behavior using CONF["gemini_model"].
        self.tier_escalation_enabled = bool(CONF.get("tier_escalation_enabled", True))

        print(f"\n{'='*60}")
        print(f" INSTAGRAM EVENT EXTRACTION PIPELINE")
        print(f"{'='*60}")
        if self.tier_escalation_enabled:
            # Tier ladder uses Flash-Lite for cheap tiers, Pro lazily
            # for the last-resort tier. The user's gemini_model config
            # is intentionally ignored here — tier escalation defines
            # its own model assignments per tier.
            self.gemini_flash_lite = genai.GenerativeModel(ec_MODEL_FLASH_LITE)
            self.gemini_pro = None  # lazy init via _setup_pro_model
            print(f"  • AI Model: Tier ladder (Flash-Lite → Pro)")
        else:
            # Legacy single-shot path — used when tier escalation is
            # explicitly disabled. Honors the user's gemini_model setting.
            model_name = CONF["gemini_model"]
            self.gemini_flash_lite = genai.GenerativeModel(model_name)
            self.gemini_pro = None
            print(f"  • AI Model: {model_name} (tier escalation disabled)")
        # Legacy alias for any older callers that referenced self.gemini_model
        self.gemini_model = self.gemini_flash_lite
        print(f"  • Parallel Workers: {self.max_workers}")
        print(f"  • Rate Limit Delay: {self.rate_limit_delay}s")

        self.setup_vision()

        # ─────────────────────────────────────────────────────────────
        # PR C — eager safety-net lookup load.
        # Previously these were lazy-loaded on the FIRST post processed.
        # If a data file was missing or malformed, the first post would
        # raise — and the outer 'Gemini error' handler would silently
        # mark it as gemini_error. Then every subsequent post would try
        # to load again (hasattr check would still be False), hit the
        # same error, and silently fail. Whole run would produce zero
        # events with no obvious indication why.
        #
        # Loading here at startup means a missing/broken data file
        # crashes the process IMMEDIATELY with a clear message.
        # ─────────────────────────────────────────────────────────────
        try:
            self._safety_net_region_lookup = ec_load_nj_municipalities()
            self._safety_net_venue_lookup = ec_load_venue_canonical()
            print(f"  • Safety net lookups: "
                  f"{len(self._safety_net_region_lookup)} NJ cities, "
                  f"{len(self._safety_net_venue_lookup)} canonical venues")
        except Exception as e:
            print(f"❌ FATAL: could not load safety net lookups: {e}")
            print(f"   Check that data/nj_municipalities.json and "
                  f"data/venue_city_canonical.json exist and are valid JSON.")
            sys.exit(1)

        atexit.register(self.emergency_save)
        atexit.register(self._write_anomaly_summary)
        signal.signal(signal.SIGINT, self.handle_interrupt)

    def _setup_run_log(self):
        """Tee stdout/stderr to outputs/run_<run_id>.log so a full record of
        what this run printed survives past the live shell session. Console
        output is unaffected."""
        try:
            self._run_log_path = self.output_dir / f'run_{self.run_id}.log'
            self._run_log_fh = open(self._run_log_path, 'w', buffering=1)
            sys.stdout = _TeeOutput(sys.__stdout__, self._run_log_fh)
            sys.stderr = _TeeOutput(sys.__stderr__, self._run_log_fh)
        except Exception as e:
            self._run_log_fh = None
            print(f"⚠ Could not open run log file: {e}")

    def _record_post_outcome(self, pid, result, user, post_date_str,
                             caption='', ocr_text='', gemini_raw='', error=''):
        """Single source of truth for marking a post processed and recording
        what happened to it. For anomalous outcomes (anything except
        events_found), captures preview text + reason so the post-run
        anomaly file lets you audit silent misses without re-scraping.

        Acquires self.lock then delegates to _record_post_outcome_locked.
        Callers that already hold self.lock should call _locked directly
        — see process_post's atomic outcome+extend block (PR A)."""
        with self.lock:
            self._record_post_outcome_locked(pid, result, user, post_date_str,
                                              caption, ocr_text, gemini_raw, error)

    def _record_post_outcome_locked(self, pid, result, user, post_date_str,
                                    caption='', ocr_text='', gemini_raw='', error=''):
        """Lock-free variant. ASSUMES caller holds self.lock. Used by
        process_post to combine outcome recording with results.extend
        in a single atomic lock block — without this pairing, a
        periodic flush firing between record_outcome and results.extend
        could snapshot post_results WITHOUT the matching events,
        creating orphans (PR A: orphan hardening)."""
        self.processed_posts.add(pid)

        # Retry-queue accounting. If this pid had a prior retry count loaded
        # from Processed_Log:
        #   • a successful outcome (events_found / no_events_found) ends the
        #     retry cycle — we still write retry_count so the row reflects
        #     how many attempts it took, but the result is terminal.
        #   • a re-failure increments the count. If it hits RETRY_MAX_ATTEMPTS
        #     we tag it permanently_failed so the next setup_sheets skips it.
        prior_retries = self.retry_attempts.get(pid, 0)
        new_retry_count = prior_retries
        if pid in self.retry_attempts:
            if result in RETRY_ELIGIBLE_TAGS:
                new_retry_count = prior_retries + 1
                if new_retry_count >= RETRY_MAX_ATTEMPTS:
                    result = 'permanently_failed'
            # If result is a success or terminal (events_found, no_events_found,
            # permanently_failed) leave new_retry_count as prior_retries — the
            # row records how many attempts came before this one.

        # Provenance: which worker thread handled this attempt, in which run,
        # on which attempt number. Persisted on every Processed_Log row AND
        # every All_Events row so a flagged cell can be traced back to the
        # exact worker/run/attempt that produced it. attempt_id is 1-based:
        # first time we see this pid this is attempt 1; first retry is 2; etc.
        worker_id = get_worker_id()
        attempt_id = prior_retries + 1
        self.worker_stats[worker_id][result] += 1

        entry = {
            'result': result,
            'account': user,
            'post_date': post_date_str,
            'retry_count': new_retry_count,
            'worker_id': worker_id,
            'run_id': self.run_id,
            'attempt_id': attempt_id,
        }
        if result != 'events_found':
            if caption:
                entry['caption_preview'] = str(caption)[:400]
            if ocr_text:
                entry['ocr_preview'] = str(ocr_text)[:400]
            if gemini_raw:
                entry['gemini_raw'] = str(gemini_raw)[:600]
            if error:
                entry['error'] = str(error)[:300]
            entry['post_url'] = f"https://www.instagram.com/p/{pid}/"
        self.post_results[pid] = entry

    def _safe_check(self, name, fn, *args):
        """PR C — guard each sanity check against unexpected exceptions.
        Previously, if any ec_check_* raised on a malformed event dict,
        the exception bubbled up past the entire sanity-check block and
        was swallowed by the outer 'Gemini error' handler — which marked
        the post as gemini_error and DISCARDED the events Gemini had
        already extracted. That's a silent data-loss bug.

        Now: log the exception with check name + pid context, return
        None for THAT check, and let the remaining checks run normally.
        Worst case becomes 'this single flag not applied' rather than
        'whole post lost'."""
        try:
            return fn(*args)
        except Exception as e:
            print(f"    ⚠ Sanity check {name!r} raised: "
                  f"{type(e).__name__}: {str(e)[:120]}")
            return None

    def _flush_processed_log(self, force=False):
        """Append any post_results entries that haven't been written to
        Processed_Log yet. Called periodically during the run + once at
        the end of save_data() with force=True.

        Called frequently (potentially after each processed post). The
        threshold check makes most calls cheap — only fires the API call
        when there's enough pending work or force=True.

        Thread safety:
        - self._flush_lock serializes flush attempts so we don't double-write
        - self.lock guards the snapshot read of self.post_results and the
          subsequent update of self._flushed_pids
        - Worker threads can keep adding to post_results during the network
          call; they're picked up in the next flush.
        """
        if not self.sheets_client or not self.log_worksheet:
            return

        # ─────────────────────────────────────────────────────────────
        # PR A — ORDER MATTERS: events to All_Events FIRST, then
        # Processed_Log. The old order (PL first, events second) meant
        # a kill between them left orphans (PL marked done, AE empty).
        # By writing events first, the worst-case failure is "events
        # in AE but no PL entry" — recoverable on next cron because
        # the post isn't yet in PL's dedup set; that's preferable to
        # orphans, which silently lose data.
        # ─────────────────────────────────────────────────────────────
        events_ok = self._flush_events_to_sheet(force=force)
        if not events_ok:
            # Events flush failed — abort PL flush so we retry both
            # together on the next call. Logging is handled inside
            # _flush_events_to_sheet.
            return

        with self._flush_lock:
            # Snapshot pending entries under the main lock
            with self.lock:
                pending = {
                    pid: dict(info) for pid, info in self.post_results.items()
                    if pid not in self._flushed_pids
                }

            if not pending:
                return
            if not force and len(pending) < self.PL_FLUSH_THRESHOLD:
                return

            date_processed = str(datetime.now(EAST_TZ).date())
            # Source column carries structured provenance now ("Auto-Bot" was
            # branding, not data). Format: "{run_id}:{worker_id}:a{attempt}".
            # Example: "20260529_142233:W3:a2" — Run timestamp, worker that
            # processed it, 2nd attempt. Older "Auto-Bot" rows from prior
            # runs are untouched; new rows just have richer Source values.
            log_rows = []
            for pid, info in pending.items():
                run_id = info.get('run_id', self.run_id)
                worker_id = info.get('worker_id', 'main')
                attempt_id = info.get('attempt_id', 1)
                source = f"{run_id}:{worker_id}:a{attempt_id}"
                log_rows.append([
                    pid, info.get('account', ''), date_processed, source, "",
                    info.get('result', ''), info.get('post_date', ''),
                    str(info.get('retry_count', 0)),
                    run_id, worker_id, str(attempt_id),
                ])

            try:
                self.log_worksheet.append_rows(log_rows)
                # Mark these pids as flushed so we don't re-write next call
                with self.lock:
                    self._flushed_pids.update(pending.keys())
                if force:
                    print(f"  ✓ Flushed {len(pending)} entries to Processed_Log (final)")
            except Exception as e:
                # Don't crash the pipeline on a flush failure; the next flush
                # (or save_data's force=True final call) will retry these.
                print(f"  ⚠ Processed_Log flush error ({len(pending)} pending): {e}")

    def _flush_events_to_sheet(self, force=False):
        """Append any new entries in self.results to All_Events.

        Mirrors _flush_processed_log's pattern but for the actual event
        rows. self._events_flushed_count tracks how far we've written.
        Threshold-gated to avoid an API call per event; force=True ignores
        the threshold for the end-of-run flush from save_data().

        Same row-build + sanitize logic as save_data() so columns match.
        Per-cell quality-flag highlighting is applied after each batch
        via _apply_quality_flag_formatting (same as save_data path).

        Returns:
          True  — succeeded, OR no-op (nothing to flush / threshold not met)
          False — Sheet write failed; caller should NOT proceed to mark
                  PL entries flushed (PR A — events go first, then PL)."""
        if not self.sheets_client or not self.main_sheet:
            return True  # no-op = no failure

        with self._flush_lock:
            # Snapshot pending events under main lock
            with self.lock:
                start_idx = self._events_flushed_count
                pending_events = self.results[start_idx:]

            if not pending_events:
                return True  # nothing to flush, success
            if not force and len(pending_events) < self.EVENTS_FLUSH_THRESHOLD:
                return True  # threshold not met, treat as success no-op

            # Build DataFrame for just the pending events. Reuse the same
            # column list + uppercase + sanitize logic as save_data().
            try:
                df = pd.DataFrame(pending_events)
            except Exception as e:
                print(f"  ⚠ Could not build DataFrame for incremental flush: {e}")
                return False

            cols = [
                'instagram_handle', 'event_name', 'date', 'start_time',
                'venue_name', 'city', 'section_of_nj', 'newsletter_description',
                'instagram_post_url', 'display_url', 'post_url', 'instagram_profile_url',
                'event_type', 'account_name', 'description', 'performer', 'price',
                'confidence', 'post_id', 'had_ocr', 'from_calendar', 'is_recurring',
                'processed_timestamp', 'quality_flags', 'recurrence_pattern',
                # Provenance — matches the Processed_Log Run/Worker/Attempt
                # columns for cross-tab joins. A flagged cell can now be
                # traced via (run_id, worker_id, attempt_id) → the specific
                # worker/attempt that produced it.
                'run_id', 'worker_id', 'attempt_id',
            ]
            df['processed_timestamp'] = datetime.now(EAST_TZ).strftime('%Y-%m-%d %H:%M:%S')
            for c in cols:
                if c not in df.columns:
                    df[c] = ""
            df = df[[c for c in cols if c in df.columns]]

            url_columns = {'instagram_post_url', 'display_url', 'post_url', 'instagram_profile_url'}
            skip_upper = url_columns | {'processed_timestamp'}
            for col_name in df.columns:
                if col_name not in skip_upper:
                    df[col_name] = df[col_name].map(lambda x: str(x).upper() if isinstance(x, str) else x)
            df.columns = [c.upper().replace('_', ' ') for c in df.columns]

            def sanitize(val):
                if val is None:
                    return ""
                if isinstance(val, float):
                    import math
                    if math.isnan(val) or math.isinf(val):
                        return ""
                if isinstance(val, list):
                    return ", ".join(str(v) for v in val)
                if isinstance(val, dict):
                    return str(val)
                return val

            try:
                evt_sheet = self.main_sheet.worksheet("All_Events")
                # Existing-tab path: if the tab was created under the prior
                # 25-col schema and we're now appending 28-col rows (the
                # provenance migration), explicitly widen first. Same reason
                # as the Processed_Log resize above — auto-extend is
                # library-version-dependent, so we make it deterministic.
                try:
                    target_cols = len(df.columns)
                    if evt_sheet.col_count < target_cols:
                        prev = evt_sheet.col_count
                        evt_sheet.resize(cols=target_cols)
                        print(f"  ↳ Widened All_Events: {prev} → {target_cols} cols")
                except Exception as e:
                    print(f"  ⚠ Could not pre-resize All_Events (will retry append anyway): {e}")

                # Header row refresh: when the schema added columns at the
                # end (RUN ID / WORKER ID / ATTEMPT ID at Z/AA/AB), existing
                # sheets kept their old 25-col header — Z/AA/AB had data but
                # no labels. Rewrite row 1 to match the current df.columns
                # whenever the live header doesn't already match.
                #
                # Cache after first successful pass so we don't re-read row
                # 1 on every subsequent EVENTS_FLUSH_THRESHOLD-sized flush
                # (1 API call * many flushes = wasted quota). Reset on
                # process restart, which is what we want — confirms once
                # per run.
                #
                # current[:len(expected)] is a slice, not a pad — when
                # current is shorter than expected (e.g., 25 vs 28), the
                # slice returns the 25-element list, which won't equal the
                # 28-element expected, so the rewrite triggers correctly.
                if not getattr(self, '_all_events_header_verified', False):
                    try:
                        expected = [str(c) for c in df.columns]
                        current  = [str(v) for v in evt_sheet.row_values(1)]
                        if current[:len(expected)] != expected:
                            evt_sheet.update(
                                range_name='A1',
                                values=[expected],
                                value_input_option='USER_ENTERED',
                            )
                            print(f"  ↳ Refreshed All_Events header row ({len(expected)} cols)")
                        self._all_events_header_verified = True
                    except Exception as e:
                        print(f"  ⚠ Could not refresh All_Events header: {e}")
            except gspread.WorksheetNotFound:
                # Width bumped from 27 → 30 to accommodate the new RUN ID /
                # WORKER ID / ATTEMPT ID provenance columns at the right edge.
                evt_sheet = self.main_sheet.add_worksheet("All_Events", 5000, 30)
                evt_sheet.append_row(df.columns.tolist())

            # Track row position for per-cell highlighting AFTER append
            start_row = len(evt_sheet.col_values(1)) + 1
            rows = [[sanitize(v) for v in row] for row in df.values.tolist()]

            try:
                evt_sheet.append_rows(rows)
                # Mark these events as flushed AFTER successful sheet write
                with self.lock:
                    self._events_flushed_count = start_idx + len(pending_events)
                if force:
                    print(f"  ✓ Flushed {len(pending_events)} events to All_Events (final)")

                # Apply per-cell quality-flag highlighting on the just-written rows
                try:
                    self._apply_quality_flag_formatting(evt_sheet, df, start_row)
                except Exception as fmt_e:
                    print(f"  ⚠ Quality-flag formatting failed (non-fatal): {fmt_e}")
                return True
            except Exception as e:
                # Don't crash; next flush retries (start_idx unchanged)
                print(f"  ⚠ All_Events flush error ({len(pending_events)} pending): {e}")
                return False

    def _note_apify_shell_record(self, post, post_num):
        """Track an Apify shell record (no id/shortCode = account returned no
        posts). Aggregated for the per-run report and the anomaly summary.

        Inferring the affected account is best-effort: shell records have
        an empty ownerUsername but the inputUrl field usually points at
        the profile, e.g. 'https://www.instagram.com/dj_peters42'."""
        with self.lock:
            self.stats.setdefault('apify_shell_records', 0)
            self.stats['apify_shell_records'] += 1
            # Best-effort extraction of the account name from the inputUrl
            input_url = (post.get('inputUrl') or post.get('url') or '').strip()
            account = ''
            if input_url:
                # Strip query/fragment, take last non-empty path segment
                u = input_url.split('?', 1)[0].split('#', 1)[0].rstrip('/')
                account = u.rsplit('/', 1)[-1] if '/' in u else u
            if not hasattr(self, '_shell_accounts'):
                self._shell_accounts = {}
            self._shell_accounts[account] = self._shell_accounts.get(account, 0) + 1

    def _write_anomaly_summary(self):
        """Dump per-post outcomes for everything that did NOT produce events,
        plus per-account scrape→extract counts. Runs at process exit (atexit)
        so it captures the state even on crash or Ctrl-C. Filename mirrors
        run_id so log/anomalies/raw-Apify files share a timestamp."""
        try:
            if not self.post_results:
                return
            out_path = self.output_dir / f'anomalies_{self.run_id}.json'
            anomalies = {pid: r for pid, r in self.post_results.items()
                         if r.get('result') != 'events_found'}
            per_account = {}
            for r in self.post_results.values():
                acct = r.get('account', '')
                bucket = per_account.setdefault(acct, {
                    'scraped': 0, 'events_found': 0,
                    'no_events_found': 0, 'gemini_error': 0, 'ocr_failed': 0,
                })
                bucket['scraped'] += 1
                bucket[r.get('result', 'unknown')] = bucket.get(r.get('result', 'unknown'), 0) + 1
            summary = {
                'run_id': self.run_id,
                'totals': {k: v for k, v in self.stats.items()
                           if not isinstance(v, dict)},
                'per_account': per_account,
                'anomalies': anomalies,
                'apify_shell_accounts': dict(getattr(self, '_shell_accounts', {})),
            }
            with open(out_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            self._check_account_regressions(per_account)
            sys.__stdout__.write(f"\n📋 Anomaly summary: {out_path} "
                                 f"({len(anomalies)} anomalous posts logged)\n")
        except Exception as e:
            sys.__stdout__.write(f"\n⚠ Failed to write anomaly summary: {e}\n")

    def _check_account_regressions(self, per_account):
        """Compare this run's per-account event counts against the most recent
        prior Events_*.csv. Flag any account that produced events in the
        previous run but produced zero this run — common signal of a typo,
        suspended account, or extraction regression."""
        try:
            import glob
            prior = sorted(glob.glob(str(self.output_dir / 'Events_*.csv')))
            prior = [p for p in prior if self.run_id not in os.path.basename(p)]
            if not prior:
                return
            prev_counts = {}
            with open(prior[-1], 'r', encoding='utf-8', errors='replace') as f:
                import csv as _csv
                rdr = _csv.DictReader(f)
                handle_col = next((c for c in (rdr.fieldnames or [])
                                   if c.lower().replace(' ', '_') == 'instagram_handle'), None)
                if not handle_col:
                    return
                for row in rdr:
                    h = (row.get(handle_col) or '').strip().lower()
                    if h:
                        prev_counts[h] = prev_counts.get(h, 0) + 1
            regressions = []
            for h, prev_n in prev_counts.items():
                cur = per_account.get(h, {})
                if prev_n >= 2 and cur.get('events_found', 0) == 0 and cur.get('scraped', 0) > 0:
                    regressions.append((h, prev_n))
            if regressions:
                sys.__stdout__.write(f"\n⚠ POSSIBLE REGRESSIONS — accounts that produced "
                                     f"events last run but zero events this run:\n")
                for h, n in sorted(regressions, key=lambda x: -x[1])[:20]:
                    sys.__stdout__.write(f"    {h:32s} (had {n} events last run)\n")
                if len(regressions) > 20:
                    sys.__stdout__.write(f"    ... and {len(regressions) - 20} more\n")
        except Exception as e:
            sys.__stdout__.write(f"⚠ Regression check failed: {e}\n")

    def handle_interrupt(self, signum, frame):
        print("\n\n⚠ INTERRUPT DETECTED - Saving all data...")
        self.emergency_save()
        # PR A — force-flush in-memory data to Sheets before exit.
        # Without this, Ctrl-C between periodic flushes leaves whatever's
        # in self.results since the last flush as local CSV only. Next
        # cron run wouldn't see those events on the sheet, and any
        # already-flushed Processed_Log entries WITHOUT matching events
        # would become orphans.
        try:
            self._flush_processed_log(force=True)
        except Exception as e:
            print(f"  ⚠ Final flush during interrupt failed: {e}")
        self.create_final_report()
        sys.exit(0)

    def emergency_save(self):
        with self.lock:
            if self.results:
                ts = datetime.now(EAST_TZ).strftime('%Y%m%d_%H%M%S')
                df = pd.DataFrame(self.results)
                csv_file = self.output_dir / f'emergency_events_{ts}.csv'
                df.to_csv(csv_file, index=False)
                print(f"✓ Emergency CSV saved: {csv_file}")
                try:
                    excel_file = self.output_dir / f'emergency_events_{ts}.xlsx'
                    df.to_excel(excel_file, index=False)
                    print(f"✓ Emergency Excel saved: {excel_file}")
                except Exception:
                    pass
                stats_file = self.output_dir / f'emergency_stats_{ts}.json'
                with open(stats_file, 'w') as f:
                    json.dump(self.stats, f, indent=2)
                self.save_checkpoint()

    def setup_vision(self):
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print("  • Vision API (OCR): DISABLED (no service account file)")
            return

        try:
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=['https://www.googleapis.com/auth/cloud-vision']
            )
            self.vision_client = vision.ImageAnnotatorClient(credentials=credentials)
            self.vision_enabled = True
            print("  • Vision API (OCR): ENABLED")
        except Exception as e:
            print(f"  • Vision API (OCR): FAILED - {e}")

    def setup_sheets(self):
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print("⚠ No service account file found. Running in offline mode.")
            return

        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
            self.sheets_client = gspread.authorize(creds)
            sheet_name = CONF["sheet_name"]

            try:
                self.main_sheet = self.sheets_client.open(sheet_name)
                print(f"✓ Connected to Sheet: {sheet_name}")
            except gspread.SpreadsheetNotFound:
                print(f"❌ Error: Could not find Google Sheet named '{sheet_name}'")
                return

            try:
                self.log_worksheet = self.main_sheet.worksheet("Processed_Log")
                all_rows = self.log_worksheet.get_all_values()
                header = all_rows[0] if all_rows else []
                data_rows = all_rows[1:] if len(all_rows) > 1 else []

                default_header = [
                    "Post ID", "Account", "Date Processed", "Source", "Notes",
                    "Result", "Post Date", "Retry Count",
                    # Provenance columns — every row records exactly which
                    # run/worker/attempt produced it. Adding columns at the
                    # right edge is safe: existing audit scripts index by
                    # header name (.index("Result")) so positions don't shift.
                    "Run ID", "Worker", "Attempt",
                ]
                updated_header = list(header)
                needs_update = False
                if not updated_header or "Post ID" not in updated_header:
                    updated_header = default_header
                    needs_update = True
                else:
                    if "Account" not in updated_header:
                        updated_header.insert(1, "Account")
                        needs_update = True
                    if "Result" not in updated_header:
                        updated_header.append("Result")
                        needs_update = True
                    if "Post Date" not in updated_header:
                        updated_header.append("Post Date")
                        needs_update = True
                    if "Retry Count" not in updated_header:
                        updated_header.append("Retry Count")
                        needs_update = True
                    if "Run ID" not in updated_header:
                        updated_header.append("Run ID")
                        needs_update = True
                    if "Worker" not in updated_header:
                        updated_header.append("Worker")
                        needs_update = True
                    if "Attempt" not in updated_header:
                        updated_header.append("Attempt")
                        needs_update = True
                if needs_update:
                    # Explicit width-resize BEFORE the header write. Without
                    # this, calling .update() with a header wider than the
                    # current grid relies on gspread's auto-extend behavior,
                    # which depends on library version + endpoint. For
                    # Processed_Log going 8 → 11 cols (the v2 schema migration
                    # this commit ships), we make the resize explicit so
                    # the first run after a pull never fails with
                    # "exceeds grid limit".
                    try:
                        current_cols = self.log_worksheet.col_count
                        target_cols = len(updated_header)
                        if current_cols < target_cols:
                            self.log_worksheet.resize(cols=target_cols)
                            print(f"  ↳ Widened Processed_Log: {current_cols} → {target_cols} cols")
                    except Exception as e:
                        print(f"  ⚠ Could not pre-resize Processed_Log (will retry update anyway): {e}")
                    self.log_worksheet.update([updated_header], value_input_option='RAW')
                    print(f"✓ Updated Processed_Log header: {updated_header}")

                read_header = header if header else updated_header
                result_col = read_header.index("Result") if "Result" in read_header else None
                post_date_col = read_header.index("Post Date") if "Post Date" in read_header else None
                date_processed_col = read_header.index("Date Processed") if "Date Processed" in read_header else 1
                retry_count_col = read_header.index("Retry Count") if "Retry Count" in read_header else None

                _DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
                acct_in_col1 = "Account" in updated_header and updated_header.index("Account") == 1
                OLD_RESULT_COL, OLD_POST_DATE_COL, OLD_DATE_PROCESSED_COL = 4, 5, 1

                # history_max_age_days is intended for live Apify auto-runs (Mode 1)
                # only. In Mode 2 (apify_enabled: false), the pipeline reads a fixed
                # static instagram_data_url, so applying the age cutoff causes the same
                # posts to drop out of dedup every week and re-process — generating
                # duplicate rows in All_Events. See docs/decisions/0005-config-mode-scoping.md.
                apify_live = bool(CONF.get("apify_enabled", False))
                if apify_live:
                    max_age_days = int(CONF.get("history_max_age_days", 30))
                    cutoff = (datetime.now(EAST_TZ) - timedelta(days=max_age_days)).date()
                else:
                    cutoff = None  # Mode 2: keep full Processed_Log dedup history

                retry_tags = RETRY_ELIGIBLE_TAGS
                # Build a latest-per-pid view first. Append-only Processed_Log
                # may now contain multiple rows for the same post (each retry
                # attempt appends a new row), so the *last* row in iteration
                # order is the current state of that pid. Without this, an
                # earlier successful retry would be shadowed by an earlier
                # ocr_failed row and force an unnecessary re-process.
                latest_per_pid = {}
                for row in data_rows:
                    if not row:
                        continue
                    pid = row[0].strip() if row else ''
                    if not pid:
                        continue
                    latest_per_pid[pid] = row  # last write wins

                skip_count = 0
                retry_count = 0
                exhausted_count = 0
                for pid, row in latest_per_pid.items():
                    is_old_row = (
                        acct_in_col1
                        and len(row) > 1
                        and _DATE_RE.match(row[1].strip())
                    )
                    _rcol = OLD_RESULT_COL if is_old_row else result_col
                    _pdcol = OLD_POST_DATE_COL if is_old_row else post_date_col
                    _dpcol = OLD_DATE_PROCESSED_COL if is_old_row else date_processed_col

                    result_tag = ''
                    if _rcol is not None and len(row) > _rcol:
                        result_tag = row[_rcol].strip().lower()

                    post_date_str_row = ''
                    if _pdcol is not None and len(row) > _pdcol:
                        post_date_str_row = row[_pdcol].strip()
                    if not post_date_str_row and len(row) > _dpcol:
                        post_date_str_row = row[_dpcol].strip()

                    row_date = None
                    if post_date_str_row:
                        try:
                            row_date = datetime.strptime(post_date_str_row, '%Y-%m-%d').date()
                        except ValueError:
                            pass

                    # Retry Count column may not exist on old rows; default 0.
                    prior_retry_count = 0
                    if retry_count_col is not None and len(row) > retry_count_col:
                        raw_rc = row[retry_count_col].strip()
                        if raw_rc.isdigit():
                            prior_retry_count = int(raw_rc)

                    if cutoff is not None and row_date and row_date < cutoff:
                        self.processed_posts.add(pid)
                        skip_count += 1
                        continue

                    if result_tag in retry_tags:
                        if prior_retry_count >= RETRY_MAX_ATTEMPTS:
                            # Retry budget exhausted → treat as terminal so
                            # the post stops cycling through the queue every
                            # week. _record_post_outcome will tag any new
                            # appearance as permanently_failed.
                            self.processed_posts.add(pid)
                            exhausted_count += 1
                            continue
                        # Eligible for active retry: don't add to processed_posts
                        # so it CAN re-process, and remember the prior count so
                        # the outcome-recording path increments correctly.
                        self.retry_attempts[pid] = prior_retry_count
                        self.retry_metadata[pid] = {
                            'prior_result': result_tag,
                            'post_date': post_date_str_row,
                        }
                        retry_count += 1
                        continue

                    if result_tag == 'permanently_failed':
                        # Already given up on this one; skip without re-trying.
                        self.processed_posts.add(pid)
                        skip_count += 1
                        continue

                    self.processed_posts.add(pid)
                    skip_count += 1

                exhausted_note = f", {exhausted_count} exhausted (>={RETRY_MAX_ATTEMPTS} attempts)" if exhausted_count else ""
                print(f"✓ History Loaded: Skipping {skip_count} IDs, {retry_count} eligible for retry (ocr_failed/gemini_error){exhausted_note}.")
            except gspread.WorksheetNotFound:
                self.log_worksheet = self.main_sheet.add_worksheet("Processed_Log", 5000, 11)
                self.log_worksheet.append_row([
                    "Post ID", "Account", "Date Processed", "Source", "Notes",
                    "Result", "Post Date", "Retry Count",
                    "Run ID", "Worker", "Attempt",
                ])
                print("✓ Created new 'Processed_Log' tab.")

        except Exception as e:
            print(f"⚠ Sheets Connection Failed: {e}")

    def clean_time(self, time_str):
        if not time_str:
            return ""
        try:
            for fmt in ['%H:%M', '%I:%M %p', '%I:%M%p', '%I %p', '%H:%M:%S']:
                try:
                    dt = datetime.strptime(str(time_str).strip().upper(), fmt)
                    formatted = dt.strftime('%I:%M %p')
                    return formatted.lstrip('0')
                except ValueError:
                    continue
        except Exception:
            pass
        return str(time_str).upper()

    def collect_carousel_urls(self, post):
        urls = []
        seen = set()

        def add(url):
            if not url or not isinstance(url, str) or url == 'null':
                return
            try:
                from urllib.parse import urlparse
                key = urlparse(url).path or url
            except Exception:
                key = url
            if key not in seen:
                seen.add(key)
                urls.append(url)

        add(post.get('displayUrl', '') or post.get('display_url', ''))

        for child in post.get('childPosts', []):
            if isinstance(child, dict):
                add(child.get('displayUrl', '') or child.get('display_url', ''))

        for img in post.get('images', []):
            if isinstance(img, str):
                add(img)
            elif isinstance(img, dict):
                add(img.get('url', '') or img.get('displayUrl', ''))

        return urls

    def extract_ocr_text(self, image_url, post_id=""):
        if not self.vision_client or not image_url or image_url == 'null':
            return ""

        wprint(f"    ↳ Downloading image from CDN...")

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
            }
            response = requests.get(image_url, headers=headers, timeout=15)

            if response.status_code != 200:
                wprint(f"    ✗ Image download failed: Status {response.status_code}")
                with self.lock:
                    self.stats['download_errors'] += 1
                    self.stats['ocr_failed'] += 1
                return ""

            wprint(f"    ✓ Image downloaded ({len(response.content)} bytes)")
            wprint(f"    ↳ Calling Vision API for OCR...")

            time.sleep(self.rate_limit_delay)

            image = vision.Image(content=response.content)
            response_ocr = self.vision_client.text_detection(image=image)

            if response_ocr.error.message:
                error_msg = response_ocr.error.message
                wprint(f"    ✗ Vision API error: {error_msg}")
                with self.lock:
                    self.stats['ocr_failed'] += 1
                    if error_msg not in self.stats['vision_errors']:
                        self.stats['vision_errors'][error_msg] = 0
                    self.stats['vision_errors'][error_msg] += 1
                low = error_msg.lower()
                if '403' in error_msg and 'billing' in low:
                    outage_watchdog.record('vision_403_billing', error_msg)
                elif '403' in error_msg:
                    outage_watchdog.record('vision_403', error_msg)
                if 'quota' in low or '429' in error_msg:
                    with self.lock:
                        self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)
                    wprint(f"    ⚠ Rate limited - increasing delay to {self.rate_limit_delay:.1f}s")
                return ""

            if response_ocr.text_annotations:
                ocr_text = response_ocr.text_annotations[0].description
                sample = ocr_text[:100].replace('\n', ' ')
                wprint(f"    ✓ OCR SUCCESS! Extracted {len(ocr_text)} characters")
                wprint(f"    📝 Sample: {sample}...")
                with self.lock:
                    self.stats['ocr_success'] += 1
                    self.successful_ocr.append(post_id)
                return ocr_text
            else:
                wprint(f"    ⚠ No text found in image")
                with self.lock:
                    self.stats['ocr_failed'] += 1

        except requests.exceptions.Timeout:
            wprint(f"    ✗ Image download timeout")
            with self.lock:
                self.stats['download_errors'] += 1
                self.stats['ocr_failed'] += 1

        except requests.exceptions.RequestException as e:
            wprint(f"    ✗ Image download error: {str(e)[:100]}")
            with self.lock:
                self.stats['download_errors'] += 1
                self.stats['ocr_failed'] += 1

        except Exception as e:
            error_str = str(e)[:150]
            wprint(f"    ✗ OCR exception: {error_str}")
            with self.lock:
                self.stats['ocr_failed'] += 1
                self.failed_ocr.append(post_id)
            low = error_str.lower()
            if '403' in error_str and 'billing' in low:
                outage_watchdog.record('vision_403_billing', error_str)
            elif '403' in error_str:
                outage_watchdog.record('vision_403', error_str)
            if '429' in error_str or 'quota' in low:
                with self.lock:
                    self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)
                wprint(f"    ⚠ Increasing delay to {self.rate_limit_delay:.1f}s")

        return ""

    # ─────────────────────────────────────────────────────────────
    # PR I — Tier escalation helpers.
    # Mirrors events_from_ids.py's 4-tier ladder so production cron
    # and recovery tool produce comparable extractions. Tier handlers
    # are class methods (vs. top-level functions in events_from_ids.py)
    # so they can read self.gemini_flash_lite, self.gemini_pro, and
    # the cached safety-net lookups without ctx-dict ceremony.
    # ─────────────────────────────────────────────────────────────

    def _setup_pro_model(self):
        """Lazy init of the Pro model. Most cron runs never hit Tier 4
        (Pro escalation only fires on quality flags, not zero events),
        so we defer the genai.GenerativeModel construction until needed."""
        if self.gemini_pro is None:
            self.gemini_pro = genai.GenerativeModel(ec_MODEL_PRO)
            wprint(f"    ↳ Pro model initialized ({ec_MODEL_PRO})")

    def _download_image_for_multimodal(self, image_url):
        """Download a single image and return ({'mime_type': str, 'data': bytes}, mime)
        or None on failure. Uses the same retry/UA pattern as extract_ocr_text."""
        if not image_url or image_url == 'null':
            return None
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            }
            resp = requests.get(image_url, headers=headers, timeout=15)
            if resp.status_code != 200:
                return None
            # Infer MIME type from Content-Type or URL extension
            ct = (resp.headers.get('Content-Type') or '').lower()
            if 'jpeg' in ct or 'jpg' in ct:
                mime = 'image/jpeg'
            elif 'png' in ct:
                mime = 'image/png'
            elif 'webp' in ct:
                mime = 'image/webp'
            else:
                # Default to JPEG (Instagram CDN serves JPEG most often)
                mime = 'image/jpeg'
            return {'mime_type': mime, 'data': resp.content}
        except Exception:
            return None

    def _collect_multimodal_images(self, post, cache_key='_cached_mm_images'):
        """Return a list of {'mime_type', 'data'} dicts ready for
        genai.GenerativeModel.generate_content() multimodal calls.

        Caches the result on the post dict under cache_key so successive
        tier escalations don't re-download the same images. The cache
        cleanup happens at the end of process_post (PR I memory hygiene)."""
        if cache_key in post:
            return post[cache_key]
        urls = self.collect_carousel_urls(post)
        images = []
        for url in urls:
            img = self._download_image_for_multimodal(url)
            if img:
                images.append(img)
        post[cache_key] = images
        return images

    def _parse_gemini_json(self, text):
        """Strip code fences and parse a JSON response. Returns dict or None.
        Mirrors events_from_ids.py.call_gemini's parsing logic."""
        if not text:
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

    def _call_gemini_tier(self, model, prompt_or_content, tier_name):
        """Wrap a Gemini call with parsing + minimal error handling.
        Returns the parsed JSON dict or None on any failure. tier_name
        is recorded for log readability only."""
        try:
            resp = model.generate_content(prompt_or_content)
            if not resp:
                return None
            text = resp.text.strip()
        except Exception as e:
            err = str(e)
            wprint(f"    ✗ Gemini error in {tier_name}: {err[:120]}")
            if '429' in err or 'quota' in err.lower():
                outage_watchdog.record('gemini_429', err)
            return None
        return self._parse_gemini_json(text)

    def _enrich_event_for_tier(self, e, post, has_ocr, is_calendar):
        """Add the metadata fields the sheet expects. Used by every
        tier so its events are ready for sanity checks.

        Returns the enriched event dict (mutated in place) or None if
        it should be dropped (no name AND no date)."""
        if not e.get('event_name') and not e.get('date'):
            return None
        if e.get('event_name') and len(e['event_name']) > 40:
            e['event_name'] = e['event_name'][:40].rsplit(' ', 1)[0]
        user = post.get('ownerUsername', '')
        owner_full_name = post.get('ownerFullName', '')
        shortcode = post.get('shortCode', '') or post.get('shortcode', '')
        display_url = post.get('displayUrl', '') or post.get('display_url', '')
        pid = post.get('id') or post.get('shortCode')
        e['instagram_handle'] = user
        e['instagram_post_url'] = f"https://www.instagram.com/p/{shortcode}/" if shortcode else ''
        e['display_url'] = display_url
        e['post_url'] = post.get('url', '')
        e['instagram_profile_url'] = f"https://www.instagram.com/{user}/" if user else ''
        e['account_name'] = owner_full_name
        e['start_time'] = self.clean_time(e.get('start_time'))
        e['post_id'] = pid
        e['had_ocr'] = has_ocr
        e['from_calendar'] = is_calendar
        e.setdefault('recurrence_pattern', '')
        return e

    def _run_tier(self, tier_name, post, post_date, ocr_text, slide_count, slides_with_text, cached_images):
        """Run a single tier. Returns (enriched_events, is_calendar, has_ocr, all_flags).

        Each tier:
          1. Builds the appropriate prompt + (optional) image content
          2. Calls Gemini and parses response
          3. Enriches resulting events with metadata
          4. Runs per-event + post-level sanity checks
          5. Returns flags so the tier ladder can decide escalation"""
        # ── Build call inputs per tier ──
        caption = post.get('caption', '') or post.get('text', '')
        if tier_name == ec_TIER_FLASH_CAPTION:
            wprint(f"  [Tier 1: Flash-Lite caption-only (no Vision)]")
            prompt = ec_build_prompt(post, "", post_date)
            data = self._call_gemini_tier(self.gemini_flash_lite, prompt, tier_name)
            has_ocr = False
        elif tier_name == ec_TIER_FLASH_IMAGE:
            wprint(f"  [Tier 2: Flash-Lite multimodal (image)]")
            if not cached_images:
                wprint(f"    ⚠ No images downloadable — falling back to caption-only handling")
                prompt = ec_build_prompt(post, "", post_date)
                data = self._call_gemini_tier(self.gemini_flash_lite, prompt, tier_name)
            else:
                wprint(f"    ✓ {len(cached_images)} image(s) prepared for multimodal")
                prompt = ec_build_prompt(post, "", post_date)
                content = [prompt] + cached_images
                data = self._call_gemini_tier(self.gemini_flash_lite, content, tier_name)
            has_ocr = False
        elif tier_name == ec_TIER_FLASH_TEXT:
            wprint(f"  [Tier 3: Flash-Lite + OCR text]")
            if not ocr_text:
                wprint(f"    ⚠ No OCR text")
            prompt = ec_build_prompt(post, ocr_text or "", post_date)
            data = self._call_gemini_tier(self.gemini_flash_lite, prompt, tier_name)
            has_ocr = bool(ocr_text)
        elif tier_name == ec_TIER_PRO_IMAGE:
            wprint(f"  [Tier 4: Pro multimodal (image)]")
            self._setup_pro_model()
            if not cached_images:
                wprint(f"    ⚠ No images — falling back to caption-only on Pro")
                prompt = ec_build_prompt(post, ocr_text or "", post_date)
                data = self._call_gemini_tier(self.gemini_pro, prompt, tier_name)
            else:
                prompt = ec_build_prompt(post, ocr_text or "", post_date)
                content = [prompt] + cached_images
                data = self._call_gemini_tier(self.gemini_pro, content, tier_name)
            has_ocr = bool(ocr_text)
        else:
            wprint(f"  ⚠ Unknown tier: {tier_name}")
            return [], False, False, []

        if not data:
            return [], False, has_ocr, []

        is_calendar = data.get('is_calendar_post', False)
        raw_events = data.get('events', [])

        # ── Enrich each event ──
        enriched = []
        for e in raw_events:
            ee = self._enrich_event_for_tier(e, post, has_ocr, is_calendar)
            if ee is not None:
                enriched.append(ee)

        # ── Per-event sanity checks ──
        source_text = (caption or '') + ' ' + (ocr_text or '')
        all_flags = set()
        for ev in enriched:
            row_flags = []
            for name, fn, args in [
                # ── Legacy checks (round 1) ──
                ('region_against_lookup', ec_check_region_against_lookup, (ev, self._safety_net_region_lookup)),
                ('date_day_match',        ec_check_date_day_match,        (ev, source_text)),
                ('venue_city',            ec_check_venue_city,            (ev, self._safety_net_venue_lookup)),
                ('low_confidence',        ec_check_low_confidence,        (ev,)),
                ('missing_date',          ec_check_missing_date,          (ev,)),
                ('date_sanity',           ec_check_date_sanity,           (ev, post_date)),
                # ── Round 2 (2026-06): wrong-field flags (pink highlight) ──
                ('venue_is_handle',         ec_check_venue_is_handle,         (ev,)),
                ('venue_equals_event_name', ec_check_venue_equals_event_name, (ev,)),
                ('venue_equals_account',    ec_check_venue_equals_account,    (ev,)),
                ('venue_truncated',         ec_check_venue_truncated,         (ev,)),
                ('venue_out_of_nj_region',  ec_check_venue_out_of_nj_region,  (ev,)),
                # ── Round 2: probably-not-event flags (orange highlight) ──
                ('event_name_generic',      ec_check_event_name_generic,      (ev,)),
                ('caption_only_no_ocr',     ec_check_caption_only_no_ocr,     (ev,)),
                # ── Round 2: missing-field flags (yellow / legacy color) ──
                ('missing_venue',           ec_check_missing_venue,           (ev,)),
                ('missing_city',            ec_check_missing_city,            (ev,)),
                ('missing_time',            ec_check_missing_time,            (ev,)),
            ]:
                f = self._safe_check(name, fn, *args)
                if f:
                    row_flags.append(f)
            ev['_row_flags'] = row_flags
            all_flags.update(row_flags)

        # ── Post-level sanity checks ──
        n_events = len(enriched)
        post_flags = []
        for name, fn, args in [
            # Legacy round 1
            ('calendar_low_events',  ec_check_calendar_low_events,  (caption or '', ocr_text or '', n_events)),
            ('carousel_low_events',  ec_check_carousel_low_events,  (slide_count, slides_with_text, n_events)),
            ('ocr_rich_low_events',  ec_check_ocr_rich_low_events,  (ocr_text or '', n_events)),
            ('no_grounding',         ec_check_no_grounding,         (caption or '', ocr_text or '', n_events)),
            # Round 2 (2026-06) — orange highlight on the QUALITY_FLAGS cell
            ('personal_post_signals', ec_check_personal_post_signals, (caption or '', n_events)),
            ('no_event_signals',      ec_check_no_event_signals,      (caption or '', ocr_text or '', n_events)),
        ]:
            f = self._safe_check(name, fn, *args)
            if f:
                post_flags.append(f)
        all_flags.update(post_flags)

        # Merge per-event + post-level flags onto each event
        for ev in enriched:
            merged = (ev.get('_row_flags') or []) + post_flags
            ev['quality_flags'] = ",".join(sorted(set(merged)))
            ev.pop('_row_flags', None)
            # Event-level log: name, date, venue, confidence, flags — visible
            # in run_*.log for every event from every tier.
            ev_name  = ev.get('event_name', '?')
            ev_date  = ev.get('date', '?')
            ev_venue = ev.get('venue', '')
            ev_conf  = ev.get('confidence', '')
            ev_flags = ev['quality_flags'] if ev['quality_flags'] else 'none'
            venue_str = f"\n      • Venue: {ev_venue}" if ev_venue else ''
            conf_str  = f"\n      • Confidence: {ev_conf}" if ev_conf != '' else ''
            wprint(f"    ✦ [{tier_name}] {ev_name!r} | {ev_date}{venue_str}{conf_str}\n      • Flags: {ev_flags}")

        return enriched, is_calendar, has_ocr, list(all_flags)

    def _process_with_tier_ladder(self, post, post_date, ocr_text,
                                   slide_count, slides_with_text,
                                   max_tier=None):
        """Run a post through the tier ladder. Returns
        (final_events, tier_used, has_ocr, is_calendar).

        Starts at TIER_FLASH_CAPTION (cheapest) and escalates only when
        needed. Caption-less posts skip Tier 1 (nothing to work with)
        and start at TIER_FLASH_IMAGE.

        Escalation rules (mirrors events_from_ids.py):
          (a) Any ESCALATION_FLAGS fired → escalate (even to Pro)
          (b) Zero events at non-final, non-Pro tier → escalate
          (c) NEVER escalate to Pro just because earlier tiers returned
              zero events. Pro only fires on a quality flag — preserves
              cost discipline (Pro is ~8× Flash-Lite per call)."""
        max_tier = max_tier or ec_TIER_PRO_IMAGE

        # Download images once, reuse across Tier 2 + Tier 4
        cached_images = self._collect_multimodal_images(post)

        has_caption = bool((post.get('caption') or '').strip())
        if not has_caption:
            wprint(f"  ↳ no caption — starting at flash_image (skipping Tier 1)")
            current_tier = ec_TIER_FLASH_IMAGE
        else:
            current_tier = ec_TIER_FLASH_CAPTION

        final_events = []
        final_is_calendar = False
        final_has_ocr = False

        while current_tier is not None:
            events, is_calendar, has_ocr, flags = self._run_tier(
                current_tier, post, post_date, ocr_text,
                slide_count, slides_with_text, cached_images,
            )

            # Decide whether to escalate
            try:
                idx = ec_TIER_LADDER.index(current_tier)
            except ValueError:
                idx = -1
            nt = ec_TIER_LADDER[idx + 1] if 0 <= idx < len(ec_TIER_LADDER) - 1 else None

            flag_escalate = any(f in ec_ESCALATION_FLAGS for f in flags)
            zero_event_escalate = (len(events) == 0 and nt is not None and nt != ec_TIER_PRO_IMAGE)
            can_escalate = nt is not None and current_tier != max_tier

            if can_escalate and (flag_escalate or zero_event_escalate):
                reason_parts = []
                if flag_escalate:
                    esc_flags = sorted(f for f in set(flags) if f in ec_ESCALATION_FLAGS)
                    reason_parts.append(f"flags={esc_flags}")
                if zero_event_escalate:
                    reason_parts.append("zero_events")
                wprint(f"  ↳ escalating to {nt}  ({', '.join(reason_parts)})")
                current_tier = nt
                continue

            # Final tier reached. Surface informational flags so user sees auto-fixes.
            informational = sorted(f for f in set(flags) if f not in ec_ESCALATION_FLAGS)
            if informational:
                wprint(f"  ↳ informational flags: {informational}")
            final_events = events
            final_is_calendar = is_calendar
            final_has_ocr = has_ocr
            break

        # Free the image cache for this post — these can be MB of bytes
        # per post, and parallel workers can accumulate quickly.
        post.pop('_cached_mm_images', None)

        # Track which tier ended up "winning"
        with self.lock:
            self.stats['tiers_used'][current_tier] = self.stats['tiers_used'].get(current_tier, 0) + 1

        return final_events, current_tier, final_has_ocr, final_is_calendar

    def process_post(self, post, post_num, total):
        """Outer wrapper: prints unprefixed entry/exit boundary lines with
        worker ID and timing, and (when buffered mode is on) buffers all
        per-post output so each post appears as one contiguous block."""
        post_id = (post.get('id', '') or post.get('shortCode', '') or
                   post.get('shortcode', '') or f'post_{post_num}')
        worker_id = get_worker_id()
        t0 = time.time()

        # Include the Instagram URL on the entry line so the post is one
        # click away when scanning the log retrospectively. Omitted when
        # there's no shortcode (Apify shell records, etc.).
        shortcode = post.get('shortCode', '') or post.get('shortcode', '')
        url_suffix = f" — https://www.instagram.com/p/{shortcode}/" if shortcode else ""

        # Entry boundary — atomic, always live (not buffered), tagged with [Wn].
        print_unbuffered(
            f"\n[{worker_id}] [{post_num}/{total}] Processing post: {post_id}{url_suffix}"
        )

        # If a prior worker tripped the outage watchdog (Vision 403 spike,
        # Gemini 429 spike), skip the rest of the queue. Don't burn API
        # calls into the same outage — the run will exit cleanly once all
        # workers drain. The marker file in outputs/ tells the user why.
        if outage_watchdog.is_aborted():
            info = outage_watchdog.get_abort_info() or {}
            print_unbuffered(
                f"[{worker_id}] [{post_num}/{total}] "
                f"⛔ skipped — outage abort active ({info.get('category', '?')})"
            )
            return None

        result = None
        crashed = False
        crash_summary = ''
        try:
            with post_buffer():
                result = self._process_post_impl(post, post_num, total, post_id)
            return result
        except Exception as e:
            # Tag the crash with [worker_id] [post_id] BEFORE letting it
            # propagate. The main-thread catch in run_pipeline only sees
            # (future, exception) and can't infer which worker died — so
            # the only authoritative attribution happens here, inside the
            # worker thread. Re-raises so the futures contract is preserved
            # (run_pipeline still counts/logs the failure).
            crashed = True
            crash_summary = f"{type(e).__name__}: {str(e)[:160]}"
            tb_last = traceback.format_exc().strip().splitlines()[-1] if traceback else ''
            print_unbuffered(
                f"❌ [{worker_id}] [{post_num}/{total}] CRASHED on {post_id}: "
                f"{crash_summary} | {tb_last}"
            )
            raise
        finally:
            elapsed = time.time() - t0
            n_events = len(result) if isinstance(result, list) else 0
            plural = '' if n_events == 1 else 's'
            if crashed:
                # Distinguish "completed with no events" (a normal outcome
                # for posts that aren't event-related) from "blew up mid-
                # extraction." Without this, the log line for both cases
                # was identical: "Done in Xs (0 events)" — misleading.
                print_unbuffered(
                    f"[{worker_id}] [{post_num}/{total}] "
                    f"Crashed after {elapsed:.1f}s ({crash_summary})"
                )
            else:
                print_unbuffered(
                    f"[{worker_id}] [{post_num}/{total}] "
                    f"Done in {elapsed:.1f}s ({n_events} event{plural})"
                )

    def _process_post_impl(self, post, post_num, total, post_id):
        with log_post_context(post_id):
            pid = post.get('id') or post.get('shortCode')

            # Apify shell-record check: when an account returns no posts (private,
            # suspended, deleted, typo'd handle, or zero-recent-posts), Apify
            # returns a placeholder entry with no id, no shortCode, no caption,
            # and no owner. The 'url' field points at the profile URL itself.
            # Previously we'd fabricate a pseudo-ID like f'post_{post_num}' and
            # process the empty post anyway, polluting Processed_Log with
            # post_NNN rows that can never collide with real Instagram IDs.
            # Now we skip these at the entry and track which accounts are dead
            # so the operator can clean up their accounts list. See ADR 0008.
            if not pid:
                self._note_apify_shell_record(post, post_num)
                return None

            # Atomic check-and-claim: add pid to the dedup set inside the same
            # lock block as the existence check. Without this, two concurrent
            # threads with the same pid (e.g. from a duplicated source list) can
            # both pass the check before either thread reaches the
            # results.extend / post_results write 10-30 seconds later, producing
            # duplicate event rows in All_Events. See ADR 0006.
            with self.lock:
                if pid in self.processed_posts:
                    self.stats['skipped_history'] += 1
                    wprint(f"  [{post_num}/{total}] @{post.get('ownerUsername', '')} | {pid} - Already processed, skipping")
                    return None
                self.processed_posts.add(pid)

            caption = post.get('caption', '') or post.get('text', '')
            user = post.get('ownerUsername', '')
            owner_full_name = post.get('ownerFullName', '')
            shortcode = post.get('shortCode', '') or post.get('shortcode', '')
            display_url = post.get('displayUrl', '') or post.get('display_url', '')
            location_name = post.get('locationName', '') or post.get('location', '')

            timestamp_val = post.get('timestamp', '')
            try:
                if timestamp_val:
                    if isinstance(timestamp_val, (int, float)):
                        post_date = datetime.fromtimestamp(timestamp_val)
                    else:
                        post_date = datetime.fromisoformat(str(timestamp_val).replace('Z', '+00:00'))
                else:
                    post_date = datetime.now(EAST_TZ)
            except Exception:
                post_date = datetime.now(EAST_TZ)
            post_date_str = post_date.strftime('%Y-%m-%d')

            # NOTE: the "Processing post" header line is now emitted by the
            # outer process_post() wrapper as an unprefixed boundary marker.
            wprint(f"  ↳ Account: @{user} ({owner_full_name})")

            if not caption and not display_url:
                wprint(f"  ⚠ No caption or image URL - skipping")
                self._record_post_outcome(pid, 'no_events_found', user, post_date_str,
                                          error='no_caption_or_image_url')
                with self.lock:
                    self.stats['processed'] += 1
                    self.stats['skipped_no_data'] += 1
                return None

            has_caption = bool(caption)
            has_location = bool(location_name)
            has_image = bool(display_url)

            ocr_text = ""
            # Retry-queue posts reconstructed from a stale apify_raw dump have
            # expired CDN image URLs — calling Vision on them would burn API
            # quota and 100% fail. The retry-queue builder flags these with
            # __caption_only_retry__ so we skip OCR entirely. Caption-only
            # Gemini still catches text-described events. (2026-05-29 design.)
            caption_only_retry = bool(post.get('__caption_only_retry__'))
            if caption_only_retry:
                wprint(f"  ↻ Retry (caption-only): skipping OCR — original image URLs likely expired")
            elif self.vision_enabled:
                all_urls = self.collect_carousel_urls(post)
                if all_urls:
                    num_slides = len(all_urls)
                    if num_slides > 1:
                        wprint(f"  📸 CAROUSEL: {num_slides} slides detected — OCR'ing all")
                    else:
                        wprint(f"  ↳ Found image URL")
                    if num_slides > 1:
                        with self.lock:
                            self.stats['carousel_posts'] += 1
                    slide_texts = []
                    for idx, url in enumerate(all_urls, 1):
                        if num_slides > 1:
                            wprint(f"    ─── Slide {idx}/{num_slides} ───")
                        text = self.extract_ocr_text(url, f"{pid}_s{idx}")
                        with self.lock:
                            self.stats['total_slides_ocrd'] += 1
                        if text:
                            with self.lock:
                                self.stats['slides_with_text'] += 1
                            if num_slides > 1:
                                slide_texts.append(f"[SLIDE {idx} of {num_slides}]\n{text}")
                            else:
                                slide_texts.append(text)
                    ocr_text = "\n\n".join(slide_texts)
                    if num_slides > 1:
                        wprint(f"  ✓ Carousel OCR complete: {len(slide_texts)}/{num_slides} slides had text")
                else:
                    wprint(f"  ⚠ No image URL found - relying on text fields")
            else:
                wprint(f"  ⚠ Vision API disabled - relying on text fields only")

            has_ocr = bool(ocr_text)
            # caption_only_retry path explicitly didn't run OCR — don't let
            # downstream tag the outcome as ocr_failed (it isn't).
            ocr_attempted_and_failed = (
                self.vision_enabled and has_image and not has_ocr and not caption_only_retry
            )
            wprint(f"  ↳ Data available: caption={has_caption}, location={has_location}, image={has_image}, OCR={has_ocr}")

            all_text = (caption + ' ' + ocr_text).lower()
            calendar_keywords = ['calendar', 'schedule', 'lineup', 'weekly', 'monthly',
                               'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
                               'saturday', 'sunday', 'every', 'recurring']
            might_be_calendar = any(keyword in all_text for keyword in calendar_keywords)
            if might_be_calendar:
                wprint(f"  📅 Possible calendar/multi-event post detected")

            time.sleep(self.rate_limit_delay)

            wprint(f"  ↳ Analyzing with Gemini AI...")

            # ─────────────────────────────────────────────────────────────
            # PR I — TIER LADDER EXTRACTION.
            # Replaces the single-shot Gemini call with the 4-tier ladder
            # (Flash-Lite caption → Flash-Lite image → Flash-Lite + OCR →
            # Pro multimodal). _process_with_tier_ladder handles prompt
            # construction, Gemini calls, enrichment, sanity checks, and
            # escalation decisions — see method docstring for the loop
            # invariants. Returns processed events with quality_flags
            # already merged, plus the winning tier name + is_calendar.
            #
            # When self.tier_escalation_enabled is False (config opt-out),
            # we cap the ladder at TIER_FLASH_TEXT so the user's preferred
            # single model (still bound to self.gemini_flash_lite from
            # __init__) gets used without spilling into Pro.
            # ─────────────────────────────────────────────────────────────
            slides_with_text = ocr_text.count('[SLIDE ') if '[SLIDE ' in ocr_text else (1 if ocr_text else 0)
            slide_count = len(self.collect_carousel_urls(post))
            max_tier = ec_TIER_PRO_IMAGE if self.tier_escalation_enabled else ec_TIER_FLASH_TEXT
            clean_json = ''  # legacy sentinel — kept for error-recording sites that reference it

            try:
                processed_events, tier_used, has_ocr_used, is_calendar = self._process_with_tier_ladder(
                    post, post_date, ocr_text,
                    slide_count=slide_count,
                    slides_with_text=slides_with_text,
                    max_tier=max_tier,
                )

                if not processed_events:
                    wprint(f"  ↳ No events found in this post (tier={tier_used})")
                    result_tag = 'ocr_failed' if ocr_attempted_and_failed else 'no_events_found'
                    reason = ('tier_ladder_no_events_after_failed_ocr' if ocr_attempted_and_failed
                              else 'tier_ladder_returned_no_events')
                    self._record_post_outcome(pid, result_tag, user, post_date_str,
                                              caption=caption, ocr_text=ocr_text,
                                              error=reason)
                    with self.lock:
                        self.stats['processed'] += 1
                        self.stats['posts_no_events'] += 1
                    return None

                _result_tag = 'events_found' if processed_events else 'no_events_found'
                # ─────────────────────────────────────────────────────────────
                # PR A — atomic outcome record + results.extend.
                # Previously these were in two separate lock acquisitions, and
                # a periodic flush firing between them could snapshot
                # post_results WITHOUT the matching events in self.results,
                # creating an orphan (post marked done in Processed_Log but
                # no events in All_Events). Combined here under one lock so
                # flushes always see either both-present or both-absent for
                # a given pid.
                # ─────────────────────────────────────────────────────────────
                with self.lock:
                    if processed_events:
                        self._record_post_outcome_locked(pid, _result_tag, user, post_date_str)
                    else:
                        self._record_post_outcome_locked(pid, _result_tag, user, post_date_str,
                                                          caption=caption, ocr_text=ocr_text,
                                                          gemini_raw=clean_json,
                                                          error='gemini_returned_events_but_all_filtered')
                    self.stats['processed'] += 1
                    if processed_events:
                        self.stats['posts_with_events'] += 1
                        self.stats['events_found'] += len(processed_events)
                        # Stamp provenance on every event row BEFORE extend.
                        # _record_post_outcome_locked just wrote the same
                        # (worker_id, run_id, attempt_id) into post_results[pid],
                        # so the All_Events row and the Processed_Log row for
                        # this post agree on all three. If a flagged cell turns
                        # up later, these three fields point at the exact
                        # run/worker/attempt that produced it.
                        prov_worker = get_worker_id()
                        prov_attempt = self.retry_attempts.get(pid, 0) + 1
                        for _ev in processed_events:
                            _ev['worker_id']  = prov_worker
                            _ev['run_id']     = self.run_id
                            _ev['attempt_id'] = prov_attempt
                        self.results.extend(processed_events)
                        if len(processed_events) > 1:
                            self.stats['multi_event_posts'] += 1
                        self.stats['max_events_in_post'] = max(
                            self.stats['max_events_in_post'], len(processed_events)
                        )
                        if is_calendar:
                            self.stats['calendar_posts'] += 1

                        if self.stats['events_found'] % 500 == 0:
                            self.save_checkpoint()
                            wprint(f"  ↳ Checkpoint saved ({self.stats['events_found']} total events)")
                    else:
                        self.stats['posts_no_events'] += 1

                if processed_events:
                    if len(processed_events) > 1:
                        wprint(f"  🎉 MULTIPLE EVENTS FOUND: {len(processed_events)} events extracted!")
                        for idx, event in enumerate(processed_events, 1):
                            recurring_tag = " ♻ recurring" if event.get('is_recurring') else ""
                            wprint(f"    {idx}. {event.get('event_name', 'Unnamed')}{recurring_tag}")
                            if event.get('date'):
                                wprint(f"       Date: {event['date']}")
                            if event.get('venue_name'):
                                wprint(f"       Venue: {event['venue_name']}")
                            wprint(f"       Confidence: {event.get('confidence', 'unknown')}")
                    else:
                        event = processed_events[0]
                        recurring_tag = " ♻ recurring" if event.get('is_recurring') else ""
                        wprint(f"  ✓ EVENT FOUND: {event.get('event_name', 'Unnamed')}{recurring_tag}")
                        wprint(f"    • Date: {event.get('date', 'N/A')}")
                        wprint(f"    • Venue: {event.get('venue_name', 'N/A')}")
                        wprint(f"    • Confidence: {event.get('confidence', 'unknown')}")
                else:
                    wprint(f"  ↳ No valid events in this post")

                return processed_events

            except Exception as e:
                error_str = str(e)
                wprint(f"  ✗ Gemini error: {error_str[:200]}")
                tb = traceback.format_exc()
                self._record_post_outcome(pid, 'gemini_error', user, post_date_str,
                                          caption=caption, ocr_text=ocr_text,
                                          error=f'unhandled_exception: {error_str[:200]} | {tb.splitlines()[-1] if tb else ""}')
                with self.lock:
                    self.stats['gemini_errors'] += 1
                    self.stats['processed'] += 1
                if '429' in error_str:
                    with self.lock:
                        self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)
                    wprint(f"  ⚠ Rate limited - delay increased to {self.rate_limit_delay:.1f}s")
                return None

    def save_checkpoint(self):
        try:
            checkpoint = {
                'processed_posts': self.processed_posts,
                'results': self.results,
                'stats': self.stats,
            }
            with open(self.checkpoint_file, 'wb') as f:
                pickle.dump(checkpoint, f)
        except Exception:
            pass

    def run_pipeline(self, posts):
        self.post_results = {}
        total = len(posts)
        self.stats['total_posts'] = total

        already_known = sum(1 for p in posts if (p.get('id') or p.get('shortCode')) in self.processed_posts)
        new_posts = total - already_known

        print(f"\n{'='*60}")
        print(f"PROCESSING {total} POSTS ({new_posts} new, {already_known} already processed)")
        print(f"Workers: {self.max_workers} | Rate Delay: {self.rate_limit_delay}s")
        print(f"Vision OCR: {'ENABLED (with image download)' if self.vision_enabled else 'DISABLED'}")
        print(f"{'='*60}")

        start_time = time.time()

        if self.max_workers > 1:
            print(f"\n⚡ Processing with {self.max_workers} parallel workers...\n")
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Capture (i, post) per future so the exception handler below
                # can name the specific post that died. Previously the error
                # line was "⚠ Worker error: <exception>" with no post_id, so
                # there was no way to correlate the crash to which post the
                # worker had been working on.
                futures = {
                    executor.submit(self.process_post, post, i + 1, total): (i, post)
                    for i, post in enumerate(posts)
                }
                for future in as_completed(futures):
                    i, post = futures[future]
                    try:
                        future.result(timeout=120)
                    except Exception as e:
                        died_pid = (post.get('id') or post.get('shortCode')
                                    or post.get('shortcode') or f'idx_{i+1}')
                        died_handle = post.get('ownerUsername', '')
                        # [main] tag so it's clearly NOT misattributed to
                        # whichever worker's lines appear adjacent.
                        print(f"⚠ [main] Worker error on post {died_pid} "
                              f"(@{died_handle}, idx {i+1}/{total}): {e}")
                    # Periodic flush — threshold check inside makes most
                    # calls cheap, so this is safe to call on every post.
                    # Without this, killed runs lose 100% of their dedup
                    # state. See ADR 0010.
                    self._flush_processed_log(force=False)
        else:
            print("\n📋 Processing sequentially...\n")
            for i, post in enumerate(posts):
                self.process_post(post, i + 1, total)
                self._flush_processed_log(force=False)

        elapsed = time.time() - start_time
        self.save_checkpoint()

        self.create_final_report(elapsed)

        return self.results, dict(self.post_results)

    def create_final_report(self, elapsed=0):
        print(f"\n{'='*60}")
        print(f"FINAL EXTRACTION REPORT")
        print(f"{'='*60}")

        print(f"\n📊 OVERALL STATISTICS:")
        print(f"  • Total posts: {self.stats['total_posts']}")
        print(f"  • Posts processed: {self.stats['processed']}")
        print(f"  • Skipped (already in history): {self.stats['skipped_history']}")
        print(f"  • Skipped (no data): {self.stats['skipped_no_data']}")
        print(f"  • Posts with events: {self.stats['posts_with_events']}")
        print(f"  • Posts with no events: {self.stats['posts_no_events']}")
        print(f"  • Total events extracted: {self.stats['events_found']}")
        print(f"  • Parallel workers used: {self.max_workers}")

        if elapsed:
            print(f"  • Processing time: {elapsed/60:.1f} minutes ({elapsed:.0f}s)")
            if self.stats['processed'] > 0:
                rate = self.stats['processed'] / elapsed
                print(f"  • Processing rate: {rate:.1f} posts/second")

        if self.stats['posts_with_events'] > 0:
            avg_events = self.stats['events_found'] / self.stats['posts_with_events']
            print(f"  • Average events per post (with events): {avg_events:.2f}")

        if self.stats['processed'] > 0:
            success_rate = (self.stats['posts_with_events'] / self.stats['processed']) * 100
            print(f"  • Event detection rate: {success_rate:.1f}%")

        print(f"\n📅 MULTI-EVENT STATISTICS:")
        print(f"  • Posts with multiple events: {self.stats['multi_event_posts']}")
        print(f"  • Maximum events in single post: {self.stats['max_events_in_post']}")
        print(f"  • Calendar/schedule posts: {self.stats['calendar_posts']}")

        print(f"\n📸 CAROUSEL STATISTICS:")
        print(f"  • Carousel posts (multi-slide): {self.stats['carousel_posts']}")
        print(f"  • Total slides OCR'd: {self.stats['total_slides_ocrd']}")
        print(f"  • Slides with text found: {self.stats['slides_with_text']}")
        if self.stats['total_slides_ocrd'] > 0:
            slide_rate = (self.stats['slides_with_text'] / self.stats['total_slides_ocrd']) * 100
            print(f"  • Slide text rate: {slide_rate:.1f}%")

        print(f"\n🔍 OCR STATISTICS:")
        total_ocr = self.stats['ocr_success'] + self.stats['ocr_failed']
        print(f"  • OCR attempts: {total_ocr}")
        print(f"  • Successful: {self.stats['ocr_success']}")
        print(f"  • Failed: {self.stats['ocr_failed']}")
        print(f"  • Download errors: {self.stats['download_errors']}")
        if total_ocr > 0:
            ocr_rate = (self.stats['ocr_success'] / total_ocr) * 100
            print(f"  • OCR success rate: {ocr_rate:.1f}%")

        if self.stats['vision_errors']:
            print(f"\n⚠ VISION API ERRORS:")
            for error, count in list(self.stats['vision_errors'].items())[:5]:
                print(f"  • {error}: {count} times")

        print(f"\n⚠ GEMINI ERRORS: {self.stats['gemini_errors']}")

        # PR I — Tier-usage report. Surfaces how often each tier ended up
        # being the "winning" tier for a post. Useful for cost forecasting
        # and verifying that escalation is firing as expected (most posts
        # should land at flash_caption or flash_image; pro_image should be
        # rare — a few percent at most).
        tiers_used = self.stats.get('tiers_used', {})
        if tiers_used and any(tiers_used.values()):
            print(f"\n🎚  TIER USAGE:")
            total = sum(tiers_used.values()) or 1
            for tier in [ec_TIER_FLASH_CAPTION, ec_TIER_FLASH_IMAGE,
                         ec_TIER_FLASH_TEXT, ec_TIER_PRO_IMAGE]:
                n = tiers_used.get(tier, 0)
                pct = 100.0 * n / total
                print(f"  • {tier:<14} {n:>5}  ({pct:>5.1f}%)")

        if self.post_results:
            from collections import Counter
            tag_counts = Counter(info.get('result', '') for info in self.post_results.values())
            print(f"\n🏷  RESULT TAG BREAKDOWN:")
            print(f"  • events_found:       {tag_counts.get('events_found', 0)}")
            print(f"  • no_events_found:    {tag_counts.get('no_events_found', 0)}")
            print(f"  • ocr_failed:         {tag_counts.get('ocr_failed', 0)}")
            print(f"  • gemini_error:       {tag_counts.get('gemini_error', 0)}")
            if tag_counts.get('permanently_failed', 0):
                print(f"  • permanently_failed: {tag_counts.get('permanently_failed', 0)} "
                      f"(retry budget exhausted at {RETRY_MAX_ATTEMPTS} attempts)")
            retried_pids = [pid for pid in self.post_results if pid in self.retry_attempts]
            if retried_pids:
                retry_outcomes = Counter(
                    self.post_results[pid].get('result', '') for pid in retried_pids
                )
                print(f"\n↻  RETRY QUEUE OUTCOMES (this run):")
                print(f"  • posts retried:      {len(retried_pids)}")
                print(f"  • now succeeded:      {retry_outcomes.get('events_found', 0)}")
                print(f"  • no_events_found:    {retry_outcomes.get('no_events_found', 0)}")
                print(f"  • still failing:      {retry_outcomes.get('ocr_failed', 0) + retry_outcomes.get('gemini_error', 0)}")
                print(f"  • permanently_failed: {retry_outcomes.get('permanently_failed', 0)}")

        # ─────────────────────────────────────────────────────────────
        # Per-worker breakdown. With 10+ parallel workers, a single broken
        # worker (stuck on a bad API key, hitting Vision quota, hung TLS
        # session, etc.) can drag the aggregate down without standing out.
        # This table surfaces who handled how many posts and with what
        # outcomes — easy to spot one worker with disproportionate errors.
        # Run ID is printed once so a Replit-log copy/paste retains the
        # provenance needed to cross-reference the Sheet's Run ID column.
        # ─────────────────────────────────────────────────────────────
        if self.worker_stats:
            print(f"\n👷  PER-WORKER BREAKDOWN (run_id={self.run_id}):")
            header = f"  {'Worker':<8} {'Total':>6} {'Events':>7} {'NoEvts':>7} {'OCRfail':>8} {'Gemini':>7} {'Perma':>6}"
            print(header)
            print(f"  {'─'*7:<8} {'─'*5:>6} {'─'*6:>7} {'─'*6:>7} {'─'*7:>8} {'─'*6:>7} {'─'*5:>6}")
            for wid in sorted(self.worker_stats.keys(),
                              key=lambda w: int(w[1:]) if w.startswith('W') and w[1:].isdigit() else 9999):
                c = self.worker_stats[wid]
                total = sum(c.values())
                print(
                    f"  {wid:<8} {total:>6} "
                    f"{c.get('events_found', 0):>7} "
                    f"{c.get('no_events_found', 0):>7} "
                    f"{c.get('ocr_failed', 0):>8} "
                    f"{c.get('gemini_error', 0):>7} "
                    f"{c.get('permanently_failed', 0):>6}"
                )

        shell_accounts = getattr(self, '_shell_accounts', {})
        if shell_accounts:
            print(f"\n⚠ APIFY RETURNED EMPTY (SHELL) RESPONSES FOR {sum(shell_accounts.values())} POSTS")
            print(f"   across {len(shell_accounts)} accounts. These accounts produced")
            print(f"   no posts at all — likely dead, private, suspended, or typo'd.")
            print(f"   Top affected accounts (review your Accounts tab):")
            top = sorted(shell_accounts.items(), key=lambda kv: -kv[1])[:20]
            for acct, n in top:
                print(f"     {n:4d}×  {acct or '(no inputUrl)'}")
            if len(shell_accounts) > 20:
                print(f"     ... and {len(shell_accounts) - 20} more (full list in anomaly summary)")

    def _apply_quality_flag_formatting(self, evt_sheet, df, start_row):
        """B5: apply per-cell highlighting AND rich-text styling based on
        each event's quality_flags. Uses the canonical FLAG_TO_COLUMNS +
        FLAG_TO_COLOR + FLAG_TO_TEXT_COLOR mappings from extraction_core
        so main.py and events_from_ids.py produce identical visual signals
        on All_Events.

        Two layers, applied independently to each row:

        1. Data-cell background color (legacy). Each flag's FLAG_TO_COLUMNS
           list names sheet columns; their cells get backgroundColor set per
           FLAG_TO_COLOR. Multiple flags on one cell → strongest color wins
           per pink > orange > yellow priority. (Orange-tier flags have no
           FLAG_TO_COLUMNS entries in round 2 — they style only the token
           inside the QUALITY FLAGS cell. See FLAG_TO_TEXT_COLOR.)

        2. Rich-text token styling in QUALITY FLAGS (round 2, 2026-06-04).
           Every flag in FLAG_TO_TEXT_COLOR renders its literal token bold
           + colored (pink for "wrong field", orange for "probably not
           event") inside the comma-separated cell content. Yellow-tier
           tokens stay unstyled. This is applied via the Sheets API's
           textFormatRuns mechanism, batched into ONE batch_update call.

        Only NEW rows being appended (start_row → end) are styled. Back-
        catalog rows are intentionally left as-is — per the user's
        2026-06-04 review, retroactive repaint is out of scope."""
        if 'QUALITY FLAGS' not in df.columns:
            return
        try:
            flag_col_idx = list(df.columns).index('QUALITY FLAGS')
        except ValueError:
            return

        # Build column-letter map from canonical header → A1 letter.
        # df.columns are uppercase + space-separated by the time we get
        # here (save_data uppercases and replaces underscores), so
        # FLAG_TO_COLUMNS keys MUST also use spaces (round-1 bug fixed
        # 2026-06-04: 'QUALITY_FLAGS' silently lost the lookup).
        col_letter_by_name = {}
        for i, c in enumerate(df.columns):
            if i < 26:
                col_letter_by_name[c] = chr(ord('A') + i)
            else:
                col_letter_by_name[c] = chr(ord('A') + (i // 26) - 1) + chr(ord('A') + (i % 26))

        # Color priority — higher rank wins when multiple flags hit one cell.
        # PINK (wrong field) > YELLOW (legacy/missing). Orange-tier flags
        # have NO FLAG_TO_COLUMNS entries in round 2 — they only style the
        # token inside the QUALITY FLAGS cell via FLAG_TO_TEXT_COLOR, never
        # paint a data cell. So the rank function only needs to disambiguate
        # pink-vs-yellow on the data-cell path.
        def _color_rank(color):
            if color is ec_FLAG_TO_COLOR.get('VENUE_IS_HANDLE'):  # pink sentinel
                return 2
            return 0  # yellow default

        format_requests = []   # data-cell background highlights (gspread batch_format)
        text_run_requests = [] # rich-text styling (Sheets batch_update updateCells)

        # Resolve QUALITY FLAGS column index for the rich-text path. We
        # need this both as a string -> letter (for the data-cell path) and
        # as a numeric 0-based index (for the textFormatRuns request).
        quality_flags_col_letter = col_letter_by_name.get('QUALITY FLAGS')

        rows_list = df.values.tolist()
        for offset, row in enumerate(rows_list):
            flags_str = row[flag_col_idx] if flag_col_idx < len(row) else ''
            if not flags_str:
                continue
            row_num = start_row + offset

            # ── Data-cell background highlights ─────────────────────────
            cell_color = {}
            for f in str(flags_str).split(','):
                f = f.strip()
                if not f:
                    continue
                cols = ec_FLAG_TO_COLUMNS.get(f, [])
                color = ec_FLAG_TO_COLOR.get(f)
                if not color:
                    # Yellow-tier flag (or unmapped) — use default yellow
                    color = ec_FLAG_BG_COLOR
                for col_label in cols:
                    existing = cell_color.get(col_label)
                    if existing is None or _color_rank(color) > _color_rank(existing):
                        cell_color[col_label] = color
            for col_label, color in cell_color.items():
                letter = col_letter_by_name.get(col_label)
                if not letter:
                    continue
                format_requests.append({
                    'range': f'{letter}{row_num}',
                    'format': {'backgroundColor': color},
                })

            # ── Rich-text token styling inside QUALITY FLAGS ───────────
            if quality_flags_col_letter:
                runs = self._build_quality_flag_text_runs(str(flags_str))
                if runs:
                    text_run_requests.append({
                        'row_num':  row_num,
                        'col_idx':  flag_col_idx,
                        'runs':     runs,
                        'value':    str(flags_str),
                    })

        # ── Apply data-cell highlights via gspread batch_format ─────────
        if format_requests:
            try:
                evt_sheet.batch_format(format_requests)
                print(f"✓ Applied {len(format_requests)} per-cell quality-flag highlights")
            except AttributeError:
                # Older gspread without batch_format — fall back to per-cell
                for fr in format_requests:
                    try:
                        evt_sheet.format(fr['range'], fr['format'])
                        time.sleep(0.3)
                    except Exception as e2:
                        print(f"    ⚠ Format failed for {fr['range']}: {str(e2)[:80]}")
            except Exception as e:
                print(f"  ⚠ batch_format failed: {str(e)[:120]}")

        # ── Apply rich-text token styling via Sheets batchUpdate ────────
        if text_run_requests:
            self._apply_quality_flag_text_runs(evt_sheet, text_run_requests)

    @staticmethod
    def _build_quality_flag_text_runs(flags_str):
        """For a QUALITY FLAGS cell content like 'FLAG_A,FLAG_B,FLAG_C',
        produce Sheets API textFormatRuns that style each pink/orange flag
        token bold + colored. Yellow-tier tokens stay in default formatting.

        Returns a list of {startIndex, format} dicts ready for inclusion in
        an updateCells request, or [] if nothing in the string needs styling.

        Sheets API note: a TextFormatRun starts at startIndex and applies
        until the next run's startIndex. To "reset" after a styled token,
        we emit a run at the position AFTER the token with the default
        format (bold=False, foregroundColor=black). When two adjacent
        styled tokens have the same format, we collapse intermediate
        resets so the comma between them stays in default format too."""
        if not flags_str:
            return []
        s = str(flags_str)
        # Build a list of (start, end, color) for every token that needs
        # styling. We scan token boundaries (comma-delimited) rather than
        # using regex search to avoid accidentally matching a flag name
        # that's a substring of another (e.g., "NO_EVENT_SIGNALS" inside
        # a hypothetical "X_NO_EVENT_SIGNALS_Y").
        spans = []
        cursor = 0
        for part in s.split(','):
            # The token starts where leading whitespace ends.
            stripped = part.lstrip()
            lead_ws = len(part) - len(stripped)
            token_start = cursor + lead_ws
            token_text = stripped.rstrip()
            token_end = token_start + len(token_text)
            if token_text:
                color = ec_FLAG_TO_TEXT_COLOR.get(token_text)
                if color:
                    spans.append((token_start, token_end, color))
            cursor += len(part) + 1  # +1 for the comma we split on

        if not spans:
            return []

        # Sort by start (already sorted, but defensive)
        spans.sort(key=lambda t: t[0])

        # Build TextFormatRuns:
        #   • A styled run at each token's start (bold + colored)
        #   • A default run right after each styled token to reset
        # The Sheets API requires the first run to start at index 0 if
        # we want any preceding text to use the default. Emit an initial
        # default run if the first styled span doesn't start at 0.
        #
        # CRITICAL invariant (per Sheets API spec): every TextFormatRun's
        # startIndex must be strictly less than len(cell_value). A run at
        # exactly len(s) makes Sheets reject the entire batchUpdate with
        # INVALID_ARGUMENT, which aborts ALL styling for the flush — not
        # just this row. So when a styled token ends AT the end of the
        # string, we omit the trailing reset (there's no text after it
        # that would need resetting anyway).
        n = len(s)
        DEFAULT_FORMAT = {"bold": False, "foregroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}
        runs = []
        if spans[0][0] > 0:
            runs.append({"startIndex": 0, "format": DEFAULT_FORMAT})
        for start, end, color in spans:
            runs.append({
                "startIndex": start,
                "format": {"bold": True, "foregroundColor": color},
            })
            if end < n:
                runs.append({
                    "startIndex": end,
                    "format": DEFAULT_FORMAT,
                })
        # Defensive: confirm runs are strictly ascending by startIndex.
        # With the emission pattern above this is guaranteed (spans are
        # sorted, tokens don't overlap, and the trailing-reset guard means
        # no run ever lands at len(s)). Asserting catches future regressions.
        for i in range(1, len(runs)):
            assert runs[i]["startIndex"] > runs[i - 1]["startIndex"], (
                f"TextFormatRuns out of order: {runs[i-1]} → {runs[i]} for {s!r}"
            )
        return runs

    def _apply_quality_flag_text_runs(self, evt_sheet, requests):
        """Apply rich-text styling to QUALITY FLAGS cells via the Sheets
        API batchUpdate. Each request specifies a row, a column index, the
        textFormatRuns to apply, and the cell value (preserved so that the
        update doesn't blank the text content).

        Bundles ALL row updates into ONE batch_update call — keeps API
        cost at one round trip even when we're styling thousands of rows."""
        if not requests:
            return
        try:
            sheet_id = evt_sheet.id
        except Exception as e:
            print(f"  ⚠ Could not resolve sheet id for rich-text runs: {e}")
            return

        body_requests = []
        for r in requests:
            row_zero = r['row_num'] - 1  # API is 0-indexed
            col_zero = r['col_idx']
            body_requests.append({
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_zero,
                        "endRowIndex": row_zero + 1,
                        "startColumnIndex": col_zero,
                        "endColumnIndex": col_zero + 1,
                    },
                    "rows": [{
                        "values": [{
                            # Preserve the cell value — without this the
                            # updateCells request clears it.
                            "userEnteredValue": {"stringValue": r['value']},
                            "textFormatRuns": r['runs'],
                        }]
                    }],
                    "fields": "userEnteredValue,textFormatRuns",
                }
            })

        try:
            self.main_sheet.batch_update({"requests": body_requests})
            print(f"✓ Applied {len(body_requests)} rich-text quality-flag styles")
        except Exception as e:
            print(f"  ⚠ rich-text batch_update failed: {str(e)[:160]}")

    def save_data(self, events, post_log):
        # Final Processed_Log flush. Most entries are already there from
        # incremental flushes during the run (see ADR 0010); this picks up
        # the tail end. force=True ignores the threshold check so even a
        # small remainder gets written.
        if self.sheets_client and self.log_worksheet:
            self._flush_processed_log(force=True)
            if self.post_results:
                tag_counts = {}
                for info in self.post_results.values():
                    tag = info.get('result', '')
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
                summary = ', '.join(f"{v} {k}" for k, v in sorted(tag_counts.items()))
                print(f"✓ Processed_Log: {len(self.post_results)} entries total this run ({summary})")

        if not events:
            print("No new events found.")
            return

        df = pd.DataFrame(events)

        # B5: schema now matches events_from_ids.py — adds QUALITY_FLAGS and
        # RECURRENCE_PATTERN columns. Existing All_Events headers are
        # forward-compatible (the two columns get appended at the right edge
        # without shifting any existing data). New extractions populate them;
        # legacy rows have empty values.
        cols = [
            'instagram_handle', 'event_name', 'date', 'start_time',
            'venue_name', 'city', 'section_of_nj', 'newsletter_description',
            'instagram_post_url', 'display_url', 'post_url', 'instagram_profile_url',
            'event_type', 'account_name', 'description', 'performer', 'price',
            'confidence', 'post_id', 'had_ocr', 'from_calendar', 'is_recurring',
            'processed_timestamp',
            'quality_flags',         # per-row sanity-check fingerprint
            'recurrence_pattern',    # pattern string for recurring events
            # Provenance — see _flush_events_to_sheet for details.
            'run_id', 'worker_id', 'attempt_id',
        ]

        df['processed_timestamp'] = datetime.now(EAST_TZ).strftime('%Y-%m-%d %H:%M:%S')

        for c in cols:
            if c not in df.columns:
                df[c] = ""
        df = df[[c for c in cols if c in df.columns]]

        url_columns = {'instagram_post_url', 'display_url', 'post_url', 'instagram_profile_url'}
        skip_upper = url_columns | {'processed_timestamp'}
        for col_name in df.columns:
            if col_name not in skip_upper:
                df[col_name] = df[col_name].map(lambda x: str(x).upper() if isinstance(x, str) else x)
        df.columns = [c.upper().replace('_', ' ') for c in df.columns]

        ts = datetime.now(EAST_TZ).strftime('%Y%m%d_%H%M%S')
        csv_file = self.output_dir / f"Events_{ts}.csv"
        df.to_csv(csv_file, index=False)
        print(f"\n✓ Saved CSV: {csv_file} ({len(events)} events)")

        try:
            excel_file = self.output_dir / f"Events_{ts}.xlsx"
            df.to_excel(excel_file, index=False)
            print(f"✓ Saved Excel: {excel_file}")
        except Exception as e:
            print(f"⚠ Excel save error: {e}")

        stats_file = self.output_dir / f"stats_{ts}.json"
        with open(stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2)
        print(f"✓ Saved stats: {stats_file}")

        if 'date' in [c.lower().replace(' ', '_') for c in df.columns]:
            print(f"\n📋 Sample of extracted events (first 5):")
            print("-" * 60)
            display_cols = [c for c in df.columns if c in ['EVENT NAME', 'DATE', 'VENUE NAME', 'CITY', 'CONFIDENCE']]
            if display_cols:
                print(df[display_cols].head(5).to_string())

        # Orphan-prevention path: events were flushed incrementally to
        # All_Events alongside each Processed_Log flush (every
        # EVENTS_FLUSH_THRESHOLD posts via _flush_events_to_sheet). The
        # force=True call from _flush_processed_log above already drained
        # any tail-end events that hadn't met the threshold yet.
        #
        # Doing a redundant batch append here would create duplicate rows,
        # so we skip it. CSV/Excel/stats files above remain the local
        # complete-record backup.
        if self.sheets_client and self.main_sheet:
            with self.lock:
                flushed = self._events_flushed_count
                total = len(self.results)
            if flushed >= total:
                print(f"✓ All {total} events already in Google Sheets via incremental flush")
            else:
                print(f"⚠ {total - flushed} events not yet in sheet — incremental flush may have failed")
                # Try a final force-flush. If it succeeds, we're caught up.
                self._flush_events_to_sheet(force=True)

        try:
            recurring_accounts.refresh(self.main_sheet)
        except Exception as e:
            print(f"⚠ Reliable Accounts refresh failed: {e}")

    def start_scheduler(self):
        target_day = CONF["schedule_day"]
        target_time = CONF["schedule_time"]

        # schedule_time is interpreted in this timezone. Hardcoded to America/New_York
        # because the Replit container's local TZ is undeclared and was producing
        # surprising firing times. To change, edit SCHEDULE_TZ here. See
        # docs/decisions/0005-config-mode-scoping.md.
        from zoneinfo import ZoneInfo
        SCHEDULE_TZ = ZoneInfo("America/New_York")

        print(f"\n⏰ BOT STARTED.")
        print(f"   Target: Every {target_day} at {target_time} ({SCHEDULE_TZ})")
        print("   Status: Waiting...")

        while True:
            now = datetime.now(SCHEDULE_TZ)
            day = now.strftime("%A")
            hm = now.strftime("%H:%M")

            if day == target_day and hm == target_time:
                print(f"\n🚀 STARTING RUN: {now}")
                run_started_at = now

                try:
                    # Try live Apify scrape first; fall back to static URL if disabled or failed
                    raw_posts = []
                    if CONF.get("apify_enabled", True):
                        raw_posts = fetch_posts_via_apify()

                    if not raw_posts:
                        url = CONF.get("instagram_data_url", "")
                        if url:
                            print("  ↳ Using static instagram_data_url as data source")
                            response = requests.get(url, timeout=120)
                            response.raise_for_status()
                            raw_posts = response.json()
                        else:
                            print("⚠ No posts fetched — Apify returned nothing and no instagram_data_url is set.")
                            raw_posts = []

                    offset = CONF.get("post_offset", 0)
                    cap = CONF.get("max_posts", 0)
                    if offset:
                        raw_posts = raw_posts[offset:]
                        print(f"  • Post offset: skipping first {offset} posts")
                    if cap:
                        raw_posts = raw_posts[:cap]
                        print(f"  • Post cap: processing up to {cap} posts")
                    self.setup_sheets()
                    # Same retry-queue merge as do_force_run — see comment there.
                    if self.retry_attempts:
                        in_data_pids = {(p.get('id') or p.get('shortCode')) for p in raw_posts}
                        in_data_pids.discard(None)
                        retry_posts = _build_retry_work_list(self, in_data_pids)
                        if retry_posts:
                            raw_posts = list(raw_posts) + retry_posts
                    events, ids = self.run_pipeline(raw_posts)
                    self.save_data(events, ids)

                    if self.main_sheet:
                        try:
                            audit.run_audit(
                                spreadsheet=self.main_sheet,
                                raw_posts=raw_posts,
                                events_extracted=len(events),
                                run_started_at=run_started_at,
                            )
                        except Exception as e:
                            print(f"⚠ Audit failed (non-fatal): {e}")

                    print("✅ Run complete. Sleeping until next window...")
                    time.sleep(70)
                except Exception as e:
                    print(f"❌ Run Failed: {e}")
                    import traceback
                    traceback.print_exc()
                    time.sleep(60)

            time.sleep(10)


def load_usernames_from_accounts_sheet():
    """
    Reads Instagram usernames from the 'Accounts' tab of the Google Sheet.

    On first run (tab missing), creates the tab and pre-loads it with the
    usernames from accounts.json. Returns a list of username strings, or None
    if the Sheets connection is unavailable or the tab is empty (so the caller
    can fall back to accounts.json).
    """
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        return None

    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client = gspread.authorize(creds)
        spreadsheet = client.open(CONF["sheet_name"])
    except Exception as e:
        print(f"⚠ Could not connect to Google Sheets for Accounts tab: {e}")
        return None

    try:
        ws = spreadsheet.worksheet("Accounts")
        print("✓ Found 'Accounts' tab in Google Sheet")
    except gspread.WorksheetNotFound:
        print("⚠ 'Accounts' tab not found — creating it and pre-loading from accounts.json")
        accounts_path = Path("accounts.json")
        seed_usernames = []
        if accounts_path.exists():
            try:
                with open(accounts_path) as f:
                    seed_usernames = json.load(f)
            except Exception as e:
                print(f"⚠ Could not read accounts.json for seeding: {e}")

        ws = spreadsheet.add_worksheet(title="Accounts", rows=max(len(seed_usernames) + 10, 100), cols=1)
        rows = [["Username"]] + [[u] for u in seed_usernames]
        ws.update(rows, value_input_option="RAW")
        print(f"✓ Created 'Accounts' tab and loaded {len(seed_usernames)} usernames")
        return seed_usernames if seed_usernames else None

    try:
        all_values = ws.col_values(1)
    except Exception as e:
        print(f"⚠ Could not read 'Accounts' tab: {e}")
        return None

    if not all_values:
        print("⚠ 'Accounts' tab is empty")
        return None

    if all_values[0].strip().lower() == "username":
        all_values = all_values[1:]

    usernames = [u.strip() for u in all_values if u.strip()]
    if not usernames:
        print("⚠ 'Accounts' tab has no usernames after header")
        return None

    print(f"✓ Loaded {len(usernames)} usernames from 'Accounts' tab")

    accounts_path = Path("accounts.json")
    previous_usernames = []
    if accounts_path.exists():
        try:
            with open(accounts_path) as f:
                previous_usernames = json.load(f)
        except Exception as e:
            print(f"⚠ Could not read previous accounts.json for drift check: {e}")

    write_succeeded = False
    try:
        tmp_path = accounts_path.with_suffix(".json.tmp")
        with open(tmp_path, "w") as f:
            json.dump(usernames, f, indent=2)
        tmp_path.replace(accounts_path)
        write_succeeded = True
        print(f"✓ accounts.json updated with {len(usernames)} usernames from Sheet")
    except Exception as e:
        print(f"⚠ Could not update accounts.json: {e}")

    if write_succeeded:
        prev_set = set(previous_usernames)
        new_set = set(usernames)
        added = new_set - prev_set
        removed = prev_set - new_set
        if added or removed:
            added_str = f"+{len(added)} added ({', '.join(sorted(added))})" if added else ""
            removed_str = f"-{len(removed)} removed ({', '.join(sorted(removed))})" if removed else ""
            delta_parts = [p for p in [added_str, removed_str] if p]
            print(f"↕ accounts.json drift detected: {', '.join(delta_parts)}")

    return usernames


def fetch_posts_via_apify():
    """
    Triggers a live apify/instagram-post-scraper run. Usernames are loaded from
    the 'Accounts' tab of the Google Sheet; falls back to accounts.json if the
    tab is missing or empty. Returns a list of raw post dicts on success, or []
    on any failure so the caller can fall back to instagram_data_url.
    """
    apify_token = os.environ.get("APIFY_API_KEY", "").strip()
    if not apify_token:
        print("⚠ APIFY_API_KEY not set — skipping Apify fetch")
        return []

    usernames = load_usernames_from_accounts_sheet()

    if usernames is None:
        print("⚠ Falling back to accounts.json for username list")
        accounts_path = Path("accounts.json")
        if not accounts_path.exists():
            print("⚠ accounts.json not found — skipping Apify fetch")
            return []
        try:
            with open(accounts_path) as f:
                usernames = json.load(f)
        except Exception as e:
            print(f"⚠ Failed to read accounts.json: {e}")
            return []

    if not usernames:
        print("⚠ No usernames found in Accounts tab or accounts.json — skipping Apify fetch")
        return []

    newer_than_days = int(CONF.get("apify_newer_than_days", 7))
    newer_than_date = (datetime.now(EAST_TZ) - timedelta(days=newer_than_days)).strftime("%Y-%m-%d")
    results_limit   = int(CONF.get("apify_posts_per_profile", 9))

    payload = {
        "username":           usernames,
        "resultsLimit":       results_limit,
        "onlyPostsNewerThan": newer_than_date,
        "skipPinnedPosts":    False,
        "dataDetailLevel":    "detailedData",
    }

    print(f"\n📡 Fetching fresh posts via Apify (instagram-post-scraper)...")
    print(f"  • Accounts:          {len(usernames)}")
    print(f"  • Posts per profile: {results_limit}")
    print(f"  • Newer than:        {newer_than_date}")

    apify_base = "https://api.apify.com/v2"
    actor      = "apify~instagram-post-scraper"

    try:
        run_resp = requests.post(
            f"{apify_base}/acts/{actor}/runs",
            params={"token": apify_token},
            json=payload,
            timeout=30,
        )
        if not run_resp.ok:
            print(f"  ↳ Apify error {run_resp.status_code}: {run_resp.text[:300]}")
            run_resp.raise_for_status()

        run_data   = run_resp.json()["data"]
        run_id     = run_data["id"]
        dataset_id = run_data["defaultDatasetId"]
        print(f"  ↳ Run ID:     {run_id}")
        print(f"  ↳ Dataset ID: {dataset_id}")
        print(f"  ↳ Waiting for run to finish (may take 20-40 min)...")

        while True:
            time.sleep(15)
            poll = requests.get(
                f"{apify_base}/actor-runs/{run_id}",
                params={"token": apify_token},
                timeout=30,
            )
            poll.raise_for_status()
            status = poll.json()["data"]["status"]
            print(f"    Status: {status}")
            if status in ("SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"):
                break

        if status != "SUCCEEDED":
            print(f"  ↳ Run ended with status: {status} — falling back to static URL")
            return []

        dataset_resp = requests.get(
            f"{apify_base}/datasets/{dataset_id}/items",
            params={"token": apify_token, "format": "json", "clean": "false"},
            timeout=120,
        )
        dataset_resp.raise_for_status()
        posts = dataset_resp.json()
        print(f"  ✓ Downloaded {len(posts)} fresh post(s) from Apify")

        try:
            ts = datetime.now(EAST_TZ).strftime('%Y%m%d_%H%M%S')
            raw_path = Path("outputs") / f'apify_raw_{ts}.json'
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            with open(raw_path, 'w') as _rf:
                json.dump({
                    'fetched_at': ts,
                    'run_id': run_id,
                    'dataset_id': dataset_id,
                    'usernames_requested': usernames,
                    'posts_per_profile_cap': results_limit,
                    'newer_than': newer_than_date,
                    'post_count': len(posts),
                    'posts': posts,
                }, _rf, default=str)
            print(f"  💾 Raw Apify dump saved: {raw_path}")
        except Exception as e:
            print(f"  ⚠ Could not save raw Apify dump: {e}")

        return posts

    except Exception as e:
        print(f"⚠ Apify fetch failed: {e} — falling back to static URL")
        return []


def reset_run_now():
    try:
        with open("config.json", "r") as f:
            cfg = json.load(f)
        cfg["run_now"] = False
        with open("config.json", "w") as f:
            json.dump(cfg, f, indent=2)
        print("✓ Reset run_now to false in config.json")
    except Exception as e:
        print(f"⚠ Could not reset run_now in config.json: {e}")


def _extract_dataset_id_from_url(url: str) -> str:
    """Pull the dataset_id out of an Apify dataset URL for archival logging.
    Returns empty string if URL doesn't match the expected pattern."""
    import re as _re
    m = _re.search(r'/datasets/([A-Za-z0-9_-]+)/items', url or '')
    return m.group(1) if m else ''


def _append_dataset_run_log(ts: str, source: str, url: str, dataset_id: str, post_count: int):
    """Append one entry to outputs/dataset_run_log.json — a permanent map of
    run_timestamp → dataset_id so future "what URL did the X run use?"
    questions become a 2-second grep.

    Surfaced today when an Apify dataset wipe forced us to manually pull
    historical dataset IDs from the user's Apify console. Going forward,
    every main.py run leaves a breadcrumb."""
    log_path = Path("outputs") / "dataset_run_log.json"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    entries = []
    if log_path.exists():
        try:
            with open(log_path) as f:
                entries = json.load(f)
        except Exception:
            entries = []
    entries.append({
        'run_timestamp': ts,
        'source':        source,      # 'apify_live' or 'static_url'
        'url':           url,
        'dataset_id':    dataset_id,
        'post_count':    post_count,
    })
    try:
        with open(log_path, 'w') as f:
            json.dump(entries, f, indent=2)
    except Exception as e:
        print(f"  ⚠ Could not append to dataset_run_log: {e}")


def _build_retry_work_list(bot, already_in_data: set) -> list:
    """Look up payloads for posts in bot.retry_attempts that aren't already
    being processed via the fresh Apify scrape, and return them as a list
    of post dicts to merge into the work list.

    Resolution order for each retry pid:
      1. outputs/apify_raw_*.json  (newest first — this is what main.py writes)
      2. outputs/apify_cache/*.json (events_from_ids.py's per-dataset cache)

    If the source dump's `fetched_at` is older than STALE_RAW_DAYS, the post's
    image URLs almost certainly point at expired IG CDN URLs. We still queue
    the post but tag it with __caption_only_retry__ so _process_post_impl
    skips OCR and runs Gemini on caption-only. (Approved design — see
    2026-05-29 conversation: caption-only retry over re-scraping.)

    Pids with no payload available anywhere are silently dropped from this
    run. They stay in retry_attempts so a future Apify scrape can pick them
    up if the post comes back into the window.
    """
    from glob import glob

    pending = {pid for pid in bot.retry_attempts if pid not in already_in_data}
    if not pending:
        return []

    work_list = []
    found_pids = set()
    stale_pids = set()
    cutoff = datetime.now(EAST_TZ) - timedelta(days=STALE_RAW_DAYS)

    def _is_stale(fetched_at_str: str) -> bool:
        if not fetched_at_str:
            return True
        # apify_raw timestamps look like '20260529_142233'
        try:
            ts = datetime.strptime(fetched_at_str, '%Y%m%d_%H%M%S').replace(tzinfo=EAST_TZ)
        except Exception:
            return True
        return ts < cutoff

    # Pass 1: newest apify_raw first. Once we find a pid we don't look further.
    raw_files = sorted(glob('outputs/apify_raw_*.json'), reverse=True)
    for path in raw_files:
        if not pending:
            break
        try:
            with open(path) as f:
                dump = json.load(f)
        except Exception:
            continue
        posts = dump.get('posts') or []
        fetched_at = dump.get('fetched_at', '')
        stale = _is_stale(fetched_at)
        for post in posts:
            pid = post.get('id') or post.get('shortCode')
            if not pid or pid not in pending:
                continue
            # Shallow copy so we don't mutate the on-disk dump if we add a flag.
            queued = dict(post)
            if stale:
                queued['__caption_only_retry__'] = True
                stale_pids.add(pid)
            queued['__is_retry__'] = True
            work_list.append(queued)
            found_pids.add(pid)
            pending.discard(pid)

    # Pass 2: per-dataset apify_cache (used by events_from_ids.py). These don't
    # carry a fetched_at — we conservatively treat them as stale → caption-only.
    if pending:
        cache_files = sorted(glob('outputs/apify_cache/*.json'), reverse=True)
        for path in cache_files:
            if not pending:
                break
            try:
                with open(path) as f:
                    posts = json.load(f)
            except Exception:
                continue
            if not isinstance(posts, list):
                continue
            for post in posts:
                pid = post.get('id') or post.get('shortCode')
                if not pid or pid not in pending:
                    continue
                queued = dict(post)
                queued['__caption_only_retry__'] = True
                queued['__is_retry__'] = True
                stale_pids.add(pid)
                work_list.append(queued)
                found_pids.add(pid)
                pending.discard(pid)

    unresolved = len(bot.retry_attempts) - len(found_pids) - len(
        {pid for pid in bot.retry_attempts if pid in already_in_data}
    )

    print(f"  ↻ Retry queue: {len(bot.retry_attempts)} eligible | "
          f"{len(work_list)} reconstructed from local archives "
          f"({len(stale_pids)} caption-only due to stale image URLs) | "
          f"{unresolved} unresolved (no local payload — will wait for Apify rescrape)")
    return work_list


def preflight_check(strict=False):
    """Validate that every piece of config required to run end-to-end is
    actually present. Prints a single banner listing every missing item so
    the operator can fix them all at once instead of discovering them one
    crash at a time. Returns the number of missing items.

    Two modes:
      strict=True  → sys.exit(1) when something is missing. Use for
                     force-run (--now) so a misconfigured manual run
                     fails immediately instead of running 20 minutes
                     before silently degrading.
      strict=False → log only, return count. Use for the scheduler so
                     the bot can sit idle waiting for the operator to
                     drop a secret in without restarting the process.

    Previously the pipeline would log a one-line warning ('⚠ No service
    account file found. Running in offline mode.') deep in setup output
    and continue, so a missing GEMINI_API_KEY → forty minutes of failing
    Gemini calls → no events found → confused operator. This banner
    fails loud, fails fast, and fails with a fix recipe."""
    issues = []

    if not os.environ.get("GEMINI_API_KEY", "").strip():
        issues.append((
            "GEMINI_API_KEY env var is empty",
            "Add it under Replit's Secrets (lock icon, left sidebar). "
            "Required for every Gemini call — pipeline cannot extract events without it.",
        ))

    apify_on = bool(CONF.get("apify_enabled", True))
    if apify_on and not os.environ.get("APIFY_API_KEY", "").strip():
        issues.append((
            "APIFY_API_KEY env var is empty (apify_enabled=true in config)",
            "Either add APIFY_API_KEY to Replit Secrets, OR set "
            "apify_enabled=false in config.json to fall back to instagram_data_url.",
        ))

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        issues.append((
            f"Google service account JSON not found at '{SERVICE_ACCOUNT_FILE}'",
            "Either (a) upload a service-account JSON to the repo and set the "
            "GOOGLE_APPLICATION_CREDENTIALS env var to its path, OR (b) name "
            "your file 'apt-mark-468506-u9-ec44cabc7335 copy.json' (legacy default). "
            "Without it: no Sheets sync, no OCR — pipeline runs in degraded local-only mode.",
        ))

    if not issues:
        return 0

    print()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print(f"║  ⚠  PREFLIGHT FAILED — {len(issues)} configuration issue(s) detected  " + " " * (16 - len(str(len(issues)))) + "║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    for i, (problem, fix) in enumerate(issues, 1):
        print(f"\n  [{i}] {problem}")
        print(f"      → {fix}")
    print()

    if strict:
        print("Exiting (use --no-preflight to override, or fix the issues above).\n")
        sys.exit(1)
    else:
        print("Continuing in degraded mode — fix the issues above before the next run.\n")
    return len(issues)


def do_force_run(bot):
    print("\n🚀 Force run initiated...\n")
    run_started_at = datetime.now(EAST_TZ)
    run_ts = run_started_at.strftime('%Y%m%d_%H%M%S')
    bot.setup_sheets()

    # Try live Apify scrape first; fall back to static URL if disabled or failed
    data = []
    data_source = ''
    data_url = ''
    data_dataset_id = ''
    if CONF.get("apify_enabled", True):
        data = fetch_posts_via_apify()
        if data:
            data_source = 'apify_live'
            # apify_live path already saves apify_raw_<ts>.json + dataset_id via fetch_posts_via_apify;
            # we record the source here for the unified run log. The most-recent
            # apify_raw file's content has dataset_id we can mirror into the run log.
            try:
                from glob import glob as _glob
                latest_raw = max(_glob('outputs/apify_raw_*.json'), default='')
                if latest_raw:
                    with open(latest_raw) as f:
                        raw = json.load(f)
                    data_dataset_id = raw.get('dataset_id', '')
                    data_url = (f"https://api.apify.com/v2/datasets/{data_dataset_id}/items"
                                if data_dataset_id else '')
            except Exception as e:
                print(f"  ⚠ Could not read latest apify_raw for run log: {e}")

    if not data:
        url = CONF.get("instagram_data_url", "")
        if url:
            print("  ↳ Using static instagram_data_url as data source")
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            data = response.json()
            data_source = 'static_url'
            data_url = url
            data_dataset_id = _extract_dataset_id_from_url(url)

            # B4: archive the static-URL response too. Previously only the
            # live-Apify path saved a raw dump. Without this, runs against
            # the static URL had no permanent record of the source data —
            # and Apify can wipe datasets (we observed this today), making
            # retroactive recovery impossible. Mirror the live-path schema.
            try:
                raw_path = Path("outputs") / f'apify_raw_{run_ts}.json'
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                with open(raw_path, 'w') as _rf:
                    json.dump({
                        'fetched_at':  run_ts,
                        'source':      'static_url',
                        'url':         url,
                        'dataset_id':  data_dataset_id,
                        'post_count':  len(data),
                        'posts':       data,
                    }, _rf, default=str)
                print(f"  💾 Raw static-URL dump saved: {raw_path}")
            except Exception as e:
                print(f"  ⚠ Could not save raw static-URL dump: {e}")
        else:
            print("❌ No posts fetched — Apify returned nothing and no instagram_data_url is set.")
            return

    # Append breadcrumb to the unified dataset_run_log (works for both paths)
    _append_dataset_run_log(run_ts, data_source, data_url, data_dataset_id, len(data))

    offset = CONF.get("post_offset", 0)
    cap    = CONF.get("max_posts", 0)
    if offset:
        data = data[offset:]
        print(f"  • Post offset: skipping first {offset} posts")
    if cap:
        data = data[:cap]
        print(f"  • Post cap: processing up to {cap} posts")

    # Merge active-retry queue posts AFTER offset/cap so the per-run cap
    # only governs the new scrape, not the retry budget. Retries should
    # be unconditional within RETRY_MAX_ATTEMPTS — they're already a
    # subset of past failures, not net-new work.
    if bot.retry_attempts:
        in_data_pids = {(p.get('id') or p.get('shortCode')) for p in data}
        in_data_pids.discard(None)
        retry_posts = _build_retry_work_list(bot, in_data_pids)
        if retry_posts:
            data = list(data) + retry_posts

    events, ids = bot.run_pipeline(data)
    bot.save_data(events, ids)

    if bot.main_sheet:
        try:
            audit.run_audit(
                spreadsheet=bot.main_sheet,
                raw_posts=data,
                events_extracted=len(events),
                run_started_at=run_started_at,
            )
        except Exception as e:
            print(f"⚠ Audit failed (non-fatal): {e}")


if __name__ == "__main__":
    # --buffered: each worker buffers its post's lines and flushes the whole
    # block atomically when the post finishes. Trades real-time visibility
    # for clean per-post blocks in the run log — good for retrospective
    # analysis, less good for live debugging. Off by default.
    if "--buffered" in sys.argv:
        set_buffered_mode(True)
        print("📦 Buffered-output mode: each post flushes as one atomic block on completion")

    # Preflight: validate every required secret/file BEFORE we spend 20+
    # minutes silently degrading. --now (manual force-run) gets strict
    # mode → exit on missing config. The scheduler stays soft so an
    # operator can drop secrets in mid-process without restarting.
    # --no-preflight bypasses (useful for offline-mode testing).
    is_force_run = ("--now" in sys.argv) or bool(CONF.get("run_now", False))
    if "--no-preflight" not in sys.argv:
        preflight_check(strict=is_force_run)

    bot = InstagramEventPipeline()
    config_forced = CONF.get("run_now", False)
    if "--now" in sys.argv or config_forced:
        if config_forced:
            print("⚡ run_now is ON in config.json — triggering immediate run")
        try:
            do_force_run(bot)
        finally:
            if config_forced:
                reset_run_now()
    else:
        bot.start_scheduler()
