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
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import recurring_accounts
import audit

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%I:%M:%S %p')
logger = logging.getLogger(__name__)

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
        "apify_posts_per_profile": 9,
        "apify_newer_than_days": 14,
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
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"


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

        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self._setup_run_log()

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
        }

        if not GEMINI_API_KEY:
            print("❌ ERROR: GEMINI_API_KEY not found in Secrets!")
        else:
            genai.configure(api_key=GEMINI_API_KEY)

        model_name = CONF["gemini_model"]
        print(f"\n{'='*60}")
        print(f" INSTAGRAM EVENT EXTRACTION PIPELINE")
        print(f"{'='*60}")
        print(f"  • AI Model: {model_name}")
        print(f"  • Parallel Workers: {self.max_workers}")
        print(f"  • Rate Limit Delay: {self.rate_limit_delay}s")
        self.gemini_model = genai.GenerativeModel(model_name)

        self.setup_vision()

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
        anomaly file lets you audit silent misses without re-scraping."""
        with self.lock:
            self.processed_posts.add(pid)
            entry = {
                'result': result,
                'account': user,
                'post_date': post_date_str,
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
        self.create_final_report()
        sys.exit(0)

    def emergency_save(self):
        with self.lock:
            if self.results:
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
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

                default_header = ["Post ID", "Account", "Date Processed", "Source", "Notes", "Result", "Post Date"]
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
                if needs_update:
                    self.log_worksheet.update([updated_header], value_input_option='RAW')
                    print(f"✓ Updated Processed_Log header: {updated_header}")

                read_header = header if header else updated_header
                result_col = read_header.index("Result") if "Result" in read_header else None
                post_date_col = read_header.index("Post Date") if "Post Date" in read_header else None
                date_processed_col = read_header.index("Date Processed") if "Date Processed" in read_header else 1

                _DATE_RE = re.compile(r'^\d{4}-\d{2}-\d{2}$')
                acct_in_col1 = "Account" in updated_header and updated_header.index("Account") == 1
                OLD_RESULT_COL, OLD_POST_DATE_COL, OLD_DATE_PROCESSED_COL = 4, 5, 1

                max_age_days = int(CONF.get("history_max_age_days", 30))
                cutoff = (datetime.now() - timedelta(days=max_age_days)).date()

                retry_tags = {'ocr_failed', 'gemini_error'}
                skip_count = 0
                retry_count = 0
                for row in data_rows:
                    if not row:
                        continue
                    pid = row[0].strip() if row else ''
                    if not pid:
                        continue

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

                    if row_date and row_date < cutoff:
                        self.processed_posts.add(pid)
                        skip_count += 1
                        continue

                    if result_tag in retry_tags:
                        retry_count += 1
                        continue

                    self.processed_posts.add(pid)
                    skip_count += 1

                print(f"✓ History Loaded: Skipping {skip_count} IDs, {retry_count} eligible for retry (ocr_failed/gemini_error).")
            except gspread.WorksheetNotFound:
                self.log_worksheet = self.main_sheet.add_worksheet("Processed_Log", 5000, 7)
                self.log_worksheet.append_row(["Post ID", "Account", "Date Processed", "Source", "Notes", "Result", "Post Date"])
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

        print(f"    ↳ Downloading image from CDN...")

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
            }
            response = requests.get(image_url, headers=headers, timeout=15)

            if response.status_code != 200:
                print(f"    ✗ Image download failed: Status {response.status_code}")
                with self.lock:
                    self.stats['download_errors'] += 1
                    self.stats['ocr_failed'] += 1
                return ""

            print(f"    ✓ Image downloaded ({len(response.content)} bytes)")
            print(f"    ↳ Calling Vision API for OCR...")

            time.sleep(self.rate_limit_delay)

            image = vision.Image(content=response.content)
            response_ocr = self.vision_client.text_detection(image=image)

            if response_ocr.error.message:
                error_msg = response_ocr.error.message
                print(f"    ✗ Vision API error: {error_msg}")
                with self.lock:
                    self.stats['ocr_failed'] += 1
                    if error_msg not in self.stats['vision_errors']:
                        self.stats['vision_errors'][error_msg] = 0
                    self.stats['vision_errors'][error_msg] += 1
                if 'quota' in error_msg.lower() or '429' in error_msg:
                    with self.lock:
                        self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)
                    print(f"    ⚠ Rate limited - increasing delay to {self.rate_limit_delay:.1f}s")
                return ""

            if response_ocr.text_annotations:
                ocr_text = response_ocr.text_annotations[0].description
                sample = ocr_text[:100].replace('\n', ' ')
                print(f"    ✓ OCR SUCCESS! Extracted {len(ocr_text)} characters")
                print(f"    📝 Sample: {sample}...")
                with self.lock:
                    self.stats['ocr_success'] += 1
                    self.successful_ocr.append(post_id)
                return ocr_text
            else:
                print(f"    ⚠ No text found in image")
                with self.lock:
                    self.stats['ocr_failed'] += 1

        except requests.exceptions.Timeout:
            print(f"    ✗ Image download timeout")
            with self.lock:
                self.stats['download_errors'] += 1
                self.stats['ocr_failed'] += 1

        except requests.exceptions.RequestException as e:
            print(f"    ✗ Image download error: {str(e)[:100]}")
            with self.lock:
                self.stats['download_errors'] += 1
                self.stats['ocr_failed'] += 1

        except Exception as e:
            error_str = str(e)[:150]
            print(f"    ✗ OCR exception: {error_str}")
            with self.lock:
                self.stats['ocr_failed'] += 1
                self.failed_ocr.append(post_id)
            if '429' in error_str or 'quota' in error_str.lower():
                with self.lock:
                    self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)
                print(f"    ⚠ Increasing delay to {self.rate_limit_delay:.1f}s")

        return ""

    def process_post(self, post, post_num, total):
        pid = post.get('id') or post.get('shortCode') or f'post_{post_num}'

        # Atomic check-and-claim: add pid to the dedup set inside the same
        # lock block as the existence check. Without this, two concurrent
        # threads with the same pid (e.g. from a duplicated source list) can
        # both pass the check before either thread reaches the
        # results.extend / post_results write 10-30 seconds later, producing
        # duplicate event rows in All_Events. See ADR 0006.
        with self.lock:
            if pid in self.processed_posts:
                self.stats['skipped_history'] += 1
                print(f"  [{post_num}/{total}] @{post.get('ownerUsername', '')} | {pid} - Already processed, skipping")
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
                post_date = datetime.now()
        except Exception:
            post_date = datetime.now()
        post_date_str = post_date.strftime('%Y-%m-%d')

        print(f"\n[{post_num}/{total}] Processing post: {pid}")
        print(f"  ↳ Account: @{user} ({owner_full_name})")

        if not caption and not display_url:
            print(f"  ⚠ No caption or image URL - skipping")
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
        if self.vision_enabled:
            all_urls = self.collect_carousel_urls(post)
            if all_urls:
                num_slides = len(all_urls)
                if num_slides > 1:
                    print(f"  📸 CAROUSEL: {num_slides} slides detected — OCR'ing all")
                else:
                    print(f"  ↳ Found image URL")
                if num_slides > 1:
                    with self.lock:
                        self.stats['carousel_posts'] += 1
                slide_texts = []
                for idx, url in enumerate(all_urls, 1):
                    if num_slides > 1:
                        print(f"    ─── Slide {idx}/{num_slides} ───")
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
                    print(f"  ✓ Carousel OCR complete: {len(slide_texts)}/{num_slides} slides had text")
            else:
                print(f"  ⚠ No image URL found - relying on text fields")
        else:
            print(f"  ⚠ Vision API disabled - relying on text fields only")

        has_ocr = bool(ocr_text)
        ocr_attempted_and_failed = self.vision_enabled and has_image and not has_ocr
        print(f"  ↳ Data available: caption={has_caption}, location={has_location}, image={has_image}, OCR={has_ocr}")

        all_text = (caption + ' ' + ocr_text).lower()
        calendar_keywords = ['calendar', 'schedule', 'lineup', 'weekly', 'monthly',
                           'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
                           'saturday', 'sunday', 'every', 'recurring']
        might_be_calendar = any(keyword in all_text for keyword in calendar_keywords)
        if might_be_calendar:
            print(f"  📅 Possible calendar/multi-event post detected")

        time.sleep(self.rate_limit_delay)

        print(f"  ↳ Analyzing with Gemini AI (checking for multiple events)...")

        prompt = f"""
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

        DATE PARSING — handle ALL of these formats and convert to YYYY-MM-DD:
        - Shorthand with dots: "3.13.26" or "3.13" → use POST DATE year if year omitted
        - Shorthand with slashes: "3/13", "3/13/26", "03/13/2026"
        - Written out: "March 13th", "March 13", "Mar 13"
        - Day references: "this Saturday", "next Friday", "this weekend" → calculate exact date from POST DATE
        - Day + date: "Saturday the 13th", "Friday, April 4th"
        - Relative: "tomorrow", "tonight" → calculate from POST DATE
        - Month only with context: "this March" → use the month with POST DATE year
        - Year shorthand: "26" means 2026, "25" means 2025

        RECURRING EVENTS — if the post describes a recurring event with no specific one-time date:
        - "Every Saturday", "Every weekend", "Every Friday night", "Weekly Thursdays"
        - "Industry Mondays", "Brunch every Sunday", "EVERY SATURDAY & SUNDAY"
        → Calculate the NEXT upcoming occurrence of that day ON OR AFTER the POST DATE and use that as the date
        → Set "is_recurring": true in the event object
        → Example: POST DATE is Wednesday 2026-03-10, event is "Every Saturday" → date = 2026-03-14

        REQUIREMENTS:
        1. "event_name": Maximum 40 characters. If the natural event name exceeds 40 characters,
           create a shorter, marketable version that captures the essence.
           Examples: "CULTR One Year Anniversary" not "CULTR ONE YEAR ANNIVERSARY CELEBRATION PARTY"
                     "Jazz & Jamz Night" not "Jesus, Jazz & Jamz Community Celebration Evening"
        2. "newsletter_description": Create a "HYPE_LINE" - a one-sentence, punchy teaser for a newsletter.
           Example: "Kick off your weekend with live jazz downtown!"
        3. "section_of_nj": North/Central/South based on city/county:
           North = Bergen/Essex/Hudson/Morris/Passaic/Sussex/Warren
           Central = Hunterdon/Mercer/Middlesex/Monmouth/Somerset/Union
           South = Atlantic/Burlington/Camden/Cape May/Cumberland/Gloucester/Ocean/Salem
        4. TIME: Strict 12-hour format (e.g. 2:00 PM).

        Return JSON with "events" list containing:
        event_name, date (YYYY-MM-DD), start_time, venue_name, city, section_of_nj,
        newsletter_description, event_type, description, performer, price, confidence, is_recurring

        Also include:
        "total_events_found": number,
        "is_calendar_post": true/false

        If no events found, return: {{"events": [], "total_events_found": 0}}
        """

        try:
            resp = self.gemini_model.generate_content(prompt)
            if not resp:
                print(f"  ✗ Gemini returned empty response")
                self._record_post_outcome(pid, 'gemini_error', user, post_date_str,
                                          caption=caption, ocr_text=ocr_text,
                                          error='gemini_returned_none')
                with self.lock:
                    self.stats['gemini_errors'] += 1
                    self.stats['processed'] += 1
                return None

            try:
                text = resp.text.strip()
            except (AttributeError, ValueError) as e:
                print(f"  ✗ Gemini response error: {e}")
                self._record_post_outcome(pid, 'gemini_error', user, post_date_str,
                                          caption=caption, ocr_text=ocr_text,
                                          error=f'response_text_access_failed: {e}')
                with self.lock:
                    self.stats['gemini_errors'] += 1
                    self.stats['processed'] += 1
                return None

            clean_json = re.sub(r'```json\s*|```', '', text).strip()

            try:
                data = json.loads(clean_json)
            except json.JSONDecodeError:
                json_match = re.search(r'\{.*\}', clean_json, re.DOTALL)
                if json_match:
                    try:
                        data = json.loads(json_match.group())
                    except Exception as parse_e:
                        print(f"  ✗ Could not parse Gemini JSON response")
                        self._record_post_outcome(pid, 'gemini_error', user, post_date_str,
                                                  caption=caption, ocr_text=ocr_text,
                                                  gemini_raw=clean_json,
                                                  error=f'json_parse_failed_after_regex: {parse_e}')
                        with self.lock:
                            self.stats['gemini_errors'] += 1
                            self.stats['processed'] += 1
                        return None
                else:
                    print(f"  ✗ No valid JSON in Gemini response")
                    self._record_post_outcome(pid, 'gemini_error', user, post_date_str,
                                              caption=caption, ocr_text=ocr_text,
                                              gemini_raw=clean_json,
                                              error='no_json_brace_in_response')
                    with self.lock:
                        self.stats['gemini_errors'] += 1
                        self.stats['processed'] += 1
                    return None

            events = data.get('events', [])
            is_calendar = data.get('is_calendar_post', False)

            if not events:
                print(f"  ↳ No events found in this post")
                result_tag = 'ocr_failed' if ocr_attempted_and_failed else 'no_events_found'
                reason = ('gemini_returned_empty_events_after_failed_ocr' if ocr_attempted_and_failed
                          else 'gemini_returned_empty_events_array')
                self._record_post_outcome(pid, result_tag, user, post_date_str,
                                          caption=caption, ocr_text=ocr_text,
                                          gemini_raw=clean_json,
                                          error=reason)
                with self.lock:
                    self.stats['processed'] += 1
                    self.stats['posts_no_events'] += 1
                return None

            processed_events = []
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
                e['start_time'] = self.clean_time(e.get('start_time'))
                e['post_id'] = pid
                e['had_ocr'] = has_ocr
                e['from_calendar'] = is_calendar
                processed_events.append(e)

            _result_tag = 'events_found' if processed_events else 'no_events_found'
            if processed_events:
                self._record_post_outcome(pid, _result_tag, user, post_date_str)
            else:
                self._record_post_outcome(pid, _result_tag, user, post_date_str,
                                          caption=caption, ocr_text=ocr_text,
                                          gemini_raw=clean_json,
                                          error='gemini_returned_events_but_all_filtered')
            with self.lock:
                self.stats['processed'] += 1
                if processed_events:
                    self.stats['posts_with_events'] += 1
                    self.stats['events_found'] += len(processed_events)
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
                        print(f"  ↳ Checkpoint saved ({self.stats['events_found']} total events)")
                else:
                    self.stats['posts_no_events'] += 1

            if processed_events:
                if len(processed_events) > 1:
                    print(f"  🎉 MULTIPLE EVENTS FOUND: {len(processed_events)} events extracted!")
                    for idx, event in enumerate(processed_events, 1):
                        recurring_tag = " ♻ recurring" if event.get('is_recurring') else ""
                        print(f"    {idx}. {event.get('event_name', 'Unnamed')}{recurring_tag}")
                        if event.get('date'):
                            print(f"       Date: {event['date']}")
                        if event.get('venue_name'):
                            print(f"       Venue: {event['venue_name']}")
                        print(f"       Confidence: {event.get('confidence', 'unknown')}")
                else:
                    event = processed_events[0]
                    recurring_tag = " ♻ recurring" if event.get('is_recurring') else ""
                    print(f"  ✓ EVENT FOUND: {event.get('event_name', 'Unnamed')}{recurring_tag}")
                    print(f"    • Date: {event.get('date', 'N/A')}")
                    print(f"    • Venue: {event.get('venue_name', 'N/A')}")
                    print(f"    • Confidence: {event.get('confidence', 'unknown')}")
            else:
                print(f"  ↳ No valid events in this post")

            return processed_events

        except Exception as e:
            error_str = str(e)
            print(f"  ✗ Gemini error: {error_str[:200]}")
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
                print(f"  ⚠ Rate limited - delay increased to {self.rate_limit_delay:.1f}s")
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
                futures = {
                    executor.submit(self.process_post, post, i + 1, total): i
                    for i, post in enumerate(posts)
                }
                for future in as_completed(futures):
                    try:
                        future.result(timeout=120)
                    except Exception as e:
                        print(f"⚠ Worker error: {e}")
        else:
            print("\n📋 Processing sequentially...\n")
            for i, post in enumerate(posts):
                self.process_post(post, i + 1, total)

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

        if self.post_results:
            from collections import Counter
            tag_counts = Counter(info.get('result', '') for info in self.post_results.values())
            print(f"\n🏷  RESULT TAG BREAKDOWN:")
            print(f"  • events_found:    {tag_counts.get('events_found', 0)}")
            print(f"  • no_events_found: {tag_counts.get('no_events_found', 0)}")
            print(f"  • ocr_failed:      {tag_counts.get('ocr_failed', 0)}")
            print(f"  • gemini_error:    {tag_counts.get('gemini_error', 0)}")

    def save_data(self, events, post_log):
        if post_log and self.sheets_client and self.log_worksheet:
            try:
                date_processed = str(datetime.now().date())
                log_rows = [
                    [pid, info.get('account', ''), date_processed, "Auto-Bot", "",
                     info.get('result', ''), info.get('post_date', '')]
                    for pid, info in post_log.items()
                ]
                self.log_worksheet.append_rows(log_rows)
                tag_counts = {}
                for info in post_log.values():
                    tag = info.get('result', '')
                    tag_counts[tag] = tag_counts.get(tag, 0) + 1
                summary = ', '.join(f"{v} {k}" for k, v in sorted(tag_counts.items()))
                print(f"✓ Logged {len(post_log)} post IDs to Processed_Log ({summary})")
            except Exception as e:
                print(f"⚠ Processed_Log write error: {e}")

        if not events:
            print("No new events found.")
            return

        df = pd.DataFrame(events)

        cols = [
            'instagram_handle', 'event_name', 'date', 'start_time',
            'venue_name', 'city', 'section_of_nj', 'newsletter_description',
            'instagram_post_url', 'display_url', 'post_url', 'instagram_profile_url',
            'event_type', 'account_name', 'description', 'performer', 'price',
            'confidence', 'post_id', 'had_ocr', 'from_calendar', 'is_recurring',
            'processed_timestamp'
        ]

        df['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
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

        if self.sheets_client and self.main_sheet:
            try:
                try:
                    evt_sheet = self.main_sheet.worksheet("All_Events")
                except gspread.WorksheetNotFound:
                    evt_sheet = self.main_sheet.add_worksheet("All_Events", 5000, 25)
                    evt_sheet.append_row(df.columns.tolist())

                def sanitize(val):
                    # gspread serializes via JSON; NaN/Inf are not JSON-compliant
                    # and cause the entire append to fail. Coerce them to empty string.
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

                rows = [[sanitize(v) for v in row] for row in df.values.tolist()]
                evt_sheet.append_rows(rows)
                print(f"✓ Uploaded {len(events)} events to Google Sheets")
            except Exception as e:
                print(f"⚠ Sheets Upload Error: {e}")

        try:
            recurring_accounts.refresh(self.main_sheet)
        except Exception as e:
            print(f"⚠ Reliable Accounts refresh failed: {e}")

    def start_scheduler(self):
        target_day = CONF["schedule_day"]
        target_time = CONF["schedule_time"]

        print(f"\n⏰ BOT STARTED.")
        print(f"   Target: Every {target_day} at {target_time}")
        print("   Status: Waiting...")

        while True:
            now = datetime.now()
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
    newer_than_date = (datetime.now() - timedelta(days=newer_than_days)).strftime("%Y-%m-%d")
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
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
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


def do_force_run(bot):
    print("\n🚀 Force run initiated...\n")
    run_started_at = datetime.now()
    bot.setup_sheets()

    # Try live Apify scrape first; fall back to static URL if disabled or failed
    data = []
    if CONF.get("apify_enabled", True):
        data = fetch_posts_via_apify()

    if not data:
        url = CONF.get("instagram_data_url", "")
        if url:
            print("  ↳ Using static instagram_data_url as data source")
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            data = response.json()
        else:
            print("❌ No posts fetched — Apify returned nothing and no instagram_data_url is set.")
            return

    offset = CONF.get("post_offset", 0)
    cap    = CONF.get("max_posts", 0)
    if offset:
        data = data[offset:]
        print(f"  • Post offset: skipping first {offset} posts")
    if cap:
        data = data[:cap]
        print(f"  • Post cap: processing up to {cap} posts")

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
