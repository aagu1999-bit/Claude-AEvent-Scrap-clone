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
from datetime import datetime
from pathlib import Path
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import gspread
from oauth2client.service_account import ServiceAccountCredentials

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


class InstagramEventPipeline:
    def __init__(self):
        self.processed_posts = set()
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
        signal.signal(signal.SIGINT, self.handle_interrupt)

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
                existing_ids = self.log_worksheet.col_values(1)[1:]
                self.processed_posts.update(existing_ids)
                print(f"✓ History Loaded: Skipping {len(existing_ids)} previously processed IDs.")
            except gspread.WorksheetNotFound:
                self.log_worksheet = self.main_sheet.add_worksheet("Processed_Log", 5000, 5)
                self.log_worksheet.append_row(["Post ID", "Date Processed", "Source", "Notes"])
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

        with self.lock:
            if pid in self.processed_posts:
                self.stats['skipped_history'] += 1
                print(f"  [{post_num}/{total}] @{post.get('ownerUsername', '')} | {pid} - Already processed, skipping")
                return None

        caption = post.get('caption', '') or post.get('text', '')
        user = post.get('ownerUsername', '')
        owner_full_name = post.get('ownerFullName', '')
        shortcode = post.get('shortCode', '') or post.get('shortcode', '')
        display_url = post.get('displayUrl', '') or post.get('display_url', '')
        location_name = post.get('locationName', '') or post.get('location', '')

        print(f"\n[{post_num}/{total}] Processing post: {pid}")
        print(f"  ↳ Account: @{user} ({owner_full_name})")

        if not caption and not display_url:
            print(f"  ⚠ No caption or image URL - skipping")
            with self.lock:
                self.processed_posts.add(pid)
                self.stats['processed'] += 1
                self.stats['skipped_no_data'] += 1
            return None

        has_caption = bool(caption)
        has_location = bool(location_name)
        has_image = bool(display_url)

        ocr_text = ""
        if self.vision_enabled and display_url:
            print(f"  ↳ Found image URL")
            ocr_text = self.extract_ocr_text(display_url, pid)
        elif self.vision_enabled:
            print(f"  ⚠ No image URL found - relying on text fields")
        else:
            print(f"  ⚠ Vision API disabled - relying on text fields only")

        has_ocr = bool(ocr_text)
        print(f"  ↳ Data available: caption={has_caption}, location={has_location}, image={has_image}, OCR={has_ocr}")

        all_text = (caption + ' ' + ocr_text).lower()
        calendar_keywords = ['calendar', 'schedule', 'lineup', 'weekly', 'monthly',
                           'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
                           'saturday', 'sunday', 'every', 'recurring']
        might_be_calendar = any(keyword in all_text for keyword in calendar_keywords)
        if might_be_calendar:
            print(f"  📅 Possible calendar/multi-event post detected")

        time.sleep(self.rate_limit_delay)

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

        print(f"  ↳ Analyzing with Gemini AI (checking for multiple events)...")

        prompt = f"""
        Extract ALL events from this Instagram post. A post may contain MULTIPLE events.

        POST DATE: {post_date.strftime('%Y-%m-%d')} (use this to resolve relative and recurring dates)
        ACCOUNT: @{user} ({owner_full_name})
        LOCATION TAG: {location_name}

        CAPTION: {caption[:2000]}
        OCR TEXT FROM IMAGE: {ocr_text[:3000]}

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
        1. "newsletter_description": Create a "HYPE_LINE" - a one-sentence, punchy teaser for a newsletter.
           Example: "Kick off your weekend with live jazz downtown!"
        2. "section_of_nj": North/Central/South based on city/county:
           North = Bergen/Essex/Hudson/Morris/Passaic/Sussex/Warren
           Central = Hunterdon/Mercer/Middlesex/Monmouth/Somerset/Union
           South = Atlantic/Burlington/Camden/Cape May/Cumberland/Gloucester/Ocean/Salem
        3. TIME: Strict 12-hour format (e.g. 2:00 PM).

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
                with self.lock:
                    self.stats['gemini_errors'] += 1
                return None

            try:
                text = resp.text.strip()
            except (AttributeError, ValueError) as e:
                print(f"  ✗ Gemini response error: {e}")
                with self.lock:
                    self.stats['gemini_errors'] += 1
                return None

            clean_json = re.sub(r'```json\s*|```', '', text).strip()

            try:
                data = json.loads(clean_json)
            except json.JSONDecodeError:
                json_match = re.search(r'\{.*\}', clean_json, re.DOTALL)
                if json_match:
                    try:
                        data = json.loads(json_match.group())
                    except Exception:
                        print(f"  ✗ Could not parse Gemini JSON response")
                        with self.lock:
                            self.stats['gemini_errors'] += 1
                        return None
                else:
                    print(f"  ✗ No valid JSON in Gemini response")
                    with self.lock:
                        self.stats['gemini_errors'] += 1
                    return None

            events = data.get('events', [])
            is_calendar = data.get('is_calendar_post', False)

            if not events:
                print(f"  ↳ No events found in this post")
                with self.lock:
                    self.processed_posts.add(pid)
                    self.stats['processed'] += 1
                    self.stats['posts_no_events'] += 1
                return None

            processed_events = []
            for e in events:
                if not e.get('event_name') and not e.get('date'):
                    continue
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

            with self.lock:
                self.processed_posts.add(pid)
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

                    if self.stats['events_found'] % 25 == 0:
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
            with self.lock:
                self.stats['gemini_errors'] += 1
                self.processed_posts.add(pid)
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

        new_ids = [r['post_id'] for r in self.results if 'post_id' in r]
        return self.results, list(set(new_ids))

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

    def save_data(self, events, new_ids):
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

        if self.sheets_client and self.log_worksheet and self.main_sheet:
            try:
                if new_ids:
                    log_rows = [[uid, str(datetime.now().date()), "Auto-Bot", ""] for uid in new_ids]
                    self.log_worksheet.append_rows(log_rows)
                    print(f"✓ Logged {len(new_ids)} post IDs to Processed_Log")

                try:
                    evt_sheet = self.main_sheet.worksheet("All_Events")
                except gspread.WorksheetNotFound:
                    evt_sheet = self.main_sheet.add_worksheet("All_Events", 5000, 25)
                    evt_sheet.append_row(df.columns.tolist())

                def sanitize(val):
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

                try:
                    url = CONF["instagram_data_url"]
                    if url:
                        response = requests.get(url, timeout=120)
                        response.raise_for_status()
                        raw_posts = response.json()
                    else:
                        print("⚠ No instagram_data_url in config.")
                        raw_posts = []

                    self.setup_sheets()
                    events, ids = self.run_pipeline(raw_posts)
                    self.save_data(events, ids)

                    print("✅ Run complete. Sleeping until next window...")
                    time.sleep(70)
                except Exception as e:
                    print(f"❌ Run Failed: {e}")
                    import traceback
                    traceback.print_exc()
                    time.sleep(60)

            time.sleep(10)


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
    bot.setup_sheets()
    url = CONF["instagram_data_url"]
    if url:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()
        events, ids = bot.run_pipeline(data)
        bot.save_data(events, ids)
    else:
        print("❌ No instagram_data_url configured.")


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
