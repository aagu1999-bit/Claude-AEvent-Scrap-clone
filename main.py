#!/usr/bin/env python3
"""
Instagram Event Extraction Pipeline
Features: Parallel Processing (3 workers), Config File, Gemini 2.0 Flash Lite,
          Google Sheets Sync, Scheduler, OCR Support
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
        "max_workers": 3,
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

    config["max_workers"] = min(5, max(1, int(config.get("max_workers", 3))))
    config["rate_limit_delay"] = max(0.1, float(config.get("rate_limit_delay", 0.5)))

    return config

CONF = load_configuration()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"


class InstagramEventPipeline:
    def __init__(self):
        self.processed_posts = set()
        self.results = []
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
            'events_found': 0,
            'posts_with_events': 0,
            'multi_event_posts': 0,
            'gemini_errors': 0,
            'ocr_success': 0,
            'ocr_failed': 0,
            'download_errors': 0,
        }

        if not GEMINI_API_KEY:
            print("❌ ERROR: GEMINI_API_KEY not found in Secrets!")
        else:
            genai.configure(api_key=GEMINI_API_KEY)

        model_name = CONF["gemini_model"]
        print(f"✓ Using AI Model: {model_name}")
        print(f"✓ Parallel Workers: {self.max_workers}")
        print(f"✓ Rate Limit Delay: {self.rate_limit_delay}s")
        self.gemini_model = genai.GenerativeModel(model_name)

        self.setup_vision()

        atexit.register(self.emergency_save)
        signal.signal(signal.SIGINT, self.handle_interrupt)

    def handle_interrupt(self, signum, frame):
        print("\n\n⚠ INTERRUPT DETECTED - Saving all data...")
        self.emergency_save()
        sys.exit(0)

    def emergency_save(self):
        with self.lock:
            if self.results:
                ts = datetime.now().strftime('%Y%m%d_%H%M%S')
                df = pd.DataFrame(self.results)
                csv_file = self.output_dir / f'emergency_events_{ts}.csv'
                df.to_csv(csv_file, index=False)
                print(f"✓ Emergency CSV saved: {csv_file}")
                self.save_checkpoint()

    def setup_vision(self):
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print("⚠ No service account file found. OCR disabled.")
            return

        try:
            credentials = service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=['https://www.googleapis.com/auth/cloud-vision']
            )
            self.vision_client = vision.ImageAnnotatorClient(credentials=credentials)
            self.vision_enabled = True
            print("✓ Vision API (OCR) enabled")
        except Exception as e:
            print(f"⚠ Vision API setup failed: {e}")

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

        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            }
            response = requests.get(image_url, headers=headers, timeout=15)
            if response.status_code != 200:
                with self.lock:
                    self.stats['download_errors'] += 1
                return ""

            time.sleep(self.rate_limit_delay)

            image = vision.Image(content=response.content)
            response_ocr = self.vision_client.text_detection(image=image)

            if response_ocr.error.message:
                with self.lock:
                    self.stats['ocr_failed'] += 1
                return ""

            if response_ocr.text_annotations:
                ocr_text = response_ocr.text_annotations[0].description
                with self.lock:
                    self.stats['ocr_success'] += 1
                return ocr_text
            else:
                with self.lock:
                    self.stats['ocr_failed'] += 1

        except Exception:
            with self.lock:
                self.stats['ocr_failed'] += 1

        return ""

    def process_post(self, post, post_num, total):
        pid = post.get('id') or post.get('shortCode') or f'post_{post_num}'

        with self.lock:
            if pid in self.processed_posts:
                self.stats['skipped_history'] += 1
                return None

        caption = post.get('caption', '') or post.get('text', '')
        user = post.get('ownerUsername', '')
        shortcode = post.get('shortCode', '') or post.get('shortcode', '')
        display_url = post.get('displayUrl', '') or post.get('display_url', '')

        if not caption and not display_url:
            with self.lock:
                self.processed_posts.add(pid)
                self.stats['processed'] += 1
            return None

        print(f"  [{post_num}/{total}] @{user} | {pid}")

        ocr_text = ""
        if self.vision_enabled and display_url:
            ocr_text = self.extract_ocr_text(display_url, pid)

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

        prompt = f"""
        Extract ALL events from this Instagram post. A post may contain MULTIPLE events.

        CAPTION: {caption[:2000]}
        ACCOUNT: @{user}
        DATE: {post_date.strftime('%Y-%m-%d')}
        LOCATION TAG: {post.get('locationName', '')}
        OCR TEXT FROM IMAGE: {ocr_text[:3000]}

        REQUIREMENTS:
        1. "newsletter_description": Create a "HYPE_LINE" - a one-sentence, punchy teaser for a newsletter.
           Example: "Kick off your weekend with live jazz downtown!"
        2. "section_of_nj": North/Central/South based on city/county:
           North = Bergen/Essex/Hudson/Morris/Passaic/Sussex/Warren
           Central = Hunterdon/Mercer/Middlesex/Monmouth/Somerset/Union
           South = Atlantic/Burlington/Camden/Cape May/Cumberland/Gloucester/Ocean/Salem
        3. TIME: Strict 12-hour format (e.g. 2:00 PM).
        4. Look for MULTIPLE events - calendars, weekly lineups, event series.

        Return JSON with "events" list containing:
        event_name, date (YYYY-MM-DD), start_time, venue_name, city, section_of_nj,
        newsletter_description, event_type, description, performer, price, confidence

        If no events found, return: {{"events": []}}
        """

        try:
            resp = self.gemini_model.generate_content(prompt)
            if not resp:
                with self.lock:
                    self.stats['gemini_errors'] += 1
                return None

            try:
                text = resp.text.strip()
            except (AttributeError, ValueError):
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
                        with self.lock:
                            self.stats['gemini_errors'] += 1
                        return None
                else:
                    with self.lock:
                        self.stats['gemini_errors'] += 1
                    return None

            events = data.get('events', [])
            if not events:
                with self.lock:
                    self.processed_posts.add(pid)
                    self.stats['processed'] += 1
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
                e['account_name'] = post.get('ownerFullName', '')
                e['start_time'] = self.clean_time(e.get('start_time'))
                e['post_id'] = pid
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

                    if self.stats['events_found'] % 25 == 0:
                        self.save_checkpoint()

            if processed_events:
                print(f"    ✓ {len(processed_events)} event(s) found")
            return processed_events

        except Exception as e:
            error_str = str(e)
            with self.lock:
                self.stats['gemini_errors'] += 1
                self.processed_posts.add(pid)
                self.stats['processed'] += 1
            if '429' in error_str:
                self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)
                print(f"    ⚠ Rate limited - delay increased to {self.rate_limit_delay:.1f}s")
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
        print(f"Vision OCR: {'ENABLED' if self.vision_enabled else 'DISABLED'}")
        print(f"{'='*60}\n")

        start_time = time.time()

        if self.max_workers > 1:
            print(f"⚡ Processing with {self.max_workers} parallel workers...\n")
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
            print("Processing sequentially...\n")
            for i, post in enumerate(posts):
                self.process_post(post, i + 1, total)

        elapsed = time.time() - start_time
        self.save_checkpoint()

        print(f"\n{'='*60}")
        print(f"PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"  Time: {elapsed/60:.1f} minutes ({elapsed:.0f}s)")
        print(f"  Posts processed: {self.stats['processed']}")
        print(f"  Skipped (history): {self.stats['skipped_history']}")
        print(f"  Events found: {self.stats['events_found']}")
        print(f"  Posts with events: {self.stats['posts_with_events']}")
        print(f"  Multi-event posts: {self.stats['multi_event_posts']}")
        print(f"  Gemini errors: {self.stats['gemini_errors']}")
        if self.vision_enabled:
            print(f"  OCR success: {self.stats['ocr_success']}")
            print(f"  OCR failed: {self.stats['ocr_failed']}")

        new_ids = [r['post_id'] for r in self.results if 'post_id' in r]
        return self.results, list(set(new_ids))

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
            'confidence', 'post_id'
        ]

        for c in cols:
            if c not in df.columns:
                df[c] = ""
        df = df[[c for c in cols if c in df.columns]]

        df = df.apply(lambda col: col.map(lambda x: str(x).upper() if isinstance(x, str) else x))
        df.columns = [c.upper().replace('_', ' ') for c in df.columns]

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = self.output_dir / f"Events_{ts}.csv"
        df.to_csv(csv_file, index=False)
        print(f"✓ Saved CSV: {csv_file} ({len(events)} events)")

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

                evt_sheet.append_rows(df.values.tolist())
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


if __name__ == "__main__":
    bot = InstagramEventPipeline()
    if "--now" in sys.argv:
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
    else:
        bot.start_scheduler()
