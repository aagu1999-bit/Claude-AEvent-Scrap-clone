#!/usr/bin/env python3
"""
Instagram Event Extraction Pipeline - Final
Features: Config File, Gemini 2.5 Flash Lite, Service Account, Strict Formatting
"""

import os
import sys
import json
import pandas as pd
import requests
import time
import re
from datetime import datetime
from pathlib import Path
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURATION LOADER ---
def load_configuration():
    # 1. Default Settings
    config = {
        "schedule_day": "Thursday",
        "schedule_time": "14:00",
        "sheet_name": "Instagram_Events_Master",
        "gemini_model": "gemini-2.5-flash-lite", # The 2.5 Lite model
        "instagram_data_url": ""
    }

    # 2. Load from config.json (Overrides defaults)
    if os.path.exists("config.json"):
        try:
            with open("config.json", "r") as f:
                file_config = json.load(f)
                config.update(file_config)
            print("✓ Loaded settings from config.json")
        except Exception as e:
            print(f"⚠ Error loading config.json: {e}")

    # 3. Load from Secrets (Overrides config.json - useful for testing)
    if os.environ.get("SCHEDULE_DAY"): config["schedule_day"] = os.environ.get("SCHEDULE_DAY")
    if os.environ.get("SCHEDULE_TIME"): config["schedule_time"] = os.environ.get("SCHEDULE_TIME")
    if os.environ.get("DATA_URL"): config["instagram_data_url"] = os.environ.get("DATA_URL")

    return config

# Load Config Globally
CONF = load_configuration()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"

# --- SETUP LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%I:%M:%S %p')

# Import Google AI
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

class InstagramEventPipeline:
    def __init__(self):
        self.processed_posts = set()
        self.sheets_client = None
        self.main_sheet = None
        self.log_worksheet = None
        self.output_dir = Path("outputs")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup Gemini
        if not GEMINI_API_KEY:
            print("❌ ERROR: GEMINI_API_KEY not found in Secrets!")
            # Don't exit here to allow scheduler to wait if key is added later
            # But for main script we need it
        else:
            genai.configure(api_key=GEMINI_API_KEY)

        # Use the model defined in Config
        model_name = CONF["gemini_model"]
        print(f"✓ Using AI Model: {model_name}")
        self.gemini_model = genai.GenerativeModel(model_name)

    def setup_sheets(self):
        """Connect to Sheets and LOAD HISTORY"""
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            print(f"⚠ No {SERVICE_ACCOUNT_FILE} found. Running in offline mode.")
            return

        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
            self.sheets_client = gspread.authorize(creds)

            sheet_name = CONF["sheet_name"]

            # Open Sheet
            try:
                self.main_sheet = self.sheets_client.open(sheet_name)
                print(f"✓ Connected to Sheet: {sheet_name}")
            except gspread.SpreadsheetNotFound:
                print(f"❌ Error: Could not find Google Sheet named '{sheet_name}'")
                print(f"   Please create it manually and share it with: {creds.service_account_email}")
                return

            # Load History
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
        """Strict X:XX AM/PM format"""
        if not time_str: return ""
        try:
            dt = None
            for fmt in ['%H:%M', '%I:%M %p', '%I:%M%p', '%I %p', '%H:%M:%S']:
                try:
                    dt = datetime.strptime(str(time_str).strip().upper(), fmt)
                    break
                except ValueError: continue

            if dt:
                formatted = dt.strftime('%I:%M %p')
                return formatted.lstrip('0')
        except:
            pass
        return str(time_str).upper()

    def analyze_post(self, post):
        """AI Analysis"""
        caption = post.get('caption', '') or post.get('text', '')
        user = post.get('ownerUsername', '')
        pid = post.get('id') or post.get('shortCode')

        prompt = f"""
        Extract events from this Instagram post.

        CAPTION: {caption}
        ACCOUNT: {user}
        DATE: {post.get('timestamp', '')}

        REQUIREMENTS:
        1. "newsletter_description": Create a "HYPE_LINE". A one-sentence, punchy teaser for a newsletter. 
           Example: "Kick off your weekend with live jazz downtown!"
        2. "section_of_nj": North/Central/South (Based on city/county).
        3. TIME: Strict 12-hour format (e.g. 2:00 PM).

        Return JSON with "events" list containing:
        event_name, date (YYYY-MM-DD), start_time, venue_name, city, section_of_nj, newsletter_description, event_type, description
        """

        try:
            resp = self.gemini_model.generate_content(prompt)
            clean_json = re.sub(r'```json\s*|```', '', resp.text).strip()
            data = json.loads(clean_json)

            events = []
            for e in data.get('events', []):
                e['instagram_handle'] = user
                e['instagram_post_url'] = f"https://www.instagram.com/p/{post.get('shortCode', '')}/"
                e['display_url'] = post.get('displayUrl', '')
                e['post_url'] = post.get('url', '')
                e['instagram_profile_url'] = f"https://www.instagram.com/{user}/"
                e['account_name'] = post.get('ownerFullName', '')
                e['start_time'] = self.clean_time(e.get('start_time'))
                events.append(e)
            return events
        except Exception:
            return []

    def run_pipeline(self, posts):
        """Process batch"""
        new_events = []
        new_ids = []

        print(f"Scanning {len(posts)} posts...")

        for post in posts:
            pid = post.get('id') or post.get('shortCode')
            if pid in self.processed_posts: continue 

            print(f"  🔎 Analyzing: {pid}")
            events = self.analyze_post(post)

            if events:
                new_events.extend(events)
                new_ids.append(pid)
                self.processed_posts.add(pid)

            time.sleep(1)

        return new_events, new_ids

    def save_data(self, events, new_ids):
        """Save with strict formatting"""
        if not events:
            print("No new events found.")
            return

        df = pd.DataFrame(events)

        # 1. COLUMN ORDER
        cols = [
            'instagram_handle', 'event_name', 'location', 'date', 'start_time',
            'venue_name', 'city', 'section_of_nj', 'newsletter_description',
            'instagram_post_url', 'display_url', 'post_url', 'instagram_profile_url',
            'event_type', 'account_name', 'description'
        ]

        # Ensure cols exist
        for c in cols:
            if c not in df.columns: df[c] = ""

        # Select/Reorder
        df = df[[c for c in cols if c in df.columns]]

        # 2. UPPERCASE
        df = df.applymap(lambda x: str(x).upper() if isinstance(x, str) else x)
        df.columns = [c.upper().replace('_', ' ') for c in df.columns]

        # 3. CSV SAVE
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_file = self.output_dir / f"Events_{ts}.csv"
        df.to_csv(csv_file, index=False)
        print(f"✓ Saved CSV: {csv_file}")

        # 4. SHEET SYNC
        if self.sheets_client:
            try:
                # Log IDs
                if new_ids:
                    log_rows = [[uid, str(datetime.now().date()), "Auto-Bot", ""] for uid in new_ids]
                    self.log_worksheet.append_rows(log_rows)

                # Append Events
                try:
                    evt_sheet = self.main_sheet.worksheet("All_Events")
                except:
                    evt_sheet = self.main_sheet.add_worksheet("All_Events", 1000, 20)
                    evt_sheet.append_row(df.columns.tolist())

                evt_sheet.append_rows(df.values.tolist())
                print(f"✓ Uploaded {len(events)} events to Sheets")
            except Exception as e:
                print(f"⚠ Sheets Upload Error: {e}")

    def start_scheduler(self):
        """Scheduler Loop"""
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
                        raw_posts = requests.get(url).json()
                    else:
                        print("⚠ No instagram_data_url in config. Using empty list.")
                        raw_posts = []

                    self.setup_sheets()
                    events, ids = self.run_pipeline(raw_posts)
                    self.save_data(events, ids)

                    print("✅ Complete. Sleeping...")
                    time.sleep(70)
                except Exception as e:
                    print(f"❌ Run Failed: {e}")
                    time.sleep(60)

            time.sleep(15)

if __name__ == "__main__":
    bot = InstagramEventPipeline()
    if "--now" in sys.argv:
        print("Force run initiated...")
        bot.setup_sheets()
        url = CONF["instagram_data_url"]
        if url:
             data = requests.get(url).json()
             e, i = bot.run_pipeline(data)
             bot.save_data(e, i)
    else:
        bot.start_scheduler()