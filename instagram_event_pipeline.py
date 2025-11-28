#!/usr/bin/env python3
"""
Instagram Event Extraction Pipeline - Claude Environment Version
Adapted from Google Colab version for Claude's Linux environment.
Features: Parallel processing, permanent Instagram URLs, image downloading for OCR
Multi-event extraction with complete error handling
"""

import os
import sys
import json
import pandas as pd
import asyncio
import requests
import time
import base64
from datetime import datetime, timedelta
from pathlib import Path
import logging
import signal
import atexit
import traceback
import re
import pickle
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import threading

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Install requirements
def install_requirements():
    """Install required packages if not present"""
    required = [
        'google-generativeai',
        'google-cloud-vision',
        'pandas',
        'aiohttp',
        'openpyxl',
        'requests'
    ]

    import subprocess
    for package in required:
        package_import_name = package.replace('-', '_').replace('google_generativeai', 'google.generativeai').replace('google_cloud_vision', 'google.cloud.vision')
        try:
            if 'google.generativeai' in package_import_name:
                import google.generativeai
            elif 'google.cloud.vision' in package_import_name:
                from google.cloud import vision
            else:
                __import__(package.replace('-', '_'))
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "--break-system-packages", package])

print("Checking dependencies...")
install_requirements()

# Import after installation
import google.generativeai as genai
from google.cloud import vision
from google.oauth2 import service_account

try:
    import nest_asyncio
    nest_asyncio.apply()
except:
    pass


class InstagramEventPipeline:
    """Optimized pipeline with parallel processing and permanent URLs"""

    def __init__(self, config=None):
        """
        Initialize with configuration dictionary instead of interactive prompts.
        
        Args:
            config (dict): Configuration with the following keys:
                - gemini_api_key (str): Required. Gemini API key
                - vision_json_path (str): Optional. Path to Google Cloud Vision service account JSON
                - instagram_data_url (str): Optional. URL to fetch Instagram JSON data
                - instagram_data_path (str): Optional. Local path to Instagram JSON file
                - instagram_data (list): Optional. Direct list of Instagram post data
                - max_workers (int): Optional. Number of parallel workers (1-5, default 3)
                - rate_limit_delay (float): Optional. Delay between API calls (default 0.5)
                - max_posts (int): Optional. Maximum posts to process (default all)
                - output_dir (str): Optional. Output directory (default current dir)
        """
        self.config = config or {}
        self.processed_posts = set()
        self.results = []
        self.failed_ocr = []
        self.successful_ocr = []
        self.vision_client = None
        self.gemini_model = None
        
        # Configuration with defaults
        self.max_workers = min(5, max(1, self.config.get('max_workers', 3)))
        self.rate_limit_delay = max(0.1, self.config.get('rate_limit_delay', 0.5))
        self.output_dir = Path(self.config.get('output_dir', '/mnt/user-data/outputs'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.checkpoint_file = self.output_dir / 'pipeline_checkpoint.pkl'
        self.lock = threading.Lock()

        # Comprehensive statistics
        self.stats = {
            'total_posts': 0,
            'processed': 0,
            'events_found': 0,
            'posts_with_events': 0,
            'multi_event_posts': 0,
            'max_events_in_post': 0,
            'calendar_posts': 0,
            'ocr_success': 0,
            'ocr_failed': 0,
            'vision_errors': {},
            'gemini_errors': 0,
            'download_errors': 0,
            'parallel_workers': self.max_workers
        }

        # Register cleanup handlers
        atexit.register(self.emergency_save)
        signal.signal(signal.SIGINT, self.handle_interrupt)

    def setup(self):
        """Setup APIs based on configuration"""
        print("\n" + "="*60)
        print(" INSTAGRAM EVENT EXTRACTION PIPELINE")
        print(" Claude Environment Version")
        print("="*60)

        # Step 1: Gemini API
        print("\n[STEP 1] Configuring Gemini API...")
        gemini_key = self.config.get('gemini_api_key')
        if not gemini_key:
            raise ValueError("gemini_api_key is required in config")
        
        try:
            genai.configure(api_key=gemini_key)
            self.gemini_model = genai.GenerativeModel('gemini-2.0-flash')
            test = self.gemini_model.generate_content("Say 'working'")
            if 'working' in test.text.lower():
                print("✓ Gemini API verified!")
            else:
                print("✓ Gemini API connected (test response received)")
        except Exception as e:
            raise ValueError(f"Gemini API setup failed: {e}")

        # Step 2: Vision API (optional)
        print("\n[STEP 2] Configuring Vision API...")
        vision_json_path = self.config.get('vision_json_path')
        
        if vision_json_path:
            if self.setup_vision_api_properly(vision_json_path):
                self.config['vision_enabled'] = True
            else:
                print("⚠ Vision API setup failed - continuing without OCR")
                self.config['vision_enabled'] = False
        else:
            print("⚠ No Vision API credentials provided - OCR disabled")
            self.config['vision_enabled'] = False

        # Step 3: Validate data source
        print("\n[STEP 3] Validating data source...")
        if self.config.get('instagram_data'):
            print(f"✓ Direct data provided: {len(self.config['instagram_data'])} posts")
        elif self.config.get('instagram_data_url'):
            print(f"✓ Data URL configured: {self.config['instagram_data_url'][:80]}...")
        elif self.config.get('instagram_data_path'):
            path = Path(self.config['instagram_data_path'])
            if path.exists():
                print(f"✓ Data file found: {path}")
            else:
                raise ValueError(f"Data file not found: {path}")
        else:
            raise ValueError("No data source provided. Set instagram_data, instagram_data_url, or instagram_data_path")

        print("\n✓ Setup complete!")
        print(f"  • Parallel workers: {self.max_workers}")
        print(f"  • Rate limit delay: {self.rate_limit_delay}s")
        print(f"  • Vision API: {'ENABLED' if self.config.get('vision_enabled') else 'DISABLED'}")
        print(f"  • Output directory: {self.output_dir}")

        return self

    def handle_interrupt(self, signum, frame):
        """Handle Ctrl+C gracefully with full data save"""
        print("\n\n⚠ INTERRUPT DETECTED - Saving all data...")
        self.emergency_save()
        self.create_final_report()
        sys.exit(0)

    def emergency_save(self):
        """Emergency save with DataFrame"""
        with self.lock:
            if self.results:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

                # Create DataFrame
                df = pd.DataFrame(self.results)

                # Save CSV
                csv_file = self.output_dir / f'emergency_events_{timestamp}.csv'
                df.to_csv(csv_file, index=False)
                print(f"✓ Emergency CSV saved: {csv_file}")

                # Save Excel
                try:
                    excel_file = self.output_dir / f'emergency_events_{timestamp}.xlsx'
                    df.to_excel(excel_file, index=False)
                    print(f"✓ Emergency Excel saved: {excel_file}")
                except Exception as e:
                    print(f"⚠ Could not save Excel: {e}")

                # Save detailed stats
                stats_file = self.output_dir / f'emergency_stats_{timestamp}.json'
                with open(stats_file, 'w') as f:
                    json.dump(self.stats, f, indent=2)
                print(f"✓ Statistics saved: {stats_file}")

                # Save checkpoint
                self.save_checkpoint()

    def setup_vision_api_properly(self, json_path):
        """Setup Vision API with proper credentials"""
        print("\n" + "-"*40)
        print("SETTING UP VISION API")
        print("-"*40)

        try:
            # Validate JSON file
            json_path = Path(json_path)
            if not json_path.exists():
                print(f"✗ File not found: {json_path}")
                return False

            # Load and validate credentials
            with open(json_path, 'r') as f:
                creds_data = json.load(f)

            required_fields = ['type', 'project_id', 'private_key', 'client_email']
            missing_fields = [field for field in required_fields if field not in creds_data]

            if missing_fields:
                print(f"✗ JSON missing required fields: {missing_fields}")
                return False

            print(f"✓ Found service account: {creds_data.get('client_email', 'unknown')}")
            print(f"✓ Project ID: {creds_data.get('project_id', 'unknown')}")

            # Create credentials with proper scope
            credentials = service_account.Credentials.from_service_account_file(
                str(json_path),
                scopes=['https://www.googleapis.com/auth/cloud-vision']
            )

            # Create Vision client with explicit credentials
            self.vision_client = vision.ImageAnnotatorClient(credentials=credentials)

            # Test the Vision API
            print("\nTesting Vision API...")
            try:
                # Test with a simple base64 image (1x1 white pixel)
                test_image_base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg=="
                test_image_bytes = base64.b64decode(test_image_base64)

                image = vision.Image(content=test_image_bytes)
                response = self.vision_client.text_detection(image=image)

                if response.error.message:
                    print(f"  ✗ Vision API error: {response.error.message}")
                    return False

                print("  ✓ Vision API test successful!")
                return True

            except Exception as e:
                print(f"  ✗ Vision API test failed: {str(e)[:100]}")
                return False

        except Exception as e:
            print(f"✗ Vision API setup failed: {e}")
            return False

    def extract_text_from_instagram_image(self, image_url, post_id=""):
        """Extract text from Instagram image by downloading it first"""
        if not self.vision_client or not image_url or image_url == 'null':
            return ""

        ocr_text = ''
        print(f"    ↳ Processing image: {image_url[:80]}...")

        try:
            # STEP 1: Download the image from Instagram CDN
            print(f"    ↳ Downloading image from Instagram CDN...")

            # Add headers to mimic a browser request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }

            response = requests.get(image_url, headers=headers, timeout=15)

            if response.status_code != 200:
                print(f"    ✗ Failed to download image: Status {response.status_code}")
                with self.lock:
                    self.stats['download_errors'] += 1
                    self.stats['ocr_failed'] += 1
                return ""

            print(f"    ✓ Image downloaded successfully ({len(response.content)} bytes)")

            # STEP 2: Send image content directly to Vision API
            print(f"    ↳ Calling Vision API for OCR...")

            # Rate limiting
            time.sleep(self.rate_limit_delay)

            # Create Vision API image object with the downloaded content
            image = vision.Image(content=response.content)

            # Perform OCR
            response_ocr = self.vision_client.text_detection(image=image)

            # Check for API errors
            if response_ocr.error.message:
                error_msg = response_ocr.error.message
                print(f"    ✗ Vision API error: {error_msg}")

                with self.lock:
                    self.stats['ocr_failed'] += 1
                    if error_msg not in self.stats['vision_errors']:
                        self.stats['vision_errors'][error_msg] = 0
                    self.stats['vision_errors'][error_msg] += 1

                # Handle rate limiting
                if 'quota' in error_msg.lower() or '429' in error_msg:
                    print(f"    ⚠ Rate limited - increasing delay")
                    self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)

                return ""

            # Extract text from response
            if response_ocr.text_annotations:
                ocr_text = response_ocr.text_annotations[0].description
                print(f"    ✓ OCR SUCCESS! Extracted {len(ocr_text)} characters")

                # Show sample of extracted text
                sample = ocr_text[:100].replace('\n', ' ')
                print(f"    📝 Sample: {sample}...")

                with self.lock:
                    self.stats['ocr_success'] += 1
                    self.successful_ocr.append(post_id)
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

            # Handle rate limiting
            if '429' in error_str or 'quota' in error_str.lower():
                self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)
                print(f"    ⚠ Increasing delay to {self.rate_limit_delay}s")

        return ocr_text

    def get_field_value(self, post_data, field_type, default=''):
        """Get field value using dynamic field mapping"""
        if hasattr(self, 'config') and 'field_mappings' in self.config:
            field_name = self.config['field_mappings'].get(field_type)
            if field_name:
                return post_data.get(field_name, default)

        # Fallback to common field names
        common_names = {
            'Caption': ['caption', 'text', 'captionText', 'description'],
            'Display URL': ['displayUrl', 'display_url', 'imageUrl', 'image_url', 'photoUrl'],
            'Image URL': ['displayUrl', 'image_url', 'mediaUrl', 'media_url'],
            'Timestamp': ['timestamp', 'takenAt', 'taken_at', 'created_time', 'date'],
            'Location': ['locationName', 'location_name', 'location', 'place', 'venue'],
            'Owner Username': ['ownerUsername', 'owner_username', 'username', 'author', 'account'],
            'Alt Text': ['alt_text', 'altText', 'alt', 'accessibility_caption'],
            'URL/Link': ['url', 'link', 'postUrl', 'post_url', 'permalink'],
            'ShortCode': ['shortCode', 'shortcode', 'code', 'id']
        }

        for name in common_names.get(field_type, []):
            if name in post_data:
                return post_data.get(name, default)

        return default

    def get_permanent_instagram_url(self, post_data):
        """Generate permanent Instagram URL from post data"""
        # Try different fields that might contain the shortcode
        shortcode = (self.get_field_value(post_data, 'ShortCode', '') or
                    post_data.get('shortCode', '') or
                    post_data.get('shortcode', '') or
                    post_data.get('code', '') or
                    post_data.get('id', ''))

        if shortcode and shortcode != '' and 'sample' not in str(shortcode).lower():
            return f"https://www.instagram.com/p/{shortcode}/"

        # Try to extract from URL if available
        url = self.get_field_value(post_data, 'URL/Link', '')
        if url and 'instagram.com' in url:
            return url

        return ''

    def extract_events_with_gemini(self, post_data, ocr_text):
        """Extract MULTIPLE events from a single post with permanent URLs"""
        # Rate limiting
        time.sleep(self.rate_limit_delay)

        # Extract fields using dynamic mapping
        caption = self.get_field_value(post_data, 'Caption', '')
        alt_text = self.get_field_value(post_data, 'Alt Text', '')
        location_name = self.get_field_value(post_data, 'Location', '')
        owner_username = self.get_field_value(post_data, 'Owner Username', '')

        # Additional fields
        owner_full_name = post_data.get('ownerFullName', '') or post_data.get('full_name', '')
        hashtags = ' '.join(post_data.get('hashtags', [])) if 'hashtags' in post_data else ''
        first_comment = post_data.get('firstComment', '') or post_data.get('first_comment', '')

        # Get permanent Instagram URL
        permanent_url = self.get_permanent_instagram_url(post_data)

        # Parse timestamp
        timestamp = self.get_field_value(post_data, 'Timestamp', '')
        try:
            if timestamp:
                if isinstance(timestamp, (int, float)):
                    post_date = datetime.fromtimestamp(timestamp)
                else:
                    post_date = datetime.fromisoformat(str(timestamp).replace('Z', '+00:00'))
            else:
                post_date = datetime.now()
        except:
            post_date = datetime.now()

        prompt = f"""
        Extract ALL event information from this Instagram post.
        IMPORTANT: This post may contain MULTIPLE events (calendars, weekly lineups, etc).
        Extract EACH event as a separate object.

        POST DATE: {post_date.strftime('%Y-%m-%d')}
        ACCOUNT: @{owner_username} ({owner_full_name})
        LOCATION TAG: {location_name}

        CAPTION: {caption[:2000]}
        ALT TEXT: {alt_text[:500]}
        HASHTAGS: {hashtags[:300]}
        FIRST COMMENT: {first_comment[:500]}

        OCR TEXT FROM IMAGE (MAY CONTAIN EVENT CALENDAR/LIST):
        {ocr_text[:3000]}

        EXTRACTION INSTRUCTIONS:
        1. Look for MULTIPLE events - calendars, weekly lineups, event series
        2. Common patterns: "Monday: Jazz Night, Tuesday: Open Mic, Wednesday: Trivia"
        3. Monthly calendars: "Dec 15 - Band Name, Dec 22 - Holiday Party"
        4. Each date/event combination should be a separate event
        5. If location_name exists, use it as venue for ALL events
        6. OCR text often contains event calendars with multiple dates
        7. For section_of_nj: North = Bergen/Essex/Hudson/Morris/Passaic/Sussex/Warren counties;
           Central = Hunterdon/Mercer/Middlesex/Monmouth/Somerset/Union counties;
           South = Atlantic/Burlington/Camden/Cape May/Cumberland/Gloucester/Ocean/Salem counties

        Return a JSON object with an array of events:
        {{
            "events": [
                {{
                    "event_name": "event title or null",
                    "date": "YYYY-MM-DD or null",
                    "day_of_week": "Monday/Tuesday/etc or null",
                    "time": "HH:MM (24hr) or null",
                    "end_time": "HH:MM (24hr) or null",
                    "venue_name": "venue (use location_name if available) or null",
                    "city": "city in New Jersey or null",
                    "state": "NJ or null",
                    "section_of_nj": "North/Central/South or null",
                    "address": "street address or null",
                    "price": "price or null",
                    "description": "brief description or null",
                    "performer": "artist/band/performer name or null",
                    "event_type": "concert/trivia/open-mic/party/market/festival/etc or null",
                    "age_restriction": "21+/18+/all ages or null",
                    "ticket_link": "URL or null",
                    "confidence": "high/medium/low",
                    "extracted_from": ["caption", "ocr", "alt_text", etc]
                }}
            ],
            "total_events_found": number,
            "is_calendar_post": true/false,
            "calendar_period": "weekly/monthly/single or null"
        }}

        Examples of multiple events to extract:
        - "Every Tuesday: Open Mic, Every Thursday: Jazz Night" = 2 recurring events
        - "Dec 15: Band A, Dec 22: Band B, Dec 29: Band C" = 3 separate events
        - Calendar images with dates and event names = each date is an event

        If no events found, return: {{"events": [], "total_events_found": 0}}

        For relative dates like "this Friday", calculate actual dates from post date.
        For recurring events, create an event for the NEXT occurrence.
        """

        try:
            response = self.gemini_model.generate_content(prompt)
            text = response.text.strip()
            text = re.sub(r'^```json\s*', '', text)
            text = re.sub(r'\s*```$', '', text)

            # Parse response
            result = json.loads(text)

            # Extract events array
            events = result.get('events', [])

            # Add metadata to each event
            processed_events = []
            for event in events:
                if event and (event.get('event_name') or event.get('date')):
                    # Add post metadata with PERMANENT URL
                    event['post_id'] = post_data.get('id', '') or post_data.get('shortCode', '')
                    event['instagram_post_url'] = permanent_url  # Permanent URL that won't expire
                    event['post_url'] = self.get_field_value(post_data, 'URL/Link', '')

                    # Add display URL with note about expiration
                    display_url = self.get_field_value(post_data, 'Display URL', '')
                    event['display_url'] = display_url
                    event['display_url_note'] = 'CDN URL - expires after few hours/days'

                    # Instagram profile info
                    event['instagram_handle'] = owner_username
                    event['instagram_profile_url'] = f"https://www.instagram.com/{owner_username}/" if owner_username else ''
                    event['account_name'] = owner_full_name

                    # OCR and extraction metadata
                    event['had_ocr'] = bool(ocr_text)
                    event['ocr_chars'] = len(ocr_text)
                    event['post_date'] = post_date.isoformat()
                    event['extraction_timestamp'] = datetime.now().isoformat()

                    # Calendar metadata
                    event['from_calendar'] = result.get('is_calendar_post', False)
                    event['calendar_type'] = result.get('calendar_period', 'single')

                    # Add combined text for reference
                    event['combined_text'] = f"{caption[:500]} {ocr_text[:500]}"

                    processed_events.append(event)

            # Log results
            if processed_events:
                print(f"    ✓ Found {len(processed_events)} event(s)")
                if len(processed_events) > 1:
                    print(f"    📅 MULTIPLE EVENTS EXTRACTED:")
                    for idx, evt in enumerate(processed_events, 1):
                        print(f"      {idx}. {evt.get('event_name', 'Unnamed')} - {evt.get('date', 'No date')}")

            return processed_events

        except Exception as e:
            with self.lock:
                self.stats['gemini_errors'] += 1
            error_msg = str(e)[:200]
            print(f"    ✗ Gemini error: {error_msg}")

            if '429' in error_msg:
                print(f"    ⚠ Rate limited - increasing delay")
                self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)

            return []

    def process_post(self, post, post_num, total):
        """Process single post that may contain MULTIPLE events"""
        post_id = post.get('id', '') or post.get('shortCode', '') or post.get('shortcode', '') or f'post_{post_num}'

        print(f"\n[{post_num}/{total}] Processing post: {post_id}")

        # Get username
        username = self.get_field_value(post, 'Owner Username', 'unknown')
        print(f"  ↳ Account: @{username}")

        # Check if already processed (thread-safe)
        with self.lock:
            if post_id in self.processed_posts:
                print("  ↳ Already processed")
                return None

        try:
            # Extract text from images
            ocr_text = ""

            # Get image URLs with dynamic mapping
            display_url = self.get_field_value(post, 'Display URL', '')
            image_url = self.get_field_value(post, 'Image URL', '')

            # Check for nested image structures
            if not display_url and not image_url:
                if 'images' in post and isinstance(post['images'], list) and post['images']:
                    first_image = post['images'][0]
                    if isinstance(first_image, dict):
                        display_url = first_image.get('url', '') or first_image.get('src', '')
                    elif isinstance(first_image, str):
                        display_url = first_image
                    print(f"  ↳ Found image URL in nested structure")

            # Process OCR with the download approach
            if self.config.get('vision_enabled'):
                if display_url:
                    print(f"  ↳ Found image URL")
                    ocr_text = self.extract_text_from_instagram_image(display_url, post_id)
                elif image_url:
                    print(f"  ↳ Found backup image URL")
                    ocr_text = self.extract_text_from_instagram_image(image_url, post_id)
                else:
                    print(f"  ⚠ No image URL found - relying on text fields")
            else:
                print(f"  ⚠ Vision API disabled - relying on text fields only")

            # Get caption
            caption = self.get_field_value(post, 'Caption', '')

            # Check for calendar keywords
            all_text = (caption + ' ' + ocr_text).lower()
            calendar_keywords = ['calendar', 'schedule', 'lineup', 'weekly', 'monthly',
                               'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
                               'saturday', 'sunday', 'every', 'recurring']

            might_be_calendar = any(keyword in all_text for keyword in calendar_keywords)

            if might_be_calendar:
                print(f"  📅 Possible calendar/multi-event post detected")

            # Show available data
            has_caption = bool(caption)
            has_location = bool(self.get_field_value(post, 'Location', ''))
            has_ocr = bool(ocr_text)

            print(f"  ↳ Data available: caption={has_caption}, location={has_location}, OCR={has_ocr}")

            # Skip if no useful data
            if not has_caption and not has_ocr:
                print(f"  ⚠ No caption or OCR text - skipping")
                with self.lock:
                    self.processed_posts.add(post_id)
                    self.stats['processed'] += 1
                return None

            # Extract events with Gemini
            print(f"  ↳ Analyzing with Gemini AI (checking for multiple events)...")
            events = self.extract_events_with_gemini(post, ocr_text)

            # Update stats (thread-safe)
            with self.lock:
                self.processed_posts.add(post_id)
                self.stats['processed'] += 1

            if events and len(events) > 0:
                # Update statistics (thread-safe)
                with self.lock:
                    self.stats['posts_with_events'] += 1

                    if len(events) > 1:
                        self.stats['multi_event_posts'] += 1
                        self.stats['max_events_in_post'] = max(self.stats['max_events_in_post'], len(events))

                    if any(event.get('from_calendar', False) for event in events):
                        self.stats['calendar_posts'] += 1

                    # Add all events to results
                    for event in events:
                        self.results.append(event)
                        self.stats['events_found'] += 1

                # Log extraction results
                if len(events) > 1:
                    print(f"  🎉 MULTIPLE EVENTS FOUND: {len(events)} events extracted!")
                    for idx, event in enumerate(events, 1):
                        print(f"    {idx}. {event.get('event_name', 'Unnamed')}")
                        if event.get('date'):
                            print(f"       Date: {event['date']}")
                        if event.get('venue_name'):
                            print(f"       Venue: {event['venue_name']}")
                        print(f"       Confidence: {event.get('confidence', 'unknown')}")
                else:
                    event = events[0]
                    print(f"  ✓ EVENT FOUND: {event.get('event_name', 'Unnamed')}")
                    print(f"    • Confidence: {event.get('confidence', 'unknown')}")
                    print(f"    • Sources: {', '.join(event.get('extracted_from', []))}")

                # Save checkpoint periodically (thread-safe)
                with self.lock:
                    if self.stats['events_found'] % 10 == 0:
                        self.save_checkpoint()
                        print(f"  ↳ Checkpoint saved ({self.stats['events_found']} total events)")

                return events
            else:
                print(f"  ↳ No events found in this post")

        except Exception as e:
            print(f"  ✗ Error: {str(e)[:200]}")
            with self.lock:
                self.stats['processed'] += 1

        return None

    def analyze_data_structure(self, first_post):
        """Analyze and display data structure of Instagram posts"""
        print("\n" + "="*60)
        print("DATA STRUCTURE ANALYSIS")
        print("="*60)

        # Check for all possible field variations
        field_mappings = {
            'Caption': ['caption', 'text', 'captionText', 'description'],
            'Display URL': ['displayUrl', 'display_url', 'imageUrl', 'image_url', 'photoUrl'],
            'Image URL': ['imageUrl', 'image_url', 'url', 'mediaUrl'],
            'Timestamp': ['timestamp', 'takenAt', 'taken_at', 'created_time'],
            'Location': ['locationName', 'location_name', 'location', 'place'],
            'Owner Username': ['ownerUsername', 'owner_username', 'username', 'author'],
            'Alt Text': ['alt_text', 'altText', 'alt', 'accessibility_caption'],
            'URL/Link': ['url', 'link', 'postUrl', 'permalink'],
            'ShortCode': ['shortCode', 'shortcode', 'code', 'id']
        }

        print("\nSearching for required fields:")
        print("-" * 40)

        found_fields = {}
        for field_type, possible_names in field_mappings.items():
            for name in possible_names:
                if name in first_post:
                    value = first_post[name]
                    has_content = bool(value) and (value != "" if isinstance(value, str) else True)
                    found_fields[field_type] = (name, has_content)

                    status = "✓" if has_content else "⚠ (empty)"
                    print(f"  • {field_type}: {status} Found as '{name}'")

                    if has_content:
                        sample = str(value)[:50]
                        if len(str(value)) > 50:
                            sample += "..."
                        print(f"    Sample: {sample}")
                    break

            if field_type not in found_fields:
                print(f"  • {field_type}: ✗ Not found")

        # Check for permanent URL capability
        print("\n" + "-" * 40)
        print("Checking for permanent URL capability:")
        if 'ShortCode' in found_fields and found_fields['ShortCode'][1]:
            print(f"  ✓ Can generate permanent Instagram URLs")
        else:
            print(f"  ⚠ No shortcode found - URLs may be temporary")

        # Store field mappings
        if found_fields:
            self.config['field_mappings'] = {}
            for field_type, (field_name, has_content) in found_fields.items():
                if field_name and has_content:
                    self.config['field_mappings'][field_type] = field_name

        print("\n✓ Data structure analysis complete")

    def save_checkpoint(self):
        """Save checkpoint with all data (thread-safe)"""
        checkpoint = {
            'processed_posts': self.processed_posts,
            'results': self.results,
            'stats': self.stats,
            'failed_ocr': self.failed_ocr,
            'successful_ocr': self.successful_ocr
        }
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(checkpoint, f)

    def load_checkpoint(self):
        """Load checkpoint if exists"""
        if self.checkpoint_file.exists():
            try:
                with open(self.checkpoint_file, 'rb') as f:
                    checkpoint = pickle.load(f)
                self.processed_posts = checkpoint.get('processed_posts', set())
                self.results = checkpoint.get('results', [])
                self.stats = checkpoint.get('stats', self.stats)
                print(f"✓ Loaded checkpoint: {len(self.processed_posts)} posts processed, {len(self.results)} events found")
                return True
            except Exception as e:
                print(f"⚠ Could not load checkpoint: {e}")
        return False

    async def fetch_instagram_data(self):
        """Fetch Instagram data from configured source"""
        print("\n" + "="*60)
        print("FETCHING INSTAGRAM DATA")
        print("="*60)

        # Direct data provided
        if 'instagram_data' in self.config and self.config['instagram_data']:
            data = self.config['instagram_data']
            print(f"✓ Using provided data: {len(data)} posts")
            return data

        # Fetch from URL
        if 'instagram_data_url' in self.config:
            url = self.config['instagram_data_url']
            print(f"Fetching from: {url[:100]}...")

            response = requests.get(url, timeout=60)
            data = response.json()
            print(f"✓ Fetched {len(data)} posts")
            return data

        # Load from file
        if 'instagram_data_path' in self.config:
            path = Path(self.config['instagram_data_path'])
            with open(path, 'r') as f:
                data = json.load(f)
            print(f"✓ Loaded {len(data)} posts from {path}")
            return data

        raise ValueError("No data source configured")

    def create_final_report(self):
        """Create comprehensive final report"""
        print("\n" + "="*60)
        print("FINAL EXTRACTION REPORT")
        print("="*60)

        print(f"\n📊 OVERALL STATISTICS:")
        print(f"  • Total posts: {self.stats['total_posts']}")
        print(f"  • Posts processed: {self.stats['processed']}")
        print(f"  • Posts with events: {self.stats['posts_with_events']}")
        print(f"  • Total events extracted: {self.stats['events_found']}")
        print(f"  • Parallel workers used: {self.max_workers}")

        if self.stats['posts_with_events'] > 0:
            avg_events = self.stats['events_found'] / self.stats['posts_with_events']
            print(f"  • Average events per post (with events): {avg_events:.2f}")

        if self.stats['processed'] > 0:
            success_rate = (self.stats['posts_with_events'] / self.stats['processed']) * 100
            print(f"  • Success rate: {success_rate:.1f}%")

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

    async def run(self):
        """Main execution pipeline with parallel processing"""
        # Fetch data
        posts = await self.fetch_instagram_data()

        if not posts:
            print("✗ No posts to process")
            return None

        self.stats['total_posts'] = len(posts)

        # Analyze data structure
        if posts and len(posts) > 0:
            self.analyze_data_structure(posts[0])

        # Apply max_posts limit if set
        max_posts = self.config.get('max_posts')
        if max_posts:
            posts = posts[:max_posts]
            print(f"\n📋 Processing limited to {max_posts} posts")

        # Check for checkpoint
        if self.config.get('resume_checkpoint', True):
            self.load_checkpoint()

        # Process posts
        print("\n" + "="*60)
        print(f"PROCESSING {len(posts)} POSTS")
        print(f"Vision API: {'ENABLED (with image download)' if self.config.get('vision_enabled') else 'DISABLED'}")
        print(f"Parallel workers: {self.max_workers}")
        print(f"Rate limit delay: {self.rate_limit_delay}s per worker")
        print("="*60)

        # Process with parallel workers
        if self.max_workers > 1:
            print(f"\n⚡ Processing with {self.max_workers} parallel workers...")

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                futures = {
                    executor.submit(self.process_post, post, i+1, len(posts)): i
                    for i, post in enumerate(posts)
                }

                # Process as they complete
                for future in as_completed(futures):
                    try:
                        result = future.result(timeout=60)
                    except Exception as e:
                        print(f"⚠ Error in parallel processing: {e}")
        else:
            # Sequential processing
            print("\n📋 Processing sequentially...")
            for i, post in enumerate(posts):
                self.process_post(post, i+1, len(posts))

        # Final save
        self.save_checkpoint()

        # Create final report
        self.create_final_report()

        # Create DataFrame and save
        if self.results:
            df = pd.DataFrame(self.results)

            # Sort by date
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                df = df.sort_values('date', na_position='last')

            # Reorder columns for better display
            column_order = [
                'event_name', 'date', 'day_of_week', 'time', 'venue_name',
                'city', 'state', 'section_of_nj', 'price', 'event_type',
                'instagram_handle', 'instagram_post_url', 'confidence',
                'from_calendar', 'display_url'
            ]

            existing_columns = [col for col in column_order if col in df.columns]
            other_columns = [col for col in df.columns if col not in column_order]
            df = df[existing_columns + other_columns]

            # Save files
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

            # CSV
            csv_file = self.output_dir / f'events_{timestamp}.csv'
            df.to_csv(csv_file, index=False)
            print(f"\n✓ CSV saved: {csv_file}")

            # Excel
            try:
                excel_file = self.output_dir / f'events_{timestamp}.xlsx'
                df.to_excel(excel_file, index=False)
                print(f"✓ Excel saved: {excel_file}")
            except Exception as e:
                print(f"⚠ Could not save Excel file: {e}")

            # Save stats
            stats_file = self.output_dir / f'stats_{timestamp}.json'
            with open(stats_file, 'w') as f:
                json.dump(self.stats, f, indent=2)
            print(f"✓ Stats saved: {stats_file}")

            # Display DataFrame info
            print("\n" + "="*60)
            print("EXTRACTED EVENTS SUMMARY")
            print("="*60)
            print(f"\n📊 Results Overview:")
            print(f"  • Total events: {len(df)}")
            if 'venue_name' in df.columns:
                print(f"  • Unique venues: {df['venue_name'].nunique()}")
            if 'date' in df.columns:
                print(f"  • Date range: {df['date'].min()} to {df['date'].max()}")
            if 'instagram_handle' in df.columns:
                print(f"  • Instagram accounts: {df['instagram_handle'].nunique()}")

            # Show sample
            print("\n📋 Sample of extracted events (first 5):")
            print("-" * 60)
            display_cols = ['event_name', 'date', 'venue_name', 'instagram_post_url']
            display_cols = [c for c in display_cols if c in df.columns]
            print(df[display_cols].head(5).to_string())

            print("\n✅ PERMANENT INSTAGRAM URLS:")
            print("The 'instagram_post_url' column contains permanent links that won't expire")
            print("Example format: https://www.instagram.com/p/{shortcode}/")

            return df
        else:
            print("\n✗ No events extracted")
            return None


def run_pipeline(config):
    """
    Convenience function to run the pipeline with a config dict.
    
    Args:
        config (dict): Configuration dictionary. Required keys:
            - gemini_api_key: Your Gemini API key
            
        Optional keys:
            - vision_json_path: Path to Google Cloud Vision service account JSON
            - instagram_data_url: URL to fetch Instagram JSON data
            - instagram_data_path: Local path to Instagram JSON file  
            - instagram_data: Direct list of Instagram post data
            - max_workers: Number of parallel workers (1-5, default 3)
            - rate_limit_delay: Delay between API calls (default 0.5)
            - max_posts: Maximum posts to process (default all)
            - output_dir: Output directory (default /mnt/user-data/outputs)
            - resume_checkpoint: Whether to resume from checkpoint (default True)
    
    Returns:
        pandas.DataFrame: Extracted events, or None if no events found
    """
    pipeline = InstagramEventPipeline(config)
    pipeline.setup()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        result = loop.run_until_complete(pipeline.run())
        return result
    finally:
        loop.close()


# Example usage
if __name__ == "__main__":
    print("="*60)
    print(" INSTAGRAM EVENT EXTRACTION PIPELINE")
    print(" Claude Environment Version")
    print("="*60)
    print()
    print("Usage:")
    print("  from instagram_event_pipeline import run_pipeline")
    print()
    print("  config = {")
    print("      'gemini_api_key': 'YOUR_GEMINI_API_KEY',")
    print("      'vision_json_path': '/path/to/service-account.json',  # Optional")
    print("      'instagram_data_url': 'https://api.apify.com/...',    # Or provide data directly")
    print("      'max_posts': 50,                                       # Optional limit")
    print("  }")
    print()
    print("  df = run_pipeline(config)")
    print()
    print("Or provide instagram_data directly as a list of post dicts.")
