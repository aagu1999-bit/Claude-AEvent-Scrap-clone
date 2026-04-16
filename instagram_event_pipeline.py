    #!/usr/bin/env python3
    """
    Instagram Event Extraction Pipeline - Claude Environment Version
    WITH BUILT-IN AUDIT TRAIL

    Every post processed gets logged with a reason code so you can see exactly
    which posts yielded zero events and why. After each run you get:
        - events_<timestamp>.csv / .xlsx  (the events, same as before)
        - audit_log_<timestamp>.csv       (EVERY post, with outcome & reason)
        - zero_event_posts_<timestamp>.csv (just the posts that came up empty)
        - audit_summary_<timestamp>.txt   (human-readable breakdown)
        - stats_<timestamp>.json          (pipeline stats)

    Reason codes:
        EVENTS_FOUND          - success
        GEMINI_RETURNED_NONE  - Gemini ran but found no events (review these manually)
        GEMINI_JSON_ERROR     - Gemini returned malformed JSON
        GEMINI_API_ERROR      - API call failed (rate limit, network, etc.)
        OCR_FAILED_NO_CAPTION - Image-only post where OCR failed
        NO_TEXT_AVAILABLE     - Nothing to extract from (video-only, etc.)
        ALREADY_PROCESSED     - Skipped due to checkpoint
        WORKER_EXCEPTION      - Parallel worker crashed
    """

    import os
    import sys
    import json
    import csv
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

    import google.generativeai as genai
    from google.cloud import vision
    from google.oauth2 import service_account

    try:
        import nest_asyncio
        nest_asyncio.apply()
    except:
        pass


    class InstagramEventPipeline:
        """Optimized pipeline with parallel processing, permanent URLs, and full audit trail"""

        def __init__(self, config=None):
            self.config = config or {}
            self.processed_posts = set()
            self.results = []
            self.failed_ocr = []
            self.successful_ocr = []
            self.vision_client = None
            self.gemini_model = None

            # Audit trail storage
            self.audit_records = []
            self.audit_lock = threading.Lock()

            self.max_workers = min(5, max(1, self.config.get('max_workers', 3)))
            self.rate_limit_delay = max(0.1, self.config.get('rate_limit_delay', 0.5))
            self.output_dir = Path(self.config.get('output_dir', './outputs'))
            self.output_dir.mkdir(parents=True, exist_ok=True)

            self.checkpoint_file = self.output_dir / 'pipeline_checkpoint.pkl'
            self.lock = threading.Lock()

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
                'parallel_workers': self.max_workers,
                # Carousel tracking
                'carousel_posts': 0,          # posts that had >1 slide
                'total_slides_processed': 0,  # total slides OCR'd across all posts
                'max_slides_in_post': 0,      # biggest carousel seen
                'slides_with_text': 0,        # slides that yielded OCR text
            }

            atexit.register(self.emergency_save)
            signal.signal(signal.SIGINT, self.handle_interrupt)

        # ------------------------------------------------------------------
        # AUDIT TRAIL
        # ------------------------------------------------------------------

        def audit_log(self, post_id, username, shortcode, reason, detail='',
                      caption_len=0, ocr_len=0, events_found=0, had_image=False,
                      slides_count=0):
            """Record one post's outcome for the audit trail."""
            with self.audit_lock:
                self.audit_records.append({
                    'timestamp': datetime.now().isoformat(timespec='seconds'),
                    'post_id': post_id,
                    'username': username,
                    'shortcode': shortcode,
                    'instagram_url': f"https://www.instagram.com/p/{shortcode}/" if shortcode else '',
                    'reason': reason,
                    'detail': str(detail)[:300],
                    'caption_length': caption_len,
                    'ocr_length': ocr_len,
                    'events_found': events_found,
                    'had_image': had_image,
                    'slides_count': slides_count,
                })

        def dump_audit_files(self):
            """Write audit CSV, zero-event sidecar, and human-readable summary."""
            if not self.audit_records:
                print("\n⚠ No audit records to write")
                return

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            fieldnames = ['timestamp', 'post_id', 'username', 'shortcode', 'instagram_url',
                          'reason', 'detail', 'caption_length', 'ocr_length',
                          'events_found', 'had_image', 'slides_count']

            audit_csv = self.output_dir / f'audit_log_{timestamp}.csv'
            with open(audit_csv, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.audit_records)
            print(f"✓ Audit log saved: {audit_csv}")

            zero_events = [r for r in self.audit_records
                           if r['events_found'] == 0 and r['reason'] != 'ALREADY_PROCESSED']
            if zero_events:
                zero_csv = self.output_dir / f'zero_event_posts_{timestamp}.csv'
                with open(zero_csv, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(zero_events)
                print(f"✓ Zero-event posts sidecar: {zero_csv}")

            summary_path = self.output_dir / f'audit_summary_{timestamp}.txt'
            reason_counts = {}
            for r in self.audit_records:
                reason_counts[r['reason']] = reason_counts.get(r['reason'], 0) + 1

            username_zero_counts = {}
            for r in zero_events:
                username_zero_counts[r['username']] = username_zero_counts.get(r['username'], 0) + 1

            with open(summary_path, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write("EXTRACTION AUDIT SUMMARY\n")
                f.write(f"Generated: {datetime.now().isoformat(timespec='seconds')}\n")
                f.write("=" * 60 + "\n\n")
                f.write(f"Total posts audited: {len(self.audit_records)}\n\n")

                f.write("OUTCOMES BY REASON:\n")
                f.write("-" * 40 + "\n")
                for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
                    pct = (count / len(self.audit_records)) * 100
                    f.write(f"  {reason:<25} {count:>5}  ({pct:>5.1f}%)\n")

                f.write("\n\nTOP ACCOUNTS WITH ZERO-EVENT POSTS:\n")
                f.write("(review these manually on Instagram to confirm nothing was missed)\n")
                f.write("-" * 40 + "\n")
                for username, count in sorted(username_zero_counts.items(), key=lambda x: -x[1])[:25]:
                    f.write(f"  @{username:<30} {count:>4} posts\n")

                f.write("\n\nREASON CODE GUIDE:\n")
                f.write("-" * 40 + "\n")
                f.write("EVENTS_FOUND          - success\n")
                f.write("GEMINI_RETURNED_NONE  - Gemini saw the text but found no event.\n")
                f.write("                        THIS IS YOUR MAIN REVIEW QUEUE - spot-check\n")
                f.write("                        a handful on Instagram to see if the model\n")
                f.write("                        is being too conservative.\n")
                f.write("GEMINI_JSON_ERROR     - Gemini returned malformed JSON. Fixable\n")
                f.write("                        with a retry or stricter prompt.\n")
                f.write("GEMINI_API_ERROR      - API call failed (rate limit, network).\n")
                f.write("                        Fixable with retry logic.\n")
                f.write("OCR_FAILED_NO_CAPTION - Image-only post where OCR failed. Check\n")
                f.write("                        the zero_event_posts CSV, visit the URL,\n")
                f.write("                        and you'll see exactly what was missed.\n")
                f.write("NO_TEXT_AVAILABLE     - No caption, no image, nothing to work with.\n")
                f.write("                        Usually legit (video-only posts, etc.)\n")
                f.write("ALREADY_PROCESSED     - Checkpoint skip. If you suspect previous\n")
                f.write("                        runs missed events, delete the .pkl file\n")
                f.write("                        and re-run.\n")

            print(f"✓ Audit summary: {summary_path}")

            print("\n" + "=" * 60)
            print("AUDIT BREAKDOWN")
            print("=" * 60)
            for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1]):
                pct = (count / len(self.audit_records)) * 100
                print(f"  {reason:<25} {count:>5}  ({pct:>5.1f}%)")

        # ------------------------------------------------------------------
        # SETUP
        # ------------------------------------------------------------------

        def setup(self):
            """Setup APIs based on configuration"""
            print("\n" + "="*60)
            print(" INSTAGRAM EVENT EXTRACTION PIPELINE")
            print(" Claude Environment Version (with audit trail)")
            print("="*60)

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
            print("\n\n⚠ INTERRUPT DETECTED - Saving all data...")
            self.emergency_save()
            self.dump_audit_files()
            self.create_final_report()
            sys.exit(0)

        def emergency_save(self):
            with self.lock:
                if self.results:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    df = pd.DataFrame(self.results)

                    csv_file = self.output_dir / f'emergency_events_{timestamp}.csv'
                    df.to_csv(csv_file, index=False)
                    print(f"✓ Emergency CSV saved: {csv_file}")

                    try:
                        excel_file = self.output_dir / f'emergency_events_{timestamp}.xlsx'
                        df.to_excel(excel_file, index=False)
                        print(f"✓ Emergency Excel saved: {excel_file}")
                    except Exception as e:
                        print(f"⚠ Could not save Excel: {e}")

                    stats_file = self.output_dir / f'emergency_stats_{timestamp}.json'
                    with open(stats_file, 'w') as f:
                        json.dump(self.stats, f, indent=2)
                    print(f"✓ Statistics saved: {stats_file}")

                    self.save_checkpoint()

        def setup_vision_api_properly(self, json_path):
            print("\n" + "-"*40)
            print("SETTING UP VISION API")
            print("-"*40)

            try:
                json_path = Path(json_path)
                if not json_path.exists():
                    print(f"✗ File not found: {json_path}")
                    return False

                with open(json_path, 'r') as f:
                    creds_data = json.load(f)

                required_fields = ['type', 'project_id', 'private_key', 'client_email']
                missing_fields = [field for field in required_fields if field not in creds_data]

                if missing_fields:
                    print(f"✗ JSON missing required fields: {missing_fields}")
                    return False

                print(f"✓ Found service account: {creds_data.get('client_email', 'unknown')}")
                print(f"✓ Project ID: {creds_data.get('project_id', 'unknown')}")

                credentials = service_account.Credentials.from_service_account_file(
                    str(json_path),
                    scopes=['https://www.googleapis.com/auth/cloud-vision']
                )

                self.vision_client = vision.ImageAnnotatorClient(credentials=credentials)

                print("\nTesting Vision API...")
                try:
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

        # ------------------------------------------------------------------
        # OCR
        # ------------------------------------------------------------------

        # ------------------------------------------------------------------
        # CAROUSEL SUPPORT
        # ------------------------------------------------------------------

        def collect_all_image_urls(self, post_data):
            """
            Collect image URLs from all slides in a carousel post.

            Apify stores carousel slides in different fields depending on actor version:
                - childPosts: array of child post objects, each with displayUrl/images
                - images: array of URL strings or {url} dicts
                - sidecarChildren: older naming

            The cover image (displayUrl) is often duplicated as the first slide, so we
            dedupe by URL.

            Returns:
                list of unique image URLs, cover first, then carousel slides in order
            """
            urls = []
            seen = set()

            def add(url):
                if not url or not isinstance(url, str):
                    return
                # Normalize for dedup: Instagram CDN URLs have identical paths with
                # different query strings (signing tokens), so compare path only.
                try:
                    from urllib.parse import urlparse
                    key = urlparse(url).path or url
                except Exception:
                    key = url
                if key in seen:
                    return
                seen.add(key)
                urls.append(url)

            # 1. Cover image (always first)
            cover = self.get_field_value(post_data, 'Display URL', '')
            add(cover)

            # 2. childPosts — the most common Apify carousel field
            child_posts = post_data.get('childPosts') or post_data.get('sidecarChildren') or []
            if isinstance(child_posts, list):
                for child in child_posts:
                    if isinstance(child, dict):
                        # Try multiple field names on each child
                        child_url = (child.get('displayUrl') or
                                     child.get('display_url') or
                                     child.get('imageUrl') or
                                     child.get('image_url') or
                                     child.get('url') or '')
                        add(child_url)

                        # Some actors nest images inside childPosts too
                        child_images = child.get('images', [])
                        if isinstance(child_images, list):
                            for img in child_images:
                                if isinstance(img, str):
                                    add(img)
                                elif isinstance(img, dict):
                                    add(img.get('url') or img.get('src') or '')
                    elif isinstance(child, str):
                        add(child)

            # 3. images array at top level (some actor versions use this for carousels)
            images = post_data.get('images', [])
            if isinstance(images, list):
                for img in images:
                    if isinstance(img, str):
                        add(img)
                    elif isinstance(img, dict):
                        add(img.get('url') or img.get('src') or img.get('displayUrl') or '')

            # 4. Backup: Image URL field
            backup = self.get_field_value(post_data, 'Image URL', '')
            add(backup)

            return urls

        def extract_text_from_carousel(self, image_urls, post_id=""):
            """
            OCR every slide in a carousel and return combined text with slide markers.

            Returns:
                tuple: (combined_ocr_text, slides_attempted, slides_with_text)
            """
            if not image_urls:
                return "", 0, 0

            if len(image_urls) == 1:
                # Single image — use existing path, no slide markers needed
                text = self.extract_text_from_instagram_image(image_urls[0], post_id)
                with self.lock:
                    self.stats['total_slides_processed'] += 1
                    if text:
                        self.stats['slides_with_text'] += 1
                return text, 1, (1 if text else 0)

            # Multi-slide carousel
            print(f"  📸 CAROUSEL DETECTED: {len(image_urls)} slides to OCR")
            with self.lock:
                self.stats['carousel_posts'] += 1
                self.stats['max_slides_in_post'] = max(self.stats['max_slides_in_post'], len(image_urls))

            slide_texts = []
            slides_with_text = 0

            for idx, url in enumerate(image_urls, 1):
                print(f"    ─── Slide {idx}/{len(image_urls)} ───")
                slide_id = f"{post_id}_slide{idx}"
                text = self.extract_text_from_instagram_image(url, slide_id)

                with self.lock:
                    self.stats['total_slides_processed'] += 1
                    if text:
                        self.stats['slides_with_text'] += 1
                        slides_with_text += 1

                if text:
                    # Add slide marker so Gemini knows which slide each block came from.
                    # Critical for weekly-calendar carousels where each slide = different day.
                    slide_texts.append(f"[SLIDE {idx} of {len(image_urls)}]\n{text}")

            if slide_texts:
                combined = "\n\n".join(slide_texts)
                print(f"  ✓ Carousel OCR complete: {slides_with_text}/{len(image_urls)} slides had text ({len(combined)} chars total)")
                return combined, len(image_urls), slides_with_text
            else:
                print(f"  ⚠ Carousel OCR: no text found on any of {len(image_urls)} slides")
                return "", len(image_urls), 0

        def extract_text_from_instagram_image(self, image_url, post_id=""):
            """Extract text from Instagram image by downloading it first"""
            if not self.vision_client or not image_url or image_url == 'null':
                return ""

            ocr_text = ''
            print(f"    ↳ Processing image: {image_url[:80]}...")

            try:
                print(f"    ↳ Downloading image from Instagram CDN...")

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
                        print(f"    ⚠ Rate limited - increasing delay")
                        self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)

                    return ""

                if response_ocr.text_annotations:
                    ocr_text = response_ocr.text_annotations[0].description
                    print(f"    ✓ OCR SUCCESS! Extracted {len(ocr_text)} characters")

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

                if '429' in error_str or 'quota' in error_str.lower():
                    self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)
                    print(f"    ⚠ Increasing delay to {self.rate_limit_delay}s")

            return ocr_text

        # ------------------------------------------------------------------
        # FIELD MAPPING
        # ------------------------------------------------------------------

        def get_field_value(self, post_data, field_type, default=''):
            if hasattr(self, 'config') and 'field_mappings' in self.config:
                field_name = self.config['field_mappings'].get(field_type)
                if field_name:
                    return post_data.get(field_name, default)

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
            shortcode = (self.get_field_value(post_data, 'ShortCode', '') or
                        post_data.get('shortCode', '') or
                        post_data.get('shortcode', '') or
                        post_data.get('code', '') or
                        post_data.get('id', ''))

            if shortcode and shortcode != '' and 'sample' not in str(shortcode).lower():
                return f"https://www.instagram.com/p/{shortcode}/"

            url = self.get_field_value(post_data, 'URL/Link', '')
            if url and 'instagram.com' in url:
                return url

            return ''

        # ------------------------------------------------------------------
        # GEMINI EXTRACTION (with per-post error tracking for audit)
        # ------------------------------------------------------------------

        def extract_events_with_gemini(self, post_data, ocr_text):
            """Extract MULTIPLE events from a single post. Tracks errors for audit trail."""
            # Reset per-post error tracker (used by process_post for audit)
            self._last_gemini_error = None
            self._last_gemini_error_type = None

            time.sleep(self.rate_limit_delay)

            caption = self.get_field_value(post_data, 'Caption', '')
            alt_text = self.get_field_value(post_data, 'Alt Text', '')
            location_name = self.get_field_value(post_data, 'Location', '')
            owner_username = self.get_field_value(post_data, 'Owner Username', '')

            owner_full_name = post_data.get('ownerFullName', '') or post_data.get('full_name', '')
            hashtags = ' '.join(post_data.get('hashtags', [])) if 'hashtags' in post_data else ''
            first_comment = post_data.get('firstComment', '') or post_data.get('first_comment', '')

            permanent_url = self.get_permanent_instagram_url(post_data)

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

            OCR TEXT FROM IMAGE(S) (MAY CONTAIN EVENT CALENDAR/LIST):
            NOTE: If this is a carousel post, text from each slide is separated by
            [SLIDE N of M] markers. Each slide may contain different events (e.g.,
            slide 1 = Monday, slide 2 = Tuesday in a weekly schedule carousel).
            Extract events from EVERY slide.

            {ocr_text[:6000]}

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

                try:
                    result = json.loads(text)
                except json.JSONDecodeError as je:
                    # Distinguish JSON parse errors from API errors for the audit trail
                    self._last_gemini_error = f"JSON parse: {str(je)[:150]} | response_start: {text[:100]}"
                    self._last_gemini_error_type = 'GEMINI_JSON_ERROR'
                    with self.lock:
                        self.stats['gemini_errors'] += 1
                    print(f"    ✗ Gemini JSON parse error: {str(je)[:100]}")
                    return []

                events = result.get('events', [])

                processed_events = []
                for event in events:
                    if event and (event.get('event_name') or event.get('date')):
                        event['post_id'] = post_data.get('id', '') or post_data.get('shortCode', '')
                        event['instagram_post_url'] = permanent_url
                        event['post_url'] = self.get_field_value(post_data, 'URL/Link', '')

                        display_url = self.get_field_value(post_data, 'Display URL', '')
                        event['display_url'] = display_url
                        event['display_url_note'] = 'CDN URL - expires after few hours/days'

                        event['instagram_handle'] = owner_username
                        event['instagram_profile_url'] = f"https://www.instagram.com/{owner_username}/" if owner_username else ''
                        event['account_name'] = owner_full_name

                        event['had_ocr'] = bool(ocr_text)
                        event['ocr_chars'] = len(ocr_text)
                        event['post_date'] = post_date.isoformat()
                        event['extraction_timestamp'] = datetime.now().isoformat()

                        event['from_calendar'] = result.get('is_calendar_post', False)
                        event['calendar_type'] = result.get('calendar_period', 'single')

                        event['combined_text'] = f"{caption[:500]} {ocr_text[:500]}"

                        processed_events.append(event)

                if processed_events:
                    print(f"    ✓ Found {len(processed_events)} event(s)")
                    if len(processed_events) > 1:
                        print(f"    📅 MULTIPLE EVENTS EXTRACTED:")
                        for idx, evt in enumerate(processed_events, 1):
                            print(f"      {idx}. {evt.get('event_name', 'Unnamed')} - {evt.get('date', 'No date')}")
                else:
                    # Gemini succeeded but returned no events — record why for audit
                    self._last_gemini_error_type = 'GEMINI_RETURNED_NONE'
                    self._last_gemini_error = f"total_events_found={result.get('total_events_found', 0)}"

                return processed_events

            except Exception as e:
                error_msg = str(e)[:200]
                self._last_gemini_error = error_msg
                self._last_gemini_error_type = 'GEMINI_API_ERROR'
                with self.lock:
                    self.stats['gemini_errors'] += 1
                print(f"    ✗ Gemini error: {error_msg}")

                if '429' in error_msg:
                    print(f"    ⚠ Rate limited - increasing delay")
                    self.rate_limit_delay = min(self.rate_limit_delay * 1.5, 5.0)

                return []

        # ------------------------------------------------------------------
        # POST PROCESSING (with audit logging)
        # ------------------------------------------------------------------

        def process_post(self, post, post_num, total):
            """Process single post. Every post gets logged to the audit trail."""
            post_id = post.get('id', '') or post.get('shortCode', '') or post.get('shortcode', '') or f'post_{post_num}'
            username = self.get_field_value(post, 'Owner Username', 'unknown')
            shortcode = post.get('shortCode', '') or post.get('shortcode', '') or post.get('code', '')
            caption = self.get_field_value(post, 'Caption', '')
            display_url = self.get_field_value(post, 'Display URL', '')
            image_url = self.get_field_value(post, 'Image URL', '')
            had_image = bool(display_url or image_url)
            slides_count = 0  # will be updated after carousel collection

            print(f"\n[{post_num}/{total}] Processing post: {post_id}")
            print(f"  ↳ Account: @{username}")

            # Reset Gemini error tracker for this post
            self._last_gemini_error = None
            self._last_gemini_error_type = None

            with self.lock:
                if post_id in self.processed_posts:
                    print("  ↳ Already processed")
                    self.audit_log(post_id, username, shortcode, 'ALREADY_PROCESSED',
                                   detail='Skipped due to checkpoint',
                                   caption_len=len(caption), had_image=had_image)
                    return None

            try:
                ocr_text = ""
                slides_count = 0

                # Collect ALL image URLs from the post (cover + carousel slides).
                # This handles single-image posts AND multi-slide carousels uniformly.
                all_image_urls = self.collect_all_image_urls(post)
                had_image = had_image or bool(all_image_urls)

                if self.config.get('vision_enabled'):
                    if all_image_urls:
                        if len(all_image_urls) > 1:
                            print(f"  ↳ Found {len(all_image_urls)} images (carousel)")
                        else:
                            print(f"  ↳ Found 1 image")
                        ocr_text, slides_count, _with_text = self.extract_text_from_carousel(
                            all_image_urls, post_id
                        )
                    else:
                        print(f"  ⚠ No image URL found - relying on text fields")
                else:
                    print(f"  ⚠ Vision API disabled - relying on text fields only")
                    slides_count = len(all_image_urls)  # still track that images existed

                all_text = (caption + ' ' + ocr_text).lower()
                calendar_keywords = ['calendar', 'schedule', 'lineup', 'weekly', 'monthly',
                                   'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
                                   'saturday', 'sunday', 'every', 'recurring']
                might_be_calendar = any(keyword in all_text for keyword in calendar_keywords)

                if might_be_calendar:
                    print(f"  📅 Possible calendar/multi-event post detected")

                has_caption = bool(caption)
                has_location = bool(self.get_field_value(post, 'Location', ''))
                has_ocr = bool(ocr_text)

                print(f"  ↳ Data available: caption={has_caption}, location={has_location}, OCR={has_ocr}")

                # Skip if no useful data — BUT log it to audit trail
                if not has_caption and not has_ocr:
                    print(f"  ⚠ No caption or OCR text - skipping")
                    with self.lock:
                        self.processed_posts.add(post_id)
                        self.stats['processed'] += 1

                    if had_image and self.config.get('vision_enabled'):
                        reason = 'OCR_FAILED_NO_CAPTION'
                        detail = 'Image existed but OCR failed; no caption fallback'
                    else:
                        reason = 'NO_TEXT_AVAILABLE'
                        detail = f'caption_empty={not has_caption}, image_present={had_image}, vision_enabled={self.config.get("vision_enabled", False)}'

                    self.audit_log(post_id, username, shortcode, reason,
                                   detail=detail,
                                   caption_len=len(caption),
                                   ocr_len=len(ocr_text),
                                   events_found=0,
                                   had_image=had_image,
                                   slides_count=slides_count)
                    return None

                print(f"  ↳ Analyzing with Gemini AI (checking for multiple events)...")
                events = self.extract_events_with_gemini(post, ocr_text)

                with self.lock:
                    self.processed_posts.add(post_id)
                    self.stats['processed'] += 1

                if events and len(events) > 0:
                    with self.lock:
                        self.stats['posts_with_events'] += 1
                        if len(events) > 1:
                            self.stats['multi_event_posts'] += 1
                            self.stats['max_events_in_post'] = max(self.stats['max_events_in_post'], len(events))
                        if any(event.get('from_calendar', False) for event in events):
                            self.stats['calendar_posts'] += 1
                        for event in events:
                            self.results.append(event)
                            self.stats['events_found'] += 1

                    # Audit: success
                    self.audit_log(post_id, username, shortcode, 'EVENTS_FOUND',
                                   detail=f'{len(events)} event(s) extracted',
                                   caption_len=len(caption),
                                   ocr_len=len(ocr_text),
                                   events_found=len(events),
                                   had_image=had_image,
                                   slides_count=slides_count)

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

                    with self.lock:
                        if self.stats['events_found'] % 10 == 0:
                            self.save_checkpoint()
                            print(f"  ↳ Checkpoint saved ({self.stats['events_found']} total events)")

                    return events
                else:
                    # Zero events — audit with specific reason
                    if self._last_gemini_error_type:
                        reason = self._last_gemini_error_type
                        detail = self._last_gemini_error or ''
                    else:
                        reason = 'GEMINI_RETURNED_NONE'
                        detail = 'Gemini ran but extracted no events'

                    self.audit_log(post_id, username, shortcode, reason,
                                   detail=detail,
                                   caption_len=len(caption),
                                   ocr_len=len(ocr_text),
                                   events_found=0,
                                   had_image=had_image,
                                   slides_count=slides_count)
                    print(f"  ↳ No events found in this post ({reason})")

            except Exception as e:
                print(f"  ✗ Error: {str(e)[:200]}")
                with self.lock:
                    self.stats['processed'] += 1
                self.audit_log(post_id, username, shortcode, 'WORKER_EXCEPTION',
                               detail=str(e)[:300],
                               caption_len=len(caption),
                               had_image=had_image,
                               slides_count=slides_count)

            return None

        # ------------------------------------------------------------------
        # DATA STRUCTURE / CHECKPOINT
        # ------------------------------------------------------------------

        def analyze_data_structure(self, first_post):
            print("\n" + "="*60)
            print("DATA STRUCTURE ANALYSIS")
            print("="*60)

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

            print("\n" + "-" * 40)
            print("Checking for permanent URL capability:")
            if 'ShortCode' in found_fields and found_fields['ShortCode'][1]:
                print(f"  ✓ Can generate permanent Instagram URLs")
            else:
                print(f"  ⚠ No shortcode found - URLs may be temporary")

            if found_fields:
                self.config['field_mappings'] = {}
                for field_type, (field_name, has_content) in found_fields.items():
                    if field_name and has_content:
                        self.config['field_mappings'][field_type] = field_name

            print("\n✓ Data structure analysis complete")

        def save_checkpoint(self):
            checkpoint = {
                'processed_posts': self.processed_posts,
                'results': self.results,
                'stats': self.stats,
                'failed_ocr': self.failed_ocr,
                'successful_ocr': self.successful_ocr,
                'audit_records': self.audit_records,
            }
            with open(self.checkpoint_file, 'wb') as f:
                pickle.dump(checkpoint, f)

        def load_checkpoint(self):
            if self.checkpoint_file.exists():
                try:
                    with open(self.checkpoint_file, 'rb') as f:
                        checkpoint = pickle.load(f)
                    self.processed_posts = checkpoint.get('processed_posts', set())
                    self.results = checkpoint.get('results', [])
                    self.stats = checkpoint.get('stats', self.stats)
                    self.audit_records = checkpoint.get('audit_records', [])
                    print(f"✓ Loaded checkpoint: {len(self.processed_posts)} posts processed, {len(self.results)} events found")
                    return True
                except Exception as e:
                    print(f"⚠ Could not load checkpoint: {e}")
            return False

        async def fetch_instagram_data(self):
            print("\n" + "="*60)
            print("FETCHING INSTAGRAM DATA")
            print("="*60)

            if 'instagram_data' in self.config and self.config['instagram_data']:
                data = self.config['instagram_data']
                print(f"✓ Using provided data: {len(data)} posts")
                return data

            if 'instagram_data_url' in self.config:
                url = self.config['instagram_data_url']
                print(f"Fetching from: {url[:100]}...")
                response = requests.get(url, timeout=60)
                data = response.json()
                print(f"✓ Fetched {len(data)} posts")
                return data

            if 'instagram_data_path' in self.config:
                path = Path(self.config['instagram_data_path'])
                with open(path, 'r') as f:
                    data = json.load(f)
                print(f"✓ Loaded {len(data)} posts from {path}")
                return data

            raise ValueError("No data source configured")

        def create_final_report(self):
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

            print(f"\n📸 CAROUSEL STATISTICS:")
            print(f"  • Carousel posts (>1 slide): {self.stats.get('carousel_posts', 0)}")
            print(f"  • Total slides processed: {self.stats.get('total_slides_processed', 0)}")
            print(f"  • Slides with text found: {self.stats.get('slides_with_text', 0)}")
            print(f"  • Max slides in single post: {self.stats.get('max_slides_in_post', 0)}")
            if self.stats.get('total_slides_processed', 0) > 0:
                slide_text_rate = (self.stats.get('slides_with_text', 0) / self.stats['total_slides_processed']) * 100
                print(f"  • Slide text rate: {slide_text_rate:.1f}%")

            if self.stats['vision_errors']:
                print(f"\n⚠ VISION API ERRORS:")
                for error, count in list(self.stats['vision_errors'].items())[:5]:
                    print(f"  • {error}: {count} times")

            print(f"\n⚠ GEMINI ERRORS: {self.stats['gemini_errors']}")

        async def run(self):
            """Main execution pipeline with parallel processing"""
            posts = await self.fetch_instagram_data()

            if not posts:
                print("✗ No posts to process")
                return None

            self.stats['total_posts'] = len(posts)

            if posts and len(posts) > 0:
                self.analyze_data_structure(posts[0])

            max_posts = self.config.get('max_posts')
            if max_posts:
                posts = posts[:max_posts]
                print(f"\n📋 Processing limited to {max_posts} posts")

            if self.config.get('resume_checkpoint', True):
                self.load_checkpoint()

            print("\n" + "="*60)
            print(f"PROCESSING {len(posts)} POSTS")
            print(f"Vision API: {'ENABLED (with image download)' if self.config.get('vision_enabled') else 'DISABLED'}")
            print(f"Parallel workers: {self.max_workers}")
            print(f"Rate limit delay: {self.rate_limit_delay}s per worker")
            print("="*60)

            if self.max_workers > 1:
                print(f"\n⚡ Processing with {self.max_workers} parallel workers...")

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = {
                        executor.submit(self.process_post, post, i+1, len(posts)): i
                        for i, post in enumerate(posts)
                    }

                    for future in as_completed(futures):
                        try:
                            result = future.result(timeout=60)
                        except Exception as e:
                            print(f"⚠ Error in parallel processing: {e}")
            else:
                print("\n📋 Processing sequentially...")
                for i, post in enumerate(posts):
                    self.process_post(post, i+1, len(posts))

            self.save_checkpoint()
            self.create_final_report()

            # Dump audit files
            self.dump_audit_files()

            if self.results:
                df = pd.DataFrame(self.results)

                if 'date' in df.columns:
                    df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    df = df.sort_values('date', na_position='last')

                column_order = [
                    'event_name', 'date', 'day_of_week', 'time', 'venue_name',
                    'city', 'state', 'section_of_nj', 'price', 'event_type',
                    'instagram_handle', 'instagram_post_url', 'confidence',
                    'from_calendar', 'display_url'
                ]

                existing_columns = [col for col in column_order if col in df.columns]
                other_columns = [col for col in df.columns if col not in column_order]
                df = df[existing_columns + other_columns]

                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

                csv_file = self.output_dir / f'events_{timestamp}.csv'
                df.to_csv(csv_file, index=False)
                print(f"\n✓ CSV saved: {csv_file}")

                try:
                    excel_file = self.output_dir / f'events_{timestamp}.xlsx'
                    df.to_excel(excel_file, index=False)
                    print(f"✓ Excel saved: {excel_file}")
                except Exception as e:
                    print(f"⚠ Could not save Excel file: {e}")

                stats_file = self.output_dir / f'stats_{timestamp}.json'
                with open(stats_file, 'w') as f:
                    json.dump(self.stats, f, indent=2)
                print(f"✓ Stats saved: {stats_file}")

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


    if __name__ == "__main__":
        print("="*60)
        print(" INSTAGRAM EVENT EXTRACTION PIPELINE")
        print(" Claude Environment Version (with audit trail)")
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
        print("Outputs (after each run):")
        print("  events_<timestamp>.csv / .xlsx   - extracted events")
        print("  audit_log_<timestamp>.csv        - every post with outcome + reason")
        print("  zero_event_posts_<timestamp>.csv - posts that came up empty (review these)")
        print("  audit_summary_<timestamp>.txt    - human-readable breakdown")
        print("  stats_<timestamp>.json           - pipeline statistics")