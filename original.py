#!/usr/bin/env python3
"""
Instagram Event Extraction Pipeline - Main Entry Point
Run this script to start the pipeline with configuration from environment variables.
"""

import os
import sys
from pathlib import Path

from instagram_event_pipeline import run_pipeline, InstagramEventPipeline

def get_config_from_env():
    """Build configuration from environment variables"""
    config = {}

    gemini_key = os.environ.get('GEMINI_API_KEY')
    if not gemini_key:
        print("ERROR: GEMINI_API_KEY environment variable is required")
        print("\nTo set it, add GEMINI_API_KEY in the Secrets tab")
        return None
    config['gemini_api_key'] = gemini_key

    vision_json = os.environ.get('GOOGLE_VISION_JSON_PATH')
    if vision_json:
        config['vision_json_path'] = vision_json

    data_url = os.environ.get('INSTAGRAM_DATA_URL')
    if data_url:
        config['instagram_data_url'] = data_url

    data_path = os.environ.get('INSTAGRAM_DATA_PATH')
    if data_path:
        config['instagram_data_path'] = data_path

    max_workers = os.environ.get('MAX_WORKERS')
    if max_workers:
        try:
            config['max_workers'] = int(max_workers)
        except ValueError:
            pass

    max_posts = os.environ.get('MAX_POSTS')
    if max_posts:
        try:
            config['max_posts'] = int(max_posts)
        except ValueError:
            pass

    rate_limit = os.environ.get('RATE_LIMIT_DELAY')
    if rate_limit:
        try:
            config['rate_limit_delay'] = float(rate_limit)
        except ValueError:
            pass

    output_dir = os.environ.get('OUTPUT_DIR', './outputs')
    config['output_dir'] = output_dir

    return config


def print_usage():
    """Print usage instructions"""
    print("=" * 60)
    print(" INSTAGRAM EVENT EXTRACTION PIPELINE")
    print(" Replit Environment")
    print("=" * 60)
    print()
    print("REQUIRED ENVIRONMENT VARIABLES:")
    print("  GEMINI_API_KEY       - Your Google Gemini API key")
    print()
    print("OPTIONAL ENVIRONMENT VARIABLES:")
    print("  GOOGLE_VISION_JSON_PATH - Path to Vision API service account JSON")
    print("  INSTAGRAM_DATA_URL      - URL to fetch Instagram JSON data")
    print("  INSTAGRAM_DATA_PATH     - Local path to Instagram JSON file")
    print("  MAX_WORKERS             - Parallel workers (1-5, default 3)")
    print("  MAX_POSTS               - Maximum posts to process")
    print("  RATE_LIMIT_DELAY        - Delay between API calls (default 0.5)")
    print("  OUTPUT_DIR              - Output directory (default ./outputs)")
    print()
    print("DATA SOURCE (provide one):")
    print("  - INSTAGRAM_DATA_URL: URL to JSON endpoint")
    print("  - INSTAGRAM_DATA_PATH: Path to local JSON file")
    print()
    print("Set these in the Secrets tab in Replit.")
    print()


def main():
    """Main entry point"""
    print_usage()

    config = get_config_from_env()

    if config is None:
        print("\nConfiguration incomplete. Please set required environment variables.")
        sys.exit(1)

    if not config.get('instagram_data_url') and not config.get('instagram_data_path'):
        print("\nNo data source configured.")
        print("Please set INSTAGRAM_DATA_URL or INSTAGRAM_DATA_PATH")
        print("\nPipeline is ready. Waiting for data source configuration...")
        print("\nTo test the pipeline, you can also import and use it programmatically:")
        print()
        print("  from instagram_event_pipeline import run_pipeline")
        print("  ")
        print("  config = {")
        print("      'gemini_api_key': 'YOUR_KEY',")
        print("      'instagram_data': [  # Your Instagram post data")
        print("          {'caption': 'Event post', 'shortCode': 'ABC123', ...}")
        print("      ]")
        print("  }")
        print("  df = run_pipeline(config)")
        return

    print("\nStarting pipeline with configuration:")
    for key, value in config.items():
        if 'key' in key.lower() or 'api' in key.lower():
            print(f"  {key}: ***hidden***")
        else:
            print(f"  {key}: {value}")

    try:
        result = run_pipeline(config)
        if result is not None:
            print(f"\n✓ Pipeline completed! Extracted {len(result)} events.")
        else:
            print("\n✗ No events were extracted.")
    except Exception as e:
        print(f"\n✗ Pipeline error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
