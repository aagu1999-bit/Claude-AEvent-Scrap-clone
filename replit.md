# Claude-AEvent-Scrap - Instagram Event Extraction Pipeline

## Overview
Python-based pipeline that extracts event information from Instagram posts using Google Gemini AI for natural language processing and Google Cloud Vision API for OCR text extraction from images.

## Project Status
- **Current State**: Fully configured and ready to run
- **Language**: Python 3.11
- **Dependencies**: Managed via uv (pyproject.toml)
- **Workflow**: Event Pipeline (console-based)

## Features
- Parallel processing with configurable workers (1-5)
- Multi-event extraction from single posts (calendars, weekly lineups)
- Image OCR using Google Cloud Vision API
- Permanent Instagram URL generation
- CSV and Excel output with sorted results
- Checkpoint/resume support for large batches
- Rate limiting and error handling

## Project Structure
```
/
├── main.py                      # Entry point script
├── instagram_event_pipeline.py  # Core pipeline class
├── pyproject.toml               # Python dependencies
├── outputs/                     # Generated output files
└── .gitignore                   # Git ignore rules
```

## Required Configuration
Set these in the **Secrets** tab:

| Variable | Required | Description |
|----------|----------|-------------|
| `GEMINI_API_KEY` | Yes | Google Gemini API key |
| `GOOGLE_VISION_JSON_PATH` | No | Path to Vision API service account JSON |
| `INSTAGRAM_DATA_URL` | One of these | URL to fetch Instagram JSON data |
| `INSTAGRAM_DATA_PATH` | One of these | Local path to Instagram JSON file |

## Optional Configuration
| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_WORKERS` | 3 | Parallel workers (1-5) |
| `MAX_POSTS` | All | Maximum posts to process |
| `RATE_LIMIT_DELAY` | 0.5 | Delay between API calls (seconds) |
| `OUTPUT_DIR` | ./outputs | Output directory path |

## Running the Pipeline

### Via Workflow
Click the Run button to start the Event Pipeline workflow. It will display usage instructions if API keys are not configured.

### Programmatic Usage
```python
from instagram_event_pipeline import run_pipeline

config = {
    'gemini_api_key': 'YOUR_GEMINI_API_KEY',
    'vision_json_path': '/path/to/service-account.json',  # Optional
    'instagram_data': [  # Or use instagram_data_url
        {'caption': 'Event post...', 'shortCode': 'ABC123', ...}
    ],
    'max_posts': 50,
}

df = run_pipeline(config)
```

## Output Files
Pipeline generates:
- `events_YYYYMMDD_HHMMSS.csv` - Event data in CSV format
- `events_YYYYMMDD_HHMMSS.xlsx` - Event data in Excel format
- `stats_YYYYMMDD_HHMMSS.json` - Processing statistics

## Recent Changes
- **2025-11-28**: Imported from GitHub and configured for Replit environment
  - Created proper project structure
  - Installed Python dependencies
  - Added main.py entry point with environment variable support
  - Configured Event Pipeline workflow

## User Preferences
- Console-based workflow (no web frontend)
- API keys stored in Secrets
