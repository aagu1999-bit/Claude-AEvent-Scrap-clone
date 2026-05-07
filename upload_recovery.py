#!/usr/bin/env python3
"""
Recovery script: uploads existing Events CSV to the All_Events tab in Google Sheets.
Usage: python upload_recovery.py outputs/Events_20260507_141723.csv
"""
import sys
import json
import math
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "outputs/Events_20260507_141723.csv"
SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"

with open("config.json") as f:
    cfg = json.load(f)
SHEET_NAME = cfg.get("sheet_name", "Instagram_Events_Master")

print(f"Loading CSV: {CSV_PATH}")
df = pd.read_csv(CSV_PATH, dtype=str).fillna("")
print(f"  {len(df)} rows, {len(df.columns)} columns")

scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)

spreadsheet = client.open(SHEET_NAME)
print(f"Connected to sheet: {SHEET_NAME}")

try:
    ws = spreadsheet.worksheet("All_Events")
    print("Found existing All_Events tab")
except gspread.WorksheetNotFound:
    ws = spreadsheet.add_worksheet("All_Events", rows=5000, cols=len(df.columns))
    print("Created new All_Events tab")

existing = ws.get_all_values()
if not existing:
    ws.append_row(df.columns.tolist())
    print("Wrote header row")

def sanitize(val):
    if val is None:
        return ""
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return ""
    return str(val)

rows = [[sanitize(v) for v in row] for row in df.values.tolist()]

BATCH = 500
total = len(rows)
uploaded = 0
for i in range(0, total, BATCH):
    chunk = rows[i:i + BATCH]
    ws.append_rows(chunk, value_input_option="RAW")
    uploaded += len(chunk)
    print(f"  Uploaded {uploaded}/{total} rows...")

print(f"\nDone! {uploaded} events written to All_Events tab in '{SHEET_NAME}'.")
