import pickle
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
import datetime

# --- CONFIGURATION ---
CHECKPOINT_FILE = "outputs/pipeline_checkpoint.pkl"  # Correct path
SERVICE_ACCOUNT_FILE = "apt-mark-468506-u9-ec44cabc7335 copy.json"  # Use your uploaded JSON
SHEET_NAME = "Instagram_Events_Master"  # The name of the sheet you want to use


def migrate():
    print("🚀 STARTING MIGRATION...")

    # 1. Load the old history
    if not Path(CHECKPOINT_FILE).exists():
        print(f"❌ Error: Could not find {CHECKPOINT_FILE}")
        return

    try:
        with open(CHECKPOINT_FILE, 'rb') as f:
            data = pickle.load(f)
            # The structure from your original code was:
            # checkpoint = {'processed_posts': set(), ...}
            old_ids = data.get('processed_posts', set())

        print(f"✓ Found {len(old_ids)} IDs in your local checkpoint file.")
    except Exception as e:
        print(f"❌ Error reading pickle file: {e}")
        return

    if not old_ids:
        print("⚠ No IDs found to migrate.")
        return

    # 2. Connect to Google Sheets
    print("connecting to Google Sheets...")
    try:
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            SERVICE_ACCOUNT_FILE, scope)
        client = gspread.authorize(creds)

        # Open or Create Sheet
        try:
            sheet = client.open(SHEET_NAME)
            print(f"✓ Opened existing sheet: {SHEET_NAME}")
        except gspread.SpreadsheetNotFound:
            print(f"✓ Creating new sheet: {SHEET_NAME}")
            sheet = client.create(SHEET_NAME)
            sheet.share(creds.service_account_email,
                        perm_type='user',
                        role='owner')

        # Get or Create the Log Tab
        try:
            worksheet = sheet.worksheet("Processed_Log")
        except:
            print("  ↳ Creating 'Processed_Log' tab...")
            worksheet = sheet.add_worksheet(title="Processed_Log",
                                            rows="5000",
                                            cols="4")
            worksheet.append_row(
                ["Post ID", "Migration Date", "Source", "Notes"])

        # 3. Check what's already in the sheet to avoid duplicates
        existing_sheet_ids = set(worksheet.col_values(1)[1:])  # Skip header

        # Filter: Only upload IDs that aren't already in the sheet
        ids_to_upload = [
            uid for uid in old_ids if uid not in existing_sheet_ids
        ]

        if not ids_to_upload:
            print(
                "✓ All IDs are already in the Google Sheet. No action needed.")
            return

        print(f"  ↳ Uploading {len(ids_to_upload)} new IDs to the cloud...")

        # 4. Prepare Batch Upload (Much faster than one by one)
        # Format: [ID, Date, Source, Note]
        today = str(datetime.date.today())
        rows = [[uid, today, "Migration Script", "Imported from checkpoint"]
                for uid in ids_to_upload]

        # Upload in chunks of 500 to be safe
        chunk_size = 500
        for i in range(0, len(rows), chunk_size):
            worksheet.append_rows(rows[i:i + chunk_size])
            print(f"    ...uploaded batch {i} - {i+chunk_size}")

        print("\n✅ MIGRATION COMPLETE!")
        print(
            f"Total IDs tracked in Sheet: {len(existing_sheet_ids) + len(ids_to_upload)}"
        )

    except Exception as e:
        print(f"❌ Migration failed: {e}")


if __name__ == "__main__":
    migrate()
