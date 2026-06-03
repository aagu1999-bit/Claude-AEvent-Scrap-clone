#!/usr/bin/env python3
"""
lookup_post.py — Full evidence trail for one or more Instagram post IDs.

Usage:
    python lookup_post.py DYDG7CHowpC
    python lookup_post.py DYDG7CHowpC AnotherShortcode YetAnother

What it does:
    For each post_id you pass, it scrapes every diagnostic data source we
    save and prints a single unified report. Designed so you can paste the
    output to a debugging partner without needing follow-up "what about X"
    questions.

Sources checked (in this order):
    1. Apify raw dumps     — outputs/apify_raw_*.json  (was the post ever scraped?)
    2. Apify cache         — outputs/apify_cache/*.json
    3. Accounts tab        — Google Sheet "Accounts" tab (is the owner in our list?)
    4. Processed_Log tab   — every row for the pid, latest first
    5. All_Events tab      — any event rows produced from this post
    6. Anomalies summaries — outputs/anomalies_*.json (Gemini partial responses)
    7. Run logs            — outputs/run_*.log  (every line tagged [pid])
    8. Events CSVs         — outputs/Events_*.csv (final event output per run)
    9. dataset_run_log     — outputs/dataset_run_log.json  (Apify URL mapping)

Auth:
    Uses the same Google service-account JSON as main.py. Set
    GOOGLE_APPLICATION_CREDENTIALS or SERVICE_ACCOUNT_FILE env var, or
    fall back to the legacy hardcoded path. If neither sheet auth nor
    the file works, the tool still runs local-archive-only.
"""

import os
import sys
import json
import csv
import glob
import argparse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────
# Configuration — mirrors main.py's conventions
# ─────────────────────────────────────────────────────────────────
SERVICE_ACCOUNT_FILE = (
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    or os.environ.get("SERVICE_ACCOUNT_FILE")
    or "apt-mark-468506-u9-ec44cabc7335 copy.json"
)
SHEET_NAME = os.environ.get("SHEET_NAME", "Instagram_Events_Master")

OUTPUTS_DIR = Path("outputs")
APIFY_RAW_GLOB    = str(OUTPUTS_DIR / "apify_raw_*.json")
APIFY_CACHE_GLOB  = str(OUTPUTS_DIR / "apify_cache" / "*.json")
RUN_LOG_GLOB      = str(OUTPUTS_DIR / "run_*.log")
ANOMALIES_GLOB    = str(OUTPUTS_DIR / "anomalies_*.json")
EVENTS_CSV_GLOB   = str(OUTPUTS_DIR / "Events_*.csv")
DATASET_RUN_LOG   = OUTPUTS_DIR / "dataset_run_log.json"

# Apify console URL template, for click-through
APIFY_DATASET_URL = "https://console.apify.com/storage/datasets/{ds_id}"


def section(title):
    print()
    print(f"  [{title}]")


def divider(char="─", width=72):
    print(char * width)


# ─────────────────────────────────────────────────────────────────
# 1 & 2. Local Apify dump search
# ─────────────────────────────────────────────────────────────────
def search_apify_dumps(pid):
    """Search every apify_raw_*.json and apify_cache/*.json for the pid.
    Returns:
      {
        'first_seen': (timestamp_str, path),
        'last_seen' : (timestamp_str, path),
        'count'     : int,
        'owner'     : str,
        'caption'   : str,
        'image_urls': [str, ...],
        'dataset_ids': set,
        'all_hits'  : [(path, dump_meta_dict), ...],
      } — or None if not found anywhere.
    """
    raw_files = sorted(glob.glob(APIFY_RAW_GLOB))
    cache_files = sorted(glob.glob(APIFY_CACHE_GLOB))

    hits = []   # list of (path, post_dict, fetched_at, dataset_id)

    for path in raw_files:
        try:
            with open(path) as f:
                dump = json.load(f)
        except Exception:
            continue
        posts = dump.get('posts') or []
        fetched_at = dump.get('fetched_at', '')
        dataset_id = dump.get('dataset_id', '')
        for p in posts:
            if (p.get('id') or p.get('shortCode')) == pid:
                hits.append((path, p, fetched_at, dataset_id))

    for path in cache_files:
        try:
            with open(path) as f:
                posts = json.load(f)
        except Exception:
            continue
        if not isinstance(posts, list):
            continue
        # Cache files are named by dataset_id; pull from filename.
        ds_id = Path(path).stem
        for p in posts:
            if (p.get('id') or p.get('shortCode')) == pid:
                # Cache files don't have a fetched_at — leave blank.
                hits.append((path, p, '', ds_id))

    if not hits:
        return None

    hits.sort(key=lambda h: h[2])  # by fetched_at; '' sorts first
    first = hits[0]
    last = hits[-1]
    rep_post = last[1]  # use the most recent for owner/caption

    image_urls = []
    for k in ('displayUrl', 'display_url'):
        v = rep_post.get(k)
        if v and v not in image_urls:
            image_urls.append(v)
    for img in (rep_post.get('images') or []):
        if isinstance(img, str) and img not in image_urls:
            image_urls.append(img)
        elif isinstance(img, dict):
            v = img.get('url') or img.get('displayUrl')
            if v and v not in image_urls:
                image_urls.append(v)

    return {
        'first_seen': (first[2] or '(no timestamp)', first[0]),
        'last_seen' : (last[2] or '(no timestamp)', last[0]),
        'count'     : len(hits),
        'owner'     : rep_post.get('ownerUsername', '') or rep_post.get('username', '') or '',
        'caption'   : rep_post.get('caption', '') or rep_post.get('text', '') or '',
        'image_urls': image_urls,
        'dataset_ids': sorted({h[3] for h in hits if h[3]}),
        'all_hits'  : [(p[0], p[2], p[3]) for p in hits],
    }


# ─────────────────────────────────────────────────────────────────
# Sheet access
# ─────────────────────────────────────────────────────────────────
def open_sheet():
    """Return (sheet_obj, error_msg). On success error_msg is None."""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        return None, f"Service account file '{SERVICE_ACCOUNT_FILE}' not found"
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive',
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
        client = gspread.authorize(creds)
        sheet = client.open(SHEET_NAME)
        return sheet, None
    except Exception as e:
        return None, f"Sheet open failed: {e}"


def get_accounts_set(sheet):
    """Return lowercased set of usernames from the 'Accounts' tab, or None."""
    if sheet is None:
        return None
    try:
        ws = sheet.worksheet("Accounts")
        col = ws.col_values(1)
        # First row is the header
        return {u.strip().lower() for u in col[1:] if u.strip()}
    except Exception:
        return None


def search_processed_log(sheet, pid):
    """Return list of dicts (one per matching row, oldest first) or []."""
    if sheet is None:
        return None
    try:
        ws = sheet.worksheet("Processed_Log")
        all_rows = ws.get_all_values()
        if not all_rows:
            return []
        header = all_rows[0]
        rows = all_rows[1:]
        matches = []
        for row in rows:
            if row and row[0].strip() == pid:
                rec = {}
                for i, col_name in enumerate(header):
                    rec[col_name] = row[i] if i < len(row) else ''
                matches.append(rec)
        return matches
    except Exception as e:
        print(f"  ⚠ Processed_Log read failed: {e}")
        return None


def search_all_events(sheet, pid):
    """Return list of dicts (one per matching row) or []."""
    if sheet is None:
        return None
    try:
        ws = sheet.worksheet("All_Events")
        all_rows = ws.get_all_values()
        if not all_rows:
            return []
        header = all_rows[0]
        # Find the POST ID column (case-insensitive)
        post_id_idx = None
        for i, h in enumerate(header):
            if h.strip().upper().replace(' ', '_') in ('POST_ID', 'POSTID', 'POST ID'):
                post_id_idx = i
                break
        if post_id_idx is None:
            # Fall back to the very last position the schema usually has it
            return []
        rows = all_rows[1:]
        matches = []
        for row in rows:
            if post_id_idx < len(row) and row[post_id_idx].strip().upper() == pid.upper():
                rec = {}
                for i, col_name in enumerate(header):
                    rec[col_name] = row[i] if i < len(row) else ''
                matches.append(rec)
        return matches
    except Exception as e:
        print(f"  ⚠ All_Events read failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
# 6. Anomaly summaries
# ─────────────────────────────────────────────────────────────────
def search_anomalies(pid):
    """Return list of (run_id, anomaly_entry_dict)."""
    matches = []
    for path in sorted(glob.glob(ANOMALIES_GLOB)):
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            continue
        run_id = data.get('run_id', Path(path).stem.replace('anomalies_', ''))
        # Anomaly file layouts have varied; defend against both:
        # newer: {'run_id':..., 'anomalies': {pid: {...}}}
        # older: a dict of pid → entry directly
        anomalies = data.get('anomalies') or data
        if isinstance(anomalies, dict) and pid in anomalies:
            matches.append((run_id, path, anomalies[pid]))
    return matches


# ─────────────────────────────────────────────────────────────────
# 7. Run log grep
# ─────────────────────────────────────────────────────────────────
def search_run_logs(pid, max_lines_per_file=80):
    """Return list of (logfile_path, [matching_line, ...])."""
    pattern = f"[{pid}]"  # the worker tag includes [pid] explicitly
    matches = []
    for path in sorted(glob.glob(RUN_LOG_GLOB)):
        hits = []
        try:
            with open(path, errors='replace') as f:
                for line in f:
                    if pattern in line:
                        hits.append(line.rstrip())
                        if len(hits) >= max_lines_per_file:
                            hits.append(f"  ... ({len(hits)}+ matches, truncated)")
                            break
        except Exception:
            continue
        if hits:
            matches.append((path, hits))
    return matches


# ─────────────────────────────────────────────────────────────────
# 8. Events CSV history
# ─────────────────────────────────────────────────────────────────
def search_events_csvs(pid):
    """Return list of (csv_path, list_of_row_dicts_for_this_pid)."""
    matches = []
    for path in sorted(glob.glob(EVENTS_CSV_GLOB)):
        rows = []
        try:
            with open(path, newline='', errors='replace') as f:
                reader = csv.DictReader(f)
                # Schemas have changed over time — try multiple column names
                for r in reader:
                    pid_in_row = (
                        r.get('POST ID') or r.get('post_id') or r.get('POST_ID')
                        or r.get('Post ID') or ''
                    ).strip()
                    if pid_in_row.upper() == pid.upper():
                        rows.append(r)
        except Exception:
            continue
        if rows:
            matches.append((path, rows))
    return matches


# ─────────────────────────────────────────────────────────────────
# 9. dataset_run_log
# ─────────────────────────────────────────────────────────────────
def load_dataset_run_log():
    if not DATASET_RUN_LOG.exists():
        return []
    try:
        with open(DATASET_RUN_LOG) as f:
            return json.load(f)
    except Exception:
        return []


def lookup_dataset_runs_for_ids(dataset_ids, run_log):
    """Return rows of run_log whose dataset_id is in dataset_ids."""
    if not dataset_ids:
        return []
    ds_set = set(dataset_ids)
    return [e for e in run_log if e.get('dataset_id') in ds_set]


# ─────────────────────────────────────────────────────────────────
# Report rendering
# ─────────────────────────────────────────────────────────────────
def fmt_caption(s, max_len=300):
    if not s:
        return "(empty)"
    s = s.replace('\n', ' ').strip()
    if len(s) > max_len:
        return s[:max_len] + f"... ({len(s)} chars total)"
    return s


def report_post(pid, sheet, accounts_set, run_log):
    print()
    divider("═")
    print(f"  POST: {pid}")
    print(f"  URL : https://www.instagram.com/p/{pid}/")
    divider("═")

    # ─── 1. Apify scrape history ─────────────────────────────────
    section("APIFY SCRAPE HISTORY")
    scrape = search_apify_dumps(pid)
    if scrape is None:
        print("  ✗ Not found in any local apify_raw_*.json or apify_cache/")
        print("    → Either never scraped, OR raw dumps have been deleted.")
        owner = None
    else:
        print(f"  • First seen : {scrape['first_seen'][0]}  ({Path(scrape['first_seen'][1]).name})")
        print(f"  • Last seen  : {scrape['last_seen'][0]}  ({Path(scrape['last_seen'][1]).name})")
        print(f"  • Times seen : {scrape['count']} dump file(s)")
        print(f"  • Owner      : @{scrape['owner']}" if scrape['owner'] else "  • Owner      : (not in payload)")
        print(f"  • Caption    : {fmt_caption(scrape['caption'])}")
        if scrape['image_urls']:
            print(f"  • Images     : {len(scrape['image_urls'])} URL(s)")
            for i, u in enumerate(scrape['image_urls'][:3], 1):
                print(f"      [{i}] {u[:120]}{'...' if len(u) > 120 else ''}")
        if scrape['dataset_ids']:
            print(f"  • Datasets   : {len(scrape['dataset_ids'])} distinct")
            for ds in scrape['dataset_ids']:
                print(f"      → {APIFY_DATASET_URL.format(ds_id=ds)}")
        owner = scrape['owner'].lower() if scrape['owner'] else None

    # ─── 2. Accounts tab membership ──────────────────────────────
    section("ACCOUNTS TAB MEMBERSHIP")
    if accounts_set is None:
        print("  ? Could not read 'Accounts' tab (no sheet access). Skipping.")
    elif owner is None:
        print("  ? Owner unknown (post not in any local Apify dump). Skipping.")
    else:
        if owner in accounts_set:
            print(f"  ✓ @{owner} IS in the Accounts tab")
        else:
            print(f"  ✗ @{owner} is NOT in the Accounts tab")
            print(f"    → If this post was scraped anyway, it came from another route.")
            print(f"      If you want this account's posts ingested regularly, add @{owner}.")

    # ─── 3. Processed_Log ────────────────────────────────────────
    section("PROCESSED_LOG ENTRIES")
    pl_rows = search_processed_log(sheet, pid)
    if pl_rows is None:
        print("  ? Could not read Processed_Log (no sheet access).")
    elif not pl_rows:
        print("  ✗ No rows in Processed_Log for this post_id.")
        print("    → The pipeline NEVER processed this post.")
    else:
        # Show newest first (assume last in iteration = newest)
        for i, row in enumerate(reversed(pl_rows), 1):
            print(f"  [{i}] " + " | ".join(
                f"{k}={v!r}" for k, v in row.items() if v
            ))

    # ─── 4. All_Events ───────────────────────────────────────────
    section("ALL_EVENTS ENTRIES")
    ae_rows = search_all_events(sheet, pid)
    if ae_rows is None:
        print("  ? Could not read All_Events (no sheet access).")
    elif not ae_rows:
        print("  ✗ No event rows in All_Events for this post_id.")
    else:
        print(f"  ✓ {len(ae_rows)} event row(s):")
        for i, row in enumerate(ae_rows, 1):
            interesting = ['EVENT NAME', 'DATE', 'START TIME', 'VENUE NAME', 'CITY',
                           'CONFIDENCE', 'QUALITY FLAGS', 'WORKER ID', 'RUN ID', 'ATTEMPT ID']
            print(f"    Event #{i}:")
            for k in interesting:
                if k in row and row[k]:
                    print(f"      {k:<14} {row[k]}")

    # ─── 5. Anomaly summaries ────────────────────────────────────
    section("ANOMALY SUMMARIES")
    anom = search_anomalies(pid)
    if not anom:
        print("  · No anomaly entries (post either succeeded or pre-dates anomaly tracking).")
    else:
        for run_id, path, entry in anom:
            print(f"  Run {run_id}  ({Path(path).name}):")
            for k, v in entry.items():
                if v:
                    sv = str(v)
                    if len(sv) > 400:
                        sv = sv[:400] + f"... ({len(str(v))} chars)"
                    print(f"    {k}: {sv}")

    # ─── 6. Run logs ─────────────────────────────────────────────
    section("RUN LOG SNIPPETS")
    logs = search_run_logs(pid)
    if not logs:
        print("  · No matches in any run_*.log (either never logged, or logs cleaned up).")
    else:
        for path, lines in logs:
            print(f"  {Path(path).name}:")
            for ln in lines:
                print(f"    {ln}")

    # ─── 7. Events CSVs ──────────────────────────────────────────
    section("EVENTS_*.CSV HISTORY")
    csvs = search_events_csvs(pid)
    if not csvs:
        print("  · No event rows for this post in any Events_*.csv file.")
    else:
        for path, rows in csvs:
            print(f"  {Path(path).name}  ({len(rows)} row(s))")

    # ─── 8. dataset_run_log cross-ref ────────────────────────────
    section("APIFY DATASET RUN LOG (cross-reference)")
    if not run_log:
        print("  · dataset_run_log.json missing or empty.")
    elif scrape is None or not scrape['dataset_ids']:
        print("  · No dataset_ids to cross-reference (post not in any local dump).")
    else:
        rows = lookup_dataset_runs_for_ids(scrape['dataset_ids'], run_log)
        if not rows:
            print("  · No matching dataset_run_log entries.")
        else:
            for r in rows:
                print(f"  • {r.get('run_timestamp', '?')}  src={r.get('source', '?')}  "
                      f"posts={r.get('post_count', '?')}  ds={r.get('dataset_id', '?')}")


def main():
    parser = argparse.ArgumentParser(description="Diagnose missed Instagram posts.")
    parser.add_argument('post_ids', nargs='+',
                        help="One or more post IDs (Instagram shortcodes). "
                             "Accepts full URLs too — the shortcode is extracted.")
    args = parser.parse_args()

    # Normalize: accept full URLs
    pids = []
    for raw in args.post_ids:
        s = raw.strip().rstrip('/')
        if 'instagram.com/p/' in s:
            s = s.rsplit('/p/', 1)[1].split('/')[0].split('?')[0]
        pids.append(s)

    print()
    divider("═")
    print(f"  lookup_post.py — diagnosing {len(pids)} post id(s)")
    print(f"  service-account: {SERVICE_ACCOUNT_FILE}")
    print(f"  sheet name     : {SHEET_NAME}")
    divider("═")

    sheet, err = open_sheet()
    if err:
        print(f"\n  ⚠ Sheet access unavailable: {err}")
        print(f"    Continuing with local-archive-only analysis (no Processed_Log, ")
        print(f"    All_Events, or Accounts tab data).")
    else:
        print(f"\n  ✓ Connected to '{SHEET_NAME}'")

    accounts_set = get_accounts_set(sheet)
    if sheet is not None and accounts_set is None:
        print(f"  ⚠ Accounts tab unreadable — owner membership check disabled.")

    run_log = load_dataset_run_log()

    for pid in pids:
        report_post(pid, sheet, accounts_set, run_log)

    print()
    divider("═")
    print("  Done. Paste the report above to your debugging partner.")
    divider("═")
    print()


if __name__ == "__main__":
    main()
