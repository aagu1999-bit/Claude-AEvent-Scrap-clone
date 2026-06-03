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


def _row_to_dict(header, row):
    """Zip a row list with a header list, padding missing cells with ''."""
    return {col: (row[i] if i < len(row) else '') for i, col in enumerate(header)}


def _find_matching_rows(ws, pid):
    """Robust row-finder. Tries multiple case variants AND falls back to the
    Instagram URL (which contains the shortcode and is the one column the
    pipeline does NOT uppercase). Returns:
      {'header': [...],
       'row_count': int,
       'matched_rows': [row_dict, ...],
       'tried': [list of strings that were searched],
       'matched_via': str describing how the match was made}
    """
    header = ws.row_values(1)
    # row_count is the worksheet's row dimension, not the populated count
    populated_row_count = None
    try:
        populated_row_count = len(ws.col_values(1))
    except Exception:
        populated_row_count = ws.row_count

    candidates = [pid, pid.upper(), pid.lower()]
    # also try the URL form — instagram_post_url / post_url contain the shortcode
    # verbatim AND are excluded from save_data's uppercase pass, so case is
    # preserved. This is the most-likely-to-match fallback if column-based
    # search misses for any reason.
    url_form = f"https://www.instagram.com/p/{pid}/"
    candidates += [url_form, url_form.lower()]

    seen_query = set()
    cells_found = []
    matched_via = None
    tried = []
    for q in candidates:
        if q in seen_query or not q:
            continue
        seen_query.add(q)
        tried.append(q)
        try:
            hits = ws.findall(q)
        except Exception as e:
            tried[-1] = f"{q} (findall error: {e})"
            continue
        if hits:
            cells_found = hits
            matched_via = q
            break

    matched_rows = []
    seen_rownums = set()
    if cells_found:
        for cell in cells_found:
            if cell.row == 1 or cell.row in seen_rownums:
                continue
            seen_rownums.add(cell.row)
            try:
                row = ws.row_values(cell.row)
            except Exception:
                continue
            matched_rows.append(_row_to_dict(header, row))

    return {
        'header': header,
        'row_count': populated_row_count,
        'matched_rows': matched_rows,
        'tried': tried,
        'matched_via': matched_via,
    }


def search_processed_log(sheet, pid):
    """Return search result dict or None (no sheet access)."""
    if sheet is None:
        return None
    try:
        ws = sheet.worksheet("Processed_Log")
    except Exception as e:
        print(f"  ⚠ Could not open Processed_Log worksheet: {e}")
        return None
    try:
        return _find_matching_rows(ws, pid)
    except Exception as e:
        print(f"  ⚠ Processed_Log search failed: {e}")
        return None


def search_all_events(sheet, pid):
    """Return search result dict or None (no sheet access)."""
    if sheet is None:
        return None
    try:
        ws = sheet.worksheet("All_Events")
    except Exception as e:
        print(f"  ⚠ Could not open All_Events worksheet: {e}")
        return None
    try:
        return _find_matching_rows(ws, pid)
    except Exception as e:
        print(f"  ⚠ All_Events search failed: {e}")
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
    pl_res = search_processed_log(sheet, pid)
    if pl_res is None:
        print("  ? Could not read Processed_Log (no sheet access).")
    else:
        print(f"  · tab has ~{pl_res['row_count']} populated rows")
        print(f"  · header ({len(pl_res['header'])} cols): {pl_res['header']}")
        print(f"  · queries tried: {pl_res['tried']}")
        if not pl_res['matched_rows']:
            print("  ✗ No rows matched any case/URL variant of the post_id.")
            print("    → The pipeline NEVER processed this post — OR there's a "
                  "case mismatch we still aren't catching. If you can SEE this "
                  "post_id in the sheet, copy the EXACT cell text and re-run.")
        else:
            print(f"  ✓ matched_via={pl_res['matched_via']!r} → "
                  f"{len(pl_res['matched_rows'])} row(s):")
            for i, row in enumerate(reversed(pl_res['matched_rows']), 1):
                print(f"  [{i}] " + " | ".join(
                    f"{k}={v!r}" for k, v in row.items() if v
                ))

    # ─── 4. All_Events ───────────────────────────────────────────
    section("ALL_EVENTS ENTRIES")
    ae_res = search_all_events(sheet, pid)
    if ae_res is None:
        print("  ? Could not read All_Events (no sheet access).")
    else:
        print(f"  · tab has ~{ae_res['row_count']} populated rows")
        print(f"  · header ({len(ae_res['header'])} cols): {ae_res['header']}")
        print(f"  · queries tried: {ae_res['tried']}")
        if not ae_res['matched_rows']:
            print("  ✗ No rows matched any case/URL variant of the post_id.")
        else:
            print(f"  ✓ matched_via={ae_res['matched_via']!r} → "
                  f"{len(ae_res['matched_rows'])} event row(s):")
            interesting = ['EVENT NAME', 'DATE', 'START TIME', 'VENUE NAME', 'CITY',
                           'CONFIDENCE', 'QUALITY FLAGS', 'INSTAGRAM HANDLE',
                           'INSTAGRAM POST URL', 'WORKER ID', 'RUN ID', 'ATTEMPT ID',
                           'PROCESSED TIMESTAMP', 'HAD OCR', 'FROM CALENDAR']
            for i, row in enumerate(ae_res['matched_rows'], 1):
                print(f"    Event #{i}:")
                for k in interesting:
                    if k in row and row[k]:
                        print(f"      {k:<22} {row[k]}")
                # also dump any non-empty columns we didn't list above
                extras = [k for k in row if k not in interesting and row[k]]
                if extras:
                    print(f"      (other non-empty: {extras})")

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

    # ─── Local archive census ─────────────────────────────────────
    # The most common "the tool says nothing exists" cause is that Replit
    # wiped outputs/ between runs. Print archive counts upfront so we know
    # whether the local-search sections are searching real data or thin air.
    raw_count    = len(glob.glob(APIFY_RAW_GLOB))
    cache_count  = len(glob.glob(APIFY_CACHE_GLOB))
    log_count    = len(glob.glob(RUN_LOG_GLOB))
    anom_count   = len(glob.glob(ANOMALIES_GLOB))
    csv_count    = len(glob.glob(EVENTS_CSV_GLOB))
    print()
    print(f"  [LOCAL ARCHIVE CENSUS]")
    print(f"  · apify_raw_*.json    : {raw_count} file(s)")
    print(f"  · apify_cache/*.json  : {cache_count} file(s)")
    print(f"  · run_*.log           : {log_count} file(s)")
    print(f"  · anomalies_*.json    : {anom_count} file(s)")
    print(f"  · Events_*.csv        : {csv_count} file(s)")
    print(f"  · dataset_run_log     : {'present' if DATASET_RUN_LOG.exists() else 'MISSING'}")
    if raw_count == 0 and log_count == 0:
        print()
        print(f"  ⚠ NO local apify_raw or run logs found in outputs/. Local-")
        print(f"    archive sections WILL come back empty for every post; this")
        print(f"    is a Replit-state issue, not a per-post diagnosis. Sheet-")
        print(f"    backed sections (Processed_Log, All_Events) are still valid.")

    for pid in pids:
        report_post(pid, sheet, accounts_set, run_log)

    print()
    divider("═")
    print("  Done. Paste the report above to your debugging partner.")
    divider("═")
    print()


if __name__ == "__main__":
    main()
