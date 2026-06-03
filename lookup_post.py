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
# Identifier resolution
# ─────────────────────────────────────────────────────────────────
# The pipeline stores `post.get('id') or post.get('shortCode')` as the
# canonical `pid` in BOTH Processed_Log column 1 and All_Events POST ID.
# When Apify provides the numeric `id` field (which it does for live
# scrapes), that numeric id is what's stored — NOT the URL shortcode.
# The shortcode only survives in the URL columns (INSTAGRAM POST URL,
# POST URL).
#
# So if a user passes the shortcode `DWzhk0Wjf6y` they will:
#   • match in All_Events via the INSTAGRAM POST URL column
#   • NOT match in Processed_Log (which only has the numeric id)
#   • NOT match in apify_raw dumps when our search bug used
#     `p.get('id') or p.get('shortCode') == pid` (short-circuit picks
#     id-or-shortCode whichever is truthy first and compares only that)
#
# `gather_identifiers` resolves a user-typed shortcode into the FULL
# identifier set (shortcode + numeric id + URL forms) by cross-referencing
# All_Events and any local Apify dumps. After this resolution, every
# downstream search can query by any-of-the-set and stops missing data.
# ─────────────────────────────────────────────────────────────────


def _add_case_variants(s, into):
    if not s:
        return
    into.add(s)
    into.add(s.strip())
    into.add(s.strip().upper())
    into.add(s.strip().lower())


def _post_id_col_idx(header):
    """Return the index of the POST ID column in a sheet header, or None.
    Tolerant of formatting variations (case, underscore vs space)."""
    for i, h in enumerate(header):
        nm = h.strip().upper().replace('_', ' ')
        if nm in ('POST ID', 'POSTID'):
            return i
    return None


def gather_identifiers(user_input, sheet):
    """Build the full identifier set for a post given user input.

    Resolution passes (each pass can add to the set; later passes use
    earlier-pass results to find more):
      1. The user input + case variants + URL form (if input looks like a
         shortcode, i.e. not a pure numeric id).
      2. All_Events lookup: for every identifier in the set, run findall
         on All_Events. From each matched row, extract POST ID (numeric
         id) and INSTAGRAM POST URL (shortcode-bearing URL). Add those.
      3. Local Apify dumps: for any dump-post whose `id` OR `shortCode`
         is in the current set, add the OTHER field too. This cross-
         pollinates id <-> shortCode when the dump knows both.

    Returns: (id_set, resolution_log)
      id_set: set of all known identifiers for this post
      resolution_log: list of human-readable steps for the report
    """
    ids = set()
    log = []
    inp = (user_input or '').strip()
    if not inp:
        return ids, log

    # Pass 1: input + variants
    _add_case_variants(inp, ids)
    is_shortcode_like = not inp.isdigit() and 'instagram.com' not in inp.lower()
    if is_shortcode_like:
        _add_case_variants(f"https://www.instagram.com/p/{inp}/", ids)
    elif 'instagram.com/p/' in inp.lower():
        # User passed a full URL — extract the shortcode part
        sc = inp.rstrip('/').rsplit('/p/', 1)[-1].split('/')[0].split('?')[0]
        _add_case_variants(sc, ids)
    log.append(f"pass1 input+variants → {len(ids)} ids")

    before = len(ids)

    # Pass 2: All_Events resolution (only if sheet is available)
    if sheet is not None:
        try:
            ws = sheet.worksheet("All_Events")
            header = ws.row_values(1)
            pid_col = _post_id_col_idx(header)
            url_cols = [i for i, h in enumerate(header)
                        if h.strip().upper() in
                        ('INSTAGRAM POST URL', 'POST URL')]
            queries_to_try = sorted(ids)
            matched_row_nums = set()
            for q in queries_to_try:
                try:
                    cells = ws.findall(q)
                except Exception:
                    continue
                for cell in cells:
                    if cell.row == 1 or cell.row in matched_row_nums:
                        continue
                    matched_row_nums.add(cell.row)
            for rn in sorted(matched_row_nums):
                try:
                    row = ws.row_values(rn)
                except Exception:
                    continue
                if pid_col is not None and pid_col < len(row) and row[pid_col]:
                    _add_case_variants(row[pid_col].strip(), ids)
                for ci in url_cols:
                    if ci < len(row) and row[ci]:
                        _add_case_variants(row[ci].strip(), ids)
                        # Also extract bare shortcode from URL
                        url_val = row[ci].strip()
                        if 'instagram.com/p/' in url_val.lower():
                            sc = url_val.rstrip('/').rsplit('/p/', 1)[-1].split('/')[0].split('?')[0]
                            _add_case_variants(sc, ids)
            log.append(f"pass2 All_Events matched {len(matched_row_nums)} row(s) → {len(ids)} ids")
        except Exception as e:
            log.append(f"pass2 All_Events failed: {e}")
    else:
        log.append("pass2 All_Events skipped (no sheet)")

    after_pass2 = len(ids)

    # Pass 3: Apify dump cross-pollination (id <-> shortCode)
    raw_files = sorted(glob.glob(APIFY_RAW_GLOB))
    cache_files = sorted(glob.glob(APIFY_CACHE_GLOB))
    dump_count = 0
    for path in raw_files + cache_files:
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            continue
        if isinstance(data, dict):
            posts = data.get('posts') or []
        elif isinstance(data, list):
            posts = data
        else:
            continue
        for p in posts:
            p_id = str(p.get('id') or '').strip()
            p_sc = str(p.get('shortCode') or p.get('shortcode') or '').strip()
            if (p_id and p_id in ids) or (p_sc and p_sc in ids):
                if p_id:
                    _add_case_variants(p_id, ids)
                if p_sc:
                    _add_case_variants(p_sc, ids)
                dump_count += 1
    log.append(f"pass3 Apify dumps cross-pollinated {dump_count} match(es) → {len(ids)} ids")

    # Drop empties
    ids.discard('')
    ids.discard(None)
    return ids, log


# ─────────────────────────────────────────────────────────────────
# 1 & 2. Local Apify dump search
# ─────────────────────────────────────────────────────────────────
def search_apify_dumps(id_set):
    """Search every apify_raw_*.json and apify_cache/*.json for any post whose
    `id` OR `shortCode` is in id_set. (Critical: check both fields
    independently rather than `id or shortCode == pid` — the latter
    short-circuits and only compares one of the two, missing matches when
    Apify provides both. This was the original bug that lied about scrape
    history.)"""
    raw_files = sorted(glob.glob(APIFY_RAW_GLOB))
    cache_files = sorted(glob.glob(APIFY_CACHE_GLOB))

    hits = []   # list of (path, post_dict, fetched_at, dataset_id)

    def _match(p):
        p_id = str(p.get('id') or '').strip()
        p_sc = str(p.get('shortCode') or p.get('shortcode') or '').strip()
        return (p_id and p_id in id_set) or (p_sc and p_sc in id_set)

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
            if _match(p):
                hits.append((path, p, fetched_at, dataset_id))

    for path in cache_files:
        try:
            with open(path) as f:
                posts = json.load(f)
        except Exception:
            continue
        if not isinstance(posts, list):
            continue
        ds_id = Path(path).stem
        for p in posts:
            if _match(p):
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


def _find_matching_rows(ws, id_set):
    """Row-finder driven by an identifier set. Runs ws.findall() for every
    identifier and aggregates matched rows, deduping by row number.

    Returns dict with header, populated row_count, list of matched_rows,
    list of queries tried, and matched_via (which queries produced hits)."""
    header = ws.row_values(1)
    try:
        populated_row_count = len(ws.col_values(1))
    except Exception:
        populated_row_count = ws.row_count

    cells_by_query = {}
    matched_via = []
    tried = []
    for q in sorted(id_set):
        if not q:
            continue
        tried.append(q)
        try:
            hits = ws.findall(q)
        except Exception as e:
            tried[-1] = f"{q} (findall error: {e})"
            continue
        if hits:
            cells_by_query[q] = hits
            matched_via.append(q)

    matched_rows = []
    seen_rownums = set()
    for q in matched_via:
        for cell in cells_by_query[q]:
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


def search_processed_log(sheet, id_set):
    if sheet is None:
        return None
    try:
        ws = sheet.worksheet("Processed_Log")
    except Exception as e:
        print(f"  ⚠ Could not open Processed_Log worksheet: {e}")
        return None
    try:
        return _find_matching_rows(ws, id_set)
    except Exception as e:
        print(f"  ⚠ Processed_Log search failed: {e}")
        return None


def search_all_events(sheet, id_set):
    if sheet is None:
        return None
    try:
        ws = sheet.worksheet("All_Events")
    except Exception as e:
        print(f"  ⚠ Could not open All_Events worksheet: {e}")
        return None
    try:
        return _find_matching_rows(ws, id_set)
    except Exception as e:
        print(f"  ⚠ All_Events search failed: {e}")
        return None


# ─────────────────────────────────────────────────────────────────
# 6. Anomaly summaries
# ─────────────────────────────────────────────────────────────────
def search_anomalies(id_set):
    """Return list of (run_id, path, anomaly_entry) for any identifier hit."""
    matches = []
    for path in sorted(glob.glob(ANOMALIES_GLOB)):
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            continue
        run_id = data.get('run_id', Path(path).stem.replace('anomalies_', ''))
        # Newer: {'run_id':..., 'anomalies': {pid: {...}}}
        # Older: a dict of pid → entry directly
        anomalies = data.get('anomalies') or data
        if not isinstance(anomalies, dict):
            continue
        for ident in id_set:
            if ident and ident in anomalies:
                matches.append((run_id, path, anomalies[ident]))
                break  # one entry per file
    return matches


# ─────────────────────────────────────────────────────────────────
# 7. Run log grep
# ─────────────────────────────────────────────────────────────────
def search_run_logs(id_set, max_lines_per_file=120):
    """Return list of (logfile_path, [matching_line, ...]).
    Matches a line if ANY identifier in id_set appears in it. Worker tags
    use `[pid]` format, so the bracket-wrapped form is the strongest signal,
    but bare identifier hits are still informative (e.g. setup logs)."""
    bracket_patterns = {f"[{ident}]" for ident in id_set if ident}
    bare_patterns = {ident for ident in id_set if ident and len(ident) > 4}
    matches = []
    for path in sorted(glob.glob(RUN_LOG_GLOB)):
        hits = []
        try:
            with open(path, errors='replace') as f:
                for line in f:
                    matched = False
                    for bp in bracket_patterns:
                        if bp in line:
                            matched = True
                            break
                    if not matched:
                        for bp in bare_patterns:
                            if bp in line:
                                matched = True
                                break
                    if matched:
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
def search_events_csvs(id_set):
    """Return list of (csv_path, list_of_row_dicts) for any identifier hit
    in the POST ID column (across schema variants)."""
    matches = []
    id_set_upper = {i.upper() for i in id_set if i}
    for path in sorted(glob.glob(EVENTS_CSV_GLOB)):
        rows = []
        try:
            with open(path, newline='', errors='replace') as f:
                reader = csv.DictReader(f)
                for r in reader:
                    pid_in_row = (
                        r.get('POST ID') or r.get('post_id') or r.get('POST_ID')
                        or r.get('Post ID') or ''
                    ).strip()
                    if pid_in_row.upper() in id_set_upper:
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

    # ─── 0. Identifier resolution ────────────────────────────────
    # CRITICAL step: the pipeline stores `post.get('id') or post.get('shortCode')`
    # as the canonical pid. When Apify provides both, the numeric `id` wins,
    # so Processed_Log / All_Events POST ID hold the numeric id and the
    # shortcode lives only in URL columns. Resolve user input to all known
    # forms before doing any search.
    section("IDENTIFIER RESOLUTION")
    id_set, res_log = gather_identifiers(pid, sheet)
    for line in res_log:
        print(f"  · {line}")
    if id_set:
        print(f"  → Final identifier set ({len(id_set)} entries):")
        for ident in sorted(id_set):
            short = ident if len(ident) <= 80 else ident[:77] + "..."
            print(f"      • {short}")
    else:
        print("  ⚠ Could not resolve any identifiers for this input.")

    # ─── 1. Apify scrape history ─────────────────────────────────
    section("APIFY SCRAPE HISTORY")
    scrape = search_apify_dumps(id_set)
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
    pl_res = search_processed_log(sheet, id_set)
    if pl_res is None:
        print("  ? Could not read Processed_Log (no sheet access).")
    else:
        print(f"  · tab has ~{pl_res['row_count']} populated rows")
        print(f"  · header ({len(pl_res['header'])} cols): {pl_res['header']}")
        print(f"  · queries tried ({len(pl_res['tried'])}): "
              f"{pl_res['tried'][:5]}{' ...' if len(pl_res['tried']) > 5 else ''}")
        if not pl_res['matched_rows']:
            print("  ✗ No rows matched any identifier.")
            print("    → If this post genuinely processed but isn't here, it was")
            print("      either added to All_Events via a recovery tool that")
            print("      bypasses Processed_Log, OR the worker crashed before")
            print("      writing Processed_Log (orphan).")
        else:
            print(f"  ✓ matched via {pl_res['matched_via']} → "
                  f"{len(pl_res['matched_rows'])} row(s):")
            for i, row in enumerate(reversed(pl_res['matched_rows']), 1):
                print(f"  [{i}] " + " | ".join(
                    f"{k}={v!r}" for k, v in row.items() if v
                ))

    # ─── 4. All_Events ───────────────────────────────────────────
    section("ALL_EVENTS ENTRIES")
    ae_res = search_all_events(sheet, id_set)
    if ae_res is None:
        print("  ? Could not read All_Events (no sheet access).")
    else:
        print(f"  · tab has ~{ae_res['row_count']} populated rows")
        print(f"  · header ({len(ae_res['header'])} cols): {ae_res['header']}")
        print(f"  · queries tried ({len(ae_res['tried'])}): "
              f"{ae_res['tried'][:5]}{' ...' if len(ae_res['tried']) > 5 else ''}")
        if not ae_res['matched_rows']:
            print("  ✗ No rows matched any identifier.")
        else:
            print(f"  ✓ matched via {ae_res['matched_via']} → "
                  f"{len(ae_res['matched_rows'])} event row(s):")
            interesting = ['POST ID', 'EVENT NAME', 'DATE', 'START TIME',
                           'VENUE NAME', 'CITY', 'CONFIDENCE', 'QUALITY FLAGS',
                           'QUALITY_FLAGS', 'INSTAGRAM HANDLE', 'INSTAGRAM POST URL',
                           'WORKER ID', 'RUN ID', 'ATTEMPT ID',
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
    anom = search_anomalies(id_set)
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
    logs = search_run_logs(id_set)
    if not logs:
        print("  · No matches in any run_*.log (either never logged, or logs cleaned up).")
    else:
        for path, lines in logs:
            print(f"  {Path(path).name}:")
            for ln in lines:
                print(f"    {ln}")

    # ─── 7. Events CSVs ──────────────────────────────────────────
    section("EVENTS_*.CSV HISTORY")
    csvs = search_events_csvs(id_set)
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
