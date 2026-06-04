"""
outage_watchdog.py — kill the run when a platform outage is detected.

WHY
---
On 2026-05-21 a compound failure (Vision API billing disabled + Gemini quota
exhausted) ran for the full pipeline duration. ~940 posts were processed
into ocr_failed/gemini_error rows. Nobody noticed until much later, when the
retry queue had to resurrect everything.

The fix is upstream: when we see the same fatal error code repeatedly in a
short window, the right move is to STOP, leave a marker, and tell a human —
not keep burning through the queue producing rows that all need recovery.

USAGE
-----
At the start of process_post (the worker entrypoint):
    from outage_watchdog import is_aborted, get_abort_info
    if is_aborted():
        info = get_abort_info()
        wprint(f"  ⛔ outage abort active ({info['category']}) — skipping")
        return

At the Vision API exception site (extract_ocr_text):
    if '403' in error_str and 'billing' in error_str.lower():
        outage_watchdog.record('vision_403_billing', error_str)
    elif '403' in error_str:
        outage_watchdog.record('vision_403', error_str)

At the Gemini exception site (_call_gemini_tier):
    if '429' in error_str or 'quota' in error_str.lower():
        outage_watchdog.record('gemini_429', error_str)

THRESHOLDS
----------
- vision_403_billing : 3 in 120s  (billing flip needs IMMEDIATE attention)
- vision_403         : 5 in  60s
- gemini_429         : 10 in 60s  (sometimes quota fluctuates briefly)

Override via env vars: WATCHDOG_<CATEGORY>_LIMIT, WATCHDOG_<CATEGORY>_WINDOW
e.g. WATCHDOG_VISION_403_LIMIT=10 WATCHDOG_VISION_403_WINDOW=120

EMAIL
-----
On abort, attempts to email ADMIN_EMAIL via SMTP if SMTP_HOST/SMTP_PORT/
SMTP_USER/SMTP_PASS are all set. Silent best-effort — failure to send does
not block the abort or crash the pipeline. If ADMIN_EMAIL is set but SMTP
isn't, we log "email skipped — SMTP not configured" so you know.

ADMIN_EMAIL accepts a single address OR a comma-separated list of recipients:
    ADMIN_EMAIL=aagu1999@gmail.com,centralgroupevents@gmail.com

A marker file is ALWAYS written to outputs/OUTAGE_ABORT_<ts>.json regardless
of email success — that's the authoritative record.
"""

import builtins
import json
import os
import smtplib
import threading
import time
from collections import deque
from email.mime.text import MIMEText
from pathlib import Path


_DEFAULT_THRESHOLDS = {
    'vision_403_billing': {'limit': 3,  'window': 120},
    'vision_403':         {'limit': 5,  'window': 60},
    'gemini_429':         {'limit': 10, 'window': 60},
}


def _threshold_for(category):
    """Resolve threshold for a category, with env-var overrides."""
    base = _DEFAULT_THRESHOLDS.get(category, {'limit': 5, 'window': 60})
    env_prefix = f"WATCHDOG_{category.upper()}"
    try:
        limit = int(os.environ.get(f"{env_prefix}_LIMIT", base['limit']))
        window = int(os.environ.get(f"{env_prefix}_WINDOW", base['window']))
    except ValueError:
        limit, window = base['limit'], base['window']
    return limit, window


_lock = threading.Lock()
_events = {}              # category -> deque[(ts, error_str)]
_aborted = False
_abort_info = None


def _print(msg):
    """Atomic write, bypassing wprint to avoid the worker tag on global banners."""
    with _lock:
        builtins.print(msg, flush=True)


def record(category, error_str):
    """Record one occurrence of the given failure category. Returns True if
    this call tripped the abort, False otherwise. Subsequent calls after
    abort are no-ops (still return True so callers can short-circuit)."""
    global _aborted, _abort_info

    if _aborted:
        return True

    now = time.time()
    limit, window = _threshold_for(category)

    with _lock:
        if _aborted:
            return True
        dq = _events.setdefault(category, deque())
        dq.append((now, str(error_str)[:300]))
        cutoff = now - window
        while dq and dq[0][0] < cutoff:
            dq.popleft()

        if len(dq) < limit:
            return False

        _aborted = True
        _abort_info = {
            'category':     category,
            'count':        len(dq),
            'window_seconds': window,
            'limit':        limit,
            'first_ts':     dq[0][0],
            'last_ts':      dq[-1][0],
            'sample_errors': [err for _, err in list(dq)[-3:]],
            'aborted_at':   now,
        }

    _write_marker(_abort_info)
    _send_email(_abort_info)
    _print_banner(_abort_info)
    return True


def is_aborted():
    return _aborted


def get_abort_info():
    return dict(_abort_info) if _abort_info else None


def reset():
    """Wipe state — for tests / between-run reuse in long-lived processes."""
    global _aborted, _abort_info
    with _lock:
        _events.clear()
        _aborted = False
        _abort_info = None


def _write_marker(info):
    try:
        ts = time.strftime('%Y%m%d_%H%M%S', time.localtime(info['aborted_at']))
        path = Path('outputs') / f'OUTAGE_ABORT_{ts}.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            json.dump(info, f, indent=2, default=str)
        _print(f"  📝 outage marker written: {path}")
    except Exception as e:
        _print(f"  ⚠ failed to write outage marker: {e}")


def _send_email(info):
    # ADMIN_EMAIL accepts a single address OR a comma-separated list:
    #   ADMIN_EMAIL=aagu1999@gmail.com,centralgroupevents@gmail.com
    raw = os.environ.get('ADMIN_EMAIL', '').strip()
    if not raw:
        return
    recipients = [e.strip() for e in raw.split(',') if e.strip()]
    if not recipients:
        return
    host = os.environ.get('SMTP_HOST', '').strip()
    port = os.environ.get('SMTP_PORT', '').strip()
    user = os.environ.get('SMTP_USER', '').strip()
    pwd  = os.environ.get('SMTP_PASS', '').strip()
    if not (host and port and user and pwd):
        _print(f"  ✉ outage email skipped — ADMIN_EMAIL set ({len(recipients)} recipient(s)) "
               f"but SMTP_HOST/PORT/USER/PASS not configured")
        return
    try:
        subject = f"[Apify Pipeline] OUTAGE ABORT: {info['category']}"
        body = (
            f"The pipeline aborted itself due to repeated failures.\n\n"
            f"Category   : {info['category']}\n"
            f"Count      : {info['count']} occurrences in {info['window_seconds']}s "
            f"(limit: {info['limit']})\n"
            f"Window     : {time.ctime(info['first_ts'])}  →  {time.ctime(info['last_ts'])}\n\n"
            f"Sample errors:\n"
            + "\n".join(f"  • {e}" for e in info['sample_errors'])
            + "\n\nMarker file written to outputs/OUTAGE_ABORT_*.json.\n"
            "The pipeline will skip remaining posts and exit cleanly.\n"
        )
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = user
        msg['To'] = ', '.join(recipients)

        with smtplib.SMTP(host, int(port), timeout=15) as s:
            s.starttls()
            s.login(user, pwd)
            s.sendmail(user, recipients, msg.as_string())
        _print(f"  ✉ outage email sent to {len(recipients)} recipient(s): {', '.join(recipients)}")
    except Exception as e:
        _print(f"  ⚠ outage email failed: {e}")


def _print_banner(info):
    bar = "═" * 72
    _print(f"\n{bar}")
    _print(f"  ⛔ OUTAGE WATCHDOG TRIPPED — aborting run")
    _print(f"     category : {info['category']}")
    _print(f"     count    : {info['count']} in {info['window_seconds']}s (limit {info['limit']})")
    _print(f"     samples  :")
    for e in info['sample_errors']:
        _print(f"       • {e[:200]}")
    _print(f"  All remaining posts will be skipped. Marker file in outputs/.")
    _print(f"{bar}\n")
