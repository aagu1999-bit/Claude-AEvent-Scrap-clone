"""
email_run_summary.py — Email the per-run CSV + Excel outputs after every
pipeline finish (whether it completed normally, was stopped via the UI's
Stop button, or aborted on an outage-watchdog trip).

Recipients
----------
Resolved from env vars in this order:
  1. RUN_SUMMARY_EMAIL  (comma-separated list)
  2. ADMIN_EMAIL        (comma-separated list; falls back to the outage-
                         watchdog recipient if RUN_SUMMARY_EMAIL is unset)

If neither is set, the email is silently skipped (with a log line).

SMTP
----
Uses the same SMTP_HOST / SMTP_PORT / SMTP_USER / SMTP_PASS env vars as
outage_watchdog.py. For Gmail, SMTP_PASS must be a 16-char App Password
(regular Gmail passwords are rejected by SMTP since 2-factor enforcement).

Idempotency
-----------
send_run_summary() will only actually send once per process. Subsequent
calls return True without re-sending. This lets us wire the call in two
places safely:
  · end of save_data() (normal "completed" path)
  · atexit handler     (stopped / aborted / crashed path)
Whichever fires first wins; the other is a no-op.

Failure mode
------------
Network / auth / disk errors are caught and logged. They never crash the
pipeline — the run's actual outputs are still in outputs/Events_*.csv on
disk regardless of whether the email got delivered.
"""

import os
import smtplib
import threading
import time
from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path


OUTPUTS_DIR = Path("outputs")
SCHEDULER_WARNING_MARKER_DIR = OUTPUTS_DIR / "scheduler_warnings"

_send_lock = threading.Lock()
_already_sent = [False]      # boxed bool — closure-mutable; gates the per-run summary
_captured_exception = [""]   # last uncaught traceback, captured by sys.excepthook


# ─────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────

def already_sent() -> bool:
    return _already_sent[0]


def reset_sent_flag():
    """For tests or long-lived processes that need to re-arm the sender."""
    with _send_lock:
        _already_sent[0] = False


def capture_exception(traceback_str: str):
    """Stash a formatted traceback for the atexit run-summary email body.

    Called from sys.excepthook (installed in main.py's __main__ block) so
    that when an uncaught exception kills the run, the resulting
    "Run stopped" email includes the actual cause rather than a vague
    "the process exited."
    """
    with _send_lock:
        _captured_exception[0] = str(traceback_str) or ""


def get_captured_exception() -> str:
    return _captured_exception[0]


def send_run_summary(status: str = "completed",
                     stats_summary: str = "",
                     run_id: str = "",
                     since_ts: str = "") -> bool:
    """Send the latest Events_*.csv + Events_*.xlsx as an email.

    status        — short tag for the subject line: 'completed', 'stopped',
                    'aborted', 'crashed', etc.
    stats_summary — multi-line text appended to the body (events found,
                    OCR success rate, retry counts, etc.).
    run_id        — timestamp/identifier shown in subject + body.
    since_ts      — if set, only attach Events_*.{csv,xlsx} files newer
                    than this filename-style timestamp (default: just take
                    the single newest pair).

    Returns True if the email was sent OR if it was already sent in this
    process. False on any failure.

    NEVER raises — pipeline shouldn't crash because email failed.
    """
    with _send_lock:
        if _already_sent[0]:
            return True

    recipients = _resolve_recipients()
    if not recipients:
        print("  ✉ run-summary email skipped — RUN_SUMMARY_EMAIL / ADMIN_EMAIL not set")
        return False

    creds = _smtp_creds()
    if not creds:
        print(f"  ✉ run-summary email skipped — SMTP_HOST/PORT/USER/PASS not configured "
              f"({len(recipients)} recipient(s) would have received it)")
        return False

    csv_path, xlsx_path = _select_attachments(since_ts)
    if not (csv_path or xlsx_path):
        print("  ✉ run-summary email skipped — no Events_*.csv or .xlsx found in outputs/")
        return False

    subject_tag = (run_id
                   or (csv_path.stem.replace("Events_", "") if csv_path else "")
                   or datetime.now().strftime("%Y%m%d_%H%M%S"))
    subject = f"[Apify Pipeline] Run {status} · {subject_tag}"

    body_lines = [
        f"Apify Instagram pipeline run {status}.",
        "",
        f"Run ID   : {run_id or '(not provided)'}",
        f"When     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z').strip()}",
        f"CSV      : {csv_path.name if csv_path else '(none)'}",
        f"Excel    : {xlsx_path.name if xlsx_path else '(none)'}",
    ]
    if stats_summary:
        body_lines += ["", "Run stats:", stats_summary]
    tb = get_captured_exception()
    if tb:
        body_lines += [
            "",
            "──────── Uncaught exception (this is what stopped the run) ────────",
            tb.strip(),
            "──────────────────────────────────────────────────────────────────",
        ]
    body_lines += [
        "",
        "The latest CSV + Excel exports are attached. They contain every event",
        "extracted during this run; the full audit trail lives in the All_Events",
        "tab of the Google Sheet.",
    ]
    body = "\n".join(body_lines)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = creds[2]
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg.set_content(body)
    _attach_file(msg, csv_path)
    _attach_file(msg, xlsx_path)

    try:
        host, port, user, pwd = creds
        with smtplib.SMTP(host, port, timeout=30) as s:
            s.starttls()
            s.login(user, pwd)
            s.sendmail(user, recipients, msg.as_string())
    except Exception as e:
        print(f"  ⚠ run-summary email failed: {e}")
        return False

    with _send_lock:
        _already_sent[0] = True
    print(f"  ✉ run-summary email sent to {len(recipients)} recipient(s): {', '.join(recipients)}")
    return True


# ─────────────────────────────────────────────────────────────────
# Internals
# ─────────────────────────────────────────────────────────────────

def _resolve_recipients() -> list:
    raw = (os.environ.get("RUN_SUMMARY_EMAIL", "").strip()
           or os.environ.get("ADMIN_EMAIL", "").strip())
    if not raw:
        return []
    return [e.strip() for e in raw.split(",") if e.strip()]


def _smtp_creds():
    host = os.environ.get("SMTP_HOST", "").strip()
    port_s = os.environ.get("SMTP_PORT", "").strip()
    user = os.environ.get("SMTP_USER", "").strip()
    pwd = os.environ.get("SMTP_PASS", "").strip()
    if not (host and port_s and user and pwd):
        return None
    try:
        port = int(port_s)
    except ValueError:
        return None
    return (host, port, user, pwd)


def _select_attachments(since_ts: str = ""):
    """Pick the newest matching Events_*.csv + .xlsx pair.

    If since_ts is provided ('YYYYMMDD_HHMMSS' style), only files whose
    name-timestamp is >= since_ts are considered. Otherwise just take the
    single newest pair (which is what we want at end of run anyway)."""
    if not OUTPUTS_DIR.exists():
        return None, None

    def _ts_of(p: Path) -> str:
        # Events_20260604_191833.csv → '20260604_191833'
        return p.stem.replace("Events_", "")

    csvs = sorted(OUTPUTS_DIR.glob("Events_*.csv"),
                  key=_ts_of, reverse=True)
    xlsxs = sorted(OUTPUTS_DIR.glob("Events_*.xlsx"),
                   key=_ts_of, reverse=True)

    if since_ts:
        csvs = [p for p in csvs if _ts_of(p) >= since_ts]
        xlsxs = [p for p in xlsxs if _ts_of(p) >= since_ts]

    return (csvs[0] if csvs else None), (xlsxs[0] if xlsxs else None)


def _send_simple(subject: str, body: str) -> bool:
    """Shared SMTP send for the no-attachment notifications below
    (apify scrape complete, scheduled-run warning). Returns True on
    success, False on any failure (never raises)."""
    recipients = _resolve_recipients()
    if not recipients:
        print(f"  ✉ email skipped — RUN_SUMMARY_EMAIL / ADMIN_EMAIL not set [{subject!r}]")
        return False
    creds = _smtp_creds()
    if not creds:
        print(f"  ✉ email skipped — SMTP creds not configured [{subject!r}]")
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = creds[2]
    msg["To"] = ", ".join(recipients)
    msg["Date"] = formatdate(localtime=True)
    msg.set_content(body)
    try:
        host, port, user, pwd = creds
        with smtplib.SMTP(host, port, timeout=30) as s:
            s.starttls()
            s.login(user, pwd)
            s.sendmail(user, recipients, msg.as_string())
        print(f"  ✉ sent: {subject}")
        return True
    except Exception as e:
        print(f"  ⚠ email failed [{subject!r}]: {e}")
        return False


def send_apify_scrape_notification(run_id: str,
                                   dataset_id: str,
                                   status: str,
                                   post_count: int = 0,
                                   accounts_requested: int = 0,
                                   triggered_at: str = "") -> bool:
    """Sent when an Apify-API-triggered scrape finishes.

    Fires once for each terminal status — SUCCEEDED, FAILED, TIMED-OUT,
    ABORTED. The UI polls Apify every few seconds; the caller is
    responsible for tracking 'have we already sent for this run_id'
    (e.g., via the job-state JSON file) so we don't spam on every poll.

    Not idempotent at the module level (different from send_run_summary)
    because there can be multiple Apify scrapes per Streamlit session.
    """
    success = status == "SUCCEEDED"
    dataset_url = f"https://console.apify.com/storage/datasets/{dataset_id}" if dataset_id else ""
    subject = (
        f"[Apify Pipeline] Scrape {'complete' if success else status.lower()} · "
        f"{post_count:,} posts" if success else
        f"[Apify Pipeline] Scrape {status.lower()} · run {run_id}"
    )
    body_lines = [
        f"Apify scrape run {run_id} finished with status: {status}",
        "",
        f"Triggered at      : {triggered_at or '(unknown)'}",
        f"Accounts requested: {accounts_requested}" if accounts_requested else "",
        f"Posts returned    : {post_count:,}" if success else f"Posts returned    : (run did not complete)",
        f"Dataset           : {dataset_url}" if dataset_url else "",
        "",
    ]
    if success:
        body_lines += [
            "The scrape is in your Apify console. To process it into All_Events,",
            "open the UI dashboard → Run screen → Path A → paste the dataset URL",
            "above → click Run Pipeline.",
        ]
    else:
        body_lines += [
            "The scrape did NOT complete normally. Check the Apify console for",
            "details — common causes are quota exhaustion, invalid usernames,",
            "or actor timeout. The pipeline run was NOT triggered.",
        ]
    return _send_simple(subject, "\n".join(line for line in body_lines if line is not None))


def send_scheduled_run_warning(target_dt_iso: str,
                               minutes_until: int = 60) -> bool:
    """Sent ~1 hour before a scheduled run is set to fire.

    Marker-file-based dedup: writes outputs/scheduler_warnings/warned_<ts>.flag
    so we don't re-send during the entire matching minute (the scheduler
    loop ticks every 10 seconds). Returns True if sent now, False if
    already warned for this target.

    target_dt_iso: ISO-format string of the upcoming run time, e.g.
                   '2026-06-11T14:00:00-04:00'. Used as the marker key.
    """
    SCHEDULER_WARNING_MARKER_DIR.mkdir(parents=True, exist_ok=True)
    safe = "".join(c if c.isalnum() else "_" for c in target_dt_iso)
    marker = SCHEDULER_WARNING_MARKER_DIR / f"warned_{safe}.flag"
    if marker.exists():
        return False

    subject = f"[Apify Pipeline] Scheduled run in ~{minutes_until} min · {target_dt_iso}"
    body = "\n".join([
        f"Heads up — a scheduled pipeline run is set to fire in ~{minutes_until} minutes.",
        "",
        f"Target time : {target_dt_iso}",
        "",
        "If you don't want this run to fire (e.g., today's accounts list",
        "is incomplete, or the Apify quota is low, or you'd rather trigger",
        "manually via the UI), either:",
        "  · stop the Replit workflow before the target time, or",
        "  · change schedule_day / schedule_time in config.json or the UI",
        "    Settings → Schedule section.",
        "",
        "Otherwise no action needed — the run will start automatically.",
    ])

    sent = _send_simple(subject, body)
    if sent:
        try:
            marker.touch()
        except Exception as e:
            # Marker write failure means we'll re-send on the next loop
            # tick — annoying but not catastrophic.
            print(f"  ⚠ could not write warning marker {marker}: {e}")

    # Opportunistic cleanup — drop markers older than 30 days so the dir
    # doesn't grow unbounded.
    try:
        cutoff = time.time() - 30 * 86400
        for old in SCHEDULER_WARNING_MARKER_DIR.glob("warned_*.flag"):
            if old.stat().st_mtime < cutoff:
                old.unlink(missing_ok=True)
    except Exception:
        pass

    return sent


def _attach_file(msg: EmailMessage, path: Path):
    if not path or not path.exists():
        return
    try:
        data = path.read_bytes()
    except Exception as e:
        print(f"  ⚠ could not read {path} for email attachment: {e}")
        return
    suffix = path.suffix.lower()
    if suffix == ".csv":
        msg.add_attachment(
            data, maintype="text", subtype="csv", filename=path.name)
    elif suffix in (".xlsx", ".xls"):
        msg.add_attachment(
            data,
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=path.name,
        )
    else:
        msg.add_attachment(
            data, maintype="application", subtype="octet-stream",
            filename=path.name)
