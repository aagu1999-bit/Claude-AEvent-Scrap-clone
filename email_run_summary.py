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
from datetime import datetime
from email.message import EmailMessage
from email.utils import formatdate
from pathlib import Path


OUTPUTS_DIR = Path("outputs")

_send_lock = threading.Lock()
_already_sent = [False]   # boxed bool — closure-mutable


# ─────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────

def already_sent() -> bool:
    return _already_sent[0]


def reset_sent_flag():
    """For tests or long-lived processes that need to re-arm the sender."""
    with _send_lock:
        _already_sent[0] = False


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
