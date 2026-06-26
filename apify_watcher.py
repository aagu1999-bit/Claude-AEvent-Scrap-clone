#!/usr/bin/env python3
"""
apify_watcher.py — Background daemon that polls an Apify actor run to
terminal status, sends the scrape-complete/failed email, and (if the
job's auto_chain flag is set) auto-triggers the pipeline.

The Streamlit UI already polls Apify while the user is on the scrape
panel, but that polling stops the moment the user closes the browser
tab. This watcher lives in its own subprocess so the email and the
auto-chain don't depend on the UI staying open.

Spawned from ui.py when the user clicks "Scrape only" or "Scrape & Run"
in Path B/C. Reads the same outputs/UI_JOB_apify_scrape.json file the
UI uses, so both processes share state.

Idempotency
-----------
Both the watcher and the UI may detect terminal status. Each guards on
the apify_notified / auto_chain_triggered flags in the JSON state file
before sending email or spawning the pipeline, so whichever fires first
wins and the other no-ops.

Usage
-----
    python apify_watcher.py outputs/UI_JOB_apify_scrape.json [--poll-secs N]

Exit codes
----------
    0 = ran to a terminal Apify status (success or fail) and notified
    1 = unrecoverable error (bad job state, missing token, etc.)
"""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import requests


APIFY_API_BASE = "https://api.apify.com/v2"
DEFAULT_POLL_SECS = 30   # Apify runs take 10-40 min — checking every 30s is plenty
MAX_RUNTIME_SECS = 60 * 90  # 90 min hard ceiling so a stuck poll loop doesn't run forever


# ── State file plumbing (mirrors ui.py's _read_job / _write_job) ──────────

def read_job(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def write_job(path: Path, info: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(info, indent=2, default=str))
    tmp.replace(path)


# ── Apify polling ─────────────────────────────────────────────────────────

def poll_status(run_id: str, token: str) -> str:
    """Returns READY / RUNNING / SUCCEEDED / FAILED / TIMED-OUT / ABORTED.
    Returns 'POLL_ERROR' on any network failure (caller decides what to
    do — we want to keep trying)."""
    try:
        r = requests.get(
            f"{APIFY_API_BASE}/actor-runs/{run_id}",
            params={"token": token},
            timeout=20,
        )
        r.raise_for_status()
        return r.json()["data"]["status"]
    except Exception as e:
        print(f"[watcher] poll error: {e}", file=sys.stderr)
        return "POLL_ERROR"


def fetch_dataset_count(dataset_id: str, token: str) -> int:
    """How many items the scrape produced. 0 on any error — count is
    informational, not load-bearing."""
    if not (dataset_id and token):
        return 0
    try:
        r = requests.get(
            f"{APIFY_API_BASE}/datasets/{dataset_id}",
            params={"token": token},
            timeout=10,
        )
        if r.ok:
            return int((r.json().get("data") or {}).get("itemCount") or 0)
    except Exception:
        pass
    return 0


# ── Email + chain coordination ───────────────────────────────────────────

def maybe_send_email(info: dict, status: str, post_count: int):
    """Send the Apify-complete/failed email iff not already sent.
    Mirrors ui.py's _maybe_send_apify_scrape_email so both processes
    produce the same email and share the same idempotency flag."""
    if info.get("apify_notified"):
        return
    try:
        import email_run_summary
        email_run_summary.send_apify_scrape_notification(
            run_id=info.get("apify_run_id", ""),
            dataset_id=info.get("apify_dataset_id", ""),
            status=status,
            post_count=int(post_count or 0),
            accounts_requested=len(info.get("accounts", []) or []),
            triggered_at=info.get("started_at", ""),
        )
        info["apify_notified"] = True
        info["apify_notified_status"] = status
    except Exception as e:
        print(f"[watcher] email failed: {e}", file=sys.stderr)


# Fields the pipeline ACTUALLY reads from each Apify item. Matches the
# URL form the user has been pasting manually (confirmed 2026-06-18),
# plus `childPosts` which is critical for carousel/multi-image posts —
# main.py reads carousel slides via post.get('childPosts') and would
# silently process them as single-image posts without it.
#
# Sticking to a curated list (vs. requesting all fields) cuts the items
# JSON payload by roughly an order of magnitude for typical posts — big
# deal on Replit where memory + bandwidth matter for 1000+ post scrapes.
_APIFY_ITEM_FIELDS = (
    "alt,caption,childPosts,displayUrl,videoUrl,locationId,locationName,"
    "profilePicUrl,username,url,timestamp,ownerUsername,ownerFullName,"
    "inputUrl,images,fullName,firstComment,id,isPinned,shortCode"
)


def _api_items_url(dataset_id: str, token: str) -> str:
    """Build the URL main.py needs to fetch dataset items as JSON.

    Apify exposes two URL forms for a dataset:

      · console.apify.com/storage/datasets/<id>
        Human-facing web page. Returns HTML. main.py would try to
        json.loads() the HTML and crash with "Expecting value: line 1
        column 1 (char 0)" — which is the exact crash the user hit
        when Path C auto-chain saved this form to config.json.

      · api.apify.com/v2/datasets/<id>/items?token=<key>&format=json
        &fields=<list>&clean=true
        JSON endpoint. This is what we want.

    URL params:
      · token=    : optional for public datasets, required for private.
                    We always include it to be safe; pulled from
                    APIFY_API_KEY env (same one the scrape was triggered with).
      · format=json : returns array of items (default would be JSONL)
      · fields=   : whitelist of keys to include per item. Massively
                    smaller payload than requesting all fields. List
                    is the union of (a) what the user's hand-pasted
                    URL has, and (b) what main.py actually reads from
                    each post — see _APIFY_ITEM_FIELDS comment above.
      · clean=true : drops null/empty fields per item, smaller still.
                     Apify's actor populates fields conditionally; clean
                     turns "all None values" into no key at all, which
                     main.py's .get(...) calls handle identically.
    """
    if not (dataset_id and token):
        return ""
    return (f"https://api.apify.com/v2/datasets/{dataset_id}/items"
            f"?token={token}&format=json"
            f"&fields={_APIFY_ITEM_FIELDS}"
            f"&clean=true")


def maybe_auto_chain(info: dict, post_count: int, repo_root: Path):
    """If auto_chain=True and Apify returned enough posts, spawn the
    pipeline as a detached subprocess. Updates info in-place."""
    if not info.get("auto_chain"):
        return
    if info.get("auto_chain_triggered"):
        return
    threshold = int(info.get("auto_chain_min_posts", 10) or 10)
    if post_count < threshold:
        print(f"[watcher] auto-chain skipped — only {post_count} posts < threshold {threshold}",
              file=sys.stderr)
        info["auto_chain_skipped"] = True
        info["auto_chain_skipped_reason"] = f"only {post_count} posts (need {threshold})"
        return

    # Update config.json so the pipeline picks up the dataset. Use the
    # API items URL (JSON) — the console URL is for humans and returns
    # HTML, which would crash main.py's response.json() call.
    dataset_id = info.get("apify_dataset_id", "")
    token = os.environ.get("APIFY_API_KEY", "").strip()
    items_url = _api_items_url(dataset_id, token)
    if not items_url:
        print(f"[watcher] cannot build API items URL "
              f"(dataset_id={dataset_id!r}, token_present={bool(token)}) "
              f"— aborting auto-chain", file=sys.stderr)
        info["auto_chain_skipped"] = True
        info["auto_chain_skipped_reason"] = "missing dataset_id or APIFY_API_KEY"
        return
    try:
        config_path = repo_root / "config.json"
        cfg = {}
        if config_path.exists():
            cfg = json.loads(config_path.read_text())
        cfg["instagram_data_url"] = items_url
        cfg["apify_enabled"] = False
        tmp = config_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(cfg, indent=2))
        tmp.replace(config_path)
    except Exception as e:
        print(f"[watcher] failed to write config.json: {e}", file=sys.stderr)
        return

    # Write a PID file for the pipeline-run job so the UI's Run screen
    # picks up the live state. Stdout/stderr → /dev/null for the same
    # reason as ui.py's _spawn for JOB_KIND_RUN: main.py has its own
    # TeeOutput → run_<ts>.log, and capturing stdout to UI_pipeline.log
    # would double the per-print disk I/O. The Run panel tails
    # run_*.log directly, so UI_pipeline.log was pure waste.
    try:
        proc = subprocess.Popen(
            [sys.executable, "main.py", "--now"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            cwd=str(repo_root),
        )
        run_job = {
            "pid": proc.pid,
            "cmd": [sys.executable, "main.py", "--now"],
            "log_path": "",
            "trigger": "path_c_watcher",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        run_job_path = repo_root / "outputs" / "UI_JOB_pipeline.json"
        write_job(run_job_path, run_job)
        info["auto_chain_triggered"] = True
        info["auto_chain_pid"] = proc.pid
        print(f"[watcher] auto-chained pipeline → PID {proc.pid}", file=sys.stderr)
    except Exception as e:
        print(f"[watcher] failed to spawn pipeline: {e}", file=sys.stderr)


# ── Main loop ────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("usage: apify_watcher.py <job-state.json> [--poll-secs N]", file=sys.stderr)
        sys.exit(1)

    job_path = Path(sys.argv[1]).resolve()
    poll_secs = DEFAULT_POLL_SECS
    for arg in sys.argv[2:]:
        if arg.startswith("--poll-secs="):
            try:
                poll_secs = max(5, int(arg.split("=", 1)[1]))
            except ValueError:
                pass

    repo_root = Path(__file__).resolve().parent

    info = read_job(job_path)
    if not info:
        print(f"[watcher] no job state at {job_path} — nothing to do", file=sys.stderr)
        sys.exit(1)

    token = os.environ.get("APIFY_API_KEY", "").strip()
    run_id = info.get("apify_run_id")
    if not (token and run_id):
        print("[watcher] missing APIFY_API_KEY or apify_run_id", file=sys.stderr)
        sys.exit(1)

    # Terminate gracefully on SIGTERM (Replit container shutdown, etc.)
    # so we don't leave half-updated job state.
    def _sigterm(s, _):
        print(f"[watcher] received signal {s}, exiting", file=sys.stderr)
        sys.exit(0)
    signal.signal(signal.SIGTERM, _sigterm)

    started = time.time()
    print(f"[watcher] watching Apify run {run_id} (poll every {poll_secs}s)", file=sys.stderr)

    while True:
        # Re-read state every loop so the UI's edits (e.g., user clicking
        # "Run pipeline anyway" or "Cancel — keep the dataset") are picked
        # up. If the state file is deleted (UI's Clear button), exit.
        info = read_job(job_path)
        if not info:
            print("[watcher] job state file gone — exiting", file=sys.stderr)
            sys.exit(0)

        status = poll_status(run_id, token)

        if status in ("SUCCEEDED",):
            post_count = fetch_dataset_count(info.get("apify_dataset_id", ""), token)
            maybe_send_email(info, status, post_count)
            maybe_auto_chain(info, post_count, repo_root)
            write_job(job_path, info)
            print(f"[watcher] terminal status SUCCEEDED ({post_count} posts) — exiting",
                  file=sys.stderr)
            return

        if status in ("FAILED", "TIMED-OUT", "ABORTED"):
            maybe_send_email(info, status, post_count=0)
            write_job(job_path, info)
            print(f"[watcher] terminal status {status} — exiting", file=sys.stderr)
            return

        if status == "POLL_ERROR":
            # Network blip — keep trying, no state mutation
            time.sleep(poll_secs)
            continue

        if time.time() - started > MAX_RUNTIME_SECS:
            print(f"[watcher] runtime ceiling {MAX_RUNTIME_SECS}s hit — exiting", file=sys.stderr)
            return

        time.sleep(poll_secs)


if __name__ == "__main__":
    main()
