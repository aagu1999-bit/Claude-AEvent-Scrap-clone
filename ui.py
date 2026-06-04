"""
ui.py — Streamlit dashboard for the Apify Instagram event pipeline.

Wraps:
  · main.py (the pipeline)              — Path A: paste dataset URL + Run
  · Apify API (apify/instagram-post-scraper) — Path B: accounts + settings + Scrape
  · lookup_post.py                       — Diagnose a missing/wrong post
  · 4 admin scripts                       — Audits + Tools

Designed primarily for the project owner, with occasional non-technical
operators in mind: big buttons, no jargon in user-facing labels, confirms
on irreversible actions, read-only on Sheet data (link out to Google for
review).

Architecture:
  · Streamlit runs in the foreground.
  · Long jobs (main.py, Apify scrape, lookup_post.py) spawn as subprocess.
  · PID + start metadata go to outputs/UI_JOB_<kind>.json so the UI can
    detect "job in progress" across browser refreshes and process restarts.
  · main.py already writes outputs/run_<ts>.log line by line — the UI
    tails that file instead of capturing the subprocess's stdout. That
    way closing the browser doesn't lose the log; you reopen and see
    where the run is.

Launching on Replit:
  streamlit run ui.py --server.port 8080 --server.address 0.0.0.0
"""

import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import streamlit as st


# ─── Constants ────────────────────────────────────────────────────────────
ROOT          = Path(__file__).resolve().parent
OUTPUTS       = ROOT / "outputs"
CONFIG_PATH   = ROOT / "config.json"
ACCOUNTS_JSON = ROOT / "accounts.json"

JOB_KIND_RUN     = "pipeline"
JOB_KIND_SCRAPE  = "apify_scrape"
JOB_KIND_LOOKUP  = "lookup"
JOB_KIND_AUDIT   = "audit"

# Apify
APIFY_ACTOR     = "apify~instagram-post-scraper"
APIFY_API_BASE  = "https://api.apify.com/v2"

# Sheet — link-out targets. The actual sheet ID isn't in the repo; we read
# from config.json if present, otherwise show generic Google Sheets entry.
SHEET_NAME_DEFAULT = "Instagram_Events_Master"

# Polling cadence for live-tail panels — fast enough to feel live, slow
# enough not to hammer Replit's request budget.
TAIL_REFRESH_SEC = 2.0


# ─── Subprocess + job-state plumbing ──────────────────────────────────────

def _job_path(kind: str) -> Path:
    return OUTPUTS / f"UI_JOB_{kind}.json"


def _write_job(kind: str, info: dict):
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    info = {**info, "started_at": info.get("started_at") or datetime.now().isoformat(timespec="seconds")}
    _job_path(kind).write_text(json.dumps(info, indent=2, default=str))


def _read_job(kind: str) -> dict | None:
    p = _job_path(kind)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:
        return None


def _clear_job(kind: str):
    p = _job_path(kind)
    if p.exists():
        p.unlink()


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False


def _job_status(kind: str) -> tuple[str, dict | None]:
    """Returns ('running' | 'crashed' | 'idle', info or None)."""
    info = _read_job(kind)
    if not info:
        return "idle", None
    pid = info.get("pid")
    if pid and _pid_alive(pid):
        return "running", info
    return "crashed", info


def _spawn(cmd: list[str], kind: str, extra: dict | None = None) -> int:
    """Start a detached subprocess, write the job-state file, return PID.
    Subprocess survives browser refresh; its stdout goes to a log file
    rather than getting piped into the UI process."""
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    log_path = OUTPUTS / f"UI_{kind}.log"
    log_fd = open(log_path, "ab")
    # start_new_session=True detaches the child from the Streamlit process
    # group so SIGINT to Streamlit doesn't kill the pipeline.
    proc = subprocess.Popen(
        cmd,
        stdout=log_fd,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    info = {"pid": proc.pid, "cmd": cmd, "log_path": str(log_path)}
    if extra:
        info.update(extra)
    _write_job(kind, info)
    return proc.pid


def _stop_job(kind: str) -> bool:
    info = _read_job(kind)
    if not info:
        return False
    pid = info.get("pid")
    if pid and _pid_alive(pid):
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except Exception:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                return False
    _clear_job(kind)
    return True


# ─── Config helpers ───────────────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_config(updates: dict):
    """Merge-and-save. Atomic via temp + rename so a crash mid-write doesn't
    leave a half-written config."""
    cur = load_config()
    cur.update(updates)
    tmp = CONFIG_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cur, indent=2))
    tmp.replace(CONFIG_PATH)


def load_accounts_from_file() -> list[str]:
    """Read accounts.json — the local fallback list."""
    if not ACCOUNTS_JSON.exists():
        return []
    try:
        data = json.loads(ACCOUNTS_JSON.read_text())
        if isinstance(data, dict) and "accounts" in data:
            data = data["accounts"]
        if isinstance(data, list):
            return [str(a).strip().lstrip("@") for a in data if str(a).strip()]
    except Exception:
        pass
    return []


# ─── Apify API ────────────────────────────────────────────────────────────

def apify_trigger_scrape(usernames: list[str], results_limit: int,
                         newer_than_days: int, skip_pinned: bool,
                         token: str) -> dict:
    """Kick off an Apify actor run. Returns {'run_id', 'dataset_id'} or
    raises on HTTP error. Non-blocking — caller polls status separately."""
    import requests
    newer_than_date = ""
    if newer_than_days > 0:
        from datetime import timedelta
        newer_than_date = (datetime.now() - timedelta(days=newer_than_days)).strftime("%Y-%m-%d")

    payload = {
        "username": usernames,
        "resultsLimit": int(results_limit),
        "skipPinnedPosts": bool(skip_pinned),
        "dataDetailLevel": "detailedData",
    }
    if newer_than_date:
        payload["onlyPostsNewerThan"] = newer_than_date

    r = requests.post(
        f"{APIFY_API_BASE}/acts/{APIFY_ACTOR}/runs",
        params={"token": token},
        json=payload,
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()["data"]
    return {"run_id": data["id"], "dataset_id": data["defaultDatasetId"]}


def apify_poll_status(run_id: str, token: str) -> str:
    """Returns Apify's status string: READY/RUNNING/SUCCEEDED/FAILED/TIMED-OUT/ABORTED."""
    import requests
    r = requests.get(
        f"{APIFY_API_BASE}/actor-runs/{run_id}",
        params={"token": token},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()["data"]["status"]


def apify_dataset_url(dataset_id: str) -> str:
    return f"https://console.apify.com/storage/datasets/{dataset_id}"


# ─── Log tailing ──────────────────────────────────────────────────────────

def latest_run_log() -> Path | None:
    if not OUTPUTS.exists():
        return None
    logs = sorted(OUTPUTS.glob("run_*.log"), reverse=True)
    return logs[0] if logs else None


def tail_text(path: Path, max_chars: int = 8000) -> str:
    """Return the last max_chars characters of a file. Cheaper than reading
    the whole file every refresh — pipeline logs grow large quickly."""
    if not path or not path.exists():
        return ""
    size = path.stat().st_size
    with open(path, "rb") as f:
        if size > max_chars:
            f.seek(size - max_chars)
            data = f.read()
            # Drop the partial first line so we start at a line boundary
            idx = data.find(b"\n")
            if idx >= 0:
                data = data[idx + 1:]
        else:
            data = f.read()
    return data.decode("utf-8", errors="replace")


def parse_run_progress(log_text: str) -> tuple[int, int]:
    """Look for '[Wn] [k/N] Processing post' lines to estimate progress.
    Returns (current, total) or (0, 0) if no progress line seen yet."""
    last = (0, 0)
    for m in re.finditer(r"\[(\d+)/(\d+)\] Processing post", log_text):
        last = (int(m.group(1)), int(m.group(2)))
    return last


# ─── Outage marker detection ─────────────────────────────────────────────

def latest_outage_marker() -> dict | None:
    if not OUTPUTS.exists():
        return None
    markers = sorted(OUTPUTS.glob("OUTAGE_ABORT_*.json"), reverse=True)
    if not markers:
        return None
    try:
        data = json.loads(markers[0].read_text())
        data["_marker_path"] = str(markers[0])
        data["_marker_name"] = markers[0].name
        return data
    except Exception:
        return None


def acknowledge_outage(marker_path: str):
    """Rename ACK'd markers so they stop appearing as banners. Keeps the
    file (for the audit trail) but with a leading underscore so the
    `OUTAGE_ABORT_*.json` glob misses it on the next pass."""
    p = Path(marker_path)
    if p.exists():
        p.rename(p.with_name("_ack_" + p.name))


# ─── Last-run stats ──────────────────────────────────────────────────────

def latest_dataset_run_log_entries(limit: int = 5) -> list[dict]:
    """Read outputs/dataset_run_log.jsonl — main.py appends one entry per
    pipeline run. Used for the dashboard summary."""
    p = OUTPUTS / "dataset_run_log.jsonl"
    if not p.exists():
        return []
    entries = []
    with open(p) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except Exception:
                continue
    return entries[-limit:][::-1]


# ─── Page rendering ───────────────────────────────────────────────────────

st.set_page_config(
    page_title="Apify Pipeline",
    page_icon="📅",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for friendlier look
st.markdown(
    """
    <style>
      .stApp [data-testid="stMetricValue"] { font-size: 1.6rem; }
      .big-btn button { height: 3.2rem; font-size: 1.1rem; font-weight: 600; }
      .danger-btn button { background-color: #b91c1c; color: white; }
      .muted { color: #6b7280; font-size: 0.85rem; }
      .outage-banner {
        background: #fef2f2; border: 2px solid #b91c1c; border-radius: 8px;
        padding: 12px 16px; margin: 8px 0;
      }
      .legend-swatch {
        display: inline-block; width: 12px; height: 12px; border-radius: 2px;
        margin-right: 6px; vertical-align: middle;
      }
    </style>
    """,
    unsafe_allow_html=True,
)


def render_outage_banner():
    marker = latest_outage_marker()
    if not marker:
        return
    st.markdown(
        f"""
        <div class="outage-banner">
          <strong>⛔ Outage marker present — {marker.get('category', 'unknown')}</strong><br/>
          {marker.get('count', '?')} failures in {marker.get('window_seconds', '?')}s.
          The previous run was aborted.
          <span class="muted">{marker.get('_marker_name', '')}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    cols = st.columns([1, 1, 4])
    with cols[0]:
        if st.button("Acknowledge", key="ack_outage"):
            acknowledge_outage(marker["_marker_path"])
            st.rerun()
    with cols[1]:
        with st.expander("Details"):
            st.json(marker)


def render_running_panel(kind: str, info: dict, label: str):
    """Live progress + log tail for a running job."""
    started = info.get("started_at", "?")
    st.info(f"**{label} running** — started {started} (PID {info.get('pid', '?')})")
    log_path = latest_run_log() if kind == JOB_KIND_RUN else Path(info.get("log_path", ""))
    log_text = tail_text(log_path) if log_path else ""
    cur, total = parse_run_progress(log_text) if kind == JOB_KIND_RUN else (0, 0)
    if total > 0:
        st.progress(cur / total, text=f"Processing {cur:,} / {total:,} posts")

    st.code(log_text or "(no log output yet — pipeline is starting up...)", language="log")

    cols = st.columns([1, 5])
    with cols[0]:
        if st.button("⏹ Stop", key=f"stop_{kind}", type="secondary"):
            _stop_job(kind)
            st.success("Stop requested. Workers will drain.")
            time.sleep(1)
            st.rerun()
    # Soft auto-refresh — Streamlit reruns the script on this sleep
    time.sleep(TAIL_REFRESH_SEC)
    st.rerun()


# ─── Sidebar navigation ──────────────────────────────────────────────────

with st.sidebar:
    st.markdown("# 📅 Apify Pipeline")
    st.markdown("&nbsp;")
    screen = st.radio(
        "Section",
        ("Run", "Lookup Post", "Accounts", "Audits & Tools"),
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.markdown(
        '<div class="muted">'
        "Flag legend<br>"
        '<span class="legend-swatch" style="background:#fce7eb"></span> pink — wrong field<br>'
        '<span class="legend-swatch" style="background:#ffe0b8"></span> orange — probably not event<br>'
        '<span class="legend-swatch" style="background:#fff7c7"></span> yellow — legacy / missing<br>'
        "</div>",
        unsafe_allow_html=True,
    )


# ─── Screen: Run ─────────────────────────────────────────────────────────

def screen_run():
    st.title("Run")
    render_outage_banner()

    run_status, run_info = _job_status(JOB_KIND_RUN)
    scrape_status, scrape_info = _job_status(JOB_KIND_SCRAPE)

    # Scrape in progress → show its panel above the form
    if scrape_status == "running":
        st.subheader("Apify scrape in progress")
        render_apify_scrape_panel(scrape_info)
        return
    if scrape_status == "crashed":
        st.warning("Previous Apify scrape ended unexpectedly. Recheck dataset URL before running pipeline.")
        if st.button("Clear scrape state", key="clear_scrape_crash"):
            _clear_job(JOB_KIND_SCRAPE)
            st.rerun()

    # Pipeline in progress → big panel takes over
    if run_status == "running":
        render_running_panel(JOB_KIND_RUN, run_info, "Pipeline")
        return
    if run_status == "crashed":
        st.warning("The previous pipeline run ended unexpectedly. Check the log before starting another.")
        if st.button("Clear run state", key="clear_run_crash"):
            _clear_job(JOB_KIND_RUN)
            st.rerun()

    cfg = load_config()
    current_url = cfg.get("instagram_data_url", "")

    # ── Path A: paste a dataset URL and run ────────────────────────────
    st.subheader("Path A — Already have a dataset URL")
    st.markdown(
        '<span class="muted">When you triggered Apify yourself on the website, paste the dataset URL here.</span>',
        unsafe_allow_html=True,
    )
    new_url = st.text_input("Dataset URL", value=current_url, key="ds_url")

    cols = st.columns([1, 1, 4])
    with cols[0]:
        if st.button("💾 Save URL", key="save_url"):
            save_config({"instagram_data_url": new_url, "apify_enabled": False})
            st.success("Saved to config.json (apify_enabled=false)")
            time.sleep(0.6)
            st.rerun()
    with cols[1]:
        st.markdown('<div class="big-btn">', unsafe_allow_html=True)
        if st.button("▶ Run Pipeline", key="run_pipeline_a", type="primary"):
            if not (new_url or current_url):
                st.error("No dataset URL set.")
            else:
                save_config({"instagram_data_url": new_url, "apify_enabled": False})
                pid = _spawn([sys.executable, "main.py"], JOB_KIND_RUN, extra={"trigger": "path_a"})
                st.success(f"Pipeline started (PID {pid}).")
                time.sleep(0.6)
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")

    # ── Path B: trigger Apify via API + just-scrape ────────────────────
    st.subheader("Path B — Trigger Apify (just scrape, doesn't run pipeline)")
    st.markdown(
        '<span class="muted">'
        "Kicks off an Apify run via API. When it finishes, paste the resulting dataset URL above "
        "and click <b>Run Pipeline</b>."
        "</span>",
        unsafe_allow_html=True,
    )

    cols = st.columns([3, 2])
    with cols[0]:
        accts_default = "\n".join(load_accounts_from_file()[:50])
        accts_input = st.text_area(
            "Accounts (one per line, with or without @)",
            value=accts_default,
            height=200,
            key="scrape_accounts",
        )
    with cols[1]:
        results_limit = st.number_input(
            "Posts per profile",
            min_value=1, max_value=200, value=int(cfg.get("apify_posts_per_profile", 25)),
            key="scrape_results_limit",
        )
        newer_than = st.number_input(
            "Newer than (days, 0 = no filter)",
            min_value=0, max_value=365, value=int(cfg.get("apify_newer_than_days", 21)),
            key="scrape_newer_than",
        )
        skip_pinned = st.checkbox("Skip pinned posts", value=False, key="scrape_skip_pinned")

    apify_token = os.environ.get("APIFY_API_KEY", "").strip()
    if not apify_token:
        st.warning("APIFY_API_KEY is not set in environment. Set it in Replit Secrets to use Path B.")

    if st.button("🚀 Scrape only (Apify API)", key="scrape_only", disabled=not apify_token):
        names = [a.strip().lstrip("@") for a in accts_input.splitlines() if a.strip()]
        if not names:
            st.error("Add at least one account.")
        else:
            try:
                result = apify_trigger_scrape(names, results_limit, newer_than, skip_pinned, apify_token)
                _write_job(JOB_KIND_SCRAPE, {
                    "pid": 0,  # no local subprocess — Apify is remote
                    "remote": True,
                    "apify_run_id": result["run_id"],
                    "apify_dataset_id": result["dataset_id"],
                    "accounts": names,
                    "results_limit": results_limit,
                    "newer_than_days": newer_than,
                })
                st.success(f"Apify run started: {result['run_id']}")
                time.sleep(0.6)
                st.rerun()
            except Exception as e:
                st.error(f"Apify call failed: {e}")

    st.markdown("---")

    # ── Status summary ──────────────────────────────────────────────────
    st.subheader("Recent runs")
    entries = latest_dataset_run_log_entries(limit=5)
    if not entries:
        st.markdown('<span class="muted">No runs logged yet.</span>', unsafe_allow_html=True)
    else:
        for e in entries:
            ts = e.get("run_id") or e.get("timestamp") or "(no timestamp)"
            posts = e.get("post_count", "?")
            src = e.get("source", "?")
            st.markdown(f"- **{ts}** · {posts} posts · `{src}`")

    st.markdown("---")
    cfg_post = load_config()
    sheet_name = cfg_post.get("sheet_name", SHEET_NAME_DEFAULT)
    st.markdown(
        f"📂 [Open **{sheet_name}** in Google Sheets](https://docs.google.com/spreadsheets/) (find the sheet by name)"
    )


def render_apify_scrape_panel(info: dict):
    """For remote Apify runs, poll status via API rather than tail a log."""
    apify_run_id = info.get("apify_run_id")
    apify_dataset_id = info.get("apify_dataset_id")
    token = os.environ.get("APIFY_API_KEY", "").strip()
    if not (apify_run_id and token):
        st.error("Scrape job has no Apify run ID or APIFY_API_KEY is unset. Clearing.")
        _clear_job(JOB_KIND_SCRAPE)
        st.rerun()
        return

    started = info.get("started_at", "?")
    try:
        status = apify_poll_status(apify_run_id, token)
    except Exception as e:
        status = f"POLL_ERROR ({e})"

    st.info(f"Apify run **{apify_run_id}** · started {started}")
    st.markdown(f"Status: **{status}**")
    st.markdown(f"Dataset (when ready): {apify_dataset_url(apify_dataset_id)}")

    if status == "SUCCEEDED":
        st.success("Scrape complete. Copy the dataset URL above into Path A and click Run Pipeline.")
        if st.button("Auto-fill into Path A and continue", key="fill_url"):
            save_config({"instagram_data_url": apify_dataset_url(apify_dataset_id), "apify_enabled": False})
            _clear_job(JOB_KIND_SCRAPE)
            st.rerun()
        if st.button("Clear scrape job", key="clear_done_scrape"):
            _clear_job(JOB_KIND_SCRAPE)
            st.rerun()
        return

    if status in ("FAILED", "TIMED-OUT", "ABORTED") or status.startswith("POLL_ERROR"):
        st.error(f"Apify run ended with status: {status}")
        if st.button("Clear failed scrape", key="clear_failed_scrape"):
            _clear_job(JOB_KIND_SCRAPE)
            st.rerun()
        return

    # Still running — soft refresh.
    if st.button("Stop polling (keep Apify run going)", key="stop_polling"):
        _clear_job(JOB_KIND_SCRAPE)
        st.rerun()
    time.sleep(TAIL_REFRESH_SEC * 3)  # Apify polling: slower than log tail
    st.rerun()


# ─── Screen: Lookup Post ─────────────────────────────────────────────────

def screen_lookup():
    st.title("Lookup Post")
    st.markdown(
        '<span class="muted">'
        "Paste an Instagram shortcode, post ID, or URL. The tool checks every local archive "
        "(Apify dumps, Processed_Log, All_Events, run logs, anomalies) and shows the full trace."
        "</span>",
        unsafe_allow_html=True,
    )

    status, info = _job_status(JOB_KIND_LOOKUP)
    if status == "running":
        st.info("Lookup in progress…")
        log_path = Path(info.get("log_path", ""))
        st.code(tail_text(log_path, max_chars=20000) or "(starting)", language="log")
        if st.button("Cancel", key="cancel_lookup"):
            _stop_job(JOB_KIND_LOOKUP)
            st.rerun()
        time.sleep(TAIL_REFRESH_SEC)
        st.rerun()
        return

    query = st.text_input("Shortcode / post ID / URL", key="lookup_q")
    if st.button("Look Up", key="run_lookup", type="primary"):
        q = query.strip()
        if not q:
            st.error("Enter a value first.")
        else:
            pid = _spawn([sys.executable, "lookup_post.py", q], JOB_KIND_LOOKUP, extra={"query": q})
            st.success(f"Started lookup (PID {pid})")
            time.sleep(0.6)
            st.rerun()

    # If a previous lookup finished, show its output
    if status == "crashed" and info:
        st.markdown("### Last lookup output")
        log_path = Path(info.get("log_path", ""))
        st.code(tail_text(log_path, max_chars=40000), language="log")
        cols = st.columns([1, 5])
        with cols[0]:
            if st.button("Clear", key="clear_lookup"):
                _clear_job(JOB_KIND_LOOKUP)
                st.rerun()


# ─── Screen: Accounts (read-only) ────────────────────────────────────────

def screen_accounts():
    st.title("Accounts")
    st.markdown(
        '<span class="muted">'
        "Read-only view of the local accounts list. Edits happen in the Google Sheet Accounts tab "
        "or by editing accounts.json on Replit. UI editing will be added in a future version."
        "</span>",
        unsafe_allow_html=True,
    )
    accts = load_accounts_from_file()
    st.metric("Accounts in local accounts.json", len(accts))
    if accts:
        st.dataframe(
            {"handle": accts},
            use_container_width=True,
            hide_index=True,
            height=min(600, 35 + 30 * min(len(accts), 20)),
        )
    cfg = load_config()
    sheet_name = cfg.get("sheet_name", SHEET_NAME_DEFAULT)
    st.markdown(
        f"📂 [Open **{sheet_name}** Accounts tab in Google Sheets](https://docs.google.com/spreadsheets/) (find the sheet by name)"
    )


# ─── Screen: Audits & Tools ──────────────────────────────────────────────

AUDIT_TOOLS = [
    {
        "name": "Quality Metrics",
        "script": "quality_metrics.py",
        "description": "Compute extraction stats over a date range — events found, OCR success rate, "
                       "tier ladder breakdown, retry counts.",
    },
    {
        "name": "Region Audit",
        "script": "audit_regions.py",
        "description": "Find rows in All_Events where CITY is set but SECTION OF NJ is missing or "
                       "doesn't match the canonical NJ municipality lookup.",
    },
    {
        "name": "Orphan Check",
        "script": "orphan_check.py",
        "description": "Find Processed_Log rows tagged `events_found` whose corresponding event "
                       "row is missing from All_Events (or vice versa).",
    },
    {
        "name": "Reprocess Weekends",
        "script": "reprocess_weekends.py",
        "description": "Force re-processing for a date range — used when a past weekend's "
                       "extractions look off and you want a fresh pass.",
        "needs_confirm_arg": True,  # this one requires --confirm
    },
    {
        "name": "History Migration",
        "script": "migrate_history.py",
        "description": "One-time migration: copy IDs from outputs/pipeline_checkpoint.pkl into "
                       "the Processed_Log sheet. Used to import a local-only history into Sheets. "
                       "Safe to re-run (skips IDs already present).",
    },
]


def screen_audits():
    st.title("Audits & Tools")
    status, info = _job_status(JOB_KIND_AUDIT)

    if status == "running":
        st.info(f"Tool running: **{info.get('script', '?')}**")
        log_path = Path(info.get("log_path", ""))
        st.code(tail_text(log_path, max_chars=40000) or "(starting)", language="log")
        if st.button("Stop", key="stop_audit"):
            _stop_job(JOB_KIND_AUDIT)
            st.rerun()
        time.sleep(TAIL_REFRESH_SEC)
        st.rerun()
        return

    if status == "crashed" and info:
        st.markdown(f"### Last run: `{info.get('script', '?')}`")
        log_path = Path(info.get("log_path", ""))
        st.code(tail_text(log_path, max_chars=40000), language="log")
        if st.button("Clear", key="clear_audit"):
            _clear_job(JOB_KIND_AUDIT)
            st.rerun()
        st.markdown("---")

    for tool in AUDIT_TOOLS:
        with st.expander(f"**{tool['name']}**"):
            st.markdown(tool["description"])
            script_path = ROOT / tool["script"]
            if not script_path.exists():
                st.warning(f"`{tool['script']}` not found in repo.")
                continue
            cmd = [sys.executable, tool["script"]]
            if tool.get("needs_confirm_arg"):
                confirm = st.checkbox(
                    "I understand this rewrites data and want to proceed",
                    key=f"confirm_{tool['script']}",
                )
                if confirm:
                    cmd.append("--confirm")
                disabled = not confirm
            else:
                disabled = False
            if st.button(f"Run {tool['name']}", key=f"run_{tool['script']}", disabled=disabled):
                pid = _spawn(cmd, JOB_KIND_AUDIT, extra={"script": tool["script"]})
                st.success(f"Started {tool['name']} (PID {pid})")
                time.sleep(0.6)
                st.rerun()

    st.markdown("---")
    st.markdown(
        '<span class="muted">'
        "More tools will land here over time (dedup, purge by date, account hygiene, etc.). "
        "For now, run those from the Replit shell."
        "</span>",
        unsafe_allow_html=True,
    )


# ─── Dispatch ────────────────────────────────────────────────────────────

if screen == "Run":
    screen_run()
elif screen == "Lookup Post":
    screen_lookup()
elif screen == "Accounts":
    screen_accounts()
elif screen == "Audits & Tools":
    screen_audits()
