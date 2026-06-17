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
                         token: str, memory_mb: int = 4096,
                         timeout_secs: int = 3600) -> dict:
    """Kick off an Apify actor run. Returns {'run_id', 'dataset_id'} or
    raises on HTTP error. Non-blocking — caller polls status separately.

    Memory tier matters: the actor uses the memory allocation to decide
    how many parallel scraping workers to spawn internally. Default 4096
    MB matches Apify's actor default and is what we were sending before;
    bumping to 8192 or 16384 typically halves or quarters wall-clock for
    large account lists (1000+ usernames) — at the cost of consuming
    compute units faster. Total compute spend is similar (memory × time
    is what's billed), just compressed.
    """
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

    # Memory + timeout are query parameters on the run-creation endpoint
    # per Apify's API spec; they're separate from the actor-input payload.
    r = requests.post(
        f"{APIFY_API_BASE}/acts/{APIFY_ACTOR}/runs",
        params={
            "token": token,
            "memory": int(memory_mb),
            "timeout": int(timeout_secs),
        },
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
        ("Run", "Settings", "Lookup Post", "Accounts", "Audits & Tools"),
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
                # --now forces immediate execution. Without it, main.py
                # enters scheduler mode and waits for the schedule_day/time
                # match instead of running. The user clicking Run Pipeline
                # almost always wants "now," not "next Thursday."
                pid = _spawn([sys.executable, "main.py", "--now"], JOB_KIND_RUN, extra={"trigger": "path_a"})
                st.success(f"Pipeline started (PID {pid}).")
                time.sleep(0.6)
                st.rerun()
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("---")

    # ── Path B / C: trigger Apify via API ──────────────────────────────
    st.subheader("Path B / C — Trigger Apify (with optional auto-chain)")
    st.markdown(
        '<span class="muted">'
        "Kicks off an Apify run via API. <b>Path B</b> stops after the scrape — you sanity-check the "
        "dataset and click Run Pipeline yourself. <b>Path C</b> auto-triggers the pipeline as soon as "
        "Apify succeeds with at least the configured minimum number of posts. Path C requires the UI "
        "tab to stay open; if you close it before Apify finishes, you'll still get the scrape-complete "
        "email and can click Run Pipeline manually."
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
        auto_min_posts = st.number_input(
            "Path C safety: min posts to auto-chain",
            min_value=1, max_value=10000,
            value=int(cfg.get("apify_min_posts_for_auto_run", 10)),
            help="Path C refuses to auto-trigger the pipeline if Apify returns fewer than this many "
                 "posts — guards against running 30 min of Gemini calls on a busted scrape.",
            key="auto_min_posts",
        )
        memory_options = {
            "4096 MB (default, ~30 min for 1000 accts)": 4096,
            "8192 MB (~15 min for 1000 accts)": 8192,
            "16384 MB (fastest, ~7 min for 1000 accts)": 16384,
            "32768 MB (overkill for most cases)": 32768,
        }
        default_mem = int(cfg.get("apify_memory_mb", 8192))
        # Match the saved value to a label; fall back to the closest higher tier
        default_label = next(
            (k for k, v in memory_options.items() if v == default_mem),
            "8192 MB (~15 min for 1000 accts)",
        )
        memory_label = st.selectbox(
            "Apify compute tier",
            options=list(memory_options.keys()),
            index=list(memory_options.keys()).index(default_label),
            help="Apify charges for compute-units (memory × time). Higher tiers finish faster "
                 "but burn the same total CUs. Match this to what you see when triggering on "
                 "the Apify website — the API was defaulting to the actor's lowest tier (4096), "
                 "which is why API-triggered scrapes felt slower than your manual runs.",
            key="apify_memory_label",
        )
        memory_mb = memory_options[memory_label]

    apify_token = os.environ.get("APIFY_API_KEY", "").strip()
    if not apify_token:
        st.warning("APIFY_API_KEY is not set in environment. Set it in Replit Secrets to use Path B/C.")

    btn_cols = st.columns([1, 1, 3])
    with btn_cols[0]:
        scrape_only_clicked = st.button(
            "🚀 Scrape only (Path B)",
            key="scrape_only",
            disabled=not apify_token,
            help="Kicks off Apify and stops. You'll get an email when it finishes; "
                 "then come back and click Run Pipeline.",
        )
    with btn_cols[1]:
        scrape_and_run_clicked = st.button(
            "🔗 Scrape & Run (Path C)",
            key="scrape_and_run",
            disabled=not apify_token,
            type="primary",
            help="Triggers Apify, polls until done, then auto-starts the pipeline as long as the "
                 "scrape returned at least the minimum post count above.",
        )

    if scrape_only_clicked or scrape_and_run_clicked:
        names = [a.strip().lstrip("@") for a in accts_input.splitlines() if a.strip()]
        if not names:
            st.error("Add at least one account.")
        else:
            try:
                result = apify_trigger_scrape(
                    names, results_limit, newer_than, skip_pinned, apify_token,
                    memory_mb=memory_mb,
                )
                # Persist the chosen settings so subsequent panels render with
                # the same defaults without the user re-picking each time.
                save_config({
                    "apify_memory_mb": int(memory_mb),
                    **({"apify_min_posts_for_auto_run": int(auto_min_posts)}
                       if scrape_and_run_clicked else {}),
                })
                _write_job(JOB_KIND_SCRAPE, {
                    "pid": 0,  # no local subprocess — Apify is remote
                    "remote": True,
                    "apify_run_id": result["run_id"],
                    "apify_dataset_id": result["dataset_id"],
                    "accounts": names,
                    "results_limit": results_limit,
                    "newer_than_days": newer_than,
                    "apify_memory_mb": int(memory_mb),
                    "auto_chain": bool(scrape_and_run_clicked),
                    "auto_chain_min_posts": int(auto_min_posts),
                })
                # Spawn the background watcher subprocess so the scrape-
                # complete email + the Path C auto-chain still fire when
                # the user closes the browser tab. The watcher polls
                # Apify on its own cadence and writes back to the same
                # JSON state file the UI reads. Idempotent guards in
                # both processes prevent double emails / double spawns.
                try:
                    watcher_log = OUTPUTS / "UI_apify_watcher.log"
                    OUTPUTS.mkdir(parents=True, exist_ok=True)
                    with open(watcher_log, "ab") as _wlog:
                        subprocess.Popen(
                            [sys.executable, "apify_watcher.py", str(_job_path(JOB_KIND_SCRAPE))],
                            stdout=_wlog,
                            stderr=subprocess.STDOUT,
                            start_new_session=True,
                            cwd=str(ROOT),
                        )
                except Exception as wexc:
                    # Watcher spawn failure is non-fatal — the UI's own
                    # polling still handles the case where the tab stays
                    # open. We just warn the user that browser-close
                    # fallback won't work for this scrape.
                    st.warning(
                        f"Background watcher couldn't be spawned ({wexc}). "
                        f"Email and Path C auto-chain will only fire if you keep this tab open."
                    )

                label = "Path C (auto-chain)" if scrape_and_run_clicked else "Path B (scrape only)"
                st.success(
                    f"Apify run started: {result['run_id']} · {label} · "
                    f"compute tier {memory_mb} MB"
                )
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


def _maybe_send_apify_scrape_email(info: dict, status: str, post_count: int = 0):
    """Send the Apify-scrape-finished email exactly once per (run_id, status).

    Streamlit's poll loop re-enters this code path every few seconds while
    the user is on the screen — without dedup we'd flood the inbox. The
    'apify_notified' field in the job state file is the source of truth.
    """
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
    except Exception as e:
        # Non-fatal — UI shouldn't break because email failed
        print(f"  ⚠ apify-complete email path raised: {e}")
        return
    # Stamp 'notified' on the job state so future polls don't re-send
    info["apify_notified"] = True
    info["apify_notified_status"] = status
    _write_job(JOB_KIND_SCRAPE, info)


def _fetch_apify_dataset_count(dataset_id: str, token: str) -> int:
    """Quick HEAD/limited GET to learn how many posts the scrape returned.
    Returns 0 on any failure — count is informational, not load-bearing."""
    if not (dataset_id and token):
        return 0
    try:
        import requests
        r = requests.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}",
            params={"token": token},
            timeout=10,
        )
        if r.ok:
            return int((r.json().get("data") or {}).get("itemCount") or 0)
    except Exception:
        pass
    return 0


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
        post_count = _fetch_apify_dataset_count(apify_dataset_id, token)
        _maybe_send_apify_scrape_email(info, status, post_count=post_count)

        # ── Path C: auto-chain to pipeline if requested ────────────────
        # Only triggers when:
        #   · this scrape was kicked off via the "Scrape & Run" (Path C) button
        #   · the scrape returned at least the user's safety threshold of posts
        #   · no pipeline run is currently in progress (defensive — shouldn't
        #     happen given the Run screen would render the run panel instead,
        #     but covers the case where someone manually started main.py from
        #     the shell at the same time)
        if info.get("auto_chain") and not info.get("auto_chain_triggered"):
            threshold = int(info.get("auto_chain_min_posts", 10) or 10)
            run_status, _ = _job_status(JOB_KIND_RUN)
            if run_status == "running":
                st.warning(
                    "Path C wanted to auto-trigger the pipeline, but another pipeline "
                    "run is already in progress. Letting that one finish — you can "
                    "click Auto-fill below to manually queue this dataset after."
                )
            elif post_count < threshold:
                st.error(
                    f"Path C safety guard: Apify returned only **{post_count} posts**, below your "
                    f"minimum of **{threshold}**. The pipeline was NOT auto-triggered — this is "
                    f"the guard catching what's probably a busted scrape (quota issue, dead accounts, "
                    f"wrong actor settings, etc.). Investigate the dataset before processing."
                )
                cols = st.columns([1, 1, 3])
                with cols[0]:
                    if st.button("Run pipeline anyway", key="auto_chain_override"):
                        save_config({"instagram_data_url": apify_dataset_url(apify_dataset_id),
                                     "apify_enabled": False})
                        pid = _spawn([sys.executable, "main.py", "--now"],
                                     JOB_KIND_RUN, extra={"trigger": "path_c_override"})
                        info["auto_chain_triggered"] = True
                        _write_job(JOB_KIND_SCRAPE, info)
                        _clear_job(JOB_KIND_SCRAPE)
                        st.success(f"Pipeline started (PID {pid}). Switch to **Run** for the log.")
                        time.sleep(0.6)
                        st.rerun()
                with cols[1]:
                    if st.button("Cancel — keep the dataset", key="auto_chain_cancel"):
                        # Treat as Path B from here: user can manually fill URL into Path A.
                        info["auto_chain"] = False
                        _write_job(JOB_KIND_SCRAPE, info)
                        st.rerun()
                return
            else:
                # Happy Path C: enough posts, no conflict — chain it.
                save_config({"instagram_data_url": apify_dataset_url(apify_dataset_id),
                             "apify_enabled": False})
                pid = _spawn([sys.executable, "main.py", "--now"],
                             JOB_KIND_RUN, extra={"trigger": "path_c_auto"})
                # Stamp the job so we don't re-spawn on the next poll tick
                # in the brief window before _clear_job takes effect.
                info["auto_chain_triggered"] = True
                _write_job(JOB_KIND_SCRAPE, info)
                _clear_job(JOB_KIND_SCRAPE)
                st.success(
                    f"🔗 Path C auto-chained · Apify returned {post_count:,} posts · "
                    f"pipeline started (PID {pid}). Switch to **Run** to watch the live log."
                )
                time.sleep(0.8)
                st.rerun()
                return

        # Path B (or Path C after manual cancellation) lands here:
        # show the dataset, let the user manually trigger.
        st.success(
            f"Scrape complete ({post_count:,} posts). Copy the dataset URL above into "
            f"Path A and click Run Pipeline."
        )
        if st.button("Auto-fill into Path A and continue", key="fill_url"):
            save_config({"instagram_data_url": apify_dataset_url(apify_dataset_id), "apify_enabled": False})
            _clear_job(JOB_KIND_SCRAPE)
            st.rerun()
        if st.button("Clear scrape job", key="clear_done_scrape"):
            _clear_job(JOB_KIND_SCRAPE)
            st.rerun()
        return

    if status in ("FAILED", "TIMED-OUT", "ABORTED") or status.startswith("POLL_ERROR"):
        _maybe_send_apify_scrape_email(info, status, post_count=0)
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


# ─── Screen: Settings ────────────────────────────────────────────────────

# Defaults mirror the ones main.py applies if config.json is absent. Source
# of truth for these values is main.py's `config = {...}` block — when that
# block changes, update this one too so the UI doesn't drift.
CONFIG_DEFAULTS = {
    "max_workers": 10,
    "rate_limit_delay": 0.5,
    "gemini_model": "gemini-2.0-flash-lite",
    "history_max_age_days": 30,
    "apify_enabled": True,
    "apify_posts_per_profile": 25,
    "apify_newer_than_days": 21,
    "apify_min_posts_for_auto_run": 10,
    "apify_memory_mb": 8192,
    "sheet_name": "Instagram_Events_Master",
    "schedule_day": "Thursday",
    "schedule_time": "14:00",
}

GEMINI_MODEL_OPTIONS = [
    "gemini-2.0-flash-lite",
    "gemini-2.0-flash",
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-2.5-pro",
]

DAY_OPTIONS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _env_state(name: str) -> str:
    """Return 'set' / 'not set' for env-var display (don't leak values)."""
    return "✓ set" if os.environ.get(name, "").strip() else "✗ not set"


def screen_settings():
    st.title("Settings")
    st.markdown(
        '<span class="muted">'
        "Edit values in <code>config.json</code> for the next pipeline run. "
        "Changes save when you click <b>Save</b> in each section — partial edits in a section "
        "without Save are discarded on navigation."
        "</span>",
        unsafe_allow_html=True,
    )

    cfg = load_config()

    # ── Pipeline run tuning ─────────────────────────────────────────────
    with st.expander("**Pipeline run tuning**", expanded=True):
        st.markdown(
            '<span class="muted">'
            "Worker count + rate-limit delay control how fast main.py burns through the post list. "
            "More workers = faster but more API contention; lower delay = faster OCR calls but "
            "higher chance of Vision quota errors."
            "</span>",
            unsafe_allow_html=True,
        )
        with st.form("form_run_tuning", clear_on_submit=False):
            workers = st.number_input(
                "max_workers (ThreadPoolExecutor)",
                min_value=1, max_value=50,
                value=int(cfg.get("max_workers", CONFIG_DEFAULTS["max_workers"])),
                help="How many post-processors run in parallel. 10 is the historical default; 15+ "
                     "increases Sheets API quota risk on flushes.",
            )
            rl_delay = st.number_input(
                "rate_limit_delay (seconds between Vision calls)",
                min_value=0.0, max_value=10.0, step=0.1,
                value=float(cfg.get("rate_limit_delay", CONFIG_DEFAULTS["rate_limit_delay"])),
                help="Sleep duration between successive Vision API calls inside a worker. The "
                     "pipeline auto-increases this on 429s, so set the floor here.",
            )
            history_days = st.number_input(
                "history_max_age_days",
                min_value=0, max_value=365,
                value=int(cfg.get("history_max_age_days", CONFIG_DEFAULTS["history_max_age_days"])),
                help="In live-Apify mode, posts older than this many days are excluded from the "
                     "history loaded at run start. Doesn't apply to static_url mode.",
            )
            model = st.selectbox(
                "gemini_model (tier 1–3 model)",
                options=GEMINI_MODEL_OPTIONS,
                index=(GEMINI_MODEL_OPTIONS.index(cfg.get("gemini_model", CONFIG_DEFAULTS["gemini_model"]))
                       if cfg.get("gemini_model", CONFIG_DEFAULTS["gemini_model"]) in GEMINI_MODEL_OPTIONS else 0),
                help="Flash-Lite is the cheapest. Switching to Flash or Pro escalates cost on every "
                     "post — tier 4 already uses Pro for fallbacks, so changing this rarely helps.",
            )
            saved = st.form_submit_button("💾 Save run tuning")
            if saved:
                save_config({
                    "max_workers": int(workers),
                    "rate_limit_delay": float(rl_delay),
                    "history_max_age_days": int(history_days),
                    "gemini_model": model,
                })
                st.success("Saved to config.json")

    # ── Apify ───────────────────────────────────────────────────────────
    with st.expander("**Apify (Path B / live API mode)**", expanded=False):
        st.markdown(
            '<span class="muted">'
            "These knobs only apply when main.py calls Apify directly (Path B from the Run screen). "
            "In your normal workflow (Path A — paste a dataset URL), they are ignored. "
            "<code>apify_enabled</code> is set to <b>false</b> automatically every time you save a "
            "dataset URL or click Run Pipeline from Path A."
            "</span>",
            unsafe_allow_html=True,
        )
        with st.form("form_apify", clear_on_submit=False):
            apify_on = st.checkbox(
                "apify_enabled",
                value=bool(cfg.get("apify_enabled", CONFIG_DEFAULTS["apify_enabled"])),
                help="When true, main.py calls Apify itself (~20–40 min wait). When false, "
                     "main.py reads from instagram_data_url (a pre-existing dataset).",
            )
            apify_limit = st.number_input(
                "apify_posts_per_profile (resultsLimit)",
                min_value=1, max_value=200,
                value=int(cfg.get("apify_posts_per_profile", CONFIG_DEFAULTS["apify_posts_per_profile"])),
                help="How many posts to pull per IG account in a live Apify run. Was 9; bumped to "
                     "25 on 2026-06-04 because high-volume accounts were falling off the bottom.",
            )
            apify_days = st.number_input(
                "apify_newer_than_days",
                min_value=0, max_value=365,
                value=int(cfg.get("apify_newer_than_days", CONFIG_DEFAULTS["apify_newer_than_days"])),
                help="Apify's onlyPostsNewerThan filter. 0 = no filter (returns up to resultsLimit "
                     "regardless of post age).",
            )
            saved = st.form_submit_button("💾 Save Apify settings")
            if saved:
                save_config({
                    "apify_enabled": bool(apify_on),
                    "apify_posts_per_profile": int(apify_limit),
                    "apify_newer_than_days": int(apify_days),
                })
                st.success("Saved to config.json")

    # ── Sheets ──────────────────────────────────────────────────────────
    with st.expander("**Sheets**", expanded=False):
        st.markdown(
            '<span class="muted">'
            "Which Google Sheet the pipeline writes to. The link-out buttons on Run / Accounts "
            "use this name."
            "</span>",
            unsafe_allow_html=True,
        )
        with st.form("form_sheets", clear_on_submit=False):
            sheet = st.text_input(
                "sheet_name",
                value=cfg.get("sheet_name", CONFIG_DEFAULTS["sheet_name"]),
                help="Exact case-sensitive Google Sheet name (must already exist).",
            )
            saved = st.form_submit_button("💾 Save sheet name")
            if saved:
                save_config({"sheet_name": sheet.strip()})
                st.success("Saved to config.json")

    # ── Schedule ────────────────────────────────────────────────────────
    with st.expander("**Schedule**", expanded=False):
        st.markdown(
            '<span class="muted">'
            "main.py has two modes: <b>scheduler</b> (loops forever waiting for the next "
            "schedule_day/time match) and <b>run now</b> (fires immediately). The UI's Run "
            "Pipeline buttons always use <code>--now</code>, so these schedule settings only "
            "matter if you launch main.py from a cron job or the Replit shell without "
            "<code>--now</code>."
            "</span>",
            unsafe_allow_html=True,
        )
        with st.form("form_schedule", clear_on_submit=False):
            day_val = cfg.get("schedule_day", CONFIG_DEFAULTS["schedule_day"])
            sched_day = st.selectbox(
                "schedule_day",
                options=DAY_OPTIONS,
                index=DAY_OPTIONS.index(day_val) if day_val in DAY_OPTIONS else 3,
            )
            sched_time = st.text_input(
                "schedule_time (24h, HH:MM)",
                value=cfg.get("schedule_time", CONFIG_DEFAULTS["schedule_time"]),
            )
            saved = st.form_submit_button("💾 Save schedule")
            if saved:
                if not re.match(r"^\d{1,2}:\d{2}$", sched_time.strip()):
                    st.error("schedule_time must look like HH:MM (e.g., 14:00)")
                else:
                    save_config({
                        "schedule_day": sched_day,
                        "schedule_time": sched_time.strip(),
                    })
                    st.success("Saved to config.json")

        # Quick "run now" shortcut from the Schedule section — same effect
        # as the Run screen's Run Pipeline (Path A) button.
        st.markdown("---")
        run_status, _ = _job_status(JOB_KIND_RUN)
        if run_status == "running":
            st.info("A pipeline run is already in progress — go to **Run** to see the live log.")
        else:
            st.markdown(
                '<span class="muted">'
                "Skip the schedule and trigger main.py right now."
                "</span>",
                unsafe_allow_html=True,
            )
            if st.button("⚡ Run Now (skip schedule)", key="settings_run_now", type="primary"):
                current_url = cfg.get("instagram_data_url", "").strip()
                if not current_url:
                    st.error("No dataset URL is set in config.json. Set one in **Run** (Path A) first.")
                else:
                    pid = _spawn(
                        [sys.executable, "main.py", "--now"],
                        JOB_KIND_RUN,
                        extra={"trigger": "settings_run_now"},
                    )
                    st.success(f"Pipeline started (PID {pid}). Go to **Run** to see the live log.")

    # ── Outage watchdog (env-var only — display only) ───────────────────
    with st.expander("**Outage watchdog**", expanded=False):
        st.markdown(
            '<span class="muted">'
            "Thresholds for Vision 403 / Gemini 429 aborts. These are environment variables, "
            "not config.json — change them on the Replit <b>Secrets</b> tab. Defaults shown "
            "in italics."
            "</span>",
            unsafe_allow_html=True,
        )
        rows = [
            ("WATCHDOG_VISION_403_BILLING_LIMIT", "3", os.environ.get("WATCHDOG_VISION_403_BILLING_LIMIT", "")),
            ("WATCHDOG_VISION_403_BILLING_WINDOW", "120", os.environ.get("WATCHDOG_VISION_403_BILLING_WINDOW", "")),
            ("WATCHDOG_VISION_403_LIMIT", "5", os.environ.get("WATCHDOG_VISION_403_LIMIT", "")),
            ("WATCHDOG_VISION_403_WINDOW", "60", os.environ.get("WATCHDOG_VISION_403_WINDOW", "")),
            ("WATCHDOG_GEMINI_429_LIMIT", "10", os.environ.get("WATCHDOG_GEMINI_429_LIMIT", "")),
            ("WATCHDOG_GEMINI_429_WINDOW", "60", os.environ.get("WATCHDOG_GEMINI_429_WINDOW", "")),
        ]
        st.dataframe(
            {
                "Variable":        [r[0] for r in rows],
                "Default":         [r[1] for r in rows],
                "Current (env)":   [r[2] or "—" for r in rows],
            },
            use_container_width=True, hide_index=True,
        )

    # ── Email notifications (env-var only — display only) ──────────────
    with st.expander("**Email notifications (outage)**", expanded=False):
        st.markdown(
            '<span class="muted">'
            "Set these on the Replit <b>Secrets</b> tab. Passwords never appear in the UI. "
            "<code>ADMIN_EMAIL</code> accepts a comma-separated list."
            "</span>",
            unsafe_allow_html=True,
        )
        rows = [
            ("ADMIN_EMAIL",  os.environ.get("ADMIN_EMAIL", "(not set)")),
            ("SMTP_HOST",    _env_state("SMTP_HOST")),
            ("SMTP_PORT",    _env_state("SMTP_PORT")),
            ("SMTP_USER",    _env_state("SMTP_USER")),
            ("SMTP_PASS",    _env_state("SMTP_PASS")),
            ("APIFY_API_KEY", _env_state("APIFY_API_KEY")),
        ]
        st.dataframe(
            {"Variable": [r[0] for r in rows], "State / value": [r[1] for r in rows]},
            use_container_width=True, hide_index=True,
        )

    # ── Raw config.json view (for power users) ─────────────────────────
    with st.expander("**Show raw config.json**", expanded=False):
        st.code(json.dumps(load_config(), indent=2), language="json")


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
elif screen == "Settings":
    screen_settings()
elif screen == "Lookup Post":
    screen_lookup()
elif screen == "Accounts":
    screen_accounts()
elif screen == "Audits & Tools":
    screen_audits()
