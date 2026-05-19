"""
worker_log.py -- per-worker [post_id] prefix for main.py print/log output.

WHY
---
main.py uses ThreadPoolExecutor with multiple workers. They share stdout, so
their lines interleave in run_*.log. Downstream tools can't reliably tell
which line belongs to which post.

This module gives each worker a thread-local post-id context. Every print()
and logger call inside that context is automatically prefixed with
"[<post_id>] ", so the line carries its own routing info no matter how the
output gets interleaved.

INTEGRATION (already done in main.py if you're here for the second pass)
------------------------------------------------------------------------
1. At top of main.py:
       from worker_log import wprint, log_post_context, get_logger

2. Replace:
       logger = logging.getLogger(__name__)
   with:
       logger = get_logger(__name__)

3. Wrap process_post() body in:
       with log_post_context(post_id):
           ...

4. Find-and-replace inside process_post() AND every helper method it calls
   on the same worker thread:
       print(   ->   wprint(

   This includes (at minimum) any of: extract_ocr_text, _setup_pro_model,
   _call_gemini_tier, _run_tier, _process_with_tier_ladder, and any other
   helper that emits per-post output.

5. If you EVER spawn child threads / a sub-executor from inside a worker
   (carousel slide OCR pool, parallel downloads, etc.), use
   submit_with_context() instead of executor.submit() so the child thread
   inherits the parent worker's post context. See its docstring below.

That's it. Lines emitted from the setup phase (before any worker starts)
have no post context and pass through unprefixed -- exact same behaviour
as the original code.
"""

import builtins
import logging
import threading
from contextlib import contextmanager

# Thread-local: each worker thread has its own current post_id.
_ctx = threading.local()

# Module-level lock to make wprint() line writes atomic across threads.
# Without this, two workers calling print() concurrently can smash their
# output onto the same physical line (their bytes interleave at the OS
# write boundary). Holding this lock around the single print() call below
# guarantees each line is emitted as one indivisible unit, so the log file
# stays parseable. The lock is short-held (one print call) so the
# performance impact is negligible.
_print_lock = threading.Lock()


def _current_post_id():
    return getattr(_ctx, 'post_id', None)


@contextmanager
def log_post_context(post_id):
    """
    Bind the current thread's log/print output to a post_id. Restores the
    prior value on exit (nestable, though we don't expect to nest).
    """
    prev = getattr(_ctx, 'post_id', None)
    _ctx.post_id = str(post_id) if post_id is not None else None
    try:
        yield
    finally:
        _ctx.post_id = prev


def wprint(*args, **kwargs):
    """
    Drop-in replacement for builtins.print() that prepends [post_id] when a
    worker context is active. Outside a worker context, behaves like print().

    Uses _print_lock to make each line write atomic. When the current thread
    is inside a post_buffer() context AND buffered mode is on, appends to
    the per-thread buffer instead of writing immediately — the buffer is
    flushed atomically when post_buffer() exits.
    """
    pid = _current_post_id()
    sep = kwargs.get('sep', ' ')
    msg = sep.join(str(a) for a in args)
    line = f"[{pid}] {msg}" if pid else msg

    buf = getattr(_ctx, 'buffer', None)
    if _buffered_mode and buf is not None:
        buf.append(line)
        return

    kwargs_clean = {k: v for k, v in kwargs.items() if k != 'sep'}
    with _print_lock:
        builtins.print(line, **kwargs_clean)


# ─────────────────────────────────────────────────────────────────
# Worker ID — short, stable per-thread identifier ("T-1", "T-2", ...)
# Derived from threading.current_thread().name so the ID is intrinsic
# to the thread, not cached via thread.ident (which the OS can recycle
# after thread death and break attribution).
#
# ThreadPoolExecutor names its workers "ThreadPoolExecutor-N_M" where
# M is the per-pool worker index — we extract M as the ID. Main thread
# becomes 'main'. Other named threads pass through.
# ─────────────────────────────────────────────────────────────────


def get_worker_id():
    """Return a short worker ID for the current thread, stable per actual
    thread without caching gotchas. Examples:
      MainThread                 -> 'main'
      ThreadPoolExecutor-0_2     -> 'T-2'
      ThreadPoolExecutor-1_0     -> 'T-0'
      anything else              -> the thread name as-is
    """
    name = threading.current_thread().name
    if name == 'MainThread':
        return 'main'
    if '_' in name:
        suffix = name.rsplit('_', 1)[1]
        if suffix.isdigit():
            return f'T-{suffix}'
    return name


# ─────────────────────────────────────────────────────────────────
# Buffered-output mode
# When ON, wprint() inside a post_buffer() context appends to a per-thread
# buffer instead of writing immediately. The buffer flushes atomically on
# post_buffer() exit, so each post's lines appear as one contiguous block
# in the log — no interleaving with other workers, ever.
#
# TRADEOFFS:
#   + Clean per-post blocks for retrospective log analysis
#   + Zero interleaving regardless of worker count
#   - Lose real-time visibility: a 30s post emits nothing until done
#   - Crash mid-post: finally-block still flushes accumulated lines, but
#     anything that hadn't reached wprint() yet is lost (same as
#     non-buffered, just looks more abrupt)
# ─────────────────────────────────────────────────────────────────

_buffered_mode = False


def set_buffered_mode(on):
    """Globally enable/disable buffered output. Call once at program start
    before any workers run. Once a worker has begun, toggling this mid-run
    is undefined."""
    global _buffered_mode
    _buffered_mode = bool(on)


def is_buffered_mode():
    return _buffered_mode


@contextmanager
def post_buffer():
    """Buffer wprint() output emitted within this context; flush atomically
    on exit. No-op when buffered mode is off (set_buffered_mode(False)).

    Always flushes on exit, including on exception — so a crash mid-post
    still surfaces whatever lines the worker had emitted up to that point.
    Wraps the flush in _print_lock so simultaneous flushes from different
    workers don't interleave block-with-block.
    """
    if not _buffered_mode:
        yield
        return
    _ctx.buffer = []
    try:
        yield
    finally:
        lines = getattr(_ctx, 'buffer', None) or []
        _ctx.buffer = None
        if lines:
            with _print_lock:
                for line in lines:
                    builtins.print(line)


def print_unbuffered(*args, **kwargs):
    """Atomic, unprefixed, unbuffered print — for boundary markers like
    'Processing post' / 'Done in Xs' that should always go straight to
    stdout regardless of buffered mode, and shouldn't get the [post_id]
    prefix from wprint."""
    with _print_lock:
        builtins.print(*args, **kwargs)


class _PostIdAdapter(logging.LoggerAdapter):
    """LoggerAdapter that prepends [post_id] to every record."""

    def process(self, msg, kwargs):
        pid = _current_post_id()
        if pid:
            return f"[{pid}] {msg}", kwargs
        return msg, kwargs


def get_logger(name):
    """Return a logger that auto-prefixes inside log_post_context()."""
    return _PostIdAdapter(logging.getLogger(name), {})


def submit_with_context(executor, fn, *args, **kwargs):
    """
    Submit fn to a child executor while propagating the current post_id
    context into the spawned thread.

    Use this INSTEAD OF executor.submit() whenever you spawn child threads
    from inside a worker -- otherwise the child thread has its own empty
    thread-local state and any wprint() calls inside fn will emit
    unprefixed lines (which the log parser then misattributes).

    Example:
        # WRONG (child thread loses context):
        futures = [pool.submit(self._ocr_slide, url) for url in urls]

        # RIGHT (child thread inherits this worker's post_id):
        futures = [submit_with_context(pool, self._ocr_slide, url) for url in urls]

    No-op if there's no active context (just behaves like executor.submit).
    """
    pid = _current_post_id()
    if pid is None:
        return executor.submit(fn, *args, **kwargs)

    def _wrapped(*a, **kw):
        with log_post_context(pid):
            return fn(*a, **kw)

    return executor.submit(_wrapped, *args, **kwargs)
