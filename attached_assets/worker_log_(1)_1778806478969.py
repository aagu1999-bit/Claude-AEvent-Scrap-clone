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
    """
    pid = _current_post_id()
    if pid:
        sep = kwargs.get('sep', ' ')
        msg = sep.join(str(a) for a in args)
        kwargs_clean = {k: v for k, v in kwargs.items() if k != 'sep'}
        builtins.print(f"[{pid}] {msg}", **kwargs_clean)
    else:
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
