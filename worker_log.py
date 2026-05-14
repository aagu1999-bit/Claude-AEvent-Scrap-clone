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

INTEGRATION (Replit agent: do these three edits in main.py)
----------------------------------------------------------
1. Add at top of main.py (with other imports):

       from worker_log import wprint, log_post_context, get_logger

2. Replace:
       logger = logging.getLogger(__name__)
   with:
       logger = get_logger(__name__)

3. In InstagramEventPipeline.process_post(), wrap the *entire* method body
   in a context manager so every line gets prefixed:

       def process_post(self, post, post_num, total):
           post_id = (post.get('id', '') or post.get('shortCode', '')
                      or post.get('shortcode', '') or f'post_{post_num}')
           with log_post_context(post_id):
               # ...existing body unchanged...

4. Find-and-replace inside process_post() (and any helpers it calls):
       print(   ->   wprint(
   The functions are signature-compatible drop-ins.

That's it. Lines emitted from the setup phase (before any worker starts)
have no post context and pass through unprefixed -- exact same behaviour
as today.
"""

import builtins
import logging
import threading
from contextlib import contextmanager

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
