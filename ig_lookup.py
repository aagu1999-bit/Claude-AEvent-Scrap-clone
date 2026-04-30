#!/usr/bin/env python3
"""
ig_lookup.py — Tiny helpers for hitting Instagram's public profile JSON
endpoint without authentication.

Used by audit.py (end-of-run silent-failure detection) and account_hygiene.py
(periodic full-tab health check). Public endpoint, no login, no cookies —
the same data anyone's browser fetches when loading a profile preview.

Rate limiting note: hammering this endpoint will get your IP temporarily
blocked (HTTP 401). Both callers throttle with delays and conservative
per-run caps. Don't call this in tight loops without a sleep.
"""

import json
import urllib.request
import urllib.error
from typing import Optional

_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
_HEADERS = {"User-Agent": _UA, "x-ig-app-id": "936619743392459"}
_ENDPOINT = "https://www.instagram.com/api/v1/users/web_profile_info/?username="


def normalize_handle(handle: str) -> str:
    """Lowercase, strip @, strip URL prefix, strip trailing slash."""
    if not handle:
        return ""
    h = handle.strip().lstrip("@").rstrip("/").lower()
    if "instagram.com/" in h:
        h = h.split("instagram.com/")[-1].split("/")[0]
    return h


def owner_from_post(post: dict) -> Optional[str]:
    """
    Extract owner username from a raw Apify post dict, trying every field
    shape the actor produces. Returns lowercase handle or None.
    """
    for key in ("ownerUsername", "username"):
        v = post.get(key)
        if v:
            return normalize_handle(v)
    for key in ("inputUrl", "url"):
        v = post.get(key)
        if v and "instagram.com/" in v:
            return normalize_handle(v)
    return None


def check_handle(handle: str, timeout: int = 15) -> dict:
    """
    Single profile lookup. Returns:
      {'status': 'real'|'missing'|'private'|'unknown'|'error',
       'followers': int|None, 'posts': int|None, 'is_private': bool}

    Status values:
      • 'real'    — profile exists and is publicly visible
      • 'missing' — IG returned 404 / "Page Not Found" (deleted or never existed)
      • 'private' — profile exists but counts not visible
      • 'unknown' — endpoint returned something we don't recognize
      • 'error'   — network / 401 rate-limit / other failure
    """
    try:
        req = urllib.request.Request(_ENDPOINT + handle, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", "ignore")

        if body.startswith("{"):
            d = json.loads(body)
            u = (d.get("data") or {}).get("user") or {}
            is_private = bool(u.get("is_private"))
            return {
                "status": "private" if is_private else "real",
                "followers": (u.get("edge_followed_by") or {}).get("count"),
                "posts": (u.get("edge_owner_to_timeline_media") or {}).get("count"),
                "is_private": is_private,
            }
        if "Page Not Found" in body:
            return {"status": "missing", "followers": None, "posts": None, "is_private": False}
        return {"status": "unknown", "followers": None, "posts": None, "is_private": False}

    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"status": "missing", "followers": None, "posts": None, "is_private": False}
        return {"status": "error", "followers": None, "posts": None, "is_private": False}
    except Exception:
        return {"status": "error", "followers": None, "posts": None, "is_private": False}
