"""Commit docs/index.html through GitHub Contents API (no git in container)."""
from __future__ import annotations

import base64
import logging
import os

import requests

log = logging.getLogger(__name__)

REPO = os.environ.get("GITHUB_REPO", "TimmyZinin/dcf-50")
TOKEN = os.environ.get("GITHUB_PAT", "")
API = f"https://api.github.com/repos/{REPO}/contents"


def put_file(path: str, content: str, message: str, branch: str = "main") -> bool:
    if not TOKEN:
        log.warning("GITHUB_PAT missing — skipping push of %s", path)
        return False

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Get current sha (if exists)
    sha = None
    r = requests.get(f"{API}/{path}", headers=headers, params={"ref": branch}, timeout=30)
    if r.status_code == 200:
        sha = r.json().get("sha")
    elif r.status_code not in (404,):
        log.warning("GET %s → %s %s", path, r.status_code, r.text[:200])

    body = {
        "message": message,
        "content": base64.b64encode(content.encode("utf-8")).decode("ascii"),
        "branch": branch,
    }
    if sha:
        body["sha"] = sha

    r = requests.put(f"{API}/{path}", headers=headers, json=body, timeout=60)
    if r.status_code in (200, 201):
        log.info("pushed %s (sha=%s)", path, r.json().get("commit", {}).get("sha", "?")[:8])
        return True
    log.error("PUT %s → %s %s", path, r.status_code, r.text[:300])
    return False
