"""macOS notifications: `osascript` in a child process, so nothing waits on it."""

from __future__ import annotations

import json
import logging
import shutil
import subprocess

log = logging.getLogger(__name__)


def notify(title: str, text: str) -> None:
    """Show a notification; logged either way, silently skipped where `osascript` doesn't exist."""
    log.warning("ALERT %s: %s", title, text)
    if shutil.which("osascript") is None:
        return
    script = f"display notification {json.dumps(text[:240])} with title {json.dumps(title[:60])}"
    try:
        subprocess.Popen(["osascript", "-e", script], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True)
    except OSError as e:
        log.warning("couldn't show a notification: %s", e)
