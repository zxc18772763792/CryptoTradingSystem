"""Store the X session token in .env.local without it appearing on screen or in chat.

Run this in your own terminal and paste the auth_token cookie of the dedicated X
account at the hidden prompt. It sets X_AUTH_TOKEN and NEWS_ENABLE_X_ACCOUNTS=true
in .env.local (gitignored), keeps every other line as it was, and prints only the
token's length. Nothing is sent anywhere.
"""
from __future__ import annotations

import getpass
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ".env.local"
_BOM = b"\xef\xbb\xbf"
_TOKEN_RE = re.compile(r"[A-Za-z0-9%._~+/-]{20,256}\Z")


def normalize_token(raw: str) -> str:
    token = str(raw or "").strip().strip("\"'")
    if token.lower().startswith("auth_token="):
        token = token.split("=", 1)[1].strip().strip("\"'")
    if not _TOKEN_RE.fullmatch(token):
        raise ValueError("that does not look like an auth_token cookie value (expected one 20+ character string)")
    return token


def update_env_text(text: str, values: Dict[str, str]) -> str:
    """Set each name to its value, replacing every active assignment and appending the missing ones."""
    newline = "\r\n" if "\r\n" in text else "\n"
    seen = set()
    lines = []
    for line in text.splitlines():
        name = line.split("=", 1)[0].strip()
        if "=" in line and not line.lstrip().startswith("#") and name in values:
            lines.append(f"{name}={values[name]}")
            seen.add(name)
        else:
            lines.append(line)
    lines.extend(f"{name}={value}" for name, value in values.items() if name not in seen)
    return newline.join(lines) + newline


def write_env_file(path: Path, values: Dict[str, str]) -> None:
    raw = path.read_bytes() if path.exists() else b""
    bom = raw.startswith(_BOM)
    text = update_env_text(raw[len(_BOM):].decode("utf-8") if bom else raw.decode("utf-8"), values)
    temp = path.with_name(path.name + ".tmp")
    temp.write_bytes((_BOM if bom else b"") + text.encode("utf-8"))
    os.replace(temp, path)


def _is_gitignored() -> bool:
    try:
        return subprocess.run(["git", "check-ignore", "-q", ENV_FILE], cwd=ROOT).returncode == 0
    except OSError:
        return False


def main() -> int:
    if not _is_gitignored():
        print(f"Refusing to write: {ENV_FILE} is not confirmed as gitignored, so the token could be committed.",
              file=sys.stderr)
        return 1
    try:
        token = normalize_token(getpass.getpass("X auth_token (hidden, then Enter): "))
    except ValueError as exc:
        print(f"Nothing written: {exc}", file=sys.stderr)
        return 1
    write_env_file(ROOT / ENV_FILE, {"X_AUTH_TOKEN": token, "NEWS_ENABLE_X_ACCOUNTS": "true"})
    print(f"Saved X_AUTH_TOKEN ({len(token)} characters) and NEWS_ENABLE_X_ACCOUNTS=true to {ENV_FILE}.")
    print("Next: fill x_accounts_handles in config/news_rules.yaml, then run scripts/probe_x_accounts.py <handle>.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
