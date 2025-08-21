from __future__ import annotations
from pathlib import Path
import re
from datetime import datetime
from zoneinfo import ZoneInfo
import hashlib


# === CONFIG ===
# Path to Original Script
TARGET_FILE = Path("MMN_WebScraper/MMN_WebScraper.py")

# Header fields to update
USER_AGENT_KEY = r'"User-Agent"'
ACCEPT_LANG_KEY = r'"Accept-Language"'

# Pools to rotate weekly (deterministic per ISO week in America/Chicago)
UA_POOL = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0',
]

ACCEPT_LANG_POOL = [
    'en-US,en;q=0.9',
    'en-US,en;q=0.8',
    'en-US,en;q=0.7',
    'en-US,en;q=0.6',
]

def week_index() -> int:
    now = datetime.now(ZoneInfo("America/Chicago"))
    token = f"{now.isocalendar().year}-{now.isocalendar().week}"
    return int(hashlib.sha256(token.encode()).hexdigest(),16)

def choose_rotations() -> tuple[str, str]:
    idx = week_index()
    return UA_POOL[idx % len(UA_POOL)], ACCEPT_LANG_POOL[idx % len(ACCEPT_LANG_POOL)]

def replace_header_value(text: str, header_json_key: str, new_value: str) -> str:
    pattern = rf'({header_json_key}\s*:\s*")[^"]*(")'
    return re.sub(pattern, rf'\1{re.escape(new_value)}\2', text)

def headers_block_present(text: str) -> bool:
    return "headers" in text and "{" in text and "}" in text

def main() -> int:
    if not TARGET_FILE.exists():
        print(f"[editor] Target file not found: {TARGET_FILE}")
        return 1
    original = TARGET_FILE.read_text(encoding="utf-8")
    if not headers_block_present(original):
        print("[editor] Could not locaterd 'headers' dict; no changes made.")
        return 0
    
    ua, al = choose_rotations()
    edited = replace_header_value(original, USER_AGENT_KEY, ua)
    edited = replace_header_value(edited, ACCEPT_LANG_KEY, al)

    if edited != original:
        TARGET_FILE.write_text(edited, encoding="utf-8")
        print(f"[editor] Updated headers:\n User-Agent -> {ua}\n Accept-Language -> {al}")
    else:
        print("[editor] No changes needed (already up to date).")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
