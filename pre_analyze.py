#!/usr/bin/env python3
"""Nightly batch: pre-populate AI analysis for today's review queue rows.
Intended to run via cron at 1 AM CST so analysts see results when they start.

Calls the local server's /api/review and /api/ai-analyze endpoints.
Skips rows that already have an ai_analysis (the endpoint returns cached=True).

Rate-limit handling:
  - Waits DELAY_BETWEEN seconds between requests.
  - On HTTP 429 (rate limited) or 529 (overloaded), retries with exponential
    backoff up to MAX_RETRIES times.
  - On any other error, logs and moves to the next row.
"""

import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

# --- Config ---
SERVER = "http://localhost:8765"
DELAY_BETWEEN = 10         # seconds between successful calls
MAX_RETRIES = 8            # max retries on rate-limit / overload
INITIAL_BACKOFF = 30       # first retry wait (seconds)
MAX_BACKOFF = 600           # cap backoff at 10 minutes

CST = timezone(timedelta(hours=-6))


def yesterday_cst():
    return (datetime.now(CST) - timedelta(days=1)).strftime("%Y-%m-%d")


def fetch_queue(filed_at):
    url = f"{SERVER}/api/review?filed_at={filed_at}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def analyze(document_url, attempt=0):
    """Call the AI analyze endpoint. Returns (status, cached, error_msg)."""
    body = json.dumps({"document_url": document_url}).encode("utf-8")
    req = urllib.request.Request(
        f"{SERVER}/api/ai-analyze",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=180) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return (resp.status, data.get("cached", False), None)
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")[:500]
        is_rate_limit = e.code in (429, 529) or (e.code == 502 and "rate_limit" in err_body)
        if is_rate_limit and attempt < MAX_RETRIES:
            # Rate limited or overloaded -- back off and retry
            backoff = min(INITIAL_BACKOFF * (2 ** attempt), MAX_BACKOFF)
            # Check for Retry-After header
            retry_after = e.headers.get("Retry-After")
            if retry_after:
                try:
                    backoff = max(backoff, int(retry_after))
                except ValueError:
                    pass
            log(f"  Rate limited ({e.code}), retry {attempt + 1}/{MAX_RETRIES} in {backoff}s...")
            time.sleep(backoff)
            return analyze(document_url, attempt + 1)
        return (e.code, False, err_body[:200])
    except Exception as e:
        return (0, False, str(e))


def log(msg):
    ts = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def main():
    filed_at = sys.argv[1] if len(sys.argv) > 1 else yesterday_cst()
    log(f"Starting batch AI analysis for filed_at={filed_at}")

    data = fetch_queue(filed_at)
    if data.get("status") != "success":
        log(f"Failed to fetch queue: {data}")
        sys.exit(1)

    rows = data.get("results", [])
    log(f"Found {len(rows)} rows in daily queue")

    analyzed = 0
    cached = 0
    errors = 0

    for i, row in enumerate(rows):
        url = row.get("document_url")
        ticker = row.get("ticker", "?")
        if not url:
            continue

        log(f"[{i + 1}/{len(rows)}] {ticker} - {url[:80]}...")
        status, was_cached, err = analyze(url)

        if err:
            log(f"  ERROR ({status}): {err}")
            errors += 1
        elif was_cached:
            log(f"  cached (already analyzed)")
            cached += 1
        else:
            log(f"  analyzed OK")
            analyzed += 1

        # Delay between requests (skip delay for cached results)
        if not was_cached and i < len(rows) - 1:
            time.sleep(DELAY_BETWEEN)

    log(f"Done. analyzed={analyzed} cached={cached} errors={errors} total={len(rows)}")


if __name__ == "__main__":
    main()
