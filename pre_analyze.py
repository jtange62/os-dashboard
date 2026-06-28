#!/usr/bin/env python3
"""Nightly batch: pre-populate AI analysis for daily candidate rows.
Intended to run via cron at 1 AM CST so analysts see results when they start.

Selects candidate rows from Postgres and calls the local server's /api/ai-analyze endpoint.
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
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

# --- Config ---
SERVER = "http://localhost:8765"
DELAY_BETWEEN = 10         # seconds between successful calls
MAX_RETRIES = 8            # max retries on rate-limit / overload
INITIAL_BACKOFF = 30       # first retry wait (seconds)
MAX_BACKOFF = 600           # cap backoff at 10 minutes

CST = timezone(timedelta(hours=-6))
ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.json"

with CONFIG_PATH.open(encoding="utf-8") as _f:
    _CFG = json.load(_f)

DB_CONFIG = {
    **_CFG["postgres"],
    "connect_timeout": 15,
    "application_name": "os-dashboard-pre-analyze",
}


def yesterday_cst():
    return (datetime.now(CST) - timedelta(days=1)).strftime("%Y-%m-%d")


def fetch_queue(filed_at):
    with psycopg.connect(**DB_CONFIG) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT document_url, ticker, date_final, filed_at, ai_analysis
            FROM historical_float_final
            WHERE filed_at = %s
              AND (
                  outstanding_shares_final IS NOT NULL
                  OR float_final IS NOT NULL
                  OR tradable_float_final IS NOT NULL
              )
            ORDER BY date_final DESC NULLS LAST, filed_at DESC NULLS LAST, document_url
            """,
            (filed_at,),
        )
        rows = cur.fetchall()
    return {"status": "success", "results": rows}


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


def process_date(filed_at):
    """Fetch daily AI candidates for a single date and analyze any un-analyzed rows."""
    log(f"Processing filed_at={filed_at}")

    data = fetch_queue(filed_at)
    if data.get("status") != "success":
        log(f"Failed to fetch queue: {data}")
        return

    rows = data.get("results", [])
    log(f"Found {len(rows)} rows in daily AI candidates")

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

    log(f"Done filed_at={filed_at}. analyzed={analyzed} cached={cached} errors={errors} total={len(rows)}")


def main():
    if len(sys.argv) > 1:
        # Manual run: process specific date(s) passed as arguments
        for d in sys.argv[1:]:
            process_date(d)
    else:
        # Cron run: process yesterday and 2 days ago (catches late arrivals)
        now = datetime.now(CST)
        for days_back in (1, 2):
            filed_at = (now - timedelta(days=days_back)).strftime("%Y-%m-%d")
            process_date(filed_at)


if __name__ == "__main__":
    main()
