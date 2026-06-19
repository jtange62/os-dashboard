"""OS Dashboard - local server with API proxy + Postgres-backed review queue.

Run:  python server.py
Then open http://localhost:8765/ in a browser.

Configuration is loaded from `config.json` next to this file. See
`config.example.json` for the expected shape.
"""
import datetime
import decimal
import gzip
import http.server
import json
import socketserver
import sys
import urllib.error
import urllib.parse
import urllib.request
import zlib
import zoneinfo
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

ROOT = Path(__file__).parent
CONFIG_PATH = ROOT / "config.json"

if not CONFIG_PATH.exists():
    sys.stderr.write(
        f"Missing {CONFIG_PATH.name}. Copy config.example.json to config.json "
        "and fill in your credentials.\n"
    )
    sys.exit(1)

with CONFIG_PATH.open(encoding="utf-8") as _f:
    _CFG = json.load(_f)

ASKEDGAR_KEY = _CFG["askedgar_api_key"]
SECAPI_KEY = _CFG["secapi_io_key"]
ANTHROPIC_KEY = _CFG.get("anthropic_api_key", "")
ELASTIC_URL = (_CFG.get("elastic_url") or "").rstrip("/")
ELASTIC_KEY = _CFG.get("elastic_api_key", "")
ELASTIC_INDEX = _CFG.get("elastic_filings_index", "stock_filings")
PORT = int(_CFG.get("port", 8765))

ASKEDGAR_BASE = "https://eapi.askedgar.io"
SECAPI_URL = "https://api.sec-api.io"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-opus-4-7"
# Cheap/fast model for pre-extracting relevant excerpts from long filings.
EXTRACT_MODEL = "claude-haiku-4-5-20251001"

# Filing-text handling (1 token ~= 4 chars for English prose):
#   - Pull up to FILING_FETCH_MAX_CHARS from Elasticsearch (cap to bound memory + extractor context).
#   - If the text is longer than FILING_EXCERPT_THRESHOLD_CHARS (~15k tokens), run a cheap-model
#     pre-extraction to pull only the share-relevant excerpts; otherwise pass the full text through.
#   - If extraction finds nothing, fall back to a hard truncation of FILING_TRUNCATE_CHARS.
FILING_FETCH_MAX_CHARS = 700000          # ~175k tokens — safely under Haiku's 200k context
FILING_EXCERPT_THRESHOLD_CHARS = 60000   # ~15k tokens
FILING_TRUNCATE_CHARS = 20000            # fallback when pre-extraction yields nothing
# Back-compat alias (kept so existing references don't break).
FILING_TEXT_MAX_CHARS = FILING_TRUNCATE_CHARS

DB_CONFIG = {
    **_CFG["postgres"],
    "connect_timeout": 15,
    "application_name": "os-dashboard",
}

CHICAGO = zoneinfo.ZoneInfo("America/Chicago")

# Editable columns in the Review Queue. Server-side allowlist.
EDITABLE_COLUMNS = {
    "outstanding_shares": "numeric",
    "outstanding_shares_reported": "numeric",
    "float": "numeric",
    "tradable_float": "numeric",
    "affiliate_shares": "numeric",
    "restricted_shares": "numeric",
    "ads_ratio": "numeric",
    "is_adr": "boolean",
    "notes": "text",
    "ai_analysis": "text",
}

HISTORICAL_COLUMNS = """
    document_url, ticker, filed_at, date_final, form_type,
    outstanding_shares, outstanding_shares_final, outstanding_shares_reported,
    float, float_final,
    tradable_float, tradable_float_final,
    test_outstanding, test_float,
    restricted_shares, affiliate_shares, five_percent_shareholders, insider_shares,
    affiliate_percent, insider_percent, institutions_percent,
    is_adr, ads_ratio, foreign_company, number_classes, classes_details,
    ownership_source, source_date, details, category, market_cap, cik,
    notes, ai_analysis, created_at, updated_at
"""

REVIEW_COLUMNS = """
    hf.document_url,
    hf.filed_at,
    hf.ticker,
    hf.form_type,
    hf.outstanding_shares,
    hf.outstanding_shares_reported,
    hf.outstanding_shares_final,
    hf.float,
    hf.float_final,
    hf.tradable_float,
    hf.tradable_float_final,
    hf.test_outstanding,
    hf.test_float,
    hf.restricted_shares,
    hf.affiliate_shares,
    hf.date_final,
    hf.ads_ratio,
    hf.category,
    hf.prompt,
    hf.details,
    hf.is_adr,
    hf.foreign_company,
    hf.number_classes,
    hf.classes_details,
    hf.ownership_source,
    hf.notes,
    hf.ai_analysis,
    hf.market_cap,
    hf.updated_at,
    (SELECT MAX(h2.date_final) FROM historical_float_final h2
       WHERE h2.ticker = hf.ticker
         AND (h2.form_type IS NULL OR h2.form_type NOT LIKE '%%MANUAL%%')
         AND (h2.outstanding_shares_final IS NOT NULL
              OR h2.float_final IS NOT NULL
              OR h2.tradable_float_final IS NOT NULL)
    ) AS ticker_max_date_final
"""

EXCHANGE_JOIN = """
    INNER JOIN screener s ON s.ticker = hf.ticker
        AND s.exchange IN ('NASDAQ', 'NYSE', 'AMEX')
"""

REVIEW_QUERY = f"""
    SELECT {REVIEW_COLUMNS}
    FROM historical_float_final hf
    LEFT JOIN screener s ON s.ticker = hf.ticker
        AND s.exchange IN ('NASDAQ', 'NYSE', 'AMEX')
    WHERE
        hf.filed_at = %(filed_at)s
        AND (
            -- Share-issuance path: always show if details flags a share issuance,
            -- regardless of exchange / category / value thresholds.
            hf.details ILIKE '%%Share issuance via%%'
            -- Test-value diff path: bypass every other filter when the
            -- secondary extractor disagrees with our finals by >20 percent.
            OR (
                hf.test_outstanding IS NOT NULL
                AND hf.outstanding_shares_final IS NOT NULL
                AND hf.outstanding_shares_final <> 0
                AND ABS(hf.test_outstanding - hf.outstanding_shares_final)
                    > 0.20 * ABS(hf.outstanding_shares_final)
            )
            OR (
                hf.test_float IS NOT NULL
                AND hf.tradable_float_final IS NOT NULL
                AND hf.tradable_float_final <> 0
                AND ABS(hf.test_float - hf.tradable_float_final)
                    > 0.20 * ABS(hf.tradable_float_final)
            )
            -- Missing-OS path: outstanding_shares_final is null but float data exists,
            -- so the row is reviewable and the OS likely failed extraction.
            OR (
                hf.outstanding_shares_final IS NULL
                AND (hf.float_final IS NOT NULL OR hf.tradable_float_final IS NOT NULL)
                AND hf.form_type NOT LIKE '%%MANUAL%%'
                AND hf.ticker IS NOT NULL
            )
            OR (
                hf.form_type NOT LIKE '%%MANUAL%%'
                AND hf.ticker IS NOT NULL
                AND (
                    -- Sub-2M finals bypass category AND exchange filters (most critical)
                    hf.outstanding_shares_final < 2000000
                    OR hf.float_final < 2000000
                    OR hf.tradable_float_final < 2000000
                    -- Normal path: must be on major exchange + category match + under-10M or ADR
                    OR (
                        s.ticker IS NOT NULL
                        AND hf.category ~ 'NEGATIVE_FLOAT|HIGH|CRITICAL|SHARES_DECREASED|SHARES_MISMATCH'
                        AND (
                            hf.outstanding_shares_final < 10000000
                            OR hf.float_final < 10000000
                            OR hf.tradable_float_final < 10000000
                            OR hf.ticker IN (
                                SELECT DISTINCT ticker FROM historical_float_final WHERE is_adr = true
                            )
                        )
                    )
                )
            )
        )
    ORDER BY hf.date_final DESC
    LIMIT 1000
"""

# Ticker mode: full history for a single ticker, no category/date filter,
# so the analyst can fix older records they spot while researching.
REVIEW_BY_TICKER_QUERY = f"""
    SELECT {REVIEW_COLUMNS}
    FROM historical_float_final hf
    WHERE hf.ticker = %(ticker)s
        AND (hf.form_type IS NULL OR hf.form_type NOT LIKE '%%MANUAL%%')
        AND (hf.outstanding_shares_final IS NOT NULL
             OR hf.float_final IS NOT NULL
             OR hf.tradable_float_final IS NOT NULL)
    ORDER BY hf.date_final DESC NULLS LAST, hf.filed_at DESC NULLS LAST
    LIMIT 1000
"""

# Low-float mode: records where float_final or tradable_float_final < 300k
REVIEW_LOW_FLOAT_QUERY = f"""
    SELECT {REVIEW_COLUMNS}
    FROM historical_float_final hf
    {EXCHANGE_JOIN}
    WHERE (hf.float_final < 300000 OR hf.tradable_float_final < 300000)
        AND hf.float_final IS NOT NULL
        AND hf.ticker IS NOT NULL
        AND (hf.ai_analysis IS NULL OR hf.ai_analysis NOT LIKE 'CORRECTED%%')
        AND COALESCE(s.industry, '') <> 'Shell Companies'
        AND COALESCE(s.companyname, '') NOT ILIKE '%%Acquisition Corp%%'
    ORDER BY hf.filed_at DESC NULLS LAST
    LIMIT 50
"""

# ADR mode: rows flagged as ADR (by either source). No float cap; same exclusions
# as low-float (major exchange, not CORRECTED, not Shell / Acquisition Corp).
REVIEW_ADR_QUERY = f"""
    SELECT {REVIEW_COLUMNS}
    FROM historical_float_final hf
    {EXCHANGE_JOIN}
    WHERE (hf.is_adr = true OR s.isadr = true)
        AND hf.ticker IS NOT NULL
        AND (hf.outstanding_shares_final IS NOT NULL
             OR hf.float_final IS NOT NULL
             OR hf.tradable_float_final IS NOT NULL)
        AND (hf.ai_analysis IS NULL OR hf.ai_analysis NOT LIKE 'CORRECTED%%')
        AND COALESCE(s.industry, '') <> 'Shell Companies'
        AND COALESCE(s.companyname, '') NOT ILIKE '%%Acquisition Corp%%'
    ORDER BY hf.filed_at DESC NULLS LAST
    LIMIT 50
"""


# Shared output format — MUST stay identical across prompts so the UI's TIER parsing works.
AI_OUTPUT_FORMAT = (
    "=== OUTPUT FORMAT (follow exactly) ===\n"
    "Use this exact structure with plain text (no markdown headers, no bold):\n\n"
    "TIER: <one of the following>\n"
    "  CRITICAL - negative float/tradable_float, or float < 200k\n"
    "  DISCREPANCY - 30%+ change from previous record not explained by a split\n"
    "  MINOR - small discrepancy under ~20%, or cosmetic issue\n"
    "  NO CHANGE NECESSARY - values look correct\n\n"
    "EXPLANATION: 2-4 sentences explaining the issue (or confirming values are fine). Be concise.\n\n"
    "RECOMMENDED CHANGES:\n"
    "If changes are needed, list each field that should change as a bullet:\n"
    "- outstanding_shares / outstanding_shares_reported: <value>\n"
    "- affiliate_shares: <value>\n"
    "- restricted_shares: <value>\n"
    "- float: <value>\n"
    "- ads_ratio: <value>   (for ADR rows missing or with a wrong ratio)\n"
    "(only list fields that need changing)\n"
    "If no changes are needed, write: No changes recommended.\n\n"
    "Keep the whole output compact -- this displays inside a small side panel."
)

# Governing rule shared by EVERY prompt: values must reflect the state AS OF date_final.
AS_OF_PRINCIPLE = (
    "=== GOVERNING RULE: VALUES MUST MATCH date_final (THE 'AS OF' DATE) ===\n"
    "The share counts on this row must reflect the company's ACTUAL outstanding / float / etc. AS OF date_final "
    "(the reported 'as of' date). date_final is the date of record and GOVERNS -- regardless of how the source "
    "document frames itself. A filing may describe a 'pre-offering' snapshot, say 'shares outstanding prior to "
    "this offering', or use a cover-page convenience date; IGNORE that framing and ask only: what were the share "
    "counts AS OF date_final?\n"
    "- If a corporate event (offering / share issuance, debt conversion, warrant exercise, buyback, split) took "
    "effect ON OR BEFORE date_final, its effect MUST already be reflected in the values. If the row shows "
    "PRE-event values while date_final is on/after the event, that is wrong -> DISCREPANCY (or CRITICAL): "
    "recommend the corrected post-event values.\n"
    "- If the event took effect AFTER date_final, then the pre-event values ARE correct for this row.\n"
    "- DO NOT excuse an as-of mismatch by saying the correct post-event value is captured in a different/separate "
    "record. Each row must be correct as of ITS OWN date_final; another record existing does not make this one "
    "right.\n"
    "- There are only two self-consistent states: (a) date_final is BEFORE the event AND values are pre-event, or "
    "(b) date_final is ON/AFTER the event AND values are post-event. If you see pre-event values with an "
    "on/after date_final (or vice-versa), call out the inconsistency explicitly and state the fix -- either the "
    "share amounts should be updated to match date_final, or date_final should be moved to match the values.\n\n"
)

# Prompt for non-ADR rows.
STANDARD_AI_PROMPT = (
    "You are a data analyst at AskEdgar reviewing a single historical_float_final row. "
    "Your job: tell the analyst whether the OS / float values look correct, or flag possible reasons they may be wrong.\n\n"
    "When SOURCE FILING TEXT is provided, it is the most authoritative input -- it is the actual filing text "
    "the OS / float values were extracted from. Cross-check the row's outstanding_shares against numbers in "
    "the filing text and call out any mismatch (wrong number picked, share class confused, percent vs absolute, etc.).\n\n"
    "Things to look for:\n"
    "- float_final or tradable_float_final < 500k or negative -> often means affiliate_shares or restricted_shares is wrong. "
    "Check recent offerings + news for share issuances that should have been counted as restricted.\n"
    "- A big drop in OS or float vs the previous historical record may be normal if a reverse split happened around that date. "
    "Confirm by checking the most recent reverse split.\n"
    "- CRITICAL split logic: compare the row's date_final (the 'as of' / reported date) to the split's execution_date. "
    "If date_final is BEFORE the split execution_date, ALL share counts on this row MUST be pre-split values. "
    "If any field (affiliate_shares, insider_shares, five_percent_shareholders, outstanding_shares, etc.) appears to be "
    "post-split while date_final is pre-split, give a clear directive: 'Analyst should multiply [field] by [split_from/split_to] "
    "to convert back to pre-split values.' Do NOT present this as optional or offer the reverse alternative. "
    "The rule is absolute: date_final before split -> all values must be pre-split. "
    "Only if date_final is AFTER the split execution_date should the numbers reflect post-split values.\n"
    "- For split-affected rows: verify the split actually occurred. Post-split price should usually be > $1, "
    "and recent news should mention the effective date.\n"
    "- If outstanding_final is very low, check whether outstanding_shares (raw) and outstanding_shares_reported diverge sharply, "
    "or whether the AI may have extracted the wrong number from the source filing -- the FILING TEXT will tell you.\n"
    "- If this looks like an ADR / foreign issuer (ADSs mentioned in the filing) but is_adr is not set, note that the row "
    "may need is_adr and ads_ratio populated and should be reviewed as an ADR.\n"
    "- Most important red flags: OS or float < 500k (especially < 100k or negative). "
    "Negatives are almost always wrong, unless the company is a brand-new SPAC or similar.\n"
    "- MISSING VALUES: if any of outstanding_shares, float, or tradable_float on the row is null/missing, "
    "try to determine the correct value from the FILING TEXT, ownership data, offerings, and recent historical "
    "records. Include your best determination as a bullet in RECOMMENDED CHANGES. If the available context is "
    "insufficient to determine it, say so explicitly in the EXPLANATION rather than guessing.\n"
    "- CRITICAL: the SCREENER block is today's current values, not truth for this row. "
    "Do NOT recommend changes by comparing the row to screener -- the row may legitimately be more "
    "current than the screener (it can reflect a brand-new issuance from its source filing) OR "
    "the row may be historical (date_final in the past). For a historical date_final, the question "
    "is 'were these values correct AS OF date_final', so the screener's current snapshot is largely "
    "irrelevant. Use the screener only as a rough magnitude sanity check (e.g. an obviously-wrong "
    "10x discrepancy in either direction); do not flag a discrepancy 'because screener says X'.\n\n"
    + AS_OF_PRINCIPLE
    + AI_OUTPUT_FORMAT
)

# Prompt for ADR rows (is_adr true on the row OR on the screener). ADR-specific concerns lead.
ADR_AI_PROMPT = (
    "You are a data analyst at AskEdgar reviewing a single historical_float_final row for an ADR / ADS-listed company "
    "(a foreign issuer trading in the US via American Depositary Shares). Your job: tell the analyst whether the "
    "OS / float values look correct, with SPECIAL ATTENTION to ADR-specific pitfalls.\n\n"
    "When SOURCE FILING TEXT is provided, it is the most authoritative input -- the actual filing the values were "
    "extracted from. Cross-check the row's numbers against it.\n\n"
    "ADR-SPECIFIC CHECKS (these are the priority):\n"
    "1. ORDINARY SHARES, NOT ADSs: outstanding_shares, outstanding_shares_reported, float, and tradable_float MUST be "
    "expressed in the underlying ORDINARY (common) shares, NEVER in ADSs/ADRs. Foreign filings (20-F, 6-K, F-1) routinely "
    "report ADS counts. If the row's values match an 'ADSs outstanding' figure in the FILING TEXT, or look far too small "
    "versus the ordinary-share count implied by market cap / price, the wrong unit was captured. This is a serious data "
    "error -> tier CRITICAL. Give the corrected ordinary-share value (ordinary = ADS_count * ads_ratio) in RECOMMENDED CHANGES.\n"
    "2. ads_ratio IS REQUIRED: ads_ratio = number of ordinary shares per 1 ADS. It must be present for an ADR row. "
    "If ads_ratio is null/blank, FLAG IT (tier at least DISCREPANCY) -- FIRST check the recent HISTORICAL FLOAT RECORDS "
    "below: if a recent prior record for this ticker has an ads_ratio, that value is the strongest candidate (confirm it "
    "against the FILING TEXT if possible) -- recommend it. Otherwise determine it from the FILING TEXT or deposit-agreement "
    "language (e.g. 'each ADS represents 10 ordinary shares' -> ads_ratio=10) and put it in RECOMMENDED CHANGES. If it cannot "
    "be determined from any source, say so explicitly in the EXPLANATION (do NOT guess a ratio). "
    "If this row's ads_ratio DIFFERS from recent prior records with no corporate action (e.g. ratio change / split) "
    "explaining it, flag that as suspicious.\n"
    "3. RECONCILE: outstanding_shares (ordinary) should roughly equal the reported ADSs * ads_ratio. If they don't "
    "reconcile, explain the discrepancy and recommend the corrected value. Also compare this row's share counts to the "
    "recent HISTORICAL FLOAT RECORDS and the current SCREENER -- an order-of-magnitude jump/drop versus recent ordinary-share "
    "counts often means ADSs were captured instead of ordinary shares (or vice versa).\n\n"
    "ALSO CHECK (general):\n"
    "- Negative or very low float/tradable_float (< 200k, especially < 100k or negative) is almost always wrong -- usually "
    "an affiliate_shares / restricted_shares error. Check offerings + news for issuances.\n"
    "- Reverse split vs date_final: compare date_final (the 'as of' date) to the split execution_date. If date_final is "
    "BEFORE the split, ALL share counts MUST be pre-split; if a field looks post-split, direct the analyst to multiply by "
    "[split_from/split_to] to convert back. The rule is absolute -- do not offer the reverse.\n"
    "- MISSING VALUES: if outstanding_shares, float, or tradable_float is null, try to determine it from the filing text / "
    "ownership / offerings / historical records and put it in RECOMMENDED CHANGES; if not determinable, say so.\n"
    "- SCREENER is today's snapshot, NOT truth for a historical row. Use it only as a rough magnitude sanity check; for a "
    "historical date_final, judge against what was true then (filing, contemporaneous offerings/news, adjacent records).\n\n"
    + AS_OF_PRINCIPLE
    + AI_OUTPUT_FORMAT
)

# Body of the share-issuance prompt (output format appended at call time, after an optional ADR note).
SHARE_ISSUANCE_BODY = (
    "You are a data analyst at AskEdgar reviewing a historical_float_final row that represents a SHARE ISSUANCE "
    "EVENT (the row's details/category indicate a share issuance). The source document is about the issuance "
    "itself -- a DEBT CONVERSION, PRIVATE PLACEMENT, REGISTERED OFFERING, WARRANT EXERCISE, or SHARE ISSUANCE FOR "
    "ACQUISITION -- and does NOT necessarily state the company's total outstanding share count.\n\n"
    "HOW THESE ROWS ARE BUILT: the newly issued shares are ADDED to the most recently reported outstanding share "
    "count. The correct outstanding_shares for this row should equal:\n"
    "    (most recent PRIOR outstanding shares) + (shares issued in this event).\n"
    "Your job is to verify that math and flag problems.\n\n"
    "STEPS:\n"
    "1. ISSUED SHARES: determine the number of shares issued in this event from the SOURCE FILING and RECENT "
    "OFFERINGS (e.g. shares sold in the offering, shares issued on conversion, warrant shares exercised, shares "
    "issued for the acquisition).\n"
    "2. PRIOR OUTSTANDING: take the most recent prior outstanding share count from the HISTORICAL FLOAT RECORDS "
    "block (our authoritative computed _final values from Postgres), using the latest record dated before this "
    "row's date_final.\n"
    "3. RECONCILE: confirm this row's outstanding_shares ~= prior outstanding + issued. If it doesn't reconcile, "
    "explain and give the corrected value in RECOMMENDED CHANGES.\n"
    "4. PREFUNDED WARRANTS: prefunded warrants are normally counted as outstanding shares (they are typically "
    "exercised immediately for nominal consideration). HOWEVER, if the prefunded warrants in this issuance are a "
    "SIGNIFICANT amount -- more than 20% of the outstanding shares -- FLAG that they should NOT be included in the "
    "outstanding share count, and give the outstanding figure excluding them.\n"
    "5. RESTRICTED SHARES: if this issuance is a PRIVATE PLACEMENT or a SHARE ISSUANCE FOR ACQUISITION, the issued "
    "shares are RESTRICTED. The row's restricted_shares should equal (prior restricted_shares from the HISTORICAL "
    "FLOAT RECORDS block) + (shares issued). If restricted_shares was not updated accordingly, recommend the corrected value. "
    "For a REGISTERED OFFERING, WARRANT EXERCISE, or DEBT CONVERSION, do NOT add the issued shares to "
    "restricted_shares unless the filing explicitly states the shares are restricted / unregistered.\n"
    "6. TIMING: if the issuance will occur AFTER this row's date_final (e.g. 'shares to be outstanding after the "
    "offering' where the offering closes later), the pre-issuance outstanding count is correct for this row -- do "
    "NOT add shares that have not been issued yet. Likewise avoid double-counting an issuance already reflected in "
    "a prior record.\n\n"
    "Also apply the general checks: negative/very-low float is almost always wrong; respect reverse-split vs "
    "date_final pre/post-split logic; the SCREENER is only a current snapshot, not truth for a historical row.\n\n"
)

# Short ADR addendum appended to the share-issuance prompt when the issuer is an ADR.
SHARE_ISSUANCE_ADR_NOTE = (
    "ADR NOTE (this issuer is an ADR): express ALL share counts in ORDINARY shares, not ADSs. If the issuance is "
    "stated in ADSs, convert with ads_ratio (ordinary = ADSs * ads_ratio) BEFORE adding to the prior ordinary-share "
    "count. ads_ratio must be present; if it is missing, flag it and recommend a value.\n\n"
)


def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def get_conn():
    return psycopg.connect(**DB_CONFIG)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if url.path.startswith("/api/"):
            self._handle_api_get(url)
            return
        if url.path == "/":
            self.path = "/dashboard.html"
        super().do_GET()

    def do_POST(self):
        url = urllib.parse.urlparse(self.path)
        try:
            if url.path == "/api/filings":
                self._handle_filings_post()
            elif url.path == "/api/review/by-document-urls":
                self._handle_review_by_urls()
            elif url.path == "/api/review/update":
                self._handle_review_update()
            elif url.path == "/api/ai-analyze":
                self._handle_ai_analyze()
            else:
                self.send_error(404, "Not Found")
        except Exception as e:
            self._send_json(500, {"status": "error", "error": {"code": "server_error", "message": str(e)}})

    def _handle_api_get(self, url):
        params = urllib.parse.parse_qs(url.query, keep_blank_values=True)
        path = url.path

        askedgar_routes = {
            "/api/historical-float": "/v1/historical-float-pro",
            "/api/ownership": "/v1/ownership",
            "/api/offerings": "/v1/offerings",
            "/api/reverse-splits": "/v1/reverse-splits",
            "/api/news-basic": "/v1/news-basic",
            "/api/screener": "/v1/screener",
            "/api/float-outstanding": "/v1/float-outstanding",
        }
        if path in askedgar_routes:
            flat = [(k, v) for k, vs in params.items() for v in vs]
            qs = urllib.parse.urlencode(flat)
            target = f"{ASKEDGAR_BASE}{askedgar_routes[path]}?{qs}"
            self._forward(urllib.request.Request(target, headers={"API-KEY": ASKEDGAR_KEY}))
            return

        try:
            if path == "/api/historical-postgres":
                self._handle_historical_postgres(params)
                return
            if path == "/api/restricted-shares":
                self._handle_restricted_shares(params)
                return
            if path == "/api/review":
                self._handle_review(params)
                return
        except Exception as e:
            self._send_json(
                500,
                {"status": "error", "error": {"code": "db_error", "message": str(e)}},
            )
            return

        self.send_error(404, "Not Found")

    def _handle_historical_postgres(self, params):
        ticker = (params.get("ticker", [""])[0] or "").upper().strip()
        if not ticker:
            self._send_json(400, {"status": "error", "error": {"code": "missing_ticker", "message": "ticker required"}})
            return
        sql = f"SELECT {HISTORICAL_COLUMNS} FROM historical_float_final " \
              f"WHERE ticker = %s ORDER BY date_final DESC NULLS LAST, filed_at DESC NULLS LAST LIMIT 200"
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (ticker,))
            rows = cur.fetchall()
        self._send_json(200, {"status": "success", "count": len(rows), "results": rows})

    def _handle_restricted_shares(self, params):
        ticker = (params.get("ticker", [""])[0] or "").upper().strip()
        if not ticker:
            self._send_json(400, {"status": "error", "error": {"code": "missing_ticker", "message": "ticker required"}})
            return
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT document_url, restricted_shares FROM historical_float_final "
                "WHERE ticker = %s AND document_url IS NOT NULL",
                (ticker,),
            )
            rows = cur.fetchall()
        # Map document_url -> restricted_shares (string for Decimal-safe transit)
        mapping = {url: (str(val) if val is not None else None) for url, val in rows}
        self._send_json(200, {"status": "success", "count": len(mapping), "results": mapping})

    def _handle_review(self, params):
        mode = (params.get("mode", [""])[0] or "").strip()
        ticker = (params.get("ticker", [""])[0] or "").strip().upper()

        if mode == "lowfloat":
            with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(REVIEW_LOW_FLOAT_QUERY)
                rows = cur.fetchall()
            self._send_json(
                200,
                {
                    "status": "success",
                    "mode": "lowfloat",
                    "count": len(rows),
                    "results": rows,
                },
            )
            return

        if mode == "adr":
            with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(REVIEW_ADR_QUERY)
                rows = cur.fetchall()
            self._send_json(
                200,
                {
                    "status": "success",
                    "mode": "adr",
                    "count": len(rows),
                    "results": rows,
                },
            )
            return

        if ticker:
            with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(REVIEW_BY_TICKER_QUERY, {"ticker": ticker})
                rows = cur.fetchall()
            self._send_json(
                200,
                {
                    "status": "success",
                    "mode": "ticker",
                    "ticker": ticker,
                    "count": len(rows),
                    "results": rows,
                },
            )
            return

        filed_at_raw = (params.get("filed_at", [""])[0] or "").strip()
        if filed_at_raw:
            filed_at = datetime.date.fromisoformat(filed_at_raw)
        else:
            filed_at = datetime.datetime.now(CHICAGO).date() - datetime.timedelta(days=1)

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(REVIEW_QUERY, {"filed_at": filed_at})
            rows = cur.fetchall()

        self._send_json(
            200,
            {
                "status": "success",
                "mode": "date",
                "filed_at": filed_at.isoformat(),
                "count": len(rows),
                "results": rows,
            },
        )

    def _handle_review_by_urls(self):
        body = self._read_json_body()
        urls = body.get("document_urls") or []
        if not isinstance(urls, list) or not urls:
            self._send_json(200, {"status": "success", "count": 0, "results": []})
            return
        urls = [str(u) for u in urls if u]

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {REVIEW_COLUMNS} FROM historical_float_final hf "
                "WHERE hf.document_url = ANY(%s) ORDER BY hf.date_final DESC NULLS LAST",
                (urls,),
            )
            rows = cur.fetchall()
        self._send_json(200, {"status": "success", "count": len(rows), "results": rows})

    def _handle_review_update(self):
        body = self._read_json_body()
        document_url = body.get("document_url")
        updates = body.get("updates") or {}
        if not document_url:
            self._send_json(400, {"status": "error", "error": {"code": "missing_pk", "message": "document_url required"}})
            return
        if not isinstance(updates, dict) or not updates:
            self._send_json(400, {"status": "error", "error": {"code": "no_updates", "message": "updates required"}})
            return

        sets = []
        values = []
        for col, raw in updates.items():
            if col not in EDITABLE_COLUMNS:
                self._send_json(400, {"status": "error", "error": {"code": "field_not_editable", "message": col}})
                return
            kind = EDITABLE_COLUMNS[col]
            value = self._coerce(raw, kind)
            sets.append(f"{col} = %s")
            values.append(value)
        sets.append("updated_at = NOW()")
        sql = f"UPDATE historical_float_final SET {', '.join(sets)} WHERE document_url = %s RETURNING {HISTORICAL_COLUMNS}"
        values.append(document_url)

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, values)
            row = cur.fetchone()
            conn.commit()
        if not row:
            self._send_json(404, {"status": "error", "error": {"code": "not_found", "message": "row not found"}})
            return
        self._send_json(200, {"status": "success", "result": row})

    @staticmethod
    def _coerce(raw, kind):
        if raw is None or raw == "":
            return None
        if kind == "numeric":
            if isinstance(raw, str):
                cleaned = raw.replace(",", "").strip()
                if not cleaned:
                    return None
                return decimal.Decimal(cleaned)
            return decimal.Decimal(str(raw))
        if kind == "boolean":
            if isinstance(raw, bool):
                return raw
            s = str(raw).strip().lower()
            if s in ("true", "yes", "y", "1"):
                return True
            if s in ("false", "no", "n", "0"):
                return False
            return None
        return str(raw)

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        if not length:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw or b"{}")

    def _send_json(self, status, payload):
        body = json.dumps(payload, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_filings_post(self):
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length) if length else b"{}"
        req = urllib.request.Request(
            SECAPI_URL,
            data=body,
            headers={
                "Authorization": SECAPI_KEY,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        self._forward(req)

    def _handle_ai_analyze(self):
        if not ANTHROPIC_KEY:
            self._send_json(500, {"status": "error", "error": {"code": "no_anthropic_key", "message": "anthropic_api_key missing in config.json"}})
            return
        body = self._read_json_body()
        document_url = body.get("document_url")
        force = bool(body.get("force"))
        if not document_url:
            self._send_json(400, {"status": "error", "error": {"code": "missing_pk", "message": "document_url required"}})
            return

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {HISTORICAL_COLUMNS}, "
                "(SELECT s.isadr FROM screener s WHERE s.ticker = historical_float_final.ticker LIMIT 1) AS screener_isadr "
                "FROM historical_float_final WHERE document_url = %s",
                (document_url,),
            )
            row = cur.fetchone()
        if not row:
            self._send_json(404, {"status": "error", "error": {"code": "not_found", "message": "row not found"}})
            return
        if row.get("ai_analysis") and not force:
            self._send_json(200, {"status": "success", "cached": True, "analysis": row["ai_analysis"]})
            return

        ticker = (row.get("ticker") or "").upper()
        if not ticker:
            self._send_json(400, {"status": "error", "error": {"code": "no_ticker", "message": "row has no ticker"}})
            return

        try:
            ctx = self._gather_ai_context(ticker, row)
            analysis = self._call_anthropic(row, ctx)
        except Exception as e:
            self._send_json(502, {"status": "error", "error": {"code": "analyzer_failed", "message": str(e)}})
            return

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE historical_float_final SET ai_analysis = %s, updated_at = NOW() WHERE document_url = %s",
                (analysis, document_url),
            )
            conn.commit()
        self._send_json(200, {"status": "success", "cached": False, "analysis": analysis})

    @staticmethod
    def _askedgar_get(path, params):
        qs = urllib.parse.urlencode(params)
        target = f"{ASKEDGAR_BASE}{path}?{qs}"
        req = urllib.request.Request(target, headers={"API-KEY": ASKEDGAR_KEY})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
            enc = resp.headers.get("Content-Encoding")
            if enc:
                data = Handler._decode_body(data, enc)
        return json.loads(data.decode("utf-8")) if data else {}

    @staticmethod
    def _get_filing_text(document_url):
        """Fetch source text for an SEC filing or Benzinga press release from Elasticsearch.
        - sec.gov URLs hit the `stock_filings` index, term match on `linkToFilingDetails`,
          return `textContent`.
        - benzinga URLs hit the `news` index, term match on `url`, return `body`
          (or `teaser` if body is empty).
        Returns truncated string, or None if not found / not configured."""
        if not (ELASTIC_URL and ELASTIC_KEY):
            return None
        u = (document_url or "").lower()
        if "benzinga" in u:
            index = "news"
            url_field = "url"
            text_fields = ("body", "teaser")
        else:
            index = ELASTIC_INDEX
            url_field = "linkToFilingDetails"
            text_fields = ("textContent",)

        body = json.dumps({
            "query": {"term": {url_field: document_url}},
            "size": 1,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{ELASTIC_URL}/{index}/_search",
            data=body,
            headers={
                "Authorization": f"ApiKey {ELASTIC_KEY}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        hits = ((data.get("hits") or {}).get("hits") or [])
        if not hits:
            return None
        src = hits[0].get("_source") or {}
        text = ""
        for f in text_fields:
            v = src.get(f)
            if v:
                text = v
                break
        if not text:
            return None
        if len(text) > FILING_FETCH_MAX_CHARS:
            text = text[:FILING_FETCH_MAX_CHARS] + "\n\n[...truncated...]"
        return text

    @staticmethod
    def _extract_filing_excerpts(text):
        """Use a cheap/fast model (Haiku) to pull VERBATIM share-relevant excerpts from a long
        filing, so the main analysis model reasons over the right passages instead of an
        arbitrary leading slice. Returns delimited excerpts, or None if nothing relevant / error."""
        if not ANTHROPIC_KEY:
            return None
        system_prompt = (
            "You are extracting excerpts from the full text of an SEC filing or press release. "
            "Output, VERBATIM, every passage relevant to the company's share structure, specifically:\n"
            "- total shares outstanding (common / ordinary), with the 'as of' date\n"
            "- shares broken out by class when there are multiple classes\n"
            "- float / tradable float / shares held by affiliates or insiders / restricted shares\n"
            "- ADS / ADR ratio language (e.g. 'each ADS represents N ordinary shares')\n"
            "- reverse splits or share consolidations and their effective dates\n"
            "- shares outstanding before/after an offering or private placement\n"
            "Output each excerpt exactly as it appears -- do NOT paraphrase, summarize, or compute. "
            "Separate distinct excerpts with a line containing only '-----'. "
            "If share amounts are stated in thousands or millions, add a short bracketed note after that "
            "excerpt, e.g. [values in thousands]. "
            "If the filing contains NO information about share counts, output exactly 'N/A'. "
            "Never output anything that is not present in the filing."
        )
        payload = {
            "model": EXTRACT_MODEL,
            "max_tokens": 4000,
            "system": system_prompt,
            "messages": [{"role": "user", "content": f"Full filing text:\n\n{text}"}],
        }
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            ANTHROPIC_URL,
            data=body,
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                parsed = json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None  # caller falls back to truncation
        parts = [b.get("text", "") for b in (parsed.get("content") or []) if b.get("type") == "text"]
        out = ("\n".join(parts)).strip()
        if not out or out.upper().startswith("N/A"):
            return None
        return out

    def _gather_ai_context(self, ticker, row):
        ctx = {}
        # Filing text (only for sec.gov / benzinga URLs — that's where the OS came from)
        doc_url = (row.get("document_url") or "").lower()
        if "sec.gov" in doc_url or "benzinga" in doc_url:
            try:
                txt = self._get_filing_text(row.get("document_url"))
                if txt:
                    if len(txt) > FILING_EXCERPT_THRESHOLD_CHARS:
                        # Long filing: pre-extract the share-relevant excerpts with a cheap model
                        # so the main model sees the right passages, not just a leading slice.
                        excerpts = self._extract_filing_excerpts(txt)
                        if excerpts:
                            ctx["filing_text"] = excerpts
                            ctx["filing_text_excerpts"] = True
                        else:
                            ctx["filing_text"] = txt[:FILING_TRUNCATE_CHARS] + "\n\n[...truncated...]"
                            ctx["filing_text_truncated"] = True
                    else:
                        ctx["filing_text"] = txt
            except Exception as e:
                ctx["filing_text_error"] = str(e)

        # Historical float records — from Postgres (our computed _final values + restricted_shares),
        # strictly before this row's date_final, MANUAL excluded, with at least one final present.
        # This replaces the AskEdgar historical-float-pro feed (which lacks _final and restricted_shares).
        try:
            sql = (
                "SELECT date_final, filed_at, form_type, document_url, outstanding_shares_final, "
                "float_final, tradable_float_final, affiliate_shares, restricted_shares, ads_ratio "
                "FROM historical_float_final "
                "WHERE ticker = %s AND document_url <> %s "
                "AND (form_type IS NULL OR form_type NOT LIKE %s) "
                "AND (outstanding_shares_final IS NOT NULL OR float_final IS NOT NULL "
                "     OR tradable_float_final IS NOT NULL) "
            )
            params = [ticker, row.get("document_url") or "", "%MANUAL%"]
            if row.get("date_final"):
                sql += "AND date_final < %s "
                params.append(row.get("date_final"))
            sql += "ORDER BY date_final DESC NULLS LAST, filed_at DESC NULLS LAST LIMIT 8"
            with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, params)
                ctx["prior_records"] = cur.fetchall()
        except Exception as e:
            ctx["prior_records_error"] = str(e)

        # News (7 most recent, excluding form_type 'grok' and 'jmt415')
        try:
            news = self._askedgar_get("/v1/news-basic", {"ticker": ticker, "limit": 30, "page": 1})
            EXCLUDED = {"grok", "jmt415"}
            kept = []
            for n in (news or {}).get("results") or []:
                ft = (n.get("form_type") or "").strip().lower()
                if ft in EXCLUDED:
                    continue
                kept.append(n)
                if len(kept) >= 7:
                    break
            ctx["news"] = kept
        except Exception as e:
            ctx["news_error"] = str(e)

        # Offerings (3 most recent)
        try:
            off = self._askedgar_get("/v1/offerings", {"ticker": ticker, "limit": 3, "page": 1})
            ctx["offerings"] = (off or {}).get("results") or []
        except Exception as e:
            ctx["offerings_error"] = str(e)

        # Reverse splits (most recent)
        try:
            sp = self._askedgar_get("/v1/reverse-splits", {"ticker": ticker, "limit": 5, "page": 1})
            splits = (sp or {}).get("results") or []
            ctx["reverse_splits"] = splits
            ctx["latest_split"] = splits[0] if splits else None
        except Exception as e:
            ctx["splits_error"] = str(e)

        # Latest ownership
        try:
            ow = self._askedgar_get("/v1/ownership", {"ticker": ticker, "limit": 1})
            ctx["ownership"] = (ow or {}).get("results") or []
        except Exception as e:
            ctx["ownership_error"] = str(e)

        # Screener (current price + market cap)
        try:
            sc = self._askedgar_get("/v1/screener", {"ticker": ticker})
            results = (sc or {}).get("results") or []
            ctx["screener"] = results[0] if results else None
        except Exception as e:
            ctx["screener_error"] = str(e)

        return ctx

    @staticmethod
    def _call_anthropic(row, ctx):
        sections = []
        sections.append("=== ROW UNDER REVIEW ===")
        sections.append(json.dumps(row, default=_json_default, indent=2))
        if ctx.get("filing_text"):
            if ctx.get("filing_text_excerpts"):
                sections.append(
                    "\n=== SOURCE FILING — RELEVANT EXCERPTS (auto-extracted from a LONG filing by a "
                    "pre-processing model; each excerpt is VERBATIM and separated by a line of '-----'. "
                    "This is NOT the full filing — it is the share-relevant passages pulled from it. "
                    "These excerpts are where the OS / float values came from.) ==="
                )
            elif ctx.get("filing_text_truncated"):
                sections.append("\n=== SOURCE FILING TEXT (long filing; excerpt-extraction found nothing, so this is the first ~5000 tokens only) ===")
            else:
                sections.append("\n=== SOURCE FILING TEXT (full text; this is where the OS / float values came from) ===")
            sections.append(ctx["filing_text"])
        elif ctx.get("filing_text_error"):
            sections.append("\n=== SOURCE FILING TEXT ===")
            sections.append(f"(failed to fetch: {ctx['filing_text_error']})")
        sections.append(
            "\n=== CURRENT PRICE / SCREENER (today's snapshot — NOT necessarily the truth for this row) ===\n"
            "This is the most-recently-updated outstanding/float for the ticker. It is good background "
            "context for sanity-checking magnitude, but it is NOT authoritative for the row under review:\n"
            "  - The row may be MORE up-to-date than the screener if it reflects a fresh issuance from "
            "    its source filing that hasn't propagated to the screener yet.\n"
            "  - The row may be HISTORICAL (date_final well in the past), in which case the screener's "
            "    current values say nothing about whether the row was correct as of date_final. For a "
            "    historical row, only judge it against what was true on date_final (use the source filing, "
            "    contemporaneous offerings/news, and adjacent historical_float records — not today's screener)."
        )
        sections.append(json.dumps(ctx.get("screener"), default=_json_default, indent=2))
        sections.append(
            "\n=== HISTORICAL FLOAT RECORDS (from Postgres — our authoritative computed _final values + "
            "restricted_shares; strictly before this row's date_final; MANUAL rows excluded; most recent first) ==="
        )
        sections.append(json.dumps(ctx.get("prior_records", []), default=_json_default, indent=2))
        sections.append("\n=== 7 MOST RECENT NEWS / FILINGS (excludes form_type grok, jmt415) ===")
        sections.append(json.dumps(ctx.get("news", []), default=_json_default, indent=2))
        sections.append("\n=== 3 MOST RECENT OFFERINGS ===")
        sections.append(json.dumps(ctx.get("offerings", []), default=_json_default, indent=2))
        sections.append("\n=== LATEST OWNERSHIP (raw, not split-adjusted; see latest split below) ===")
        sections.append(json.dumps(ctx.get("ownership", []), default=_json_default, indent=2))
        sections.append("\n=== MOST RECENT REVERSE SPLIT ===")
        sections.append(json.dumps(ctx.get("latest_split"), default=_json_default, indent=2))
        user_content = "\n".join(sections)

        # Prompt selection. Precedence: share-issuance > ADR > standard.
        # A share-issuance row that is also an ADR uses the issuance prompt with an ADR note folded in.
        is_adr_row = (row.get("is_adr") is True) or (row.get("screener_isadr") is True)
        details_l = (row.get("details") or "").lower()
        category_l = (row.get("category") or "").lower()
        is_issuance = "share issuance" in details_l or "share issuance" in category_l
        if is_issuance:
            system_prompt = SHARE_ISSUANCE_BODY + (SHARE_ISSUANCE_ADR_NOTE if is_adr_row else "") + AS_OF_PRINCIPLE + AI_OUTPUT_FORMAT
        elif is_adr_row:
            system_prompt = ADR_AI_PROMPT
        else:
            system_prompt = STANDARD_AI_PROMPT

        payload = {
            "model": ANTHROPIC_MODEL,
            "max_tokens": 16000,
            "thinking": {"type": "adaptive"},
            "output_config": {"effort": "high"},
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_content}],
        }
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            ANTHROPIC_URL,
            data=body,
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                resp_data = resp.read()
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Anthropic HTTP {e.code}: {err_body}")
        parsed = json.loads(resp_data.decode("utf-8"))
        # content is a list of blocks; we want the text block(s).
        text_parts = []
        for block in parsed.get("content") or []:
            if block.get("type") == "text" and block.get("text"):
                text_parts.append(block["text"])
        return ("\n".join(text_parts)).strip() or "(no analysis returned)"

    def _forward(self, req):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = self._decode_body(resp.read(), resp.headers.get("Content-Encoding"))
                self.send_response(resp.status)
                self.send_header(
                    "Content-Type", resp.headers.get("Content-Type", "application/json")
                )
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
        except urllib.error.HTTPError as e:
            data = self._decode_body(e.read(), e.headers.get("Content-Encoding") if e.headers else None)
            ct = (e.headers.get("Content-Type") if e.headers else None) or "application/json"
            self.send_response(e.code)
            self.send_header("Content-Type", ct)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self._send_json(502, {"status": "error", "error": {"code": "proxy_error", "message": str(e)}})

    @staticmethod
    def _decode_body(data, encoding):
        if not data or not encoding:
            return data
        enc = encoding.lower().strip()
        try:
            if enc == "gzip":
                return gzip.decompress(data)
            if enc == "deflate":
                try:
                    return zlib.decompress(data)
                except zlib.error:
                    return zlib.decompress(data, -zlib.MAX_WBITS)
        except Exception:
            return data
        return data

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[server] {fmt % args}\n")


class ReusableTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def _ensure_schema():
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE historical_float_final "
                "ADD COLUMN IF NOT EXISTS ai_analysis TEXT"
            )
            conn.commit()
    except Exception as e:
        sys.stderr.write(f"[server] schema check failed: {e}\n")


def main():
    _ensure_schema()
    with ReusableTCPServer(("127.0.0.1", PORT), Handler) as httpd:
        print(f"OS Dashboard running at http://localhost:{PORT}/")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")


if __name__ == "__main__":
    main()
