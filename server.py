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

# ~5000 tokens worth of filing text. 1 token ~= 4 chars for English prose.
FILING_TEXT_MAX_CHARS = 20000

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
    hf.updated_at
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
            -- Test-value diff path: bypass every other filter when the
            -- secondary extractor disagrees with our finals by >20 percent.
            (
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
            OR (
                hf.form_type NOT LIKE '%%MANUAL%%'
                AND hf.ticker IS NOT NULL
                AND (
                    -- Sub-1M finals bypass category AND exchange filters (most critical)
                    hf.outstanding_shares_final < 1000000
                    OR hf.float_final < 1000000
                    OR hf.tradable_float_final < 1000000
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
    ORDER BY hf.filed_at DESC NULLS LAST
    LIMIT 100
"""


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
                f"SELECT {HISTORICAL_COLUMNS} FROM historical_float_final WHERE document_url = %s",
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
        if len(text) > FILING_TEXT_MAX_CHARS:
            text = text[:FILING_TEXT_MAX_CHARS] + "\n\n[...truncated...]"
        return text

    def _gather_ai_context(self, ticker, row):
        ctx = {}
        # Filing text (only for sec.gov / benzinga URLs — that's where the OS came from)
        doc_url = (row.get("document_url") or "").lower()
        if "sec.gov" in doc_url or "benzinga" in doc_url:
            try:
                txt = self._get_filing_text(row.get("document_url"))
                if txt:
                    ctx["filing_text"] = txt
            except Exception as e:
                ctx["filing_text_error"] = str(e)

        # Historical float (5 most recent, excluding the row itself if it appears)
        try:
            hist = self._askedgar_get("/v1/historical-float-pro", {"ticker": ticker, "limit": 10, "page": 1})
            results = (hist or {}).get("results") or []
            results = [r for r in results if r.get("document_url") != row.get("document_url")][:5]
            ctx["historical_float"] = results
        except Exception as e:
            ctx["historical_float_error"] = str(e)

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
            sections.append("\n=== SOURCE FILING TEXT (truncated to ~5000 tokens; this is where the OS / float values came from) ===")
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
        sections.append("\n=== 5 MOST RECENT HISTORICAL FLOAT RECORDS (excluding this row) ===")
        sections.append(json.dumps(ctx.get("historical_float", []), default=_json_default, indent=2))
        sections.append("\n=== 7 MOST RECENT NEWS / FILINGS (excludes form_type grok, jmt415) ===")
        sections.append(json.dumps(ctx.get("news", []), default=_json_default, indent=2))
        sections.append("\n=== 3 MOST RECENT OFFERINGS ===")
        sections.append(json.dumps(ctx.get("offerings", []), default=_json_default, indent=2))
        sections.append("\n=== LATEST OWNERSHIP (raw, not split-adjusted; see latest split below) ===")
        sections.append(json.dumps(ctx.get("ownership", []), default=_json_default, indent=2))
        sections.append("\n=== MOST RECENT REVERSE SPLIT ===")
        sections.append(json.dumps(ctx.get("latest_split"), default=_json_default, indent=2))
        user_content = "\n".join(sections)

        system_prompt = (
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
            "- If the ticker is an ADR (is_adr=true), outstanding_shares and float values SHOULD be expressed in ordinary shares, "
            "NOT ADRs. The ads_ratio field handles the conversion (ads_ratio = ordinary shares per ADR). "
            "If the numbers look like ADR-level counts (much smaller than expected given the company's market cap and share price), "
            "flag that the source filing may have reported ADR counts instead of ordinary shares.\n"
            "- Most important red flags: OS or float < 500k (especially < 100k or negative). "
            "Negatives are almost always wrong, unless the company is a brand-new SPAC or similar.\n"
            "- CRITICAL: the SCREENER block is today's current values, not truth for this row. "
            "Do NOT recommend changes by comparing the row to screener -- the row may legitimately be more "
            "current than the screener (it can reflect a brand-new issuance from its source filing) OR "
            "the row may be historical (date_final in the past). For a historical date_final, the question "
            "is 'were these values correct AS OF date_final', so the screener's current snapshot is largely "
            "irrelevant. Use the screener only as a rough magnitude sanity check (e.g. an obviously-wrong "
            "10x discrepancy in either direction); do not flag a discrepancy 'because screener says X'.\n\n"
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
            "(only list fields that need changing)\n"
            "If no changes are needed, write: No changes recommended.\n\n"
            "Keep the whole output compact -- this displays inside a small side panel."
        )

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
