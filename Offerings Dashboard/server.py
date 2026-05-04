"""Offerings Dashboard - local server with API proxy + Postgres-backed review queue.

Run:  python server.py
Then open http://localhost:8766/ in a browser.

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
ANTHROPIC_KEY = _CFG.get("anthropic_api_key", "")
ELASTIC_URL = (_CFG.get("elastic_url") or "").rstrip("/")
ELASTIC_KEY = _CFG.get("elastic_api_key", "")
ELASTIC_INDEX = _CFG.get("elastic_filings_index", "stock_filings")
PORT = int(_CFG.get("port", 8766))

ASKEDGAR_BASE = "https://eapi.askedgar.io"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_MODEL = "claude-opus-4-7"

# ~5000 tokens worth of filing text. 1 token ~= 4 chars for English prose.
FILING_TEXT_MAX_CHARS = 20000

DB_CONFIG = {
    **_CFG["postgres"],
    "connect_timeout": 15,
    "application_name": "offerings-dashboard",
}

CHICAGO = zoneinfo.ZoneInfo("America/Chicago")

# Editable columns in the Review Queue. Server-side allowlist.
# `id` is the PK and is intentionally NOT editable.
EDITABLE_COLUMNS = {
    "ticker": "text",
    "headline": "text",
    "selling_shareholder_details": "text",
    "commentary": "text",
    "done": "boolean",
    "disclosure_type": "text",
    "askedgar_url": "text",
    "offering_amount": "numeric",
    "share_price": "numeric",
    "conversion_price": "numeric",
    "shares_amount": "numeric",
    "warrants_amount": "numeric",
    "warrants_exercise_price": "numeric",
    "potential_owners": "text",
    "bank": "text",
    "form_type": "text",
    "offering_type": "text",
    "period_ending": "date",
    "doublecheck": "text",
}

REVIEW_COLUMNS = """
    id,
    ticker,
    filed_at,
    period_ending,
    headline,
    selling_shareholder_details,
    commentary,
    done,
    disclosure_type,
    askedgar_url,
    offering_amount,
    share_price,
    conversion_price,
    shares_amount,
    warrants_amount,
    warrants_exercise_price,
    potential_owners,
    bank,
    form_type,
    offering_type,
    financing_activity,
    doublecheck,
    updated_at
"""

# `done` filter values (URL param `done`):
#   "true"     -> done = TRUE
#   "false"    -> done = FALSE
#   "null"     -> done IS NULL
#   "notnull"  -> done IS NOT NULL
#   anything else -> no filter
DONE_CLAUSES = {
    "true":    "AND done = TRUE",
    "false":   "AND done = FALSE",
    "null":    "AND done IS NULL",
    "notnull": "AND done IS NOT NULL",
}

def _build_review_query(done_filter):
    done_sql = DONE_CLAUSES.get(done_filter, "")
    return f"""
        SELECT {REVIEW_COLUMNS}
        FROM offering_data_new_final_clone
        WHERE
            filed_at = %(filed_at)s
            AND financing_activity = TRUE
            {done_sql}
        ORDER BY ticker ASC
        LIMIT 1000
    """

def _build_review_by_ticker_query(done_filter):
    done_sql = DONE_CLAUSES.get(done_filter, "")
    return f"""
        SELECT {REVIEW_COLUMNS}
        FROM all_financings_final
        WHERE ticker = %(ticker)s
        {done_sql}
        ORDER BY filed_at DESC NULLS LAST, id DESC
        LIMIT 1000
    """


# ----------------- Warrants & Convertibles review queue -----------------
# Mirrors the offerings queue but against `warrants_convertibles4`. The
# analyst-facing analogue of `done` here is `verified`. `link_to_ticker` from
# the user's SELECT isn't an actual column — it's a derived display value the
# UI computes from `ticker`, so it's intentionally not in this allowlist.
WARRANTS_EDITABLE_COLUMNS = {
    "ticker": "text",
    "form_type": "text",
    "details": "text",
    "selling_shareholder_details": "text",
    "owners_two": "text",
    "verified": "boolean",
    "askedgar_url": "text",
    "pp_clause": "text",
    "price_protection": "text",
    "warrants_amount": "numeric",
    "warrants_exercise_price": "numeric",
    "expiration_date": "date",
    "exercisable_date": "date",
    "prefunded_cost": "numeric",
    "offering_amount": "numeric",
    "conversion_price": "numeric",
    "maturity_date": "date",
    "convertible_date": "date",
    "owners": "text",
    "bank": "text",
    "registered": "text",
    "summary": "text",
    "notes": "text",
    "warrants_remaining": "numeric",
    "convertible_debt_remaining": "numeric",
    "file_no": "text",
    "document_url": "text",
    "filed_at": "date",
}

WARRANTS_REVIEW_COLUMNS = """
    id,
    ticker,
    filed_at,
    form_type,
    details,
    selling_shareholder_details,
    owners_two,
    registered,
    summary,
    verified,
    askedgar_url,
    pp_clause,
    price_protection,
    warrants_amount,
    warrants_exercise_price,
    warrants_remaining,
    expiration_date,
    exercisable_date,
    prefunded_cost,
    offering_amount,
    conversion_price,
    convertible_debt_remaining,
    maturity_date,
    convertible_date,
    owners,
    bank,
    document_url,
    file_no,
    notes,
    updated_at
"""

VERIFIED_CLAUSES = {
    "true":    "AND verified = TRUE",
    "false":   "AND verified = FALSE",
    "null":    "AND verified IS NULL",
    "notnull": "AND verified IS NOT NULL",
}

def _build_warrants_query(verified_filter):
    v_sql = VERIFIED_CLAUSES.get(verified_filter, "")
    return f"""
        SELECT {WARRANTS_REVIEW_COLUMNS}
        FROM warrants_convertibles4
        WHERE filed_at = %(filed_at)s
        {v_sql}
        ORDER BY ticker ASC
        LIMIT 1000
    """

def _build_warrants_by_ticker_query(verified_filter):
    v_sql = VERIFIED_CLAUSES.get(verified_filter, "")
    return f"""
        SELECT {WARRANTS_REVIEW_COLUMNS}
        FROM warrants_convertibles3
        WHERE ticker = %(ticker)s
        {v_sql}
        ORDER BY filed_at DESC NULLS LAST, id DESC
        LIMIT 1000
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
            if url.path == "/api/review/by-ids":
                self._handle_review_by_ids()
            elif url.path == "/api/review/update":
                self._handle_review_update()
            elif url.path == "/api/review/create":
                self._handle_review_create()
            elif url.path == "/api/warrants/by-ids":
                self._handle_warrants_by_ids()
            elif url.path == "/api/warrants/update":
                self._handle_warrants_update()
            elif url.path == "/api/warrants/create":
                self._handle_warrants_create()
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
            "/api/offerings-advanced": "/v1/offerings-advanced",
            "/api/dilution-data-advanced": "/v1/dilution-data-advanced",
            "/api/news-basic": "/v1/news-basic",
        }
        if path in askedgar_routes:
            flat = [(k, v) for k, vs in params.items() for v in vs]
            qs = urllib.parse.urlencode(flat)
            target = f"{ASKEDGAR_BASE}{askedgar_routes[path]}?{qs}"
            self._forward(urllib.request.Request(target, headers={"API-KEY": ASKEDGAR_KEY}))
            return

        try:
            if path == "/api/review":
                self._handle_review(params)
                return
            if path == "/api/warrants":
                self._handle_warrants_review(params)
                return
        except Exception as e:
            self._send_json(
                500,
                {"status": "error", "error": {"code": "db_error", "message": str(e)}},
            )
            return

        self.send_error(404, "Not Found")

    def _handle_review(self, params):
        ticker = (params.get("ticker", [""])[0] or "").strip().upper()
        done_filter = (params.get("done", [""])[0] or "").strip().lower()

        if ticker:
            sql = _build_review_by_ticker_query(done_filter)
            with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, {"ticker": ticker})
                rows = cur.fetchall()
            self._send_json(
                200,
                {
                    "status": "success",
                    "mode": "ticker",
                    "ticker": ticker,
                    "done": done_filter,
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

        sql = _build_review_query(done_filter)
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, {"filed_at": filed_at})
            rows = cur.fetchall()

        self._send_json(
            200,
            {
                "status": "success",
                "mode": "date",
                "filed_at": filed_at.isoformat(),
                "done": done_filter,
                "count": len(rows),
                "results": rows,
            },
        )

    def _handle_review_by_ids(self):
        body = self._read_json_body()
        ids = body.get("ids") or []
        if not isinstance(ids, list) or not ids:
            self._send_json(200, {"status": "success", "count": 0, "results": []})
            return
        try:
            id_ints = [int(x) for x in ids if x is not None and x != ""]
        except (TypeError, ValueError):
            self._send_json(400, {"status": "error", "error": {"code": "bad_ids", "message": "ids must be integers"}})
            return
        if not id_ints:
            self._send_json(200, {"status": "success", "count": 0, "results": []})
            return

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {REVIEW_COLUMNS} FROM offering_data_new_final_clone "
                "WHERE id = ANY(%s) ORDER BY filed_at DESC NULLS LAST",
                (id_ints,),
            )
            rows = cur.fetchall()
        self._send_json(200, {"status": "success", "count": len(rows), "results": rows})

    @staticmethod
    def _offerings_table(mode):
        return "all_financings_final" if mode == "ticker" else "offering_data_new_final_clone"

    @staticmethod
    def _warrants_table(mode):
        return "warrants_convertibles3" if mode == "ticker" else "warrants_convertibles4"

    def _handle_review_update(self):
        body = self._read_json_body()
        row_id = body.get("id")
        mode = body.get("mode", "date")
        updates = body.get("updates") or {}
        if row_id is None or row_id == "":
            self._send_json(400, {"status": "error", "error": {"code": "missing_pk", "message": "id required"}})
            return
        try:
            row_id = int(row_id)
        except (TypeError, ValueError):
            self._send_json(400, {"status": "error", "error": {"code": "bad_id", "message": "id must be integer"}})
            return
        if not isinstance(updates, dict) or not updates:
            self._send_json(400, {"status": "error", "error": {"code": "no_updates", "message": "updates required"}})
            return

        table = self._offerings_table(mode)
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
        sql = (
            f"UPDATE {table} SET {', '.join(sets)} "
            f"WHERE id = %s RETURNING {REVIEW_COLUMNS}"
        )
        values.append(row_id)

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, values)
            row = cur.fetchone()
            conn.commit()
        if not row:
            self._send_json(404, {"status": "error", "error": {"code": "not_found", "message": "row not found"}})
            return
        self._send_json(200, {"status": "success", "result": row})

    def _handle_review_create(self):
        body = self._read_json_body()
        mode = body.get("mode", "date")
        table = self._offerings_table(mode)
        cols = []
        vals = []
        for col in ("ticker", "filed_at"):
            v = body.get(col)
            if v is not None and v != "":
                cols.append(col)
                if col == "filed_at":
                    vals.append(datetime.date.fromisoformat(str(v)[:10]))
                else:
                    vals.append(str(v))
        if cols:
            placeholders = ", ".join(["%s"] * len(cols))
            col_list = ", ".join(cols)
            sql = (
                f"INSERT INTO {table} ({col_list}) "
                f"VALUES ({placeholders}) RETURNING {REVIEW_COLUMNS}"
            )
        else:
            sql = f"INSERT INTO {table} DEFAULT VALUES RETURNING {REVIEW_COLUMNS}"
            vals = []
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, vals)
            row = cur.fetchone()
            conn.commit()
        self._send_json(200, {"status": "success", "result": row})

    # ----------------- Warrants & Convertibles handlers -----------------
    def _handle_warrants_review(self, params):
        ticker = (params.get("ticker", [""])[0] or "").strip().upper()
        verified_filter = (params.get("verified", [""])[0] or "").strip().lower()

        if ticker:
            sql = _build_warrants_by_ticker_query(verified_filter)
            with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
                cur.execute(sql, {"ticker": ticker})
                rows = cur.fetchall()
            self._send_json(
                200,
                {
                    "status": "success",
                    "mode": "ticker",
                    "ticker": ticker,
                    "verified": verified_filter,
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

        sql = _build_warrants_query(verified_filter)
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, {"filed_at": filed_at})
            rows = cur.fetchall()

        self._send_json(
            200,
            {
                "status": "success",
                "mode": "date",
                "filed_at": filed_at.isoformat(),
                "verified": verified_filter,
                "count": len(rows),
                "results": rows,
            },
        )

    def _handle_warrants_by_ids(self):
        body = self._read_json_body()
        ids = body.get("ids") or []
        if not isinstance(ids, list) or not ids:
            self._send_json(200, {"status": "success", "count": 0, "results": []})
            return
        try:
            id_ints = [int(x) for x in ids if x is not None and x != ""]
        except (TypeError, ValueError):
            self._send_json(400, {"status": "error", "error": {"code": "bad_ids", "message": "ids must be integers"}})
            return
        if not id_ints:
            self._send_json(200, {"status": "success", "count": 0, "results": []})
            return

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {WARRANTS_REVIEW_COLUMNS} FROM warrants_convertibles4 "
                "WHERE id = ANY(%s) ORDER BY filed_at DESC NULLS LAST",
                (id_ints,),
            )
            rows = cur.fetchall()
        self._send_json(200, {"status": "success", "count": len(rows), "results": rows})

    def _handle_warrants_update(self):
        body = self._read_json_body()
        row_id = body.get("id")
        mode = body.get("mode", "date")
        updates = body.get("updates") or {}
        if row_id is None or row_id == "":
            self._send_json(400, {"status": "error", "error": {"code": "missing_pk", "message": "id required"}})
            return
        try:
            row_id = int(row_id)
        except (TypeError, ValueError):
            self._send_json(400, {"status": "error", "error": {"code": "bad_id", "message": "id must be integer"}})
            return
        if not isinstance(updates, dict) or not updates:
            self._send_json(400, {"status": "error", "error": {"code": "no_updates", "message": "updates required"}})
            return

        table = self._warrants_table(mode)
        sets = []
        values = []
        for col, raw in updates.items():
            if col not in WARRANTS_EDITABLE_COLUMNS:
                self._send_json(400, {"status": "error", "error": {"code": "field_not_editable", "message": col}})
                return
            kind = WARRANTS_EDITABLE_COLUMNS[col]
            value = self._coerce(raw, kind)
            sets.append(f"{col} = %s")
            values.append(value)
        sets.append("updated_at = NOW()")
        sql = (
            f"UPDATE {table} SET {', '.join(sets)} "
            f"WHERE id = %s RETURNING {WARRANTS_REVIEW_COLUMNS}"
        )
        values.append(row_id)

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, values)
            row = cur.fetchone()
            conn.commit()
        if not row:
            self._send_json(404, {"status": "error", "error": {"code": "not_found", "message": "row not found"}})
            return
        self._send_json(200, {"status": "success", "result": row})

    def _handle_warrants_create(self):
        body = self._read_json_body()
        mode = body.get("mode", "date")
        table = self._warrants_table(mode)
        cols = []
        vals = []
        for col in ("ticker", "filed_at"):
            v = body.get(col)
            if v is not None and v != "":
                cols.append(col)
                if col == "filed_at":
                    vals.append(datetime.date.fromisoformat(str(v)[:10]))
                else:
                    vals.append(str(v))
        if cols:
            placeholders = ", ".join(["%s"] * len(cols))
            col_list = ", ".join(cols)
            sql = (
                f"INSERT INTO {table} ({col_list}) "
                f"VALUES ({placeholders}) RETURNING {WARRANTS_REVIEW_COLUMNS}"
            )
        else:
            sql = f"INSERT INTO {table} DEFAULT VALUES RETURNING {WARRANTS_REVIEW_COLUMNS}"
            vals = []
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, vals)
            row = cur.fetchone()
            conn.commit()
        self._send_json(200, {"status": "success", "result": row})

    # -------------------- AI analysis --------------------
    def _handle_ai_analyze(self):
        if not ANTHROPIC_KEY:
            self._send_json(500, {"status": "error", "error": {"code": "no_anthropic_key", "message": "anthropic_api_key missing in config.json"}})
            return
        body = self._read_json_body()
        row_id = body.get("id")
        force = bool(body.get("force"))
        if row_id is None or row_id == "":
            self._send_json(400, {"status": "error", "error": {"code": "missing_pk", "message": "id required"}})
            return
        try:
            row_id = int(row_id)
        except (TypeError, ValueError):
            self._send_json(400, {"status": "error", "error": {"code": "bad_id", "message": "id must be integer"}})
            return

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {REVIEW_COLUMNS} FROM offering_data_new_final_clone WHERE id = %s",
                (row_id,),
            )
            row = cur.fetchone()
        if not row:
            self._send_json(404, {"status": "error", "error": {"code": "not_found", "message": "row not found"}})
            return
        if row.get("doublecheck") and not force:
            self._send_json(200, {"status": "success", "cached": True, "analysis": row["doublecheck"]})
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
                "UPDATE offering_data_new_final_clone SET doublecheck = %s, updated_at = NOW() WHERE id = %s",
                (analysis, row_id),
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
    def _extract_filing_url(askedgar_url):
        """Extract the raw sec.gov or benzinga URL from an askedgar_url.
        askedgar_url has two patterns:
        - https://app.askedgar.io/filing?...&filingUrl=<encoded-sec.gov-url>
        - https://www.benzinga.com/... (direct URL for news articles)
        """
        if not askedgar_url:
            return None
        u = askedgar_url.lower()
        if "benzinga" in u:
            return askedgar_url
        parsed = urllib.parse.urlparse(askedgar_url)
        qs = urllib.parse.parse_qs(parsed.query)
        filing_urls = qs.get("filingUrl", [])
        return filing_urls[0] if filing_urls else None

    @staticmethod
    def _get_filing_text(filing_url):
        """Fetch source text from Elasticsearch for an SEC filing or news article."""
        if not (ELASTIC_URL and ELASTIC_KEY):
            return None
        if not filing_url:
            return None
        u = (filing_url or "").lower()
        if "benzinga" in u:
            index = "news"
            url_field = "url"
            text_fields = ("body", "teaser")
        else:
            index = ELASTIC_INDEX
            url_field = "linkToFilingDetails"
            text_fields = ("textContent",)

        body = json.dumps({
            "query": {"term": {url_field: filing_url}},
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
        # Filing text — extract the real filing URL from askedgar_url
        filing_url = self._extract_filing_url(row.get("askedgar_url"))
        if filing_url:
            try:
                txt = self._get_filing_text(filing_url)
                if txt:
                    ctx["filing_text"] = txt
            except Exception as e:
                ctx["filing_text_error"] = str(e)

        # Recent offerings (5 most recent via AskEdgar advanced endpoint)
        try:
            off = self._askedgar_get("/v1/offerings-advanced", {"ticker": ticker, "limit": 5, "page": 1})
            ctx["recent_offerings"] = (off or {}).get("results") or []
        except Exception as e:
            ctx["recent_offerings_error"] = str(e)

        # Recent news & filings (5 most recent)
        try:
            news = self._askedgar_get("/v1/news-basic", {"ticker": ticker, "limit": 5, "page": 1})
            ctx["recent_news"] = (news or {}).get("results") or []
        except Exception as e:
            ctx["recent_news_error"] = str(e)

        return ctx

    @staticmethod
    def _call_anthropic(row, ctx):
        sections = []

        # --- Extracted data with field descriptions ---
        sections.append("=== EXTRACTED DATA (the row being reviewed) ===")
        sections.append("Field descriptions:")
        sections.append("- ticker: the stock ticker this entry is recorded under")
        sections.append("- headline: standardized category label for the financing event (see prompt rules)")
        sections.append("- done: true/false — whether this row is verified and should be committed to the production financings table")
        sections.append("- disclosure_type: indicates if this is a potentially new development and if the financing activity is the primary topic of the filing")
        sections.append("- selling_shareholder_details: a summary of the financing activity")
        sections.append("- commentary: commentary on whether the initial AI analysis indicated this is a duplicate of an existing entry")
        sections.append("- offering_amount: dollar amount of the offering")
        sections.append("- share_price: price per share")
        sections.append("- conversion_price: conversion price for convertible instruments")
        sections.append("- shares_amount: total shares sold/issued (includes pre-funded warrants)")
        sections.append("- warrants_amount: total warrants in offering (excludes pre-funded warrants)")
        sections.append("- warrants_exercise_price: exercise price per warrant")
        sections.append("- bank: placement agent, sales agent, or underwriter")
        sections.append("- potential_owners: named investors")
        sections.append("- form_type: SEC form type (8-K, S-1, etc.)")
        sections.append("- period_ending: period ending date if applicable")
        sections.append("- filed_at: date the filing was made")
        sections.append("")
        row_clean = {k: v for k, v in row.items() if k not in ("doublecheck", "updated_at", "financing_activity")}
        sections.append(json.dumps(row_clean, default=_json_default, indent=2))

        # --- Filing text ---
        if ctx.get("filing_text"):
            sections.append("\n=== ORIGINAL FILING TEXT (the raw source document) ===")
            sections.append(ctx["filing_text"])
        elif ctx.get("filing_text_error"):
            sections.append("\n=== ORIGINAL FILING TEXT ===")
            sections.append(f"(failed to fetch: {ctx['filing_text_error']})")
        else:
            sections.append("\n=== ORIGINAL FILING TEXT ===")
            sections.append("(not available — Elasticsearch not configured)")

        # --- Historical data ---
        sections.append("\n=== HISTORICAL DATA: 5 MOST RECENT OFFERINGS ===")
        sections.append(json.dumps(ctx.get("recent_offerings", []), default=_json_default, indent=2))

        sections.append("\n=== HISTORICAL DATA: 5 MOST RECENT NEWS & FILINGS ===")
        sections.append(json.dumps(ctx.get("recent_news", []), default=_json_default, indent=2))

        user_content = "\n".join(sections)

        system_prompt = (
            "You are a senior data quality analyst reviewing SEC filing data that has been extracted "
            "by an AI and reviewed by a human data specialist. Your job is to double-check everything.\n\n"
            "You will receive:\n"
            "1. The ORIGINAL FILING TEXT (the raw source document)\n"
            "2. The EXTRACTED DATA\n"
            "3. HISTORICAL DATA (recent financings, filings and news)\n\n"
            "You must verify TWO things:\n\n"
            "## A) EXTRACTION ACCURACY\n"
            "Check every extracted field against the original filing text using these rules.\n\n"
            "**IMPORTANT**: Only flag issues that are materially wrong. Do NOT flag:\n"
            "- Minor rounding differences (e.g., $0.65 vs $0.66)\n"
            "- Trivially minor field discrepancies that don't change the meaning\n"
            "- Edge cases where either interpretation is defensible\n"
            "Focus on errors that would mislead someone reviewing the data.\n\n"
            "### headline\n"
            "Must use one of these exact headlines in CAPITAL LETTERS:\n\n"
            "- `REGISTERED OFFERING (TYPE)` — TYPE is one of: UNDERWRITTEN, RDO, BEST EFFORTS, DEBT OFFERING, RIGHTS OFFERING. "
            "'RDO' = registered direct offering. 'DEBT OFFERING' = notes/bonds. 'RIGHTS OFFERING' = company offers existing "
            "shareholders right to buy more shares. If type unknown, just 'REGISTERED OFFERING'. **IMPORTANT**: MUST explicitly "
            "reference a registration statement (S-1, S-3, F-1, F-3, F-10, or a File No. like '333-258326'). No registration "
            "statement = use PRIVATE PLACEMENT instead. Over-allotment closings = OTHER, not REGISTERED OFFERING.\n"
            "- `PRIVATE PLACEMENT (TYPE)` — TYPE is one of: EQUITY, CONVERTIBLE NOTE, CONVERTIBLE PREFERRED, NOTE, DESPAC. "
            "'NOTE' = non-convertible debt. 'DESPAC' = done in conjunction with business combination/merger. If type unknown, "
            "just 'PRIVATE PLACEMENT'. An unregistered offering where equity or debt securities are sold for immediate cash proceeds.\n"
            "- `WARRANT EXERCISE` — Only when warrants are exercised for cash proceeds or cashless. Must specifically state warrants "
            "were exercised and company receives cash (unless cashless). Simple issuance of warrants does NOT count.\n"
            "- `NEW EQUITY LINE` — Also known as SPA, SEPA, ELOC, Standby Equity Purchase Agreement. Key traits: (1) single "
            "institutional investor, (2) shares sold over a period of time at varying prices based on formula/market price, not all at once.\n"
            "- `NEW ATM` — New at-the-market or controlled equity offering agreement, often via S-3/F-3, but no shares sold yet.\n"
            "- `DEBT OFFERING` — Non-convertible bonds/notes.\n"
            "- `LOAN` — Traditional bank loans.\n"
            "- `CREDIT FACILITY` — New revolving credit lines. Do not include amendments unless they significantly increase borrowing capacity.\n"
            "- `ASSET SALE` — Assets sold for cash.\n"
            "- `SHARE ISSUANCE FOR ACQUISITION` — Shares issued to acquire companies/assets/services. Mark 'PENDING' if shares not yet issued. "
            "Exclude mergers/reverse mergers.\n"
            "- `FINANCIAL/SHAREHOLDER RESULTS` — Quarterly/annual reports or shareholder votes. If the filing ALSO discloses distinct new "
            "financing activities, those should be separate entries with the specific financing headline appended with "
            "', FINANCIAL/SHAREHOLDER RESULTS'. Only use alone if NO new financing activities.\n"
            "- `IPO` — Initial Public Offerings.\n"
            "- `DEBT CONVERSION` — Debt converted into equity.\n"
            "- `ATM USED` — Sales/proceeds from an existing at-the-market (ATM), controlled equity, standby equity, or equity line of "
            "credit (ELOC) agreement. This is NOT a new ATM setup (that's NEW ATM), but actual shares sold and proceeds received under "
            "an existing agreement. The offering_amount represents the incremental amount not previously reported.\n"
            "- `OTHER, [BRIEF DESCRIPTION]` — Anything not fitting above categories.\n\n"
            "**PENDING rules**: Prepend 'PENDING' if (a) only plans/expectations without a confirmed transaction, or (b) a REGISTERED "
            "OFFERING or PRIVATE PLACEMENT is missing key details (shares, price, amount). Do NOT prepend PENDING if the deal is "
            "announced and committed but just hasn't officially closed yet.\n\n"
            "### offering_type\n"
            "- Only for REGISTERED OFFERING — the form type (S-1, S-3, F-1, F-3) from the registration statement reference\n"
            "- null if not applicable\n\n"
            "### offering_amount\n"
            "- Dollar amount that has ACTUALLY CLOSED or been committed. NOT \"up to\" amounts\n"
            "- Multi-tranche: only the closed tranche amount\n"
            "- null if not applicable\n\n"
            "### shares_amount\n"
            "- Total shares sold/issued. INCLUDE pre-funded warrants (treat as shares)\n"
            "- If not stated, should be calculated as offering_amount / share_price (or / conversion_price)\n"
            "- null only if not stated AND cannot be calculated\n\n"
            "### share_price\n"
            "- Price per share in USD. Convert foreign currency\n"
            "- **IMPORTANT**: If share_price would be null but conversion_price is populated, share_price is intentionally set to "
            "the conversion_price value. Do NOT flag this as an error.\n"
            "- null if not applicable and no conversion_price exists\n\n"
            "### conversion_price\n"
            "- For convertible notes/debt or preferred stock. Use fixed/floor price, NOT variable/formula prices\n"
            "- null if not applicable\n\n"
            "### warrants_amount\n"
            "- Total warrants in offering. EXCLUDE pre-funded warrants\n"
            "- If defined as percentage of shares, must be calculated\n"
            "- null if none\n\n"
            "### warrants_exercise_price\n"
            "- Exercise price per warrant in USD. EXCLUDE pre-funded warrants\n"
            "- null if not applicable\n\n"
            "### bank\n"
            "- Primary Placement Agent, Sales Agent, Book Running Manager, or Underwriter\n"
            "- null if not specified\n\n"
            "### potential_owners\n"
            "- Named investors, comma-separated. Exclude generic \"institutional investors\"\n"
            "- null if not specified\n\n"
            "### selling_shareholder_details\n"
            "- Should be 3-4 sentence summary with key numbers and dates\n\n"
            "## B) TICKER ATTRIBUTION (especially for news/press releases)\n"
            "- The Ticker field identifies which company this entry is recorded under\n"
            "- For news articles that mention multiple companies, verify the financing activity ACTUALLY belongs to the assigned ticker\n"
            "- If the article is about Company A's financing but the entry is recorded under Company B (who is only mentioned as a "
            "peer/comparable), flag this as a ticker mismatch — this is a critical error\n\n"
            "## C) DUPLICATE LABEL ACCURACY\n"
            "The `commentary` field contains the duplicate label. Verify it against the historical entries:\n"
            "- POTENTIAL DUPLICATE: headline + details are substantially similar to a historical entry\n"
            "- NOT DUPLICATE: no historical entry is substantially similar\n"
            "- N/A: no historical entries exist\n"
            "- Both \"NOT DUPLICATE\" and \"N/A\" are acceptable when no historical entries exist. Do NOT flag this.\n"
            "- Do NOT check duplicate labels for ATM USED headlines (these are frequent recurring entries and duplicates are expected).\n\n"
            "## D) OTHER HEADLINE HANDLING\n"
            "Filings with headline 'OTHER' (or 'OTHER, ...') should NOT be committed to the database (Done = False). "
            "HOWEVER, if the filing indicates adjustments to outstanding warrants or convertibles — such as warrants exercised "
            "or converted, amounts changed, conversion or exercise prices changed, etc. — then it still NEEDS ATTENTION. "
            "While the offering data row itself won't be committed, existing warrant/convertible records in the database may "
            "need to be updated. Flag this clearly in the Analysis.\n\n"
            "Output in this format:\n\n"
            "Done: True or False (if it should be committed to the main database)\n\n"
            "Status: OK or 'NEEDS ATTENTION'\n\n"
            "Analysis: short description of why its ok or needs attention\n\n"
            "Corrected / Missing Values:\n\n"
            "(in bullet format, list both corrected values AND any values that are present in the filing text but were "
            "not extracted — e.g. a share price, bank, investor name, etc. that appears in the source but is null/missing "
            "in the extracted data. Format:\n\n"
            "- headline: REGISTERED OFFERING (corrected)\n"
            "- shares_amount: XXX (corrected)\n"
            "- bank: Goldman Sachs (missing — found in filing)\n"
            "- potential_owners: Armistice Capital (missing — found in filing)\n\n"
            "If no corrections or missing values, write: No corrections needed.)\n\n"
            "Keep the whole output compact — this displays inside a small side panel."
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
        text_parts = []
        for block in parsed.get("content") or []:
            if block.get("type") == "text" and block.get("text"):
                text_parts.append(block["text"])
        return ("\n".join(text_parts)).strip() or "(no analysis returned)"

    @staticmethod
    def _coerce(raw, kind):
        if raw is None or raw == "":
            return None
        if kind == "numeric":
            if isinstance(raw, str):
                cleaned = raw.replace(",", "").replace("$", "").strip()
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
        if kind == "date":
            if isinstance(raw, datetime.date):
                return raw
            s = str(raw).strip()
            if not s:
                return None
            return datetime.date.fromisoformat(s[:10])
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
            self.send_response(e.code)
            self.send_header("Content-Type", "application/json")
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


def _ensure_columns():
    """Add columns that may not exist yet (idempotent)."""
    stmts = [
        "ALTER TABLE offering_data_new_final_clone ADD COLUMN IF NOT EXISTS doublecheck TEXT",
        "ALTER TABLE all_financings_final ADD COLUMN IF NOT EXISTS doublecheck TEXT",
    ]
    try:
        with get_conn() as conn, conn.cursor() as cur:
            for s in stmts:
                cur.execute(s)
            conn.commit()
    except Exception as e:
        sys.stderr.write(f"[server] column migration note: {e}\n")


def main():
    _ensure_columns()
    with ReusableTCPServer(("127.0.0.1", PORT), Handler) as httpd:
        print(f"Offerings Dashboard running at http://localhost:{PORT}/")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")


if __name__ == "__main__":
    main()
