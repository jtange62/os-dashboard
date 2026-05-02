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
PORT = int(_CFG.get("port", 8765))

ASKEDGAR_BASE = "https://eapi.askedgar.io"
SECAPI_URL = "https://api.sec-api.io"

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
    restricted_shares, affiliate_shares, five_percent_shareholders, insider_shares,
    affiliate_percent, insider_percent, institutions_percent,
    is_adr, ads_ratio, foreign_company, number_classes, classes_details,
    ownership_source, source_date, details, category, market_cap, cik,
    notes, created_at, updated_at
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
    hf.market_cap,
    hf.updated_at
"""

REVIEW_QUERY = f"""
    SELECT {REVIEW_COLUMNS}
    FROM historical_float_final hf
    WHERE
        hf.form_type NOT LIKE '%%MANUAL%%'
        AND hf.filed_at = %(filed_at)s
        AND hf.category ~ 'NEGATIVE_FLOAT|HIGH|CRITICAL|SHARES_DECREASED|SHARES_MISMATCH'
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
        ticker = (params.get("ticker", [""])[0] or "").strip().upper()

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


def main():
    with ReusableTCPServer(("127.0.0.1", PORT), Handler) as httpd:
        print(f"OS Dashboard running at http://localhost:{PORT}/")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")


if __name__ == "__main__":
    main()
