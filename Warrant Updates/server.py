"""Warrant Updates Dashboard - local server with API proxy + Postgres editing.

Run:  python server.py
Then open http://localhost:8767/ in a browser.

Configuration is loaded from `config.json` next to this file, or from the
parent OS Dashboard `config.json` when running from this sibling folder.
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
    CONFIG_PATH = ROOT.parent / "config.json"

if not CONFIG_PATH.exists():
    sys.stderr.write(
        "Missing config.json. Copy config.example.json to config.json, or keep "
        "one in the parent OS Dashboard folder.\n"
    )
    sys.exit(1)

with CONFIG_PATH.open(encoding="utf-8") as _f:
    _CFG = json.load(_f)

ASKEDGAR_KEY = _CFG["askedgar_api_key"]
PORT = int(_CFG.get("warrant_updates_port", _CFG.get("port_warrant_updates", 8767)))
ASKEDGAR_BASE = "https://eapi.askedgar.io"
CHICAGO = zoneinfo.ZoneInfo("America/Chicago")

DB_CONFIG = {
    **_CFG["postgres"],
    "connect_timeout": 15,
    "application_name": "warrant-updates-dashboard",
}

ADJUSTMENT_COLUMNS = """
    a.id,
    a.ticker,
    a.form_type,
    a.document_url,
    a.filed_at,
    a.type,
    a.details,
    a.change,
    a.new_warrants_amount,
    a.new_warrants_remaining,
    a.new_warrants_exercise_price,
    a.new_offering_amount,
    a.new_convertible_debt_remaining,
    a.new_conversion_price,
    a.new_pp_clause,
    a.summary,
    a.floor_price,
    a.confidence_level,
    a.match_status,
    a.match_basis,
    a.review_status,
    a.review_notes,
    a.reviewed_at,
    s.price AS last_sale_price,
    s.market_cap_final,
    COALESCE(m.match_count, 0) AS wc3_match_count,
    mwc.id AS matched_wc3_id
"""

WC3_COLUMNS = """
    id,
    ticker,
    filed_at,
    form_type,
    details,
    selling_shareholder_details,
    warrants_amount,
    warrants_remaining,
    warrants_exercise_price,
    conversion_price,
    offering_amount,
    convertible_debt_remaining,
    expiration_date,
    maturity_date,
    pp_clause,
    price_protection,
    floor_price,
    derivative_type,
    verified,
    owners,
    owners_two,
    registered,
    notes,
    document_url,
    askedgar_url,
    file_no,
    updated_at
"""

WC3_EDITABLE_COLUMNS = {
    "ticker": "text",
    "filed_at": "date",
    "form_type": "text",
    "details": "text",
    "document_url": "text",
    "askedgar_url": "text",
    "derivative_type": "text",
    "verified": "boolean",
    "pp_clause": "text",
    "warrants_exercise_price": "numeric",
    "warrants_amount": "numeric",
    "warrants_remaining": "numeric",
    "expiration_date": "date",
    "conversion_price": "numeric",
    "offering_amount": "numeric",
    "convertible_debt_remaining": "numeric",
    "maturity_date": "date",
}

REVIEW_STATUS_VALUES = {"corrected", "no_change", "added_new", "need_help"}


def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} is not JSON serializable")


def get_conn():
    return psycopg.connect(**DB_CONFIG)


def normalize_derivative_type(value):
    text = (value or "").strip().lower().replace(" ", "_")
    if text in {"warrant", "warrants"}:
        return "warrant"
    if text in {"convertible_debt", "convertible_note", "debt"}:
        return "convertible_debt"
    if text in {"convertible_preferred", "preferred"}:
        return "convertible_preferred"
    if "preferred" in text:
        return "convertible_preferred"
    if "debt" in text or "note" in text or "debenture" in text:
        return "convertible_debt"
    return "warrant"


def adjustment_join_sql(where_sql):
    return f"""
        SELECT {ADJUSTMENT_COLUMNS}
        FROM warrants_convertibles_adjustments a
        LEFT JOIN screener s ON s.ticker = a.ticker
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS match_count
            FROM warrants_convertibles3 wc
            WHERE wc.ticker = a.ticker
              AND wc.details = a.details
              AND a.details IS NOT NULL
              AND a.details <> 'No Match'
        ) m ON TRUE
        LEFT JOIN LATERAL (
            SELECT wc.id
            FROM warrants_convertibles3 wc
            WHERE wc.ticker = a.ticker
              AND wc.details = a.details
              AND a.details IS NOT NULL
              AND a.details <> 'No Match'
            ORDER BY wc.filed_at DESC NULLS LAST, wc.id DESC
            LIMIT 1
        ) mwc ON TRUE
        WHERE {where_sql}
        ORDER BY a.filed_at DESC NULLS LAST, a.ticker ASC, a.details ASC, a.id DESC
        LIMIT 1000
    """


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.log_date_time_string(), fmt % args))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        try:
            if path in ("/", "/dashboard.html"):
                self.path = "/dashboard.html"
                return super().do_GET()
            if path == "/api/adjustments":
                return self._handle_adjustments(params)
            if path == "/api/context":
                return self._handle_context(params)
            if path == "/api/dilution-data-advanced":
                return self._handle_dilution(params)
            if path == "/api/health":
                return self._send_json(200, {"status": "ok"})
            return super().do_GET()
        except Exception as exc:
            self._send_json(500, {"status": "error", "error": {"message": str(exc)}})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        try:
            if parsed.path == "/api/wc3/update":
                return self._handle_wc3_update()
            if parsed.path == "/api/wc3/create":
                return self._handle_wc3_create()
            if parsed.path == "/api/adjustments/update-review":
                return self._handle_adjustment_review_update()
            self._send_json(404, {"status": "error", "error": {"message": "not found"}})
        except Exception as exc:
            self._send_json(500, {"status": "error", "error": {"message": str(exc)}})

    def _send_json(self, status, data):
        body = json.dumps(data, default=_json_default).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    @staticmethod
    def _coerce(value, kind):
        if value == "":
            return None
        if value is None:
            return None
        if kind == "numeric":
            return decimal.Decimal(str(value).replace(",", ""))
        if kind == "boolean":
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in {"true", "1", "yes", "y"}:
                return True
            if text in {"false", "0", "no", "n"}:
                return False
            return None
        if kind == "date":
            return datetime.date.fromisoformat(str(value)[:10])
        if kind == "text":
            return str(value)
        return value

    def _handle_adjustments(self, params):
        ticker = (params.get("ticker", [""])[0] or "").strip().upper()
        filed_at_raw = (params.get("filed_at", [""])[0] or "").strip()
        review_status = (params.get("review_status", ["open"])[0] or "open").strip().lower()

        values = {}
        clauses = ["a.change <> 'NONE'"]

        if filed_at_raw:
            values["filed_at"] = datetime.date.fromisoformat(filed_at_raw)
            clauses.append("a.filed_at = %(filed_at)s")
        elif not ticker:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(filed_at) FROM warrants_convertibles_adjustments "
                    "WHERE change <> 'NONE'"
                )
                filed_at = cur.fetchone()[0]
            if filed_at is None:
                filed_at = datetime.datetime.now(CHICAGO).date()
            values["filed_at"] = filed_at
            clauses.append("a.filed_at = %(filed_at)s")

        if ticker:
            values["ticker"] = ticker
            clauses.append("a.ticker = %(ticker)s")

        if review_status == "open":
            clauses.append("a.review_status IS NULL")
        elif review_status in REVIEW_STATUS_VALUES:
            values["review_status"] = review_status
            clauses.append("a.review_status = %(review_status)s")
        elif review_status not in {"", "all"}:
            self._send_json(400, {"status": "error", "error": {"message": "bad review_status"}})
            return

        where = " AND ".join(clauses)
        if ticker and "filed_at" in values:
            mode = "date_ticker"
        elif ticker:
            mode = "ticker"
        else:
            mode = "date"

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(adjustment_join_sql(where), values)
            rows = cur.fetchall()

        self._send_json(
            200,
            {
                "status": "success",
                "mode": mode,
                "filed_at": values.get("filed_at"),
                "ticker": ticker or None,
                "review_status": review_status,
                "count": len(rows),
                "results": rows,
            },
        )

    def _handle_adjustment_review_update(self):
        body = self._read_json_body()
        adjustment_id = body.get("id")
        updates = body.get("updates") or {}
        try:
            adjustment_id = int(adjustment_id)
        except (TypeError, ValueError):
            self._send_json(400, {"status": "error", "error": {"message": "id required"}})
            return
        if not isinstance(updates, dict):
            self._send_json(400, {"status": "error", "error": {"message": "updates required"}})
            return

        allowed = {"review_status", "review_notes"}
        sets = []
        values = []
        for col, raw in updates.items():
            if col not in allowed:
                self._send_json(400, {"status": "error", "error": {"message": f"{col} is not editable"}})
                return
            value = None if raw in (None, "") else str(raw)
            if col == "review_status" and value is not None and value not in REVIEW_STATUS_VALUES:
                self._send_json(400, {"status": "error", "error": {"message": "bad review_status"}})
                return
            sets.append(f"{col} = %s")
            values.append(value)

        if not sets:
            self._send_json(400, {"status": "error", "error": {"message": "updates required"}})
            return

        sets.append("reviewed_at = NOW()")
        values.append(adjustment_id)

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"UPDATE warrants_convertibles_adjustments SET {', '.join(sets)} "
                "WHERE id = %s",
                values,
            )
            cur.execute(adjustment_join_sql("a.id = %(id)s"), {"id": adjustment_id})
            row = cur.fetchone()
            conn.commit()

        if not row:
            self._send_json(404, {"status": "error", "error": {"message": "adjustment not found"}})
            return
        self._send_json(200, {"status": "success", "result": row})

    def _handle_context(self, params):
        raw_id = params.get("id", [""])[0]
        try:
            adjustment_id = int(raw_id)
        except (TypeError, ValueError):
            self._send_json(400, {"status": "error", "error": {"message": "id required"}})
            return

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(adjustment_join_sql("a.id = %(id)s"), {"id": adjustment_id})
            adjustment = cur.fetchone()
            if not adjustment:
                self._send_json(404, {"status": "error", "error": {"message": "adjustment not found"}})
                return

            cur.execute(
                f"SELECT {WC3_COLUMNS} FROM warrants_convertibles3 "
                "WHERE ticker = %s AND details = %s "
                "ORDER BY filed_at DESC NULLS LAST, id DESC LIMIT 20",
                (adjustment["ticker"], adjustment["details"]),
            )
            exact_matches = cur.fetchall()

            cur.execute(
                f"SELECT {WC3_COLUMNS} FROM warrants_convertibles3 "
                "WHERE ticker = %s "
                "ORDER BY filed_at DESC NULLS LAST, id DESC LIMIT 40",
                (adjustment["ticker"],),
            )
            candidates = cur.fetchall()

        self._send_json(
            200,
            {
                "status": "success",
                "adjustment": adjustment,
                "exact_matches": exact_matches,
                "candidates": candidates,
            },
        )

    def _handle_wc3_update(self):
        body = self._read_json_body()
        row_id = body.get("id")
        updates = body.get("updates") or {}
        try:
            row_id = int(row_id)
        except (TypeError, ValueError):
            self._send_json(400, {"status": "error", "error": {"message": "id required"}})
            return
        if not isinstance(updates, dict) or not updates:
            self._send_json(400, {"status": "error", "error": {"message": "updates required"}})
            return

        sets = []
        values = []
        for col, raw in updates.items():
            if col not in WC3_EDITABLE_COLUMNS:
                self._send_json(400, {"status": "error", "error": {"message": f"{col} is not editable"}})
                return
            kind = WC3_EDITABLE_COLUMNS[col]
            value = self._coerce(raw, kind)
            if col == "derivative_type":
                value = normalize_derivative_type(value) if value is not None else None
            sets.append(f"{col} = %s")
            values.append(value)
        sets.append("updated_at = NOW()")
        values.append(row_id)

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"UPDATE warrants_convertibles3 SET {', '.join(sets)} "
                f"WHERE id = %s RETURNING {WC3_COLUMNS}",
                values,
            )
            row = cur.fetchone()
            conn.commit()

        if not row:
            self._send_json(404, {"status": "error", "error": {"message": "row not found"}})
            return
        self._send_json(200, {"status": "success", "result": row})

    def _handle_wc3_create(self):
        body = self._read_json_body()
        fields = body.get("fields") or {}
        if not isinstance(fields, dict):
            self._send_json(400, {"status": "error", "error": {"message": "fields required"}})
            return

        fields["verified"] = True
        fields["derivative_type"] = normalize_derivative_type(fields.get("derivative_type"))
        if not fields.get("ticker"):
            self._send_json(400, {"status": "error", "error": {"message": "ticker required"}})
            return
        if not fields.get("filed_at"):
            self._send_json(400, {"status": "error", "error": {"message": "filed_at required"}})
            return
        if not fields.get("details"):
            self._send_json(400, {"status": "error", "error": {"message": "details required"}})
            return

        cols = []
        values = []
        for col, raw in fields.items():
            if col not in WC3_EDITABLE_COLUMNS:
                self._send_json(400, {"status": "error", "error": {"message": f"{col} is not editable"}})
                return
            cols.append(col)
            values.append(self._coerce(raw, WC3_EDITABLE_COLUMNS[col]))

        placeholders = ", ".join(["%s"] * len(cols))
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"INSERT INTO warrants_convertibles3 ({', '.join(cols)}) "
                f"VALUES ({placeholders}) RETURNING {WC3_COLUMNS}",
                values,
            )
            row = cur.fetchone()
            conn.commit()
        self._send_json(200, {"status": "success", "result": row})

    def _handle_dilution(self, params):
        ticker = (params.get("ticker", [""])[0] or "").strip().upper()
        if not ticker:
            self._send_json(400, {"status": "error", "error": {"message": "ticker required"}})
            return
        limit = (params.get("limit", ["100"])[0] or "100").strip()
        page = (params.get("page", ["1"])[0] or "1").strip()
        data = self._askedgar_get(
            "/v1/dilution-data-advanced",
            {"ticker": ticker, "limit": limit, "page": page},
        )
        self._send_json(200, data)

    def _askedgar_get(self, path, params):
        query = urllib.parse.urlencode(params)
        url = f"{ASKEDGAR_BASE}{path}?{query}"
        req = urllib.request.Request(url, headers={"API-KEY": ASKEDGAR_KEY})
        try:
            with urllib.request.urlopen(req, timeout=45) as res:
                raw = res.read()
                enc = (res.headers.get("Content-Encoding") or "").lower()
                if enc == "gzip":
                    raw = gzip.decompress(raw)
                elif raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                elif raw[:2] in (b"\x78\x9c", b"\x78\xda"):
                    raw = zlib.decompress(raw)
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"AskEdgar HTTP {exc.code}: {body[:300]}")


def main():
    with socketserver.ThreadingTCPServer(("", PORT), Handler) as httpd:
        httpd.daemon_threads = True
        print(f"Warrant Updates running at http://localhost:{PORT}/")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")


if __name__ == "__main__":
    main()
