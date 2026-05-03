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
PORT = int(_CFG.get("port", 8766))

ASKEDGAR_BASE = "https://eapi.askedgar.io"

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
    "period_ending": "date",
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
    financing_activity,
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


def main():
    with ReusableTCPServer(("127.0.0.1", PORT), Handler) as httpd:
        print(f"Offerings Dashboard running at http://localhost:{PORT}/")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")


if __name__ == "__main__":
    main()
