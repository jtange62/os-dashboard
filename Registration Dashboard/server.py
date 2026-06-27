"""Registration Dashboard - local server with Postgres-backed review queue.

Run:  python server.py
Then open http://localhost:8769/ in a browser.

Configuration is loaded from `config.json` next to this file, or from the
parent OS Dashboard `config.json` when running from this sibling folder.
"""
import datetime
import decimal
import hashlib
import http.server
import http.cookies
import json
import secrets
import socketserver
import sys
import urllib.parse
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

PORT = int(_CFG.get("registration_dashboard_port", _CFG.get("port_registration", 8769)))
CHICAGO = zoneinfo.ZoneInfo("America/Chicago")

DB_CONFIG = {
    **_CFG["postgres"],
    "connect_timeout": 15,
    "application_name": "registration-dashboard",
}

AUTH_USERS = {
    "jmt415": hashlib.sha256(b"askedgar2115").hexdigest(),
}
SESSIONS = set()

_LOGIN_PAGE = """\
<!DOCTYPE html>
<html><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Login - Registration Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,sans-serif;background:#f5f6f8;display:flex;
     align-items:center;justify-content:center;min-height:100vh;color:#202938}
.card{background:#fff;padding:2rem;border:1px solid #d7dde5;border-radius:8px;
      box-shadow:0 8px 24px rgba(32,41,56,.12);width:340px}
h2{margin-bottom:1.2rem;font-size:1.25rem}
label{display:block;font-size:.85rem;color:#657184;margin-bottom:.3rem;font-weight:650}
input{width:100%%;padding:.55rem .7rem;border:1px solid #b9c3cf;border-radius:6px;
      font-size:.95rem;margin-bottom:1rem}
input:focus{outline:none;border-color:#1667b7;box-shadow:0 0 0 2px #e8f2ff}
button{width:100%%;padding:.6rem;background:#1667b7;color:#fff;border:none;
       border-radius:6px;font-size:1rem;font-weight:700;cursor:pointer}
button:hover{filter:brightness(.96)}
.err{color:#b93030;font-size:.85rem;margin-bottom:.8rem;display:%(err_display)s}
</style></head>
<body><div class="card">
<h2>Registration Dashboard</h2>
<div class="err">%(err_msg)s</div>
<form method="POST" action="/login">
<label for="u">Username</label><input id="u" name="username" autocomplete="username" autofocus>
<label for="p">Password</label><input id="p" name="password" type="password" autocomplete="current-password">
<button type="submit">Log in</button>
</form></div></body></html>
"""

REGISTRATION_FORMS = (
    "424B5",
    "S-1",
    "F-1",
    "S-3",
    "F-3",
    "F-10",
    "S-3ASR",
    "F-3ASR",
    "1-A",
    "SUPPL",
)

MAIN_COLUMNS = """
    ticker,
    headline,
    form_type,
    offering_type,
    "13f_filers",
    potential_owners,
    askedgar_url,
    selling_shareholder_details,
    amount_remaining_atm,
    offering_amount,
    file_no,
    bank,
    filed_at,
    is_atm,
    effective_status,
    primary_filing,
    document_url
"""

OFFERING_HISTORY_COLUMNS = """
    id,
    filed_at,
    headline,
    offering_amount,
    share_price,
    potential_owners,
    askedgar_url
"""

DILUTION_HISTORY_COLUMNS = """
    id,
    filed_at,
    details,
    registered,
    owners,
    file_no,
    warrants_exercise_price,
    warrants_amount,
    offering_amount,
    conversion_price
"""

# Field name -> (SQL identifier, type)
MAIN_EDITABLE_COLUMNS = {
    "headline": ("headline", "text"),
    "offering_type": ("offering_type", "text"),
    "13f_filers": ('"13f_filers"', "text"),
    "potential_owners": ("potential_owners", "text"),
    "selling_shareholder_details": ("selling_shareholder_details", "text"),
    "amount_remaining_atm": ("amount_remaining_atm", "numeric"),
    "offering_amount": ("offering_amount", "numeric"),
    "file_no": ("file_no", "text"),
    "bank": ("bank", "text"),
    "is_atm": ("is_atm", "boolean"),
    "effective_status": ("effective_status", "boolean"),
    "primary_filing": ("primary_filing", "boolean"),
}

OFFERING_EDITABLE_COLUMNS = {
    "potential_owners": "text",
}

DILUTION_EDITABLE_COLUMNS = {
    "registered": "text",
    "owners": "text",
    "file_no": "text",
}


def _json_default(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} is not JSON serializable")


def get_conn():
    return psycopg.connect(**DB_CONFIG)


def default_filed_at():
    return datetime.datetime.now(CHICAGO).date() - datetime.timedelta(days=1)


def main_review_sql():
    placeholders = ", ".join(["%s"] * len(REGISTRATION_FORMS))
    return f"""
        SELECT {MAIN_COLUMNS}
        FROM offering_data_main
        WHERE filed_at = %s
          AND form_type IN ({placeholders})
        ORDER BY filed_at DESC
        LIMIT 400
    """


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, fmt, *args):
        sys.stderr.write("%s - %s\n" % (self.log_date_time_string(), fmt % args))

    def _get_session(self):
        cookie_header = self.headers.get("Cookie", "")
        cookie = http.cookies.SimpleCookie()
        try:
            cookie.load(cookie_header)
        except http.cookies.CookieError:
            return None
        morsel = cookie.get("session")
        return morsel.value if morsel else None

    def _is_authenticated(self):
        token = self._get_session()
        return token is not None and token in SESSIONS

    def _send_login_page(self, error=""):
        html = _LOGIN_PAGE % {
            "err_display": "block" if error else "none",
            "err_msg": error,
        }
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _require_auth(self):
        if self._is_authenticated():
            return True
        self.send_response(302)
        self.send_header("Location", "/login")
        self.end_headers()
        return False

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = urllib.parse.parse_qs(parsed.query)
        try:
            if path == "/login":
                return self._send_login_page()
            if path == "/logout":
                token = self._get_session()
                if token:
                    SESSIONS.discard(token)
                self.send_response(302)
                self.send_header("Set-Cookie", "session=; Path=/; Max-Age=0")
                self.send_header("Location", "/login")
                self.end_headers()
                return
            if path == "/api/health":
                return self._send_json(200, {"status": "ok"})
            if not self._require_auth():
                return
            if path in ("/", "/dashboard.html"):
                self.path = "/dashboard.html"
                return super().do_GET()
            if path == "/api/review":
                return self._handle_review(params)
            if path == "/api/context":
                return self._handle_context(params)
            return super().do_GET()
        except Exception as exc:
            self._send_json(500, {"status": "error", "error": {"message": str(exc)}})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        try:
            if parsed.path == "/login":
                return self._handle_login()
            if not self._require_auth():
                return
            if parsed.path == "/api/main/update":
                return self._handle_main_update()
            if parsed.path == "/api/offerings/update":
                return self._handle_offerings_update()
            if parsed.path == "/api/dilution/update":
                return self._handle_dilution_update()
            self._send_json(404, {"status": "error", "error": {"message": "not found"}})
        except Exception as exc:
            self._send_json(500, {"status": "error", "error": {"message": str(exc)}})

    def _handle_login(self):
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length).decode("utf-8") if length else ""
        fields = urllib.parse.parse_qs(body, keep_blank_values=True)
        username = (fields.get("username") or [""])[0].strip()
        password = (fields.get("password") or [""])[0]
        if AUTH_USERS.get(username) == hashlib.sha256(password.encode("utf-8")).hexdigest():
            token = secrets.token_hex(32)
            SESSIONS.add(token)
            self.send_response(302)
            self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; SameSite=Lax")
            self.send_header("Location", "/")
            self.end_headers()
            return
        self._send_login_page("Invalid username or password.")

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
        if value is None or value == "":
            return None
        if kind == "numeric":
            return decimal.Decimal(str(value).replace(",", "").replace("$", "").strip())
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
        return str(value)

    def _handle_review(self, params):
        filed_at_raw = (params.get("filed_at", [""])[0] or "").strip()
        filed_at = (
            datetime.date.fromisoformat(filed_at_raw)
            if filed_at_raw
            else default_filed_at()
        )
        values = [filed_at, *REGISTRATION_FORMS]
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(main_review_sql(), values)
            rows = cur.fetchall()
        self._send_json(
            200,
            {
                "status": "success",
                "filed_at": filed_at,
                "count": len(rows),
                "results": rows,
            },
        )

    def _handle_context(self, params):
        document_url = (params.get("document_url", [""])[0] or "").strip()
        if not document_url:
            self._send_json(400, {"status": "error", "error": {"message": "document_url required"}})
            return

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT {MAIN_COLUMNS} FROM offering_data_main WHERE document_url = %s",
                (document_url,),
            )
            main_row = cur.fetchone()
            if not main_row:
                self._send_json(404, {"status": "error", "error": {"message": "registration row not found"}})
                return

            ticker = (main_row.get("ticker") or "").strip().upper()
            offerings = []
            dilution = []
            if ticker:
                cur.execute(
                    f"""
                    SELECT {DILUTION_HISTORY_COLUMNS}
                    FROM warrants_convertibles3
                    WHERE ticker = %s
                      AND verified = TRUE
                    ORDER BY filed_at DESC NULLS LAST, id DESC
                    """,
                    (ticker,),
                )
                dilution = cur.fetchall()

                cur.execute(
                    f"""
                    SELECT {OFFERING_HISTORY_COLUMNS}
                    FROM all_financings_final
                    WHERE ticker = %s
                      AND (
                        headline ILIKE %s
                        OR headline ILIKE %s
                        OR headline ILIKE %s
                      )
                    ORDER BY filed_at DESC NULLS LAST, id DESC
                    """,
                    (
                        ticker,
                        "%REGISTERED OFFERING%",
                        "%WARRANT EXERCISE%",
                        "%PRIVATE PLACEMENT%",
                    ),
                )
                offerings = cur.fetchall()

        self._send_json(
            200,
            {
                "status": "success",
                "result": {
                    "main": main_row,
                    "dilution": dilution,
                    "offerings": offerings,
                },
            },
        )

    def _handle_main_update(self):
        body = self._read_json_body()
        document_url = (body.get("document_url") or "").strip()
        updates = body.get("updates") or {}
        if not document_url:
            self._send_json(400, {"status": "error", "error": {"message": "document_url required"}})
            return
        if not isinstance(updates, dict) or not updates:
            self._send_json(400, {"status": "error", "error": {"message": "updates required"}})
            return

        sets = []
        values = []
        for field, raw in updates.items():
            if field not in MAIN_EDITABLE_COLUMNS:
                self._send_json(400, {"status": "error", "error": {"message": f"{field} is not editable"}})
                return
            identifier, kind = MAIN_EDITABLE_COLUMNS[field]
            sets.append(f"{identifier} = %s")
            values.append(self._coerce(raw, kind))
        values.append(document_url)

        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                UPDATE offering_data_main
                SET {', '.join(sets)}
                WHERE document_url = %s
                RETURNING {MAIN_COLUMNS}
                """,
                values,
            )
            row = cur.fetchone()
            conn.commit()
        if not row:
            self._send_json(404, {"status": "error", "error": {"message": "registration row not found"}})
            return
        self._send_json(200, {"status": "success", "result": row})

    def _handle_offerings_update(self):
        rows = self._read_update_rows()
        results = self._update_many(
            table="all_financings_final",
            pk="id",
            columns=OFFERING_EDITABLE_COLUMNS,
            returning=OFFERING_HISTORY_COLUMNS,
            rows=rows,
        )
        self._send_json(200, {"status": "success", "results": results})

    def _handle_dilution_update(self):
        rows = self._read_update_rows()
        results = self._update_many(
            table="warrants_convertibles3",
            pk="id",
            columns=DILUTION_EDITABLE_COLUMNS,
            returning=DILUTION_HISTORY_COLUMNS,
            rows=rows,
        )
        self._send_json(200, {"status": "success", "results": results})

    def _read_update_rows(self):
        body = self._read_json_body()
        if isinstance(body.get("rows"), list):
            rows = body["rows"]
        elif body.get("id") is not None:
            rows = [{"id": body.get("id"), "updates": body.get("updates") or {}}]
        else:
            rows = []
        if not rows:
            raise ValueError("rows required")
        for row in rows:
            if "id" not in row:
                raise ValueError("id required")
            if not isinstance(row.get("updates"), dict) or not row["updates"]:
                raise ValueError("updates required")
        return rows

    def _update_many(self, table, pk, columns, returning, rows):
        results = []
        with get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
            for row in rows:
                try:
                    row_id = int(row["id"])
                except (TypeError, ValueError):
                    raise ValueError("id must be an integer")

                sets = []
                values = []
                for field, raw in row["updates"].items():
                    if field not in columns:
                        raise ValueError(f"{field} is not editable")
                    sets.append(f"{field} = %s")
                    values.append(self._coerce(raw, columns[field]))
                values.append(row_id)
                cur.execute(
                    f"""
                    UPDATE {table}
                    SET {', '.join(sets)}
                    WHERE {pk} = %s
                    RETURNING {returning}
                    """,
                    values,
                )
                updated = cur.fetchone()
                if not updated:
                    raise ValueError(f"{table} row {row_id} not found")
                results.append(updated)
            conn.commit()
        return results


class ReusableTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    with ReusableTCPServer(("127.0.0.1", PORT), Handler) as httpd:
        print(f"Registration Dashboard running at http://localhost:{PORT}/")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down.")


if __name__ == "__main__":
    main()
