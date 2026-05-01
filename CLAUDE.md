# OS Dashboard

Internal tool for the AskEdgar data team to research a ticker's historical float and outstanding shares, and to review records flagged by the data pipeline.

## What it is

A single-page browser dashboard backed by a tiny Python proxy server. The browser talks only to `http://localhost:8765` — the proxy holds the API keys and Postgres creds, calls the upstreams, and serves the static page.

Two tabs:

- **Research** — enter a ticker and get a snapshot, a chart, the historical float records, recent offerings, the latest ownership table (split-adjusted), and recent ownership-table filings.
- **Review Queue** — date-driven list of records flagged by category for the analyst to inspect and edit. Edits write back to Postgres.

## Run it

```
py -3 server.py        # or double-click start.bat
```

`start.bat` auto-installs `psycopg[binary]` on first run, then opens the browser.

`config.json` (gitignored) holds the AskEdgar key, sec-api.io key, and Postgres creds. Copy `config.example.json` to `config.json` to bootstrap a fresh checkout.

## Architecture

```
Browser  ──>  http://localhost:8765/  (server.py)
                         │
                         ├─ /api/historical-float    → eapi.askedgar.io/v1/historical-float-pro
                         ├─ /api/restricted-shares   → Postgres (historical_float_final)
                         ├─ /api/ownership           → eapi.askedgar.io/v1/ownership
                         ├─ /api/offerings           → eapi.askedgar.io/v1/offerings
                         ├─ /api/reverse-splits      → eapi.askedgar.io/v1/reverse-splits
                         ├─ /api/filings             → api.sec-api.io (Filing Query API, POST)
                         ├─ /api/historical-postgres → Postgres (historical_float_final by ticker)
                         ├─ /api/review              → Postgres (filtered review query)
                         ├─ /api/review/by-document-urls → Postgres (fetch rows by PK list)
                         └─ /api/review/update       → Postgres (UPDATE allowlisted columns)
```

The proxy decompresses gzipped upstream responses (sec-api.io serves gzip even without `Accept-Encoding`) and never forwards the raw API keys to the browser.

## Files

- `server.py` — proxy + Postgres wrapper. ~280 lines, stdlib `http.server` + `psycopg`.
- `dashboard.html` — single-file UI. Vanilla JS, Chart.js from CDN, no build step.
- `config.json` / `config.example.json` — runtime credentials. `config.json` is gitignored.
- `start.bat` — Windows launcher that ensures `psycopg` is installed.
- `credentials.md`, `sec-api-docs.md` — reference docs. `credentials.md` is gitignored.

## Data sources

### AskEdgar API (eapi.askedgar.io)
- `/v1/historical-float-pro` — float / OS / affiliate history. **Note:** percent fields (`affiliate_percent`, `insider_percent`, `institutions_percent`) are **decimal fractions** (0.718 = 71.8%). UI multiplies by 100 for display. Has no `restricted_shares` field — we side-fetch from Postgres for that.
- `/v1/ownership` — beneficial owner rows grouped by `reported_date`.
- `/v1/offerings` — recent share issuances.
- `/v1/reverse-splits` — `{execution_date, split_from, split_to}`. Adjustment factor = `split_to / split_from`.

Auth: `API-KEY: <key>` header.

### sec-api.io
- `https://api.sec-api.io` — Lucene query against EDGAR filings. POST with JSON body `{query, from, size, sort}`.
- Auth: `Authorization: <key>` header (no `Bearer`).
- Lucene phrase matching tokenizes hyphenated form types — `formType:"20-F"` will also match `"NT 20-F"`. The client filters results against an allow-list of strict matches (`OWNERSHIP_FORMS`).

### Postgres
- Database: AWS Lightsail RDS, `postgres` DB. Connection requires `sslmode=require`. Host/user/password live in `config.json`.
- Table: `historical_float_final`. Primary key: `document_url` (single column).
- The `notes` column on this table is exposed as the analyst-editable notes field in the Review Queue UI — analyst notes persist directly in production data.

## Review Queue specifics

Current filter ([server.py](server.py) → `REVIEW_QUERY`):
```sql
WHERE form_type NOT LIKE '%MANUAL%'
  AND filed_at = :filed_at                              -- defaults to yesterday CDT
  AND category ~ 'NEGATIVE_FLOAT|HIGH|CRITICAL|SHARES_DECREASED|SHARES_MISMATCH'
ORDER BY date_final DESC
LIMIT 1000
```

Editable columns (server-side allowlist in `EDITABLE_COLUMNS`):
- Numeric: `outstanding_shares`, `outstanding_shares_reported`, `float`, `tradable_float`, `affiliate_shares`, `restricted_shares`, `ads_ratio`
- Boolean: `is_adr`
- Text: `notes`

### Pin-on-save
When a row is saved, its `document_url` goes into `localStorage` (`os-dashboard-pinned-urls`). On the next refresh, any pinned URL that no longer matches the SQL filter is fetched separately via `/api/review/by-document-urls` and shown in a faded "stale" row above the live queue. Click the `pinned ✕` badge to unpin.

This is per-browser — pins don't sync across analysts. If shared pins become useful, store them in a small Postgres table keyed by `(user_id, document_url)`.

### OS sync warning
`outstanding_shares` and `outstanding_shares_reported` are expected to be equal. If the analyst edits one and they diverge, both inputs turn yellow with a tooltip; on save we prompt for confirmation before committing a mismatch.

## Research tab specifics

- **Big change highlighting** in the Historical Float Records table flags any cell that changed ≥25% vs the next-older `reported_date`. Threshold is `BIG_CHANGE = 0.25` in [dashboard.html](dashboard.html).
- **Negative float / tradable float** cells turn red.
- **Split-adjusted ownership** — for each ownership group, factor = Π(split_to/split_from) for splits with `execution_date > reported_date`. Each share-amount cell shows the adjusted value with `raw: N` beneath when the factor isn't 1. The aggregate `Outstanding`, `Sum of common`, and the `<tfoot>` totals row all use the same factor.

## Conventions for changes

- Prefer editing existing files. The whole UI is one HTML file by design — keep it that way unless it gets unmanageable.
- Don't pin `dashboard.html`'s state in localStorage beyond what's already there (`os-dashboard-pinned-urls`); the URL is the source of truth for `?ticker=…&tab=…`.
- Numeric API fields can arrive as `"123.45"` strings (psycopg JSON-encodes Decimal as string). Always coerce with the `num(v)` helper before doing math.
- New editable Review columns must be added to BOTH `EDITABLE_COLUMNS` (server, with type) and the row renderer (client). The server allowlists; do not bypass it.
- Postgres queries containing literal `%` need it doubled (`'%%MANUAL%%'`) because psycopg uses `%s` placeholders.

## Things to know

- The chart pulls from AskEdgar, not Postgres, so it can't show `restricted_shares` directly — we side-fetch by ticker and merge by `document_url`. Rows whose URLs aren't in Postgres show "—".
- The dashboard never imports a frontend framework. Chart.js is the only external dependency, loaded from CDN.
- `start.bat` runs Python with `-u` (unbuffered) so server logs flush immediately when redirected.
- Server prints to stderr via `log_message` override; stdout has the startup banner.
