# OS Dashboard

Internal tool for the AskEdgar data team to research a ticker's historical float and outstanding shares, and to review records flagged by the data pipeline.

## What it is

A single-page browser dashboard backed by a tiny Python proxy server. The browser talks only to `http://localhost:8765` — the proxy holds the API keys and Postgres creds, calls the upstreams, and serves the static page.

Two tabs:

- **Research** — enter a ticker and get a snapshot, a chart, the historical float records, recent offerings, the latest ownership table (split-adjusted), and recent ownership-table filings.
- **Review Queue** — list of records to inspect and edit. Two modes: a daily queue filtered by `filed_at` + a category regex, or a ticker view that returns the full history of a single ticker. Edits write back to Postgres.

## Sibling app: Offerings Dashboard

[Offerings Dashboard/](Offerings%20Dashboard/) is a sister app in this same repo, running on port 8766. Same stack (Python proxy + single-file HTML + Postgres), different data: AskEdgar offerings/dilution/news endpoints on the Research tab, and two review sub-queues (offerings + warrants/convertibles). See [Offerings Dashboard/offeringwarrantssummary.md](Offerings%20Dashboard/offeringwarrantssummary.md) for the equivalent overview. Both apps can run side-by-side (8765 + 8766).

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
                         ├─ /api/historical-float        → eapi.askedgar.io/v1/historical-float-pro
                         ├─ /api/restricted-shares       → Postgres (just restricted_shares per document_url)
                         ├─ /api/ownership               → eapi.askedgar.io/v1/ownership
                         ├─ /api/offerings               → eapi.askedgar.io/v1/offerings
                         ├─ /api/reverse-splits          → eapi.askedgar.io/v1/reverse-splits
                         ├─ /api/filings                 → api.sec-api.io (Filing Query API, POST)
                         ├─ /api/historical-postgres     → Postgres (historical_float_final by ticker)
                         ├─ /api/review                  → Postgres
                         │     ?filed_at=YYYY-MM-DD      → daily mode (category regex filter)
                         │     ?ticker=X                 → ticker mode (full history, no other filters)
                         ├─ /api/review/by-document-urls → Postgres (fetch rows by PK list, used for pin merge)
                         └─ /api/review/update           → Postgres (UPDATE allowlisted columns)
```

The proxy decompresses gzipped upstream responses (sec-api.io serves gzip even without `Accept-Encoding`) and never forwards the raw API keys to the browser.

## Files

- `server.py` — proxy + Postgres wrapper. ~300 lines, stdlib `http.server` + `psycopg`.
- `dashboard.html` — single-file UI. Vanilla JS, Chart.js from CDN, no build step.
- `config.json` / `config.example.json` — runtime credentials. `config.json` is gitignored.
- `start.bat` — Windows launcher that ensures `psycopg` is installed.
- `SUMMARY.md` — human-readable overview of features.
- `credentials.md`, `sec-api-docs.md` — reference docs. `credentials.md` is gitignored.

## Data sources

### AskEdgar API (`eapi.askedgar.io`)

- `/v1/historical-float-pro` — float / OS / affiliate history. **Percent fields are decimal fractions** (0.718 = 71.8%); the UI multiplies by 100. Has no `restricted_shares` — we side-fetch from Postgres and merge by `document_url`.
- `/v1/ownership` — beneficial owner rows grouped by `reported_date`.
- `/v1/offerings` — recent share issuances.
- `/v1/reverse-splits` — `{execution_date, split_from, split_to}`. Adjustment factor = `split_to / split_from`.

Auth: `API-KEY: <key>` header.

### sec-api.io
- `https://api.sec-api.io` — Lucene query against EDGAR filings. POST with JSON body `{query, from, size, sort}`.
- Auth: `Authorization: <key>` header (no `Bearer`).
- Lucene phrase matching tokenizes hyphenated form types — `formType:"20-F"` will also match `"NT 20-F"`. The client filters results against an allow-list of strict matches (`OWNERSHIP_FORMS`).

### Postgres
- AWS Lightsail RDS, `postgres` DB. Connection requires `sslmode=require`. Host/user/password live in `config.json`.
- Primary table: `historical_float_final`. PK: `document_url` (single column).
- The `notes` column on this table is exposed as the analyst-editable notes field in the Review Queue UI — analyst notes persist directly in production data.
- `screener` is no longer joined in the daily query (the previous tradable-float / market-cap filter was replaced with the category regex).

## Review Queue specifics

### Daily mode (default)
Filter ([server.py](server.py) → `REVIEW_QUERY`):
```sql
WHERE form_type NOT LIKE '%MANUAL%'
  AND filed_at = :filed_at                              -- defaults to yesterday CDT
  AND category ~ 'NEGATIVE_FLOAT|HIGH|CRITICAL|SHARES_DECREASED|SHARES_MISMATCH'
ORDER BY date_final DESC
LIMIT 1000
```

### Ticker mode
Filter ([server.py](server.py) → `REVIEW_BY_TICKER_QUERY`):
```sql
WHERE ticker = :ticker
ORDER BY date_final DESC NULLS LAST, filed_at DESC NULLS LAST
LIMIT 1000
```
Intentionally no category / form / date filters — the analyst is opting into "show me everything for this ticker so I can fix older records I spot in the Research tab".

### Editable columns
Server-side allowlist in `EDITABLE_COLUMNS`:
- Numeric: `outstanding_shares`, `outstanding_shares_reported`, `float`, `tradable_float`, `affiliate_shares`, `restricted_shares`, `ads_ratio`
- Boolean: `is_adr`
- Text: `notes`

Adding a new editable column requires updating BOTH `EDITABLE_COLUMNS` (server, with type) and the row + panel renderers (client). The server allowlists; do not bypass it.

### Detail panel
Clicking any Review-Queue row opens a side panel with every field grouped by section: source filing / computed (read-only) / editable / details / prompt. The panel and the inline row editors share state — every editable input has `data-url` + `data-field` attributes, and `inputsFor(url, field)` finds all matching inputs across the row and panel. `onEditableChange` mirrors values, dirty class, and OS-sync warning to all matching inputs. Saving from either UI re-renders the row AND refreshes the panel.

Panel header has prev/next arrows + a "N / total" position indicator. Order is the insertion order of `reviewRowsByUrl` (pinned rows first in daily mode, then SQL results). Boundary buttons disable. Keyboard: `j`/`↓` next, `k`/`↑` previous, `Esc` close. Shortcuts skip when focus is in an input/textarea/select.

### Pin-on-save (daily mode only)
When a row is saved, its `document_url` goes into `localStorage` (`os-dashboard-pinned-urls`). On the next refresh, any pinned URL that no longer matches the SQL filter is fetched separately via `/api/review/by-document-urls` and shown in a faded "stale" row above the live queue. Click the `pinned ✕` badge to unpin. Skipped in ticker mode because the SQL already returns the full history.

This is per-browser — pins don't sync across analysts. If shared pins become useful, store them in a small Postgres table keyed by `(user_id, document_url)`.

### OS sync warning
`outstanding_shares` and `outstanding_shares_reported` are expected to be equal. If the analyst edits one and they diverge, both inputs turn yellow with a tooltip wherever they're rendered (row + panel); on save we prompt for confirmation before committing a mismatch.

## Research tab specifics

- **Big-change highlighting** in the Historical Float Records table flags any cell that changed ≥25% vs the next-older `reported_date`. Threshold is `BIG_CHANGE = 0.25` in [dashboard.html](dashboard.html). Applies to `outstanding_shares`, `float`, `tradable_float`, `affiliate_shares`, `restricted_shares`.
- **Negative float / tradable float** cells turn red.
- **Restricted shares column + chart line** are populated from a side-fetch to `/api/restricted-shares` (Postgres) merged by `document_url`. Rows where Postgres has no matching URL show "—".
- **Split-adjusted ownership** — for each ownership group, factor = Π(`split_to`/`split_from`) for splits with `execution_date > reported_date`. Each share-amount cell shows the adjusted value with `raw: N` beneath when the factor isn't 1. The aggregate `Outstanding`, the meta-row `Sum of common`, and the `<tfoot>` totals row all use the same factor.
- **Ownership totals row** sums only **non-investor** owners (i.e., owner_type containing "investor" or "10+" is excluded). The label reads `Total non-investor (N of M)` so it's obvious how many were filtered out.

## Quick-link pills next to ticker (`dilutionLinks`)

Rendered next to the ticker on the Research snapshot, the Review Queue row, and the detail-panel header.

| Pill | Destination | Behavior |
|------|-------------|----------|
| `DT` | `https://dilutiontracker.com/app/search/{TICKER}?a=ps7ydb` | new browser tab |
| `AE` | `https://app.askedgar.io/ticker/{TICKER}/dilution` | new browser tab |
| `RP` | `/?ticker={TICKER}` (this dashboard's Research tab) | new browser tab |
| `RQ` | switches to Review Queue ticker mode for `{TICKER}` | same tab; falls back to a real URL on middle-click |

The `<span class="dl-icons">` wrapper has `onclick="event.stopPropagation()"` so clicks on these in a Review row don't also open the detail panel.

## URL state

| Param | Meaning |
|-------|---------|
| `tab=research \| review` | which top-level tab is active |
| `ticker=X` | autoload ticker on Research tab |
| `rmode=date \| ticker` | Review Queue mode |
| `rt=X` | Review Queue ticker (only used when `rmode=ticker`) |

## Conventions for changes

- Prefer editing existing files. The UI is one HTML file by design — keep it that way unless it gets unmanageable.
- Don't pin `dashboard.html`'s state in localStorage beyond what's already there (`os-dashboard-pinned-urls`); the URL is the source of truth for tabs/tickers/modes.
- Numeric API/Postgres fields can arrive as `"123.45"` strings (psycopg JSON-encodes Decimal as string). Always coerce with the `num(v)` helper before doing math.
- New editable Review columns must be added to BOTH `EDITABLE_COLUMNS` (server) and the row + panel renderers (client).
- Postgres queries containing literal `%` need it doubled (`'%%MANUAL%%'`) because psycopg uses `%s` placeholders.
- New CSS classes inside the detail panel should NOT reuse names already used elsewhere (`.panel-grid` was renamed to `.panel-fields` after a clobbering bug — the legacy `.panel-grid` is the Research-tab vertical-stack wrapper).

## Things to know

- The chart pulls from AskEdgar, not Postgres, so it can't show `restricted_shares` directly — we side-fetch by ticker and merge by `document_url`. Rows whose URLs aren't in Postgres show "—".
- The dashboard never imports a frontend framework. Chart.js is the only external dependency, loaded from CDN.
- `start.bat` runs Python with `-u` (unbuffered) so server logs flush immediately when redirected.
- Server prints to stderr via `log_message` override; stdout has the startup banner.
- Killing the server: on Windows, `py.exe` is a launcher that spawns a child `python.exe`. Stopping the wrapper does NOT kill the child. Use `Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*server.py*" }` to find them all.
