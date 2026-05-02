# Offerings Dashboard

Internal tool for the AskEdgar data team to research a ticker's offerings and warrants/convertibles activity, and to review records flagged by the daily pipeline. Sister app to the OS Dashboard (which covers float / outstanding shares).

## What it is

A single-page browser dashboard backed by a Python proxy server. The browser only talks to `http://localhost:8766` — the proxy holds the AskEdgar API key and Postgres credentials, calls the upstreams, and serves the static page.

Two top-level tabs:

- **Research** — enter a ticker and get a snapshot, recent offerings (with funds & underwriters), warrants/convertibles dilution data, and recent news/filings. Click any news row to pop a side panel with the full AskEdgar summary.
- **Review Queue** — date- or ticker-driven analyst review of records flagged by the daily pipeline. Edits write back to Postgres. Two sub-queues:
  - **Offerings** — `offering_data_new_final_clone` (status field: `done`)
  - **Warrants & Convertibles** — `warrants_convertibles4` (status field: `verified`)

Sub-queues share the same mode toggle (Daily / By ticker), date picker, and ticker input. The status filter (Done / Verified) is per-sub-queue.

## Run it

```
py -3 server.py        # or double-click start.bat
```

`start.bat` auto-installs `psycopg[binary]` on first run, then opens the browser at http://localhost:8766/.

`config.json` (gitignored) holds the AskEdgar key and Postgres creds. Copy `config.example.json` to `config.json` to bootstrap a fresh checkout. The shape mirrors the OS Dashboard's config — `secapi_io_key` is unused here but harmless if present.

## Architecture

```
Browser  ──>  http://localhost:8766/  (server.py)
                         │
                         ├─ /api/offerings-advanced     → eapi.askedgar.io/v1/offerings-advanced
                         ├─ /api/dilution-data-advanced → eapi.askedgar.io/v1/dilution-data-advanced
                         ├─ /api/news-basic             → eapi.askedgar.io/v1/news-basic
                         │
                         ├─ /api/review                 → Postgres (offering_data_new_final_clone, filtered)
                         ├─ /api/review/by-ids          → Postgres (fetch by id list)
                         ├─ /api/review/update          → Postgres (UPDATE allowlisted columns)
                         │
                         ├─ /api/warrants               → Postgres (warrants_convertibles4, filtered)
                         ├─ /api/warrants/by-ids        → Postgres (fetch by id list)
                         └─ /api/warrants/update        → Postgres (UPDATE allowlisted columns)
```

The proxy decompresses gzipped AskEdgar responses and never forwards the raw API key to the browser.

## Files

- `server.py` — proxy + Postgres wrapper. ~430 lines, stdlib `http.server` + `psycopg`.
- `dashboard.html` — single-file UI. Vanilla JS, no build step, no Chart.js (no time-series chart in this app).
- `config.json` / `config.example.json` — runtime credentials. `config.json` is gitignored.
- `start.bat` — Windows launcher that ensures `psycopg` is installed.
- `offeringwarrantssummary.md` — this file.

## Data sources

### AskEdgar API (eapi.askedgar.io)

- `/v1/offerings-advanced` — offerings with `investors` and `bank` columns added (vs the basic `/v1/offerings`).
- `/v1/dilution-data-advanced` — mixed list of `WarrantPro` and `ConvertiblePro` records. The UI splits them client-side based on which fields are populated.
- `/v1/news-basic` — recent filings by ticker. The `summary` field is rendered through a light markdown helper (`**bold**`, `*` bullets, `1.) header` lines) into the news side panel.

Auth: `API-KEY: <key>` header.

### Postgres

- Database: same Lightsail RDS as the OS Dashboard (`sslmode=require`).
- Tables:
  - `offering_data_new_final_clone` — daily offerings pipeline output. PK: `id` (auto-increment).
  - `warrants_convertibles4` — daily warrants/convertibles pipeline output. PK: `id` (auto-increment).
- Both tables have `filed_at`, `updated_at`, and a boolean status column (`done` / `verified`).

## Review Queue specifics

### Offerings sub-queue

Filter (when `done` filter not "All"):
```sql
SELECT ...
FROM offering_data_new_final_clone
WHERE filed_at = :filed_at
  AND financing_activity = TRUE
  [AND done = TRUE | AND done = FALSE | AND done IS NULL | AND done IS NOT NULL]
ORDER BY ticker ASC
LIMIT 1000
```

Editable columns (server-side allowlist in `EDITABLE_COLUMNS`):
- Numeric: `offering_amount`, `share_price`, `conversion_price`, `shares_amount`, `warrants_amount`, `warrants_exercise_price`
- Boolean: `done`
- Text: `ticker`, `headline`, `selling_shareholder_details`, `commentary`, `disclosure_type`, `askedgar_url`, `potential_owners`, `bank`, `form_type`

Default status filter: `done = true`.

### Warrants & Convertibles sub-queue

Filter (when `verified` filter not "All"):
```sql
SELECT ...
FROM warrants_convertibles4
WHERE filed_at = :filed_at
  [AND verified = TRUE | AND verified = FALSE | AND verified IS NULL | AND verified IS NOT NULL]
ORDER BY ticker ASC
LIMIT 1000
```

Editable columns (server-side allowlist in `WARRANTS_EDITABLE_COLUMNS`):
- Numeric: `warrants_amount`, `warrants_exercise_price`, `prefunded_cost`, `offering_amount`, `conversion_price`
- Boolean: `verified`
- Date: `expiration_date`, `exercisable_date`, `maturity_date`, `convertible_date`
- Text: `ticker`, `form_type`, `details`, `selling_shareholder_details`, `owners_two`, `askedgar_url`, `pp_clause`, `price_protection`, `owners`, `bank`

Default status filter: `verified = true`.

`details` plays the same role as `headline` does for offerings. `link_to_ticker` is not a real column on `warrants_convertibles4` — if you've seen it in a Retool query, that was a derived alias, not a stored value, and it's intentionally absent from this app.

### Ticker mode (both sub-queues)

Same shape: `WHERE ticker = :ticker [AND status_filter] ORDER BY filed_at DESC NULLS LAST, id DESC LIMIT 1000`. No `financing_activity` filter on offerings ticker mode — analyst can fix older records flagged through any path.

### Pin-on-save

When a row is saved, its `id` goes into `localStorage` under a per-sub-queue key (`offerings-dashboard-pinned-ids` and `offerings-dashboard-pinned-warrants`). On the next refresh, any pinned ID that no longer matches the SQL filter is fetched separately via `/api/.../by-ids` and shown faded as "stale" above the live queue. Click `pinned ✕` to unpin. Per-browser only; not shared across analysts.

### Detail panel

Slide-in panel (560 px wide) with the same edits as the row, plus prev/next navigation (`j` / `k` / arrow keys / Esc). Switching sub-tabs closes the panel automatically — different tables, different field sets.

## Research tab specifics

The snapshot card row summarises the ticker: latest offering form/$/shares, recent offering count + summed amount, open warrants count + remaining, open convertibles count + debt remaining, latest news date.

The dilution-data table is split into Warrants and Convertibles client-side based on which fields each row populates (warrants get `warrants_*` and dates; convertibles get `conversion_price`, `maturity_date`, `convertible_*`).

The news section pops a side panel with the AskEdgar summary on click. The summary is markdown-ish — light renderer turns `**bold**` into `<strong>`, `*` lines into list items, and "1.) Header" lines bold.

## Quick-link icons next to tickers

Same convention as OS Dashboard, with one addition:

- **DT** — DilutionTracker (external)
- **AE** — AskEdgar dilution page (external)
- **OS** — OS Dashboard at `localhost:8765` (sister app, cross-check float/OS data)
- **RP** — Research tab in this app (new browser tab)
- **RQ** — Review Queue ticker mode in this app

## Conventions for changes

- Single-file HTML by design — keep it that way unless it gets unmanageable.
- The two review queues are wired through a queue-config object pattern (`OFFERINGS_QUEUE` / `WARRANTS_QUEUE`). To add a third queue, define a new config (api endpoints, pinKey, status field, column type lists, render functions) and add it to `QUEUES`.
- New editable columns must be added to BOTH the server allowlist (`EDITABLE_COLUMNS` or `WARRANTS_EDITABLE_COLUMNS`, with type) AND the queue config's type list (`numeric` / `boolean` / `text` / `date`) AND the queue's `renderRow` / `renderPanel`. The server allowlists; do not bypass it.
- Date columns use `<input type="date">` and serialise as `YYYY-MM-DD` to/from the server's `_coerce(..., "date")`.
- Numeric Postgres values arrive as Decimal-as-string (psycopg encodes Decimal as a JSON string). Always run them through `num()` before math.
- Postgres queries containing literal `%` need it doubled (`'%%foo%%'`) because psycopg uses `%s` placeholders.
- The status filter URL params (`rdone`, `rverified`) are written explicitly (including empty for "All") so refreshes preserve the user's choice — they override the per-queue default.
- URL is the source of truth for tab/sub-tab/mode/ticker/status. Only pinned IDs go in localStorage.

## Things to know

- This app does not import any frontend framework. There's no chart in this app (offerings don't have a meaningful single-value time series the way float / OS does).
- AskEdgar percent fields can arrive as decimal fractions in some endpoints — none of the offerings/dilution endpoints used here have percent fields, so there's no `× 100` adjustment needed. (Different from OS Dashboard.)
- Both apps share the same Postgres credentials. Running them concurrently on different ports (8765 + 8766) is fine.
- Future integration: the natural merge point is dropping the Research-tab quick links and combining the Review Queue tabs in one app with three or more sub-queues (OS / Offerings / Warrants).
