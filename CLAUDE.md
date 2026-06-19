# CLAUDE.md

These rules apply to every task in this project unless explicitly overridden.
Bias: caution over speed on non-trivial work. Use judgment on trivial tasks.

## Rule 1 — Think Before Coding
State assumptions explicitly. If uncertain, ask rather than guess.
Present multiple interpretations when ambiguity exists.
Push back when a simpler approach exists.
Stop when confused. Name what's unclear.

## Rule 2 — Simplicity First
Minimum code that solves the problem. Nothing speculative.
No features beyond what was asked. No abstractions for single-use code.
Test: would a senior engineer say this is overcomplicated? If yes, simplify.

## Rule 3 — Surgical Changes
Touch only what you must. Clean up only your own mess.
Don't "improve" adjacent code, comments, or formatting.
Don't refactor what isn't broken. Match existing style.

## Rule 4 — Goal-Driven Execution
Define success criteria. Loop until verified.
Don't follow steps. Define success and iterate.
Strong success criteria let you loop independently.

## Rule 5 — Use the model only for judgment calls
Use me for: classification, drafting, summarization, extraction.
Do NOT use me for: routing, retries, deterministic transforms.
If code can answer, code answers.

## Rule 6 — Token budgets are not advisory
Per-task: 4,000 tokens. Per-session: 30,000 tokens.
If approaching budget, summarize and start fresh.
Surface the breach. Do not silently overrun.

## Rule 7 — Surface conflicts, don't average them
If two patterns contradict, pick one (more recent / more tested).
Explain why. Flag the other for cleanup.
Don't blend conflicting patterns.

## Rule 8 — Read before you write
Before adding code, read exports, immediate callers, shared utilities.
"Looks orthogonal" is dangerous. If unsure why code is structured a way, ask.

## Rule 9 — Tests verify intent, not just behavior
Tests must encode WHY behavior matters, not just WHAT it does.
A test that can't fail when business logic changes is wrong.

## Rule 10 — Checkpoint after every significant step
Summarize what was done, what's verified, what's left.
Don't continue from a state you can't describe back.
If you lose track, stop and restate.

## Rule 11 — Match the codebase's conventions, even if you disagree
Conformance > taste inside the codebase.
If you genuinely think a convention is harmful, surface it. Don't fork silently.

## Rule 12 — Fail loud
"Completed" is wrong if anything was skipped silently.
"Tests pass" is wrong if any were skipped.
Default to surfacing uncertainty, not hiding it.

---

# OS Dashboard

Internal tool for the AskEdgar data team to research a ticker's historical float and outstanding shares, and to review records flagged by the data pipeline.

## What it is

A single-page browser dashboard backed by a tiny Python proxy server. The browser talks only to `http://localhost:8765` — the proxy holds the API keys and Postgres creds, calls the upstreams, and serves the static page.

Two tabs:

- **Research** — enter a ticker and get a snapshot, a chart, the historical float records, recent offerings, the latest ownership table (split-adjusted), and recent ownership-table filings.
- **Review Queue** — list of records to inspect and edit. Three modes: a daily queue filtered by `filed_at` + tiered filters, a ticker view that returns the full history of a single ticker, or a low-float view. Edits write back to Postgres.

## Sibling app: Offerings Dashboard

[Offerings Dashboard/](Offerings%20Dashboard/) is a sister app in this same repo, running on port 8766. Same stack (Python proxy + single-file HTML + Postgres), different data: AskEdgar offerings/dilution/news endpoints on the Research tab, and two review sub-queues (offerings + warrants/convertibles). See [Offerings Dashboard/offeringwarrantssummary.md](Offerings%20Dashboard/offeringwarrantssummary.md) for the equivalent overview. Both apps can run side-by-side (8765 + 8766).

## Run it

```
py -3 server.py        # or double-click start.bat
```

`start.bat` auto-installs `psycopg[binary]` on first run, then opens the browser.

`config.json` (gitignored) holds the AskEdgar key, sec-api.io key, Anthropic key, and Postgres creds. Copy `config.example.json` to `config.json` to bootstrap a fresh checkout.

## Architecture

```
Browser  ──>  http://localhost:8765/  (server.py)
                         │
                         ├─ /api/historical-float        → eapi.askedgar.io/v1/historical-float-pro
                         ├─ /api/restricted-shares       → Postgres (just restricted_shares per document_url)
                         ├─ /api/ownership               → eapi.askedgar.io/v1/ownership
                         ├─ /api/offerings               → eapi.askedgar.io/v1/offerings
                         ├─ /api/reverse-splits          → eapi.askedgar.io/v1/reverse-splits
                         ├─ /api/float-outstanding       → eapi.askedgar.io/v1/float-outstanding
                         ├─ /api/filings                 → api.sec-api.io (Filing Query API, POST)
                         ├─ /api/historical-postgres     → Postgres (historical_float_final by ticker)
                         ├─ /api/review                  → Postgres
                         │     ?filed_at=YYYY-MM-DD      → daily mode (tiered filter logic)
                         │     ?ticker=X                 → ticker mode (full history, no other filters)
                         │     ?mode=lowfloat            → low-float mode
                         ├─ /api/review/by-document-urls → Postgres (fetch rows by PK list, used for pin merge)
                         ├─ /api/review/update           → Postgres (UPDATE allowlisted columns)
                         └─ /api/ai-analyze              → Anthropic Claude (POST, generates AI analysis)
```

The proxy decompresses gzipped upstream responses (sec-api.io serves gzip even without `Accept-Encoding`) and never forwards the raw API keys to the browser.

## Files

- `server.py` — proxy + Postgres wrapper + AI analysis. stdlib `http.server` + `psycopg`.
- `dashboard.html` — single-file UI. Vanilla JS, Chart.js from CDN, no build step.
- `pre_analyze.py` — nightly batch script that pre-populates AI analysis for the daily queue. Runs via cron at 1 AM CST on Lightsail.
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
- `/v1/float-outstanding` — market cap, float, outstanding shares snapshot for a ticker. Used in the detail panel to display current market cap.

Auth: `API-KEY: <key>` header.

### sec-api.io
- `https://api.sec-api.io` — Lucene query against EDGAR filings. POST with JSON body `{query, from, size, sort}`.
- Auth: `Authorization: <key>` header (no `Bearer`).
- Lucene phrase matching tokenizes hyphenated form types — `formType:"20-F"` will also match `"NT 20-F"`. The client filters results against an allow-list of strict matches (`OWNERSHIP_FORMS`).

### Postgres
- AWS Lightsail RDS, `postgres` DB. Connection requires `sslmode=require`. Host/user/password live in `config.json`.
- Primary table: `historical_float_final`. PK: `document_url` (single column).
- The `notes` column on this table is exposed as the analyst-editable notes field in the Review Queue UI — analyst notes persist directly in production data.
- `screener` table is LEFT JOINed in the daily query to filter by exchange (NASDAQ/NYSE/AMEX) on the normal path. Sub-1M rows bypass this join.

## Review Queue specifics

### Daily mode (default)
Filter ([server.py](server.py) → `REVIEW_QUERY`):
```sql
FROM historical_float_final hf
LEFT JOIN screener s ON s.ticker = hf.ticker
    AND s.exchange IN ('NASDAQ', 'NYSE', 'AMEX')
WHERE
    hf.form_type NOT LIKE '%MANUAL%'
    AND hf.filed_at = :filed_at                   -- defaults to yesterday CDT
    AND hf.ticker IS NOT NULL
    AND (
        -- Sub-1M path: bypasses category AND exchange filters
        hf.outstanding_shares_final < 1000000
        OR hf.float_final < 1000000
        OR hf.tradable_float_final < 1000000
        -- Normal path: exchange + category + under-10M or ADR
        OR (
            s.ticker IS NOT NULL
            AND hf.category ~ 'NEGATIVE_FLOAT|HIGH|CRITICAL|SHARES_DECREASED|SHARES_MISMATCH'
            AND (
                hf.outstanding_shares_final < 10000000
                OR hf.float_final < 10000000
                OR hf.tradable_float_final < 10000000
                OR hf.ticker IN (SELECT DISTINCT ticker FROM historical_float_final WHERE is_adr = true)
            )
        )
    )
ORDER BY hf.date_final DESC
LIMIT 1000
```

**Client-side filters** (applied after fetch, no re-query):
- **Category dropdown**: All, Negative Float, Critical, High, Shares Decreased, Shares Mismatch, Other
- **AI Tier dropdown**: All, Critical, Discrepancy, Minor, No Change, No analysis

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
Clicking any Review-Queue row opens a **double-wide (1200px) side panel** with a two-column layout:
- **Left column**: Computed (read-only) values (OS Final, Float Final, Tradable Final) + Editable fields
- **Right column**: AI Analysis (with Regenerate link) + Source filing info (document link, ownership src, dates, category, flags, market cap from AskEdgar API)

Every editable input has `data-url` + `data-field` attributes, and `inputsFor(url, field)` finds all matching inputs across the row and panel. `onEditableChange` mirrors values, dirty class, and OS-sync warning to all matching inputs. Saving from either UI re-renders the row AND refreshes the panel.

Panel header has prev/next arrows + a "N / total" position indicator. Order is the insertion order of `reviewRowsByUrl` (pinned rows first in daily mode, then SQL results). Boundary buttons disable. Keyboard: `j`/`↓` next, `k`/`↑` previous, `Esc` close. Shortcuts skip when focus is in an input/textarea/select.

### AI Analysis
- **On-demand**: Opening a row's panel triggers AI analysis if `ai_analysis` is NULL. Uses `/api/ai-analyze` which calls Claude (Anthropic API) with filing context.
- **Batch**: `pre_analyze.py` runs nightly via cron at 1 AM CST, processes yesterday's daily queue rows. Skips already-analyzed rows. 10-second delay between requests, exponential backoff on rate limits (up to 8 retries).
- **AI tier column**: The queue table shows a color-coded tier badge (CRITICAL/DISCREPANCY/MINOR/NO CHANGE) extracted from the analysis text. Column is sortable.
- **ADR guidance**: The AI prompt notes that ADR tickers should have values expressed in ordinary shares, not ADRs. The `ads_ratio` field handles the conversion.

### Pin-on-save (daily mode only)
When a row is saved, its `document_url` goes into `localStorage` (`os-dashboard-pinned-urls`). On the next refresh, any pinned URL that no longer matches the SQL filter is fetched separately via `/api/review/by-document-urls` — but only if its `filed_at` matches the selected date. Click the `pinned ✕` badge to unpin. Skipped in ticker mode because the SQL already returns the full history.

This is per-browser — pins don't sync across analysts.

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

## Deployment

- **GitHub repo**: `https://github.com/jasontange/os-dashboard`
- **Lightsail instance**: `os-review-micro` — Ubuntu, Virginia Zone A
  - IP: `44.204.200.18`
  - SSH key: `C:\Users\jason\Downloads\LightsailDefaultKey-us-east-1.pem`
  - SSH: `ssh -i "C:\Users\jason\Downloads\LightsailDefaultKey-us-east-1.pem" ubuntu@44.204.200.18`
- **Repo on instance**: `/opt/os-dashboard-repo/` (symlinked from `/opt/os-dashboard` and `/opt/offerings-dashboard`)
- **Git auth on instance**: token embedded in the HTTPS remote URL (already configured). `git pull` works without extra credentials.
- **Systemd services**: `os-dashboard.service` (port 8765), `offerings-dashboard.service` (port 8766). Both set to `Restart=always`.
- **Cron job** (ubuntu user): `0 1 * * * /usr/bin/python3 /opt/os-dashboard/pre_analyze.py >> /opt/os-dashboard/pre_analyze.log 2>&1` — runs nightly batch AI analysis at 1 AM CST.
- **One-off batch scripts on the instance (NOT in git):** `/opt/os-dashboard/adr_analyze.py`, `lowfloat_analyze.py`, `reg_issuance.py` — ad-hoc AI-analysis backfills that query Postgres and POST to `/api/ai-analyze`. They reuse `config.json` and have hardcoded queries/date ranges (e.g. `reg_issuance.py` filters a fixed `filed_at` window), so edit before reuse. Left untracked on purpose; could be consolidated into one parameterized `batch_analyze.py` if they're worth keeping.
- **config.json**: gitignored — already present on the instance at both `/opt/os-dashboard-repo/config.json` and `/opt/os-dashboard-repo/Offerings Dashboard/config.json`. If credentials change, `scp` the updated file to the instance.
- **Deploy steps**: push to GitHub, then SSH in and pull + restart:
  ```
  ssh -i "C:\Users\jason\Downloads\LightsailDefaultKey-us-east-1.pem" ubuntu@44.204.200.18
  cd /opt/os-dashboard-repo && git pull
  sudo systemctl restart os-dashboard           # if server.py changed
  sudo systemctl restart offerings-dashboard    # if Offerings Dashboard/server.py changed
  ```
  Or as a one-liner from local:
  ```
  ssh -i "C:\Users\jason\Downloads\LightsailDefaultKey-us-east-1.pem" ubuntu@44.204.200.18 "cd /opt/os-dashboard-repo && git pull && sudo systemctl restart os-dashboard"
  ```

## Things to know

- The chart pulls from AskEdgar, not Postgres, so it can't show `restricted_shares` directly — we side-fetch by ticker and merge by `document_url`. Rows whose URLs aren't in Postgres show "—".
- The dashboard never imports a frontend framework. Chart.js is the only external dependency, loaded from CDN.
- `start.bat` runs Python with `-u` (unbuffered) so server logs flush immediately when redirected.
- Server prints to stderr via `log_message` override; stdout has the startup banner.
- Killing the server: on Windows, `py.exe` is a launcher that spawns a child `python.exe`. Stopping the wrapper does NOT kill the child. Use `Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like "*server.py*" }` to find them all.
- When the review tab is active, `body.review-active` disables page scroll so only the table scrolls (single scrollbar). Research tab scrolls normally.
