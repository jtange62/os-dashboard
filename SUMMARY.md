# OS Dashboard — Feature Summary

A local web dashboard for the AskEdgar data team to research a ticker's float / outstanding-share history and to review records flagged by the data pipeline.

Run `start.bat` (or `py -3 server.py`), open http://localhost:8765/, type a ticker, hit Research.

---

## Research Tab

Type a ticker in the top-right input. Everything below loads in parallel.

### Latest Snapshot
A row of cards: Ticker, Outstanding, Float, Tradable Float, Affiliate %, Restricted, Insider %, ADS Ratio, Market Cap, plus a link to the source filing. Float / Tradable Float go red if the value is negative. Affiliate % colors yellow at ≥30% and orange at ≥50%. Flags row at the top calls out Foreign / ADR / multiple share classes / negative float.

### Historical Float & Outstanding Shares (chart)
Line chart with five series — Outstanding, Float, Tradable Float, Affiliate Shares, Restricted Shares — sorted by reported date.

### Historical Float Records (table)
Per-filing history sorted by `reported_date` DESC. Shows form, OS, Float, Tradable, Affiliate Shares, Restricted, Aff %, Insider %, ADR, ADS Ratio, ownership-source link, filing link.

- **Yellow** highlight on cells where the value changed ≥25% vs the next-older record (Outstanding / Float / Tradable / Affiliate / Restricted).
- **Red** highlight on negative Float or Tradable Float.

### Recent Offerings
Up to 5 most recent offerings — date, form, headline (with expandable selling-shareholder details), shares, share price, warrants, conversion price, amount raised, link to filing. Helps spot recent share issuances.

### Latest Ownership (split-adjusted)
The most recent beneficial-ownership table grouped by `reported_date`. If reverse splits occurred AFTER that reported date, every share count is adjusted to current basis (multiplier = Π `split_to / split_from`) and shown alongside the raw value. A yellow note above the table reports the splits applied.

- Footer row sums **only non-investor owners** (Director, Executive, etc.) — investor-class rows like "10+ Percent Investor" are excluded so the total approximates affiliate holdings.
- `% of OS` column on each row uses `common / outstanding_shares_final` from the same filing (basis-consistent).

### Recent Filings With Ownership Tables
Most recent filings whose form types typically contain ownership tables: S-1, F-1, DEF 14A, PRE 14A, DEF 14C, PRE 14C, 10-K, 20-F (plus `/A` amendments). Each row links straight to the filing (re-formatted as an AskEdgar URL).

---

## Review Queue Tab

Daily list of records flagged for review, with full inline editing back to Postgres.

### Two modes (segmented control at the top)

**Daily queue** — defaults to yesterday's CDT date. Shows records where:
- `form_type` is not `MANUAL`
- `filed_at` matches the chosen date
- `category` contains at least one of: `NEGATIVE_FLOAT`, `HIGH`, `CRITICAL`, `SHARES_DECREASED`, `SHARES_MISMATCH` (case-sensitive)

Date input + Today / Yesterday (CDT) buttons.

**By ticker** — enter a ticker, get its full history from `historical_float_final` (no category filter, no date filter, no MANUAL exclusion). Used when researching a ticker turns up an older record that needs fixing. `← Back to daily queue` button to switch back.

### Inline table
Each row exposes editable inputs for: `outstanding_shares`, `outstanding_shares_reported`, `float`, `tradable_float`, `restricted_shares`, `affiliate_shares`, `ads_ratio`, `is_adr`, plus a `notes` textarea. Read-only computed columns (OS Final / Float Final / Tradable Final) flag negatives in red.

### Detail panel (Retool-style)
Click a row to open a side panel with every field, broken into Source filing / Computed / Editable / Details / Prompt sections. All editable fields are editable in the panel too, and they stay in sync with the row inputs (typing in either updates both, plus the dirty/warn state).

Panel header has:
- `↑` / `↓` arrows + a `N / total` position indicator → step through rows without closing the panel.
- Keyboard: `j` or `↓` next, `k` or `↑` previous, `Esc` to close.
- `Save` / `Revert` / `×` close.
- The active row in the table is left-bordered + tinted so it's obvious which one is open.

### OS sync warning
`outstanding_shares` and `outstanding_shares_reported` are expected to match. If they diverge after editing, both inputs turn yellow with a tooltip; on save you get a confirmation prompt.

### Pin-on-save (daily mode)
When a row is saved, its filing URL is pinned in `localStorage`. Next refresh, any pinned URL that no longer matches the daily filter is fetched separately and shown faded with a `pinned ✕  stale` badge so you don't lose track of records you just fixed. Click the badge to unpin.

Pins are per-browser. Ticker mode skips this since the full history is already returned.

### Notes column
Notes are stored directly in `historical_float_final.notes` so they persist in production data and can be reviewed via the same DB.

---

## Quick-link pills next to every ticker

Wherever a ticker is shown (Research snapshot, Review Queue rows, detail-panel header), there's a row of compact link badges:

| Pill | What it does |
|------|--------------|
| **DT** | DilutionTracker page in a new browser tab |
| **AE** | AskEdgar dilution view in a new browser tab |
| **RP** | This dashboard's Research tab for the same ticker, in a new browser tab |
| **RQ** | Switches the current tab to the Review Queue in ticker mode for that ticker |

Middle-click works on RQ as a regular `?tab=review&...` link if you'd rather open it in a new tab.

---

## URL state

The dashboard preserves enough state in the URL that a refresh or shared link reproduces the view:

| Parameter | Effect |
|-----------|--------|
| `tab=research` or `tab=review` | which top-level tab is open |
| `ticker=X` | autoload ticker on the Research tab |
| `rmode=date` or `rmode=ticker` | Review Queue mode |
| `rt=X` | Review Queue ticker (used with `rmode=ticker`) |

Example: `http://localhost:8765/?tab=review&rmode=ticker&rt=AEHL` opens the Review Queue with AEHL's full history.

---

## Architecture (one-liner)

Browser → Python proxy on `localhost:8765` → AskEdgar API + sec-api.io + AWS Postgres. The proxy holds the API keys and DB password (in `config.json`, gitignored). No frontend build step — single HTML file with vanilla JS and Chart.js from CDN.

For implementation details, conventions, and gotchas, see [CLAUDE.md](CLAUDE.md).
