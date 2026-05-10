"""One-shot backfill: run the warrants doublecheck analyzer over a single filed_at.

Skips rows whose owners_two contains 'NOT WARRANT OR CONVERTIBLE' and rows that
already have a doublecheck. Throttles to BG_DELAY_BETWEEN seconds between calls.

    py -3 backfill_warrants_doublecheck.py --filed-at 2026-05-08 --dry-run
    py -3 backfill_warrants_doublecheck.py --filed-at 2026-05-08 --yes
"""
import argparse
import sys
import time

from psycopg.rows import dict_row

import server


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    ap = argparse.ArgumentParser()
    ap.add_argument("--filed-at", required=True, help="YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true", help="count candidates only")
    ap.add_argument("--yes", action="store_true", help="skip confirmation")
    ap.add_argument("--limit", type=int, default=None, help="max rows to process")
    ap.add_argument("--force", action="store_true", help="re-analyze rows that already have doublecheck")
    args = ap.parse_args()

    # Ensure doublecheck column exists (idempotent).
    server._ensure_columns()

    sql = f"""
        SELECT {server.WARRANTS_REVIEW_COLUMNS}
        FROM warrants_convertibles4
        WHERE filed_at = %(filed_at)s
          AND details IS NOT NULL
          AND owners_two IS NOT NULL
          AND owners_two NOT LIKE %(skip)s
          {"" if args.force else "AND doublecheck IS NULL"}
        ORDER BY ticker ASC, id ASC
    """
    with server.get_conn() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"filed_at": args.filed_at, "skip": "%NOT WARRANT OR CONVERTIBLE%"})
        rows = cur.fetchall()

    if args.limit:
        rows = rows[: args.limit]

    print(f"Candidates for {args.filed_at}: {len(rows)} rows")
    for r in rows:
        print(f"  id={r['id']:>7}  {r['ticker']:<8}  owners_two={(r.get('owners_two') or '')[:60]!r}")

    if args.dry_run:
        return

    if not server.ANTHROPIC_KEY:
        sys.exit("anthropic_api_key missing in config.json")

    if not args.yes:
        ans = input(f"\nProceed with {len(rows)} Anthropic calls? [y/N] ").strip().lower()
        if ans != "y":
            print("Aborted.")
            return

    analyzed = 0
    errors = 0
    for i, row in enumerate(rows):
        row_id = row["id"]
        ticker = (row.get("ticker") or "").upper()
        if not ticker:
            print(f"[{i+1}/{len(rows)}] id={row_id} — skipped (no ticker)")
            continue
        print(f"[{i+1}/{len(rows)}] {ticker} id={row_id}")
        try:
            ctx = server.Handler._gather_warrants_ai_context(ticker, row)
            analysis = server.Handler._call_anthropic_warrants(row, ctx)
        except Exception as e:
            print(f"    FAILED: {e}")
            errors += 1
            continue

        try:
            with server.get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "UPDATE warrants_convertibles4 "
                    "SET doublecheck = %s, updated_at = NOW() "
                    "WHERE id = %s",
                    (analysis, row_id),
                )
                conn.commit()
            analyzed += 1
            print(f"    OK ({len(analysis)} chars)")
        except Exception as e:
            print(f"    DB save failed: {e}")
            errors += 1

        if i < len(rows) - 1:
            time.sleep(server.BG_DELAY_BETWEEN)

    print(f"\nDone: analyzed={analyzed} errors={errors} total={len(rows)}")


if __name__ == "__main__":
    main()
