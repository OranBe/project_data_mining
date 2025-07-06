#!/usr/bin/env python3
"""
query_runner.py

Query a range of IDs in the OpenAlex tables (works, works_authorships,
institutions_geo) and export the results to CSV.

Fields exported:
    work_id, publication_year, institution_id, country, author_position
"""

import argparse
import csv
import os
import sys
from datetime import datetime

# from connecting_postgresql_db import execute_pg_query
import sys, os

base_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
)
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

# עכשיו אפשר לייבא:
from connecting_postgresql_db import execute_pg_query


# --------------------------------------------------------------------
# CLI parsing
# --------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query OpenAlex (joined tables) ID range and save to CSV"
    )
    parser.add_argument("min_id", help="Lowest work ID (inclusive)")
    parser.add_argument("max_id", help="Highest work ID (inclusive)")
    parser.add_argument("output_csv", help="Destination CSV path")
    return parser.parse_args()


# --------------------------------------------------------------------
# Main routine
# --------------------------------------------------------------------
def main() -> None:
    args = parse_args()
    os.makedirs(os.path.dirname(args.output_csv), exist_ok=True)

    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
        f"Querying works.id BETWEEN '{args.min_id}' AND '{args.max_id}'"
    )

    # ----------------------------------------------------------------
    # SQL query (no LIMIT clause)
    # ----------------------------------------------------------------
    sql = f"""
        SELECT
            wa.work_id,
            w.publication_year,
            wa.institution_id,
            ig.country,
            wa.author_position
        FROM   openalex.works             AS w
        JOIN   openalex.works_authorships AS wa  ON w.id              = wa.work_id
        JOIN   openalex.institutions_geo  AS ig  ON wa.institution_id = ig.institution_id
        WHERE  w.id BETWEEN '{args.min_id}' AND '{args.max_id}'
          AND  w.publication_year IS NOT NULL
          AND  wa.author_position  IS NOT NULL
          AND  ig.country          IS NOT NULL;
    """

    # ----------------------------------------------------------------
    # Execute query
    # ----------------------------------------------------------------
    try:
        # result = execute_pg_query(sql)   # iterable or CursorResult
        result = execute_pg_query("SET max_parallel_workers_per_gather = 0; " + sql)

    except Exception as exc:
        print(f"DB query failed: {exc}", file=sys.stderr)
        sys.exit(1)

    # ----------------------------------------------------------------
    # Stream rows to CSV
    # ----------------------------------------------------------------
    row_count = 0
    try:
        with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["work_id", "publication_year", "institution_id",
                 "country", "author_position"]
            )
            for row in result:
                writer.writerow([row[0], row[1], row[2], row[3], row[4]])
                row_count += 1
    except Exception as exc:
        print(f"Failed to write CSV '{args.output_csv}': {exc}", file=sys.stderr)
        sys.exit(1)

    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
        f"Wrote {row_count} rows to '{args.output_csv}'."
    )


if __name__ == "__main__":
    main()
