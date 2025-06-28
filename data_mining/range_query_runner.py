#!/usr/bin/env python3
"""
query_runner.py

Run a parameter-range query against the `openalex.works` table and export the
results (id, publication_year) to a CSV file.

Usage
-----
$ python query_runner.py <min_id> <max_id> <output_csv>

Example (inside an SBATCH script)
---------------------------------
python query_runner.py "W1234567890" "W1234570000" "data/subgroup_001.csv"
"""

import argparse
import csv
import os
import sys
from datetime import datetime

from connecting_postgresql_db import execute_pg_query



def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Query a slice of openalex.works by ID range and save to CSV."
    )
    parser.add_argument("min_id", help="Lowest ID in the slice (inclusive)")
    parser.add_argument("max_id", help="Highest ID in the slice (inclusive)")
    parser.add_argument("output_csv", help="Path to the CSV file to create")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.output_csv), exist_ok=True)

    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
        f"Querying IDs BETWEEN '{args.min_id}' AND '{args.max_id}'..."
    )

    # Build and run the SQL
    sql = f"""
        SELECT id, publication_year
        FROM openalex.works
        WHERE id BETWEEN '{args.min_id}' AND '{args.max_id}'
        ORDER BY id;
    """
    try:
        rows = execute_pg_query(sql)
    except Exception as exc:
        print(f"Database query failed: {exc}", file=sys.stderr)
        sys.exit(1)

    # Write result set to CSV
    try:
        with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "publication_year"])  # header
            writer.writerows(rows)
    except Exception as exc:
        print(f"Failed to write CSV '{args.output_csv}': {exc}", file=sys.stderr)
        sys.exit(1)

    print(
        f"[{datetime.now():%Y-%m-%d %H:%M:%S}] "
        f"Wrote {len(rows)} rows to '{args.output_csv}'."
    )

if __name__ == "__main__":
    main()
