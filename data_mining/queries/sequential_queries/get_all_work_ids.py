import csv
from connecting_postgresql_db import execute_pg_query

# -------------------------------------------
# Fetch all IDs from openalex.works and store them in a CSV file
# -------------------------------------------
def fetch_and_save_all_ids(file_path: str) -> None:
    """
    Query openalex.works for every 'id' (ordered) and write them to a CSV.

    Args:
        file_path (str): Destination path for the CSV containing the IDs
    """
    print(f"Fetching all 'id' values from openalex.works and saving to {file_path}...")
    query = """
        SELECT id
        FROM openalex.works
        ORDER BY id
        LIMIT;
    """
    result = execute_pg_query(query)  # Assumes you have a helper function

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id"])  # Header
        for row in result:
            writer.writerow([row[0]])  # Only one column (id)
    
    print(f"Finished saving all IDs to {file_path}.")
