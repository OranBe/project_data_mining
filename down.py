import csv
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from connecting_postgresql_db import execute_pg_query
from typing import List, Iterable, Any

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
INDEX_FILE_PATH      = "data/works_ids_index.csv"
SUBGROUP_RESULTS_DIR = "data/subgroup_results"
NUM_SUBGROUPS        = 10
MERGED_OUTPUT_FILE   = "data/merged_openalex_works_data.csv"  # Final merged file


# ------------------------------------------------------------------
# 1. Fetch all IDs from openalex.works and store them in a CSV file
# ------------------------------------------------------------------
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
        ORDER BY id;
    """
    result = execute_pg_query(query)      # ← Assumes you have a helper function

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["id"])           # Header
        for row in result:
            writer.writerow([row[0]])     # Only one column (id)
    print(f"Finished saving all IDs to {file_path}.")


# ------------------------------------------------------------------
# 2. Read IDs back from the CSV file
# ------------------------------------------------------------------
def read_ids_from_file(file_path: str) -> List[str]:
    """
    Load the list of IDs that were previously written to disk.

    Args:
        file_path (str): Path to the CSV containing the IDs

    Returns:
        List[str]: All IDs in the file
    """
    print(f"Reading IDs from {file_path}...")
    ids: List[str] = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)                      # Skip header
        for row in reader:
            if row:                       # Skip empty rows
                ids.append(row[0])

    print(f"Finished reading {len(ids)} IDs.")
    return ids


# ------------------------------------------------------------------
# 3. Utility: yield successive n-sized chunks from a list
# ------------------------------------------------------------------
def chunk_list(lst: List[Any], n: int) -> Iterable[List[Any]]:
    """
    Lazy generator that yields chunks of size n from lst.

    Args:
        lst (List[Any]): The list to chunk
        n (int): Desired chunk size
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


# ------------------------------------------------------------------
# 4. Query each subgroup and write results into separate CSV files
# ------------------------------------------------------------------
def process_subgroups_and_save_results(
    all_ids: List[str], num_subgroups: int, output_dir: str
) -> None:
    """
    Split IDs into roughly equal sub-ranges, query each range, and
    write the results (id, publication_year) to individual CSV files.

    Args:
        all_ids (List[str]): The complete list of IDs, sorted
        num_subgroups (int): Desired number of splits
        output_dir (str): Directory where subgroup CSVs will be saved
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")

    total_ids  = len(all_ids)
    chunk_size = (total_ids + num_subgroups - 1) // num_subgroups  # Ceiling division
    id_chunks  = list(chunk_list(all_ids, chunk_size))

    print(f"Starting to process {len(id_chunks)} subgroups.")

    for i, id_chunk in enumerate(id_chunks):
        if not id_chunk:          # Safety: skip empty chunks
            continue

        print(f"Processing subgroup {i + 1}/{len(id_chunks)} with {len(id_chunk)} IDs...")

        # Because IDs are sorted, we can query by an inclusive range (BETWEEN)
        min_id, max_id = id_chunk[0], id_chunk[-1]

        subgroup_query = f"""
            SELECT id, publication_year
            FROM openalex.works
            WHERE id BETWEEN '{min_id}' AND '{max_id}'
            ORDER BY id;
        """

        try:
            subgroup_result = execute_pg_query(subgroup_query)

            output_file = os.path.join(output_dir, f"subgroup_{i + 1:03d}.csv")
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["id", "publication_year"])  # Column headers
                for row in subgroup_result:
                    writer.writerow([row[0], row[1]])

            print(f"Saved results for subgroup {i + 1} to {output_file}.")

        except Exception as e:
            print(f"Error processing subgroup {i + 1}: {e}")


# ------------------------------------------------------------------
# 5. Merge all subgroup CSV files into a single CSV
# ------------------------------------------------------------------
def merge_csv_files(input_dir: str, output_file: str) -> None:
    """
    Merge every CSV stored in *input_dir* into a single output file.

    Args:
        input_dir (str): Directory containing the subgroup CSVs
        output_file (str): Destination path for the merged CSV
    """
    print(f"Merging CSV files from '{input_dir}' into '{output_file}'...")

    all_files = sorted(f for f in os.listdir(input_dir) if f.endswith(".csv"))
    if not all_files:
        print(f"No CSV files found in '{input_dir}'. Nothing to merge.")
        return

    header_written = False
    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)

        for filename in all_files:
            filepath = os.path.join(input_dir, filename)
            print(f"Reading {filepath}...")

            with open(filepath, "r", encoding="utf-8") as infile:
                reader = csv.reader(infile)
                header = next(reader)     # First row is the header

                # Write header once, then only write data rows
                if not header_written:
                    writer.writerow(header)
                    header_written = True

                for row in reader:
                    writer.writerow(row)

    print(f"Successfully merged all files into '{output_file}'.")


# --- הפעלת התהליך המלא ---
if __name__ == "__main__":
    # 1. שליפת כל ה-IDs ושמירתם לקובץ
    fetch_and_save_all_ids(INDEX_FILE_PATH)

    # 2. קריאת ה-IDs מהקובץ
    # all_works_ids = read_ids_from_file(INDEX_FILE_PATH)

    # 3. ביצוע שאילתות בלולאה ושמירת התוצאות
    # שימו לב: עם 200 מיליון רשומות, חלוקה ל-100 תתי-קבוצות אומרת כ-2 מיליון ID's בכל תת-קבוצה.
    # שליפה לפי BETWEEN עם טווחים גדולים עדיין עשויה לדרוש זמן.
    # אם הביצועים איטיים מדי, ייתכן שיהיה צורך לשקול חלוקה לתתי-קבוצות קטנות יותר
    # או אסטרטגיות מתקדמות יותר כמו COPY FROM STDIN עם קובץ זמני.
    # עם זאת, עבור Index Only Scan (כמו בשלב 1) ולאחר מכן Index Scan עם טווחים, זה אמור להיות יעיל.
    # process_subgroups_and_save_results(all_works_ids, NUM_SUBGROUPS, SUBGROUP_RESULTS_DIR)

    # merge_csv_files(SUBGROUP_RESULTS_DIR, MERGED_OUTPUT_FILE)
    # print("\nMerge process completed.")

    print("\nProcess completed.")