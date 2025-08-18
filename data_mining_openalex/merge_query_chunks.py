import csv
import os
from itertools import chain
from tqdm import tqdm

# ------------------------------------------------------------------
# Merge all subgroup CSV files into a single CSV using itertools.chain
# ------------------------------------------------------------------
def merge_csv_files(input_dir: str, output_file: str) -> None:
    """
    Merge every CSV stored in *input_dir* into a single output file using itertools.chain
    for more efficient memory usage.
    
    Args:
        input_dir (str): Directory containing the subgroup CSVs
        output_file (str): Destination path for the merged CSV
    """
    print(f"Merging CSV files from '{input_dir}' into '{output_file}'...")

    # Get all CSV files in the directory
    all_files = sorted(f for f in os.listdir(input_dir) if f.endswith(".csv"))
    if not all_files:
        print(f"No CSV files found in '{input_dir}'. Nothing to merge.")
        return

    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        header_written = False

        # Add progress bar to iterate over the files
        for filename in tqdm(all_files, desc="Merging files", unit="file"):
            filepath = os.path.join(input_dir, filename)

            with open(filepath, "r", encoding="utf-8") as infile:
                reader = csv.reader(infile)
                header = next(reader)  # Read the header

                # Write the header once
                if not header_written:
                    writer.writerow(header)
                    header_written = True

                # Use itertools.chain to merge the rows
                for row in chain(reader):  # This will flatten the rows and write them directly
                    writer.writerow(row)

    print(f"Successfully merged all files into '{output_file}'.")
