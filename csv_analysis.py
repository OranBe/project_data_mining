import pandas as pd

def count_records_in_csv(csv_path):
    """
    Counts the number of records (rows) in a CSV file excluding the header.

    Args:
    csv_path (str): Path to the CSV file.

    Returns:
    int: Total number of records in the CSV file.
    """
    with open(csv_path, 'r', encoding='utf-8') as f:
        total = sum(1 for _ in f) - 1  # Subtract 1 for the header row

    return total

def read_first_n_rows(csv_path, nrows=20):
    """
    Reads the first 'n' rows of a CSV file without loading the entire file into memory.

    Args:
    csv_path (str): Path to the CSV file.
    nrows (int): Number of rows to read from the CSV file. Default is 20.

    Returns:
    pd.DataFrame: DataFrame containing the first 'n' rows of the CSV file.
    """
    df = pd.read_csv(
        csv_path,
        nrows=nrows,       # Reads only the first 'n' rows
        low_memory=True    # Splits columns by data types to save memory
    )
    return df