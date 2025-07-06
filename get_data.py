from data_mining.connecting_postgresql_db import get_data_from_pg_db


def get_total_works_per_year():
    """
    This function retrieves the total works count per year.
    """
    query = """
            SELECT year, SUM (works_count) AS total_works_count_per_year
            FROM openalex.sources_counts_by_year
            WHERE year <= 2023
            GROUP BY year
            ORDER BY year; \
            """
    results = get_data_from_pg_db(query)
    return results





