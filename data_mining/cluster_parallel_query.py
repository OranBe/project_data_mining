import csv
import os
import subprocess
import time
from typing import List

from connecting_postgresql_db import execute_pg_query

INDEX_FILE_PATH      = "data/works_ids_index_all.csv"
SUBGROUP_RESULTS_DIR = "data/subgroup_results"
NUM_SUBGROUPS        = 10000
# ---------------------------------------------------------------
# Slurm / cluster-specific parameters (edit to match your cluster)
# ---------------------------------------------------------------
JOB_SCRIPT_DIR      = "data_mining/queries/parallel_queries/slurm_query_jobs"          # Where SBATCH files will be written
JOB_OUTPUT_DIR      = "data_mining/queries/parallel_queries/slurm_query_logs"          # Stdout / stderr directory
QUERY_MEM           = "2G"                        # Memory per job
QUERY_TIME          = "72:00:00"                  # Wall-time per job
CPUS_PER_TASK       = 2                            # vCPUs per job
MAX_JOBS_RUNNING    = 180                          # Concurrency limit


def free_connections_exceed(threshold: int = 10) -> bool:
    """
    Check whether the number of available (non-superuser) connection slots
    in the PostgreSQL server exceeds the given threshold.

    Args:
        threshold (int): The minimum number of free connections to compare against.

    Returns:
        bool: True if free connections > threshold, False otherwise.
    """
    query = f"""
    WITH
        cfg AS (
            SELECT
                current_setting('max_connections')::int                AS max_conn,
                current_setting('superuser_reserved_connections')::int AS super_res
        ),
        used AS (
            SELECT count(*) AS active_cnt
            FROM   pg_stat_activity
        )
    SELECT
        (cfg.max_conn - cfg.super_res - used.active_cnt) > {threshold}  AS free_more_than_threshold
    FROM cfg, used;
    """

    result = execute_pg_query(query)
    row = result.fetchone()     # fetch the single-row result
    return bool(row[0])

# ------------------------------------------------------------------
# NEW 1 – count_lines(): מספר השורות (ל-wc -l אין cost בזיכרון)
# ------------------------------------------------------------------
def count_lines(file_path: str) -> int:
    """Return number of data lines (excluding header) using `wc -l`."""
    out = subprocess.check_output(["wc", "-l", file_path], text=True)
    total = int(out.strip().split()[0]) - 1      # minus header
    return total

# ------------------------------------------------------------------
# NEW 2 – iter_id_ranges(): מייצר טווחי (min_id, max_id) ב-Streaming
#       אין החזקת רשימה שלמה; קורא את הקובץ פעם שנייה בלבד
# ------------------------------------------------------------------
def iter_id_ranges(file_path: str, num_subgroups: int):
    """
    Yield contiguous (min_id, max_id) tuples, assuming IDs are pre-sorted.

    • Uses O(1) memory.
    • Reads the file sequentially; chunk size = ceil(total_lines / num_subgroups).
    """
    total_lines = count_lines(file_path)
    chunk_size  = (total_lines + num_subgroups - 1) // num_subgroups

    with open(file_path, newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)                             # skip header

        first_id, count = None, 0
        for row in reader:
            if not first_id:
                first_id = row[0]
            count += 1

            if count == chunk_size:
                yield first_id, row[0]           # close chunk
                first_id, count = None, 0

        # last partial chunk
        if first_id:
            yield first_id, row[0]
# ---------------------------------------------------------------
# Utility – throttle concurrent jobs
# ---------------------------------------------------------------
def limit_jobs(job_queue: List[str], max_jobs_running: int) -> List[str]:
    """
    Wait until strictly fewer than *max_jobs_running* Slurm jobs from job_queue
    are still present in the queue (PENDING or RUNNING), then return the
    updated list of still-running job IDs.
    """
    while True:
        still_running: List[str] = []
        for job in job_queue:
            proc = subprocess.run(
                ["squeue", "-h", "-j", str(job)],  # -h → no header
                stdout=subprocess.PIPE,
                text=True
            )
            # If stdout not empty ⇒ job still exists
            if proc.stdout.strip():
                still_running.append(job)

        job_queue[:] = still_running  # update in place

        n_running = len(job_queue)
        # condition 1: much below limit
        if n_running < max_jobs_running - 150: #40
            print(f"Jobs finished, {n_running} running jobs remaining.")
            time.sleep(5)  # give scheduler time to update
            return job_queue
        # condition 2: moderately below limit and enough free connections
        if n_running < max_jobs_running - 20 and free_connections_exceed(30):
            print(f"Jobs finished, {n_running} running jobs remaining.")
            time.sleep(5)
            return job_queue
        # condition 3: just below limit and enough free connections
        if n_running < max_jobs_running and free_connections_exceed(15):
            print(f"Jobs finished, {n_running} running jobs remaining.")
            time.sleep(15)
            return job_queue

        # otherwise wait and retry
        # print(f"Waiting for jobs to finish... {n_running} running jobs.")
        time.sleep(30)

# ---------------------------------------------------------------
# Core – create one SBATCH file per subgroup
# ---------------------------------------------------------------
def create_query_job(
    subgroup_idx: int,
    id_range: tuple[str, str],
    output_csv: str,
) -> str:
    """
    Build a SBATCH script that queries a specific ID range and writes results
    to *output_csv*.  Returns the path to the written script.
    """
    if not os.path.exists(JOB_SCRIPT_DIR):
        os.makedirs(JOB_SCRIPT_DIR)

    if not os.path.exists(JOB_OUTPUT_DIR):
        os.makedirs(JOB_OUTPUT_DIR)

    job_name   = f"works_q_{subgroup_idx:03d}"
    script_path = os.path.join(JOB_SCRIPT_DIR, f"{job_name}.sbatch")

    stdout_file = os.path.join(JOB_OUTPUT_DIR, f"{job_name}.out")
    stderr_file = os.path.join(JOB_OUTPUT_DIR, f"{job_name}.err")

    # NOTE: Adapt the Python call below to point at your actual query runner
    with open(script_path, "w") as sb:
        sb.write(f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --output={stdout_file}
#SBATCH --error={stderr_file}
#SBATCH --time={QUERY_TIME}
#SBATCH --mem={QUERY_MEM}
#SBATCH --cpus-per-task={CPUS_PER_TASK}
#SBATCH --mail-user=oranbe@post.bgu.ac.il # users email for sending job status notifications
#SBATCH --mail-type=FAIL        # conditions for sending the email

python data_mining/queries/parallel_queries/get_work_year_institution_country.py "{id_range[0]}" "{id_range[1]}" "{output_csv}"
""")

    return script_path

# ---------------------------------------------------------------
# Wrapper – generate SBATCH jobs for all subgroups and submit them
# ---------------------------------------------------------------
def submit_subgroup_jobs(index_csv: str, num_subgroups: int, output_dir: str) -> List[str]:
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    job_queue: List[str] = []

    for idx, (min_id, max_id) in enumerate(iter_id_ranges(index_csv, num_subgroups), start=1):
        # if idx not in [3985,3967]:
        if idx < 5556:
            continue
        csv_path   = os.path.join(output_dir, f"subgroup_{idx:03d}.csv")
        script_path = create_query_job(
            subgroup_idx = idx,
            id_range     = (min_id, max_id),
            output_csv   = csv_path,
        )

        job_queue = limit_jobs(job_queue, MAX_JOBS_RUNNING)
        submit = subprocess.run(["sbatch", script_path], stdout=subprocess.PIPE)
        job_id = submit.stdout.decode().strip().split()[-1]
        job_queue.append(job_id)

        print(f"Submitted subgroup {idx}/{num_subgroups} (IDs {min_id}–{max_id}) – Job ID {job_id}")

    return job_queue

# ---------------------------------------------------------------
job_ids = submit_subgroup_jobs(INDEX_FILE_PATH, NUM_SUBGROUPS, SUBGROUP_RESULTS_DIR)
print(f"Submitted {len(job_ids)} jobs.")
# ---------------------------------------------------------------
