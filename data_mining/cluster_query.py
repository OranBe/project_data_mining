import csv
import os
import subprocess
import time
from typing import List, Iterable, Any

from down import read_ids_from_file

INDEX_FILE_PATH      = "data/works_ids_index.csv"
SUBGROUP_RESULTS_DIR = "data/subgroup_results"
NUM_SUBGROUPS        = 10
# ---------------------------------------------------------------
# Slurm / cluster-specific parameters (edit to match your cluster)
# ---------------------------------------------------------------
JOB_SCRIPT_DIR      = "slurm_query_jobs"          # Where SBATCH files will be written
JOB_OUTPUT_DIR      = "slurm_query_logs"          # Stdout / stderr directory
QUERY_MEM           = "8G"                        # Memory per job
QUERY_TIME          = "02:00:00"                  # Wall-time per job
CPUS_PER_TASK       = 2                           # vCPUs per job
MAX_JOBS_RUNNING    = 20                          # Concurrency limit

# ---------------------------------------------------------------
# Utility – split list into chunks
# ---------------------------------------------------------------
def chunk_list(lst: List[Any], n: int) -> Iterable[List[Any]]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

# ---------------------------------------------------------------
# Utility – throttle concurrent jobs
# ---------------------------------------------------------------
def limit_jobs(job_queue: List[str], max_jobs_running: int) -> None:
    """
    Polls the Slurm queue and waits until the number of *running or pending*
    jobs in job_queue is < max_jobs_running.
    """
    while len(job_queue) >= max_jobs_running:
        # Remove finished jobs from the local queue
        job_queue[:] = [
            job
            for job in job_queue
            if subprocess.run(["squeue", "-h", "-j", str(job)]).returncode == 0
        ]

        if len(job_queue) < max_jobs_running:
            return

        time.sleep(10)  # Wait 10 s before checking again

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

# Activate environment if needed, e.g.:
# module load anaconda && source activate myenv

python range_query_runner.py "{id_range[0]}" "{id_range[1]}" "{output_csv}"
""")

    return script_path

# ---------------------------------------------------------------
# Wrapper – generate SBATCH jobs for all subgroups and submit them
# ---------------------------------------------------------------
def submit_subgroup_jobs(
    all_ids: List[str],
    num_subgroups: int,
    output_dir: str,
) -> List[str]:
    """
    Split *all_ids* into *num_subgroups* contiguous ranges and submit one Slurm
    job per range that writes results into *output_dir*.

    Returns:
        job_queue (List[str]): List of submitted job IDs.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    total_ids  = len(all_ids)
    chunk_size = (total_ids + num_subgroups - 1) // num_subgroups
    id_chunks  = list(chunk_list(all_ids, chunk_size))

    job_queue: List[str] = []

    for idx, chunk in enumerate(id_chunks):
        if not chunk:
            continue

        min_id, max_id = chunk[0], chunk[-1]
        csv_path       = os.path.join(output_dir, f"subgroup_{idx+1:03d}.csv")

        # Create SBATCH script for this subgroup
        script_path = create_query_job(
            subgroup_idx = idx + 1,
            id_range     = (min_id, max_id),
            output_csv   = csv_path,
        )

        # Throttle submission to avoid flooding the scheduler
        limit_jobs(job_queue, MAX_JOBS_RUNNING)

        # Submit the job
        submit = subprocess.run(["sbatch", script_path], stdout=subprocess.PIPE)
        job_id = submit.stdout.decode().strip().split()[-1]
        job_queue.append(job_id)

        print(f"Submitted subgroup {idx+1}/{len(id_chunks)} (IDs {min_id}–{max_id}) – Job ID {job_id}")

    return job_queue

# ---------------------------------------------------------------
# Example usage:
ids = read_ids_from_file(INDEX_FILE_PATH)
job_ids = submit_subgroup_jobs(ids, NUM_SUBGROUPS, SUBGROUP_RESULTS_DIR)
print(f"Submitted {len(job_ids)} jobs: {job_ids}")
# ---------------------------------------------------------------
