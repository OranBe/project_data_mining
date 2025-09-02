#!/bin/bash
#SBATCH --job-name=main_query
#SBATCH --output=/sise/robertmo-group/Oran/data_mining/project_data/data/q2/parallel_query_out.txt
#SBATCH --error=/sise/robertmo-group/Oran/data_mining/project_data/data/q2/parallel_query_err.txt
#SBATCH --time=6-23:59:00                 # Time limit
#SBATCH --mem=20G                        # Memory requested (20GB)
#SBATCH --cpus-per-task=5                # Number of CPUs per task
#SBATCH --mail-user=oran@b.ac.il # User's email for sending job status notifications
#SBATCH --mail-type=BEGIN,END,FAIL        # Conditions for sending the email

# Load Anaconda module
module load anaconda

# Import Conda configuration and activate the environment
source /storage/modules/packages/anaconda/etc/profile.d/conda.sh
conda activate /home/oranbe/.conda/envs/data_mining_project

# Debugging: Check Python path
echo "Python path:"
which python
echo "Python version:"
python --version

# Change directory to the project directory
cd /sise/robertmo-group/Oran/data_mining/project_data/

# Run the experiment script
python -u /sise/robertmo-group/Oran/data_mining/project_data/data_mining/cluster_parallel_query.py
