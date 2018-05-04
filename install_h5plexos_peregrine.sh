#! /bin/sh

# Automatically creates a conda environment on Peregrine for running h5plexos.
# This should probably work on any Linux system with Conda installed
# (although `module load conda` might throw an error).

REPO_ARCHIVE_URL="https://github.nrel.gov/PCM/h5plexos/archive/master.zip"
CONDA_ENV_URL="https://github.nrel.gov/raw/PCM/h5plexos/master/h5plexos_env.yml"

# Create and populate installation folder
mkdir h5plexos_installer
wget -O h5plexos_installer/h5plexos.zip $REPO_ARCHIVE_URL
wget -O h5plexos_installer/h5plexos_env.yml $CONDA_ENV_URL

# Create and configure conda environment
module load conda
conda env update -f h5plexos_installer/h5plexos_env.yml
source activate h5plexos
pip install h5plexos_installer/h5plexos.zip

# Clean up installation files
rm -r h5plexos_installer

## From now on, to create plexos_solution.h5:
# source activate h5plexos
# h5plexos plexos_solution.zip
