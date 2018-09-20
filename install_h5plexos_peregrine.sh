#! /bin/sh

# Automatically creates a conda environment on Peregrine for running h5plexos.
# This should probably work on any Linux system with Conda installed
# (although `module load conda` might throw an error).

CONDA_ENV_URL="https://github.nrel.gov/raw/PCM/h5plexos/master/h5plexos_env.yml"

# Download conda environment file
wget -O h5plexos_env.yml $CONDA_ENV_URL

# Create and configure conda environment
module load conda
conda env update -f h5plexos_env.yml

## From now on, to create plexos_solution.h5:
# source activate h5plexos
# h5plexos plexos_solution.zip
