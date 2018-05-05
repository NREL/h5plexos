# H5PLEXOS

HDF5 conversion scripts originally pulled out from https://github.nrel.gov/aces/plexos-coad

Note: This package is functional, but a work in progress. The structure of the HDF5 output file will be changing in the near future.

## Peregrine Easy-Installer

Running the following on Peregine will create a Conda environment that you can use to run `h5plexos`:

```sh
curl https://github.nrel.gov/raw/PCM/h5plexos/master/install_h5plexos_peregrine.sh | sh
```

Once that's been done, processing a PLEXOS solution is as simple as:

```sh
module load conda
source activate h5plexos
h5plexos PLEXOS_Solution.zip
```

## Generic Installer

This repository's master branch zip archive is a functional pip installation package. Download the zipfile from GitHub ("Clone or download" -> "Download ZIP" on the repo homepage) and install the package with `pip`:

```sh
pip install my/path/to/download/master.zip
```

You'll need the `h5py` package installed as well.
