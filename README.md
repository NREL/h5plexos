# H5PLEXOS

HDF5 conversion scripts originally pulled out from https://github.nrel.gov/aces/plexos-coad

Note: This package is functional, but a work in progress. The querying API and structure of the HDF5 output file are stabilizing but still subject to change.

## Installation

### Peregrine Easy-Installer

Running the following on Peregine will create a Conda environment that you can use to run `h5plexos`:

```sh
curl https://github.nrel.gov/raw/PCM/h5plexos/master/install_h5plexos_peregrine.sh | sh
```

Once that's been done, load Peregrine's conda module and activate the newly-created environment before processing a PLEXOS solution as described below.

```sh
module load conda
source activate h5plexos
```

### Generic Installer

This repository's master branch zip archive is a functional pip installation package. Download the zipfile from GitHub ("Clone or download" -> "Download ZIP" on the repo homepage) and install the package with `pip`:

```sh
pip install my/path/to/download/master.zip
```

You'll need the `h5py` package installed as well.

## PLEXOS Zipfile Processing

With the package installed as detailed above, processing a solution file is as simple as:

```sh
h5plexos PLEXOS_Solution.zip # Saves out to PLEXOS_Solution.h5
```


The processor can also be called from a Python script:

```python
from h5plexos.process import process_solution
process_solution("PLEXOS_Solution.zip", "PLEXOS_Solution.h5") # Saves out to PLEXOS_Solution.h5
```

## Querying the HDF5 file

Once the solution has been processed, the resulting file can be queried by object type and property name, with optional filtering by object name and category:

```python
from h5plexos.query import PLEXOSSolution

with PLEXOSSolution("PLEXOS_Solution.h5") as db:

    lineflow = db.line("Flow") # Flow data for all lines
    regionload = db.region("Load", names=["CAISO", "ERCOT"]) # Load for CAISO and ERCOT regions
    vggeneration = db.generator("Generation", categories=["PV", "Wind"]) # Generation for units in the PV and Wind categories

```

Queries return values in a Pandas series with a `MultiIndex` describing object category, object name, property name, timestamp, and value band. The series can be easily [unstacked](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.unstack.html) to create a dataframe organized with columns of your choosing (e.g. object category + name).

Querying membership properties (e.g. generator-level reserve provisions) and filtering queries on other object relations (generators in a region, lines attached to a particular node, etc) is theoretically possible given the data stored in the HDF5 file, but not currently supported by the query API. Stay tuned!
