# H5PLEXOS

HDF5 conversion scripts originally pulled out from https://github.nrel.gov/aces/plexos-coad

Note: This package is functional, but a work in progress. The querying API and
structure of the HDF5 output file are stabilizing but still subject to change.

## Installation

### Pre-installed Eagle environment

If you're working on Eagle, NREL's HPC cluster, the quickest way to get started
is to load the Eagle environment module:

```sh
module use /home/gstephen/apps/modules
module load h5plexos
```

This will automatically activate a Conda environment that provides the h5plexos
Python package and command line tool (see below for usage).

### Conda Environment File

If you're using Conda for environment management but aren't on Eagle, you can
use the `h5plexos_env.yml` file in this repository to create an h5plexos
environment.
Assuming you've saved `h5plexos_env.yml` to the current directory:

```sh
conda env update -f h5plexos_env.yml
```

### Generic Installer

A more general option is to manually install the package in your own Python
environment, Conda-managed or otherwise.
This repository's master branch zip archive is a functional pip installation
package, you can install it directly from GitHub via `pip`:

```sh
pip install https://github.com/NREL/h5plexos/archive/master.zip
```

You'll need the `pandas` and `h5py` packages installed as well.

## PLEXOS Zipfile Processing

With the package installed in the current environment as detailed above,
processing a solution file is as simple as:

```sh
h5plexos PLEXOS_Solution.zip # Saves out to PLEXOS_Solution.h5
```

The processor can also be called from a Python script:

```python
from h5plexos.process import process_solution
process_solution("PLEXOS_Solution.zip", "PLEXOS_Solution.h5") # Saves out to PLEXOS_Solution.h5
```

## Querying the HDF5 file

Once the solution has been processed, the resulting file can be queried by
object type and property name, with optional filtering by object name and
category:

```python
from h5plexos.query import PLEXOSSolution

with PLEXOSSolution("PLEXOS_Solution.h5") as db:

    # Flow data for all lines
    lineflow = db.line("Flow")

    # Load for CAISO and ERCOT regions
    regionload = db.region("Load", names=["CAISO", "ERCOT"])

    # Generation for units in the PV and Wind categories
    vggeneration = db.generator("Generation", categories=["PV", "Wind"])

```

Queries return values in a Pandas series with a `MultiIndex` describing object
category, object name, property name, timestamp, and value band.
The standard Pandas tools can then be used for
[aggregation](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.groupby.html),
[unstacking](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.unstack.html), etc.

Relationship properties can be queried in the same way, filtering on either
the parent or child objects:

```python
from h5plexos.query import PLEXOSSolution

with PLEXOSSolution("PLEXOS_Solution.h5") as db:

    # Natural gas offtakes for all (relevant) generators
    ng_offtakes = db.generator_fuels("Offtake", children=["Natural Gas"])

    # Coal offtakes for specific generators
    coal_offtakes = db.generator_fuels("Offtake", parents=["Generator_7", "CoalPlant123"], children=["Coal"])

    # Provisions of any reserve from any generator
    all_gen_provisions = db.reserve_generators("Provision")

    # Reserve provisions from a particular set of generators
    gen_provisions = db.reserve_generators("Provision", children=["Generator_1", "Generator_5"])

```

Object lists are available in the `db.objects` dictionary, while
relations (direct memberships as well as some extra convenience relations)
between objects are stored in the `db.relations` dictionary. These data can be
combined together with standard Pandas
[join](https://pandas.pydata.org/pandas-docs/stable/merging.html#database-style-dataframe-joining-merging)
functionalities to filter datasets (i.e. by determining the object names to query)
based on particular criteria (generators in a region, lines attached to a
particular node, etc).

```python
from h5plexos.query import PLEXOSSolution

with PLEXOSSolution("PLEXOS_Solution.h5") as db:

    # List of all generators
    db.objects["generators"]

    # Generators by region
    db.relations["region_generators"]

```
