# H5PLEXOS

This package provides a Python interface for reading HDF5 files with H5PLEXOS
v0.5 and v0.6 formatting. To create v0.5 files, use a version of this package
in the [0.5 series](https://github.com/NREL/h5plexos/releases). To create v0.6
files, use H5PLEXOS.jl.

Note: This package is functional, but a work in progress. The querying API and
HDF5 file structure are stabilizing but still subject to change.

## Installation

A zip archive of this repository is a functional pip installation
package. For example, you can install the latest development version directly
from GitHub via `pip`:

```sh
pip install https://github.com/NREL/h5plexos/archive/master.zip
```
You can also install a specific release by downloading the corresponding archive
from the [releases page](https://github.com/NREL/h5plexos/releases).

You'll need the `pandas` and `h5py` packages installed as well.

## Querying an H5PLEXOS HDF5 file

Once a PLEXOS solution has been processed, the resulting HDF5 file can be
queried by object type and property name, with optional filtering by object
name and category:

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

In newer h5plexos files (file format v0.6.2 or above), there will also be a
`db.blocks` dictionary of mappings from chronological time intervals to
non-chronological time blocks. These mappings can be used to relate LT, PASA,
and MT solution block results to periods in time.

```python
from h5plexos.query import PLEXOSSolution

with PLEXOSSolution("PLEXOS_Solution.h5") as db:

    # LT solution interval->block mapping
    db.blocks["LT"]

```
