import h5py
import numpy as np
import pandas as pd

# We can probably do better
def issequence(x):
    return hasattr(x, '__iter__')

class PLEXOSSolution:

    def __init__(self, h5filepath):
        self.h5file = h5py.File(h5filepath, "r")

        self.objects = {}
        for name, dset in self.h5file["/metadata/objects"].items():
            idx = pd.MultiIndex.from_tuples(
                [(d[1].decode("UTF8"), d[0].decode("UTF8")) for d in dset],
                names = ["category", "name"])
            self.objects[name] = pd.Series(range(len(idx)), index=idx).sort_index()

        self.timestamps = {}
        for name, dset in self.h5file["/metadata/times"].items():
            self.timestamps[name] = pd.DatetimeIndex([d.decode("UTF8") for d in dset])


    def close(self):
        self.h5file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getattr__(self, object_class):

        def _partial_query_objects(*args, **kwargs):
            return self.query_objects(object_class, *args, **kwargs)

        return _partial_query_objects

    # Query methods

    def query_objects(self, object_class, prop,
                     names=slice(None), categories=slice(None),
                     timescale="interval", timespan=slice(None),
                     phase="ST"):

        #TODO: Time slicing still not supported
        timespan = slice(None)

        obj_lookup = self.objects[object_class].loc[(categories, names),]
        data_path = "/data/" + "/".join([phase, timescale, object_class, prop])

        dset = self.h5file[data_path]
        n_bands = dset.shape[2]
        data = dset[np.sort(obj_lookup.values), timespan, :]

        # Multiindex on category, name, property, time, band
        # TODO: Something cleaner than this!
        idx = pd.MultiIndex.from_product(
            [[x for x in obj_lookup.index], # List object categories and names
             [prop], # Report property (in preperation for multi-property queries)
             self.timestamps[timescale], # List all timestamps (but eventually not)
             range(1, n_bands+1)] # List all bands
        )
        idx = pd.MultiIndex.from_tuples(
            [(c, n, p, t, b) for ((c, n), p, t, b) in idx],
            names=["category", "name", "property", "timestamp", "band"])

        return pd.Series(data=data.reshape(-1), index=idx)
