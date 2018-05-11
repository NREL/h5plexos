import h5py
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
            self.objects[name] = pd.Series(range(len(idx)), index=idx)

        self.timestamps = {}
        for name, dset in self.h5file["/metadata/times"].items():
            self.timestamps[name] = pd.DatetimeIndex([d.decode("UTF8") for d in dset])


    def close(self):
        self.h5file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


    # Query methods

    def query_objects(self, object_class, prop,
                     names=None, categories=None,
                     bands=None,
                     timescale="period_0", timespan=None,
                     phase="phase_4"):

        if not names:
            names = slice(None)

        if not categories:
            category = slice(None)

        obj_lookup = self.objects[object_class].loc[(category, names),]

        # Doesn't support bands or time slicing (yet)
        data_path = ("/data/" + object_class + "/" + prop + "/"
                     + timescale + "/" + phase)
        data = self.h5file[data_path][obj_lookup.values, :]

        return pd.DataFrame(data=data.T,
                            index=self.timestamps[timescale],
                            columns=obj_lookup.index)
