import h5py
import numpy as np
import pandas as pd
import re
import time

# We can probably do better
def issequence(x):
    return hasattr(x, '__iter__')

version_rgx = re.compile("^v(\d+)\.(\d+)\.(\d+)$")

class PLEXOSSolution:

    def __init__(self, h5filepath):
        self.h5file = h5py.File(h5filepath, "r")
        
        self.versionstring = self.h5file.attrs.get("h5plexos")
    
        if self.versionstring:
            self.versionstring = self.versionstring.decode("UTF8")
            v = version_rgx.match(self.versionstring)
            v = v.group(1,2,3)
        else: 
            v = ('0','5','0')
        
        
        if (('0','6','0') <= v and v < ('0','7','0')):
            print("Querying H5PLEXOS " + self.versionstring + " file")
        else:
            print("Querying H5PLEXOS v0.5.0 file")
        
        self.version = v
        
        
        self.objects = {}
        for name, dset in self.h5file["/metadata/objects"].items():
            idx = pd.MultiIndex.from_tuples(
                [(d[1].decode("UTF8"), d[0].decode("UTF8")) for d in dset],
                names = ["category", "name"])
            self.objects[name] = pd.Series(range(len(idx)), index=idx).sort_index()
            
        
        self.relations = {}
        for name, dset in self.h5file["/metadata/relations"].items():
            idx = pd.MultiIndex.from_tuples(
                [(d[0].decode("UTF8"), d[1].decode("UTF8")) for d in dset],
                names = ["parent", "child"])
            self.relations[name] = pd.Series(range(len(idx)), index=idx)
        
        
        if (('0','6','0') <= self.version and self.version < ('0','7','0')):
            self.timestamps = {}
            for name, dset in self.h5file["/metadata/times"].items():
                self.timestamps[name] = pd.to_datetime(dset[:],
                                                       format="%Y-%m-%dT%H:%M:%S")
        else:
            self.timestamps = {}
            for name, dset in self.h5file["/metadata/times"].items():
                self.timestamps[name] = pd.to_datetime([d.decode("UTF8") for d in dset],
                                                    format="%Y-%m-%dT%H:%M:%S")

    def close(self):
        self.h5file.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __getattr__(self, collection):

        # TODO: Somethting smarter to determine whether querying object or relation properties
        if "_" in collection:
            f = self.query_relation_property
        else:
            f = self.query_object_property

        def _partial_query(*args, **kwargs):
            return f(collection, *args, **kwargs)

        return _partial_query

    # Query methods

    def query_object_property(
            self, object_class, prop,
            names=slice(None), categories=slice(None),
            timescale="interval", timespan=slice(None), phase="ST"):
        
        if (('0','6','0') <= self.version and self.version < ('0','7','0')):
            object_class += "s"
            obj_lookup = self.objects[object_class].loc[(categories, names),].sort_values()
            data_path = "/data/" + "/".join([phase, timescale, object_class, prop])
    
            dset = self.h5file[data_path]
            n_bands = dset.shape[2]
            n_periods = dset.shape[1]
            period_offset = dset.attrs["period_offset"]
            data = dset[obj_lookup.values, :, :]
    
            timestamps = self.timestamps[timescale][period_offset:(period_offset+n_periods)]
    
            # Multiindex on category, name, property, time, band
            idx = pd.MultiIndex.from_product(
                [[x for x in obj_lookup.index], # List object categories and names
                 [prop], # Report property (in preperation for multi-property queries)
                 timestamps, # List all timestamps in data range
                 range(1, n_bands+1)] # List all bands
            )
        else:
            timespan = slice(None)
            obj_lookup = self.objects[object_class].loc[(categories, names),].sort_values()
            data_path = "/data/" + "/".join([phase, timescale, object_class, prop])
    
            dset = self.h5file[data_path]
            n_bands = dset.shape[2]
            data = dset[obj_lookup.values, timespan, :]

    
            # Multiindex on category, name, property, time, band
            idx = pd.MultiIndex.from_product(
                [[x for x in obj_lookup.index], # List object categories and names
                 [prop], # Report property (in preperation for multi-property queries)
                 self.timestamps[timescale],
                 range(1, n_bands+1)] # List all bands
            )
            
        idx = pd.MultiIndex.from_tuples(
            [(c, n, p, t, b) for ((c, n), p, t, b) in idx],
            names=["category", "name", "property", "timestamp", "band"])

        return pd.Series(data=data.reshape(-1), index=idx).dropna().sort_index()

    def query_relation_property(
            self, relation, prop,
            parents=slice(None), children=slice(None),
            timescale="interval", timespan=slice(None), phase="ST"):
           
        
        if (('0','6','0') <= self.version and self.version < ('0','7','0')):        
            relation_lookup = self.relations[relation].loc[(parents, children),].sort_values()
            data_path = "/data/" + "/".join([phase, timescale, relation, prop])
            dset = self.h5file[data_path]
            n_bands = dset.shape[2]
            n_periods = dset.shape[3]
            period_offset = dset.attrs["period_offset"]
            data = dset[relation_lookup.values, :, :]
    
            timestamps = self.timestamps[timescale][period_offset:(period_offset+n_periods)]
    
            # Multiindex on parent, child, property, time, band
            idx = pd.MultiIndex.from_product(
                [[x for x in relation_lookup.index], # List object categories and names
                 [prop], # Report property (in preperation for multi-property queries)
                 timestamps, # List all timestamps (but eventually not)
                 range(1, n_bands+1)] # List all bands
            )
        else:
            timespan = slice(None)
            relation_lookup = self.relations[relation].loc[(parents, children),].sort_values()
            data_path = "/data/" + "/".join([phase, timescale, relation, prop])
            dset = self.h5file[data_path]
            n_bands = dset.shape[2]
            data = dset[relation_lookup.values, timespan, :]
    
            # Multiindex on parent, child, property, time, band
            idx = pd.MultiIndex.from_product(
                [[x for x in relation_lookup.index], # List object categories and names
                 [prop], # Report property (in preperation for multi-property queries)
                 self.timestamps[timescale], # List all timestamps (but eventually not)
                 range(1, n_bands+1)] # List all bands
            )
            
        idx = pd.MultiIndex.from_tuples(
            [(c, n, p, t, b) for ((c, n), p, t, b) in idx],
            names=["parent", "child", "property", "timestamp", "band"])

        return pd.Series(data=data.reshape(-1), index=idx).dropna().sort_index()
