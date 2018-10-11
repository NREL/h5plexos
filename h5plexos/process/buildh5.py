'''Tools to parse Plexos solution zipfiles'''

import argparse
import logging
import os
import re
import struct
from zipfile import ZipFile

import h5py
import numpy as np

from . import tempdb
from .metadata import (create_time_dset,
                       create_object_dset, object_dset_name,
                       create_relation_dset, relation_dset_name)

def period_name_to_num(period_name):
    return int(period_name.replace("period_", ""))

timescales = {
    0: "interval",
    1: "day",
    2: "week",
    3: "month",
    4: "year"
}

phases = {
    1: "LT",
    2: "PASA",
    3: "MT",
    4: "ST"
}

def process_solution(zipfilename, h5filename=None, verbose=False):
    """Read in the plexos solution zipfile and save data to an hdf5 file

    Args:
        zipfilename - string name for zipfile
        h5filename - string name for h5file to write to, will be overwritten if
            exists.  If None, use the base zipfilename with h5 suffix
        verbose - boolean specifying whether or not to show information on
            processed file. Defaults to False.

    Returns:
        h5py.File
    """

    # Determine output filename and remove if it already exists
    if h5filename is None:
        h5filename = zipfilename[:-4]+'.h5'
    try:
        os.remove(h5filename)
    except OSError:
        pass

    sol_zip = ZipFile(zipfilename)
    sol_filelist = sol_zip.namelist()
    logging.info("Zip contains: %s", sol_filelist)

    # Needs ^Model.*xml$, ^t_data_[0-4].BIN$, and ^Model.*Log.*.txt$
    model_xml = None
    data_files = []
    model_log = None
    for sol_f in sol_filelist:
        if re.match("^Model.*xml$", sol_f):
            model_xml = sol_f
        elif re.match("^t_data_[0-4].BIN$", sol_f):
            data_files.append(sol_f)
        elif re.match("^Model.*Log.*.txt$", sol_f):
            model_log = sol_f
    logging.info("Zipfile contains files:")
    logging.info("    Model xml: %s", model_xml)
    logging.info("    Data files: %s", data_files)
    logging.info("    Model log: %s", model_log)
    if model_xml is None or len(data_files) == 0 or model_log is None:
        logging.error("Missing required files from zipfile.  Found: %s", sol_filelist)
        raise Exception("Invalid zipfile %s"%zipfilename)

    # Load metadata database
    dbcon = tempdb.load(sol_zip.open(model_xml),
                        create_db_file=False,
                        remove_invalid_chars=True)
    cur = dbcon.cursor()
    cur2 = dbcon.cursor()

    with h5py.File(h5filename, 'w', driver="core") as h5file:

        # Set up the HDF5 file
        data_group = h5file.create_group("data") # Holds property values
        metadata_group = h5file.create_group("metadata")
        objects_group = metadata_group.create_group("objects")
        relations_group = metadata_group.create_group("relations")
        times_group = metadata_group.create_group("times")

        # Predetermine number of bands associated with each property
        cur.execute("""SELECT p.name, MAX(k.band_id)
            FROM key k
            INNER JOIN property p ON k.property_id = p.property_id
            GROUP BY p.name""")
        band_counts = {prop: n_bands for (prop, n_bands) in cur.fetchall()}

        # Create HDF5 metadata datasets for each type of object/membership
        entity_counts = {}
        entity_idxs = {}
        cur.execute("""SELECT DISTINCT
            c.name AS collection, c.collection_id as collection_id,
            c1.name AS parent_class, c1.class_id AS parent_class_id,
            c2.name AS child_class, c2.class_id AS child_class_id
            FROM membership m
            INNER JOIN collection c ON m.collection_id = c.collection_id
            INNER JOIN class c1 ON m.parent_class_id = c1.class_id
            INNER JOIN class c2 ON m.child_class_id = c2.class_id""")
        for (collection, collection_id, parent_class, parent_class_id,
            child_class, child_class_id) in cur.fetchall():

            if parent_class == "System":
                dset, dset_name, dset_idxs = create_object_dset(
                    child_class, child_class_id,
                    cur2, objects_group)

            else:
                dset, dset_name, dset_idxs = create_relation_dset(
                    parent_class, collection, collection_id,
                    cur2, relations_group)

            print(len(dset), dset_name) if verbose else None
            entity_idxs[dset_name] = dset_idxs
            entity_counts[dset_name] = len(dset)

        # Create HDF5 metadata datasets for time indices in each timescale/period
        timestep_counts = {}
        cur.execute("SELECT name FROM sqlite_master " +
                    "WHERE type='table' and name LIKE 'period_%'")
        for (period_name,) in cur.fetchall():
            period_num = period_name_to_num(period_name)
            timescale = timescales[period_num]
            dset, dset_name = create_time_dset(timescale, cur2, times_group)
            print(len(dset), dset_name) if verbose else None
            timestep_counts[dset_name] = len(dset)

        # Create Time lists for each phase, needed as the period to
        # interval data sometimes comes out dirty
        cur.execute("SELECT phase_id FROM key GROUP BY phase_id")
        for (phase_id,) in cur.fetchall():
            logging.info("Creating time tables for phase %s", phase_id)
            cur2.execute("""SELECT min(pe.datetime) FROM phase_%d p
                INNER JOIN period_0 pe ON p.interval_id=pe.interval_id
                GROUP BY p.period_id"""%(phase_id))
            data = [x[0].encode('utf8') for x in cur2.fetchall()]
            phase = phases[phase_id]
            dset = times_group.create_dataset(phase, data=data,
                                              chunks=(len(data),),
                                              compression="gzip",
                                              compression_opts=1)
            print(len(dset), phase) if verbose else None
            timestep_counts[phase] = len(dset)

        # Add in the binary result data
        for period in range(5):

            # Check if binary file exists, otherwise, skip this period
            bin_name = "t_data_%d.BIN"%period
            if bin_name not in sol_filelist:
                continue

            bin_file = sol_zip.open(bin_name, "r")
            logging.info("Reading period %d binary data", period)
            num_read = 0

            # TODO: Data position/length/offset error checking
            # Do not order by position, it was created as TEXT
            cmd = "SELECT key_id, length, position FROM key_index WHERE period_type_id=?"
            cur.execute(cmd, (period,))
            for row in cur.fetchall():

                length = int(row[1])
                value_data = list(struct.unpack('<%dd'%length, bin_file.read(8*length)))

                cur2.execute("""SELECT parent_class, child_class, collection,
                    parent_name, child_name, prop_name,
                    period_type_id, phase_id, band_id, unit, summary_unit
                    FROM key_to_dataset
                    WHERE key_id=? AND period_type_id=?""", (row[0], period))

                dataset = cur2.fetchall()
                if len(dataset) != 1:
                    logging.error("Multiple datasets returned for key %s", row[0])
                    logging.error(dataset)
                    continue

                (parent_class, child_class, collection,
                 parent_name, child_name, prop_name,
                 period_type_id, phase_id, band_id,
                 unit, summary_unit) = dataset[0]

                if parent_class == "System":
                    collection_name = object_dset_name(child_class)
                    entity_idx = entity_idxs[collection_name][child_name]

                else:
                    collection_name = relation_dset_name(parent_class, collection)
                    entity_idx = entity_idxs[collection_name][parent_name, child_name]

                timescale = timescales[period_type_id]
                phase = phases[phase_id]
                dataset_path = '/'.join([phase, timescale, collection_name, prop_name])
                n_timesteps = timestep_counts[timescale]

                logging.info("Inserting data from key %s to dataset_path %s",
                            row[0], dataset_path)

                if dataset_path in data_group:
                    dset = data_group[dataset_path]

                else:
                    n_entities = entity_counts[collection_name]
                    n_bands = band_counts[prop_name]
                    dset = data_group.create_dataset(
                        dataset_path, dtype=np.float64,
                        shape=(n_entities, n_timesteps, n_bands),
                        chunks=(1, n_timesteps, n_bands),
                        compression="gzip", compression_opts=1)
                    dset.attrs['unit'] = unit if period == 0 else summary_unit

                dset[entity_idx, :, band_id-1] = np.pad(
                    value_data, (0, n_timesteps-len(value_data)),
                    'constant', constant_values=np.nan)
                num_read += length

            logging.info("Read %s values", num_read)

    h5file = h5py.File(h5filename, 'r')
    return h5file
