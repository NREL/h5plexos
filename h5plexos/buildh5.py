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
from .metadata import create_object_dataset, create_collection_dataset

def process_solution(zipfilename, h5filename=None):
    """Read in the plexos solution zipfile and save data to an hdf5 file

    Args:
        zipfilename - string name for zipfile
        h5filename - string name for h5file to write to, will be overwritten if
            exists.  If None, use the base zipfilename with h5 suffix

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

    h5file = h5py.File(h5filename, 'w', driver="core")
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

    # Set up the HDF5 file
    data_group = h5file.create_group("data") # Holds property values
    metadata_group = h5file.create_group("metadata")
    objects_group = metadata_group.create_group("objects")
    relations_group = metadata_group.create_group("relations")
    times_group = metadata_group.create_group("times")

    # Create HDF5 metadata datasets for each type of object/membership
    object_counts = {}
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
            dset, dset_name = create_object_dataset(
                collection, child_class_id, cur2, objects_group)

        else:
            dset, dset_name = create_collection_dataset(
                parent_class, collection, collection_id, cur2, relations_group)

        print(len(dset), dset_name)
        object_counts[dset_name] = len(dset)

    # Add time lists for each period.  Each period has its own way of storing time
    timestep_counts = {}
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' and name LIKE 'period_%'")
    period_list = [x[0] for x in cur.fetchall()]

    if "period_0" in period_list:
        cur.execute("SELECT datetime FROM period_0 ORDER BY interval_id")
        times_group.create_dataset("period_0", data=[x[0].encode('utf8') for x in cur.fetchall()])
        timestep_counts["period_0"] = len(times_group["period_0"])

    if "period_1" in period_list:
        cur.execute("SELECT date FROM period_1 ORDER BY day_id")
        times_group.create_dataset("period_1", data=[x[0].encode('utf8') for x in cur.fetchall()])
        timestep_counts["period_1"] = len(times_group["period_1"])

    if "period_2" in period_list:
        logging.warn("Period 2 not implemented yet")
        # TODO: Find something with period 2

    if "period_3" in period_list:
        cur.execute("SELECT month_beginning FROM period_3 ORDER BY month_id")
        times_group.create_dataset("period_3", data=[x[0].encode('utf8') for x in cur.fetchall()])
        timestep_counts["period_3"] = len(times_group["period_3"])

    if "period_4" in period_list:
        cur.execute("SELECT year_ending FROM period_4 ORDER BY fiscal_year_id")
        times_group.create_dataset("period_4", data=[x[0].encode('utf8') for x in cur.fetchall()])
        timestep_counts["period_4"] = len(times_group["period_4"])

    # Create Time lists for each phase, needed as the period to
    # interval data sometimes comes out dirty
    cur.execute("SELECT phase_id FROM key GROUP BY phase_id")
    for (phase_id,) in cur.fetchall():
        logging.info("Creating time tables for phase %s", phase_id)
        cur2.execute("""SELECT min(pe.datetime) FROM phase_%d p
            INNER JOIN period_0 pe ON p.interval_id=pe.interval_id
            GROUP BY p.period_id"""%(phase_id))
        dat = [x[0].encode('utf8') for x in cur2.fetchall()]
        times_group.create_dataset("phase_%d"%phase_id, data=dat)
        timestep_counts["phase_%d"%phase_id] = len(times_group["period_4"])

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

            # Need property.name, object.name, class.name, phase_id
            cur2.execute("""SELECT parent_class, collection, parent_name, child_name,
                prop_name, period_type_id, phase_id, is_multi_band, band_id, unit
                FROM key_to_dataset
                WHERE key_id=? AND period_type_id=?""", (row[0], period))

            dataset = cur2.fetchall()
            if len(dataset) != 1:
                logging.error("Multiple datasets returned for key %s", row[0])
                logging.error(dataset)
                continue

            (parent_class, collection, parent_name, child_name, prop_name,
             period_type_id, phase_id, is_multi_band, band_id, unit) = dataset[0]

            if parent_class == "System":
                # If this bottlenecks maybe pre-generate a hash table
                # during metadata creation
                obj_idx = np.where(
                    objects_group[collection]["name"] == bytes(child_name, "UTF8"))[0][0]

            else:
                collection = '.'.join([parent_class, collection])
                obj_idx = np.where(
                    relations_group[collection]["parent"] == bytes(parent_name, "UTF8") &
                    relations_group[collection]["child"] == bytes(child_name, "UTF8")
                )[0][0]

            period_name = 'period_%d'%period_type_id
            phase_name = 'phase_%d'%phase_id
            dataset_path = '/'.join([collection, prop_name, period_name, phase_name])
            n_timesteps = timestep_counts[period_name]

            if is_multi_band == "true":
                dataset_path = dataset_path + '/band_%d'%band_id

            logging.info("Inserting data from key %s to dataset_path %s",
                         row[0], dataset_path)

            if dataset_path not in data_group:
                n_objects = object_counts[collection]
                dset = data_group.create_dataset(dataset_path, dtype=np.float64,
                                                 shape=(n_objects, n_timesteps),
                                                 chunks=(1, n_timesteps),
                                                 compression="gzip",
                                                 compression_opts=1)
                dset.attrs['unit'] = unit

            else:
                dset = data_group[dataset_path]

            dset[obj_idx, :] = np.pad(value_data,
                                      (0, n_timesteps-len(value_data)),
                                      'constant', constant_values=np.nan)
            num_read += length

        logging.info("Read %s values", num_read)

    h5file.close()
    h5file = h5py.File(h5filename, 'r')
    return h5file
