import numpy as np
import pandas as pd

def object_dset_name(child_class):
    return child_class.lower()


def relation_dset_name(parent_class, collection):
    collection = collection.replace(" ", "")
    dset_name = "_".join([parent_class, collection]).lower()
    return dset_name


# Each period has its own way of storing timestamps
timescale_query_params = {
    "interval": ("datetime", "period_0", "interval_id", False),
    "day": ("date", "period_1", "day_id", False),
    "week": ("week_ending", "period_2", "week_id", True),
    "month": ("month_beginning", "period_3", "month_id", True),
    "year": ("year_ending", "period_4", "fiscal_year_id", True),
}

def create_time_dset(timescale, cur, h5group, length, offset):

    data_col, table_name, order_col, strip_tz = timescale_query_params[timescale]

    cur.execute("SELECT %s FROM %s ORDER BY %s"%(
        data_col, table_name, order_col))
    data = [x[0] for x in cur.fetchall()]

    if strip_tz:
        data = [dt[:-6] for dt in data]

    # Piggy-back on pandas date format guessing
    data = pd.to_datetime(data[offset:(offset+length)],
        dayfirst=True # WARNING: For some locales, dayfirst=True may not be correct...
        ).strftime("%Y-%m-%dT%H:%M:%S")

    dset = h5group.create_dataset(
        timescale,
        data=[dt.encode('utf8') for dt in data],
        chunks=(length,), compression="gzip", compression_opts=1)

    return dset, timescale


def create_object_dset(class_name, class_id, cur, h5group):

    #TODO: Avoid fixed-length strings? Will truncate long names.
    # Or at least throw a warning if truncation happens?
    meta_dtype = np.dtype([
        ("name", "S64"),
        ("category", "S64")
    ])

    cur.execute("""SELECT ob.name AS name, cat.name as category
        FROM object ob
        INNER JOIN category cat ON ob.category_id=cat.category_id
        WHERE ob.class_id=?""", [class_id])

    data = [row for row in cur.fetchall()]
    dset_idxs = {n: i for (i, (n, c)) in enumerate(data)}
    data = np.array(data, dtype=meta_dtype)

    dset_name = object_dset_name(class_name)
    dset = h5group.create_dataset(
        dset_name, data=data,
        compression="gzip", compression_opts=1
    )

    return dset, dset_name, dset_idxs


def create_relation_dset(parent_class, collection,
                              collection_id, cur, h5group):

    relation_dtype = np.dtype([
        ("parent", "S64"),
        ("child", "S64")
    ])

    cur.execute("""SELECT o1.name AS parent_name, o2.name AS child_name
        FROM membership m
        INNER JOIN collection c ON m.collection_id = c.collection_id
        INNER JOIN object o1 ON m.parent_object_id = o1.object_id
        INNER JOIN object o2 ON m.child_object_id = o2.object_id
        WHERE c.collection_id = ?""", (collection_id,))

    data = [row for row in cur.fetchall()]
    dset_idxs = {(p, c): i for (i, (p, c)) in enumerate(data)}
    data = np.array(data, dtype=relation_dtype)

    dset_name = relation_dset_name(parent_class, collection)
    dset = h5group.create_dataset(dset_name,
        data=data, compression="gzip", compression_opts=1)

    return dset, dset_name, dset_idxs
