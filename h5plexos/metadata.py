import numpy as np

def create_collection_dataset(collection, collection_id,
                              parent_class, parent_class_id,
                              child_class, child_class_id,
                              cur, h5group):

    relation_dtype = np.dtype([
        ("parent_" + parent_class, "S64"),
        ("child_" + child_class, "S64")
    ])

    cur.execute("""SELECT o1.name AS parent_name, o2.name AS child_name
        FROM membership m
        INNER JOIN collection c ON m.collection_id = c.collection_id
        INNER JOIN object o1 ON m.parent_object_id = o1.object_id
        INNER JOIN object o2 ON m.child_object_id = o2.object_id
        WHERE c.collection_id = ?""", (collection_id,))

    data = np.array([row for row in cur.fetchall()], dtype=relation_dtype)

    dset_name = ".".join([parent_class, collection])
    dset = h5group.create_dataset(dset_name,
        data=data, compression="gzip", compression_opts=1)

    return dset, dset_name

# Function should take a SQLite DB connection and an
# HDF5 Group object, and create and return a new HDF5
# dataset in the HDF5 group.

# Default metadata selection. Can also serve as a
# starting point for custom functions
def default_metadata(class_name, class_id, cur, metadata_group):

    meta_dtype = np.dtype([
        ("name", "S64"),
        ("category", "S64")
    ])

    cur.execute("""SELECT ob.name AS name, cat.name as category
        FROM object ob
        INNER JOIN category cat ON ob.category_id=cat.category_id
        WHERE ob.class_id=?""", [class_id])

    #TODO: Avoid fixed-length strings? Will truncate long names.
    # Or at least throw a warning if truncation happens?
    data = np.array([row for row in cur.fetchall()], dtype=meta_dtype)

    dset = metadata_group.create_dataset(
        class_name, data=data,
        compression="gzip", compression_opts=1
    )

    return dset, class_name

# Custom metadata selection definitions

def generator_metadata(class_name, class_id, cur, h5group):
    return default_metadata(class_name, class_id, cur, h5group)

def line_metadata(class_name, class_id, cur, h5group):
    return default_metadata(class_name, class_id, cur, h5group)


metadata_creators = {
    "Generator": generator_metadata,
    "Line": line_metadata
}

def create_object_dataset(class_name, class_id, cur, h5group):
    f = metadata_creators.get(class_name, default_metadata)
    return f(class_name, class_id, cur, h5group)
