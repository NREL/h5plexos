import numpy as np

# Function should take a SQLite DB connection and an
# HDF5 Group object, and create and return a new HDF5
# dataset in the HDF5 group.

# Default metadata selection. Can also serve as a
# starting point for custom functions
def default_metadata(class_meta, cur, metadata_group):

    meta_dtype = np.dtype([
        ("name", "S30"),
        ("category", "S30")
    ])

    cur.execute("""SELECT ob.name AS name, cat.name as category
        FROM object ob
        INNER JOIN category cat ON ob.category_id=cat.category_id
        WHERE ob.class_id=?""", [class_meta['class_id']])

    #TODO: Avoid fixed-length strings - will truncate long names
    data = np.array([row for row in cur.fetchall()], dtype=meta_dtype)

    objects_metadata = metadata_group.create_dataset(
        class_meta['name'],
        data=data,
        compression="gzip",
        compression_opts=1
    )

    # Save class metadata as attributes on objects dataset
    for (name, val) in class_meta.items():
        objects_metadata.attrs[name] = val

    return objects_metadata

# Custom metadata selection definitions

def generator_metadata(class_meta, cur, h5group):
    return default_metadata(class_meta, cur, h5group)

def line_metadata(class_meta, cur, h5group):
    return default_metadata(class_meta, cur, h5group)


metadata_creators = {
    "Generator": generator_metadata,
    "Line": line_metadata
}

def create_metadata_dataset(class_meta, dbcon, h5group):
    f = metadata_creators.get(class_meta["name"], default_metadata)
    return f(class_meta, dbcon, h5group)
