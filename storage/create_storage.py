"""
Storage operations for CREATE TABLE
"""
import os
import json
from visualizer import print_trace, print_result
from utils import table_paths

DATA_DIR = "data"
META_DIR = "metadata"
SUPPORTED_TYPES = ["INT", "DOUBLE", "CHAR", "VARCHAR"]


def create_table(table, columns, primary_key=None):
    """
    Create a new table with specified columns and optional primary key
    """
    tbl, meta = table_paths(table)

    if os.path.exists(tbl):
        raise Exception("Table already exists")

    # datatype validation
    for name, dtype in columns:
        if dtype.upper() not in SUPPORTED_TYPES:
            raise Exception(
                f"Unsupported datatype {dtype}"
            )

    # Normalize primary_key to always be a list (for composite key support)
    if primary_key:
        if isinstance(primary_key, str):
            primary_key = [primary_key]  # Convert single key to list
        
        names = [c[0] for c in columns]
        
        # Validate all primary key columns exist
        for pk_col in primary_key:
            if pk_col not in names:
                raise Exception(
                    f"Primary Key column '{pk_col}' must be valid column"
                )

    open(tbl, "w").close()

    with open(meta, "w") as f:
        json.dump({
            "columns": columns,
            "primary_key": primary_key
        }, f)

    print_trace("STORAGE ENGINE", [
        f"Created Data File : {tbl}",
        f"Created Metadata : {meta}"
    ])

    print_trace("FILE SYSTEM", [
        f"{table}.tbl initialized"
    ])

    print_result("✅ Table Created Successfully")
