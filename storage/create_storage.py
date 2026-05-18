"""
Storage operations for CREATE TABLE
"""
import os
import json
from visualizer import print_trace, print_result
from utils import table_paths, check_table_exists

DATA_DIR = "data"
META_DIR = "metadata"
SUPPORTED_TYPES = ["INT", "DOUBLE", "CHAR", "VARCHAR"]


def create_table(table, columns, primary_key=None, foreign_keys=None, database=None):
    """
    Create a new table with specified columns and optional primary key.
    If database is specified, creates table in that database.
    If database is None, uses legacy path (for backward compatibility).
    """
    tbl, meta = table_paths(table, database)

    # Create directories if they don't exist
    if database:
        os.makedirs(os.path.dirname(tbl), exist_ok=True)
        os.makedirs(os.path.dirname(meta), exist_ok=True)

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

    if foreign_keys is None:
        foreign_keys = []

    # Validate foreign keys
    column_names = [c[0] for c in columns]
    for fk in foreign_keys:
        fk_col = fk.get("column")
        ref_table = fk.get("references_table")
        ref_col = fk.get("references_column")

        if fk_col not in column_names:
            raise Exception(f"Foreign key column '{fk_col}' must be a valid column")

        ref_tbl, ref_meta = table_paths(ref_table, database)
        check_table_exists(ref_tbl, ref_meta)

        ref_metadata = json.load(open(ref_meta))
        ref_columns = [c[0] for c in ref_metadata["columns"]]
        if ref_col not in ref_columns:
            raise Exception(
                f"Referenced column '{ref_col}' not found in {ref_table}"
            )

    open(tbl, "w").close()

    with open(meta, "w") as f:
        json.dump({
            "columns": columns,
            "primary_key": primary_key,
            "foreign_keys": foreign_keys
        }, f)

    print_trace("STORAGE ENGINE", [
        f"Created Data File : {tbl}",
        f"Created Metadata : {meta}"
    ])

    print_trace("FILE SYSTEM", [
        f"{table}.tbl initialized"
    ])

    print_result("✅ Table Created Successfully")
