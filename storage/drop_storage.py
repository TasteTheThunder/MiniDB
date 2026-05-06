"""
Storage operations for DROP TABLE
"""
import os
from visualizer import print_trace, print_result
from utils import (
    table_paths,
    check_table_exists
)


def drop_table(table, database=None):
    """
    Drop (delete) a table completely.
    If database is specified, drops database-specific table.
    """
    tbl, meta = table_paths(table, database)
    check_table_exists(tbl, meta)

    os.remove(tbl)
    os.remove(meta)

    print_trace("STORAGE ENGINE", [
        f"Deleted {tbl}",
        f"Deleted {meta}"
    ])

    print_trace("FILE SYSTEM", [
        f"{table} removed"
    ])

    print_result("✅ Table Dropped")
