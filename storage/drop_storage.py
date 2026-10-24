"""
Storage operations for DROP TABLE
"""
import os
from index.index_utils import (
    list_indices,
    delete_index,
    invalidate_row_offsets,
)
from index.query_stats import get_query_stats
from index.index_manager import invalidate_table_index_cache
from normalization.fd_manager import delete_fds
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

    # Remove index artifacts (hash/sorted and offsets)
    removed_index_files = []
    for column, index_type in list_indices(table, database):
        if delete_index(table, column, index_type, database):
            removed_index_files.append(f"{table}_{column}.{index_type}")

    if invalidate_row_offsets(table, database):
        removed_index_files.append(f"{table}.offsets")
    invalidate_table_index_cache(table, database)

    # Remove query stats for the table
    stats = get_query_stats(database)
    stats.reset_stats(table=table)

    # Remove functional dependencies file if present
    delete_fds(table, database)

    trace_lines = [
        f"Deleted {tbl}",
        f"Deleted {meta}"
    ]
    if removed_index_files:
        trace_lines.append(
            "Deleted index files: " + ", ".join(sorted(removed_index_files))
        )

    print_trace("STORAGE ENGINE", trace_lines)

    print_trace("FILE SYSTEM", [
        f"{table} removed"
    ])

    print_result("✅ Table Dropped")
