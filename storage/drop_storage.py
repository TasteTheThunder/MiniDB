"""
Storage operations for DROP TABLE
"""
import os
from index.index_utils import (
    list_indices,
    delete_index,
    get_offsets_path
)
from index.query_stats import get_query_stats
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

    offsets_path = get_offsets_path(table, database)
    if os.path.exists(offsets_path):
        os.remove(offsets_path)
        removed_index_files.append(os.path.basename(offsets_path))

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
