"""
Storage operations for DELETE
"""
import json
from visualizer import print_trace, print_result
from utils import (
    table_paths,
    check_table_exists,
    compare
)
from index.index_manager import get_index_manager


def delete_row(table, condition, database=None):
    """
    Delete rows from a table based on condition.
    If database is specified, deletes from database-specific table.
    """
    tbl, meta = table_paths(table, database)
    check_table_exists(tbl, meta)

    metadata = json.load(open(meta))
    columns = [c[0] for c in metadata["columns"]]

    cond_col, op, val = condition
    ci = columns.index(cond_col)

    new = []
    deleted = 0

    for row in open(tbl):
        vals = row.strip().split(",")

        if compare(
            vals[ci],
            op,
            val
        ):
            deleted += 1
        else:
            new.append(row)

    open(tbl, "w").writelines(new)

    # =====================================
    # INDEX MAINTENANCE (rebuild if needed)
    # =====================================

    if deleted > 0:
        manager = get_index_manager(database)
        indices = manager.list_indices(table)
        if indices:
            table_data = [row.strip().split(",") for row in new]
            rebuilt = []
            for col_name, index_type in indices:
                if col_name not in columns:
                    continue
                col_idx = columns.index(col_name)
                if index_type == "hash":
                    manager.create_hash_index(table, col_name, table_data, col_idx)
                    rebuilt.append(f"{col_name}.hash")
                elif index_type == "sorted":
                    manager.create_sorted_index(table, col_name, table_data, col_idx)
                    rebuilt.append(f"{col_name}.sorted")

            if rebuilt:
                print_trace("INDEX", [
                    "Rebuilt indices: " + ", ".join(rebuilt)
                ])

    print_trace("STORAGE ENGINE", [
        f"Deleting rows where {cond_col} {op} {val}",
        f"Rows Deleted : {deleted}"
    ])

    print_trace("FILE SYSTEM", [
        f"{table}.tbl rewritten"
    ])

    print_result("✅ DELETE Completed")
