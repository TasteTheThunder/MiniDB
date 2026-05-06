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

    print_trace("STORAGE ENGINE", [
        f"Deleting rows where {cond_col} {op} {val}",
        f"Rows Deleted : {deleted}"
    ])

    print_trace("FILE SYSTEM", [
        f"{table}.tbl rewritten"
    ])

    print_result("✅ DELETE Completed")
