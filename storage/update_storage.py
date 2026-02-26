"""
Storage operations for UPDATE
"""
import json
from visualizer import print_trace, print_result
from utils import (
    table_paths,
    check_table_exists,
    remove_quotes,
    compare,
    validate_value
)


def update_row(table, set_data, condition):
    """
    Update rows in a table based on condition
    """
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)

    metadata = json.load(open(meta))
    columns = [c[0] for c in metadata["columns"]]

    set_col, set_val = set_data
    cond_col, op, cond_val = condition

    dtype = None

    for c in metadata["columns"]:
        if c[0] == set_col:
            dtype = c[1]

    validate_value(set_val, dtype)

    if dtype.upper() in ["CHAR", "VARCHAR"]:
        set_val = remove_quotes(set_val)

    si = columns.index(set_col)
    ci = columns.index(cond_col)

    new = []
    updated = 0

    for row in open(tbl):
        vals = row.strip().split(",")

        if compare(
            vals[ci],
            op,
            cond_val
        ):
            vals[si] = set_val
            updated += 1

        new.append(",".join(vals) + "\n")

    open(tbl, "w").writelines(new)

    print_trace("STORAGE ENGINE", [
        f"Updating rows where {cond_col} {op} {cond_val}",
        f"Rows Updated : {updated}"
    ])

    print_trace("FILE SYSTEM", [
        f"{table}.tbl rewritten"
    ])

    print_result("✅ UPDATE Completed")
