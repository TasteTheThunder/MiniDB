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
from index.index_manager import get_index_manager
from .foreign_key_manager import (
    validate_foreign_key_on_update,
    validate_restrict_on_update
)


def update_row(table, set_data, condition, database=None):
    """
    Update rows in a table based on condition.
    If database is specified, updates database-specific table.
    """
    tbl, meta = table_paths(table, database)
    check_table_exists(tbl, meta)

    metadata = json.load(open(meta))
    columns = [c[0] for c in metadata["columns"]]

    set_col, set_val = set_data
    cond_col, op, cond_val = condition

    if set_col not in columns:
        raise Exception(f"No column with name {set_col} found in {table}")
    if cond_col not in columns:
        raise Exception(f"No column with name {cond_col} found in {table}")

    dtype = None

    for c in metadata["columns"]:
        if c[0] == set_col:
            dtype = c[1]

    validate_value(set_val, dtype)

    if dtype.upper() in ["CHAR", "VARCHAR"]:
        set_val = remove_quotes(set_val)

    validate_foreign_key_on_update(table, set_col, set_val, database)

    si = columns.index(set_col)
    ci = columns.index(cond_col)

    new = []
    updated = 0
    affected_rows = []

    for row in open(tbl):
        vals = row.strip().split(",")

        if compare(
            vals[ci],
            op,
            cond_val
        ):
            affected_rows.append(list(vals))
            vals[si] = set_val
            updated += 1

        new.append(",".join(vals) + "\n")

    if updated == 0:
        raise Exception(
            f"No matching row found for {cond_col} {op} {cond_val} in {table}"
        )

    validate_restrict_on_update(table, columns, set_col, affected_rows, database)

    open(tbl, "w").writelines(new)

    # =====================================
    # INDEX MAINTENANCE (rebuild if needed)
    # =====================================

    if updated > 0:
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
        f"Updating rows where {cond_col} {op} {cond_val}",
        f"Rows Updated : {updated}"
    ])

    print_trace("FILE SYSTEM", [
        f"{table}.tbl rewritten"
    ])

    print_result("✅ UPDATE Completed")
