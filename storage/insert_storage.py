"""
Storage operations for INSERT
"""
import json
from visualizer import print_trace, print_result
from utils import (
    table_paths,
    check_table_exists,
    remove_quotes,
    validate_value
)
from index.index_manager import get_index_manager
from .foreign_key_manager import validate_foreign_keys_on_insert


def insert_row(table, values, insert_columns=None, database=None):
    """
    Insert a row into a table.
    If database is specified, inserts into database-specific table.
    """
    import os
    tbl, meta = table_paths(table, database)
    # Create directories if needed
    if database:
        os.makedirs(os.path.dirname(tbl), exist_ok=True)
        os.makedirs(os.path.dirname(meta), exist_ok=True)
    check_table_exists(tbl, meta)

    metadata = json.load(open(meta))
    column_defs = metadata["columns"]
    columns = [c[0] for c in column_defs]
    pk = metadata.get("primary_key")

    # =====================================
    # BUILD FINAL ROW (NULL DEFAULT)
    # =====================================

    final_values = ["NULL"] * len(columns)

    # ---------- OLD INSERT ----------

    if insert_columns is None:
        if len(values) != len(columns):
            raise Exception(
                "Column count mismatch"
            )
        insert_columns = columns

    # ---------- COLUMN INSERT ----------

    if len(insert_columns) != len(values):
        raise Exception(
            "Column count mismatch"
        )

    # fill values

    for col, value in zip(insert_columns, values):
        if col not in columns:
            raise Exception(
                f"Column {col} not found"
            )

        idx = columns.index(col)
        datatype = column_defs[idx][1]

        validate_value(
            value,
            datatype
        )

        if datatype.upper() in ["CHAR", "VARCHAR"]:
            value = remove_quotes(value)

        final_values[idx] = value

    # =====================================
    # PRIMARY KEY CHECK
    # =====================================

    existing_rows = []
    if os.path.exists(tbl):
        with open(tbl) as f:
            existing_rows = f.readlines()

    if pk:
        # Support both single and composite primary keys
        if isinstance(pk, str):
            pk = [pk]  # Convert to list for uniform handling
        
        # Get indices for all primary key columns
        pk_indices = [columns.index(pk_col) for pk_col in pk]
        
        # Check that none of the PK columns are NULL
        for i, pk_col in zip(pk_indices, pk):
            if final_values[i] == "NULL":
                raise Exception(
                    f"Primary Key column '{pk_col}' cannot be NULL"
                )
        
        # Duplicate check - compare composite key
        for row in existing_rows:
            existing = row.strip().split(",")
            
            # Check if all PK columns match (composite key duplicate check)
            if all(existing[i] == final_values[i] for i in pk_indices):
                if len(pk) == 1:
                    raise Exception(
                        f"Primary Key violation: {pk[0]} = {final_values[pk_indices[0]]}"
                    )
                else:
                    pk_values = ", ".join([f"{pk[i]}={final_values[pk_indices[i]]}" for i in range(len(pk))])
                    raise Exception(
                        f"Composite Primary Key violation: ({pk_values})"
                    )

    # =====================================
    # FOREIGN KEY CHECK
    # =====================================

    validate_foreign_keys_on_insert(table, columns, final_values, database)

    # =====================================
    # WRITE FILE
    # =====================================

    row_num = len(existing_rows)
    line = ",".join(final_values)
    open(tbl, "a").write(line + "\n")

    # =====================================
    # INDEX MAINTENANCE (if any index exists)
    # =====================================

    manager = get_index_manager(database)
    indices = manager.list_indices(table)
    if indices:
        updated_indices = []
        for col_name, index_type in indices:
            if col_name not in columns:
                continue
            col_idx = columns.index(col_name)
            value = final_values[col_idx]

            if index_type == "hash":
                index = manager.get_hash_index(table, col_name)
                if index:
                    index.insert(value, row_num)
                    index.save()
                    updated_indices.append(f"{col_name}.hash")
            elif index_type == "sorted":
                index = manager.get_sorted_index(table, col_name)
                if index:
                    index.insert(value, row_num)
                    index.save()
                    updated_indices.append(f"{col_name}.sorted")

        if updated_indices:
            print_trace("INDEX", [
                "Updated indices: " + ", ".join(updated_indices)
            ])

    print_trace("STORAGE ENGINE", [
        f"Open File : {tbl}",
        "Mode : Append",
        f"Data Written : {line}"
    ])

    print_trace("FILE SYSTEM", [
        f"{table}.tbl updated"
    ])

    print_result("✅ Row Inserted Successfully")
