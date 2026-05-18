"""
Foreign key metadata utilities and referential integrity enforcement.
"""
import json
import os
from visualizer import print_trace
from utils import table_paths, check_table_exists, remove_quotes


def _load_metadata(table, database=None):
    _tbl, meta = table_paths(table, database)
    check_table_exists(_tbl, meta)
    return json.load(open(meta))


def _load_rows(table, database=None):
    tbl, meta = table_paths(table, database)
    check_table_exists(tbl, meta)
    rows = []
    with open(tbl, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(line.split(","))
    return rows


def _list_meta_files(database=None):
    meta_dir = os.path.join("metadata", database) if database else "metadata"
    if not os.path.exists(meta_dir):
        return []
    return [
        os.path.join(meta_dir, f)
        for f in os.listdir(meta_dir)
        if f.endswith(".meta")
    ]


def _table_name_from_meta(meta_path):
    return os.path.splitext(os.path.basename(meta_path))[0]


def get_foreign_keys(table, database=None):
    metadata = _load_metadata(table, database)
    return metadata.get("foreign_keys", []) or []


def find_referencing_foreign_keys(target_table, database=None):
    references = []
    for meta_path in _list_meta_files(database):
        meta = json.load(open(meta_path))
        table_name = _table_name_from_meta(meta_path)

        for fk in meta.get("foreign_keys", []) or []:
            if fk.get("references_table") == target_table:
                references.append({
                    "table": table_name,
                    "column": fk.get("column"),
                    "references_table": target_table,
                    "references_column": fk.get("references_column")
                })

    return references


def _value_exists(table, column, value, database=None):
    metadata = _load_metadata(table, database)
    columns = [c[0] for c in metadata["columns"]]
    if column not in columns:
        raise Exception(f"No column with name {column} found in {table}")

    col_idx = columns.index(column)
    rows = _load_rows(table, database)
    for row in rows:
        if row[col_idx] == value:
            return True
    return False


def validate_foreign_keys_on_insert(table, columns, values, database=None):
    foreign_keys = get_foreign_keys(table, database)
    if not foreign_keys:
        return

    traces = []

    for fk in foreign_keys:
        col = fk.get("column")
        ref_table = fk.get("references_table")
        ref_col = fk.get("references_column")

        if col not in columns:
            raise Exception(f"Foreign key column '{col}' not found in {table}")

        value = values[columns.index(col)]
        value = remove_quotes(str(value))

        if value == "NULL":
            continue

        traces.append(f"Checking if {ref_table}.{ref_col} = {value} exists")

        if not _value_exists(ref_table, ref_col, value, database):
            raise Exception(
                f"Foreign key violation: {table}.{col} references {ref_table}.{ref_col} ({value})"
            )

    if traces:
        print_trace("FOREIGN KEY", traces)


def validate_foreign_key_on_update(table, set_col, set_val, database=None):
    foreign_keys = get_foreign_keys(table, database)
    if not foreign_keys:
        return

    for fk in foreign_keys:
        if fk.get("column") != set_col:
            continue

        ref_table = fk.get("references_table")
        ref_col = fk.get("references_column")
        value = remove_quotes(str(set_val))

        if value == "NULL":
            return

        print_trace("FOREIGN KEY", [
            f"Checking if {ref_table}.{ref_col} = {value} exists"
        ])

        if not _value_exists(ref_table, ref_col, value, database):
            raise Exception(
                f"Foreign key violation: {table}.{set_col} references {ref_table}.{ref_col} ({value})"
            )


def validate_restrict_on_delete(table, columns, deleted_rows, database=None):
    references = find_referencing_foreign_keys(table, database)
    if not references or not deleted_rows:
        return

    referenced_cols = {ref["references_column"] for ref in references}
    values_by_column = {col: set() for col in referenced_cols}

    for row in deleted_rows:
        for col in referenced_cols:
            if col in columns:
                values_by_column[col].add(row[columns.index(col)])

    for ref in references:
        child_table = ref["table"]
        child_column = ref["column"]
        ref_col = ref["references_column"]
        values = values_by_column.get(ref_col, set())

        if not values:
            continue

        child_rows = _load_rows(child_table, database)
        child_meta = _load_metadata(child_table, database)
        child_columns = [c[0] for c in child_meta["columns"]]

        if child_column not in child_columns:
            continue

        child_idx = child_columns.index(child_column)

        for row in child_rows:
            if row[child_idx] in values:
                raise Exception(
                    f"Cannot delete row because it is referenced by foreign key in table {child_table}"
                )


def validate_restrict_on_update(table, columns, set_col, affected_rows, database=None):
    references = find_referencing_foreign_keys(table, database)
    if not references or not affected_rows:
        return

    ref_columns = {ref["references_column"] for ref in references}
    if set_col not in ref_columns:
        return

    old_values = set()
    for row in affected_rows:
        old_values.add(row[columns.index(set_col)])

    if not old_values:
        return

    for ref in references:
        if ref["references_column"] != set_col:
            continue

        child_table = ref["table"]
        child_column = ref["column"]

        child_rows = _load_rows(child_table, database)
        child_meta = _load_metadata(child_table, database)
        child_columns = [c[0] for c in child_meta["columns"]]

        if child_column not in child_columns:
            continue

        child_idx = child_columns.index(child_column)

        for row in child_rows:
            if row[child_idx] in old_values:
                raise Exception(
                    f"Cannot update row because it is referenced by foreign key in table {child_table}"
                )

    print_trace("FOREIGN KEY", [
        f"Referential integrity check passed for updates on {table}.{set_col}"
    ])
