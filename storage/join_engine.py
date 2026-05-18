"""
Nested-loop join engine for NirvahaDB.
"""
import json
from visualizer import print_trace
from utils import table_paths, check_table_exists


def _load_table(table, database=None):
    tbl, meta = table_paths(table, database)
    check_table_exists(tbl, meta)

    metadata = json.load(open(meta))
    columns = [c[0] for c in metadata["columns"]]

    rows = []
    with open(tbl, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(line.split(","))

    return tbl, columns, rows


def _resolve_join_column(table, columns, column_name):
    if "." in column_name:
        parts = column_name.split(".")
        if len(parts) != 2 or parts[0] != table:
            raise Exception(
                f"Invalid join column '{column_name}' for table '{table}'"
            )
        column_name = parts[1]

    if column_name not in columns:
        raise Exception(f"No column with name {column_name} found in {table}")

    return column_name, columns.index(column_name)


def _prefix_columns(table, columns):
    return [f"{table}.{col}" for col in columns]


def nested_loop_join(left_table, right_table, left_column, right_column, join_type, database=None):
    join_type = join_type.upper()
    if join_type not in ["INNER", "LEFT", "RIGHT"]:
        raise Exception(f"Unsupported join type: {join_type}")

    left_tbl, left_cols, left_rows = _load_table(left_table, database)
    right_tbl, right_cols, right_rows = _load_table(right_table, database)

    left_col, left_idx = _resolve_join_column(left_table, left_cols, left_column)
    right_col, right_idx = _resolve_join_column(right_table, right_cols, right_column)

    joined_columns = _prefix_columns(left_table, left_cols) + _prefix_columns(right_table, right_cols)

    comparisons = 0
    matched_rows = 0
    joined_rows = []
    right_matched = [False] * len(right_rows)
    sample_matches = []

    for left_row in left_rows:
        left_val = left_row[left_idx]
        row_matched = False

        for r_index, right_row in enumerate(right_rows):
            comparisons += 1
            right_val = right_row[right_idx]

            if left_val == right_val:
                row_matched = True
                right_matched[r_index] = True
                matched_rows += 1
                joined_rows.append(left_row + right_row)

                if len(sample_matches) < 5:
                    sample_matches.append(
                        f"Matched Row : {left_table}.{left_col} = {right_table}.{right_col} -> {left_val} = {right_val}"
                    )

        if not row_matched and join_type == "LEFT":
            joined_rows.append(left_row + ["NULL"] * len(right_cols))

    if join_type == "RIGHT":
        for right_row, is_matched in zip(right_rows, right_matched):
            if not is_matched:
                joined_rows.append(["NULL"] * len(left_cols) + right_row)

    print_trace("JOIN ENGINE", [
        f"Join Type : {join_type}",
        f"Left Table : {left_table} ({len(left_rows)} row(s))",
        f"Right Table : {right_table} ({len(right_rows)} row(s))",
        f"Join Condition : {left_table}.{left_col} = {right_table}.{right_col}",
        f"Comparisons : {comparisons}",
        f"Matched Rows : {matched_rows}",
        f"Output Rows : {len(joined_rows)}"
    ])

    if sample_matches:
        print_trace("JOIN MATCH", sample_matches)

    return joined_columns, joined_rows
