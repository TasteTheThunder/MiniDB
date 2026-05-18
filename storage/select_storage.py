"""
Storage operations for SELECT
"""
import json
from visualizer import print_trace, print_result
from utils import (
    table_paths,
    check_table_exists,
    compare,
    remove_quotes
)
from index.index_manager import get_index_manager
from index.index_utils import build_row_offsets, load_row_offsets
from .join_engine import nested_loop_join
from .subquery_engine import execute_subquery, extract_in_values, ensure_non_correlated


def _determine_operator_type(op):
    """
    Determine if operator is equality or range query.
    
    Args:
        op: Operator string ('=', '<', '>', '<=', '>=')
    
    Returns:
        'equality' or 'range'
    """
    return 'equality' if op == '=' else 'range'


def _get_filtered_rows_with_index(table, tbl, condition, columns, metadata, database=None):
    """
    Attempt to use index for filtering, fallback to full scan.
    
    Args:
        table: Table name
        tbl: Path to table file
        condition: Tuple (column, operator, value) or None
        columns: List of column names
        metadata: Table metadata dict
    
    Returns:
        Tuple (filtered_rows, rows_scanned, used_index, index_hits)
            - filtered_rows: list of row values [val1, val2, ...]
            - rows_scanned: number of rows examined
            - used_index: bool indicating if index was used
            - index_hits: number of row ids returned by index (None if no index)
    """
    manager = get_index_manager(database)
    
    def _full_scan_filter(col_idx, op, val):
        """Run a full scan filter and return (filtered_rows, all_rows)."""
        all_rows = open(tbl).readlines()
        filtered_rows = []
        for row in all_rows:
            vals = row.strip().split(",")
            if compare(vals[col_idx], op, val):
                filtered_rows.append(vals)
        return filtered_rows, all_rows

    if not condition:
        # No condition, must do full table scan
        rows = open(tbl).readlines()
        filtered = [row.strip().split(",") for row in rows]
        return (filtered, len(rows), False, None)
    
    col, op, val = condition
    
    if col not in columns:
        raise Exception(f"No column with name {col} found in {table}")
    
    col_idx = columns.index(col)
    
    # Determine operator type and record query
    op_type = _determine_operator_type(op)
    should_create_hash, should_create_sorted = manager.record_query(table, col, op)
    
    # Try to use existing index
    row_numbers = manager.search_with_index(table, col, op, val)
    
    if row_numbers is not None:
        # Index was found and used
        filtered, scanned_lines = _read_rows_by_numbers(
            tbl,
            sorted(row_numbers),
            table=table,
            database=database
        )
        
        print_trace("INDEX", [
            f"Index used for column '{col}'",
            f"Row numbers retrieved: {len(row_numbers)}"
        ])

        # Safety fallback for stale indices:
        # if an index says "no rows", verify with full scan once.
        # This prevents false negatives when index files are outdated.
        if len(row_numbers) == 0:
            scan_filtered, scan_rows = _full_scan_filter(col_idx, op, val)
            if scan_filtered:
                print_trace("INDEX", [
                    f"Stale index detected for {table}.{col}",
                    "Falling back to full scan and rebuilding index"
                ])

                table_data = [row.strip().split(",") for row in scan_rows]
                try:
                    if op == '=':
                        manager.create_hash_index(table, col, table_data, col_idx)
                    else:
                        manager.create_sorted_index(table, col, table_data, col_idx)
                except Exception as e:
                    print(f"Warning: Could not rebuild stale index: {e}")

                return (scan_filtered, len(scan_rows), False, None)

        return (filtered, scanned_lines, True, len(row_numbers))
    
    # No index available, do full table scan
    filtered, rows = _full_scan_filter(col_idx, op, val)
    
    # Check if we should create index after threshold reached
    if should_create_hash or should_create_sorted:
        try:
            # Load full table data for index creation
            all_rows = open(tbl).readlines()
            table_data = [row.strip().split(",") for row in all_rows]
            
            create_hash = op == "=" and should_create_hash
            create_sorted = op != "=" and should_create_sorted

            if create_hash or create_sorted:
                print_trace("INDEX CREATION", [
                    f"Building index for {table}.{col}",
                    f"{'HASH' if create_hash else 'SORTED'} index"
                ])

                if create_hash:
                    manager.create_hash_index(table, col, table_data, col_idx)
                else:
                    manager.create_sorted_index(table, col, table_data, col_idx)
        except Exception as e:
            print(f"Warning: Could not create index: {e}")
    
    return (filtered, len(rows), False, None)


def _read_rows_by_numbers(tbl, row_numbers, table=None, database=None):
    """
    Efficiently read specific rows from a file.
    
    Args:
        tbl: Path to table file
        row_numbers: List of row numbers to read (0-indexed)
    
    Returns:
        Tuple (rows, scanned_lines)
    """
    if not row_numbers:
        return ([], 0)

    rows = []
    targets = sorted(set(row_numbers))

    if table:
        offsets = load_row_offsets(tbl, table, database)
        if offsets and targets:
            max_target = targets[-1]
            if max_target >= len(offsets):
                offsets = build_row_offsets(tbl, table, database)

            with open(tbl, "rb") as f:
                for row_id in targets:
                    if row_id < 0 or row_id >= len(offsets):
                        continue
                    f.seek(offsets[row_id])
                    line = f.readline().decode("utf-8").strip()
                    rows.append(line.split(","))

            return (rows, len(rows))

    # Fallback: sequential read when offsets are unavailable
    targets_set = set(row_numbers)
    max_target = max(targets_set)
    scanned_lines = 0

    with open(tbl, "r") as f:
        for idx, line in enumerate(f):
            scanned_lines += 1

            if idx in targets_set:
                rows.append(line.strip().split(","))
                if len(rows) == len(targets_set):
                    break

            if idx >= max_target:
                break

    return (rows, scanned_lines)


def _condition_type(condition):
    if not condition:
        return None
    if isinstance(condition, tuple):
        return "simple"
    if isinstance(condition, dict):
        return condition.get("type")
    return "simple"


def _resolve_column(columns, column):
    if column in columns:
        return column, columns.index(column)

    if "." in column:
        if column in columns:
            return column, columns.index(column)
        raise Exception(f"No column with name {column} found")

    matches = [c for c in columns if c.endswith("." + column)]
    if len(matches) == 1:
        return matches[0], columns.index(matches[0])
    if len(matches) > 1:
        raise Exception(f"Ambiguous column '{column}'")
    raise Exception(f"No column with name {column} found")


def _apply_condition(rows, columns, condition, database=None):
    if not condition:
        return rows

    cond_type = _condition_type(condition)

    if cond_type == "simple":
        if isinstance(condition, dict):
            column = condition["column"]
            op = condition["operator"]
            value = condition["value"]
        else:
            column, op, value = condition

        _resolved, col_idx = _resolve_column(columns, column)
        filtered = []
        for row in rows:
            if compare(row[col_idx], op, value):
                filtered.append(row)
        return filtered

    if cond_type == "in":
        column = condition["column"]
        _resolved, col_idx = _resolve_column(columns, column)

        sub_rows, _sub_cols = execute_subquery(condition["subquery"], database)
        values = set(extract_in_values(sub_rows))

        print_trace("SUBQUERY", [
            f"Subquery Result : {list(values)}"
        ])

        filtered = []
        for row in rows:
            if remove_quotes(str(row[col_idx])) in values:
                filtered.append(row)
        return filtered

    if cond_type == "exists":
        ensure_non_correlated(condition["subquery"])
        sub_rows, _sub_cols = execute_subquery(condition["subquery"], database)
        exists = len(sub_rows) > 0
        negated = condition.get("negated", False)

        print_trace("SUBQUERY", [
            f"EXISTS Result : {exists}",
            f"Negated : {negated}"
        ])

        if (exists and not negated) or ((not exists) and negated):
            return rows
        return []

    raise Exception("Invalid WHERE condition")


def select_rows(
        table,
        condition,
        selected_columns=None,
        aggregate=None,
        agg_column=None,
        group_by=None,
    having=None,
        order_by=None,
        limit=None,
    database=None,
    join=None,
    return_rows=False
):
    """
    Query data from a table with various filtering and aggregation options.
    Uses adaptive indexing when available.
    If database is specified, queries database-specific table.
    """
    tbl = None
    metadata = None
    columns = []
    used_index = False
    index_hits = None
    rows_scanned = 0

    if join:
        columns, filtered = nested_loop_join(
            table,
            join["table"],
            join["left_column"],
            join["right_column"],
            join["type"],
            database=database
        )
        rows_scanned = len(filtered)
    else:
        tbl, meta = table_paths(table, database)
        check_table_exists(tbl, meta)

        metadata = json.load(open(meta))
        columns = [c[0] for c in metadata["columns"]]

    if selected_columns and selected_columns != ["*"]:
        resolved = []
        for col in selected_columns:
            resolved_col, _ = _resolve_column(columns, col)
            resolved.append(resolved_col)
        selected_columns = resolved

    if group_by:
        resolved_group, _ = _resolve_column(columns, group_by)
        group_by = resolved_group

    if order_by:
        sort_col, _sort_order = order_by
        if "(" not in sort_col:
            resolved_sort, _ = _resolve_column(columns, sort_col)
            order_by = (resolved_sort, _sort_order)

    if aggregate and aggregate != "COUNT":
        if not agg_column:
            raise Exception("Aggregate column is required")
        resolved_agg, _ = _resolve_column(columns, agg_column)
        agg_column = resolved_agg

    if having:
        if not (aggregate and group_by):
            raise Exception("HAVING requires GROUP BY with an aggregate")
        expected_col = agg_column if agg_column else "*"
        if having["agg_column"] != "*":
            resolved_having_col, _ = _resolve_column(columns, having["agg_column"])
            having["agg_column"] = resolved_having_col
        if having["aggregate"] != aggregate:
            raise Exception("HAVING aggregate must match SELECT aggregate")
        if having["agg_column"] != expected_col:
            raise Exception("HAVING aggregate column must match SELECT aggregate column")

    cond_type = _condition_type(condition)

    if not join and cond_type == "simple":
        if isinstance(condition, dict):
            condition_tuple = (
                condition["column"],
                condition["operator"],
                condition["value"]
            )
        else:
            condition_tuple = condition

        filtered, rows_scanned, used_index, index_hits = _get_filtered_rows_with_index(
            table, tbl, condition_tuple, columns, metadata, database
        )
    else:
        if not join:
            rows = open(tbl).readlines()
            filtered = [row.strip().split(",") for row in rows]
            rows_scanned = len(rows)

        filtered = _apply_condition(filtered, columns, condition, database)

    if join:
        print_trace("STORAGE ENGINE", [
            f"Join Query : {table} {join['type']} JOIN {join['table']}",
            f"Rows Scanned : {rows_scanned}",
            "Index Used : No"
        ])
    else:
        trace_lines = [
            f"Open File : {tbl}",
            f"Rows Scanned : {rows_scanned}",
            f"Index Used : {'Yes' if used_index else 'No'}"
        ]
        if used_index:
            trace_lines.append(f"Index Hits : {index_hits}")

        print_trace("STORAGE ENGINE", trace_lines)

    # ===========================
    # ORDER BY (for non-aggregated queries)
    # ===========================

    # Skip ORDER BY here if we have GROUP BY - it will be handled in the GROUP BY section
    if order_by and not (aggregate and group_by):
        sort_col, sort_order = order_by

        if sort_col in columns:
            sort_idx = columns.index(sort_col)

            try:
                filtered.sort(
                    key=lambda row: float(row[sort_idx]) if row[sort_idx] != "NULL" else float("-inf"),
                    reverse=(sort_order == "DESC")
                )
            except (ValueError, IndexError):
                filtered.sort(
                    key=lambda row: row[sort_idx],
                    reverse=(sort_order == "DESC")
                )

    # ===========================
    # LIMIT
    # ===========================

    if limit:
        filtered = filtered[:limit]

    # ===========================
    # AGGREGATE PART
    # ===========================

    if aggregate:

        # ===========================
        # GROUP BY
        # ===========================

        if group_by:
            grp_idx = columns.index(group_by)
            groups = {}
            
            for row in filtered:
                grp_val = row[grp_idx]
                if grp_val not in groups:
                    groups[grp_val] = []
                groups[grp_val].append(row)
            
            # Determine aggregate column name for display
            agg_display = f"{aggregate}({agg_column if agg_column else '*'})"
            
            # Calculate aggregate results for each group
            results = []
            for grp_val, grp_rows in groups.items():
                if aggregate == "COUNT":
                    result = len(grp_rows)
                else:
                    idx = columns.index(agg_column)
                    nums = [float(r[idx]) for r in grp_rows if r[idx] != "NULL"]
                    
                    if not nums:
                        result = "NULL"
                    elif aggregate == "SUM":
                        result = sum(nums)
                    elif aggregate == "AVG":
                        result = sum(nums) / len(nums)
                    elif aggregate == "MIN":
                        result = min(nums)
                    elif aggregate == "MAX":
                        result = max(nums)
                
                results.append((grp_val, result))
            
            # Apply HAVING filter
            if having:
                op = having["operator"]
                val = having["value"]
                filtered_results = []
                for grp_val, result in results:
                    if result == "NULL":
                        continue
                    if compare(str(result), op, val):
                        filtered_results.append((grp_val, result))
                results = filtered_results

            # Handle ORDER BY
            if order_by:
                order_column, order_direction = order_by
                
                # Check if ordering by aggregate function
                if order_column.upper().startswith(aggregate.upper()):
                    # Order by aggregate result (second element in tuple)
                    results.sort(key=lambda x: x[1] if x[1] != "NULL" else float('-inf'), 
                                reverse=(order_direction == "DESC"))
                else:
                    # Order by group column (first element in tuple)
                    results.sort(key=lambda x: x[0], 
                                reverse=(order_direction == "DESC"))
            
            if return_rows:
                result_rows = [[grp_val, result] for grp_val, result in results]
                return result_rows, [group_by, agg_display]

            print(f"\n{group_by} | {agg_display}")
            print("-" * 40)

            for grp_val, result in results:
                print(f"{grp_val} | {result}")

            print_trace("FILE SYSTEM", [
                "Grouped aggregate computed"
            ])

            print_result("✅ Aggregate Operation Completed")
            return

        # ===========================
        # SIMPLE AGGREGATE (NO GROUP BY)
        # ===========================

        if aggregate == "COUNT":
            result = len(filtered)
            if return_rows:
                return [[result]], ["COUNT(*)"]
            print(f"\nCOUNT = {result}")
        else:
            idx = columns.index(agg_column)
            nums = [float(r[idx]) for r in filtered if r[idx] != "NULL"]

            if not nums:
                if return_rows:
                    return [["NULL"]], [f"{aggregate}({agg_column})"]
                print("No rows")
                return

            if aggregate == "SUM":
                result = sum(nums)
            elif aggregate == "AVG":
                result = sum(nums) / len(nums)
            elif aggregate == "MIN":
                result = min(nums)
            elif aggregate == "MAX":
                result = max(nums)

            if return_rows:
                return [[result]], [f"{aggregate}({agg_column})"]
            print(f"\n{aggregate}({agg_column}) = {result}")

        print_trace("FILE SYSTEM", [
            "Aggregate computed"
        ])

        print_result("✅ Aggregate Operation Completed")
        return

    # ======================
    # NORMAL SELECT
    # ======================

    if selected_columns == ["*"]:
        selected_columns = columns

    indexes = [columns.index(c) for c in selected_columns]
    result_rows = [[r[i] for i in indexes] for r in filtered]

    if return_rows:
        return result_rows, selected_columns

    print("\nResult:")
    print(" | ".join(selected_columns))

    for r in result_rows:
        print(" | ".join(r))

    print_trace("FILE SYSTEM", [
        f"{len(filtered)} row(s) returned"
    ])

    print_result("✅ SELECT Operation Completed")
