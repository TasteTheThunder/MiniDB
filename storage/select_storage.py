"""
Storage operations for SELECT
"""
import json
from visualizer import print_trace, print_result
from utils import (
    table_paths,
    check_table_exists,
    compare
)
from index.index_manager import get_index_manager


def _determine_operator_type(op):
    """
    Determine if operator is equality or range query.
    
    Args:
        op: Operator string ('=', '<', '>', '<=', '>=')
    
    Returns:
        'equality' or 'range'
    """
    return 'equality' if op == '=' else 'range'


def _get_filtered_rows_with_index(table, tbl, condition, columns, metadata):
    """
    Attempt to use index for filtering, fallback to full scan.
    
    Args:
        table: Table name
        tbl: Path to table file
        condition: Tuple (column, operator, value) or None
        columns: List of column names
        metadata: Table metadata dict
    
    Returns:
        Tuple (filtered_rows, rows_scanned, used_index)
            - filtered_rows: list of row values [val1, val2, ...]
            - rows_scanned: number of rows examined
            - used_index: bool indicating if index was used
    """
    manager = get_index_manager()
    
    if not condition:
        # No condition, must do full table scan
        rows = open(tbl).readlines()
        filtered = [row.strip().split(",") for row in rows]
        return (filtered, len(rows), False)
    
    col, op, val = condition
    
    if col not in columns:
        # Column doesn't exist, full scan needed
        rows = open(tbl).readlines()
        filtered = [row.strip().split(",") for row in rows]
        return (filtered, len(rows), False)
    
    col_idx = columns.index(col)
    
    # Determine operator type and record query
    op_type = _determine_operator_type(op)
    should_create_hash, should_create_sorted = manager.record_query(table, col, op)
    
    # Try to use existing index
    row_numbers = manager.search_with_index(table, col, op, val)
    
    if row_numbers is not None:
        # Index was found and used
        filtered = []
        all_rows = open(tbl).readlines()
        
        for row_num in sorted(row_numbers):
            if row_num < len(all_rows):
                filtered.append(all_rows[row_num].strip().split(","))
        
        print_trace("INDEX", [
            f"Index used for column '{col}'",
            f"Row numbers retrieved: {len(row_numbers)}"
        ])
        
        return (filtered, len(all_rows), True)
    
    # No index available, do full table scan
    rows = open(tbl).readlines()
    filtered = []
    
    for row in rows:
        vals = row.strip().split(",")
        
        if not compare(vals[col_idx], op, val):
            continue
        
        filtered.append(vals)
    
    # Check if we should create index after threshold reached
    if should_create_hash or should_create_sorted:
        try:
            # Load full table data for index creation
            all_rows = open(tbl).readlines()
            table_data = [row.strip().split(",") for row in all_rows]
            
            print_trace("INDEX CREATION", [
                f"Building index for {table}.{col}",
                f"{'HASH' if should_create_hash else 'SORTED'} index"
            ])
            
            if should_create_hash:
                manager.create_hash_index(table, col, table_data, col_idx)
            else:
                manager.create_sorted_index(table, col, table_data, col_idx)
        except Exception as e:
            print(f"Warning: Could not create index: {e}")
    
    return (filtered, len(rows), False)


def _read_rows_by_numbers(tbl, row_numbers):
    """
    Efficiently read specific rows from a file.
    
    Args:
        tbl: Path to table file
        row_numbers: List of row numbers to read (0-indexed)
    
    Returns:
        List of rows as list of values
    """
    rows = []
    all_lines = open(tbl).readlines()
    
    for row_num in row_numbers:
        if row_num < len(all_lines):
            rows.append(all_lines[row_num].strip().split(","))
    
    return rows


def select_rows(
        table,
        condition,
        selected_columns=None,
        aggregate=None,
        agg_column=None,
        group_by=None,
        order_by=None,
        limit=None
):
    """
    Query data from a table with various filtering and aggregation options.
    Uses adaptive indexing when available.
    """
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)

    metadata = json.load(open(meta))
    columns = [c[0] for c in metadata["columns"]]

    # Use index-aware filtering
    filtered, rows_scanned, used_index = _get_filtered_rows_with_index(
        table, tbl, condition, columns, metadata
    )

    print_trace("STORAGE ENGINE", [
        f"Open File : {tbl}",
        f"Rows Scanned : {rows_scanned}",
        f"Index Used : {'Yes' if used_index else 'No'}"
    ])

    # ===========================
    # ORDER BY (for non-aggregated queries)
    # ===========================

    # Skip ORDER BY here if we have GROUP BY - it will be handled in the GROUP BY section
    if order_by and not (aggregate and group_by):
        sort_col, sort_order = order_by
        
        # Check if column exists in the table
        if sort_col in columns:
            sort_idx = columns.index(sort_col)
            
            # Try to sort numerically, fall back to string sort
            try:
                filtered.sort(
                    key=lambda row: float(row[sort_idx]) if row[sort_idx] != "NULL" else float('-inf'),
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
            
            # Print results
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
            print(f"\nCOUNT = {result}")

        else:
            idx = columns.index(agg_column)
            nums = [float(r[idx]) for r in filtered]

            if not nums:
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

    print("\nResult:")
    print(" | ".join(selected_columns))

    for r in filtered:
        print(" | ".join([r[i] for i in indexes]))

    print_trace("FILE SYSTEM", [
        f"{len(filtered)} row(s) returned"
    ])

    print_result("✅ SELECT Operation Completed")
