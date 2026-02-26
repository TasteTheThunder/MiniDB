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
    Query data from a table with various filtering and aggregation options
    """
    tbl, meta = table_paths(table)
    check_table_exists(tbl, meta)

    metadata = json.load(open(meta))
    columns = [c[0] for c in metadata["columns"]]

    rows = open(tbl).readlines()

    print_trace("STORAGE ENGINE", [
        f"Open File : {tbl}",
        f"Rows Scanned : {len(rows)}"
    ])

    filtered = []

    for row in rows:
        vals = row.strip().split(",")

        if condition:
            col, op, val = condition
            idx = columns.index(col)

            if not compare(vals[idx], op, val):
                continue

        filtered.append(vals)

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
