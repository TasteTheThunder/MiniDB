"""
Parser for SELECT command
"""

def parse_select(tokens):
    """
    Parse SELECT statement
    Syntax: SELECT columns FROM table [WHERE condition] [GROUP BY col] [ORDER BY col] [LIMIT n]
    """
    from_index = tokens.index("FROM")

    select_columns = [
        t for t in tokens[1:from_index]
        if t != ","
    ]

    table = tokens[from_index + 1]

    condition = None
    group_by = None
    order_by = None
    limit = None
    aggregate = None
    agg_column = None


    # ---------------- WHERE ----------------
    if "WHERE" in tokens:
        idx = tokens.index("WHERE")
        condition = (
            tokens[idx + 1],   # column
            tokens[idx + 2],   # operator
            tokens[idx + 3]    # value
        )


    # ---------------- AGGREGATE ----------------
    aggregates = ["COUNT", "SUM", "AVG", "MIN", "MAX"]

    if select_columns:

        # Check all columns for aggregate functions
        for col in select_columns:
            if col.upper() in aggregates:
                aggregate = col.upper()
                # Find the column being aggregated
                if "(" in tokens:
                    # Find position of this aggregate in tokens
                    for i, t in enumerate(tokens):
                        if t.upper() == aggregate and i + 1 < len(tokens) and tokens[i + 1] == "(":
                            agg_column = tokens[i + 2]
                            break
                break


    # ---------------- GROUP BY ----------------
    if "GROUP" in tokens and "BY" in tokens:
        g = tokens.index("GROUP")
        group_by = tokens[g + 2]


    # ---------------- ORDER BY ----------------
    if "ORDER" in tokens and "BY" in tokens:
        o = tokens.index("ORDER")
        column = tokens[o + 2]
        
        # Check if ORDER BY uses an aggregate function like COUNT(*), SUM(col), etc.
        if column.upper() in ["COUNT", "SUM", "AVG", "MIN", "MAX"]:
            # Check if next tokens are ( column/* )
            if o + 3 < len(tokens) and tokens[o + 3] == "(":
                # Find the closing parenthesis
                close_paren_idx = o + 3
                while close_paren_idx < len(tokens) and tokens[close_paren_idx] != ")":
                    close_paren_idx += 1
                
                # Extract the aggregate column (or *)
                agg_col = tokens[o + 4] if o + 4 < len(tokens) else "*"
                column = f"{column}({agg_col})"
                
                # Check for ASC/DESC after the closing parenthesis
                order = "ASC"
                if close_paren_idx + 1 < len(tokens):
                    if tokens[close_paren_idx + 1] in ["ASC", "DESC"]:
                        order = tokens[close_paren_idx + 1]
            else:
                # Regular column with possible ASC/DESC
                order = "ASC"
                if len(tokens) > o + 3:
                    if tokens[o + 3] in ["ASC", "DESC"]:
                        order = tokens[o + 3]
        else:
            # Regular column ORDER BY
            order = "ASC"
            if len(tokens) > o + 3:
                if tokens[o + 3] in ["ASC", "DESC"]:
                    order = tokens[o + 3]

        order_by = (column, order)


    # ---------------- LIMIT ----------------
    if "LIMIT" in tokens:
        l = tokens.index("LIMIT")
        limit = int(tokens[l + 1])


    # ---------------- FINAL COMMAND ----------------
    # Filter out aggregate function tokens from columns
    non_agg_columns = select_columns
    if aggregate:
        # Remove aggregate-related tokens: aggregate name, '(', column, ')'
        non_agg_columns = [
            c for c in select_columns 
            if c.upper() not in ["COUNT", "SUM", "AVG", "MIN", "MAX", "(", ")"]
            and c != agg_column
        ]
        # If we have GROUP BY, use non-aggregate columns, otherwise None
        if group_by:
            select_columns = non_agg_columns if non_agg_columns else None
        else:
            select_columns = None
    
    command = {
        "type": "SELECT",
        "table": table,
        "columns": select_columns,
        "aggregate": aggregate,
        "agg_column": agg_column,
        "condition": condition,
        "group_by": group_by,
        "order_by": order_by,
        "limit": limit
    }
    
    return command
